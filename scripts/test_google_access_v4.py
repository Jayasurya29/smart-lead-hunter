"""Phase 2 v4.6 — Hospitality enrichment.

For each contact extracted in v4.5, look up:
  - Brand intel from BrandRegistry (parent, tier, operating model, GPO)
  - Management company intel from MANAGEMENT_COMPANY_INTEL
  - Procurement priority (P1/P2/P3/P4) from title + brand-aware tier classifier
  - Opportunity level from BrandRegistry (high/medium/low)
  - Opportunity score = interaction_count × contact_score_multiplier

New CSV columns:
  parent_company, brand_tier, operating_model, gpo,
  procurement_priority, priority_reason,
  opportunity_level, opportunity_score, management_company

This is the part SigParser literally cannot do. They give you contacts.
We give you contacts pre-classified by procurement value to JA Uniforms.
"""

from __future__ import annotations

import asyncio
import base64
import csv
import hashlib
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import httpx

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from google.oauth2 import service_account  # noqa: E402
from googleapiclient.discovery import build  # noqa: E402
from googleapiclient.errors import HttpError  # noqa: E402

try:
    import phonenumbers
except ImportError:
    sys.exit("❌ phonenumbers not installed. Run: pip install phonenumbers")

from app.services.ai_client import ai_generate  # noqa: E402
from app.config.brand_registry import BrandRegistry, BrandInfo  # noqa: E402
from app.config.procurement_intelligence import (  # noqa: E402
    MANAGEMENT_COMPANY_INTEL,
    get_management_company_intel,
)


# ──────────────────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────────────────

CREDENTIALS_PATH = PROJECT_ROOT / "credentials" / "slh-contact-sync.json"
TARGET_USER = "ugarcia@jauniforms.com"
SCAN_DAYS_BACK = 45
MAX_EMAILS_TO_SCAN = 20000

OUTPUT_DIR = PROJECT_ROOT / "contact_exports"

DEBUG_DUMP_SIGNATURES = True

PHONE_DEFAULT_REGION = "US"
MIN_CONFIDENCE = 0.6

SCOPES = [
    "https://www.googleapis.com/auth/contacts.readonly",
    "https://www.googleapis.com/auth/contacts.other.readonly",
    "https://www.googleapis.com/auth/gmail.readonly",
]

OWN_DOMAINS = {"jauniforms.com", "jauniforms.org", "ja-uniforms.com", "ja-uniforms.org"}

NOREPLY_PATTERNS = [
    "noreply", "no-reply", "donotreply", "do-not-reply",
    "mailer-daemon", "postmaster", "bounces", "auto-reply",
    "auto-confirm", "auto-receipt", "automated",
]
MASS_MAIL_PATTERNS = ["newsletter", "unsubscribe", "digest"]

SAAS_DOMAINS = {
    "mailchimp.com", "constantcontact.com", "campaignmonitor.com",
    "sendgrid.net", "mandrillapp.com", "sparkpostmail.com",
    "hs-send.com", "hubspotemail.net",
    "amazonses.com", "ses.amazonaws.com",
    "facebookmail.com",
    "notify.railway.app", "news.railway.app",
    "notifications.google.com",
}

PERSONAL_DOMAINS = {
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "aol.com",
    "icloud.com", "me.com", "mac.com", "msn.com", "live.com",
    "comcast.net", "verizon.net", "att.net", "sbcglobal.net", "cox.net",
    "earthlink.net", "ymail.com", "rocketmail.com", "googlemail.com",
    "protonmail.com", "proton.me", "pm.me", "fastmail.com",
    "mail.com", "gmx.com", "yandex.com", "duck.com", "hey.com",
}

GENERIC_DOMAINS_NO_ORG = {
    "google.com", "googlegroups.com", "microsoft.com", "microsoftonline.com",
    "office365.com", "office.com", "outlook.com",
    "linkedin.com", "github.com", "amazon.com", "amazonses.com",
    "apple.com", "icloud.com",
    "zendesk.com", "zoho.com", "salesforce.com",
}

CSV_COLUMNS = [
    "email", "first_name", "last_name", "display_name",
    "title", "organization", "phone", "address", "linkedin_url",
    "confidence", "interaction_count",
    "has_signature", "in_other_contacts", "in_saved_contacts",
    "org_source",
    # v4.6: hospitality enrichment columns
    "parent_company",
    "brand_tier",
    "operating_model",
    "gpo",
    "procurement_priority",
    "priority_reason",
    "opportunity_level",
    "opportunity_score",
    "management_company",
    "mailbox", "scanned_at",
]


# ──────────────────────────────────────────────────────────────────────────
# Body preprocessing (v4.4)
# ──────────────────────────────────────────────────────────────────────────

_ANGLE_URL_RE = re.compile(r"<https?://[^>\s]*>", re.IGNORECASE)
_IMAGE_MARKER_RE = re.compile(r"\[image:[^\]]*\]", re.IGNORECASE)
_CID_REF_RE = re.compile(r"\[cid:[^\]]*\]", re.IGNORECASE)
_WARNING_LINE_PATTERNS = [
    re.compile(r"^\s*CAUTION:.*$", re.MULTILINE | re.IGNORECASE),
    re.compile(r"^.*This message is from an EXTERNAL SENDER.*$",
               re.MULTILINE | re.IGNORECASE),
    re.compile(r"^\s*\[EXTERNAL\].*$", re.MULTILINE | re.IGNORECASE),
    re.compile(r"^\s*\*\*\* EXTERNAL EMAIL.*$", re.MULTILINE | re.IGNORECASE),
    re.compile(r"^.*External Email.*Do not click.*$",
               re.MULTILINE | re.IGNORECASE),
]


def reflow_soft_wrapped_lines(text: str) -> tuple[str, int]:
    lines = text.split("\n")
    if len(lines) < 2:
        return text, 0
    out: List[str] = []
    joins = 0
    skip_next = False
    for i, line in enumerate(lines):
        if skip_next:
            skip_next = False
            continue
        if i + 1 < len(lines):
            cur = line.rstrip()
            nxt = lines[i + 1]
            cur_stripped = cur.strip()
            is_short = 0 < len(cur_stripped) < 25
            is_single_word = " " not in cur_stripped and "\t" not in cur_stripped
            ends_with_letter = bool(cur_stripped) and cur_stripped[-1].isalpha()
            next_starts_lower = bool(nxt) and bool(nxt.lstrip()) and nxt.lstrip()[0].islower()
            if is_short and is_single_word and ends_with_letter and next_starts_lower:
                out.append(cur + nxt.lstrip())
                skip_next = True
                joins += 1
                continue
        out.append(line)
    return "\n".join(out), joins


def preprocess_body(body: str) -> tuple[str, Dict[str, int]]:
    stats = {"urls_stripped": 0, "images_stripped": 0, "warnings_stripped": 0, "lines_joined": 0}
    if not body:
        return body, stats
    body, n = _ANGLE_URL_RE.subn("", body)
    stats["urls_stripped"] = n
    body, n1 = _IMAGE_MARKER_RE.subn("", body)
    body, n2 = _CID_REF_RE.subn("", body)
    stats["images_stripped"] = n1 + n2
    for pat in _WARNING_LINE_PATTERNS:
        body, n = pat.subn("", body)
        stats["warnings_stripped"] += n
    body, n = reflow_soft_wrapped_lines(body)
    stats["lines_joined"] = n
    body = re.sub(r"\n{3,}", "\n\n", body)
    return body, stats


# ──────────────────────────────────────────────────────────────────────────
# Domain-to-organization inference (v4.5)
# ──────────────────────────────────────────────────────────────────────────

_DOMAIN_PREFIXES_TO_STRIP = ("mail.", "email.", "e.", "news.", "info.", "m.",
                              "go.", "hi.", "hello.", "support.", "noreply.",
                              "no-reply.", "messages.", "send.", "post.")


def infer_organization_from_domain(domain: str) -> Optional[str]:
    if not domain or "@" in domain:
        return None
    domain = domain.lower().strip()
    if domain in PERSONAL_DOMAINS:
        return None
    if domain in OWN_DOMAINS:
        return None
    if domain in GENERIC_DOMAINS_NO_ORG:
        return None
    for prefix in _DOMAIN_PREFIXES_TO_STRIP:
        if domain.startswith(prefix):
            domain = domain[len(prefix):]
            break
    parts = domain.split(".")
    if len(parts) < 2:
        return None
    if len(parts) >= 3 and parts[-2] in {"co", "com", "org", "net", "gov", "ac", "edu"} and len(parts[-1]) == 2:
        stem = ".".join(parts[:-2])
    else:
        stem = ".".join(parts[:-1])
    if "." in stem:
        stem = stem.split(".")[-1]
    if not stem or len(stem) < 2:
        return None
    chunks = stem.replace("_", "-").split("-")
    out_words: List[str] = []
    for chunk in chunks:
        if not chunk:
            continue
        if 2 <= len(chunk) <= 3 and chunk.isalpha():
            out_words.append(chunk.upper())
        else:
            out_words.append(chunk.capitalize())
    name = " ".join(out_words).strip()
    return name if name else None


# ──────────────────────────────────────────────────────────────────────────
# NEW v4.6: Hospitality enrichment — procurement priority classifier
# ──────────────────────────────────────────────────────────────────────────

# P1 — direct procurement decision-makers (uniform buying authority)
P1_TITLE_KEYWORDS = [
    "director of procurement", "vp procurement", "svp procurement",
    "director of purchasing", "vp purchasing",
    "purchasing manager", "purchasing director",
    "chief procurement officer", "cpo",
    "director of housekeeping",  # primary uniform spec author at property level
    "executive housekeeper",
    "head housekeeper",
]

# P2 — operational decision-makers / strong influencers
# Mix of phrases (substring match) and word-boundary tokens to avoid false hits
# like 'coo' matching inside 'coordinator'.
P2_TITLE_KEYWORDS = [
    "general manager", "hotel manager", "resort manager",
    "director of operations", "director of rooms",
    "chief operating officer",
    "vp operations", "vp hotel operations", "svp operations",
    "regional vp operations", "regional vice president operations",
    "vp of operations", "senior vice president operations",
    "director of food and beverage", "f&b director", "director of f&b",
    "director of food & beverage",
]
# Short tokens that must match as whole words only (regex word boundaries)
P2_WORD_TOKENS = ("gm", "coo")

# P3 — useful contacts but not direct buyers
P3_TITLE_KEYWORDS = [
    "human resources", "people and culture", "people & culture",
    "talent",
    "executive chef", "head chef",
    "director of sales", "director of events",
    "controller", "director of finance",
    "front office manager", "front desk manager",
    "spa director", "director of spa",
    "accounts payable", "accounts receivable",
    "ap coordinator", "ap manager",
]
P3_WORD_TOKENS = ("hr",)

# P4 — corporate/non-buyer roles
P4_TITLE_KEYWORDS = [
    "legal", "attorney", "counsel",
    "information technology",
    "marketing", "public relations", "communications",
    "engineering", "maintenance",
    "security",
]
P4_WORD_TOKENS = ("it", "pr")


def _has_word_token(text: str, tokens: tuple) -> bool:
    """True if any token appears as a whole word (case-insensitive)."""
    for tok in tokens:
        if re.search(rf"\b{re.escape(tok)}\b", text, re.IGNORECASE):
            return True
    return False

# Words that suggest the title represents a real procurement decision authority
# even when the literal title differs (e.g. "Sr. Buyer, Procurement SA")
PROCUREMENT_AUTHORITY_HINTS = (
    "procurement", "purchasing", "buyer", "sourcing",
)


def classify_procurement_priority(
    title: Optional[str],
    brand_info: BrandInfo,
    domain: str,
) -> Tuple[str, str]:
    """Classify a contact's procurement priority based on title + brand tier system.

    Returns (priority, reason) where priority is P1/P2/P3/P4/P_unknown.

    Logic:
      1. Strong P1 — title matches direct procurement keyword
      2. If brand uses GPO (Avendra/HSM) and title is property-level → demote
         one level because property buyers have limited authority in GPO model
      3. Use brand_info.tier_for_title() to classify tier (property/cluster/regional)
      4. Procurement-flavored regional roles → P1 regardless of brand
      5. Otherwise fall through to P2/P3/P4 keyword checks
    """
    if not title:
        return "P_unknown", "no title available"

    tl = title.lower().strip()

    # Hard P4 first — exclude before any other classification
    if any(kw in tl for kw in P4_TITLE_KEYWORDS) or _has_word_token(tl, P4_WORD_TOKENS):
        return "P4", "non-buyer role (legal/IT/marketing/etc.)"

    # Strong P1 — direct procurement / housekeeping authority
    for kw in P1_TITLE_KEYWORDS:
        if kw in tl:
            return "P1", f"direct procurement title: '{kw.strip()}'"

    # Procurement-flavored regional or cluster roles
    has_proc_authority = any(h in tl for h in PROCUREMENT_AUTHORITY_HINTS)
    brand_title_tier = brand_info.tier_for_title(title)

    if has_proc_authority:
        return "P1", "procurement keyword in title (sourcing/buyer/purchasing)"

    # Operational/GM titles — but downgrade if GPO-constrained
    p2_match = (
        any(kw in tl for kw in P2_TITLE_KEYWORDS)
        or _has_word_token(tl, P2_WORD_TOKENS)
    )
    if p2_match:
        # Property GM at a GPO-constrained brand has limited uniform authority
        if brand_info.gpo and brand_title_tier == "property":
            return "P3", (
                f"property-level operational role at GPO-constrained brand "
                f"({brand_info.gpo}); limited uniform authority"
            )
        # Regional/cluster ops at any brand → P2
        if brand_title_tier in ("regional", "cluster"):
            return "P2", f"{brand_title_tier} operational executive"
        # Property GM at non-GPO brand → P2
        return "P2", "property-level operational decision-maker"

    # P3 — useful but indirect
    if any(kw in tl for kw in P3_TITLE_KEYWORDS) or _has_word_token(tl, P3_WORD_TOKENS):
        return "P3", "secondary contact (HR/F&B/Sales/admin)"

    # Default — unknown
    return "P_unknown", "title didn't match known patterns"


def enrich_contact(merged: dict) -> dict:
    """Look up brand intel + procurement priority + management company.

    Mutates and returns merged dict with new fields populated.
    """
    org = merged.get("organization") or ""
    title = merged.get("title")
    domain = merged["email"].split("@")[-1].lower() if "@" in merged.get("email", "") else ""

    # Default empty enrichment
    enrichment_defaults = {
        "parent_company": None,
        "brand_tier": None,
        "operating_model": None,
        "gpo": None,
        "procurement_priority": "P_unknown",
        "priority_reason": "no organization to enrich",
        "opportunity_level": None,
        "opportunity_score": None,
        "management_company": None,
    }

    if not org:
        merged.update(enrichment_defaults)
        return merged

    # ── BRAND LOOKUP ──────────────────────────────────────────────────
    brand_info: BrandInfo = BrandRegistry.lookup(org)

    # If brand_info is the "unknown" default, mark it differently in output
    # so we can distinguish "we have no brand intel" from real lookups
    is_unknown_brand = brand_info.parent_company == "Unknown"

    if is_unknown_brand:
        merged["parent_company"] = None
        merged["brand_tier"] = None
        merged["operating_model"] = "independent"  # safe default per registry
        merged["gpo"] = None
        merged["opportunity_level"] = brand_info.opportunity_level
    else:
        merged["parent_company"] = brand_info.parent_company
        merged["brand_tier"] = brand_info.tier
        merged["operating_model"] = brand_info.operating_model
        merged["gpo"] = brand_info.gpo
        merged["opportunity_level"] = brand_info.opportunity_level

    # ── MANAGEMENT COMPANY LOOKUP ─────────────────────────────────────
    # Check if the org IS a management company (Towne Park, Crescent, etc.)
    mgmt_intel = get_management_company_intel(org)
    merged["management_company"] = org if mgmt_intel else None

    # ── PROCUREMENT PRIORITY ──────────────────────────────────────────
    priority, reason = classify_procurement_priority(title, brand_info, domain)
    merged["procurement_priority"] = priority
    merged["priority_reason"] = reason

    # ── OPPORTUNITY SCORE ─────────────────────────────────────────────
    # interaction_count × brand multiplier, rounded
    interactions = merged.get("interaction_count") or 0
    multiplier = brand_info.contact_score_multiplier
    merged["opportunity_score"] = round(interactions * multiplier, 2)

    return merged


# ──────────────────────────────────────────────────────────────────────────
# Regex utilities
# ──────────────────────────────────────────────────────────────────────────

_EMAIL_REGEX = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")
_HTML_TAG_RE = re.compile(r"<[^>]+>")


def extract_emails(s: str) -> List[str]:
    if not s:
        return []
    return [m.group(0).lower() for m in _EMAIL_REGEX.finditer(s)]


def extract_display_name(header_value: str) -> str:
    if not header_value:
        return ""
    m = re.match(r"^\s*\"?([^\"<]+?)\"?\s*<", header_value)
    return m.group(1).strip() if m else ""


def domain_of(email: str) -> str:
    return email.split("@")[-1].lower() if "@" in email else ""


def passes_hard_filters(email: str) -> tuple[bool, str]:
    if "@" not in email:
        return False, "malformed"
    if email == TARGET_USER:
        return False, "self"
    d = domain_of(email)
    if d in OWN_DOMAINS:
        return False, "own_company"
    local = email.split("@")[0].lower()
    if any(p in local for p in NOREPLY_PATTERNS):
        return False, "noreply"
    if any(p in local for p in MASS_MAIL_PATTERNS):
        return False, "mass_mail"
    if d in SAAS_DOMAINS or any(d.endswith("." + s) for s in SAAS_DOMAINS):
        return False, "saas"
    return True, "ok"


def soft_keep_personal(email: str, merged: dict) -> bool:
    d = domain_of(email)
    if d not in PERSONAL_DOMAINS:
        return True
    return bool(merged.get("organization") or merged.get("title"))


def extract_sender_name_from_segment(segment: str) -> str:
    head = segment[:1000]
    m = re.search(
        r"On\s+.{5,150}?,\s*([A-Z][^<\n]{1,80}?)(?=\s*<|\s+wrote:)",
        head,
    )
    if m:
        name = m.group(1).strip().strip('"').strip(",")
        if name and len(name) > 1:
            return name
    m = re.search(
        r"^[\s>]*From:\s+([^<\n]{2,80}?)(?:\s*<|\s*$)",
        head,
        re.MULTILINE,
    )
    if m:
        name = m.group(1).strip().strip('"')
        if name and "@" not in name and len(name) > 1:
            return name
    return ""


def signature_name_matches_header(parsed: dict, header_name: str) -> bool:
    fn = (parsed.get("first_name") or "").lower().strip()
    ln = (parsed.get("last_name") or "").lower().strip()
    if not fn and not ln:
        return True
    if not header_name:
        return True
    hn = header_name.lower()
    if fn and len(fn) > 1 and fn in hn:
        return True
    if ln and len(ln) > 1 and ln in hn:
        return True
    return False


# ──────────────────────────────────────────────────────────────────────────
# Phone validation
# ──────────────────────────────────────────────────────────────────────────

_PHONE_PREFIXES = (
    "tel:", "tel.", "phone:", "ph:", "t:", "telephone:",
    "mobile:", "cell:", "m:", "c:", "office:", "o:", "direct:", "d:",
    "fax:", "f:",
)


def validate_phone(raw: Optional[str], default_region: str = PHONE_DEFAULT_REGION) -> Optional[str]:
    if not raw or not isinstance(raw, str):
        return None
    raw = raw.strip()
    low = raw.lower()
    for prefix in _PHONE_PREFIXES:
        if low.startswith(prefix):
            raw = raw[len(prefix):].strip()
            low = raw.lower()
            break
    raw_base = re.split(
        r"\s*(?:ext|x|extension)\.?\s*\d",
        raw,
        flags=re.IGNORECASE,
        maxsplit=1,
    )[0].strip()
    for region in (default_region, None):
        try:
            num = phonenumbers.parse(raw_base, region)
            if phonenumbers.is_valid_number(num):
                return phonenumbers.format_number(
                    num, phonenumbers.PhoneNumberFormat.E164
                )
        except phonenumbers.NumberParseException:
            continue
    return None


# ──────────────────────────────────────────────────────────────────────────
# Google clients
# ──────────────────────────────────────────────────────────────────────────


def build_credentials():
    if not CREDENTIALS_PATH.exists():
        sys.exit(f"❌ Credentials not found: {CREDENTIALS_PATH}")
    print(f"🔐 Loading credentials from {CREDENTIALS_PATH}")
    print(f"   Impersonating: {TARGET_USER}")
    creds = service_account.Credentials.from_service_account_file(
        str(CREDENTIALS_PATH), scopes=SCOPES
    )
    return creds.with_subject(TARGET_USER)


def build_gmail_client(creds):
    return build("gmail", "v1", credentials=creds, cache_discovery=False)


def build_people_client(creds):
    return build("people", "v1", credentials=creds, cache_discovery=False)


def dump_other_contacts(people) -> Dict[str, dict]:
    print("\n📇 Dumping Other Contacts via People API...")
    contacts: Dict[str, dict] = {}
    page_token = None
    pages = 0
    while True:
        try:
            resp = people.otherContacts().list(
                pageSize=1000,
                readMask="names,emailAddresses,phoneNumbers",
                pageToken=page_token,
            ).execute()
        except HttpError as e:
            print(f"   ❌ otherContacts error: {e}")
            break
        for person in resp.get("otherContacts", []):
            emails = person.get("emailAddresses", [])
            if not emails:
                continue
            primary_email = (emails[0].get("value") or "").lower()
            if not primary_email:
                continue
            names = person.get("names", [])
            phones = person.get("phoneNumbers", [])
            contacts[primary_email] = {
                "email": primary_email,
                "first_name": names[0].get("givenName") if names else None,
                "last_name": names[0].get("familyName") if names else None,
                "display_name": names[0].get("displayName") if names else None,
                "phone": phones[0].get("value") if phones else None,
                "source": "other_contacts",
            }
        pages += 1
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    print(f"   ✓ {len(contacts)} contacts from Other Contacts ({pages} page(s))")
    return contacts


def dump_saved_contacts(people) -> Dict[str, dict]:
    print("\n📇 Dumping Saved Contacts via People API...")
    contacts: Dict[str, dict] = {}
    page_token = None
    while True:
        try:
            resp = people.people().connections().list(
                resourceName="people/me",
                pageSize=1000,
                personFields="names,emailAddresses,organizations,phoneNumbers,addresses",
                pageToken=page_token,
            ).execute()
        except HttpError as e:
            print(f"   ❌ connections error: {e}")
            break
        for person in resp.get("connections", []):
            emails = person.get("emailAddresses", [])
            if not emails:
                continue
            primary_email = (emails[0].get("value") or "").lower()
            if not primary_email:
                continue
            names = person.get("names", [])
            orgs = person.get("organizations", [])
            phones = person.get("phoneNumbers", [])
            addrs = person.get("addresses", [])
            contacts[primary_email] = {
                "email": primary_email,
                "first_name": names[0].get("givenName") if names else None,
                "last_name": names[0].get("familyName") if names else None,
                "display_name": names[0].get("displayName") if names else None,
                "organization": orgs[0].get("name") if orgs else None,
                "title": orgs[0].get("title") if orgs else None,
                "phone": phones[0].get("value") if phones else None,
                "address": addrs[0].get("formattedValue") if addrs else None,
                "source": "saved_contacts",
            }
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    print(f"   ✓ {len(contacts)} contacts from Saved Contacts")
    return contacts


# ──────────────────────────────────────────────────────────────────────────
# Gmail
# ──────────────────────────────────────────────────────────────────────────


def list_message_ids(gmail) -> List[str]:
    query = f"newer_than:{SCAN_DAYS_BACK}d -in:chats -in:spam -in:trash"
    print(f"\n📧 Listing messages (last {SCAN_DAYS_BACK} days)...")
    ids: List[str] = []
    page_token = None
    while len(ids) < MAX_EMAILS_TO_SCAN:
        try:
            resp = gmail.users().messages().list(
                userId="me", q=query,
                maxResults=min(500, MAX_EMAILS_TO_SCAN - len(ids)),
                pageToken=page_token,
            ).execute()
        except HttpError as e:
            print(f"   ❌ list error: {e}")
            break
        for m in resp.get("messages", []):
            ids.append(m["id"])
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    print(f"   ✓ Found {len(ids)} messages")
    return ids


def fetch_full_message(gmail, message_id: str) -> Optional[dict]:
    try:
        return gmail.users().messages().get(
            userId="me", id=message_id, format="full"
        ).execute()
    except HttpError:
        return None


def _b64_decode(data: str) -> str:
    try:
        return base64.urlsafe_b64decode(data + "=" * (-len(data) % 4)).decode(
            "utf-8", errors="replace"
        )
    except Exception:
        return ""


def _strip_html(html: str) -> str:
    if not html:
        return ""
    html = re.sub(r"<(script|style)[^>]*>.*?</\1>", "", html, flags=re.IGNORECASE | re.DOTALL)
    html = re.sub(r"</td>\s*<td[^>]*>", " | ", html, flags=re.IGNORECASE)
    html = re.sub(r"</?(br|p|div|tr|li|h[1-6])[^>]*>", "\n", html, flags=re.IGNORECASE)
    text = _HTML_TAG_RE.sub("", html)
    text = (text.replace("&nbsp;", " ").replace("&amp;", "&")
                .replace("&lt;", "<").replace("&gt;", ">").replace("&quot;", '"')
                .replace("&#39;", "'").replace("&#x27;", "'"))
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    return text


def extract_plain_text(payload: dict) -> str:
    mime = payload.get("mimeType", "")
    body = payload.get("body", {})
    if mime == "text/plain" and body.get("data"):
        return _b64_decode(body["data"])
    if mime.startswith("multipart/"):
        parts = payload.get("parts") or []
        plain = ""
        html = ""
        for p in parts:
            text = extract_plain_text(p)
            if p.get("mimeType") == "text/plain" and not plain:
                plain = text
            elif p.get("mimeType") == "text/html" and not html:
                html = text
            elif p.get("mimeType", "").startswith("multipart/"):
                nested = extract_plain_text(p)
                if nested and not plain:
                    plain = nested
        return plain or _strip_html(html)
    if mime == "text/html" and body.get("data"):
        return _strip_html(_b64_decode(body["data"]))
    return ""


_SECTION_BOUNDARIES = [
    re.compile(r"^[\s>]*From:\s.+$", re.MULTILINE),
    re.compile(r"^[\s>]*On\s.{5,200}wrote:\s*$", re.MULTILINE),
    re.compile(r"^[\s>]*-+\s*Forwarded message\s*-+", re.MULTILINE | re.IGNORECASE),
    re.compile(r"^[\s>]*-+\s*Original Message\s*-+", re.MULTILINE | re.IGNORECASE),
    re.compile(r"^[\s>]*Begin forwarded message:", re.MULTILINE | re.IGNORECASE),
    re.compile(r"^[\s>]*Sent:\s.+$", re.MULTILINE),
    re.compile(r"^[\s>]*_{10,}\s*$", re.MULTILINE),
    re.compile(r"^[\s>]*={10,}\s*$", re.MULTILINE),
    re.compile(r"^[\s>]*-{10,}\s*$", re.MULTILINE),
    re.compile(r"^[\s>]*Le\s.{5,200}écrit\s*:\s*$", re.MULTILINE),
    re.compile(r"^[\s>]*Am\s.{5,200}schrieb\s.+:\s*$", re.MULTILINE),
]


def split_into_segments(body: str) -> List[str]:
    if not body:
        return []
    positions = [0]
    for pattern in _SECTION_BOUNDARIES:
        for m in pattern.finditer(body):
            positions.append(m.start())
    positions.append(len(body))
    positions = sorted(set(positions))
    segments = []
    for i in range(len(positions) - 1):
        seg = body[positions[i]:positions[i + 1]].strip()
        if len(seg) > 50:
            segments.append(seg)
    if not segments:
        return [body.strip()] if body.strip() else []
    return segments


def extract_email_in_segment_header(segment: str) -> Optional[str]:
    head = segment[:1000]
    m = re.search(
        r"From:\s*(?:[^<\n]{0,100}<)?([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})",
        head,
    )
    if m:
        return m.group(1).lower()
    near = re.search(
        r"(?:From|wrote|Sent|mailto):.{0,300}?([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})",
        head,
        re.DOTALL,
    )
    if near:
        return near.group(1).lower()
    return None


FOOTER_MARKERS = [
    "CONFIDENTIALITY NOTICE", "Confidentiality Notice",
    "This email and any attachments",
    "This message contains confidential",
    "This e-mail and any attachments",
    "DISCLAIMER", "Disclaimer:",
    "Unsubscribe", "View this email in your browser",
    "Privacy Policy",
    "Please consider the environment",
    "THE RITZ-CARLTON CONFIDENTIAL",
    "This communication contains information from",
]


def extract_signature_block(segment: str) -> str:
    lines = segment.splitlines()
    body_start = 0
    for i, ln in enumerate(lines):
        s = ln.strip().lstrip(">").strip()
        if re.match(r"^(From|Sent|To|Cc|Bcc|Subject|Date|Reply-To):\s", s):
            body_start = i + 1
        elif body_start > 0 and not s:
            body_start = i + 1
            break
    body_lines = [ln.lstrip(">").rstrip() for ln in lines[body_start:]]
    clean_body = "\n".join(body_lines).strip()
    if not clean_body:
        return ""
    for marker in FOOTER_MARKERS:
        idx = clean_body.find(marker)
        if idx > 100:
            clean_body = clean_body[:idx].strip()
            break
    non_empty = [ln for ln in clean_body.splitlines() if ln.strip()]
    tail = "\n".join(non_empty[-30:])
    if len(tail) > 2000:
        tail = tail[-2000:]
    return tail


SIGNATURE_PROMPT = """Extract contact info from this email signature block.
The block may contain extra body text — find the signature within it.
Return ONLY valid JSON. Use null for missing fields. No markdown, no preamble.

{
  "first_name": "First name only",
  "last_name": "Last name only",
  "title": "Job title",
  "organization": "Company name",
  "email": "Email address if present",
  "phone": "Direct/office phone if present (any format)",
  "mobile": "Mobile phone if separate",
  "address": "Full street address if present",
  "linkedin_url": "LinkedIn URL if present",
  "is_real_person": true,
  "confidence": 0.0
}

Rules:
- Skip 'Sent from my iPhone', legal disclaimers, marketing footers.
- If no clear person is identifiable, return all nulls and is_real_person=false.
- Set is_real_person=false for: marketing email signatures, automated
  notifications, role-only signatures (e.g. 'Sales Team', 'Front Desk',
  'Notification Portal'), bot-generated content, transactional receipts.
- Set confidence (0.0 to 1.0) reflecting how certain you are this represents a
  real individual professional contact. 1.0 = clean signature with name+title+org.
  0.5 = partial/ambiguous. 0.0 = not a signature.
- Do NOT invent data not visible in the block.

Block:
"""


async def parse_signature(client, signature: str) -> dict:
    if not signature or len(signature) < 20:
        return {}
    prompt = SIGNATURE_PROMPT + signature
    try:
        text = await ai_generate(client, prompt, model="gemini-2.5-flash-lite")
    except Exception as e:
        print(f"   ⚠️  Gemini error: {e}")
        return {}
    if not text:
        return {}
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    try:
        data = json.loads(text)
        return data if isinstance(data, dict) else {}
    except json.JSONDecodeError:
        return {}


def write_csv(contacts: List[dict], mailbox: str) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M")
    safe_mailbox = mailbox.replace("@", "_at_").replace(".", "_")
    path = OUTPUT_DIR / f"contacts_{safe_mailbox}_{timestamp}.csv"
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for c in contacts:
            row = {col: c.get(col) for col in CSV_COLUMNS}
            for bcol in ("has_signature", "in_other_contacts", "in_saved_contacts"):
                row[bcol] = "yes" if row.get(bcol) else "no"
            writer.writerow(row)
    return path


def write_debug_jsonl(records: List[dict], mailbox: str) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M")
    safe_mailbox = mailbox.replace("@", "_at_").replace(".", "_")
    path = OUTPUT_DIR / f"sig_debug_{safe_mailbox}_{timestamp}.jsonl"
    with path.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    return path


# ──────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────


async def main():
    scan_started = datetime.now().isoformat(timespec="seconds")

    print("=" * 80)
    print(f"Phase 2 v4.6 — Hospitality enrichment (BrandRegistry + Procurement Intel)"
          f"{' [DEBUG DUMP ON]' if DEBUG_DUMP_SIGNATURES else ''}")
    print(f"Target mailbox: {TARGET_USER}")
    print(f"Window: last {SCAN_DAYS_BACK} days  |  Cap: {MAX_EMAILS_TO_SCAN}")
    print(f"Confidence threshold: {MIN_CONFIDENCE}  |  Phone region: {PHONE_DEFAULT_REGION}")
    print(f"Output: {OUTPUT_DIR}")
    print(f"BrandRegistry: {len(BrandRegistry.list_all_brands())} brands loaded")
    print(f"Management companies: {len(MANAGEMENT_COMPANY_INTEL)} loaded")
    print("Mode: READ-ONLY. No DB. No Insightly push.")
    print("=" * 80)

    creds = build_credentials()
    gmail = build_gmail_client(creds)
    people = build_people_client(creds)

    other_contacts = dump_other_contacts(people)
    saved_contacts = dump_saved_contacts(people)

    message_ids = list_message_ids(gmail)

    sig_cache: Dict[str, dict] = {}
    gmail_contacts: Dict[str, dict] = {}
    seen_in_headers: Dict[str, dict] = {}

    sig_rejected = {
        "ja_mismatch": 0,
        "email_mismatch": 0,
        "not_real_person": 0,
        "low_confidence": 0,
        "name_mismatch": 0,
    }
    rescued_via_parsed_email = 0
    phone_validation = {"valid": 0, "invalid": 0}
    preproc_totals = {"urls_stripped": 0, "images_stripped": 0,
                      "warnings_stripped": 0, "lines_joined": 0}
    debug_records: List[dict] = []

    print(f"\n🔎 Walking {len(message_ids)} messages for signatures...")

    async with httpx.AsyncClient(timeout=60.0) as client:
        for i, msg_id in enumerate(message_ids, start=1):
            if i % 50 == 0:
                print(f"   ... {i}/{len(message_ids)} "
                      f"(sigs: {len(gmail_contacts)}, headers: {len(seen_in_headers)})")

            msg = fetch_full_message(gmail, msg_id)
            if not msg:
                continue

            headers = {h["name"]: h.get("value", "")
                       for h in (msg.get("payload", {}).get("headers", []) or [])}

            for hdr in ("From", "To", "Cc", "Bcc"):
                val = headers.get(hdr, "")
                display = extract_display_name(val) if hdr == "From" else ""
                for email in extract_emails(val):
                    entry = seen_in_headers.setdefault(email, {
                        "email": email,
                        "display_name": display,
                        "interaction_count": 0,
                    })
                    entry["interaction_count"] += 1
                    if display and not entry["display_name"]:
                        entry["display_name"] = display

            body = extract_plain_text(msg.get("payload", {}))
            if not body:
                continue

            body, preproc_stats = preprocess_body(body)
            for k, v in preproc_stats.items():
                preproc_totals[k] += v

            segments = split_into_segments(body)
            top_sender = next(iter(extract_emails(headers.get("From", ""))), None)
            top_sender_name = extract_display_name(headers.get("From", ""))

            for seg_idx, seg in enumerate(segments):
                if seg_idx == 0:
                    sig_owner = top_sender
                    header_name = top_sender_name
                else:
                    sig_owner = extract_email_in_segment_header(seg)
                    header_name = extract_sender_name_from_segment(seg)

                if not sig_owner or sig_owner == TARGET_USER:
                    continue
                if domain_of(sig_owner) in OWN_DOMAINS:
                    continue

                sig_block = extract_signature_block(seg)
                if not sig_block or len(sig_block) < 30:
                    continue

                sig_hash = hashlib.sha256(sig_block.encode("utf-8")).hexdigest()[:16]
                if sig_hash in sig_cache:
                    parsed = sig_cache[sig_hash]
                else:
                    parsed = await parse_signature(client, sig_block)
                    sig_cache[sig_hash] = parsed

                debug_rec = {
                    "msg_id": msg_id,
                    "seg_idx": seg_idx,
                    "sig_owner": sig_owner,
                    "header_name": header_name,
                    "signature_block": sig_block,
                    "parsed": parsed,
                    "final_status": None,
                }

                if not parsed:
                    debug_rec["final_status"] = "gemini_returned_nothing"
                    if DEBUG_DUMP_SIGNATURES:
                        debug_records.append(debug_rec)
                    continue

                parsed_org = (parsed.get("organization") or "").lower()
                if any(t in parsed_org for t in ("jauniforms", "j.a. uniforms", "ja uniforms")):
                    if domain_of(sig_owner) not in OWN_DOMAINS:
                        sig_rejected["ja_mismatch"] += 1
                        debug_rec["final_status"] = "rejected_ja_mismatch"
                        if DEBUG_DUMP_SIGNATURES:
                            debug_records.append(debug_rec)
                        continue

                parsed_email = (parsed.get("email") or "").lower().strip()
                if parsed_email and parsed_email != sig_owner and domain_of(parsed_email) != domain_of(sig_owner):
                    sig_rejected["email_mismatch"] += 1
                    debug_rec["final_status"] = "rejected_email_mismatch"
                    if DEBUG_DUMP_SIGNATURES:
                        debug_records.append(debug_rec)
                    continue

                if parsed.get("is_real_person") is False:
                    sig_rejected["not_real_person"] += 1
                    debug_rec["final_status"] = "rejected_not_real_person"
                    if DEBUG_DUMP_SIGNATURES:
                        debug_records.append(debug_rec)
                    continue

                conf = parsed.get("confidence")
                if isinstance(conf, (int, float)) and conf < MIN_CONFIDENCE:
                    sig_rejected["low_confidence"] += 1
                    debug_rec["final_status"] = "rejected_low_confidence"
                    if DEBUG_DUMP_SIGNATURES:
                        debug_records.append(debug_rec)
                    continue

                if not signature_name_matches_header(parsed, header_name):
                    rescue_email = parsed_email if parsed_email else None
                    can_rescue = (
                        rescue_email
                        and rescue_email != sig_owner
                        and domain_of(rescue_email) == domain_of(sig_owner)
                        and domain_of(rescue_email) not in OWN_DOMAINS
                    )
                    if can_rescue:
                        sig_owner = rescue_email
                        rescued_via_parsed_email += 1
                        debug_rec["sig_owner"] = sig_owner
                        debug_rec["rescued"] = True
                    else:
                        sig_rejected["name_mismatch"] += 1
                        debug_rec["final_status"] = "rejected_name_mismatch"
                        if DEBUG_DUMP_SIGNATURES:
                            debug_records.append(debug_rec)
                        continue

                for phone_field in ("phone", "mobile"):
                    raw = parsed.get(phone_field)
                    if raw:
                        cleaned = validate_phone(raw)
                        if cleaned:
                            parsed[phone_field] = cleaned
                            phone_validation["valid"] += 1
                        else:
                            parsed[phone_field] = None
                            phone_validation["invalid"] += 1

                existing = gmail_contacts.get(sig_owner)
                new_score = sum(1 for v in parsed.values() if v)
                old_score = sum(1 for v in (existing or {}).get("parsed", {}).values() if v)
                if not existing or new_score > old_score:
                    gmail_contacts[sig_owner] = {"email": sig_owner, "parsed": parsed}

                if debug_rec.get("rescued"):
                    seen_in_headers.setdefault(sig_owner, {
                        "email": sig_owner,
                        "display_name": "",
                        "interaction_count": 0,
                    })
                    seen_in_headers[sig_owner]["interaction_count"] += 1

                debug_rec["final_status"] = "kept"
                debug_rec["parsed"] = parsed
                if DEBUG_DUMP_SIGNATURES:
                    debug_records.append(debug_rec)

    print(f"\n   ✓ Gmail: {len(gmail_contacts)} contacts with parsed signatures")
    print(f"   ✓ Gmail: {len(seen_in_headers)} unique emails across all headers")
    print(f"   ✓ {len(sig_cache)} unique signatures cached")

    print("\n   Preprocessing impact (all messages combined):")
    print(f"     URL wrappers stripped:    {preproc_totals['urls_stripped']}")
    print(f"     Image refs stripped:      {preproc_totals['images_stripped']}")
    print(f"     Warning lines stripped:   {preproc_totals['warnings_stripped']}")
    print(f"     Soft-wrap lines joined:   {preproc_totals['lines_joined']}")

    print("\n   Signature-level rejections:")
    print(f"     JA org mismatch:        {sig_rejected['ja_mismatch']}")
    print(f"     parsed-email mismatch:  {sig_rejected['email_mismatch']}")
    print(f"     not real person:        {sig_rejected['not_real_person']}")
    print(f"     confidence < {MIN_CONFIDENCE}:       {sig_rejected['low_confidence']}")
    print(f"     name not in header:     {sig_rejected['name_mismatch']}")
    print(f"   RESCUED via parsed email: {rescued_via_parsed_email}")
    print(f"   Phone validation: {phone_validation['valid']} valid, {phone_validation['invalid']} rejected")

    # ── MERGE ─────────────────────────────────────────────────────────
    print("\n🔗 Merging sources (interaction-only universe)...")
    all_emails = set(seen_in_headers.keys()) | set(saved_contacts.keys()) | set(gmail_contacts.keys())

    final_contacts = []
    rejected = {"own_company": 0, "noreply": 0, "mass_mail": 0,
                "saas": 0, "self": 0, "malformed": 0, "personal_no_org": 0}
    org_inferred_count = 0

    for email in all_emails:
        ok, reason = passes_hard_filters(email)
        if not ok:
            rejected[reason] = rejected.get(reason, 0) + 1
            continue

        sig = (gmail_contacts.get(email) or {}).get("parsed") or {}
        saved = saved_contacts.get(email) or {}
        other = other_contacts.get(email) or {}
        header = seen_in_headers.get(email) or {}

        def pick(*pairs):
            for d, k in pairs:
                v = d.get(k)
                if v:
                    return v
            return None

        org = pick((sig, "organization"), (saved, "organization"))
        org_source = "signature" if sig.get("organization") else \
                     "saved_contacts" if saved.get("organization") else None
        if not org:
            inferred = infer_organization_from_domain(domain_of(email))
            if inferred:
                org = inferred
                org_source = "domain_inferred"
                org_inferred_count += 1

        merged = {
            "email": email,
            "first_name": pick((sig, "first_name"), (saved, "first_name"), (other, "first_name")),
            "last_name": pick((sig, "last_name"), (saved, "last_name"), (other, "last_name")),
            "title": pick((sig, "title"), (saved, "title")),
            "organization": org,
            "org_source": org_source,
            "phone": pick((sig, "phone"), (sig, "mobile"), (saved, "phone")) or validate_phone(other.get("phone")),
            "address": pick((sig, "address"), (saved, "address")),
            "linkedin_url": sig.get("linkedin_url"),
            "display_name": header.get("display_name") or saved.get("display_name") or other.get("display_name"),
            "interaction_count": header.get("interaction_count", 0),
            "has_signature": bool(sig),
            "in_other_contacts": email in other_contacts,
            "in_saved_contacts": email in saved_contacts,
            "confidence": sig.get("confidence"),
            "mailbox": TARGET_USER,
            "scanned_at": scan_started,
        }

        if not soft_keep_personal(email, merged):
            rejected["personal_no_org"] += 1
            continue

        # v4.6: ENRICH with brand intel + procurement priority
        merged = enrich_contact(merged)

        final_contacts.append(merged)

    print(f"   Rejected — own company:          {rejected.get('own_company', 0)}")
    print(f"   Rejected — no-reply pattern:     {rejected.get('noreply', 0)}")
    print(f"   Rejected — mass-mail pattern:    {rejected.get('mass_mail', 0)}")
    print(f"   Rejected — SaaS domain:          {rejected.get('saas', 0)}")
    print(f"   Rejected — self:                 {rejected.get('self', 0)}")
    print(f"   Rejected — personal w/o org:     {rejected.get('personal_no_org', 0)}")
    print(f"   Organizations inferred from domain: {org_inferred_count}")
    print(f"   ✅ KEPT:                          {len(final_contacts)}")

    # ── HOSPITALITY ENRICHMENT STATS ──────────────────────────────────
    n = max(len(final_contacts), 1)
    brands_recognized = sum(1 for c in final_contacts if c.get("parent_company"))
    p1 = sum(1 for c in final_contacts if c.get("procurement_priority") == "P1")
    p2 = sum(1 for c in final_contacts if c.get("procurement_priority") == "P2")
    p3 = sum(1 for c in final_contacts if c.get("procurement_priority") == "P3")
    p4 = sum(1 for c in final_contacts if c.get("procurement_priority") == "P4")
    p_unknown = sum(1 for c in final_contacts if c.get("procurement_priority") == "P_unknown")
    high_opp = sum(1 for c in final_contacts if c.get("opportunity_level") == "high")
    med_opp = sum(1 for c in final_contacts if c.get("opportunity_level") == "medium")
    low_opp = sum(1 for c in final_contacts if c.get("opportunity_level") == "low")
    avendra = sum(1 for c in final_contacts if c.get("gpo") == "Avendra")
    mgmt_co = sum(1 for c in final_contacts if c.get("management_company"))

    print("\n" + "=" * 80)
    print("🏨 HOSPITALITY ENRICHMENT:")
    print(f"   Brands recognized via BrandRegistry: {brands_recognized}/{n} ({100*brands_recognized/n:.0f}%)")
    print("   Procurement priority breakdown:")
    print(f"     P1 (direct buyers):         {p1:4} ({100*p1/n:.0f}%)")
    print(f"     P2 (operational/GMs):       {p2:4} ({100*p2/n:.0f}%)")
    print(f"     P3 (HR/F&B/secondary):      {p3:4} ({100*p3/n:.0f}%)")
    print(f"     P4 (non-buyers):            {p4:4} ({100*p4/n:.0f}%)")
    print(f"     P_unknown:                  {p_unknown:4} ({100*p_unknown/n:.0f}%)")
    print("   Opportunity level:")
    print(f"     High (independents/collections): {high_opp:4}")
    print(f"     Medium:                          {med_opp:4}")
    print(f"     Low (Disney/Sandals/all-inc):    {low_opp:4}")
    print(f"   Avendra-constrained:                {avendra:4}  (JA needs Avendra approval)")
    print(f"   Known management companies:         {mgmt_co:4}")

    # ── SORT: priority first, then opportunity score ──────────────────
    priority_order = {"P1": 1, "P2": 2, "P3": 3, "P_unknown": 4, "P4": 5}
    final_contacts.sort(
        key=lambda c: (
            priority_order.get(c.get("procurement_priority", "P_unknown"), 4),
            -(c.get("opportunity_score") or 0),
        )
    )

    csv_path = write_csv(final_contacts, TARGET_USER)
    debug_path = None
    if DEBUG_DUMP_SIGNATURES and debug_records:
        debug_path = write_debug_jsonl(debug_records, TARGET_USER)

    with_sig = sum(1 for c in final_contacts if c["has_signature"])
    have_title = sum(1 for c in final_contacts if c.get("title"))
    have_org = sum(1 for c in final_contacts if c.get("organization"))
    have_phone = sum(1 for c in final_contacts if c.get("phone"))
    uniq_orgs = set()
    for c in final_contacts:
        o = (c.get("organization") or "").strip().lower()
        if o:
            uniq_orgs.add(o)

    print("\n" + "=" * 80)
    print(f"Field coverage ({len(final_contacts)} contacts):")
    print(f"   Signature parsed:    {with_sig:4} ({100*with_sig/n:.0f}%)")
    print(f"   Title:               {have_title:4} ({100*have_title/n:.0f}%)")
    print(f"   Organization:        {have_org:4} ({100*have_org/n:.0f}%)")
    print(f"   Phone (validated):   {have_phone:4} ({100*have_phone/n:.0f}%)")
    print(f"   Unique organizations: {len(uniq_orgs)}")
    print("=" * 80)

    print("\nTop 15 (sorted by priority + opportunity_score):")
    print(f"{'Pri':<5} {'Email':<38} {'Name':<22} {'Title':<28} {'Brand/Org':<25} {'Tier':<10} {'Opp':<5}")
    print("-" * 140)
    for c in final_contacts[:15]:
        name = " ".join(filter(None, [c.get("first_name"), c.get("last_name")])) or "—"
        pri = c.get("procurement_priority", "—")
        title = (c.get("title") or "—")[:28]
        org = (c.get("organization") or "—")[:25]
        tier = (c.get("brand_tier") or "—").replace("tier", "T")[:10]
        opp = c.get("opportunity_level") or "—"
        print(f"{pri:<5} {c['email'][:38]:<38} {name[:22]:<22} {title:<28} {org:<25} {tier:<10} {opp:<5}")

    print("\n" + "=" * 80)
    print(f"✅ Exported {len(final_contacts)} enriched contacts to:")
    print(f"   {csv_path}")
    if debug_path:
        print(f"\n🔍 Debug dump → {debug_path}")
    print("=" * 80)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⏹️  Interrupted")
        sys.exit(130)
    except Exception as e:
        print(f"\n❌ ERROR: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
