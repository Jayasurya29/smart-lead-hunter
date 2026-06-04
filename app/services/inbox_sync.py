"""app/services/inbox_sync.py

Refactored v4.7 Gmail contact-extraction pipeline as a reusable async module.

Core entry point: sync_mailbox(mailbox_email, session, ...)

Pipeline per mailbox:
  1. Load Gmail History API cursor from mailbox_sync_state (incremental sync)
  2. Fetch message IDs (full backfill on first run, delta on subsequent runs)
  3. For each message: extract body → preprocess → split segments →
     isolate signature block → Gemini Flash-Lite parse → validate
  4. Merge with Google Contacts (Other + Saved) for the mailbox
  5. Enrich with BrandRegistry + procurement priority
  6. Upsert to contacts table via contact_dedup.bulk_upsert_contacts
  7. Update mailbox_sync_state cursor + run stats

Changes in v4.7 (2026-05-18):
  - Parallel Gemini sig parsing with asyncio.Semaphore(20) — was sequential
    (~1/sec). Now processes all unique sig blocks concurrently in one batch.
  - _normalize_saved_contact_orgs(): Gemini splits "Company - Person Name"
    format org fields from Google Saved Contacts at ingest time, so we never
    have to run a cleanup script again (fixes Alex Arencibia's contact format).
  - _DOMAIN_ORG_OVERRIDES: domain-stem → canonical org name for common clients
    (Towne Park, SP Plus, Laz Parking, Ocean Reef Club, etc.) so _infer_org()
    returns clean names instead of "Townepark" or "Spplus".

Credentials: credentials/slh-contact-sync.json
Scopes: gmail.readonly + contacts.readonly + contacts.other.readonly
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import httpx
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.services.ai_client import ai_generate
from app.config.brand_registry import BrandRegistry, BrandInfo
from app.config.procurement_intelligence import (
    MANAGEMENT_COMPANY_INTEL,  # noqa: F401  (imported for future use)
    get_management_company_intel,
    match_gateway_contact,
    gateway_for_brand,
)
from app.services.contact_dedup import bulk_upsert_contacts

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────────────────

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
CREDENTIALS_PATH = PROJECT_ROOT / "credentials" / "slh-contact-sync.json"

GMAIL_SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/contacts.readonly",
    "https://www.googleapis.com/auth/contacts.other.readonly",
]

OWN_DOMAINS: set[str] = {
    "jauniforms.com",
    "jauniforms.org",
    "ja-uniforms.com",
    "ja-uniforms.org",
}

JA_TEAM_NAMES: set[str] = {
    "sai",
    "jayasurya",
    "kandregula",
    "menchu",
    "ugarcia",
}

NOREPLY_PATTERNS = [
    "noreply",
    "no-reply",
    "no.reply",
    "donotreply",
    "do-not-reply",
    "mailer-daemon",
    "postmaster",
    "bounces",
    "auto-reply",
    "auto-confirm",
    "auto-receipt",
    "automated",
    "notifications",
    "notification",
    "alerts",
    "alert",
    "announcements",
    "announcement",
    "pressrelease",
    "guestsurveys",
    "sweeps",
    "voicemail",
    "system",
    "bss_",
    "quickbooks",
    "fedex",
    "carfax",
    "progressivecommercial",
    "growhello",
    "account-services",
    "trending",
    "insider",
    "open-source-ceo",
    "webmaster",
]

MASS_MAIL_PATTERNS = ["newsletter", "unsubscribe", "digest", "dmarc", "mailer-daemon"]

SAAS_DOMAINS: set[str] = {
    # ESP / blast platforms (2026-06-04 Phase-3 additions)
    "ccsend.com",
    "kajabimail.net",
    "mailchimp.com",
    "constantcontact.com",
    "campaignmonitor.com",
    "sendgrid.net",
    "mandrillapp.com",
    "sparkpostmail.com",
    "hs-send.com",
    "hubspotemail.net",
    "amazonses.com",
    "ses.amazonaws.com",
    "facebookmail.com",
    "birchstreet.net",
    "sciquest.com",
    "jaggaer.com",
    "jaggaer.net",
    "paymerang.com",
    "amerantbank.com",
    "quore.com",
    "phonesuit.com",
    "supportone.us",
    "multivariants.com",
    "m3as.com",
    "grispi.com",
    "shopify.com",
    "hubspot.com",
    "feedback-marriott.com",
    "message.fedex.com",
    "email.netsuite.com",
    "notification.intuit.com",
    "mail.beehiiv.com",
    "unbounce.com",
    "mail.clickup.com",
    "apollo.io",
    "mail.apollo.io",
    "hunter.io",
    "mail.hunter.io",
    "rocketreach.co",
    "findymail.com",
    "mail.findymail.com",
}

PERSONAL_DOMAINS: set[str] = {
    "gmail.com",
    "yahoo.com",
    "hotmail.com",
    "outlook.com",
    "aol.com",
    "icloud.com",
    "me.com",
    "mac.com",
    "msn.com",
    "live.com",
    "comcast.net",
    "verizon.net",
    "att.net",
    "protonmail.com",
    "proton.me",
    "fastmail.com",
    "mail.com",
    "gmx.com",
}

GENERIC_DOMAINS_NO_ORG: set[str] = {
    "google.com",
    "microsoft.com",
    "linkedin.com",
    "github.com",
    "amazon.com",
    "apple.com",
    "zendesk.com",
    "zoho.com",
    "salesforce.com",
}

# Role / non-person local parts — mass or transactional addresses, never an
# individual buyer. Matched as whole tokens (split on . _ - +) so we never nick
# a real name like "john.newsome" ("news" is not a token there). Hotel-relevant
# role words (sales, reservations, events, catering) are deliberately EXCLUDED.
ROLE_LOCALPARTS: set[str] = {
    "info",
    "news",
    "newsletter",
    "orders",
    "order",
    "marketing",
    "press",
    "media",
    "hello",
    "team",
    "members",
    "member",
    "billing",
    "invoice",
    "invoices",
    "receipts",
    "statements",
    "support",
    "help",
    "care",
    "service",
    "customerservice",
    "updates",
    "update",
    "digest",
    "social",
    "community",
    "contact",
    "subscribe",
    "unsubscribe",
    "feedback",
    "survey",
    "surveys",
    "marketingteam",
    "newsroom",
    "list",
    "lists",
    "dmarc",
    "dmarcreport",
    "abuse",
    "postmaster",
    "daemon",
    "notifications",
    "notification",
    "alerts",
    "alert",
    "reports",
    "report",
}

# Marketing / bulk subdomain prefixes — when the leftmost domain label is one of
# these, the address is a blast stream (e.zoro.com, e.weareprogressives.org),
# not a person. High-precision list; tune as needed.
BULK_SUBDOMAIN_PREFIXES: tuple = (
    "e",
    "em",
    "email",
    "t",
    "go",
    "click",
    "send",
    "mailer",
    "news",
    "newsletter",
    "marketing",
    "editorial",
    "campaign",
    "campaigns",
    "promo",
    "promotions",
    "engage",
    "members",
    "comms",
    "messages",
    "message",
    "eg",
    "mg",
    "broadcast",
    "blast",
    "reply",
    "notify",
    "notifications",
    "alerts",
    "updates",
    "bounce",
    "mta",
)

# Consumer / e-commerce / news / job-board / social domains — never hotel
# buyers. (icloud/amazon/linkedin already live in the personal/generic sets.)
CONSUMER_DOMAINS: set[str] = {
    # Apple relay
    "privaterelay.appleid.com",
    "appleid.com",
    # e-commerce / delivery / payments
    "instacart.com",
    "netflix.com",
    "zoro.com",
    "ebay.com",
    "etsy.com",
    "walmart.com",
    "target.com",
    "bestbuy.com",
    "doordash.com",
    "ubereats.com",
    "uber.com",
    "lyft.com",
    "paypal.com",
    "venmo.com",
    "chewy.com",
    "wayfair.com",
    "homedepot.com",
    "lowes.com",
    "costco.com",
    # job boards / recruiting
    "indeedemail.com",
    "indeed.com",
    "glassdoor.com",
    "ziprecruiter.com",
    "monster.com",
    # news / media / newsletter platforms
    "theguardian.com",
    "nytimes.com",
    "wsj.com",
    "washingtonpost.com",
    "cnn.com",
    "bloomberg.com",
    "reuters.com",
    "substack.com",
    "beehiiv.com",
    "medium.com",
    "alphasignal.ai",
    "morningbrew.com",
    # streaming / social / consumer apps
    "spotify.com",
    "facebook.com",
    "instagram.com",
    "twitter.com",
    "x.com",
    "tiktok.com",
    "youtube.com",
    "pinterest.com",
    "reddit.com",
}

# ── Domain-stem → canonical org name ─────────────────────────────────────
# Used by _infer_org() before BrandRegistry lookup so common clients and
# hotel brands with multi-word names get clean display names rather than
# naive capitalization ("Townepark", "Fourseasons", "Lazparking").
# ADD new domains here whenever _infer_org() produces ugly names.
_DOMAIN_ORG_OVERRIDES: dict[str, str] = {
    # Parking / valet (JA clients)
    "townepark": "Towne Park",
    "spplus": "SP Plus",
    "lazparking": "Laz Parking",
    "ameripark": "Ameripark",
    "parkingmgt": "PMC Parking Management",
    "impark": "Impark",
    "vpne": "VPNE Parking Solutions",
    "premierparking": "Premier Parking",
    "aaaparking": "AAA Parking",
    "denisonparking": "Denison Parking",
    "park1": "Park One",
    # Hotels / resorts not in BrandRegistry or with awkward stems
    "grandbeachhotel": "Grand Beach Hotel",
    "biltmorehotel": "Biltmore Hotel",
    "remingtonhotels": "Remington Hotels",
    "rosenhotels": "Rosen Hotels & Resorts",
    "rosencentre": "Rosen Centre Hotel",
    "rosenshinglecreek": "Rosen Shingle Creek Resort",
    "mrchotels": "MRC Hotels",
    "oceanreef": "Ocean Reef Club",
    "oasismarinas": "OASIS Marinas",
    "hawkscay": "Hawks Cay Resort",
    "cheeca": "Cheeca Lodge",
    "mutinyhotel": "Mutiny Hotel",
    "nationalhotel": "National Hotel",
    "thebetsyhotel": "The Betsy Hotel",
    "thesetaihotel": "The Setai Hotel",
    "naplesbayresort": "Naples Bay Resort",
    "tidesinn": "Tides Inn",
    "boceanfortlauderdale": "B Ocean Fort Lauderdale",
    "grandgbd": "Grand Beach Hotel Surfside",
    "elevationhotel": "Elevation Hotel",
    "mainsailhotels": "Mainsail Hotels",
    "pyramidglobal": "Pyramid Global Hospitality",
    "noblehousehotels": "Noble House Hotels",
    "gatehospitality": "Gate Hospitality",
    "samarhospitality": "Samar Hospitality",
    "mrhospitality": "MR Hospitality",
    "apchospitality": "APC Hospitality",
    "silvertoncasino": "Silverton Casino",
    "cbayresort": "Curtain Bluff Resort",
    "peabodymemphis": "Peabody Memphis",
    "thestellahotel": "The Stella Hotel",
    "thesagamore": "The Sagamore",
    "thebodyholiday": "The Body Holiday",
    "thebenwestpalm": "The Ben West Palm",
    "thened": "The Ned",
    "thenines": "The Nines Hotel",
    "thesomerset": "The Somerset",
    "thestovallhouse": "The Stovall House",
    "thestrandtci": "The Strand TCI",
    "theshoreclubtc": "The Shore Club Turks & Caicos",
    "theiveyshotel": "The Ivey's Hotel",
    "southbeachgroup": "South Beach Group",
    "otesaga": "Otesaga Resort Hotel",
    "halfmoon": "Half Moon Resort",
    "hammockbeach": "Hammock Beach Resort",
    "pvresorts": "PV Resorts",
    "resortjh": "Resort at Jackson Hole",
    "lacanteraresort": "La Cantera Resort",
    "seagatedelray": "Seagate Hotel Delray",
    "oceansedgekeywest": "Ocean's Edge Key West",
    "bungalowskeylargo": "Bungalows Key Largo",
    "sailfishpoint": "Sailfish Point",
    "fisherislandclub": "Fisher Island Club",
    "riversiidhotel": "Riverside Hotel",
    "wgresorts": "Waterfall Glen Resorts",
    "rbpropertiesinc": "RB Properties",
    # Healthcare / other uniform buyers
    "wellpath": "Wellpath",
    "wvumedicine": "WVU Medicine",
    "med": None,  # med.miami.edu → University of Miami Medical — skip
    # Marinas
    "westracbelize": "Westrac Belize",
}

FOOTER_MARKERS = [
    "CONFIDENTIALITY NOTICE",
    "Confidentiality Notice",
    "This email and any attachments",
    "This message contains confidential",
    "DISCLAIMER",
    "Disclaimer:",
    "Unsubscribe",
    "View this email in your browser",
    "Privacy Policy",
    "Please consider the environment",
]

MIN_CONFIDENCE = 0.6
SCAN_DAYS_BACK_INITIAL = 2
MAX_EMAILS_PER_RUN = 5000
PHONE_DEFAULT_REGION = "US"

# Parallel Gemini limits
_SIG_PARSE_SEMAPHORE = 20  # concurrent sig-parse calls
_ORG_SPLIT_SEMAPHORE = 10  # concurrent org-split calls

SIGNATURE_PROMPT = """Extract contact info from this email signature block.
Return ONLY valid JSON. Use null for missing fields. No markdown, no preamble.

{
  "first_name": null,
  "last_name": null,
  "title": null,
  "organization": null,
  "email": null,
  "phone": null,
  "mobile": null,
  "address": null,
  "linkedin_url": null,
  "is_real_person": true,
  "confidence": 0.0
}

Rules:
- Skip 'Sent from my iPhone', legal disclaimers, marketing footers.
- is_real_person=false for: marketing sigs, automated notifications,
  role-only sigs ('Sales Team', 'Front Desk'), bots, transactional receipts.
- confidence 0.0–1.0: 1.0 = clean name+title+org, 0.5 = partial, 0.0 = not a sig.
- Do NOT invent data not visible in the block.
- address: copy the street/city/state EXACTLY as printed in the block,
  partial is fine (city only is OK). NEVER infer a city or state from the
  company, hotel, or district name — e.g. for a "River Market Hotel"
  signature with no printed address, address MUST be null (do not guess
  Little Rock or Kansas City). A wrong guessed address is far worse than null.

Block:
"""

# NEW 2026-05-18: Prompt for splitting "Company - Person" saved contact orgs.
ORG_SPLIT_PROMPT = """\
You are parsing Google Contact entries where the organization field contains
"Company Name - Person Name" or similar mixed formats saved manually.

For each entry classify:
- org: the real organization/company name (string or null if personal only)
- person_name: the real person's full name (string or null if org only)
- is_personal: true if this is a personal/hobby contact (not a business contact)

Return ONLY a JSON array (no markdown, no preamble) with one object per entry
IN THE SAME ORDER. Each object: {"id":<int>,"org":<str|null>,"person_name":<str|null>,"is_personal":<bool>}

Rules:
- "Towne Park - Cindy Wetzel" → org="Towne Park", person_name="Cindy Wetzel"
- "MANDARIN ORIENTAL - MIAMI" → both sides are company info, org="Mandarin Oriental Miami", person_name=null
- "Tennis - Carlos Cuervo" → org=null, person_name="Carlos Cuervo", is_personal=true
- "Leo - Matias father" → org=null, person_name=null, is_personal=true
- ALL CAPS on both sides → merge into single org, no person split
- "Mr. C - Carlos Robledo" → org="Mr. C", person_name="Carlos Robledo" (Mr. C is a hotel brand)
- If the right side looks like a city/location not a person → keep as part of org
- When unsure → keep original as org, person_name=null

Entries:
"""

# ──────────────────────────────────────────────────────────────────────────
# Regex precompiles
# ──────────────────────────────────────────────────────────────────────────

_EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")
_HTML_TAG_RE = re.compile(r"<[^>]+>")
_ANGLE_URL_RE = re.compile(r"<https?://[^>\s]*>", re.IGNORECASE)
_IMAGE_MARKER_RE = re.compile(r"\[image:[^\]]*\]", re.IGNORECASE)
_CID_RE = re.compile(r"\[cid:[^\]]*\]", re.IGNORECASE)
_WARNING_RE = [
    re.compile(r"^\s*CAUTION:.*$", re.MULTILINE | re.IGNORECASE),
    re.compile(r"^.*This message is from an EXTERNAL SENDER.*$", re.MULTILINE | re.IGNORECASE),
    re.compile(r"^\s*\[EXTERNAL\].*$", re.MULTILINE | re.IGNORECASE),
    re.compile(r"^\s*\*\*\* EXTERNAL EMAIL.*$", re.MULTILINE | re.IGNORECASE),
]
_SECTION_BOUNDARIES = [
    re.compile(r"^[\s>]*From:\s.+$", re.MULTILINE),
    re.compile(r"^[\s>]*On\s.{5,200}wrote:\s*$", re.MULTILINE),
    re.compile(r"^[\s>]*-+\s*Forwarded message\s*-+", re.MULTILINE | re.IGNORECASE),
    re.compile(r"^[\s>]*-+\s*Original Message\s*-+", re.MULTILINE | re.IGNORECASE),
    re.compile(r"^[\s>]*Begin forwarded message:", re.MULTILINE | re.IGNORECASE),
    re.compile(r"^[\s>]*_{10,}\s*$", re.MULTILINE),
    re.compile(r"^[\s>]*-{10,}\s*$", re.MULTILINE),
]

# ──────────────────────────────────────────────────────────────────────────
# Google API clients
# ──────────────────────────────────────────────────────────────────────────


def _build_creds(mailbox: str) -> service_account.Credentials:
    if not CREDENTIALS_PATH.exists():
        raise FileNotFoundError(f"Gmail credentials not found: {CREDENTIALS_PATH}")
    creds = service_account.Credentials.from_service_account_file(
        str(CREDENTIALS_PATH), scopes=GMAIL_SCOPES
    )
    return creds.with_subject(mailbox)


def _gmail(mailbox: str):
    return build("gmail", "v1", credentials=_build_creds(mailbox), cache_discovery=False)


def _people(mailbox: str):
    return build("people", "v1", credentials=_build_creds(mailbox), cache_discovery=False)


# ──────────────────────────────────────────────────────────────────────────
# Body helpers
# ──────────────────────────────────────────────────────────────────────────


def _b64(data: str) -> str:
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
    text_out = _HTML_TAG_RE.sub("", html)
    text_out = (
        text_out.replace("&nbsp;", " ")
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", '"')
        .replace("&#39;", "'")
    )
    text_out = re.sub(r"\n{3,}", "\n\n", text_out)
    return re.sub(r"[ \t]+", " ", text_out)


def _extract_plain(payload: dict) -> str:
    mime = payload.get("mimeType", "")
    body = payload.get("body", {})
    if mime == "text/plain" and body.get("data"):
        return _b64(body["data"])
    if mime.startswith("multipart/"):
        plain = html = ""
        for part in payload.get("parts") or []:
            t = _extract_plain(part)
            if part.get("mimeType") == "text/plain" and not plain:
                plain = t
            elif part.get("mimeType") == "text/html" and not html:
                html = t
            elif part.get("mimeType", "").startswith("multipart/") and not plain:
                plain = t
        return plain or _strip_html(html)
    if mime == "text/html" and body.get("data"):
        return _strip_html(_b64(body["data"]))
    return ""


def _preprocess(body: str) -> str:
    if not body:
        return body
    body = _ANGLE_URL_RE.sub("", body)
    body = _IMAGE_MARKER_RE.sub("", body)
    body = _CID_RE.sub("", body)
    for pat in _WARNING_RE:
        body = pat.sub("", body)
    return re.sub(r"\n{3,}", "\n\n", body)


def _split_segments(body: str) -> list[str]:
    if not body:
        return []
    positions = [0]
    for pat in _SECTION_BOUNDARIES:
        for m in pat.finditer(body):
            positions.append(m.start())
    positions.append(len(body))
    positions = sorted(set(positions))
    segments = []
    for i in range(len(positions) - 1):
        seg = body[positions[i] : positions[i + 1]].strip()
        if len(seg) > 50:
            segments.append(seg)
    return segments or ([body.strip()] if body.strip() else [])


def _extract_sig_block(segment: str) -> str:
    lines = segment.splitlines()
    body_lines = [ln.lstrip(">").rstrip() for ln in lines]
    clean = "\n".join(body_lines).strip()
    if not clean:
        return ""
    for marker in FOOTER_MARKERS:
        idx = clean.find(marker)
        if idx > 100:
            clean = clean[:idx].strip()
            break
    non_empty = [ln for ln in clean.splitlines() if ln.strip()]
    tail = "\n".join(non_empty[-30:])
    return tail[-2000:] if len(tail) > 2000 else tail


def _extract_emails(s: str) -> list[str]:
    return [m.group(0).lower() for m in _EMAIL_RE.finditer(s)] if s else []


def _domain(email: str) -> str:
    return email.split("@")[-1].lower() if "@" in email else ""


def _display_name(header_val: str) -> str:
    if not header_val:
        return ""
    m = re.match(r'^\s*"?([^"<]+?)"?\s*<', header_val)
    return m.group(1).strip() if m else ""


def _seg_email(segment: str) -> Optional[str]:
    head = segment[:1000]
    m = re.search(
        r"From:\s*(?:[^<\n]{0,100}<)?([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})",
        head,
    )
    return m.group(1).lower() if m else None


def _seg_name(segment: str) -> str:
    head = segment[:1000]
    m = re.search(r"On\s+.{5,150}?,\s*([A-Z][^<\n]{1,80}?)(?=\s*<|\s+wrote:)", head)
    if m:
        name = m.group(1).strip().strip('"').strip(",")
        if name and len(name) > 1:
            return name
    m = re.search(r"^[\s>]*From:\s+([^<\n]{2,80}?)(?:\s*<|\s*$)", head, re.MULTILINE)
    if m:
        name = m.group(1).strip().strip('"')
        if name and "@" not in name and len(name) > 1:
            return name
    return ""


def _normalize_header_name(name: str) -> str:
    """'Doe, Jane' (Outlook header order) → 'Jane Doe'."""
    n = (name or "").strip().strip('"').strip()
    if "," in n and "@" not in n:
        last, _, first = n.partition(",")
        first, last = first.strip(), last.strip()
        if first and last:
            return f"{first} {last}"
    return n


_HDR_PAIR_RE = re.compile(
    r'"?([^"<>\n;@]{2,60}?)"?\s*<([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})>'
)


def _seg_participants(segment: str) -> list[tuple[str, str]]:
    """(name, email) pairs from embedded To:/Cc: lines in a forwarded/quoted
    segment head (2026-06-04). Forwarded hotel threads name people — the GM
    cc'd on a quote thread a colleague forwarded — who never emailed us
    directly and have no signature in the thread. Without this they were
    invisible to the harvest."""
    head = segment[:1500]
    out: list[tuple[str, str]] = []
    seen: set[str] = set()
    for m in re.finditer(r"^[\s>]*(?:To|Cc|CC):\s*(.+)$", head, re.MULTILINE):
        line = m.group(1)[:500]
        for pm in _HDR_PAIR_RE.finditer(line):
            nm = _normalize_header_name(pm.group(1).strip(' ,"'))
            em = pm.group(2).lower()
            if em not in seen:
                seen.add(em)
                out.append((nm, em))
        for em in _extract_emails(line):
            if em not in seen:
                seen.add(em)
                out.append(("", em))
    return out[:10]


def _name_matches_header(parsed: dict, header_name: str) -> bool:
    fn = (parsed.get("first_name") or "").lower().strip()
    ln = (parsed.get("last_name") or "").lower().strip()
    if not fn and not ln:
        return True
    # FIX 2026-05-18: empty header_name → False (forces domain-match rescue path)
    if not header_name:
        return False
    hn = header_name.lower()
    return (fn and len(fn) > 1 and fn in hn) or (ln and len(ln) > 1 and ln in hn)


def _is_ja_team_leak(parsed: dict, sig_owner: str) -> bool:
    fn = (parsed.get("first_name") or "").lower().strip()
    ln = (parsed.get("last_name") or "").lower().strip()
    if not fn and not ln:
        return False
    if _domain(sig_owner) in OWN_DOMAINS:
        return False
    if fn and fn in JA_TEAM_NAMES:
        return True
    if ln and ln in JA_TEAM_NAMES:
        return True
    return False


def _is_role_localpart(local: str) -> bool:
    """True if the local part is a role/mass address (info@, news@, orders@…).
    Token-based so real names containing a role substring aren't nicked."""
    tokens = [t for t in re.split(r"[._+\-]", local) if t]
    return any(t in ROLE_LOCALPARTS for t in tokens)


# ESP machine labels: short alpha prefix + digits, optional hyphen/suffix —
# em5875, em-681158, mta01, k1, shared1, mailera3u (2026-06-04, widened after
# the Phase-3 report showed the narrow form missing real blast senders).
_BULK_NUMBERED_RE = re.compile(r"^[a-z]{1,6}-?\d+[a-z0-9]*$")


def _is_bulk_subdomain(domain: str) -> bool:
    """True if the leftmost label is a marketing/blast subdomain — exact
    prefixes (e.x.com, comms.x.com) or numbered ESP labels (em5875.x.com,
    mta01.x.com), which the exact list missed (2026-06-04)."""
    parts = domain.split(".")
    if len(parts) < 3:
        return False
    head = parts[0]
    return head in BULK_SUBDOMAIN_PREFIXES or bool(_BULK_NUMBERED_RE.match(head))


def _passes_hard_filters(email: str, own_mailbox: str) -> tuple[bool, str]:
    if "@" not in email:
        return False, "malformed"
    if email == own_mailbox:
        return False, "self"
    d = _domain(email)
    if d in OWN_DOMAINS:
        return False, "own_company"
    local = email.split("@")[0].lower()
    # Echo address (2026-06-04): localpart IS a domain — vendor ESPs encode
    # the recipient domain as the localpart (jauniforms.com@em5486.sanmar.com).
    # Never a person, regardless of the sending domain.
    if local in OWN_DOMAINS or local.endswith((".com", ".net", ".org", ".io")):
        return False, "echo_address"
    # Cold-outreach spam farms rotate throwaway TLDs — no hotel or operator
    # has ever mailed JA from .info/.help/.click (Phase-3 evidence:
    # composely*.info ×4, tcpr*.info ×3, *.help ×4...).
    if d.endswith((".info", ".help", ".click")):
        return False, "spam_tld"
    if any(p in local for p in NOREPLY_PATTERNS):
        return False, "noreply"
    if any(p in local for p in MASS_MAIL_PATTERNS):
        return False, "mass_mail"
    if _is_role_localpart(local):
        return False, "role_address"
    if d in SAAS_DOMAINS or any(d.endswith("." + s) for s in SAAS_DOMAINS):
        return False, "saas"
    if d in CONSUMER_DOMAINS or any(d.endswith("." + s) for s in CONSUMER_DOMAINS):
        return False, "consumer"
    if _is_bulk_subdomain(d):
        return False, "bulk_subdomain"
    return True, "ok"


def _infer_org(domain: str) -> Optional[str]:
    if not domain or domain in PERSONAL_DOMAINS | OWN_DOMAINS | GENERIC_DOMAINS_NO_ORG:
        return None
    for prefix in ("mail.", "email.", "e.", "news.", "info.", "m.", "go.", "support."):
        if domain.startswith(prefix):
            domain = domain[len(prefix) :]
            break
    parts = domain.split(".")
    if len(parts) < 2:
        return None
    if len(parts) == 2:
        stem = ".".join(parts[:-1])
    elif (
        len(parts) >= 3 and parts[-2] in {"co", "com", "org", "net", "gov"} and len(parts[-1]) == 2
    ):
        stem = ".".join(parts[:-2])
    else:
        stem = ".".join(parts[:-1])
    if "." in stem:
        stem = stem.split(".")[-1]
    if not stem or len(stem) < 2:
        return None

    # NEW 2026-05-18: Check _DOMAIN_ORG_OVERRIDES first — covers JA's clients
    # (Towne Park, SP Plus, Ocean Reef, etc.) and hotels with multi-word names.
    stem_lower = stem.lower()
    if stem_lower in _DOMAIN_ORG_OVERRIDES:
        result = _DOMAIN_ORG_OVERRIDES[stem_lower]
        return result  # None means "skip this domain" (e.g. med.miami.edu)

    # Try BrandRegistry (covers hotel brands like Four Seasons, Ritz-Carlton)
    for candidate in (
        stem,
        stem.replace("-", " "),
        re.sub(r"([a-z])([A-Z])", r"\1 \2", stem),
    ):
        brand_info = BrandRegistry.lookup(candidate)
        if brand_info.parent_company != "Unknown":
            return brand_info.parent_company

    chunks = stem.replace("_", "-").split("-")
    words = [c.upper() if 2 <= len(c) <= 3 and c.isalpha() else c.capitalize() for c in chunks if c]
    name = " ".join(words).strip()
    return name or None


# ──────────────────────────────────────────────────────────────────────────
# Phone validation
# ──────────────────────────────────────────────────────────────────────────

_PHONE_PREFIXES = (
    "tel:",
    "phone:",
    "ph:",
    "mobile:",
    "cell:",
    "m:",
    "direct:",
    "d:",
    "office:",
    "o:",
)


def _validate_phone(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    try:
        import phonenumbers
    except ImportError:
        return raw.strip() or None
    raw = raw.strip()
    low = raw.lower()
    for p in _PHONE_PREFIXES:
        if low.startswith(p):
            raw = raw[len(p) :].strip()
            break
    raw_base = re.split(r"\s*(?:ext|x|extension)\.?\s*\d", raw, flags=re.IGNORECASE, maxsplit=1)[
        0
    ].strip()
    for region in (PHONE_DEFAULT_REGION, None):
        try:
            num = phonenumbers.parse(raw_base, region)
            if phonenumbers.is_valid_number(num):
                return phonenumbers.format_number(num, phonenumbers.PhoneNumberFormat.E164)
        except phonenumbers.NumberParseException:
            continue
    return None


# ──────────────────────────────────────────────────────────────────────────
# Google Contacts helpers
# ──────────────────────────────────────────────────────────────────────────


def _dump_other_contacts(people) -> dict[str, dict]:
    contacts: dict[str, dict] = {}
    page_token = None
    while True:
        try:
            resp = (
                people.otherContacts()
                .list(
                    pageSize=1000,
                    readMask="names,emailAddresses,phoneNumbers",
                    pageToken=page_token,
                )
                .execute()
            )
        except HttpError as e:
            logger.warning(f"inbox_sync: otherContacts error: {e}")
            break
        for person in resp.get("otherContacts", []):
            emails = person.get("emailAddresses", [])
            if not emails:
                continue
            email = (emails[0].get("value") or "").lower()
            if not email:
                continue
            names = person.get("names", [])
            phones = person.get("phoneNumbers", [])
            contacts[email] = {
                "first_name": names[0].get("givenName") if names else None,
                "last_name": names[0].get("familyName") if names else None,
                "display_name": names[0].get("displayName") if names else None,
                "phone": phones[0].get("value") if phones else None,
            }
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return contacts


def _dump_saved_contacts(people) -> dict[str, dict]:
    contacts: dict[str, dict] = {}
    page_token = None
    while True:
        try:
            resp = (
                people.people()
                .connections()
                .list(
                    resourceName="people/me",
                    pageSize=1000,
                    personFields="names,emailAddresses,organizations,phoneNumbers,addresses",
                    pageToken=page_token,
                )
                .execute()
            )
        except HttpError as e:
            logger.warning(f"inbox_sync: savedContacts error: {e}")
            break
        for person in resp.get("connections", []):
            emails = person.get("emailAddresses", [])
            if not emails:
                continue
            email = (emails[0].get("value") or "").lower()
            if not email:
                continue
            names = person.get("names", [])
            orgs = person.get("organizations", [])
            phones = person.get("phoneNumbers", [])
            addrs = person.get("addresses", [])
            contacts[email] = {
                "first_name": names[0].get("givenName") if names else None,
                "last_name": names[0].get("familyName") if names else None,
                "display_name": names[0].get("displayName") if names else None,
                "organization": orgs[0].get("name") if orgs else None,
                "title": orgs[0].get("title") if orgs else None,
                "phone": phones[0].get("value") if phones else None,
                "address": addrs[0].get("formattedValue") if addrs else None,
            }
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return contacts


async def _normalize_saved_contact_orgs(
    client: httpx.AsyncClient,
    saved_contacts: dict[str, dict],
) -> dict[str, dict]:
    """NEW 2026-05-18: Fix 'Company - Person' format org fields at ingest time.

    Alex Arencibia's Google Contacts store entries like "Towne Park - Cindy Wetzel"
    in the organization field. This splits them correctly using Gemini so the
    pipeline always receives clean org + person name data — no cleanup scripts needed.

    Only processes entries where org contains ' - ' or ' – '.
    Modifies saved_contacts in place and returns it.
    """
    candidates = {
        email: data
        for email, data in saved_contacts.items()
        if data.get("organization")
        and (" - " in data["organization"] or " – " in data["organization"])
    }

    if not candidates:
        return saved_contacts

    logger.info(
        f"inbox_sync: normalizing {len(candidates)} saved contacts "
        f"with 'Company - Person' org format"
    )

    emails = list(candidates.keys())
    batches = [emails[i : i + 25] for i in range(0, len(emails), 25)]
    sem = asyncio.Semaphore(_ORG_SPLIT_SEMAPHORE)

    async def _split_batch(batch_emails: list[str]) -> tuple[list[str], list[dict]]:
        entries = [
            {"id": i, "original": candidates[e]["organization"]} for i, e in enumerate(batch_emails)
        ]
        prompt = ORG_SPLIT_PROMPT + json.dumps(entries, ensure_ascii=False)
        async with sem:
            try:
                raw = await ai_generate(client, prompt, model="gemini-2.5-flash-lite")
            except Exception as exc:
                logger.debug(f"inbox_sync: org-split Gemini error: {exc}")
                return batch_emails, []
        if not raw:
            return batch_emails, []
        raw = raw.strip()
        if raw.startswith("```"):
            raw = re.sub(r"^```(?:json)?\s*", "", raw)
            raw = re.sub(r"\s*```$", "", raw)
        try:
            parsed = json.loads(raw)
            return batch_emails, (parsed if isinstance(parsed, list) else [])
        except json.JSONDecodeError:
            return batch_emails, []

    tasks = [_split_batch(batch) for batch in batches]
    batch_results = await asyncio.gather(*tasks, return_exceptions=True)

    fixed = 0
    for result in batch_results:
        if isinstance(result, Exception):
            continue
        batch_emails, items = result
        for item in items:
            idx = item.get("id")
            if idx is None or idx >= len(batch_emails):
                continue
            email = batch_emails[idx]
            contact = saved_contacts[email]

            new_org = (item.get("org") or "").strip() or None
            person_name = (item.get("person_name") or "").strip() or None
            is_personal = bool(item.get("is_personal", False))

            if is_personal:
                contact["organization"] = None
            elif new_org:
                contact["organization"] = new_org

            # Only fill names if they aren't already set
            if person_name and not contact.get("first_name"):
                parts = person_name.split(None, 1)
                contact["first_name"] = parts[0] if parts else None
                contact["last_name"] = parts[1] if len(parts) > 1 else None

            fixed += 1

    logger.info(f"inbox_sync: org-split normalized {fixed} saved contacts")
    return saved_contacts


# ──────────────────────────────────────────────────────────────────────────
# Gmail message fetch
# ──────────────────────────────────────────────────────────────────────────


def _list_message_ids_full(
    gmail,
    mailbox: str,
    scan_days_override: Optional[int] = None,
) -> list[str]:
    days = scan_days_override if scan_days_override else SCAN_DAYS_BACK_INITIAL
    query = f"newer_than:{days}d -in:chats -in:spam -in:trash"
    ids: list[str] = []
    page_token = None
    while len(ids) < MAX_EMAILS_PER_RUN:
        try:
            resp = (
                gmail.users()
                .messages()
                .list(
                    userId="me",
                    q=query,
                    maxResults=min(500, MAX_EMAILS_PER_RUN - len(ids)),
                    pageToken=page_token,
                )
                .execute()
            )
        except HttpError as e:
            logger.error(f"inbox_sync: Gmail list error for {mailbox}: {e}")
            break
        for m in resp.get("messages", []):
            ids.append(m["id"])
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return ids


def _list_message_ids_incremental(
    gmail,
    mailbox: str,
    history_id: str,
) -> tuple[list[str], Optional[str]]:
    try:
        resp = (
            gmail.users()
            .history()
            .list(
                userId="me",
                startHistoryId=history_id,
                historyTypes=["messageAdded"],
                maxResults=500,
            )
            .execute()
        )
    except HttpError as e:
        if e.resp.status == 404:
            logger.warning(
                f"inbox_sync: history expired for {mailbox} "
                f"(history_id={history_id}), falling back to full scan"
            )
            return _list_message_ids_full(gmail, mailbox), None
        raise

    ids: list[str] = []
    for record in resp.get("history", []):
        for added in record.get("messagesAdded", []):
            msg_id = added.get("message", {}).get("id")
            if msg_id:
                ids.append(msg_id)

    new_history_id = resp.get("historyId") or history_id
    return ids, new_history_id


def _get_current_history_id(gmail) -> Optional[str]:
    try:
        profile = gmail.users().getProfile(userId="me").execute()
        return str(profile.get("historyId"))
    except HttpError:
        return None


def _fetch_message(gmail, msg_id: str) -> Optional[dict]:
    try:
        return gmail.users().messages().get(userId="me", id=msg_id, format="full").execute()
    except HttpError:
        return None


# ──────────────────────────────────────────────────────────────────────────
# Hospitality enrichment
# ──────────────────────────────────────────────────────────────────────────

_P1_KW = [
    "director of procurement",
    "vp procurement",
    "director of purchasing",
    "vp purchasing",
    "purchasing manager",
    "purchasing director",
    "purchasing agent",
    "purchasing supervisor",
    "purchasing coordinator",
    "chief procurement officer",
    "cpo",
    "procurement manager",
    "procurement director",
    "procurement specialist",
    "procurement agent",
    "category manager",
    "strategic sourcing",
    # Materials / supply chain / inventory — all buyer-side roles.
    "materials manager",
    "director of materials",
    "materials management",
    "supply chain manager",
    "supply chain director",
    "director of supply chain",
    "vp supply chain",
    "inventory manager",
    "inventory control",
    "storeroom manager",
    "stores manager",
    # Housekeeping + laundry/linen/wardrobe — own and reorder uniforms directly.
    "director of housekeeping",
    "executive housekeeper",
    "assistant executive housekeeper",
    "head housekeeper",
    "housekeeping manager",
    "laundry manager",
    "linen manager",
    "linen room",
    "uniform room",
    "wardrobe manager",
]
_P2_KW = [
    "general manager",
    "hotel manager",
    "resort manager",
    "managing director",
    "area general manager",
    "assistant general manager",
    "executive assistant manager",
    "director of operations",
    "director of rooms",
    "rooms division manager",
    "director of rooms division",
    "chief operating officer",
    "vp operations",
    "vp hotel operations",
    "svp operations",
    "director of food and beverage",
    "f&b director",
    "director of food & beverage",
]
_P2_TOKENS = ("gm", "coo")
_P3_KW = [
    "human resources",
    "people and culture",
    "people & culture",
    "talent",
    "executive chef",
    "head chef",
    "director of sales",
    "director of events",
    "controller",
    "director of finance",
    "front office manager",
    "spa director",
    "accounts payable",
    "accounts receivable",
]
_P3_TOKENS = ("hr",)
_P4_KW = [
    "legal",
    "attorney",
    "counsel",
    "information technology",
    "marketing",
    "public relations",
    "communications",
    "engineering",
    "maintenance",
    "security",
]
_P4_TOKENS = ("it", "pr")
_PROC_HINTS = ("procurement", "purchasing", "buyer", "sourcing", "supply chain")


def _word_match(text_str: str, tokens: tuple) -> bool:
    return any(re.search(rf"\b{re.escape(t)}\b", text_str, re.IGNORECASE) for t in tokens)


def _classify_priority(title: Optional[str], brand_info: BrandInfo) -> tuple[str, str]:
    if not title:
        return "P_unknown", "no title"
    tl = title.lower().strip()

    # Order matters: we check from the STRONGEST buyer signal down, and treat
    # P4 (non-buyer departments) as a FALLBACK, not a veto. Previously P4 was
    # checked first, so any title containing an incidental token like "IT" or
    # "Marketing" was dumped into P4 before procurement was even considered —
    # e.g. "VP, IT Procurement" or "Director of Operations & IT" were wrongly
    # buried. Now a real procurement/operational title always wins.

    # P1 — the buyer. Procurement / purchasing / supply-chain / housekeeping.
    if any(kw in tl for kw in _P1_KW):
        return "P1", "direct procurement title"
    if any(h in tl for h in _PROC_HINTS):
        return "P1", "procurement keyword in title"

    # P2 — operational decision-maker.
    if any(kw in tl for kw in _P2_KW) or _word_match(tl, _P2_TOKENS):
        # GPO (e.g. Avendra) is recorded as an informational flag on the
        # contact for sales awareness only — it must NEVER affect priority or
        # score. Operational decision-makers stay P2 regardless of GPO.
        return "P2", "operational decision-maker"

    # P3 — secondary internal contact.
    if any(kw in tl for kw in _P3_KW) or _word_match(tl, _P3_TOKENS):
        return "P3", "secondary contact"

    # P4 — known non-buyer department. Fallback only: reached just for titles
    # that matched none of the above, so it can't override a real buyer.
    if any(kw in tl for kw in _P4_KW) or _word_match(tl, _P4_TOKENS):
        return "P4", "non-buyer role"

    return "P_unknown", "title not matched"


def _enrich(contact: dict) -> dict:
    org = contact.get("organization") or ""
    title = contact.get("title")

    if not org:
        contact.update(
            {
                "parent_company": None,
                "brand_tier": None,
                "operating_model": None,
                "gpo": None,
                "procurement_priority": "P_unknown",
                "priority_reason": "no organization",
                "opportunity_level": None,
                "opportunity_score": None,
                "management_company": None,
            }
        )
        return contact

    brand_info: BrandInfo = BrandRegistry.lookup(org)
    is_unknown = brand_info.parent_company == "Unknown"

    contact["parent_company"] = None if is_unknown else brand_info.parent_company
    contact["brand_tier"] = None if is_unknown else brand_info.tier
    contact["operating_model"] = brand_info.operating_model
    contact["gpo"] = None if is_unknown else brand_info.gpo
    contact["opportunity_level"] = brand_info.opportunity_level

    mgmt_intel = get_management_company_intel(org)
    contact["management_company"] = org if mgmt_intel else None

    priority, reason = _classify_priority(title, brand_info)
    contact["procurement_priority"] = priority
    contact["priority_reason"] = reason

    # ── Procurement-gateway knowledge (HPI etc.) — see procurement_intelligence ──
    # Case A: the contact works AT a gateway → force P1; they're the real buyer
    #         for a whole family of properties, whatever their title says.
    # Case B: their brand's purchasing is centralized at a gateway → attach an
    #         awareness note only. Priority is NOT changed — we surface
    #         centralization as knowledge, never as a penalty on the property.
    domain = _domain(contact.get("email") or "")
    gw = match_gateway_contact(org, contact.get("parent_company"), domain)
    if gw:
        contact["procurement_priority"] = gw["priority"]  # P1
        contact["priority_reason"] = (
            f"{gw['short']} — centralized procurement gateway " f"({gw['covers']}). {gw['note']}"
        )
    else:
        brand_gw = gateway_for_brand(org, contact.get("parent_company"))
        if brand_gw:
            contact["priority_reason"] = (
                f"{reason} · purchasing centralized at {brand_gw['short']} "
                f"({brand_gw['covers']})"
            )

    interactions = contact.get("interaction_count") or 0
    contact["opportunity_score"] = round(interactions * brand_info.contact_score_multiplier, 2)
    return contact


# ──────────────────────────────────────────────────────────────────────────
# Signature parsing (async, Gemini Flash-Lite)
# ──────────────────────────────────────────────────────────────────────────


async def _parse_sig(client: httpx.AsyncClient, sig_block: str) -> dict:
    if not sig_block or len(sig_block) < 20:
        return {}
    prompt = SIGNATURE_PROMPT + sig_block
    try:
        text_out = await ai_generate(client, prompt, model="gemini-2.5-flash-lite")
    except Exception as e:
        logger.debug(f"inbox_sync: Gemini sig parse error: {e}")
        return {}
    if not text_out:
        return {}
    text_out = text_out.strip()
    if text_out.startswith("```"):
        text_out = re.sub(r"^```(?:json)?\s*", "", text_out)
        text_out = re.sub(r"\s*```$", "", text_out)
    try:
        data = json.loads(text_out)
        return data if isinstance(data, dict) else {}
    except json.JSONDecodeError:
        return {}


# ──────────────────────────────────────────────────────────────────────────
# mailbox_sync_state helpers
# ──────────────────────────────────────────────────────────────────────────


async def _upsert_sync_state(session: AsyncSession, mailbox: str, **kwargs) -> None:
    now = datetime.now(timezone.utc)
    result = await session.execute(
        text("SELECT 1 FROM mailbox_sync_state WHERE mailbox = :m"),
        {"m": mailbox},
    )
    exists = result.fetchone() is not None

    if not exists:
        cols = ["mailbox", "updated_at", "created_at"] + list(kwargs.keys())
        vals = [":mailbox", ":now", ":now"] + [f":{k}" for k in kwargs]
        await session.execute(
            text(f"INSERT INTO mailbox_sync_state ({', '.join(cols)}) VALUES ({', '.join(vals)})"),
            {"mailbox": mailbox, "now": now, **kwargs},
        )
    else:
        if kwargs:
            set_clause = ", ".join(f"{k} = :{k}" for k in kwargs)
            await session.execute(
                text(
                    f"UPDATE mailbox_sync_state SET {set_clause}, updated_at = :now "
                    f"WHERE mailbox = :mailbox"
                ),
                {"mailbox": mailbox, "now": now, **kwargs},
            )


# ──────────────────────────────────────────────────────────────────────────
# Main entry point
# ──────────────────────────────────────────────────────────────────────────


async def sync_mailbox(
    mailbox: str,
    session: AsyncSession,
    *,
    force_full_scan: bool = False,
    scan_days_override: Optional[int] = None,
) -> dict[str, Any]:
    """Sync one mailbox and upsert contacts into the contacts table."""
    run_start = datetime.now(timezone.utc)
    logger.info(f"inbox_sync: starting sync for {mailbox}")

    await _upsert_sync_state(session, mailbox, last_run_status="running")
    await session.commit()

    stats: dict[str, Any] = {
        "mailbox": mailbox,
        "messages_scanned": 0,
        "contacts_found": 0,
        "new_contacts": 0,
        "updated_contacts": 0,
        "errors": 0,
        "status": "success",
    }

    rejections = {
        "ja_org_mismatch": 0,
        "ja_team_leak": 0,
        "ja_sig_on_external": 0,
        "email_domain_mismatch": 0,
        "not_real_person": 0,
        "low_confidence": 0,
        "name_mismatch": 0,
        "rescued_via_parsed_email": 0,
    }

    try:
        gmail = _gmail(mailbox)
        people = _people(mailbox)

        other_contacts = _dump_other_contacts(people)
        saved_contacts = _dump_saved_contacts(people)
        logger.info(
            f"inbox_sync: {mailbox} — "
            f"{len(other_contacts)} other, {len(saved_contacts)} saved contacts"
        )

        state_row = await session.execute(
            text("SELECT last_history_id FROM mailbox_sync_state WHERE mailbox = :m"),
            {"m": mailbox},
        )
        state = state_row.mappings().first()
        last_history_id = (state or {}).get("last_history_id") if state else None
        new_history_id: Optional[str] = None

        if force_full_scan or not last_history_id:
            logger.info(
                f"inbox_sync: {mailbox} — full backfill "
                f"(last {scan_days_override or SCAN_DAYS_BACK_INITIAL}d)"
            )
            message_ids = _list_message_ids_full(gmail, mailbox, scan_days_override)
            new_history_id = _get_current_history_id(gmail)
        else:
            logger.info(f"inbox_sync: {mailbox} — incremental from history_id={last_history_id}")
            message_ids, new_history_id = _list_message_ids_incremental(
                gmail, mailbox, last_history_id
            )
            if new_history_id is None:
                new_history_id = _get_current_history_id(gmail)

        logger.info(f"inbox_sync: {mailbox} — {len(message_ids)} messages to process")
        stats["messages_scanned"] = len(message_ids)

        sig_cache: dict[str, dict] = {}
        gmail_contacts: dict[str, dict] = {}
        seen_in_headers: dict[str, dict] = {}

        # ── Single shared HTTP client for all Gemini calls in this run ──
        async with httpx.AsyncClient(timeout=120.0) as http_client:
            # ── Step 0: Normalize saved contact org fields ────────────
            # Fixes "Towne Park - Cindy Wetzel" → org + person name split
            # BEFORE any merge logic runs, so all downstream code gets clean data.
            saved_contacts = await _normalize_saved_contact_orgs(http_client, saved_contacts)

            # ── Phase 1: Fetch messages, track headers, queue sig blocks ──
            # Pure sync work — no Gemini calls here. Collects:
            #   pending_sigs  → unique sig blocks that need Gemini parsing
            #   work_items    → (sig_hash, sig_owner, header_name) per segment
            pending_sigs: dict[str, str] = {}  # sig_hash → sig_block
            work_items: list[tuple] = []  # (sig_hash, sig_owner, header_name)

            for idx, msg_id in enumerate(message_ids):
                if idx % 200 == 0 and idx > 0:
                    logger.debug(f"inbox_sync: {mailbox} — phase1 {idx}/{len(message_ids)}")

                msg = _fetch_message(gmail, msg_id)
                if not msg:
                    continue

                headers = {
                    h["name"]: h.get("value", "")
                    for h in (msg.get("payload", {}).get("headers") or [])
                }

                for hdr in ("From", "To", "Cc", "Bcc"):
                    val = headers.get(hdr, "")
                    display = _display_name(val) if hdr == "From" else ""
                    for email in _extract_emails(val):
                        entry = seen_in_headers.setdefault(
                            email,
                            {
                                "email": email,
                                "display_name": display,
                                "interaction_count": 0,
                            },
                        )
                        entry["interaction_count"] += 1
                        if display and not entry["display_name"]:
                            entry["display_name"] = display

                body = _extract_plain(msg.get("payload", {}))
                if not body:
                    continue

                body = _preprocess(body)
                segments = _split_segments(body)
                top_sender = next(iter(_extract_emails(headers.get("From", ""))), None)
                top_sender_name = _display_name(headers.get("From", ""))

                for seg_idx, seg in enumerate(segments):
                    sig_owner = top_sender if seg_idx == 0 else _seg_email(seg)
                    header_name = top_sender_name if seg_idx == 0 else _seg_name(seg)

                    # ── Embedded To:/Cc: participants (2026-06-04) ──
                    # People named on forwarded threads who never emailed us
                    # directly. Feed them into seen_in_headers so they ride
                    # the exact same merge + hard-filter pipeline as
                    # top-level recipients.
                    if seg_idx > 0:
                        for p_name, p_email in _seg_participants(seg):
                            if p_email == mailbox or _domain(p_email) in OWN_DOMAINS:
                                continue
                            entry = seen_in_headers.setdefault(
                                p_email,
                                {
                                    "email": p_email,
                                    "display_name": p_name,
                                    "interaction_count": 0,
                                },
                            )
                            entry["interaction_count"] += 1
                            if p_name and not entry["display_name"]:
                                entry["display_name"] = p_name

                    if not sig_owner or sig_owner == mailbox:
                        continue
                    if _domain(sig_owner) in OWN_DOMAINS:
                        continue

                    # ── No-signature forwarded sender (2026-06-04) ──
                    # The embedded From: line already gave us name + email.
                    # Previously these died at the sig-block check below;
                    # now they survive via the header path even when the
                    # forwarded message carried no signature.
                    if seg_idx > 0:
                        _hn = _normalize_header_name(header_name)
                        entry = seen_in_headers.setdefault(
                            sig_owner,
                            {
                                "email": sig_owner,
                                "display_name": _hn,
                                "interaction_count": 0,
                            },
                        )
                        entry["interaction_count"] += 1
                        if _hn and not entry["display_name"]:
                            entry["display_name"] = _hn

                    sig_block = _extract_sig_block(seg)
                    if not sig_block or len(sig_block) < 30:
                        continue

                    sig_hash = hashlib.sha256(sig_block.encode()).hexdigest()[:16]
                    if sig_hash not in sig_cache:
                        pending_sigs[sig_hash] = sig_block

                    work_items.append((sig_hash, sig_owner, header_name))

            # ── Phase 2: Parallel Gemini sig parsing ─────────────────
            # All unique sig blocks parsed concurrently — was sequential.
            # Rate limit: Semaphore(20) → ~1200 RPM max against 2000 RPM limit.
            if pending_sigs:
                logger.info(
                    f"inbox_sync: {mailbox} — parsing {len(pending_sigs)} unique "
                    f"sig blocks in parallel (sem={_SIG_PARSE_SEMAPHORE})"
                )
                _parse_sem = asyncio.Semaphore(_SIG_PARSE_SEMAPHORE)

                async def _parse_one(h: str, b: str) -> tuple[str, dict]:
                    async with _parse_sem:
                        return h, await _parse_sig(http_client, b)

                parse_tasks = [
                    _parse_one(h, b) for h, b in pending_sigs.items() if h not in sig_cache
                ]
                parse_results = await asyncio.gather(*parse_tasks, return_exceptions=True)
                for r in parse_results:
                    if isinstance(r, Exception):
                        logger.debug(f"inbox_sync: sig parse task error: {r}")
                        continue
                    h, parsed = r
                    sig_cache[h] = parsed or {}

                logger.info(
                    f"inbox_sync: {mailbox} — sig parsing done, " f"cache={len(sig_cache)} entries"
                )

            # ── Phase 3: Apply results, validation gauntlet ──────────
            for sig_hash, sig_owner, header_name in work_items:
                parsed = sig_cache.get(sig_hash)
                if not parsed:
                    continue

                # Layer 1: explicit JA org in parsed sig
                parsed_org = (parsed.get("organization") or "").lower()
                if any(t in parsed_org for t in ("jauniforms", "j.a. uniforms", "ja uniforms")):
                    rejections["ja_org_mismatch"] += 1
                    continue

                # Layer 2: JA team name on external email
                if _is_ja_team_leak(parsed, sig_owner):
                    rejections["ja_team_leak"] += 1
                    logger.debug(
                        f"inbox_sync: rejected JA team leak — "
                        f"{sig_owner} → {parsed.get('first_name')} {parsed.get('last_name')}"
                    )
                    continue

                # Layer 2.5: JA domain email in sig but external sig_owner
                _parsed_email_raw = (parsed.get("email") or "").lower().strip()
                if (
                    _parsed_email_raw
                    and _domain(_parsed_email_raw) in OWN_DOMAINS
                    and _domain(sig_owner) not in OWN_DOMAINS
                ):
                    rejections["ja_sig_on_external"] += 1
                    logger.debug(
                        f"inbox_sync: rejected JA sig on external — "
                        f"sig_owner={sig_owner}, parsed_email={_parsed_email_raw}"
                    )
                    continue

                # Layer 3: email domain mismatch
                parsed_email = (parsed.get("email") or "").lower().strip()
                if parsed_email and parsed_email != sig_owner:
                    if _domain(parsed_email) != _domain(sig_owner):
                        rejections["email_domain_mismatch"] += 1
                        continue

                # Layer 4: not a real person
                if parsed.get("is_real_person") is False:
                    rejections["not_real_person"] += 1
                    continue

                # Layer 5: confidence threshold
                conf = parsed.get("confidence")
                if isinstance(conf, (int, float)) and conf < MIN_CONFIDENCE:
                    rejections["low_confidence"] += 1
                    continue

                # Layer 6: name-in-header check with rescue path
                if not _name_matches_header(parsed, header_name):
                    if (
                        parsed_email
                        and parsed_email != sig_owner
                        and _domain(parsed_email) == _domain(sig_owner)
                        and _domain(parsed_email) not in OWN_DOMAINS
                    ):
                        sig_owner = parsed_email
                        rejections["rescued_via_parsed_email"] += 1
                    else:
                        rejections["name_mismatch"] += 1
                        continue

                # Validate phones
                for field in ("phone", "mobile"):
                    raw = parsed.get(field)
                    if raw:
                        parsed[field] = _validate_phone(raw)

                # Keep richest sig per email
                new_score = sum(1 for v in parsed.values() if v)
                old_score = sum(1 for v in (gmail_contacts.get(sig_owner) or {}).values() if v)
                if not gmail_contacts.get(sig_owner) or new_score > old_score:
                    gmail_contacts[sig_owner] = parsed

        logger.info(
            f"inbox_sync: {mailbox} — "
            f"{len(gmail_contacts)} sig contacts, "
            f"{len(seen_in_headers)} header contacts"
        )
        if any(rejections.values()):
            logger.info(f"inbox_sync: {mailbox} — rejections: {rejections}")

        # ── Merge all sources ─────────────────────────────────────────
        all_emails = (
            set(seen_in_headers.keys()) | set(saved_contacts.keys()) | set(gmail_contacts.keys())
        )

        merged_contacts: list[dict] = []

        for email in all_emails:
            ok, _ = _passes_hard_filters(email, mailbox)
            if not ok:
                continue

            sig = gmail_contacts.get(email) or {}
            saved = saved_contacts.get(email) or {}
            other = other_contacts.get(email) or {}
            header = seen_in_headers.get(email) or {}

            def pick(*pairs):
                for d, k in pairs:
                    v = d.get(k)
                    if v:
                        return v
                return None

            _header_dn = (header.get("display_name") or "").strip()
            _header_fn = None
            _header_ln = None
            if _header_dn:
                _parts = _header_dn.split(None, 1)
                if len(_parts) >= 2:
                    _header_fn = _parts[0]
                    _header_ln = _parts[1]
                elif len(_parts) == 1 and len(_parts[0]) > 1:
                    _header_fn = _parts[0]
            _header_name_dict = {"first_name": _header_fn, "last_name": _header_ln}

            org = pick((sig, "organization"), (saved, "organization"))
            org_source = (
                "signature"
                if sig.get("organization")
                else "saved_contacts"
                if saved.get("organization")
                else None
            )
            if not org:
                inferred = _infer_org(_domain(email))
                if inferred:
                    org = inferred
                    org_source = "domain_inferred"

            if _domain(email) in PERSONAL_DOMAINS and not org:
                continue

            contact = {
                "email": email,
                "first_name": pick(
                    (sig, "first_name"),
                    (saved, "first_name"),
                    (_header_name_dict, "first_name"),
                ),
                "last_name": pick(
                    (sig, "last_name"),
                    (saved, "last_name"),
                    (_header_name_dict, "last_name"),
                ),
                "display_name": (_header_dn or saved.get("display_name") or None),
                "title": pick((sig, "title"), (saved, "title")),
                "organization": org,
                "org_source": org_source,
                "phone": (
                    pick((sig, "phone"), (sig, "mobile"), (saved, "phone"))
                    or _validate_phone(other.get("phone"))
                ),
                "address": pick((sig, "address"), (saved, "address")),
                "linkedin_url": sig.get("linkedin_url"),
                "has_signature": bool(sig),
                "confidence": sig.get("confidence"),
                "interaction_count": header.get("interaction_count", 0),
                "source_mailbox": mailbox,
            }

            contact = _enrich(contact)
            merged_contacts.append(contact)

        stats["contacts_found"] = len(merged_contacts)
        logger.info(f"inbox_sync: {mailbox} — {len(merged_contacts)} contacts after merge/filter")

        if merged_contacts:
            upsert_stats = await bulk_upsert_contacts(
                session,
                merged_contacts,
                source_mailbox=mailbox,
            )
            await session.commit()
            stats["new_contacts"] = upsert_stats["inserted"]
            stats["updated_contacts"] = upsert_stats["updated"]
            stats["errors"] = upsert_stats["errors"]

        await _upsert_sync_state(
            session,
            mailbox,
            last_history_id=new_history_id,
            last_synced_at=run_start,
            last_run_status="success",
            last_run_contacts_found=stats["contacts_found"],
            last_run_new_contacts=stats["new_contacts"],
            last_run_updated_contacts=stats["updated_contacts"],
            last_run_messages_scanned=stats["messages_scanned"],
            last_run_error=None,
            consecutive_errors=0,
        )
        await session.commit()

        logger.info(
            f"inbox_sync: {mailbox} done — "
            f"new={stats['new_contacts']} updated={stats['updated_contacts']} "
            f"errors={stats['errors']}"
        )
        return stats

    except Exception as e:
        logger.exception(f"inbox_sync: fatal error for {mailbox}: {e}")
        stats["status"] = "error"
        stats["error"] = str(e)[:500]

        try:
            state_row = await session.execute(
                text("SELECT consecutive_errors FROM mailbox_sync_state WHERE mailbox = :m"),
                {"m": mailbox},
            )
            row = state_row.mappings().first()
            consecutive = ((row or {}).get("consecutive_errors") or 0) + 1

            await _upsert_sync_state(
                session,
                mailbox,
                last_run_status="error",
                last_run_error=str(e)[:500],
                consecutive_errors=consecutive,
            )
            await session.commit()
        except Exception as inner:
            logger.error(f"inbox_sync: failed to record error state for {mailbox}: {inner}")

        return stats
