"""app/services/inbox_sync.py

Refactored v4.6 Gmail contact-extraction pipeline as a reusable async module.

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

Credentials: credentials/slh-contact-sync.json
Scopes: gmail.readonly + contacts.readonly + contacts.other.readonly
"""

from __future__ import annotations

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

# JA team first/last names used for cross-attribution leak detection
# (catches the cbrown@evolutionpgs.com → "Sai Kandregula" bug from the
# Ugarcia audit). If a parsed signature has a name in this set but the
# email's domain is NOT a JA domain, we reject the parse as a leak.
JA_TEAM_NAMES: set[str] = {
    "sai",
    "jayasurya",
    "kandregula",
    "menchu",
    "ugarcia",
}

# Expanded from audit of 43 junk entries in Ugarcia's mailbox.
NOREPLY_PATTERNS = [
    "noreply",
    "no-reply",
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

MASS_MAIL_PATTERNS = ["newsletter", "unsubscribe", "digest"]

# Expanded from audit — covers SigParser-equivalent procurement-platform
# notifications + cold prospecting tools that pollute the contact pool.
SAAS_DOMAINS: set[str] = {
    # Email infrastructure / ESPs
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
    # Procurement / payment / accounting platforms
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
    # Marketing / notification subdomains seen in Ugarcia audit
    "feedback-marriott.com",
    "message.fedex.com",
    "email.netsuite.com",
    "notification.intuit.com",
    "mail.beehiiv.com",
    "unbounce.com",
    "mail.clickup.com",
    # Cold prospecting tools (so we don't ingest their reply addresses)
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
# FIX 2026-05-14: 1 day was too tight — first Monday-morning run after
# the weekend would miss Saturday/Sunday emails. 2 days closes the gap.
# Manual backfill script uses scan_days_override to do bigger windows.
SCAN_DAYS_BACK_INITIAL = 2

MAX_EMAILS_PER_RUN = 5000  # Safety cap per mailbox per Celery run
PHONE_DEFAULT_REGION = "US"

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

Block:
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
    re.compile(
        r"^.*This message is from an EXTERNAL SENDER.*$", re.MULTILINE | re.IGNORECASE
    ),
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
    return build(
        "gmail", "v1", credentials=_build_creds(mailbox), cache_discovery=False
    )


def _people(mailbox: str):
    return build(
        "people", "v1", credentials=_build_creds(mailbox), cache_discovery=False
    )


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
    html = re.sub(
        r"<(script|style)[^>]*>.*?</\1>", "", html, flags=re.IGNORECASE | re.DOTALL
    )
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


def _name_matches_header(parsed: dict, header_name: str) -> bool:
    fn = (parsed.get("first_name") or "").lower().strip()
    ln = (parsed.get("last_name") or "").lower().strip()
    if not fn and not ln:
        return True
    if not header_name:
        return True
    hn = header_name.lower()
    return (fn and len(fn) > 1 and fn in hn) or (ln and len(ln) > 1 and ln in hn)


def _is_ja_team_leak(parsed: dict, sig_owner: str) -> bool:
    """Detect JA team signature mis-attributed to an external email.

    The Ugarcia audit found `cbrown@evolutionpgs.com` got attached to a
    parsed signature reading "Sai Kandregula / IT Associate / JA Uniforms".
    The org-mismatch guard catches cases where the parsed_org explicitly
    contains "JA Uniforms" — but that guard misses when Gemini leaves the
    organization blank and only the name is JA-staff.

    This guard rejects parses where:
      - the parsed first or last name is a known JA team name, AND
      - the sig_owner email is NOT on a JA domain
    """
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


def _passes_hard_filters(email: str, own_mailbox: str) -> tuple[bool, str]:
    if "@" not in email:
        return False, "malformed"
    if email == own_mailbox:
        return False, "self"
    d = _domain(email)
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
        len(parts) >= 3
        and parts[-2] in {"co", "com", "org", "net", "gov"}
        and len(parts[-1]) == 2
    ):
        stem = ".".join(parts[:-2])
    else:
        stem = ".".join(parts[:-1])
    if "." in stem:
        stem = stem.split(".")[-1]
    if not stem or len(stem) < 2:
        return None
    chunks = stem.replace("_", "-").split("-")
    words = [
        c.upper() if 2 <= len(c) <= 3 and c.isalpha() else c.capitalize()
        for c in chunks
        if c
    ]
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
    raw_base = re.split(
        r"\s*(?:ext|x|extension)\.?\s*\d", raw, flags=re.IGNORECASE, maxsplit=1
    )[0].strip()
    for region in (PHONE_DEFAULT_REGION, None):
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


# ──────────────────────────────────────────────────────────────────────────
# Gmail message fetch
# ──────────────────────────────────────────────────────────────────────────


def _list_message_ids_full(
    gmail,
    mailbox: str,
    scan_days_override: Optional[int] = None,
) -> list[str]:
    """Full backfill — fetch last N days of messages.

    Args:
        scan_days_override: If set, use this many days instead of
            SCAN_DAYS_BACK_INITIAL. The backfill script
            (scripts/backfill_contacts.py) passes a larger value for
            one-time historical scans without touching the global.
    """
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
    """Delta sync — fetch only messages since last_history_id.

    Returns (message_ids, new_history_id).
    Falls back to full backfill if history expired (404).
    """
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
    """Get the current historyId from the mailbox profile."""
    try:
        profile = gmail.users().getProfile(userId="me").execute()
        return str(profile.get("historyId"))
    except HttpError:
        return None


def _fetch_message(gmail, msg_id: str) -> Optional[dict]:
    try:
        return (
            gmail.users()
            .messages()
            .get(userId="me", id=msg_id, format="full")
            .execute()
        )
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
    "chief procurement officer",
    "cpo",
    "director of housekeeping",
    "executive housekeeper",
    "head housekeeper",
]
_P2_KW = [
    "general manager",
    "hotel manager",
    "resort manager",
    "director of operations",
    "director of rooms",
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
_PROC_HINTS = ("procurement", "purchasing", "buyer", "sourcing")


def _word_match(text_str: str, tokens: tuple) -> bool:
    return any(
        re.search(rf"\b{re.escape(t)}\b", text_str, re.IGNORECASE) for t in tokens
    )


def _classify_priority(title: Optional[str], brand_info: BrandInfo) -> tuple[str, str]:
    if not title:
        return "P_unknown", "no title"
    tl = title.lower().strip()
    if any(kw in tl for kw in _P4_KW) or _word_match(tl, _P4_TOKENS):
        return "P4", "non-buyer role"
    if any(kw in tl for kw in _P1_KW):
        return "P1", "direct procurement title"
    if any(h in tl for h in _PROC_HINTS):
        return "P1", "procurement keyword in title"
    p2 = any(kw in tl for kw in _P2_KW) or _word_match(tl, _P2_TOKENS)
    if p2:
        tier = brand_info.tier_for_title(title)
        if brand_info.gpo and tier == "property":
            return "P3", f"GPO-constrained property role ({brand_info.gpo})"
        return "P2", "operational decision-maker"
    if any(kw in tl for kw in _P3_KW) or _word_match(tl, _P3_TOKENS):
        return "P3", "secondary contact"
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

    interactions = contact.get("interaction_count") or 0
    contact["opportunity_score"] = round(
        interactions * brand_info.contact_score_multiplier, 2
    )
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
# (Removed dead _get_sync_state function 2026-05-14 — sync_mailbox queries
# last_history_id inline, _get_sync_state was never called.)


async def _upsert_sync_state(session: AsyncSession, mailbox: str, **kwargs) -> None:
    """Insert or update mailbox_sync_state row."""
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
            text(
                f"INSERT INTO mailbox_sync_state ({', '.join(cols)}) VALUES ({', '.join(vals)})"
            ),
            {"mailbox": mailbox, "now": now, **kwargs},
        )
    else:
        if kwargs:
            set_clause = ", ".join(f"{k} = :{k}" for k in kwargs)
            await session.execute(
                text(
                    f"UPDATE mailbox_sync_state SET {set_clause}, updated_at = :now WHERE mailbox = :mailbox"
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
    """Sync one mailbox and upsert contacts into the contacts table.

    Args:
        mailbox: Full email address to sync, e.g. "ugarcia@jauniforms.com"
        session: AsyncSession for DB writes.
        force_full_scan: Ignore History API cursor and do a full backfill.
        scan_days_override: When doing a full scan, scan this many days
            instead of SCAN_DAYS_BACK_INITIAL. Used by the manual
            backfill CLI script (scripts/backfill_contacts.py).

    Returns:
        Run stats dict: {mailbox, messages_scanned, contacts_found,
                         new_contacts, updated_contacts, errors, status}
    """
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

    # Rejection-stat counters (logged at end, useful for debugging
    # quality issues like the cbrown leak).
    rejections = {
        "ja_org_mismatch": 0,
        "ja_team_leak": 0,
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

        # Determine message ID list
        state_row = await session.execute(
            text("SELECT last_history_id FROM mailbox_sync_state WHERE mailbox = :m"),
            {"m": mailbox},
        )
        state = state_row.mappings().first()
        last_history_id = (state or {}).get("last_history_id") if state else None

        new_history_id: Optional[str] = None

        if force_full_scan or not last_history_id:
            days = scan_days_override if scan_days_override else SCAN_DAYS_BACK_INITIAL
            logger.info(f"inbox_sync: {mailbox} — full backfill (last {days}d)")
            message_ids = _list_message_ids_full(gmail, mailbox, scan_days_override)
            new_history_id = _get_current_history_id(gmail)
        else:
            logger.info(
                f"inbox_sync: {mailbox} — incremental from history_id={last_history_id}"
            )
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

        async with httpx.AsyncClient(timeout=60.0) as http_client:
            for idx, msg_id in enumerate(message_ids):
                if idx % 200 == 0 and idx > 0:
                    logger.debug(f"inbox_sync: {mailbox} — {idx}/{len(message_ids)}")

                msg = _fetch_message(gmail, msg_id)
                if not msg:
                    continue

                headers = {
                    h["name"]: h.get("value", "")
                    for h in (msg.get("payload", {}).get("headers") or [])
                }

                # Header interaction-count tracking
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

                    if not sig_owner or sig_owner == mailbox:
                        continue
                    if _domain(sig_owner) in OWN_DOMAINS:
                        continue

                    sig_block = _extract_sig_block(seg)
                    if not sig_block or len(sig_block) < 30:
                        continue

                    sig_hash = hashlib.sha256(sig_block.encode()).hexdigest()[:16]
                    if sig_hash in sig_cache:
                        parsed = sig_cache[sig_hash]
                    else:
                        parsed = await _parse_sig(http_client, sig_block)
                        sig_cache[sig_hash] = parsed

                    if not parsed:
                        continue

                    # Validation gauntlet ───────────────────────────────────

                    # Layer 1: JA org mismatch (explicit JA Uniforms in parsed_org)
                    parsed_org = (parsed.get("organization") or "").lower()
                    if any(
                        t in parsed_org
                        for t in ("jauniforms", "j.a. uniforms", "ja uniforms")
                    ):
                        rejections["ja_org_mismatch"] += 1
                        continue

                    # Layer 2: JA team leak (Sai/Menchu/Ugarcia name on external email)
                    # This catches the cbrown@evolutionpgs.com bug Layer 1 missed.
                    if _is_ja_team_leak(parsed, sig_owner):
                        rejections["ja_team_leak"] += 1
                        logger.debug(
                            f"inbox_sync: rejected JA team leak — "
                            f"{sig_owner} → parsed as "
                            f"{parsed.get('first_name')} {parsed.get('last_name')}"
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

                    # Layer 6: name in header — try rescue via parsed email
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

                    # Keep the richest sig per email
                    new_score = sum(1 for v in parsed.values() if v)
                    old_score = sum(
                        1 for v in (gmail_contacts.get(sig_owner) or {}).values() if v
                    )
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
            set(seen_in_headers.keys())
            | set(saved_contacts.keys())
            | set(gmail_contacts.keys())
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

            # Skip personal-domain contacts with no org (not hospitality)
            if _domain(email) in PERSONAL_DOMAINS and not org:
                continue

            contact = {
                "email": email,
                "first_name": pick(
                    (sig, "first_name"), (saved, "first_name"), (other, "first_name")
                ),
                "last_name": pick(
                    (sig, "last_name"), (saved, "last_name"), (other, "last_name")
                ),
                "display_name": (
                    header.get("display_name")
                    or saved.get("display_name")
                    or other.get("display_name")
                ),
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
        logger.info(
            f"inbox_sync: {mailbox} — {len(merged_contacts)} contacts after merge/filter"
        )

        # ── Upsert to contacts table ──────────────────────────────────
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

        # ── Update sync state ─────────────────────────────────────────
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
                text(
                    "SELECT consecutive_errors FROM mailbox_sync_state WHERE mailbox = :m"
                ),
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
            logger.error(
                f"inbox_sync: failed to record error state for {mailbox}: {inner}"
            )

        return stats
