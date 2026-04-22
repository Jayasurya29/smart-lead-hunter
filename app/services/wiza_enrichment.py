"""
LEAD GENERATOR — Wiza Email Enrichment
========================================
Finds verified work email addresses for hotel contacts using the Wiza API.

PRIMARY INPUT: LinkedIn URL
  - Highest match rate — Wiza's database is keyed on LinkedIn IDs
  - Exact person match (no ambiguity with common names)
  - Always preferred when available

FALLBACK INPUT: Name + Company (scope-aware)
  - Fires ONLY when LinkedIn is absent (never found, or rejected by
    our two-stage verifier for common-name cases like "Rob Smith")
  - Company choice depends on contact.scope:
      hotel_specific       → hotel name
      chain_area           → management company
      management_corporate → management company
      chain_corporate      → brand parent
      owner                → owner company
  - Prefers contact.organization when populated (snippet extractor's
    direct output is more reliable than scope-based mapping)

IMPORTANT: If LinkedIn lookup returns "not found", we do NOT fall back
to name+company. A verified LinkedIn match with no email means Wiza
knows exactly who we mean but doesn't have their email — falling back
risks finding a different person with the same name.

API FLOW (async):
  1. POST /api/individual_reveals → returns {id, status: "queued"}
  2. Poll GET /api/individual_reveals/:id until is_complete=true
  3. Email is in data.email (or data.emails[0].email)

Credits are consumed only when an email is successfully found.
Failed lookups are free. Cost per found email is 2 credits
(empirically confirmed — Wiza's docs say 1 but real-world billing is 2).

SETUP:
  1. Add WIZA_API_KEY to your .env (get from wiza.co/app/settings/api)
  2. Purchase API credits at wiza.co (contact Wiza to enable API access)

API DOCS: https://docs.wiza.co/api-reference
"""

import asyncio
import logging
import os
import re
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

WIZA_BASE = "https://wiza.co/api"

# Wiza's email statuses (from docs):
#   "valid"      — high confidence, deliverable  → HIGH
#   "catch_all"  — domain accepts all mail       → MEDIUM
#   "risky"      — some deliverability risk      → MEDIUM
#   "unknown"    — could not verify              → skip
#   "invalid"    — known undeliverable           → skip
VALID_STATUSES = {"valid", "catch_all", "risky"}

# Polling settings for the async reveal
POLL_INTERVAL_SEC = 3
POLL_TIMEOUT_SEC = 60  # most reveals complete in 5-15 seconds

# Generic organization names that shouldn't be trusted as the
# company input — if contact.organization matches any of these,
# fall back to scope-based mapping from lead fields instead.
_GENERIC_ORG_PATTERNS = {
    "",
    "unknown",
    "n/a",
    "none",
    "hotel",
    "resort",
    "hospitality",
    "management",
    "services",
    "hotels",
    "resorts",
    "company",
}


def _get_api_key() -> Optional[str]:
    key = os.getenv("WIZA_API_KEY", "").strip()
    if not key or key == "your-wiza-api-key-here":
        return None
    return key


def _normalize_linkedin_url(url: str) -> Optional[str]:
    """
    Normalize LinkedIn URL to the format Wiza expects.
    Handles country subdomains (jm.linkedin.com, ca.linkedin.com, etc.)
    and strips query params.
    """
    if not url:
        return None
    url = url.strip()
    m = re.search(r"linkedin\.com/in/([^/?&#\s]+)", url, re.IGNORECASE)
    if not m:
        return None
    profile_id = m.group(1).rstrip("/")
    return f"https://www.linkedin.com/in/{profile_id}"


def _confidence_from_status(status: str) -> str:
    """Map Wiza email_status to our internal confidence label."""
    s = (status or "").lower()
    if s == "valid":
        return "high"
    if s in ("catch_all", "risky"):
        return "medium"
    return "low"


def _is_generic_org(org: str) -> bool:
    """Check if an organization name is too generic to be useful."""
    if not org:
        return True
    cleaned = org.strip().lower()
    if cleaned in _GENERIC_ORG_PATTERNS:
        return True
    # Single word under 4 chars is suspicious
    if len(cleaned) < 4:
        return True
    return False


def pick_wiza_company(
    contact_organization: Optional[str],
    contact_scope: Optional[str],
    hotel_name: Optional[str] = None,
    management_company: Optional[str] = None,
    brand_parent: Optional[str] = None,
    owner_company: Optional[str] = None,
) -> Optional[str]:
    """
    Choose the right company to send to Wiza for a name+company lookup.

    Priority:
      1. contact.organization (if not generic/empty) — extraction source
         already determined where this person actually works.
      2. Scope-based mapping from lead fields:
           hotel_specific       → hotel_name
           chain_area           → management_company
           management_corporate → management_company
           chain_corporate      → brand_parent
           owner                → owner_company
      3. Safe fallback chain: management_company → hotel_name → brand_parent

    Returns None only if we have absolutely nothing to send.

    Example (Kali's Adam Butts, scope=management_corporate):
      contact_organization = "Crescent Hotels & Resorts"  (set by extractor)
      → returns "Crescent Hotels & Resorts"
    """
    # Priority 1: trust the extractor's organization field
    if contact_organization and not _is_generic_org(contact_organization):
        return contact_organization.strip()

    # Priority 2: scope-based mapping
    scope = (contact_scope or "").lower().strip()
    scope_map = {
        "hotel_specific": hotel_name,
        "chain_area": management_company,
        "management_corporate": management_company,
        "chain_corporate": brand_parent,
        "owner": owner_company,
    }
    if scope in scope_map and scope_map[scope]:
        return scope_map[scope].strip()

    # Priority 3: safe fallback chain — management company tends to
    # be where most non-property-staff actually work
    for candidate in (management_company, hotel_name, brand_parent, owner_company):
        if candidate:
            return candidate.strip()

    return None


async def _post_reveal_and_poll(
    client: httpx.AsyncClient,
    headers: dict,
    reveal_body: dict,
    log_name: str,
) -> Optional[dict]:
    """
    Low-level Wiza flow: POST /individual_reveals + poll until complete.
    Returns the reveal `data` dict on success, None on any failure.
    """
    # ── Step 1: POST to start the reveal ─────────────────
    start_resp = await client.post(
        f"{WIZA_BASE}/individual_reveals",
        headers=headers,
        json=reveal_body,
    )

    if start_resp.status_code == 401:
        logger.error(
            "Wiza: Unauthorized — check WIZA_API_KEY and that API "
            "access is enabled on your account"
        )
        return None
    if start_resp.status_code == 402:
        logger.warning("Wiza: Out of API credits")
        return None
    if start_resp.status_code == 429:
        logger.warning("Wiza: Rate limited on start")
        return None
    if start_resp.status_code not in (200, 201):
        logger.warning(
            f"Wiza start error {start_resp.status_code} for {log_name}: "
            f"{start_resp.text[:200]}"
        )
        return None

    start_data = start_resp.json()
    reveal = start_data.get("data", {}) or {}
    reveal_id = reveal.get("id")
    if not reveal_id:
        logger.warning(
            f"Wiza: no reveal id returned for {log_name}: " f"{str(start_data)[:200]}"
        )
        return None

    # ── Step 2: Poll until complete ──────────────────────
    get_url = f"{WIZA_BASE}/individual_reveals/{reveal_id}"
    elapsed = 0
    reveal_data = None

    while elapsed < POLL_TIMEOUT_SEC:
        await asyncio.sleep(POLL_INTERVAL_SEC)
        elapsed += POLL_INTERVAL_SEC

        poll_resp = await client.get(get_url, headers=headers)
        if poll_resp.status_code != 200:
            logger.warning(f"Wiza poll error {poll_resp.status_code} for {log_name}")
            return None

        payload = poll_resp.json()
        reveal_data = payload.get("data", {}) or {}
        if reveal_data.get("is_complete"):
            return reveal_data

    logger.warning(
        f"Wiza: reveal {reveal_id} did not complete in "
        f"{POLL_TIMEOUT_SEC}s for {log_name}"
    )
    return None


def _extract_email_from_reveal(reveal_data: dict) -> tuple[Optional[str], str]:
    """
    Pull the best email + status out of a completed reveal.
    Returns (email, status). email is None if nothing usable found.
    """
    email = reveal_data.get("email")
    status = (reveal_data.get("email_status") or "").lower()

    # Fallback: check emails[] array (Wiza sometimes uses the array
    # even when the top-level fields are populated)
    if not email:
        emails_arr = reveal_data.get("emails") or []
        if emails_arr:
            first = emails_arr[0] or {}
            email = first.get("email")
            status = (first.get("email_status") or status).lower()

    return email, status


async def enrich_contact_email(
    linkedin_url: Optional[str] = None,
    contact_name: Optional[str] = None,
    name: Optional[str] = None,
    company: Optional[str] = None,
    domain: Optional[str] = None,
) -> Optional[dict]:
    """
    Enrich a contact with their verified work email via Wiza.

    PRIMARY PATH — use LinkedIn URL:
        await enrich_contact_email(
            linkedin_url="https://linkedin.com/in/adambutts1",
            contact_name="Adam Butts",
        )

    FALLBACK PATH — name + company (when LinkedIn is absent):
        await enrich_contact_email(
            name="Adam Butts",
            company="Crescent Hotels & Resorts",
            contact_name="Adam Butts",  # for logging
        )

    FALLBACK PATH — name + domain (best for name+X lookups):
        await enrich_contact_email(
            name="Adam Butts",
            domain="crescenthotels.com",
            contact_name="Adam Butts",
        )

    BEHAVIOR:
      - If linkedin_url is provided, it is ALWAYS used. The name/company/
        domain args are ignored. LinkedIn lookup is a terminal attempt —
        if Wiza returns not_found, we do NOT fall back to name+company
        (that risks finding a different person with the same name).
      - If no linkedin_url, we use name + (company OR domain). Either
        company or domain is required by Wiza when going this route.
      - If we have neither LinkedIn nor (name+company/domain), returns None.

    Args:
        linkedin_url: LinkedIn profile URL (any subdomain OK)
        contact_name: Display name for logging (defaults to `name` or URL)
        name: Full name of the contact (used when linkedin_url is absent)
        company: Company name (used when linkedin_url is absent)
        domain: Company domain (used when linkedin_url is absent)

    Returns:
        dict with keys: email, email_status, confidence, credits_used, match_method
        or None if not found / API not configured / invalid input
    """
    api_key = _get_api_key()
    if not api_key:
        logger.debug("WIZA_API_KEY not set — skipping Wiza enrichment")
        return None

    # ── Decide which input mode to use ──────────────────
    reveal_payload: Optional[dict] = None
    match_method = ""

    if linkedin_url:
        normalized_url = _normalize_linkedin_url(linkedin_url)
        if normalized_url:
            reveal_payload = {"profile_url": normalized_url}
            match_method = "linkedin"
        else:
            logger.debug(f"Invalid LinkedIn URL: {linkedin_url}")
            # Don't fall through to name+company — if caller gave us a
            # URL they intended to use it. Fall-through would imply a
            # name+company fallback they didn't ask for.
            return None
    elif name and (company or domain):
        reveal_payload = {"full_name": name.strip()}
        if domain:
            reveal_payload["domain"] = domain.strip()
            match_method = "name+domain"
        else:
            reveal_payload["company"] = company.strip()
            match_method = "name+company"
    else:
        logger.debug(
            "Wiza: insufficient input — need either linkedin_url OR "
            "(name AND (company OR domain))"
        )
        return None

    log_name = contact_name or name or linkedin_url or "?"
    body = {
        "individual_reveal": reveal_payload,
        "enrichment_level": "partial",  # email only, not phone
        "email_options": {
            "accept_work": True,
            "accept_personal": False,
            "accept_generic": False,
        },
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            reveal_data = await _post_reveal_and_poll(
                client=client,
                headers=headers,
                reveal_body=body,
                log_name=f"{log_name} [{match_method}]",
            )
            if not reveal_data:
                return None

            email, status = _extract_email_from_reveal(reveal_data)

            if not email:
                logger.info(f"Wiza: no email found for {log_name} via {match_method}")
                return None

            if status and status not in VALID_STATUSES:
                logger.info(
                    f"Wiza: low-confidence status '{status}' for {log_name} "
                    f"(email={email}) via {match_method} — skipping"
                )
                return None

            # Credit usage — Wiza charges 2 credits per found email in
            # practice (empirically confirmed 2026-04-22). The API response
            # sometimes populates credits.email_credits with the actual
            # number used; fall back to 2 if absent.
            credits_info = reveal_data.get("credits") or {}
            credits_used = credits_info.get("email_credits") or 2

            logger.info(
                f"Wiza ✓ {log_name} → {email} " f"[{status or 'ok'}] via {match_method}"
            )
            return {
                "email": email,
                "email_status": status or "valid",
                "confidence": _confidence_from_status(status),
                "credits_used": credits_used,
                "match_method": match_method,
            }

    except httpx.TimeoutException:
        logger.warning(f"Wiza timeout for {log_name}")
        return None
    except Exception as e:
        logger.warning(f"Wiza enrichment error for {log_name}: {e}")
        return None


async def check_wiza_credits() -> Optional[dict]:
    """
    Check remaining Wiza credit balance.

    Returns dict with keys: email_credits, phone_credits, api_credits,
    and legacy credits_remaining.
    Returns None if API not configured / error.
    """
    api_key = _get_api_key()
    if not api_key:
        return None

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                f"{WIZA_BASE}/meta/credits",
                headers={"Authorization": f"Bearer {api_key}"},
            )
            if resp.status_code == 401:
                logger.error("Wiza: Unauthorized on credits check — check WIZA_API_KEY")
                return None
            if resp.status_code != 200:
                logger.warning(f"Wiza credits check: HTTP {resp.status_code}")
                return None

            data = resp.json()
            credits = data.get("credits", {}) or {}

            # api_credits can be an int OR a dict {total, email_credits, ...}
            api_credits_raw = credits.get("api_credits")
            if isinstance(api_credits_raw, dict):
                api_credits = api_credits_raw.get("total")
            else:
                api_credits = api_credits_raw

            return {
                "credits_remaining": api_credits,  # legacy key for routes/contacts.py
                "email_credits": credits.get("email_credits"),
                "phone_credits": credits.get("phone_credits"),
                "api_credits": api_credits,
            }
    except Exception as e:
        logger.warning(f"Wiza credit check failed: {e}")
        return None
