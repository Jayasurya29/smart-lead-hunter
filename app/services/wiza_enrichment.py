"""
LEAD GENERATOR — Wiza Email Enrichment
========================================
Finds verified work email addresses for hotel contacts using the Wiza API.

HOW IT WORKS:
  - Input: LinkedIn profile URL (e.g. linkedin.com/in/john-smith-gm)
  - Wiza looks up the profile and returns the person's work email
  - Credits only consumed when an email is successfully found ($0.05/email)
  - Email statuses: "verified" > "guessed" > "notfound"

SETUP:
  1. Add WIZA_API_KEY to your .env (get from wiza.co/app/settings/api)
  2. Purchase credits at wiza.co ($50 = 2,000 credits)

API DOCS: https://wiza.co/api-docs
"""

import logging
import os
import re
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

WIZA_BASE = "https://wiza.co/api"

# Email statuses we consider good enough to save
VALID_STATUSES = {"verified", "guessed", "accept_all"}


def _get_api_key() -> Optional[str]:
    key = os.getenv("WIZA_API_KEY", "").strip()
    if not key or key == "your-wiza-api-key-here":
        return None
    return key


def _normalize_linkedin_url(url: str) -> Optional[str]:
    """
    Normalize LinkedIn URL to the format Wiza expects.
    e.g. linkedin.com/in/john-smith → https://www.linkedin.com/in/john-smith
    """
    if not url:
        return None
    url = url.strip()
    # Extract the /in/ path
    m = re.search(r"linkedin\.com/in/([^/?&#\s]+)", url, re.IGNORECASE)
    if not m:
        return None
    profile_id = m.group(1).rstrip("/")
    return f"https://www.linkedin.com/in/{profile_id}"


async def enrich_contact_email(
    linkedin_url: str,
    contact_name: Optional[str] = None,
) -> Optional[dict]:
    """
    Enrich a contact with their verified work email via Wiza.

    Args:
        linkedin_url: LinkedIn profile URL
        contact_name: Contact's name (for logging only)

    Returns:
        dict with keys: email, email_status, confidence, credits_used
        or None if not found / API not configured
    """
    api_key = _get_api_key()
    if not api_key:
        logger.debug("WIZA_API_KEY not set — skipping Wiza enrichment")
        return None

    normalized_url = _normalize_linkedin_url(linkedin_url)
    if not normalized_url:
        logger.debug(f"Invalid LinkedIn URL: {linkedin_url}")
        return None

    log_name = contact_name or linkedin_url

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                f"{WIZA_BASE}/enrichments",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json={"profile_url": normalized_url},
            )

            if resp.status_code == 402:
                logger.warning("Wiza: Out of credits — purchase more at wiza.co")
                return None

            if resp.status_code == 429:
                logger.warning("Wiza: Rate limited")
                return None

            if resp.status_code not in (200, 201):
                logger.warning(f"Wiza API error {resp.status_code} for {log_name}")
                return None

            data = resp.json()

            # Wiza response shape: {"data": {"email": "...", "email_status": "verified", ...}}
            enriched = data.get("data") or data.get("result") or data
            email = enriched.get("email") or enriched.get("work_email")
            status = enriched.get("email_status") or enriched.get("status", "")

            if not email or status == "notfound":
                logger.info(f"Wiza: No email found for {log_name}")
                return None

            if status not in VALID_STATUSES:
                logger.info(
                    f"Wiza: Low-confidence status '{status}' for {log_name} — skipping"
                )
                return None

            confidence = (
                "high"
                if status == "verified"
                else "medium"
                if status in ("guessed", "accept_all")
                else "low"
            )

            logger.info(f"Wiza ✓ {log_name} → {email} [{status}]")
            return {
                "email": email,
                "email_status": status,
                "confidence": confidence,
                "credits_used": 1,
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
    Returns dict with credits_remaining or None if API not configured.
    """
    api_key = _get_api_key()
    if not api_key:
        return None

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                f"{WIZA_BASE}/account",
                headers={"Authorization": f"Bearer {api_key}"},
            )
            if resp.status_code != 200:
                return None
            data = resp.json()
            credits = (
                data.get("credits_remaining")
                or data.get("data", {}).get("credits_remaining")
                or data.get("credits")
            )
            return {"credits_remaining": credits}
    except Exception as e:
        logger.warning(f"Wiza credit check failed: {e}")
        return None
