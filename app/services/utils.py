"""
SMART LEAD HUNTER - Shared Utilities
=====================================
Common functions used across modules to prevent logic divergence.
"""

import re
from datetime import datetime
from zoneinfo import ZoneInfo

LOCAL_TZ = ZoneInfo("America/New_York")


def local_now() -> datetime:
    """Return current local time (Eastern) as timezone-aware datetime."""
    return datetime.now(LOCAL_TZ)


def normalize_hotel_name(name: str) -> str:
    """Normalize hotel name for deduplication.

    Strips special characters, lowercases, and collapses whitespace.

    Used by:
    - orchestrator.py (save_leads_to_database)
    - scraping_tasks.py (_save_lead_impl)
    - Any future dedup logic

    Examples:
        "Ritz-Carlton Miami" → "ritzcarlton miami"
        "Four Seasons® Orlando" → "four seasons orlando"
        "  The St. Regis  " → "the st regis"
    """
    if not name:
        return ""
    # Remove all non-alphanumeric except spaces, then collapse whitespace
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9\s]", "", name.lower())).strip()


def clean_html_to_text(html: str) -> str:
    """Strip HTML to clean text for lead extraction.

    Audit Fix M-10: Centralized from 6+ duplicate implementations.
    Removes script, style, nav, footer, header, noscript, svg, iframe, aside tags.
    """
    import re as _re
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "lxml")
    for tag in soup(
        [
            "script",
            "style",
            "nav",
            "footer",
            "header",
            "noscript",
            "svg",
            "iframe",
            "aside",
        ]
    ):
        tag.decompose()
    text = soup.get_text(separator="\n", strip=True)
    text = _re.sub(r"\n{3,}", "\n\n", text)
    return text
