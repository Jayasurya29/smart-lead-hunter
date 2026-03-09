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


# ═══════════════════════════════════════════════════════════════
# OPENING DATE TIMELINE
# ═══════════════════════════════════════════════════════════════


def months_to_opening(opening_date: str) -> int:
    """Parse opening date text into approximate months from now.
    Returns 99 if unknown, negative if past."""
    if not opening_date:
        return 99

    text = opening_date.lower().strip()
    now = datetime.now()

    # Extract year
    year_match = re.search(r"20\d{2}", text)
    # Handle "2026/27" format — use later year
    dual_year = re.search(r"(20\d{2})/(20)?\d{2}", text)
    if dual_year:
        base = int(dual_year.group(1))
        year = base + 1
    elif year_match:
        year = int(year_match.group())
    else:
        return 99

    # Extract month
    month = None
    month_map = {
        "january": 1,
        "february": 2,
        "march": 3,
        "april": 4,
        "may": 5,
        "june": 6,
        "july": 7,
        "august": 8,
        "september": 9,
        "october": 10,
        "november": 11,
        "december": 12,
    }
    for name, num in month_map.items():
        if name in text:
            month = num
            break

    if not month:
        if "q1" in text or "early" in text or "winter" in text:
            month = 2
        elif "q2" in text or "spring" in text:
            month = 5
        elif "q3" in text or "summer" in text or "mid" in text:
            month = 7
        elif "q4" in text or "fall" in text or "autumn" in text or "late" in text:
            month = 10
        else:
            month = 6

    return (year - now.year) * 12 + (month - now.month)


def get_timeline_label(opening_date: str) -> str:
    """Get timeline label for a lead based on opening date.

    EXPIRED: already past opening
    LATE: 0-3 months out
    URGENT: 3-6 months out
    HOT: 6-12 months out (sweet spot for uniform sales)
    WARM: 12-18 months out
    COOL: 18+ months out
    TBD: current year with no month info, or no date at all
    """
    if not opening_date or not opening_date.strip():
        return "TBD"

    text = opening_date.lower().strip()
    current_year = str(datetime.now().year)

    # "2026 or 2027" style — too vague
    if re.fullmatch(r"20\d{2}\s+or\s+20\d{2}", text):
        return "TBD"

    # Bare current year only (e.g. "2026") — could be any month
    # Future bare years (2027, 2028) use mid-year estimate
    if re.fullmatch(r"20\d{2}", text) and text == current_year:
        return "TBD"

    months = months_to_opening(opening_date)
    if months < 0:
        return "EXPIRED"
    elif months <= 3:
        return "LATE"
    elif months <= 6:
        return "URGENT"
    elif months <= 12:
        return "HOT"
    elif months <= 18:
        return "WARM"
    else:
        return "COOL"
