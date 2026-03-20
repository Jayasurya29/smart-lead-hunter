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
# OPENING DATE PARSING — Single source of truth
# ═══════════════════════════════════════════════════════════════

# FIX H-01: Shared month-from-text parser used by BOTH utils (timeline labels)
# and scorer (timing score). Previously divergent: scorer mapped "winter" → Nov,
# utils mapped "winter" → Feb. "Winter 2027" = early 2027, so month 2 is correct.

_MONTH_NAMES = {
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
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "sept": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}

# Quarter/season → representative month
_SEASON_MONTHS = {
    "q1": 2,
    "first quarter": 2,
    "q2": 5,
    "second quarter": 5,
    "spring": 5,
    "q3": 8,
    "third quarter": 8,
    "summer": 8,
    "q4": 11,
    "fourth quarter": 11,
    "fall": 11,
    "autumn": 11,
    # "winter" = Q1 of that year (Jan/Feb), NOT Q4
    "winter": 2,
    # Vague qualifiers
    "early": 3,
    "mid": 6,
    "late": 10,
    "end": 10,
}


def parse_month_from_text(text: str, default: int = 6) -> int:
    """Parse month number from opening date text.

    Single source of truth for month extraction. Used by both
    months_to_opening() and scorer.get_timing_score().

    Returns month number (1-12), or `default` if no month found.
    """
    text_lower = text.lower().strip()

    # Try full/abbreviated month names first
    for name, num in _MONTH_NAMES.items():
        if name in text_lower:
            return num

    # Try quarter/season keywords
    for keyword, month in _SEASON_MONTHS.items():
        if keyword in text_lower:
            return month

    return default


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

    # FIX H-01: Use shared month parser (was inline duplicate)
    month = parse_month_from_text(text, default=6)

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
