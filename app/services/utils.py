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


# State abbreviation to full name mapping
_STATE_ABBR = {
    "AL": "Alabama",
    "AK": "Alaska",
    "AZ": "Arizona",
    "AR": "Arkansas",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DE": "Delaware",
    "FL": "Florida",
    "GA": "Georgia",
    "HI": "Hawaii",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "IA": "Iowa",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "ME": "Maine",
    "MD": "Maryland",
    "MA": "Massachusetts",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MS": "Mississippi",
    "MO": "Missouri",
    "MT": "Montana",
    "NE": "Nebraska",
    "NV": "Nevada",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NY": "New York",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PA": "Pennsylvania",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VT": "Vermont",
    "VA": "Virginia",
    "WA": "Washington",
    "WV": "West Virginia",
    "WI": "Wisconsin",
    "WY": "Wyoming",
    "DC": "District of Columbia",
}


def normalize_state(state: str) -> str:
    """Convert state abbreviation to full name. Passes through if already full."""
    if not state:
        return state
    state = state.strip()
    return _STATE_ABBR.get(state.upper(), state)


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
    # Compound seasons (checked first due to dict ordering)
    "early summer": 6,
    "late summer": 9,
    "early spring": 4,
    "late spring": 5,
    "early fall": 9,
    "late fall": 11,
    "early winter": 1,
    "late winter": 3,
    "mid summer": 7,
    "mid spring": 4,
    "mid fall": 10,
    # Vague qualifiers
    "early": 3,
    "mid": 6,
    # "late 2026" / "end of 2026" = Q4, not October.
    # October alone is "mid-fall". "Late" in a year context means Nov/Dec.
    "late": 12,
    "end": 12,
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

    # Try quarter/season keywords (longest first so "early summer" beats "summer")
    for keyword, month in sorted(_SEASON_MONTHS.items(), key=lambda x: -len(x[0])):
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

    Business rule: uniform sales require 6+ months lead time to win.
    Anything under 3 months is too late = EXPIRED (don't pursue).
    The sweet spot is HOT (6-12 months) when decisions are being made.

    EXPIRED: past opening OR 0-3 months future (too late for sales cycle)
    URGENT:  3-6 months out  (tight but possible)
    HOT:     6-12 months out (sweet spot — active decision window)
    WARM:    12-18 months out (planning phase)
    COOL:    18+ months out  (too early, watchlist)
    TBD:     year only with no month info, ambiguous ranges, or no date
    """
    if not opening_date or not opening_date.strip():
        return "TBD"

    text = opening_date.lower().strip()
    current_year = str(datetime.now().year)

    # "2026 or 2027" style — too vague
    if re.fullmatch(r"20\d{2}\s+or\s+20\d{2}", text):
        return "TBD"

    # Year ranges like "2025-2026", "2026/27", "2026 to 2027" are too vague
    # to bucket reliably. The source didn't commit to a specific window, so
    # neither should we — return TBD instead of guessing a month.
    if re.search(r"20\d{2}\s*[-/]\s*20?\d{2}", text):
        return "TBD"
    if re.search(r"20\d{2}\s+to\s+20\d{2}", text):
        return "TBD"

    # Bare current year only (e.g. "2026") — could be any month
    # Future bare years (2027, 2028) use mid-year estimate
    if re.fullmatch(r"20\d{2}", text) and text == current_year:
        return "TBD"

    months = months_to_opening(opening_date)
    # Business rule: uniform sales cycle requires ~6+ months lead time.
    # Anything under 3 months is too late to win the deal, so it's bucketed
    # as EXPIRED (don't pursue), not URGENT. The "sweet spot" is 6-12 months.
    #
    # Non-overlapping boundaries using strict < so every month lands in
    # exactly one bucket:
    #   months <  3  → EXPIRED  (past or 0, 1, 2 months)
    #   months <  6  → URGENT   (3, 4, 5)
    #   months < 12  → HOT      (6, 7, 8, 9, 10, 11)
    #   months < 18  → WARM     (12, 13, 14, 15, 16, 17)
    #   months ≥ 18  → COOL     (18+)
    if months < 3:
        return "EXPIRED"
    elif months < 6:
        return "URGENT"
    elif months < 12:
        return "HOT"
    elif months < 18:
        return "WARM"
    else:
        return "COOL"
