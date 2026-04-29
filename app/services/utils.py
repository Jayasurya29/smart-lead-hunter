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

    # FIX: Parse numeric month from ISO-ish formats FIRST.
    # Critical because Gemini returns ISO dates (e.g. "2026-12-18") from
    # the Smart Fill extractor prompt, and without this check the month-name
    # fallback treats them as "no month → default=6 (June)", silently
    # bucketing every December opening as 8 months earlier than reality.
    # Handles: YYYY-MM-DD, YYYY/MM/DD, YYYY.MM.DD, MM-DD-YYYY, MM/DD/YYYY.
    iso_match = re.search(r"20\d{2}[-/\.](\d{1,2})[-/\.]\d{1,2}", text_lower)
    if iso_match:
        m = int(iso_match.group(1))
        if 1 <= m <= 12:
            return m
    us_match = re.search(r"\b(\d{1,2})[-/](\d{1,2})[-/]20\d{2}\b", text_lower)
    if us_match:
        m = int(us_match.group(1))
        if 1 <= m <= 12:
            return m

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


# ─────────────────────────────────────────────────────────────────────────────
# Opening date specificity / regression guard
# ─────────────────────────────────────────────────────────────────────────────
#
# Smart Fill / Full Refresh sometimes returns a less-specific value than what's
# already on the lead ("Late 2026" → "2026"). Without a guard, this regression
# overwrites good data and can incorrectly push leads into the EXPIRED bucket
# (because bare "2026" gets parsed as January 2026 by months_to_opening).
#
# Specificity hierarchy (most → least):
#   5  Full date with day            "2026-09-15", "September 15 2026"
#   4  Month + year                  "September 2026", "Sep 2026"
#   3  Quarter                        "Q3 2026", "Q1 2026"
#   2  Half / season                 "Late 2026", "Spring 2026", "Fall 2026", "H2 2026"
#   1  Year only                     "2026"
#   0  Multi-year / vague / empty    "2026 or 2027", "TBD", ""
#
# Rule (used by enrichment apply paths): new value is accepted only if it's
# at least as specific as current. Year shifted backward without higher
# specificity is rejected as suspicious data.


def opening_date_specificity(value: str | None) -> int:
    """Return 0-5 specificity score for an opening_date string."""
    if not value:
        return 0
    s = value.strip().lower()
    if not s or s in ("tbd", "tba", "none", "unknown", "n/a"):
        return 0

    # Multi-year ranges or "or" lists → vague
    if " or " in s or " - " in s or " through " in s:
        # But a date range like "Sep 15-20 2026" is fine; only count "or" as vague
        if " or " in s:
            return 0

    # ISO-style or with explicit day: 5
    if re.search(r"\b(19|20)\d{2}-\d{1,2}-\d{1,2}\b", s):
        return 5
    if re.search(
        r"\b(jan(uary)?|feb(ruary)?|mar(ch)?|apr(il)?|may|jun(e)?|jul(y)?|aug(ust)?|"
        r"sep(t(ember)?)?|oct(ober)?|nov(ember)?|dec(ember)?)\s+\d{1,2}(st|nd|rd|th)?,?\s*(19|20)\d{2}\b",
        s,
    ):
        return 5
    if re.search(
        r"\b\d{1,2}(st|nd|rd|th)?\s+(jan(uary)?|feb(ruary)?|mar(ch)?|apr(il)?|may|jun(e)?|"
        r"jul(y)?|aug(ust)?|sep(t(ember)?)?|oct(ober)?|nov(ember)?|dec(ember)?)\s+(19|20)\d{2}\b",
        s,
    ):
        return 5

    # Month + year: 4
    if re.search(
        r"\b(jan(uary)?|feb(ruary)?|mar(ch)?|apr(il)?|may|jun(e)?|jul(y)?|aug(ust)?|"
        r"sep(t(ember)?)?|oct(ober)?|nov(ember)?|dec(ember)?)\s*,?\s*(19|20)\d{2}\b",
        s,
    ):
        return 4

    # Quarter + year: 3
    if re.search(r"\bq[1-4]\s*(19|20)\d{2}\b", s):
        return 3

    # Half / season + year: 2
    if re.search(
        r"\b(early|mid(-?year)?|late|first half|second half|h[12]|"
        r"spring|summer|fall|autumn|winter|holiday)\s*,?\s*(19|20)\d{2}\b",
        s,
    ):
        return 2

    # Year only: 1
    if re.search(r"^\s*(19|20)\d{2}\s*$", s):
        return 1
    # Year embedded in something else but no other markers
    if re.search(r"\b(19|20)\d{2}\b", s):
        return 1

    return 0


def _extract_year(value: str | None) -> int | None:
    """Pull the dominant 4-digit year out of a freeform opening_date string."""
    if not value:
        return None
    m = re.search(r"\b((?:19|20)\d{2})\b", value)
    return int(m.group(1)) if m else None


def should_accept_opening_date(
    current: str | None, candidate: str | None
) -> tuple[bool, str]:
    """Decide whether a new opening_date should overwrite the current one.

    Returns (accept: bool, reason: str). The reason is for logging only.

    Rules:
      1. If current is empty → accept anything non-empty.
      2. If candidate is empty → reject (don't blank-out a real value).
      3. If candidate is more specific OR equally specific → accept,
         BUT only if the year hasn't shifted backward (suspicious).
      4. If candidate is less specific → reject (regression).
      5. If years differ and the candidate is less specific → reject.

    The year-shift-backward case is conservative: we accept only when
    the new value carries higher specificity (e.g. correcting "2027"
    → "September 2026" is fine; "2027" → "2026" is rejected).
    """
    cur = (current or "").strip()
    cand = (candidate or "").strip()

    if not cand:
        return False, "candidate is empty"
    if not cur:
        return True, "current is empty, accepting"

    if cand == cur:
        return False, "no change"

    cur_spec = opening_date_specificity(cur)
    cand_spec = opening_date_specificity(cand)

    cur_year = _extract_year(cur)
    cand_year = _extract_year(cand)

    # Year shifted backward — only accept if specificity strictly increases
    if cur_year and cand_year and cand_year < cur_year:
        if cand_spec > cur_spec:
            return True, (
                f"year shifted back ({cur_year}→{cand_year}) but specificity "
                f"increased ({cur_spec}→{cand_spec})"
            )
        return False, (
            f"year shifted back ({cur_year}→{cand_year}) without specificity gain "
            f"({cur_spec}→{cand_spec}) — rejected as suspicious"
        )

    if cand_spec < cur_spec:
        return False, (
            f"specificity regression ({cur_spec}→{cand_spec}): "
            f"{cur!r} is more specific than {cand!r}"
        )

    return True, f"specificity ok ({cur_spec}→{cand_spec})"
