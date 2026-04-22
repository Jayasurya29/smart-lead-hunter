"""
SMART LEAD HUNTER — Source Tier Classifier
=============================================
Classifies evidence sources by trust tier. Called during evidence capture
to tag each snippet's source URL with a quality badge that sales can rely on.

TRUST HIERARCHY (highest → lowest):
    primary      → Company's own website (commonwealthhotels.com/team,
                   birklainvestment.com). Direct from the source.
    official     → Official press wire (prnewswire.com, businesswire.com,
                   globenewswire.com) OR company newsroom subdomain.
    trade        → Industry trade publication (hotelmanagement.net,
                   hospitalitynet.org, hotelbusiness.com, travelweekly.com).
    aggregator   → Third-party data aggregator (rocketreach.co, zoominfo.com,
                   theorg.com, signalhire.com, lusha.com). Often stale.
    indirect     → LinkedIn posts/comments, personal blogs, secondary
                   mentions where the target is referenced but not the
                   direct subject.
    unknown      → Couldn't classify. Treat as weak evidence.

USAGE:
    from app.services.source_tier import classify_source_tier
    tier = classify_source_tier("https://www.commonwealthhotels.com/our-team")
    # → "primary"
"""

import re
from typing import Optional


# Domains classified as PRIMARY sources (the company's own website).
# This is a partial list — populated lazily as we encounter new management
# companies / owners. For unknown domains, we fall back to heuristics.
_PRIMARY_DOMAINS = {
    # Management companies
    "commonwealthhotels.com",
    "crescenthotels.com",
    "aimbridgehospitality.com",
    "highgate.com",
    "pyramidglobal.com",
    "davidsonhospitality.com",
    "heihotels.com",
    "concordhotels.com",
    "interstatehotels.com",
    # Brand parents
    "hyatt.com",
    "marriott.com",
    "hilton.com",
    "ihg.com",
    "accor.com",
    "fourseasons.com",
    "rosewoodhotels.com",
    "mandarinoriental.com",
    "peninsula.com",
    "aman.com",
    # All-inclusive operators
    "sandals.com",
    "royalton.com",
    "playaresorts.com",
}

# Official press-release wire services
_OFFICIAL_WIRE_DOMAINS = {
    "prnewswire.com",
    "businesswire.com",
    "globenewswire.com",
    "newswire.ca",
    "apnews.com",
    "prweb.com",
}

# Industry trade publications
_TRADE_PUBLICATION_DOMAINS = {
    "hotelmanagement.net",
    "hospitalitynet.org",
    "hotelbusiness.com",
    "hotel-online.com",
    "hotelexecutive.com",
    "travelweekly.com",
    "skift.com",
    "lodgingmagazine.com",
    "hotelsmag.com",
    "hotelnewsresource.com",
    "hotelier.ca",
    "hospitalitydesign.com",
    "hotelnewsnow.com",
    "ehotelier.com",
    "travelmarketreport.com",
    "travelandtourworld.com",
    "bloomberg.com",
    "forbes.com",
    "wsj.com",
    "reuters.com",
    "cnbc.com",
    "finance.yahoo.com",
}

# Third-party data aggregators — treat with skepticism, often stale
_AGGREGATOR_DOMAINS = {
    "rocketreach.co",
    "zoominfo.com",
    "theorg.com",
    "signalhire.com",
    "lusha.com",
    "contactout.com",
    "apollo.io",
    "crunchbase.com",
    "leadiq.com",
    "cognism.com",
    "seamless.ai",
    "datanyze.com",
    "slintel.com",
    "comparably.com",
    "spokeo.com",
    "clustrmaps.com",
    "corporationwiki.com",
}

# LinkedIn subdomains — treated specially (see logic below)
_LINKEDIN_DOMAINS = {
    "linkedin.com",
    "www.linkedin.com",
}


def _extract_domain(url: str) -> str:
    """Extract the registrable domain from a URL (drops protocol + www + path)."""
    if not url:
        return ""
    url = url.strip().lower()
    # Strip protocol
    if "://" in url:
        url = url.split("://", 1)[1]
    # Take host portion only
    host = url.split("/", 1)[0]
    # Strip common subdomain prefixes for lookup
    for prefix in ("www.", "newsroom.", "press.", "news.", "investor."):
        if host.startswith(prefix):
            host = host[len(prefix) :]
            break
    return host


def _is_company_newsroom(url: str, known_company_domain: Optional[str] = None) -> bool:
    """
    Check if URL is a company newsroom subdomain. e.g. newsroom.hyatt.com,
    press.marriott.com — these are treated as PRIMARY because they're the
    company publishing about itself directly.
    """
    url_lower = (url or "").lower()
    for prefix in ("newsroom.", "press.", "news.", "investor."):
        if f"://{prefix}" in url_lower or f".{prefix}" in url_lower:
            return True
    # If the full URL contains a known primary domain in its host AND
    # has "newsroom" or "press" in the path, treat as primary too.
    if known_company_domain and known_company_domain in url_lower:
        if "/newsroom" in url_lower or "/press" in url_lower:
            return True
    return False


def classify_source_tier(
    url: Optional[str],
    source_type: Optional[str] = None,
) -> str:
    """
    Classify a source URL into one of:
      primary | official | trade | aggregator | indirect | unknown

    Args:
        url: The source URL.
        source_type: Optional hint about how the contact was extracted
                     (e.g. "linkedin_snippet", "press_release", "snippet").
                     Used as a secondary signal when the domain is ambiguous.

    Examples:
        >>> classify_source_tier("https://www.commonwealthhotels.com/our-team")
        'primary'
        >>> classify_source_tier("https://www.prnewswire.com/news-releases/...")
        'official'
        >>> classify_source_tier("https://rocketreach.co/jane-doe-email_12345")
        'aggregator'
        >>> classify_source_tier("https://www.linkedin.com/in/jane-doe")
        'official'  # LinkedIn profile = official-tier identity
        >>> classify_source_tier("https://www.linkedin.com/posts/someone_activity_...")
        'indirect'  # LinkedIn post by someone else = indirect mention
    """
    if not url:
        return "unknown"

    url_lower = url.strip().lower()
    domain = _extract_domain(url)

    # ── Primary: company's own website ──
    if domain in _PRIMARY_DOMAINS:
        return "primary"

    # Company newsroom subdomain of a known primary
    if _is_company_newsroom(url):
        return "primary"

    # ── Official: press wire services ──
    if domain in _OFFICIAL_WIRE_DOMAINS:
        return "official"

    # ── LinkedIn handling ──
    # LinkedIn profiles (/in/) = official identity source
    # LinkedIn posts (/posts/) = indirect (post by someone else)
    # LinkedIn jobs (/jobs/) = indirect (recruitment activity)
    if domain in _LINKEDIN_DOMAINS or domain.endswith(".linkedin.com"):
        if "/in/" in url_lower:
            return "official"
        if "/posts/" in url_lower or "/pulse/" in url_lower:
            return "indirect"
        if "/jobs/" in url_lower:
            return "indirect"
        if "/company/" in url_lower:
            return "official"
        return "indirect"

    # ── Aggregator: third-party data brokers ──
    if domain in _AGGREGATOR_DOMAINS:
        return "aggregator"

    # ── Trade publications ──
    if domain in _TRADE_PUBLICATION_DOMAINS:
        return "trade"

    # ── Heuristic: if the URL path looks like a press release, treat as trade ──
    if any(
        marker in url_lower
        for marker in (
            "/press-release/",
            "/press/",
            "/news/",
            "/announcement/",
            "/press_release/",
        )
    ):
        return "trade"

    # ── Default ──
    return "unknown"


def trust_score(tier: str) -> int:
    """
    Numeric ordering for trust tiers. Higher = more trustworthy.
    Used when sorting multiple evidence items by strength.
    """
    return {
        "primary": 5,
        "official": 4,
        "trade": 3,
        "aggregator": 2,
        "indirect": 1,
        "unknown": 0,
    }.get(tier, 0)


# ═══════════════════════════════════════════════════════════════
# EVIDENCE DATE PARSING
# ═══════════════════════════════════════════════════════════════


def extract_year_from_url_or_snippet(url: str = "", snippet: str = "") -> Optional[int]:
    """
    Best-effort extraction of year from URL path or snippet text.
    Used to detect stale evidence (>18 months old).

    Examples:
        extract_year(".../2021/03/appointment-...")  → 2021
        extract_year("", "COVINGTON, Ky., March 3, 2021 /PRNewswire/")  → 2021
    """
    # Try URL first — press releases often have year in path
    # e.g. /news/2024/07/..., /press-releases/2021/announcement-...
    if url:
        m = re.search(r"/(19\d{2}|20\d{2})/", url)
        if m:
            return int(m.group(1))

    # Try snippet — press release format often has "Month Day, YEAR" early
    if snippet:
        # Match patterns like "March 3, 2021", "Sept. 2023", "2024"
        year_patterns = [
            r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z.]*\s+\d{1,2},?\s+((?:19|20)\d{2})\b",
            r"\b((?:19|20)\d{2})\b",  # last resort, any 4-digit year
        ]
        for pat in year_patterns:
            m = re.search(pat, snippet, flags=re.IGNORECASE)
            if m:
                try:
                    year = int(m.group(1))
                    # Sanity check: accept only realistic ranges
                    if 2000 <= year <= 2030:
                        return year
                except (ValueError, IndexError):
                    continue

    return None


def is_stale(
    year: Optional[int], current_year: int = 2026, months_threshold: int = 18
) -> bool:
    """
    Return True if source year is older than `months_threshold` months
    from `current_year`.  >= 18 months old by default is stale.
    """
    if year is None:
        return False  # unknown age — don't penalize
    # Rough month conversion: 18mo = 1.5 years
    years_old = current_year - year
    return years_old >= (months_threshold / 12)
