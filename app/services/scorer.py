"""
SMART LEAD HUNTER - LEAD SCORING SYSTEM
========================================
Complete scoring system for hotel leads (100 points max)

Last Updated: February 2026

M-07 FIX: Brand matching now uses word-boundary regex for short brand names
(<=4 chars) to prevent false positives. "Trump International" no longer
matches "tru" (Tier 5), "The Glorious Hotel" no longer matches "glo", etc.

SCORING BREAKDOWN:
- Brand Tier:           25 pts (25%) - Quality + uniform variety
- Location:             20 pts (20%) - Your market = your edge
- Timing:               25 pts (25%) - Urgency = action NOW
- Room Count:           15 pts (15%) - Order size potential
- Contact Info:          8 pts (8%)  - Sales efficiency
- New Build:             4 pts (4%)  - Order certainty
- Existing Client:       3 pts (3%)  - Relationship bonus

TOTAL:                 100 pts

SKIP FILTERS (Don't Save):
- Tier 5 Budget brands (Hampton, Holiday Inn, etc.)
- International locations (outside US & Caribbean)
"""

from datetime import datetime
from typing import Dict, Tuple
import re

from app.config.intelligence_config import (
    SCORE_HOT_THRESHOLD,
    SCORE_WARM_THRESHOLD,
    SCORE_COOL_THRESHOLD,
)

# Brand tier lists — single source of truth in canonical_tiers.py.
# Previously 580+ lines of hardcoded brand->tier assignments lived in this
# file, with 116 conflicts against brand_registry.py and the Smart Fill
# prompt's TIER RULES block. Consolidated to app/config/canonical_tiers.py
# which is derived from STR 2024 Chain Scale + CBRE luxury-split research.
# Change tiers in canonical_tiers.py ONLY. Run `python -m scripts.audit_brand_tiers`
# after any change to verify all three sources agree.
from app.config.canonical_tiers import (
    TIER1_ULTRA_LUXURY,
    TIER2_LUXURY,
    TIER3_UPPER_UPSCALE,
    TIER4_UPSCALE,
    TIER5_SKIP,
)

# =============================================================================
# M-07: WORD-BOUNDARY BRAND MATCHING
# =============================================================================

# Threshold: brands with <= this many characters use word-boundary regex
_SHORT_BRAND_THRESHOLD = 4

# Pre-compiled word-boundary patterns for short brands (built lazily)
_brand_patterns: Dict[str, re.Pattern] = {}


def _brand_matches(brand: str, text: str) -> bool:
    """M-07: Match a brand name against hotel text with word-boundary
    awareness for short names.

    For brands <= 4 chars (like "glo", "tru", "riu",
    "royalton",
    "royalton vessence", "w"), uses \\b
    word-boundary regex so "Global Luxury Resort" won't match "glo"
    but "Glo Hotel" will.

    For longer brands (like "equinox hotel",
    "four seasons", "ritz-carlton"), uses
    plain substring matching which is safe and fast.
    """
    stripped = brand.strip()
    if len(stripped) <= _SHORT_BRAND_THRESHOLD:
        if stripped not in _brand_patterns:
            # Escape regex special chars, wrap in word boundaries
            escaped = re.escape(stripped)
            _brand_patterns[stripped] = re.compile(
                r"\b" + escaped + r"\b", re.IGNORECASE
            )
        return bool(_brand_patterns[stripped].search(text))
    else:
        # Both sides lowercased for case-insensitive match
        # (caller typically lowercases text, but be explicit for safety)
        return brand.lower() in text.lower()


# Short-keyword threshold for location matching (Audit Fix M-03 audit)
_LOC_SHORT_THRESHOLD = 3
_loc_patterns: dict = {}


def _location_keyword_matches(keyword: str, text: str) -> bool:
    """Match location keyword with word-boundary awareness for short keywords.

    Prevents 'fl' matching 'buffalo', 'rio' matching 'ontario', etc.
    Short keywords (<= 3 chars) use regex word boundaries.
    Longer keywords use plain substring matching (safe enough).
    """
    stripped = keyword.strip()
    if len(stripped) <= _LOC_SHORT_THRESHOLD:
        if stripped not in _loc_patterns:
            import re

            escaped = re.escape(stripped)
            _loc_patterns[stripped] = re.compile(r"\b" + escaped + r"\b", re.IGNORECASE)
        return bool(_loc_patterns[stripped].search(text))
    else:
        return stripped in text


# =============================================================================
# BRAND TIER CLASSIFICATION (25 pts max)
# =============================================================================
# Tier lists (TIER1_ULTRA_LUXURY ... TIER5_SKIP) are imported at module-top
# from app.config.canonical_tiers. See the import block near the top.


if __name__ == "__main__":
    # Quick stats
    print(f"Tier 1 (Ultra Luxury) : {len(TIER1_ULTRA_LUXURY)} brands")
    print(f"Tier 2 (Luxury)       : {len(TIER2_LUXURY)} brands")
    print(f"Tier 3 (Upper Upscale): {len(TIER3_UPPER_UPSCALE)} brands")
    print(f"Tier 4 (Upscale)      : {len(TIER4_UPSCALE)} brands")
    print(f"Tier 5 (Skip)         : {len(TIER5_SKIP)} brands")
    print(
        f"TOTAL                 : {len(TIER1_ULTRA_LUXURY) + len(TIER2_LUXURY) + len(TIER3_UPPER_UPSCALE) + len(TIER4_UPSCALE) + len(TIER5_SKIP)} brands"
    )

    # Test the gaps from discovery run
    test_brands = [
        "One&Only",
        "Disney Lakeshore Lodge",
        "Shore Club",
        "Crowne Plaza",
        "Le Meridien",
        "Signia by Hilton",
        "Aman",
        "Ritz-Carlton",
        "InterContinental",
        "Auberge",
        "Waldorf Astoria",
        "Wynn",
        "Fontainebleau",
        "Bellagio",
        "Park Hyatt",
        "Half Moon",
        "Curtain Bluff",
    ]
    print("\n--- Test Results ---")
    for name in test_brands:
        name_lower = name.lower()
        found = False
        for tier_num, tier_name, tier_list in [
            (1, "Ultra Luxury", TIER1_ULTRA_LUXURY),
            (2, "Luxury", TIER2_LUXURY),
            (3, "Upper Upscale", TIER3_UPPER_UPSCALE),
            (4, "Upscale", TIER4_UPSCALE),
            (5, "Skip", TIER5_SKIP),
        ]:
            for brand in tier_list:
                if brand in name_lower:
                    print(f"  {name:35s} → Tier {tier_num} ({tier_name})")
                    found = True
                    break
            if found:
                break
        if not found:
            print(f"  {name:35s} → Tier 0 (UNKNOWN)")

# Tier name mapping for database storage
TIER_NAMES = {
    1: "tier1_ultra_luxury",
    2: "tier2_luxury",
    3: "tier3_upper_upscale",
    4: "tier4_upscale",
    5: "tier5_skip",
    0: "unknown",
}


# =============================================================================
# PRE-COMPUTED BRAND LOOKUP (Audit Fix P-01)
# =============================================================================
# O(1) lookup instead of O(n) iteration across 440+ brands.
# Built at module load time from the tier lists above.
_BRAND_TIER_MAP: dict = {}
for _brand in TIER5_SKIP:
    _BRAND_TIER_MAP[_brand] = (5, "Budget/Skip", 0)
for _brand in TIER1_ULTRA_LUXURY:
    _BRAND_TIER_MAP[_brand] = (1, "Ultra Luxury", 25)
for _brand in TIER2_LUXURY:
    _BRAND_TIER_MAP[_brand] = (2, "Luxury", 20)
for _brand in TIER3_UPPER_UPSCALE:
    _BRAND_TIER_MAP[_brand] = (3, "Upper Upscale", 15)
for _brand in TIER4_UPSCALE:
    _BRAND_TIER_MAP[_brand] = (4, "Upscale", 10)


def get_brand_tier(hotel_name: str) -> Tuple[int, str, int]:
    """
    Determine the tier of a hotel based on its name.

    M-07: Uses word-boundary matching for short brand names to prevent
    false positives (e.g. "Trump" no longer matches "tru").

    Returns: (tier_number, tier_name, points)
    """
    name_lower = hotel_name.lower()

    # Audit Fix P-01: Try O(1) exact lookup first, fall back to substring matching
    if name_lower in _BRAND_TIER_MAP:
        return _BRAND_TIER_MAP[name_lower]

    # Substring/word-boundary matching for partial names
    # Check Tier 5 FIRST (to filter out budget hotels)
    for brand in TIER5_SKIP:
        if _brand_matches(brand, name_lower):
            return (5, "Budget/Skip", 0)

    # Check Tier 1 (Ultra Luxury)
    for brand in TIER1_ULTRA_LUXURY:
        if _brand_matches(brand, name_lower):
            return (1, "Ultra Luxury", 25)

    # Check Tier 2 (Luxury)
    for brand in TIER2_LUXURY:
        if _brand_matches(brand, name_lower):
            return (2, "Luxury", 20)

    # Check Tier 3 (Upper Upscale)
    for brand in TIER3_UPPER_UPSCALE:
        if _brand_matches(brand, name_lower):
            return (3, "Upper Upscale", 15)

    # Check Tier 4 (Upscale)
    for brand in TIER4_UPSCALE:
        if _brand_matches(brand, name_lower):
            return (4, "Upscale", 10)

    # Unknown brand
    return (0, "Unknown", 5)


def get_brand_tier_name(tier_num: int) -> str:
    """Get database-friendly tier name"""
    return TIER_NAMES.get(tier_num, "unknown")


def should_skip_brand(hotel_name: str) -> bool:
    """Check if hotel should be filtered out (budget/select tier)"""
    tier, _, _ = get_brand_tier(hotel_name)
    return tier == 5


# =============================================================================
# LOCATION SCORING (20 pts max)
# =============================================================================

# Primary Market - FLORIDA
FLORIDA_KEYWORDS = [
    "florida",
    "fl",
    "miami",
    "miami beach",
    "south beach",
    "brickell",
    "coral gables",
    "coconut grove",
    "orlando",
    "lake buena vista",
    "kissimmee",
    "tampa",
    "st. petersburg",
    "clearwater",
    "jacksonville",
    "ponte vedra",
    "amelia island",
    "fort lauderdale",
    "hollywood fl",
    "dania beach",
    "naples",
    "marco island",
    "bonita springs",
    "key west",
    "key largo",
    "islamorada",
    "marathon fl",
    "florida keys",
    "palm beach",
    "west palm beach",
    "boca raton",
    "delray beach",
    "jupiter",
    "sarasota",
    "longboat key",
    "siesta key",
    "destin",
    "pensacola",
    "panama city beach",
    "fort myers",
    "sanibel",
    "captiva",
    "daytona beach",
    "st. augustine",
    "gainesville fl",
    "tallahassee",
]

# Caribbean - YOUR SECONDARY MARKET
CARIBBEAN_KEYWORDS = [
    # ── US territory postal codes ──
    "pr",  # Puerto Rico
    "vi",  # US Virgin Islands
    # Bahamas
    "bahamas",
    "bs",  # ISO
    "nassau",
    "paradise island",
    "exuma",
    "harbour island",
    "bimini",
    "eleuthera",
    "abaco",
    "grand bahama",
    "freeport bahamas",
    "andros",
    # Cayman
    "cayman islands",
    "grand cayman",
    "cayman",
    "cayman brac",
    "little cayman",
    # NOTE: "KY" intentionally NOT added — conflicts with Kentucky
    # Turks & Caicos
    "turks and caicos",
    "turks & caicos",
    "providenciales",
    "turks",
    "grace bay",
    "parrot cay",
    "ambergris cay",
    "pine cay",
    "north caicos",
    "middle caicos",
    "south caicos",
    "tc",  # ISO
    # Jamaica
    "jamaica",
    "jm",  # ISO
    "montego bay",
    "ocho rios",
    "negril",
    "kingston jamaica",
    "port antonio",
    # Barbados
    "barbados",
    "bb",  # ISO
    "bridgetown",
    "christ church",
    "holetown",
    "st. james barbados",
    "st james barbados",
    # St. Lucia
    "st. lucia",
    "saint lucia",
    "st lucia",
    "lc",  # ISO
    "castries",
    "soufriere",
    # Bermuda
    "bermuda",
    "bm",  # ISO
    "hamilton bermuda",
    # ABC Islands — Aruba
    "aruba",
    "aw",  # ISO
    "oranjestad",
    "palm beach aruba",
    "eagle beach aruba",
    "noord aruba",
    # ABC Islands — Curaçao
    "curacao",
    "curaçao",
    "cw",  # ISO
    "willemstad",
    # ABC Islands — Bonaire
    "bonaire",
    "kralendijk",
    # US Virgin Islands
    "usvi",
    "u.s. virgin islands",
    "us virgin islands",
    "st. thomas",
    "st thomas usvi",
    "st. john usvi",
    "st john usvi",
    "st. croix",
    "st croix",
    # British Virgin Islands
    "bvi",
    "british virgin islands",
    "virgin gorda",
    "tortola",
    "anegada",
    "jost van dyke",
    "vg",  # ISO
    # Anguilla
    "anguilla",
    # Antigua & Barbuda
    "antigua",
    "antigua and barbuda",
    "barbuda",
    "st. john's antigua",
    "ag",  # ISO
    # Dominica (separate from Dominican Republic)
    "dominica",
    "roseau",
    "dm",  # ISO
    # Dominican Republic
    "dominican republic",
    "punta cana",
    "santo domingo",
    "cap cana",
    "la romana",
    # Puerto Rico
    "puerto rico",
    "san juan",
    "dorado",
    "vieques",
    "culebra",
    "rincon",
    # St. Martin / St. Maarten
    "st. martin",
    "saint martin",
    "st. maarten",
    "sint maarten",
    "philipsburg",
    "marigot",
    "sx",  # ISO (Sint Maarten)
    # St. Barts
    "st. barts",
    "st barts",
    "saint barthélemy",
    "saint barthelemy",
    "st. barthelemy",
    "st barthelemy",
    "gustavia",
    # Grenada
    "grenada",
    "st. george's grenada",
    "st georges grenada",
    "carriacou",
    "gd",  # ISO
    # St. Kitts & Nevis
    "st. kitts",
    "saint kitts",
    "nevis",
    "st. kitts and nevis",
    "basseterre",
    "charlestown nevis",
    "kn",  # ISO
    # St. Vincent & the Grenadines
    "st. vincent",
    "saint vincent",
    "grenadines",
    "mustique",
    "canouan",
    "bequia",
    "union island",
    "vc",  # ISO
    # Trinidad & Tobago
    "trinidad",
    "tobago",
    "trinidad and tobago",
    "port of spain",
    "scarborough tobago",
    "tt",  # ISO
    # French Caribbean
    "martinique",
    "fort-de-france",
    "guadeloupe",
    "basse-terre",
    # Generic
    "caribbean",
    "west indies",
    # NOTE: Intentionally skipped as conflict/ambiguity risks:
    #   "ky" → Kentucky; "ms" → Mississippi; "do" → common English word;
    #   "ai" → Anguilla but matches too broadly.
]

# Strong US Markets (existing client presence)
STRONG_US_KEYWORDS = [
    # California
    "california",
    "ca",
    "los angeles",
    "san francisco",
    "san diego",
    "beverly hills",
    "santa monica",
    "malibu",
    "napa",
    "laguna beach",
    "newport beach",
    "la jolla",
    "palm springs",
    "carmel",
    "monterey",
    "hollywood ca",
    # New York
    "new york",
    "ny",
    "manhattan",
    "brooklyn",
    "hamptons",
    "long island",
    "westchester",
    # Texas
    "texas",
    "tx",
    "austin tx",
    "houston",
    "dallas",
    "san antonio",
    "fort worth",
    # Georgia
    "georgia",
    "ga",
    "atlanta",
    "savannah",
    # Tennessee
    "tennessee",
    "tn",
    "nashville",
    "memphis",
    # South Carolina
    "south carolina",
    "sc",
    "charleston sc",
    "myrtle beach",
    "hilton head",
]

# Other US States (lower priority but still US)
OTHER_US_KEYWORDS = [
    # ── Full state names + 2-letter postal codes ──
    "alabama",
    "al",
    "alaska",
    "ak",
    "arizona",
    "az",
    "arkansas",
    "ar",
    "colorado",
    "co",
    "connecticut",
    "ct",
    "delaware",
    "de",
    "hawaii",
    "hi",
    "idaho",
    "id",
    "illinois",
    "il",
    "indiana",
    "in",
    "iowa",
    "ia",
    "kansas",
    "ks",
    "kentucky",
    "ky",
    "louisiana",
    "la",
    "maine",
    "me",
    "maryland",
    "md",
    "massachusetts",
    "ma",
    "michigan",
    "mi",
    "minnesota",
    "mn",
    "mississippi",
    "ms",
    "missouri",
    "mo",
    "montana",
    "mt",
    "nebraska",
    "ne",
    "nevada",
    "nv",
    "new hampshire",
    "nh",
    "new jersey",
    "nj",
    "new mexico",
    "nm",
    "north carolina",
    "nc",
    "north dakota",
    "nd",
    "ohio",
    "oh",
    "oklahoma",
    "ok",
    "oregon",
    "or",
    "pennsylvania",
    "pa",
    "rhode island",
    "ri",
    "south dakota",
    "sd",
    "utah",
    "ut",
    "vermont",
    "vt",
    "virginia",
    "va",
    "washington",
    "wa",
    "west virginia",
    "wv",
    "wisconsin",
    "wi",
    "wyoming",
    "wy",
    # DC variants
    "district of columbia",
    "washington dc",
    "dc",
    "d.c.",
    # Common US cities not in strong markets
    "chicago",
    "boston",
    "seattle",
    "denver",
    "phoenix",
    "las vegas",
    "portland",
    "philadelphia",
    "new orleans",
    "scottsdale",
    "aspen",
    "vail",
    "park city",
    # US indicators
    "usa",
    "united states",
    "u.s.a.",
    "america",
]

# International locations to SKIP
INTERNATIONAL_SKIP = [
    # Canada
    "canada",
    "toronto",
    "vancouver",
    "montreal",
    "calgary",
    "ottawa",
    "whistler",
    "banff",
    "quebec",
    "british columbia",
    "ontario",
    "alberta",
    # Mexico (not Caribbean)
    "mexico",
    "mexico city",
    "cancun",
    "los cabos",
    "cabo san lucas",
    "riviera maya",
    "playa del carmen",
    "tulum",
    "puerto vallarta",
    "guadalajara",
    "monterrey",
    # Europe
    "europe",
    "european",
    "united kingdom",
    "uk",
    "england",
    "london",
    "manchester",
    "scotland",
    "edinburgh",
    "ireland",
    "dublin",
    "france",
    "paris",
    "nice",
    "cannes",
    "monaco",
    "french riviera",
    "italy",
    "rome",
    "milan",
    "milano",
    "florence",
    "venice",
    "tuscany",
    "sardinia",
    "spain",
    "madrid",
    "barcelona",
    "marbella",
    "ibiza",
    "mallorca",
    "germany",
    "berlin",
    "munich",
    "frankfurt",
    "hamburg",
    "switzerland",
    "zurich",
    "geneva",
    "st. moritz",
    "gstaad",
    "austria",
    "vienna",
    "salzburg",
    "portugal",
    "lisbon",
    "porto",
    "algarve",
    "vilamoura",
    "netherlands",
    "amsterdam",
    "holland",
    "belgium",
    "brussels",
    "greece",
    "athens",
    "santorini",
    "mykonos",
    "croatia",
    "dubrovnik",
    "czech republic",
    "prague",
    "hungary",
    "budapest",
    "poland",
    "warsaw",
    "sweden",
    "stockholm",
    "norway",
    "oslo",
    "denmark",
    "copenhagen",
    "finland",
    "helsinki",
    "russia",
    "moscow",
    "st. petersburg russia",
    "turkey",
    "istanbul",
    "bodrum",
    # Asia
    "asia",
    "asian",
    "china",
    "beijing",
    "shanghai",
    "hong kong",
    "shenzhen",
    "guangzhou",
    "dalian",
    "suzhou",
    "chengdu",
    "hangzhou",
    "japan",
    "tokyo",
    "kyoto",
    "osaka",
    "south korea",
    "korea",
    "seoul",
    "busan",
    "singapore",
    "thailand",
    "bangkok",
    "phuket",
    "chiang mai",
    "koh samui",
    "vietnam",
    "hanoi",
    "ho chi minh",
    "nha trang",
    "indonesia",
    "bali",
    "jakarta",
    "malaysia",
    "kuala lumpur",
    "philippines",
    "manila",
    "boracay",
    "india",
    "mumbai",
    "delhi",
    "new delhi",
    "bengaluru",
    "bangalore",
    "goa",
    "jaipur",
    "sri lanka",
    "colombo",
    "maldives",
    "male",
    "cambodia",
    "siem reap",
    "myanmar",
    "burma",
    "taiwan",
    "taipei",
    "kaohsiung",
    # Middle East
    "middle east",
    "makkah",
    "mecca",
    "medina",
    "united arab emirates",
    "uae",
    "dubai",
    "hatta",  # Dubai exclave
    "abu dhabi",
    "saudi arabia",
    "riyadh",
    "jeddah",
    "red sea",
    "qatar",
    "doha",
    "bahrain",
    "manama",
    "oman",
    "muscat",
    "kuwait",
    "israel",
    "tel aviv",
    "jerusalem",
    "jordan",
    "amman",
    "dead sea",
    "lebanon",
    "beirut",
    "egypt",
    "cairo",
    "sharm el sheikh",
    "morocco",
    "marrakech",
    "casablanca",
    # Africa
    "africa",
    "african",
    "south africa",
    "cape town",
    "johannesburg",
    "kenya",
    "nairobi",
    "mombasa",
    "tanzania",
    "zanzibar",
    "serengeti",
    "rwanda",
    "kigali",
    "mauritius",
    "seychelles",
    "botswana",
    "namibia",
    "zimbabwe",
    # Australia & Pacific
    "australia",
    "sydney",
    "melbourne",
    "brisbane",
    "perth",
    "gold coast",
    "new zealand",
    "auckland",
    "queenstown",
    "fiji",
    "french polynesia",
    "bora bora",
    "tahiti",
    # South America
    "south america",
    "brazil",
    "sao paulo",
    "rio de janeiro",
    "rio",
    "argentina",
    "buenos aires",
    "chile",
    "santiago",
    "colombia",
    "bogota",
    "cartagena",
    "peru",
    "lima",
    "cusco",
    "ecuador",
    "quito",
    "uruguay",
    "montevideo",
    # Central America (not Caribbean)
    "costa rica",
    "san jose costa rica",
    "guanacaste",
    "panama",
    "panama city",
    "belize",
    "belize city",
    "guatemala",
    "guatemala city",
    "honduras",
    "nicaragua",
    "el salvador",
]

# Location type mapping for database
LOCATION_TYPES = {
    "florida": "florida",
    "caribbean": "caribbean",
    "strong_us": "usa",
    "other_us": "usa",
    "unknown_us": "usa",
    "international": "international",
}


def get_location_score(
    city: str = None, state: str = None, country: str = None
) -> Tuple[int, str, str]:
    """
    Score location based on market priority.

    Returns: (points, location_tier_description, location_type)
    - Florida: +20 pts, "florida"
    - Caribbean: +15 pts, "caribbean"
    - Strong US Markets: +15 pts, "usa"
    - Other US: +10 pts, "usa"
    - International: SKIP (return -1), "international"

    FIX: Check US/Caribbean BEFORE international keywords.
    Previously, "Rome, Georgia, USA" was wrongly skipped because
    "rome" (an international keyword) was found as a substring before
    the USA country check ran.  Now, if state or country indicates
    USA, we skip the international keyword check entirely.
    """
    # Combine all location fields for matching
    location_parts = [
        str(city or "").lower().strip(),
        str(state or "").lower().strip(),
        str(country or "").lower().strip(),
    ]
    location_text = " ".join(location_parts)
    country_lower = str(country or "").lower().strip()
    state_lower = str(state or "").lower().strip()

    # Empty location - can't determine, assume US
    if not location_text.strip() or location_text.strip() == "none":
        return (10, "Unknown - Assume US", "usa")

    # ── STEP 1: Determine if this is a known US or Caribbean location ──
    # Check country field first to avoid international false positives
    # on US cities with international names (Rome GA, Milan TN, Venice FL, etc.)
    is_us = (
        country_lower in ["usa", "us", "united states", "america", "u.s.a.", "u.s."]
        or state_lower in FLORIDA_KEYWORDS
        or state_lower in STRONG_US_KEYWORDS
        or state_lower in OTHER_US_KEYWORDS
    )

    caribbean_countries = [
        "bahamas",
        "jamaica",
        "barbados",
        "bermuda",
        "aruba",
        "curacao",
        "dominican republic",
        "puerto rico",
        "trinidad and tobago",
        "cayman islands",
        "turks and caicos",
        "st. lucia",
        "antigua",
        "grenada",
        "st. kitts",
        "anguilla",
        "bvi",
        "usvi",
    ]
    is_caribbean = any(cc in country_lower for cc in caribbean_countries)

    # ── STEP 2: If US, score by sub-market (skip international check) ──
    if is_us:
        # Check Florida first (Primary Market - 53% of business)
        for fl_keyword in FLORIDA_KEYWORDS:
            if _location_keyword_matches(fl_keyword, location_text):
                return (20, "Florida", "florida")

        # Check Strong US Markets
        for us_keyword in STRONG_US_KEYWORDS:
            if _location_keyword_matches(us_keyword, location_text):
                return (15, "Strong US Market", "usa")

        # Check Other US States
        for us_keyword in OTHER_US_KEYWORDS:
            if _location_keyword_matches(us_keyword, location_text):
                return (10, "Other US", "usa")

        # USA but unrecognized sub-market
        return (10, "USA (unspecified location)", "usa")

    # ── STEP 3: If Caribbean, score as secondary market ──
    if is_caribbean:
        return (15, "Caribbean", "caribbean")

    for carib_keyword in CARIBBEAN_KEYWORDS:
        if _location_keyword_matches(carib_keyword, location_text):
            return (15, "Caribbean", "caribbean")

    # ── STEP 4: Check international keywords ──
    # Audit Fix H-09: When BOTH state AND country are empty, city alone could
    # be a US city with an international name (Rome GA, Naples FL, Paris TX).
    # BUT — those cities always come with a US state. If state is also empty,
    # the city is the only signal and we CAN safely check it against
    # international keywords. "Dubai" with no state is never Rome, GA.
    #
    # Rule: skip international check only when state is empty AND city is
    # ambiguous (i.e. city alone is a known US city name). Otherwise check.
    state_is_empty = not state_lower or state_lower in ("none", "null", "")
    country_is_empty = not country_lower or country_lower in ("none", "null", "")

    # If we have a state and it's a US state, we already caught it in Step 1/2.
    # If state is empty but city matches an international keyword → reject it.
    # If state is present and not US, also check international keywords.
    if not state_is_empty or not country_is_empty:
        # State or country present — standard check (H-09 safe zone)
        for intl_keyword in INTERNATIONAL_SKIP:
            if _location_keyword_matches(intl_keyword, location_text):
                return (-1, f"International - SKIP ({intl_keyword})", "international")
    else:
        # State AND country both empty — city only. Check city against
        # international keywords. Safe because US cities with foreign names
        # (Rome GA, Paris TX) always have a state field.
        city_lower = str(city or "").lower().strip()
        for intl_keyword in INTERNATIONAL_SKIP:
            if intl_keyword in city_lower:
                return (
                    -1,
                    f"International - SKIP (city: {intl_keyword})",
                    "international",
                )

    # ── STEP 5: Country allowlist gate ──
    # If country is populated and wasn't recognized as US or Caribbean in
    # Step 1, it's international. Clean, definitive reject.
    if country_lower and country_lower not in ("", "none", "null", "unknown", "n/a"):
        return (-1, f"International - SKIP (country: {country})", "international")

    # ── STEP 6: State allowlist gate ──
    # Country is empty/unknown. If state is populated but wasn't recognized
    # as a US state in Step 1 or a Caribbean territory in Step 3's loop,
    # treat as international. This catches non-US provinces/prefectures
    # (Shandong, Ontario, Bavaria, Lombardia, Hokkaido, Guangdong, etc.)
    # that the INTERNATIONAL_SKIP keyword blocklist can't reasonably cover.
    #
    # By the time we reach here:
    #   - Step 1 confirmed state_lower is NOT in any US state list
    #   - Step 3's carib loop confirmed location_text doesn't match any
    #     Caribbean territory keyword
    # So a non-empty, non-placeholder state here is definitionally international.
    if state_lower and state_lower not in ("", "none", "null", "unknown", "n/a", "na"):
        return (
            -1,
            f"International - SKIP (non-US state/region: {state})",
            "international",
        )

    # ── STEP 7: Unknown — give benefit of doubt ──
    # All location fields empty or ambiguous. Assume US to avoid false
    # rejections when extraction is incomplete for an otherwise valid lead.
    return (10, "Unknown - Assume US", "usa")


def should_skip_location(
    city: str = None, state: str = None, country: str = None
) -> bool:
    """Check if location should be filtered out (international)"""
    score, _, _ = get_location_score(city, state, country)
    return score == -1


def is_known_us_or_caribbean_city(city: str = None) -> bool:
    """Check if a city name alone is recognized as a US or Caribbean market.

    Used by lead_factory's hard gate to decide whether city-only extraction
    (no state, no country) is trustworthy. Famous US cities like "Chicago"
    or recognized Caribbean markets like "Gustavia" can pass the gate on
    city alone — unknown foreign cities like "Jinan" cannot.

    Returns True if the city matches any keyword in FLORIDA_KEYWORDS,
    STRONG_US_KEYWORDS, OTHER_US_KEYWORDS, or CARIBBEAN_KEYWORDS.

    Note: this does NOT check INTERNATIONAL_SKIP — callers should rely on
    the full get_location_score() pipeline for international rejection.
    This helper is purely for "is this city in our target market list?"
    """
    if not city:
        return False
    city_lower = str(city).lower().strip()
    if not city_lower or city_lower in ("none", "null", "unknown", "n/a"):
        return False

    for kw_list in (
        FLORIDA_KEYWORDS,
        STRONG_US_KEYWORDS,
        OTHER_US_KEYWORDS,
        CARIBBEAN_KEYWORDS,
    ):
        for kw in kw_list:
            if _location_keyword_matches(kw, city_lower):
                return True
    return False


# =============================================================================
# TIMING SCORING (25 pts max)
# =============================================================================


def get_timing_score(opening_date: str = None) -> Tuple[int, str, int]:
    """
    Score based on opening timing relative to TODAY.

    Rules (as of Feb 2026):
    - Past / already opened    → 0 pts, EXPIRED (should not save)
    - 1-2 months out           → 5 pts, LONG SHOT (probably committed)
    - 3-6 months out           → 25 pts, HOT (actively sourcing uniforms)
    - 7-12 months out          → 18 pts, WARM (in planning phase)
    - 13-24 months out         → 12 pts, PIPELINE (worth tracking)
    - 25+ months out           → 6 pts, EARLY (too far out)

    Returns: (points, timing_tier, year)
    """
    if not opening_date:
        return (4, "Unknown", None)

    date_str = str(opening_date).lower()
    now = datetime.now()
    current_year = now.year
    current_month = now.month

    # Extract year — use max to handle "delayed to 2027" cases
    year_matches = re.findall(r"20\d{2}", date_str)
    if year_matches:
        year = max(int(y) for y in year_matches)
    elif "this year" in date_str:
        year = current_year
    elif "next year" in date_str:
        year = current_year + 1
    else:
        return (4, "Unknown", None)

    # REJECT: past years are already open
    if year < current_year:
        return (0, f"{year} - EXPIRED", year)

    # Parse month from date string
    # FIX H-01: Use shared parser from utils.py (was divergent — scorer mapped
    # "winter" → 11/Nov, utils mapped "winter" → 2/Feb). Now consistent.
    from app.services.utils import parse_month_from_text

    opening_month = parse_month_from_text(date_str, default=6)

    # Calculate months until opening
    months_out = (year - current_year) * 12 + (opening_month - current_month)

    # Score by months out
    if months_out <= 0:
        return (0, f"{year} - EXPIRED", year)
    elif months_out <= 2:
        return (5, f"{year} - Long Shot", year)
    elif months_out <= 6:
        return (25, f"{year} - HOT (sourcing now)", year)
    elif months_out <= 12:
        return (18, f"{year} - Warm (planning)", year)
    elif months_out <= 24:
        return (12, f"{year} - Pipeline", year)
    else:
        return (6, f"{year}+ - Early", year)


# =============================================================================
# ROOM COUNT SCORING (15 pts max)
# =============================================================================


def get_room_count_score(room_count: int = None) -> Tuple[int, str]:
    """
    Score based on number of rooms.

    Returns: (points, size_tier)
    """
    if not room_count:
        return (2, "Unknown")

    try:
        rooms = int(room_count)
    except (ValueError, TypeError):
        if isinstance(room_count, str):
            match = re.search(r"\d+", room_count)
            if match:
                rooms = int(match.group())
            else:
                return (2, "Unknown")
        else:
            return (2, "Unknown")

    if rooms >= 500:
        return (15, "500+ rooms - Mega")
    elif rooms >= 300:
        return (12, "300-499 rooms - Large")
    elif rooms >= 150:
        return (9, "150-299 rooms - Medium-Large")
    elif rooms >= 100:
        return (6, "100-149 rooms - Medium")
    elif rooms >= 50:
        return (4, "50-99 rooms - Boutique")
    else:
        return (2, "<50 rooms - Small")


# =============================================================================
# CONTACT INFO SCORING (8 pts max)
# =============================================================================


def get_contact_score(
    contact_name: str = None, contact_email: str = None, contact_phone: str = None
) -> Tuple[int, str, Dict]:
    """
    Score based on contact information availability.
    """
    points = 0
    breakdown = {}

    # Check for name
    if (
        contact_name
        and str(contact_name).strip()
        and str(contact_name).lower() not in ["none", "unknown", "n/a", ""]
    ):
        points += 3
        breakdown["name"] = 3

    # Check for email
    if contact_email and str(contact_email).strip() and "@" in str(contact_email):
        points += 3
        breakdown["email"] = 3

    # Check for phone
    if (
        contact_phone
        and str(contact_phone).strip()
        and str(contact_phone).lower() not in ["none", "unknown", "n/a", ""]
    ):
        if re.search(r"\d{3,}", str(contact_phone)):
            points += 2
            breakdown["phone"] = 2

    # Determine tier description
    if points == 8:
        tier = "Full Contact - Ready!"
    elif points >= 6:
        tier = "Good Contact Info"
    elif points >= 3:
        tier = "Partial Contact"
    else:
        tier = "No Contact - Research Needed"

    return (points, tier, breakdown)


# =============================================================================
# NEW BUILD SCORING (4 pts max)
# =============================================================================


def get_new_build_score(
    project_type: str = None, description: str = None
) -> Tuple[int, str]:
    """
    Score based on whether it's a new build, conversion, or renovation.
    """
    combined_text = f"{project_type or ''} {description or ''}".lower()

    new_build_keywords = [
        "new build",
        "new construction",
        "ground up",
        "new hotel",
        "new resort",
        "new opening",
        "newly built",
        "brand new",
    ]
    for keyword in new_build_keywords:
        if keyword in combined_text:
            return (4, "New Build")

    conversion_keywords = [
        "conversion",
        "converting",
        "rebranding",
        "rebrand",
        "formerly",
        "was previously",
        "changing to",
        "becoming",
    ]
    for keyword in conversion_keywords:
        if keyword in combined_text:
            return (3, "Brand Conversion")

    renovation_keywords = [
        "renovation",
        "renovating",
        "refurbishment",
        "refresh",
        "remodel",
        "upgrade",
        "restoration",
    ]
    for keyword in renovation_keywords:
        if keyword in combined_text:
            return (2, "Renovation")

    return (1, "Unknown - Assume New")


# =============================================================================
# EXISTING CLIENT RELATIONSHIP (3 pts max)
# =============================================================================

EXISTING_CLIENT_BRANDS = [
    "grand hyatt",
    "graduate",
    "graduate hotels",
    "loews",
    "equinox hotel",
    "four seasons",
    "ritz-carlton",
    "ritz carlton",
    "hilton",
    "kimpton",
    "intercontinental",
    "divi resorts",
    "doubletree",
    "caesars palace",
    "conrad",
    "waldorf",
    "jw marriott",
    "westin",
    "sheraton",
    "sonesta hotel",
    "autograph collection",
    "st. regis",
    "st regis",
    "w hotel",
    "w hotels",
    "andaz",
    "archer hotel",
    "park hyatt",
    "fairmont",
    "mandarin oriental",
]


def get_existing_client_score(
    hotel_name: str = None, brand: str = None
) -> Tuple[int, str]:
    """
    Score based on whether the brand is an existing client.
    """
    combined_text = f"{hotel_name or ''} {brand or ''}".lower()

    for client_brand in EXISTING_CLIENT_BRANDS:
        if _brand_matches(client_brand, combined_text):
            return (3, f"Existing Client - {client_brand.title()}")

    return (0, "No Existing Relationship")


# =============================================================================
# MAIN SCORING FUNCTION
# =============================================================================


def calculate_lead_score(
    hotel_name: str,
    city: str = None,
    state: str = None,
    country: str = None,
    opening_date: str = None,
    room_count: int = None,
    contact_name: str = None,
    contact_email: str = None,
    contact_phone: str = None,
    project_type: str = None,
    description: str = None,
    brand: str = None,
) -> Dict:
    """
    Calculate total lead score and determine if lead should be saved.

    Returns dict with:
    - total_score: int (0-100)
    - should_save: bool
    - skip_reason: str (if should_save is False)
    - score_tier: str (HOT/WARM/COOL/COLD)
    - brand_tier: str (for database)
    - location_type: str (for database)
    - opening_year: int (for database)
    - breakdown: dict with detailed scoring
    """
    result = {
        "total_score": 0,
        "should_save": True,
        "skip_reason": None,
        "score_tier": None,
        "brand_tier": None,
        "location_type": None,
        "opening_year": None,
        "breakdown": {},
    }

    # 1. BRAND TIER (25 pts max)
    brand_tier_num, brand_tier_name, brand_points = get_brand_tier(hotel_name)
    # Also check the brand field if hotel name didn't match
    if brand_tier_num == 0 and brand:
        brand_tier_num, brand_tier_name, brand_points = get_brand_tier(brand)
    result["brand_tier"] = get_brand_tier_name(brand_tier_num)

    if brand_tier_num == 5:
        result["should_save"] = False
        result["skip_reason"] = f"Budget brand: {hotel_name}"
        result["breakdown"]["brand"] = {
            "points": 0,
            "tier": brand_tier_name,
            "skip": True,
        }
        return result

    result["breakdown"]["brand"] = {
        "points": brand_points,
        "tier": brand_tier_name,
        "tier_num": brand_tier_num,
    }
    result["total_score"] += brand_points

    # 2. LOCATION (20 pts max)
    location_points, location_tier, location_type = get_location_score(
        city, state, country
    )

    # ── NAME-BASED INTERNATIONAL CHECK ──────────────────────────────────────
    # If location fields are all empty, the geo filter can't catch international
    # leads. Check the hotel NAME itself against international keywords.
    # e.g. "Park Hyatt Kyoto Gardens", "Langham London", "Le Méridien Paris"
    # would all pass through as "Unknown - Assume US" without this check.
    if location_points >= 0 and not city and not state and not country:
        name_lower = hotel_name.lower()
        for intl_kw in INTERNATIONAL_SKIP:
            if len(intl_kw) >= 4 and intl_kw in name_lower:
                # Make sure it's not a US city with the same name (Paris TX, etc.)
                # by requiring the keyword to NOT be preceded/followed by a US context
                location_points = -1
                location_tier = f"International in name ({intl_kw})"
                location_type = "international"
                break

    result["location_type"] = location_type

    if location_points == -1:
        result["should_save"] = False
        result["skip_reason"] = (
            f"International: {city}, {state}, {country} ({location_tier})"
        )
        result["breakdown"]["location"] = {
            "points": 0,
            "tier": location_tier,
            "skip": True,
        }
        return result

    result["breakdown"]["location"] = {"points": location_points, "tier": location_tier}
    result["total_score"] += location_points

    # 3. TIMING (25 pts max)
    timing_points, timing_tier, opening_year = get_timing_score(opening_date)
    result["opening_year"] = opening_year
    result["breakdown"]["timing"] = {"points": timing_points, "tier": timing_tier}
    # Expired leads (past openings, already opened) — don't save as a "potential lead",
    # but if the hotel is in a US/Caribbean target location, route it to existing_hotels
    # so the sales team can still prospect it (replacement cycles, new hires, etc.).
    if timing_points == 0 and opening_year:
        result["should_save"] = False
        result["skip_reason"] = f"Expired opening ({opening_year}): {hotel_name}"
        result["breakdown"]["timing"]["skip"] = True
        # Flag for downstream routing. lead_factory will read this.
        # Only route if the location scoring already determined this is a valid
        # US/Caribbean target (location_points > 0 means it passed the international filter).
        if location_points > 0:
            result["route_to"] = "existing_hotels"
            result["route_reason"] = (
                f"Already opened ({opening_year}) — routed to existing hotels "
                f"for post-opening prospecting"
            )
        return result
    result["total_score"] += timing_points

    # 4. ROOM COUNT (15 pts max)
    room_points, room_tier = get_room_count_score(room_count)
    result["breakdown"]["rooms"] = {"points": room_points, "tier": room_tier}
    result["total_score"] += room_points

    # 5. CONTACT INFO (8 pts max)
    contact_points, contact_tier, contact_breakdown = get_contact_score(
        contact_name, contact_email, contact_phone
    )
    result["breakdown"]["contact"] = {
        "points": contact_points,
        "tier": contact_tier,
        "detail": contact_breakdown,
    }
    result["total_score"] += contact_points

    # 6. NEW BUILD (4 pts max)
    build_points, build_tier = get_new_build_score(project_type, description)
    result["breakdown"]["new_build"] = {"points": build_points, "tier": build_tier}
    result["total_score"] += build_points

    # 7. EXISTING CLIENT (3 pts max)
    client_points, client_tier = get_existing_client_score(hotel_name, brand)
    result["breakdown"]["existing_client"] = {
        "points": client_points,
        "tier": client_tier,
    }
    result["total_score"] += client_points

    # Determine score tier
    if result["total_score"] >= SCORE_HOT_THRESHOLD:
        result["score_tier"] = "HOT"
    elif result["total_score"] >= SCORE_WARM_THRESHOLD:
        result["score_tier"] = "WARM"
    elif result["total_score"] >= SCORE_COOL_THRESHOLD:
        result["score_tier"] = "COOL"
    else:
        result["score_tier"] = "COLD"

    return result


# =============================================================================
# QUICK SCORE FUNCTION (for test_scrape.py)
# =============================================================================


def quick_score(
    hotel_name: str,
    city: str = None,
    state: str = None,
    country: str = None,
    opening_date: str = None,
    room_count: int = None,
    brand: str = None,
) -> Tuple[int, bool, str, str, str]:
    """
    Quick scoring for scraper - returns essential info only.

    Returns: (score, should_save, skip_reason, brand_tier, location_type)
    """
    result = calculate_lead_score(
        hotel_name=hotel_name,
        city=city,
        state=state,
        country=country,
        opening_date=opening_date,
        room_count=room_count,
        brand=brand,
    )

    return (
        result["total_score"],
        result["should_save"],
        result["skip_reason"],
        result["brand_tier"],
        result["location_type"],
    )


# =============================================================================
# LEAD SCORER CLASS (for scraping_tasks.py compatibility)
# =============================================================================


class LeadScorer:
    """
    Lead scoring class - wrapper around scoring functions.

    Usage:
        scorer = LeadScorer()
        breakdown = scorer.score_with_breakdown(hotel_dict)

        if scorer.is_budget_brand(hotel_dict):
            print("Skip this one")
    """

    def score(self, hotel: Dict) -> int:
        """
        Score a hotel and return just the total score.

        Args:
            hotel: Dict with hotel_name, city, state, country, opening_date, room_count, etc.

        Returns:
            int: Total score (0-100)
        """
        result = self.score_with_breakdown(hotel)
        return result.total

    def score_with_breakdown(self, hotel: Dict) -> "ScoreBreakdown":
        """
        Score a hotel and return detailed breakdown.

        Args:
            hotel: Dict with hotel data

        Returns:
            ScoreBreakdown object with all scoring details
        """
        # Call the main scoring function
        result = calculate_lead_score(
            hotel_name=hotel.get("hotel_name", ""),
            city=hotel.get("city"),
            state=hotel.get("state"),
            country=hotel.get("country"),
            opening_date=hotel.get("opening_date"),
            room_count=hotel.get("room_count"),
            contact_name=hotel.get("contact_name"),
            contact_email=hotel.get("contact_email"),
            contact_phone=hotel.get("contact_phone"),
            project_type=hotel.get("project_type"),
            description=hotel.get("description"),
            brand=hotel.get("brand"),
        )

        # Convert to ScoreBreakdown object
        breakdown = result.get("breakdown", {})

        return ScoreBreakdown(
            total=result.get("total_score", 0),
            should_save=result.get("should_save", True),
            skip_reason=result.get("skip_reason"),
            score_tier=result.get("score_tier"),
            brand_tier=result.get("brand_tier"),
            location_type=result.get("location_type"),
            opening_year=result.get("opening_year"),
            # Component scores
            location=breakdown.get("location", {}).get("points", 0),
            brand=breakdown.get("brand", {}).get("points", 0),
            timing=breakdown.get("timing", {}).get("points", 0),
            room_count=breakdown.get("rooms", {}).get("points", 0),
            contact=breakdown.get("contact", {}).get("points", 0),
            new_build=breakdown.get("new_build", {}).get("points", 0),
            existing_client=breakdown.get("existing_client", {}).get("points", 0),
        )

    def is_budget_brand(self, hotel: Dict) -> bool:
        """Check if hotel is a budget brand (should be skipped)"""
        hotel_name = hotel.get("hotel_name", "")
        return should_skip_brand(hotel_name)

    def should_skip(self, hotel: Dict) -> bool:
        """Check if hotel should be skipped (budget brand or international)"""
        # Check brand
        if self.is_budget_brand(hotel):
            return True

        # Check location
        if should_skip_location(
            hotel.get("city"), hotel.get("state"), hotel.get("country")
        ):
            return True

        return False


class ScoreBreakdown:
    """
    Score breakdown object returned by LeadScorer.score_with_breakdown()
    """

    def __init__(
        self,
        total: int = 0,
        should_save: bool = True,
        skip_reason: str = None,
        score_tier: str = None,
        brand_tier: str = None,
        location_type: str = None,
        opening_year: int = None,
        location: int = 0,
        brand: int = 0,
        timing: int = 0,
        room_count: int = 0,
        contact: int = 0,
        new_build: int = 0,
        existing_client: int = 0,
    ):
        self.total = total
        self.should_save = should_save
        self.skip_reason = skip_reason
        self.score_tier = score_tier
        self.brand_tier = brand_tier
        self.location_type = location_type
        self.opening_year = opening_year
        self.location = location
        self.brand = brand
        self.timing = timing
        self.room_count = room_count
        self.contact = contact
        self.new_build = new_build
        self.existing_client = existing_client

    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON storage"""
        return {
            "total": self.total,
            "should_save": self.should_save,
            "skip_reason": self.skip_reason,
            "score_tier": self.score_tier,
            "brand_tier": self.brand_tier,
            "location_type": self.location_type,
            "opening_year": self.opening_year,
            "location": self.location,
            "brand": self.brand,
            "timing": self.timing,
            "room_count": self.room_count,
            "contact": self.contact,
            "new_build": self.new_build,
            "existing_client": self.existing_client,
        }


# Convenience function for direct import
# Module-level singleton (Audit Fix L-03: avoid creating new instance per call)
_scorer = LeadScorer()


def score_lead(hotel: Dict) -> int:
    """
    Quick function to score a lead.

    Usage:
        from app.services.scorer import score_lead
        score = score_lead(hotel_dict)
    """
    return _scorer.score(hotel)


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================


def format_score_breakdown(result: Dict) -> str:
    """Format score breakdown for display"""
    lines = []
    lines.append(f"{'=' * 60}")
    lines.append(f"TOTAL SCORE: {result['total_score']}/100 [{result['score_tier']}]")
    lines.append(f"{'=' * 60}")

    if not result["should_save"]:
        lines.append(f"!! SKIP: {result['skip_reason']}")
        return "\n".join(lines)

    breakdown = result["breakdown"]

    lines.append(
        f"Brand:     {breakdown['brand']['points']:>2} pts - {breakdown['brand']['tier']}"
    )
    lines.append(
        f"Location:  {breakdown['location']['points']:>2} pts - {breakdown['location']['tier']}"
    )
    lines.append(
        f"Timing:    {breakdown['timing']['points']:>2} pts - {breakdown['timing']['tier']}"
    )
    lines.append(
        f"Rooms:     {breakdown['rooms']['points']:>2} pts - {breakdown['rooms']['tier']}"
    )
    lines.append(
        f"Contact:   {breakdown['contact']['points']:>2} pts - {breakdown['contact']['tier']}"
    )
    lines.append(
        f"Build:     {breakdown['new_build']['points']:>2} pts - {breakdown['new_build']['tier']}"
    )
    lines.append(
        f"Client:    {breakdown['existing_client']['points']:>2} pts - {breakdown['existing_client']['tier']}"
    )

    return "\n".join(lines)


# =============================================================================
# TEST
# =============================================================================

if __name__ == "__main__":
    print("=" * 70)
    print("SCORER TEST")
    print("=" * 70)

    test_cases = [
        # Should SKIP - International
        ("Four Seasons Toronto", "Toronto", None, "Canada"),
        ("Hilton Makkah", "Makkah", None, "Saudi Arabia"),
        ("Hyatt Regency Vilamoura", "Vilamoura", "Algarve", "Portugal"),
        # Should SAVE - Florida
        ("Four Seasons Fort Lauderdale", "Fort Lauderdale", "Florida", "USA"),
        ("Ritz-Carlton Miami Beach", "Miami Beach", "FL", "USA"),
        # Should SAVE - Caribbean
        ("Ritz-Carlton Grand Cayman", "Grand Cayman", None, "Cayman Islands"),
        ("Four Seasons Bahamas", "Nassau", None, "Bahamas"),
        # Should SKIP - Budget
        ("Hampton Inn Orlando", "Orlando", "Florida", "USA"),
        ("Holiday Inn Miami", "Miami", "Florida", "USA"),
        # M-07: FALSE POSITIVE TESTS - these should NOT match budget brands
        ("Trump International Miami", "Miami", "Florida", "USA"),
        ("The Glorious Hotel Miami", "Miami Beach", "FL", "USA"),
        ("Global Luxury Resort Naples", "Naples", "FL", "USA"),
        ("Triumph Hotel Nashville", "Nashville", "TN", "USA"),
        # M-07: TRUE POSITIVE TESTS - these SHOULD match
        ("Tru by Hilton Orlando", "Orlando", "FL", "USA"),
        ("Glo Hotel Downtown", "Tampa", "FL", "USA"),
        ("Riu Palace Cancun", "Cancun", None, "Mexico"),
    ]

    for hotel, city, state, country in test_cases:
        score, should_save, skip_reason, brand_tier, location_type = quick_score(
            hotel_name=hotel,
            city=city,
            state=state,
            country=country,
            opening_date="2026",
        )

        status = "SAVE" if should_save else "SKIP"
        reason = skip_reason or f"Score: {score}"
        print(f"{status:4} | {hotel:40} | {reason}")

    # Test the LeadScorer class
    print("\n" + "=" * 70)
    print("LEADSCORER CLASS TEST")
    print("=" * 70)

    scorer = LeadScorer()
    test_hotel = {
        "hotel_name": "Four Seasons Miami Beach",
        "city": "Miami Beach",
        "state": "Florida",
        "country": "USA",
        "opening_date": "2026",
        "room_count": 200,
        "contact_email": "sales@fourseasons.com",
    }

    breakdown = scorer.score_with_breakdown(test_hotel)
    print(f"Hotel: {test_hotel['hotel_name']}")
    print(f"Total Score: {breakdown.total}")
    print(f"Score Tier: {breakdown.score_tier}")
    print(f"Should Save: {breakdown.should_save}")
    print(f"Brand: {breakdown.brand} pts ({breakdown.brand_tier})")
    print(f"Location: {breakdown.location} pts ({breakdown.location_type})")
    print(f"Timing: {breakdown.timing} pts")
    print(f"Rooms: {breakdown.room_count} pts")
    print(f"Contact: {breakdown.contact} pts")
