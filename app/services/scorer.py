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
from typing import Optional, Dict, Tuple, List
import re


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

    For brands <= 4 chars (like "glo", "tru", "riu", "w"), uses \\b
    word-boundary regex so "Global Luxury Resort" won't match "glo"
    but "Glo Hotel" will.

    For longer brands (like "four seasons", "ritz-carlton"), uses
    plain substring matching which is safe and fast.
    """
    stripped = brand.strip()
    if len(stripped) <= _SHORT_BRAND_THRESHOLD:
        if stripped not in _brand_patterns:
            # Escape regex special chars, wrap in word boundaries
            escaped = re.escape(stripped)
            _brand_patterns[stripped] = re.compile(
                r'\b' + escaped + r'\b', re.IGNORECASE
            )
        return bool(_brand_patterns[stripped].search(text))
    else:
        # Both sides lowercased for case-insensitive match
        # (caller typically lowercases text, but be explicit for safety)
        return brand.lower() in text.lower()


# =============================================================================
# BRAND TIER CLASSIFICATION (25 pts max)
# =============================================================================

TIER1_ULTRA_LUXURY = [
    "alila", "aman", "amangiri", "amanera", "amanyara", "armani hotel",
    "banyan tree", "belmond", "bulgari",
    "capella", "cheval blanc",
    "dorchester collection",
    "eden rock",
    "faena",
    "jade mountain",
    "leading hotels of the world",
    "mandarin oriental", "miraval",
    "nikki beach resort",
    "oberoi", "oetker collection", "one&only", "one & only",
    "peninsula", "preferred hotels",
    "raffles", "rocco forte", "rosewood",
    "six senses", "small luxury hotels",
    "the leading hotels",
    "viceroy",
]

TIER2_LUXURY = [
    "acqualina", "auberge",
    "conrad",
    "four seasons",
    "grand hyatt",
    "langham", "le blanc", "loews", "lxr",
    "montage",
    "nobu hotel", "nomad",
    "park hyatt",
    "regent", "ritz-carlton", "ritz carlton", "ritz-carlton reserve",
    "st. regis", "st regis",
    "the luxury collection", "luxury collection",
    "waldorf astoria", "waldorf-astoria",
    "zoetry",
]

TIER3_UPPER_UPSCALE = [
    "1 hotel", "1hotel",
    "ace hotel", "andaz", "autograph collection",
    "canopy by hilton", "canopy", "curio collection", "curio",
    "delano", "destination by hyatt", "dream hotel",
    "edition", "embassy suites",
    "fairmont",
    "graduate hotels", "graduate austin", "graduate", "grand wailea",
    "hard rock hotel", "hotel indigo", "hyatt centric", "hyatt regency",
    "intercontinental", "impression by secrets",
    "jw marriott",
    "kimpton",
    "le meridien",
    "live aqua",
    "mgallery", "mr. c",
    "newbury boston",
    "omni",
    "pendry", "proper hotel",
    "renaissance",
    "secrets resorts", "secrets", "signia", "sls", "sofitel", "swissotel",
    "tapestry collection", "tapestry", "tempo by hilton", "the standard",
    "thompson", "tribute portfolio",
    "unbound collection", "unico",
    "vignette collection", "virgin hotels",
    "w hotel", "w hotels", "w miami", "w south beach", "w fort lauderdale",
    "w new york", "w los angeles", "w hollywood", "w chicago", "w austin",
    "zemi", "hyatt zilara", "hyatt ziva", "zilara", "ziva",
]

TIER4_UPSCALE = [
    "breathless resorts", "breathless",
    "crowne plaza",
    "delta hotels", "doubletree", "dreams resorts", "dreams",
    "el san juan hotel", "even hotels",
    "gaylord",
    "hilton hotel", "hilton hotels", "hilton miami", "hilton orlando",
    "hilton fort lauderdale", "hilton los angeles", "hilton new york",
    "hilton chicago", "hilton san", "hilton bay", "hilton resort", "hilton beach",
    "hyatt",
    "marriott hotel", "marriott hotels", "marriott",
    "novotel",
    "pullman",
    "riu palace", "riu",
    "sandals", "sheraton", "sonesta", "sunscape resorts", "sunscape",
    "voco",
    "westin", "wyndham grand",
    "bahia principe", "barcelo", "iberostar", "karisma",
    "excellence resorts", "excellence", "palace resorts", "paradisus",
]

TIER5_SKIP = [
    "ac hotel", "ac hotels", "aloft", "americinn", "avid hotels", "atwell suites",
    "baymont", "best western",
    "cambria", "candlewood", "caption by hyatt", "clarion", "club med",
    "comfort inn", "comfort suites", "country inn", "courtyard",
    "days inn", "drury",
    "econo lodge", "element", "extended stay",
    "fairfield", "four points",
    "garner", "glo",
    "hampton", "hampton inn", "hawthorn", "hilton garden inn",
    "holiday inn", "holiday inn express", "home2", "homewood",
    "hyatt house", "hyatt place",
    "la quinta", "livsmart",
    "microtel", "motel 6", "motto", "moxy",
    "park inn", "protea",
    "quality inn",
    "radisson", "ramada", "red lion", "red roof", "residence inn", "rodeway",
    "sleep inn", "spark by hilton", "springhill", "staybridge", "studiores", "super 8",
    "towneplace", "tru by hilton", "tru",
    "wingate", "woodspring", "wyndham",
]

# Tier name mapping for database storage
TIER_NAMES = {
    1: "tier1_ultra_luxury",
    2: "tier2_luxury",
    3: "tier3_upper_upscale",
    4: "tier4_upscale",
    5: "tier5_skip",
    0: "unknown"
}


def get_brand_tier(hotel_name: str) -> Tuple[int, str, int]:
    """
    Determine the tier of a hotel based on its name.

    M-07: Uses word-boundary matching for short brand names to prevent
    false positives (e.g. "Trump" no longer matches "tru").

    Returns: (tier_number, tier_name, points)
    """
    name_lower = hotel_name.lower()

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
    "florida", "fl",
    "miami", "miami beach", "south beach", "brickell", "coral gables", "coconut grove",
    "orlando", "lake buena vista", "kissimmee",
    "tampa", "st. petersburg", "clearwater",
    "jacksonville", "ponte vedra", "amelia island",
    "fort lauderdale", "hollywood fl", "dania beach",
    "naples", "marco island", "bonita springs",
    "key west", "key largo", "islamorada", "marathon fl", "florida keys",
    "palm beach", "west palm beach", "boca raton", "delray beach", "jupiter",
    "sarasota", "longboat key", "siesta key",
    "destin", "pensacola", "panama city beach",
    "fort myers", "sanibel", "captiva",
    "daytona beach", "st. augustine",
    "gainesville fl", "tallahassee",
]

# Caribbean - YOUR SECONDARY MARKET
CARIBBEAN_KEYWORDS = [
    # Bahamas
    "bahamas", "nassau", "paradise island", "exuma", "harbour island", "bimini", "eleuthera",
    # Cayman
    "cayman islands", "grand cayman", "cayman",
    # Turks & Caicos
    "turks and caicos", "turks & caicos", "providenciales", "turks", "grace bay",
    # Jamaica
    "jamaica", "montego bay", "ocho rios", "negril", "kingston jamaica",
    # Barbados
    "barbados", "bridgetown",
    # St. Lucia
    "st. lucia", "saint lucia", "st lucia",
    # Bermuda
    "bermuda",
    # ABC Islands
    "aruba", "curacao", "bonaire",
    # US Virgin Islands
    "usvi", "u.s. virgin islands", "st. thomas", "st. john usvi", "st. croix",
    # British Virgin Islands
    "bvi", "british virgin islands", "virgin gorda", "tortola",
    # Other Caribbean
    "anguilla",
    "antigua", "antigua and barbuda",
    "dominican republic", "punta cana", "santo domingo", "cap cana", "la romana",
    "puerto rico", "san juan", "dorado", "vieques", "culebra",
    "st. martin", "saint martin", "st. maarten", "sint maarten",
    "grenada", "st. george's grenada",
    "st. kitts", "saint kitts", "nevis", "st. kitts and nevis",
    "trinidad", "tobago", "trinidad and tobago",
    "martinique", "guadeloupe",
    "caribbean",
]

# Strong US Markets (existing client presence)
STRONG_US_KEYWORDS = [
    # California
    "california", "los angeles", "san francisco", "san diego", "beverly hills",
    "santa monica", "malibu", "napa", "laguna beach", "newport beach", "la jolla",
    "palm springs", "carmel", "monterey", "hollywood ca",
    # New York
    "new york", "manhattan", "brooklyn", "hamptons", "long island", "westchester",
    # Texas
    "texas", "austin tx", "houston", "dallas", "san antonio", "fort worth",
    # Georgia
    "georgia", "atlanta", "savannah",
    # Tennessee
    "tennessee", "nashville", "memphis",
    # South Carolina
    "south carolina", "charleston sc", "myrtle beach", "hilton head",
]

# Other US States (lower priority but still US)
OTHER_US_KEYWORDS = [
    "alabama", "alaska", "arizona", "arkansas", "colorado", "connecticut",
    "delaware", "hawaii", "idaho", "illinois", "indiana", "iowa", "kansas",
    "kentucky", "louisiana", "maine", "maryland", "massachusetts", "michigan",
    "minnesota", "mississippi", "missouri", "montana", "nebraska", "nevada",
    "new hampshire", "new jersey", "new mexico", "north carolina", "north dakota",
    "ohio", "oklahoma", "oregon", "pennsylvania", "rhode island",
    "south dakota", "utah", "vermont", "virginia", "washington", "west virginia",
    "wisconsin", "wyoming", "district of columbia", "washington dc", "d.c.",
    # Common US cities not in strong markets
    "chicago", "boston", "seattle", "denver", "phoenix", "las vegas", "portland",
    "philadelphia", "new orleans", "scottsdale", "aspen", "vail", "park city",
    # US indicators
    "usa", "united states", "u.s.a.", "america",
]

# International locations to SKIP
INTERNATIONAL_SKIP = [
    # Canada
    "canada", "toronto", "vancouver", "montreal", "calgary", "ottawa", "whistler",
    "banff", "quebec", "british columbia", "ontario", "alberta",

    # Mexico (not Caribbean)
    "mexico", "mexico city", "cancun", "los cabos", "cabo san lucas", "riviera maya",
    "playa del carmen", "tulum", "puerto vallarta", "guadalajara", "monterrey",

    # Europe
    "europe", "european",
    "united kingdom", "uk", "england", "london", "manchester", "scotland", "edinburgh",
    "ireland", "dublin",
    "france", "paris", "nice", "cannes", "monaco", "french riviera",
    "italy", "rome", "milan", "milano", "florence", "venice", "tuscany", "sardinia",
    "spain", "madrid", "barcelona", "marbella", "ibiza", "mallorca",
    "germany", "berlin", "munich", "frankfurt", "hamburg",
    "switzerland", "zurich", "geneva", "st. moritz", "gstaad",
    "austria", "vienna", "salzburg",
    "portugal", "lisbon", "porto", "algarve", "vilamoura",
    "netherlands", "amsterdam", "holland",
    "belgium", "brussels",
    "greece", "athens", "santorini", "mykonos",
    "croatia", "dubrovnik",
    "czech republic", "prague",
    "hungary", "budapest",
    "poland", "warsaw",
    "sweden", "stockholm",
    "norway", "oslo",
    "denmark", "copenhagen",
    "finland", "helsinki",
    "russia", "moscow", "st. petersburg russia",
    "turkey", "istanbul", "bodrum",

    # Asia
    "asia", "asian",
    "china", "beijing", "shanghai", "hong kong", "shenzhen", "guangzhou", "dalian",
    "suzhou", "chengdu", "hangzhou",
    "japan", "tokyo", "kyoto", "osaka",
    "south korea", "korea", "seoul", "busan",
    "singapore",
    "thailand", "bangkok", "phuket", "chiang mai", "koh samui",
    "vietnam", "hanoi", "ho chi minh", "nha trang",
    "indonesia", "bali", "jakarta",
    "malaysia", "kuala lumpur",
    "philippines", "manila", "boracay",
    "india", "mumbai", "delhi", "new delhi", "bengaluru", "bangalore", "goa", "jaipur",
    "sri lanka", "colombo",
    "maldives", "male",
    "cambodia", "siem reap",
    "myanmar", "burma",
    "taiwan", "taipei", "kaohsiung",

    # Middle East
    "middle east", "makkah", "mecca", "medina",
    "united arab emirates", "uae", "dubai", "abu dhabi",
    "saudi arabia", "riyadh", "jeddah", "red sea",
    "qatar", "doha",
    "bahrain", "manama",
    "oman", "muscat",
    "kuwait",
    "israel", "tel aviv", "jerusalem",
    "jordan", "amman", "dead sea",
    "lebanon", "beirut",
    "egypt", "cairo", "sharm el sheikh",
    "morocco", "marrakech", "casablanca",

    # Africa
    "africa", "african",
    "south africa", "cape town", "johannesburg",
    "kenya", "nairobi", "mombasa",
    "tanzania", "zanzibar", "serengeti",
    "rwanda", "kigali",
    "mauritius",
    "seychelles",
    "botswana",
    "namibia",
    "zimbabwe",

    # Australia & Pacific
    "australia", "sydney", "melbourne", "brisbane", "perth", "gold coast",
    "new zealand", "auckland", "queenstown",
    "fiji",
    "french polynesia", "bora bora", "tahiti",

    # South America
    "south america",
    "brazil", "sao paulo", "rio de janeiro", "rio",
    "argentina", "buenos aires",
    "chile", "santiago",
    "colombia", "bogota", "cartagena",
    "peru", "lima", "cusco",
    "ecuador", "quito",
    "uruguay", "montevideo",

    # Central America (not Caribbean)
    "costa rica", "san jose costa rica", "guanacaste",
    "panama", "panama city",
    "belize", "belize city",
    "guatemala", "guatemala city",
    "honduras", "nicaragua", "el salvador",
]

# Location type mapping for database
LOCATION_TYPES = {
    "florida": "florida",
    "caribbean": "caribbean",
    "strong_us": "usa",
    "other_us": "usa",
    "unknown_us": "usa",
    "international": "international"
}


def get_location_score(city: str = None, state: str = None, country: str = None) -> Tuple[int, str, str]:
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
        str(country or "").lower().strip()
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
        "bahamas", "jamaica", "barbados", "bermuda", "aruba", "curacao",
        "dominican republic", "puerto rico", "trinidad and tobago",
        "cayman islands", "turks and caicos", "st. lucia", "antigua",
        "grenada", "st. kitts", "anguilla", "bvi", "usvi"
    ]
    is_caribbean = any(cc in country_lower for cc in caribbean_countries)

    # ── STEP 2: If US, score by sub-market (skip international check) ──
    if is_us:
        # Check Florida first (Primary Market - 53% of business)
        for fl_keyword in FLORIDA_KEYWORDS:
            if fl_keyword in location_text:
                return (20, "Florida", "florida")

        # Check Strong US Markets
        for us_keyword in STRONG_US_KEYWORDS:
            if us_keyword in location_text:
                return (15, "Strong US Market", "usa")

        # Check Other US States
        for us_keyword in OTHER_US_KEYWORDS:
            if us_keyword in location_text:
                return (10, "Other US", "usa")

        # USA but unrecognized sub-market
        return (10, "USA (unspecified location)", "usa")

    # ── STEP 3: If Caribbean, score as secondary market ──
    if is_caribbean:
        return (15, "Caribbean", "caribbean")

    for carib_keyword in CARIBBEAN_KEYWORDS:
        if carib_keyword in location_text:
            return (15, "Caribbean", "caribbean")

    # ── STEP 4: Check international keywords (only for non-US locations) ──
    for intl_keyword in INTERNATIONAL_SKIP:
        if intl_keyword in location_text:
            return (-1, f"International - SKIP ({intl_keyword})", "international")

    # ── STEP 5: If country is specified and not matched above, it's international ──
    if country_lower and country_lower not in ["", "none", "null"]:
        return (-1, f"International - SKIP (country: {country})", "international")

    # Unknown - give benefit of doubt
    return (10, "Unknown - Assume US", "usa")


def should_skip_location(city: str = None, state: str = None, country: str = None) -> bool:
    """Check if location should be filtered out (international)"""
    score, _, _ = get_location_score(city, state, country)
    return score == -1


# =============================================================================
# TIMING SCORING (25 pts max)
# =============================================================================

def get_timing_score(opening_date: str = None) -> Tuple[int, str, int]:
    """
    Score based on opening year.

    P-01 FIX: Now uses current_year dynamically instead of hardcoded 2026/2027/2028.
    This means the scoring stays correct as years roll over without code changes.
    
    Logic: current year or earlier = URGENT, +1 = Hot, +2 = Warm, +3 or later = Track

    Returns: (points, timing_tier, year)
    """
    if not opening_date:
        return (4, "Unknown", None)

    date_str = str(opening_date).lower()
    current_year = datetime.now().year

    # Try to extract year from date string
    year_match = re.search(r'20\d{2}', date_str)
    if year_match:
        year = int(year_match.group())
    else:
        # Check for relative year references
        if "this year" in date_str:
            year = current_year
        elif "next year" in date_str:
            year = current_year + 1
        else:
            return (4, "Unknown", None)

    # P-01: Score based on distance from current year (not hardcoded years)
    years_out = year - current_year

    if years_out <= 0:
        # This year or already past — URGENT (may already be open/opening soon)
        return (25, f"{year} - URGENT!", year)
    elif years_out == 1:
        return (18, f"{year} - Hot", year)
    elif years_out == 2:
        return (12, f"{year} - Warm", year)
    else:
        return (6, f"{year}+ - Track", year)


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
            match = re.search(r'\d+', room_count)
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
    contact_name: str = None,
    contact_email: str = None,
    contact_phone: str = None
) -> Tuple[int, str, Dict]:
    """
    Score based on contact information availability.
    """
    points = 0
    breakdown = {}

    # Check for name
    if contact_name and str(contact_name).strip() and str(contact_name).lower() not in ["none", "unknown", "n/a", ""]:
        points += 3
        breakdown["name"] = 3

    # Check for email
    if contact_email and str(contact_email).strip() and "@" in str(contact_email):
        points += 3
        breakdown["email"] = 3

    # Check for phone
    if contact_phone and str(contact_phone).strip() and str(contact_phone).lower() not in ["none", "unknown", "n/a", ""]:
        if re.search(r'\d{3,}', str(contact_phone)):
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

def get_new_build_score(project_type: str = None, description: str = None) -> Tuple[int, str]:
    """
    Score based on whether it's a new build, conversion, or renovation.
    """
    combined_text = f"{project_type or ''} {description or ''}".lower()

    new_build_keywords = ["new build", "new construction", "ground up", "new hotel",
                          "new resort", "new opening", "newly built", "brand new"]
    for keyword in new_build_keywords:
        if keyword in combined_text:
            return (4, "New Build")

    conversion_keywords = ["conversion", "converting", "rebranding", "rebrand",
                          "formerly", "was previously", "changing to", "becoming"]
    for keyword in conversion_keywords:
        if keyword in combined_text:
            return (3, "Brand Conversion")

    renovation_keywords = ["renovation", "renovating", "refurbishment", "refresh",
                          "remodel", "upgrade", "restoration"]
    for keyword in renovation_keywords:
        if keyword in combined_text:
            return (2, "Renovation")

    return (1, "Unknown - Assume New")


# =============================================================================
# EXISTING CLIENT RELATIONSHIP (3 pts max)
# =============================================================================

EXISTING_CLIENT_BRANDS = [
    "graduate", "graduate hotels",
    "loews",
    "four seasons",
    "ritz-carlton", "ritz carlton",
    "hilton",
    "marriott",
    "hyatt",
    "kimpton",
    "intercontinental",
    "embassy suites",
    "doubletree",
    "conrad",
    "waldorf",
    "jw marriott",
    "westin",
    "sheraton",
    "autograph collection",
    "st. regis", "st regis",
    "w hotel", "w hotels",
    "andaz",
    "grand hyatt",
    "park hyatt",
    "fairmont",
    "mandarin oriental",
]


def get_existing_client_score(hotel_name: str = None, brand: str = None) -> Tuple[int, str]:
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
        "breakdown": {}
    }

    # 1. BRAND TIER (25 pts max)
    brand_tier_num, brand_tier_name, brand_points = get_brand_tier(hotel_name)
    result["brand_tier"] = get_brand_tier_name(brand_tier_num)

    if brand_tier_num == 5:
        result["should_save"] = False
        result["skip_reason"] = f"Budget brand: {hotel_name}"
        result["breakdown"]["brand"] = {"points": 0, "tier": brand_tier_name, "skip": True}
        return result

    result["breakdown"]["brand"] = {
        "points": brand_points,
        "tier": brand_tier_name,
        "tier_num": brand_tier_num
    }
    result["total_score"] += brand_points

    # 2. LOCATION (20 pts max)
    location_points, location_tier, location_type = get_location_score(city, state, country)
    result["location_type"] = location_type

    if location_points == -1:
        result["should_save"] = False
        result["skip_reason"] = f"International: {city}, {state}, {country} ({location_tier})"
        result["breakdown"]["location"] = {"points": 0, "tier": location_tier, "skip": True}
        return result

    result["breakdown"]["location"] = {
        "points": location_points,
        "tier": location_tier
    }
    result["total_score"] += location_points

    # 3. TIMING (25 pts max)
    timing_points, timing_tier, opening_year = get_timing_score(opening_date)
    result["opening_year"] = opening_year
    result["breakdown"]["timing"] = {
        "points": timing_points,
        "tier": timing_tier
    }
    result["total_score"] += timing_points

    # 4. ROOM COUNT (15 pts max)
    room_points, room_tier = get_room_count_score(room_count)
    result["breakdown"]["rooms"] = {
        "points": room_points,
        "tier": room_tier
    }
    result["total_score"] += room_points

    # 5. CONTACT INFO (8 pts max)
    contact_points, contact_tier, contact_breakdown = get_contact_score(
        contact_name, contact_email, contact_phone
    )
    result["breakdown"]["contact"] = {
        "points": contact_points,
        "tier": contact_tier,
        "detail": contact_breakdown
    }
    result["total_score"] += contact_points

    # 6. NEW BUILD (4 pts max)
    build_points, build_tier = get_new_build_score(project_type, description)
    result["breakdown"]["new_build"] = {
        "points": build_points,
        "tier": build_tier
    }
    result["total_score"] += build_points

    # 7. EXISTING CLIENT (3 pts max)
    client_points, client_tier = get_existing_client_score(hotel_name, brand)
    result["breakdown"]["existing_client"] = {
        "points": client_points,
        "tier": client_tier
    }
    result["total_score"] += client_points

    # Determine score tier
    if result["total_score"] >= 70:
        result["score_tier"] = "HOT"
    elif result["total_score"] >= 50:
        result["score_tier"] = "WARM"
    elif result["total_score"] >= 30:
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
    brand: str = None
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
        brand=brand
    )
    
    return (
        result["total_score"],
        result["should_save"],
        result["skip_reason"],
        result["brand_tier"],
        result["location_type"]
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
    
    def score_with_breakdown(self, hotel: Dict) -> 'ScoreBreakdown':
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
            hotel.get("city"),
            hotel.get("state"),
            hotel.get("country")
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
def score_lead(hotel: Dict) -> int:
    """
    Quick function to score a lead.
    
    Usage:
        from app.services.scorer import score_lead
        score = score_lead(hotel_dict)
    """
    scorer = LeadScorer()
    return scorer.score(hotel)


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def format_score_breakdown(result: Dict) -> str:
    """Format score breakdown for display"""
    lines = []
    lines.append(f"{'='*60}")
    lines.append(f"TOTAL SCORE: {result['total_score']}/100 [{result['score_tier']}]")
    lines.append(f"{'='*60}")

    if not result["should_save"]:
        lines.append(f"!! SKIP: {result['skip_reason']}")
        return "\n".join(lines)

    breakdown = result["breakdown"]

    lines.append(f"Brand:     {breakdown['brand']['points']:>2} pts - {breakdown['brand']['tier']}")
    lines.append(f"Location:  {breakdown['location']['points']:>2} pts - {breakdown['location']['tier']}")
    lines.append(f"Timing:    {breakdown['timing']['points']:>2} pts - {breakdown['timing']['tier']}")
    lines.append(f"Rooms:     {breakdown['rooms']['points']:>2} pts - {breakdown['rooms']['tier']}")
    lines.append(f"Contact:   {breakdown['contact']['points']:>2} pts - {breakdown['contact']['tier']}")
    lines.append(f"Build:     {breakdown['new_build']['points']:>2} pts - {breakdown['new_build']['tier']}")
    lines.append(f"Client:    {breakdown['existing_client']['points']:>2} pts - {breakdown['existing_client']['tier']}")

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
            opening_date="2026"
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
        "contact_email": "sales@fourseasons.com"
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