"""
Lead Geo Enrichment Service
============================
Finds the official website and geocoordinates for pre-opening hotel leads.

Two tasks:
1. Website discovery — searches Google via Serper to find the hotel's
   official domain, filtering out news/social/booking sites.

2. Geocoding — converts city+state+country to lat/lng using the
   Nominatim geocoding API (free, no key required). Falls back to
   city-level coordinates when the hotel address isn't known yet
   (which is normal for pre-opening leads).

Both are called:
  - Automatically on first save (lead_factory.py)
  - Via POST /leads/{id}/enrich-geo for existing leads
  - Via POST /leads/bulk-enrich-geo to backfill all leads missing coords
"""

import asyncio
import logging
import os
import urllib.parse
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

# Domains that are never official hotel websites
SKIP_DOMAINS = {
    "tripadvisor.com",
    "booking.com",
    "expedia.com",
    "hotels.com",
    "marriott.com",
    "hilton.com",
    "hyatt.com",
    "ihg.com",
    "accor.com",
    "google.com",
    "facebook.com",
    "instagram.com",
    "twitter.com",
    "linkedin.com",
    "youtube.com",
    "yelp.com",
    "foursquare.com",
    "wikipedia.org",
    "wikidata.org",
    "hoteldive.com",
    "skift.com",
    "hospitalitynet.org",
    "lodgingmagazine.com",
    "businesswire.com",
    "prnewswire.com",
    "globenewswire.com",
    "kayak.com",
    "priceline.com",
    "hoteltonight.com",
    "agoda.com",
    "airbnb.com",
    "vrbo.com",
    "cnn.com",
    "bbc.com",
    "nytimes.com",
    "wsj.com",
}

# Official hotel domain patterns — higher confidence
OFFICIAL_PATTERNS = [
    r"hotel",
    r"resort",
    r"inn",
    r"spa",
    r"lodge",
    r"suites",
]


def _is_official_site(url: str, hotel_name: str) -> bool:
    """Heuristic: is this URL likely the hotel's own website?"""
    if not url:
        return False
    try:
        parsed = urllib.parse.urlparse(url)
        domain = parsed.netloc.lower().replace("www.", "")
    except Exception:
        return False

    # Skip known aggregators / news sites
    for skip in SKIP_DOMAINS:
        if skip in domain:
            return False

    # Bonus: domain contains hotel name words or hospitality terms
    name_words = [w.lower() for w in hotel_name.split() if len(w) > 3]
    for w in name_words:
        if w in domain:
            return True
    for pat in OFFICIAL_PATTERNS:
        if pat in domain:
            return True

    # Accept anything that's not a known aggregator — better than nothing
    return True


async def find_hotel_website(
    hotel_name: str,
    city: Optional[str],
    state: Optional[str],
    brand: Optional[str] = None,
) -> Optional[str]:
    """
    Search Google via Serper to find the official website for a hotel.
    Returns the URL string or None if not found.
    """
    api_key = os.getenv("SERPER_API_KEY")
    if not api_key:
        logger.debug("SERPER_API_KEY not set — skipping website discovery")
        return None

    location = " ".join(filter(None, [city, state]))
    query = f'"{hotel_name}" {location} official site'

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                "https://google.serper.dev/search",
                json={"q": query, "num": 5},
                headers={
                    "X-API-KEY": api_key,
                    "Content-Type": "application/json",
                },
            )
            if resp.status_code != 200:
                return None

            data = resp.json()

            # Check knowledge graph first — most reliable
            kg = data.get("knowledgeGraph", {})
            kg_website = (
                kg.get("website") or kg.get("sitelinks", [{}])[0].get("link")
                if kg
                else None
            )
            if kg_website and _is_official_site(kg_website, hotel_name):
                logger.info(f"Website (KG): {hotel_name} → {kg_website}")
                return kg_website

            # Check organic results
            for r in data.get("organic", []):
                url = r.get("link", "")
                if _is_official_site(url, hotel_name):
                    logger.info(f"Website (organic): {hotel_name} → {url}")
                    return url

    except Exception as e:
        logger.warning(f"Website discovery failed for {hotel_name}: {e}")

    return None


# ── Bounding boxes for coordinate validation ──────────────────────────────
# (min_lat, max_lat, min_lon, max_lon)
# Used to reject clearly wrong geocoding results

# US state bounding boxes — prevents Alabama being returned for California
_US_STATE_BOUNDS: dict[str, tuple] = {
    "alabama": (30.2, 35.0, -88.5, -84.9),
    "alaska": (51.0, 71.5, -180.0, -130.0),
    "arizona": (31.3, 37.0, -114.8, -109.0),
    "az": (31.3, 37.0, -114.8, -109.0),
    "arkansas": (33.0, 36.5, -94.6, -89.6),
    "ar": (33.0, 36.5, -94.6, -89.6),
    "california": (32.5, 42.0, -124.5, -114.1),
    "ca": (32.5, 42.0, -124.5, -114.1),
    "colorado": (37.0, 41.0, -109.1, -102.0),
    "co": (37.0, 41.0, -109.1, -102.0),
    "connecticut": (40.9, 42.1, -73.7, -71.8),
    "ct": (40.9, 42.1, -73.7, -71.8),
    "florida": (24.4, 31.0, -87.7, -80.0),
    "fl": (24.4, 31.0, -87.7, -80.0),
    "georgia": (30.4, 35.0, -85.6, -80.8),
    "ga": (30.4, 35.0, -85.6, -80.8),
    "hawaii": (18.9, 22.3, -160.3, -154.8),
    "hi": (18.9, 22.3, -160.3, -154.8),
    "idaho": (41.9, 49.0, -117.2, -111.0),
    "id": (41.9, 49.0, -117.2, -111.0),
    "illinois": (36.9, 42.5, -91.5, -87.5),
    "il": (36.9, 42.5, -91.5, -87.5),
    "indiana": (37.8, 41.8, -88.1, -84.8),
    "in": (37.8, 41.8, -88.1, -84.8),
    "kansas": (36.9, 40.0, -102.1, -94.6),
    "ks": (36.9, 40.0, -102.1, -94.6),
    "kentucky": (36.5, 39.1, -89.6, -81.9),
    "ky": (36.5, 39.1, -89.6, -81.9),
    "louisiana": (28.9, 33.0, -94.0, -88.8),
    "la": (28.9, 33.0, -94.0, -88.8),
    "maryland": (37.9, 39.7, -79.5, -75.0),
    "md": (37.9, 39.7, -79.5, -75.0),
    "massachusetts": (41.2, 42.9, -73.5, -69.9),
    "ma": (41.2, 42.9, -73.5, -69.9),
    "michigan": (41.7, 48.3, -90.4, -82.4),
    "mi": (41.7, 48.3, -90.4, -82.4),
    "minnesota": (43.5, 49.4, -97.2, -89.5),
    "mn": (43.5, 49.4, -97.2, -89.5),
    "mississippi": (30.2, 35.0, -91.7, -88.1),
    "ms": (30.2, 35.0, -91.7, -88.1),
    "missouri": (36.0, 40.6, -95.8, -89.1),
    "mo": (36.0, 40.6, -95.8, -89.1),
    "montana": (44.4, 49.0, -116.1, -104.0),
    "mt": (44.4, 49.0, -116.1, -104.0),
    "nebraska": (40.0, 43.0, -104.1, -95.3),
    "ne": (40.0, 43.0, -104.1, -95.3),
    "nevada": (35.0, 42.0, -120.0, -114.0),
    "nv": (35.0, 42.0, -120.0, -114.0),
    "new hampshire": (42.7, 45.3, -72.6, -70.6),
    "nh": (42.7, 45.3, -72.6, -70.6),
    "new jersey": (38.9, 41.4, -75.6, -73.9),
    "nj": (38.9, 41.4, -75.6, -73.9),
    "new mexico": (31.3, 37.0, -109.1, -103.0),
    "nm": (31.3, 37.0, -109.1, -103.0),
    "new york": (40.5, 45.0, -79.8, -71.9),
    "ny": (40.5, 45.0, -79.8, -71.9),
    "north carolina": (33.8, 36.6, -84.3, -75.5),
    "nc": (33.8, 36.6, -84.3, -75.5),
    "ohio": (38.4, 42.3, -84.8, -80.5),
    "oh": (38.4, 42.3, -84.8, -80.5),
    "oklahoma": (33.6, 37.0, -103.0, -94.4),
    "ok": (33.6, 37.0, -103.0, -94.4),
    "oregon": (41.9, 46.3, -124.6, -116.5),
    "or": (41.9, 46.3, -124.6, -116.5),
    "pennsylvania": (39.7, 42.3, -80.5, -74.7),
    "pa": (39.7, 42.3, -80.5, -74.7),
    "rhode island": (41.1, 42.0, -71.9, -71.1),
    "ri": (41.1, 42.0, -71.9, -71.1),
    "south carolina": (32.0, 35.2, -83.4, -78.5),
    "sc": (32.0, 35.2, -83.4, -78.5),
    "south dakota": (42.5, 45.9, -104.1, -96.4),
    "sd": (42.5, 45.9, -104.1, -96.4),
    "tennessee": (34.9, 36.7, -90.3, -81.6),
    "tn": (34.9, 36.7, -90.3, -81.6),
    "texas": (25.8, 36.5, -106.6, -93.5),
    "tx": (25.8, 36.5, -106.6, -93.5),
    "utah": (36.9, 42.0, -114.1, -109.0),
    "ut": (36.9, 42.0, -114.1, -109.0),
    "vermont": (42.7, 45.0, -73.4, -71.5),
    "vt": (42.7, 45.0, -73.4, -71.5),
    "virginia": (36.5, 39.5, -83.7, -75.2),
    "va": (36.5, 39.5, -83.7, -75.2),
    "washington": (45.5, 49.0, -124.8, -116.9),
    "wa": (45.5, 49.0, -124.8, -116.9),
    "west virginia": (37.2, 40.6, -82.6, -77.7),
    "wv": (37.2, 40.6, -82.6, -77.7),
    "wisconsin": (42.5, 47.1, -92.9, -86.8),
    "wi": (42.5, 47.1, -92.9, -86.8),
    "wyoming": (41.0, 45.0, -111.1, -104.1),
    "wy": (41.0, 45.0, -111.1, -104.1),
    "district of columbia": (38.8, 39.0, -77.1, -76.9),
    "dc": (38.8, 39.0, -77.1, -76.9),
    "puerto rico": (17.8, 18.6, -67.3, -65.2),
    "pr": (17.8, 18.6, -67.3, -65.2),
    "us virgin islands": (17.6, 18.4, -65.1, -64.5),
    "u.s. virgin islands": (17.6, 18.4, -65.1, -64.5),
    "usvi": (17.6, 18.4, -65.1, -64.5),
    "vi": (17.6, 18.4, -65.1, -64.5),
}

# Caribbean + international country bounds
_COUNTRY_BOUNDS: dict[str, tuple] = {
    "bahamas": (20.8, 27.4, -80.0, -72.5),
    "jamaica": (17.6, 18.6, -78.4, -76.1),
    "barbados": (12.9, 13.4, -59.7, -59.3),
    "bermuda": (32.2, 32.4, -64.9, -64.6),
    "aruba": (12.3, 12.7, -70.1, -69.8),
    "curacao": (11.9, 12.5, -69.3, -68.6),
    "curaçao": (11.9, 12.5, -69.3, -68.6),
    "dominican republic": (17.3, 20.1, -72.1, -68.2),
    "turks and caicos": (21.1, 22.1, -72.7, -71.0),
    "turks & caicos": (21.1, 22.1, -72.7, -71.0),
    "st. lucia": (13.6, 14.1, -61.1, -60.8),
    "saint lucia": (13.6, 14.1, -61.1, -60.8),
    "antigua": (16.9, 17.2, -62.0, -61.6),
    "antigua and barbuda": (16.9, 17.2, -62.0, -61.6),
    "cayman islands": (19.2, 19.8, -81.5, -79.6),
    "trinidad and tobago": (10.0, 11.4, -61.9, -60.5),
    "grenada": (11.9, 12.3, -61.8, -61.5),
    "st. kitts": (17.0, 17.5, -62.9, -62.5),
    "saint kitts": (17.0, 17.5, -62.9, -62.5),
    "anguilla": (18.1, 18.3, -63.2, -62.9),
    "bvi": (18.3, 18.8, -64.8, -64.3),
    "british virgin islands": (18.3, 18.8, -64.8, -64.3),
    "st. maarten": (17.9, 18.2, -63.2, -62.9),
    "saint martin": (17.9, 18.2, -63.2, -62.9),
    "mexico": (14.5, 32.7, -118.4, -86.7),
    "canada": (41.7, 83.0, -141.0, -52.6),
}

# Geoapify ISO country codes
_ISO_CODES: dict[str, str] = {
    "united states": "us",
    "bahamas": "bs",
    "jamaica": "jm",
    "barbados": "bb",
    "bermuda": "bm",
    "aruba": "aw",
    "curacao": "cw",
    "curaçao": "cw",
    "dominican republic": "do",
    "puerto rico": "pr",
    "turks and caicos": "tc",
    "turks & caicos": "tc",
    "st. lucia": "lc",
    "saint lucia": "lc",
    "antigua": "ag",
    "antigua and barbuda": "ag",
    "cayman islands": "ky",
    "trinidad and tobago": "tt",
    "grenada": "gd",
    "st. kitts": "kn",
    "saint kitts": "kn",
    "anguilla": "ai",
    "bvi": "vg",
    "british virgin islands": "vg",
    "usvi": "vi",
    "us virgin islands": "vi",
    "u.s. virgin islands": "vi",
    "st. maarten": "sx",
    "sint maarten": "sx",
    "saint martin": "mf",
    "mexico": "mx",
    "canada": "ca",
}


def _validate_coords(lat: float, lon: float, country: str, state: str = "") -> bool:
    """
    Validate coordinates are in the correct country/state.
    Checks state-level bounds for US leads (prevents Alabama for California).
    Checks country bounds for Caribbean/international.
    """
    country_key = (country or "").lower().strip()
    state_key = (state or "").lower().strip()

    # US state validation — most specific check
    is_us = country_key in (
        "united states",
        "usa",
        "us",
        "u.s.",
        "u.s.a.",
        "america",
        "",
    )
    if is_us and state_key:
        bounds = _US_STATE_BOUNDS.get(state_key)
        if bounds:
            min_lat, max_lat, min_lon, max_lon = bounds
            valid = min_lat <= lat <= max_lat and min_lon <= lon <= max_lon
            if not valid:
                logger.debug(
                    f"Coord validation failed: ({lat:.4f}, {lon:.4f}) not in {state_key} "
                    f"bounds ({min_lat}-{max_lat}, {min_lon}-{max_lon})"
                )
            return valid
        # US state not in our dict — do broad US check
        return 18.0 <= lat <= 72.0 and -180.0 <= lon <= -66.0

    # Caribbean / international validation
    bounds = _COUNTRY_BOUNDS.get(country_key)
    if bounds:
        min_lat, max_lat, min_lon, max_lon = bounds
        valid = min_lat <= lat <= max_lat and min_lon <= lon <= max_lon
        if not valid:
            logger.debug(
                f"Coord validation failed: ({lat:.4f}, {lon:.4f}) not in {country_key}"
            )
        return valid

    # Unknown country — accept but warn
    return True


async def geocode_hotel(
    hotel_name: str,
    city: Optional[str],
    state: Optional[str],
    country: Optional[str],
    address: Optional[str] = None,
    zip_code: Optional[str] = None,
) -> Optional[tuple[float, float]]:
    """
    Geocode a hotel lead using Geoapify. Country-aware query ordering +
    result_type-based candidate selection.

    STRATEGY (empirically tuned against Jamaican resorts, 2026-04-24):

      For USA / Puerto Rico / USVI (specific street grid, addresses precise):
        1. address + city + state + country     (building-level)
        2. hotel_name + city + state + country  (POI fallback)
        3. city + state + country               (city center, last resort)

      For Caribbean / other international:
        1. hotel_name + city + state + country  (POI lookup — PARISH IS CRITICAL)
        2. address + city + state + country     (district-level if address is rich)
        3. hotel_name + city + country          (POI without parish — risky)
        4. city + state + country               (city center, last resort)

    KEY FINDINGS that drive this design:

      - Geoapify's `confidence` score is unreliable for Caribbean POI lookups.
        Wrong matches frequently return confidence=1.0; correct matches
        sometimes return 0.0. We IGNORE confidence and rank by result_type.

      - Parish/state is essential for Caribbean POI disambiguation.
        "Sandals, Montego Bay, Jamaica" returned Sandals WHITEHOUSE (29 mi
        wrong, different parish). Adding "Saint James" made it return the
        correct resort (0.95 mi away).

      - `result_type` hierarchy (most to least precise):
          amenity    → named POI (best — a specific business/landmark)
          building   → specific building
          address    → house-level address (US)
          street     → street centroid (approximate)
          suburb     → neighborhood center
          city/town  → city center (last-resort fallback)

    We always prefer a higher-tier result_type, even from a later attempt,
    over a lower-tier result from an earlier attempt.

    Returns (latitude, longitude) or None.
    """
    api_key = os.getenv("GEOAPIFY_API_KEY", "").strip()
    if not api_key:
        logger.warning("GEOAPIFY_API_KEY not set — skipping geocoding")
        return None

    country_norm = (country or "USA").strip()
    if country_norm.lower() in (
        "usa",
        "us",
        "united states",
        "america",
        "u.s.",
        "u.s.a.",
    ):
        country_norm = "United States"

    country_key = country_norm.lower()
    country_code = _ISO_CODES.get(country_key, "us")

    # ── Normalize state/parish string for Caribbean queries ──────────
    # Gemini returns Jamaica parishes variously as "Saint James",
    # "St. James", or "Saint James Parish". Geoapify hits vary by format
    # — "Saint James Parish" caused a 55-mile wrong match (Sandals Royal
    # *Plantation* in St. Ann instead of Sandals Royal *Caribbean* in
    # St. James). Strip the "Parish" suffix so queries normalize to
    # "Saint James" regardless of DB casing.
    state_clean = (state or "").strip()
    _PARISH_SUFFIXES = (" parish", " county", " district")
    state_lower = state_clean.lower()
    for suffix in _PARISH_SUFFIXES:
        if state_lower.endswith(suffix):
            state_clean = state_clean[: -len(suffix)].strip()
            break

    is_us_family = country_key in (
        "united states",
        "puerto rico",
        "u.s. virgin islands",
        "usvi",
    )

    # ── Hotel-name token set for name-match verification ─────────────
    # Geoapify fuzzy-matches "Sandals Royal Caribbean" and happily
    # returns "Sandals Royal Plantation" (55 mi wrong) because both
    # start with "Sandals Royal". We need to verify that the returned
    # amenity's name actually contains the DISTINGUISHING tokens of
    # the hotel we asked for, not just a common prefix.
    #
    # Strategy: extract all significant tokens (≥3 chars, alphanumeric,
    # case-insensitive) from the hotel name. When we get back an amenity
    # result, its `formatted` or `name` must share ≥60% of those tokens
    # AND must include the most-specific token (the last significant
    # token, which is usually the distinguishing one like "Caribbean"
    # vs "Plantation" vs "Whitehouse").
    import re as _re

    _STOP_WORDS = {
        "the",
        "and",
        "of",
        "at",
        "in",
        "by",
        "hotel",
        "resort",
        "spa",
        "inn",
        "suites",
        "villa",
        "resorts",
    }
    hotel_tokens = [
        t.lower()
        for t in _re.findall(r"[A-Za-z0-9]+", hotel_name)
        if len(t) >= 3 and t.lower() not in _STOP_WORDS
    ]
    # The last significant token is usually the distinguishing one
    # ("Caribbean" in "Sandals Royal Caribbean", "Whitehouse" in
    # "Sandals Whitehouse"). Require it in any amenity match.
    hotel_distinguishing_token = hotel_tokens[-1] if hotel_tokens else ""

    # ── City-center coord for proximity sanity check ─────────────────
    # Before running hotel queries, geocode the city alone. Any amenity
    # result that lands >30 miles from the city center is almost
    # certainly a wrong property (a different sibling in the same
    # brand, a different town with the same name, etc.). This catches
    # both the Sandals Whitehouse 29-mile error and the Sandals Royal
    # Plantation 55-mile error.
    MAX_AMENITY_MILES_FROM_CITY = 25.0

    async def _haversine_miles(lat1, lon1, lat2, lon2) -> float:
        import math

        R = 3958.8
        lat1r, lat2r = math.radians(lat1), math.radians(lat2)
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = (
            math.sin(dlat / 2) ** 2
            + math.cos(lat1r) * math.cos(lat2r) * math.sin(dlon / 2) ** 2
        )
        return 2 * R * math.asin(math.sqrt(a))

    # result_type → priority. Higher = more specific = preferred.
    _TYPE_PRIORITY = {
        "amenity": 6,  # Named POI (hotel as a business)
        "building": 6,  # Specific building
        "address": 5,  # House-level address
        "street": 3,  # Street centroid
        "suburb": 2,  # Neighborhood
        "locality": 1,  # Town
        "city": 1,  # City center
        "town": 1,  # Town
        "county": 0,  # County/parish
        "state": 0,
        "country": 0,
    }

    # Pre-geocode the city to establish a proximity anchor.
    # If this fails, we skip proximity checks entirely (don't want to
    # reject valid matches just because the city lookup failed).
    city_anchor: Optional[tuple[float, float]] = None
    if city:
        anchor_text = (
            f"{city}, {state_clean}, {country_norm}"
            if state_clean
            else f"{city}, {country_norm}"
        )
        try:
            anchor_params = {
                "text": anchor_text,
                "apiKey": api_key,
                "limit": 1,
                "format": "json",
                "lang": "en",
            }
            if country_code:
                anchor_params["filter"] = f"countrycode:{country_code}"
            async with httpx.AsyncClient(timeout=10) as client:
                anchor_resp = await client.get(
                    "https://api.geoapify.com/v1/geocode/search",
                    params=anchor_params,
                )
            if anchor_resp.status_code == 200:
                anchor_results = anchor_resp.json().get("results", [])
                if anchor_results:
                    city_anchor = (
                        float(anchor_results[0]["lat"]),
                        float(anchor_results[0]["lon"]),
                    )
                    logger.debug(
                        f"City anchor for '{hotel_name}': {anchor_text} "
                        f"→ ({city_anchor[0]:.4f}, {city_anchor[1]:.4f})"
                    )
        except Exception as e:
            logger.debug(f"City anchor lookup failed: {e}")

    def _name_matches(formatted: str) -> bool:
        """
        Verify a returned amenity's formatted string contains the
        distinguishing tokens of the hotel we asked for — not just a
        prefix match. Prevents "Sandals Royal Caribbean" → "Sandals
        Royal Plantation" false matches.
        """
        if not hotel_tokens:
            return True  # Can't verify — don't block
        fmt_lower = (formatted or "").lower()
        # The distinguishing token (last significant word) must be present
        if hotel_distinguishing_token and hotel_distinguishing_token not in fmt_lower:
            return False
        # And ≥60% of significant tokens must appear overall
        matches = sum(1 for t in hotel_tokens if t in fmt_lower)
        return matches / len(hotel_tokens) >= 0.6

    async def _try_query(text: str, verify_name: bool = False) -> Optional[dict]:
        """
        Run one Geoapify geocode query. Returns the highest-priority
        valid result as a dict, or None.

        When `verify_name=True` (used for name-based queries), amenity
        results are checked against the hotel's distinguishing tokens.
        Wrong-sibling matches get rejected even if Geoapify gave them
        priority 6.

        Proximity: amenity/building results >25 miles from the city
        anchor are rejected — almost certainly wrong property.
        """
        params: dict = {
            "text": text,
            "apiKey": api_key,
            "limit": 5,
            "format": "json",
            "lang": "en",
        }
        if country_code:
            params["filter"] = f"countrycode:{country_code}"

        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(
                    "https://api.geoapify.com/v1/geocode/search",
                    params=params,
                )
            if resp.status_code != 200:
                return None
            results = resp.json().get("results", [])
        except Exception as e:
            logger.warning(f"Geoapify query failed for '{text}': {e}")
            return None

        best: Optional[dict] = None
        for r in results:
            try:
                lat = float(r["lat"])
                lon = float(r["lon"])
            except (KeyError, ValueError, TypeError):
                continue
            if not _validate_coords(lat, lon, country_norm, state_clean):
                continue
            rtype = (r.get("result_type") or "").lower()
            priority = _TYPE_PRIORITY.get(rtype, 0)
            formatted = r.get("formatted", "")

            # ── Name-match verification for amenity/building results ─
            # Only applied when the caller asked for it (i.e. this was
            # a hotel-name-based query, so the returned amenity should
            # actually be THIS hotel).
            if verify_name and priority >= 5:
                if not _name_matches(formatted):
                    logger.debug(
                        f"Rejected [name-mismatch] for '{hotel_name}' "
                        f"'{text}': got '{formatted}' — missing "
                        f"distinguishing token {hotel_distinguishing_token!r}"
                    )
                    continue

            # ── Proximity sanity check ───────────────────────────────
            # Amenity/building >25 mi from city anchor = wrong property.
            # City-level results (priority 1) are expected to be at the
            # city centroid, so we skip proximity checks for them.
            if priority >= 5 and city_anchor is not None:
                dist = await _haversine_miles(lat, lon, city_anchor[0], city_anchor[1])
                if dist > MAX_AMENITY_MILES_FROM_CITY:
                    logger.debug(
                        f"Rejected [too-far] for '{hotel_name}' '{text}': "
                        f"got '{formatted}' at {dist:.1f} mi from "
                        f"{city} (limit {MAX_AMENITY_MILES_FROM_CITY})"
                    )
                    continue

            candidate = {
                "lat": lat,
                "lng": lon,
                "type": rtype,
                "priority": priority,
                "formatted": formatted,
            }
            if best is None or candidate["priority"] > best["priority"]:
                best = candidate

        return best

    # ── Build attempt list (country-aware) ───────────────────────────
    attempts: list[tuple[str, str]] = []

    if is_us_family:
        # USA: street addresses are precise — try address first
        if address and address.strip():
            parts = [address.strip()]
            if zip_code and zip_code.strip():
                parts.append(zip_code.strip())
            elif city and state_clean:
                parts.extend([city, state_clean])
            attempts.append(("address", ", ".join(parts) + f", {country_norm}"))
        if city and state_clean:
            attempts.append(
                (
                    "name+city+state",
                    f"{hotel_name}, {city}, {state_clean}, {country_norm}",
                )
            )
        if city and state_clean:
            attempts.append(("city+state", f"{city}, {state_clean}, {country_norm}"))
        if city:
            attempts.append(("city", f"{city}, {country_norm}"))
    else:
        # Caribbean / international: POI lookup WITH PARISH first.
        # Empirically: adding the state/parish cut "Sandals Royal Caribbean"
        # distance error from 29 miles to 0.95 miles.
        if city and state_clean:
            attempts.append(
                (
                    "name+city+state",
                    f"{hotel_name}, {city}, {state_clean}, {country_norm}",
                )
            )
        # Address-based fallback for Caribbean: EXTRACT each district-like
        # piece of the address and try it alone with country only.
        #
        # Empirical finding (2026-04-24): "Mahoe Bay, Montego Bay, St. James,
        # Jamaica" returns WRONG STREET (Mahoe Close, 3.27 mi away) because
        # Geoapify's parser chokes on over-specified Caribbean addresses.
        # But "Mahoe Bay, Jamaica" alone returns the correct DISTRICT as an
        # amenity 1.20 mi away. So we split the address on commas and try
        # each piece individually as "{piece}, {country}".
        #
        # Filter pieces: must be ≥5 chars, not purely numeric, not a plain
        # street type suffix ("Avenue", "Road") — we want named districts.
        if address and address.strip():
            addr_pieces = [p.strip() for p in address.split(",") if p.strip()]
            _STREET_TYPE_ALONE = {
                "avenue",
                "ave",
                "street",
                "st",
                "road",
                "rd",
                "drive",
                "dr",
                "boulevard",
                "blvd",
                "lane",
                "ln",
                "way",
                "highway",
                "hwy",
                "court",
                "ct",
                "place",
                "pl",
            }
            for i, piece in enumerate(addr_pieces):
                pl = piece.lower()
                if (
                    len(piece) >= 5
                    and not piece.replace(" ", "").isdigit()
                    and pl not in _STREET_TYPE_ALONE
                ):
                    attempts.append((f"addr_part_{i}", f"{piece}, {country_norm}"))
        # POI without parish — risky but sometimes works
        if city:
            attempts.append(("name+city", f"{hotel_name}, {city}, {country_norm}"))
        # City center last resort
        if city and state_clean:
            attempts.append(("city+state", f"{city}, {state_clean}, {country_norm}"))
        if city:
            attempts.append(("city", f"{city}, {country_norm}"))
        if state_clean:
            attempts.append(("state", f"{state_clean}, {country_norm}"))

    # ── Run attempts; keep best-priority result across all ───────────
    # We don't stop at the first attempt — a later attempt might return
    # a higher-priority result_type. We collect them all and pick the
    # single best candidate.
    #
    # For attempts that include the hotel name, we enable name-match
    # verification so Geoapify can't silently return a different
    # property with a similar prefix (e.g., "Sandals Royal Caribbean"
    # → "Sandals Royal Plantation" 55 mi away).
    best_overall: Optional[dict] = None
    best_label: str = ""
    for label, query in attempts:
        # Enable name-match verification on any attempt that includes
        # the hotel name in the query. These are the attempts where
        # Geoapify might return a wrong-sibling amenity.
        verify = label.startswith("name+")
        r = await _try_query(query, verify_name=verify)
        if r is None:
            continue
        logger.debug(
            f"Geocode attempt [{label}] for '{hotel_name}': "
            f"type={r['type']} prio={r['priority']} @ ({r['lat']:.4f}, {r['lng']:.4f}) "
            f"→ {r['formatted']!r}"
        )
        if best_overall is None or r["priority"] > best_overall["priority"]:
            best_overall = r
            best_label = label
            # Early exit: if we got a POI/building/address match, that's
            # about as good as it gets — no need to try more
            if r["priority"] >= 5:
                break

    if best_overall is None:
        logger.warning(
            f"Could not geocode: {hotel_name} / {city}, {state_clean}, {country_norm}"
        )
        return None

    # Extra guard: if the best we got is a city-level fallback AND we had
    # specific data (address or name + parish), that's a red flag worth
    # logging but we still return the coords as a best-effort answer.
    if best_overall["priority"] <= 1 and (address or (hotel_name and state_clean)):
        logger.warning(
            f"Geocoded [{best_label}] '{hotel_name}' to city-level only "
            f"({best_overall['type']}): ({best_overall['lat']:.4f}, "
            f"{best_overall['lng']:.4f}) — could not find building/POI match"
        )
    else:
        logger.info(
            f"Geocoded [{best_label}] '{hotel_name}' as {best_overall['type']}: "
            f"({best_overall['lat']:.4f}, {best_overall['lng']:.4f}) — "
            f"{best_overall['formatted']}"
        )

    return (best_overall["lat"], best_overall["lng"])


async def enrich_lead_geo(
    hotel_name: str,
    city: Optional[str],
    state: Optional[str],
    country: Optional[str],
    brand: Optional[str] = None,
    existing_website: Optional[str] = None,
    address: Optional[str] = None,
    zip_code: Optional[str] = None,
) -> dict:
    """
    Run both website discovery and geocoding for a lead.
    Returns dict with: website, latitude, longitude, website_verified

    Runs website + geocoding concurrently to save time.
    """

    async def _noop(v):
        return v

    website_coro = (
        find_hotel_website(hotel_name, city, state, brand)
        if not existing_website
        else _noop(existing_website)
    )
    coords_coro = geocode_hotel(hotel_name, city, state, country, address, zip_code)

    website_result, coords_result = await asyncio.gather(
        website_coro, coords_coro, return_exceptions=True
    )

    website = website_result if not isinstance(website_result, Exception) else None
    coords = coords_result if not isinstance(coords_result, Exception) else None

    return {
        "hotel_website": website or existing_website,
        "latitude": coords[0] if coords else None,
        "longitude": coords[1] if coords else None,
        "website_verified": "auto"
        if website and not existing_website
        else ("manual" if existing_website else None),
    }
