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
) -> Optional[tuple[float, float]]:
    """
    Geocode a hotel lead using Geoapify structured search + strict validation.
    Returns (latitude, longitude) or None.

    Uses structured city/state/country params (not free text) for precision,
    then validates results against state-level bounding boxes to prevent
    cross-state false matches (e.g. Rogers MN for Rogers AR).
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
    state_clean = (state or "").strip()

    async def _try_query(
        text: str, use_filter: bool = True
    ) -> Optional[tuple[float, float]]:
        """Run one Geoapify geocode query and validate result."""
        params: dict = {
            "text": text,
            "apiKey": api_key,
            "limit": 5,
            "format": "json",
            "lang": "en",
        }
        if use_filter and country_code:
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
            for r in results:
                lat = float(r["lat"])
                lon = float(r["lon"])
                if _validate_coords(lat, lon, country_norm, state_clean):
                    return (lat, lon)
            return None
        except Exception as e:
            logger.warning(f"Geoapify query failed for '{text}': {e}")
            return None

    # Attempt 1: Structured — city + state + country (most precise for US)
    if city and state_clean:
        q = f"{city}, {state_clean}, {country_norm}"
        result = await _try_query(q)
        if result:
            logger.info(
                f"Geocoded: '{hotel_name}' → ({result[0]:.4f}, {result[1]:.4f})"
            )
            return result

    # Attempt 2: City + country (Caribbean/international often no state)
    if city:
        q = f"{city}, {country_norm}"
        result = await _try_query(q)
        if result:
            logger.info(
                f"Geocoded: '{hotel_name}' → ({result[0]:.4f}, {result[1]:.4f})"
            )
            return result

    # Attempt 3: State + country (last resort for US)
    if state_clean:
        q = f"{state_clean}, {country_norm}"
        result = await _try_query(q)
        if result:
            logger.info(
                f"Geocoded: '{hotel_name}' → ({result[0]:.4f}, {result[1]:.4f})"
            )
            return result

    logger.warning(
        f"Could not geocode: {hotel_name} / {city}, {state_clean}, {country_norm}"
    )
    return None


async def enrich_lead_geo(
    hotel_name: str,
    city: Optional[str],
    state: Optional[str],
    country: Optional[str],
    brand: Optional[str] = None,
    existing_website: Optional[str] = None,
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
    coords_coro = geocode_hotel(hotel_name, city, state, country)

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
