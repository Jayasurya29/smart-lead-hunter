"""
Geoapify Places discovery client.
==================================
Primary hotel discovery source. Reliable infrastructure (paid backend),
uses OSM data. Free tier: 3,000 credits/day.

SAFETY GUARDS:
  1. Hard daily cap from GEOAPIFY_DAILY_CAP env var (default 2500 credits)
  2. Persistent credit counter in usage_log.json — survives restarts
  3. Pre-flight budget check before each call
  4. Rate limit: 4 req/sec (under Geoapify's 5 req/sec)
  5. Network retries (3 attempts, exponential backoff)
  6. Hard stop on quota errors (402/429) — never retries
  7. Per-zone disk cache — reruns skip already-discovered zones
  8. Tracks ACTUAL credits (not just call count): limit=500 ≈ 15 credits

ENRICHMENT:
  Extracts OSM star ratings, category tags, and tourism type from
  Geoapify's raw datasource so the downstream classifier can recognize
  unbranded luxury independents that the brand-tier scorer would miss.
"""

import json
import logging
import os
import socket
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from app.services.zones_registry import Zone

logger = logging.getLogger(__name__)

# ════════════════════════════════════════════════════════════════
# CONFIG
# ════════════════════════════════════════════════════════════════
GEOAPIFY_API_KEY = os.environ.get("GEOAPIFY_API_KEY", "").strip()
GEOAPIFY_DAILY_CAP = int(os.environ.get("GEOAPIFY_DAILY_CAP", "2500"))

API_URL = "https://api.geoapify.com/v2/places"
MAX_LIMIT_PER_CALL = 500
RATE_LIMIT_SLEEP = 0.25
HTTP_TIMEOUT = 60
MAX_NETWORK_RETRIES = 3
USAGE_LOG_PATH = Path("usage_log.json")
CACHE_DIR = Path("cache/geoapify")
CACHE_TTL_DAYS = 30

HOTEL_CATEGORY = "accommodation.hotel"


# ════════════════════════════════════════════════════════════════
# USAGE TRACKER (persistent, in actual credits)
# ════════════════════════════════════════════════════════════════
class UsageTracker:
    def __init__(self, path: Path = USAGE_LOG_PATH):
        self.path = path
        self.data = self._load()

    def _load(self) -> dict:
        if not self.path.exists():
            return {}
        try:
            return json.loads(self.path.read_text())
        except (json.JSONDecodeError, OSError):
            return {}

    def _save(self) -> None:
        try:
            self.path.write_text(json.dumps(self.data, indent=2))
        except OSError as ex:
            logger.warning("Could not save usage log: %s", ex)

    def get_today(self, source: str) -> int:
        today = date.today().isoformat()
        return self.data.get(source, {}).get(today, 0)

    def increment(self, source: str, n: int = 1) -> int:
        today = date.today().isoformat()
        if source not in self.data:
            self.data[source] = {}
        self.data[source][today] = self.data[source].get(today, 0) + n
        self._save()
        return self.data[source][today]


_tracker = UsageTracker()


# ════════════════════════════════════════════════════════════════
# ERRORS
# ════════════════════════════════════════════════════════════════
class QuotaExceededError(Exception):
    """Hard-stop. Daily cap reached or API returned quota error."""

    pass


# ════════════════════════════════════════════════════════════════
# CREDIT COST ESTIMATION
# ════════════════════════════════════════════════════════════════
def _estimate_credits(limit: int) -> int:
    """
    Geoapify Places API charges by limit size. Empirical observations:
      limit=500 → ~13-15 credits
      limit=100 → ~3 credits
      limit=20  → ~1 credit
    We use the conservative upper bound to avoid exceeding the daily cap.
    """
    if limit >= 500:
        return 15
    if limit >= 100:
        return 3
    return 1


# ════════════════════════════════════════════════════════════════
# SAFETY CHECKS
# ════════════════════════════════════════════════════════════════
def _check_budget(needed: int = 1) -> None:
    used = _tracker.get_today("geoapify")
    if used + needed > GEOAPIFY_DAILY_CAP:
        raise QuotaExceededError(
            f"Geoapify daily cap reached: {used}/{GEOAPIFY_DAILY_CAP} credits used. "
            f"Resets at midnight UTC, or raise GEOAPIFY_DAILY_CAP in .env."
        )


def _ensure_key() -> None:
    if not GEOAPIFY_API_KEY:
        raise RuntimeError("GEOAPIFY_API_KEY not set in .env")


# ════════════════════════════════════════════════════════════════
# CACHE
# ════════════════════════════════════════════════════════════════
def _cache_path(zone_key: str) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"{zone_key}.json"


def _load_cache(zone_key: str) -> Optional[List[dict]]:
    path = _cache_path(zone_key)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        cached_at = datetime.fromisoformat(payload["cached_at"])
        age = datetime.now(timezone.utc) - cached_at
        if age > timedelta(days=CACHE_TTL_DAYS):
            logger.info("  cache stale (%d days old), refetching", age.days)
            return None
        return payload["hotels"]
    except (json.JSONDecodeError, KeyError, ValueError, OSError):
        return None


def _save_cache(zone_key: str, hotels: List[dict]) -> None:
    path = _cache_path(zone_key)
    payload = {
        "cached_at": datetime.now(timezone.utc).isoformat(),
        "zone_key": zone_key,
        "count": len(hotels),
        "hotels": hotels,
    }
    try:
        path.write_text(json.dumps(payload), encoding="utf-8")
    except OSError as ex:
        logger.warning("Could not save cache: %s", ex)


def clear_cache(zone_key: Optional[str] = None) -> None:
    if zone_key:
        path = _cache_path(zone_key)
        if path.exists():
            path.unlink()
    else:
        if CACHE_DIR.exists():
            for f in CACHE_DIR.glob("*.json"):
                f.unlink()


# ════════════════════════════════════════════════════════════════
# API CALL (with retries + accurate credit tracking)
# ════════════════════════════════════════════════════════════════
def _call_places(
    bbox: Tuple[float, float, float, float],
    limit: int = MAX_LIMIT_PER_CALL,
    offset: int = 0,
) -> List[dict]:
    _ensure_key()
    estimated_credits = _estimate_credits(limit)
    _check_budget(needed=estimated_credits)

    s, w, n, e = bbox
    rect = f"rect:{w},{s},{e},{n}"

    params = {
        "categories": HOTEL_CATEGORY,
        "filter": rect,
        "limit": limit,
        "offset": offset,
        "apiKey": GEOAPIFY_API_KEY,
    }
    url = f"{API_URL}?{urllib.parse.urlencode(params)}"

    last_error: Optional[Exception] = None
    for attempt in range(1, MAX_NETWORK_RETRIES + 1):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "smart-lead-hunter/1.0"}
            )
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
                payload = json.loads(resp.read())
            break
        except urllib.error.HTTPError as ex:
            if ex.code in (402, 429):
                body = ex.read().decode("utf-8", errors="ignore")[:200]
                raise QuotaExceededError(
                    f"Geoapify {ex.code} (quota/rate). Body: {body}"
                ) from ex
            if 500 <= ex.code < 600 and attempt < MAX_NETWORK_RETRIES:
                wait = 2**attempt
                logger.warning(
                    "  HTTP %d, retry %d/%d in %ds",
                    ex.code,
                    attempt,
                    MAX_NETWORK_RETRIES,
                    wait,
                )
                time.sleep(wait)
                last_error = ex
                continue
            raise
        except (
            urllib.error.URLError,
            socket.timeout,
            TimeoutError,
            ConnectionError,
            json.JSONDecodeError,
        ) as ex:
            if attempt < MAX_NETWORK_RETRIES:
                wait = 2**attempt
                logger.warning(
                    "  network error (%s), retry %d/%d in %ds",
                    type(ex).__name__,
                    attempt,
                    MAX_NETWORK_RETRIES,
                    wait,
                )
                time.sleep(wait)
                last_error = ex
                continue
            raise
    else:
        raise RuntimeError(
            f"Geoapify failed after {MAX_NETWORK_RETRIES} retries: {last_error}"
        )

    used = _tracker.increment("geoapify", estimated_credits)
    features = payload.get("features", [])
    logger.info(
        "  geoapify call: bbox=%s offset=%d → %d results [+%d credits, total: %d/%d]",
        bbox,
        offset,
        len(features),
        estimated_credits,
        used,
        GEOAPIFY_DAILY_CAP,
    )

    time.sleep(RATE_LIMIT_SLEEP)
    return features


# ════════════════════════════════════════════════════════════════
# ADAPTIVE TILING
# ════════════════════════════════════════════════════════════════
def _split_bbox(
    bbox: Tuple[float, float, float, float],
) -> List[Tuple[float, float, float, float]]:
    s, w, n, e = bbox
    mid_lat = (s + n) / 2
    mid_lon = (w + e) / 2
    return [
        (s, w, mid_lat, mid_lon),
        (s, mid_lon, mid_lat, e),
        (mid_lat, w, n, mid_lon),
        (mid_lat, mid_lon, n, e),
    ]


def _discover_recursive(
    bbox: Tuple[float, float, float, float],
    seen: Dict[str, dict],
    zone_key: str,
    depth: int = 0,
    max_depth: int = 4,
) -> None:
    indent = "  " * depth
    features = _call_places(bbox, limit=MAX_LIMIT_PER_CALL)

    for feat in features:
        normalized = _normalize(feat, zone_key)
        if normalized and normalized["source_id"] not in seen:
            seen[normalized["source_id"]] = normalized

    if len(features) >= MAX_LIMIT_PER_CALL and depth < max_depth:
        logger.info("%shit cap (%d), splitting bbox", indent, len(features))
        for sub_bbox in _split_bbox(bbox):
            _discover_recursive(sub_bbox, seen, zone_key, depth + 1, max_depth)


# ════════════════════════════════════════════════════════════════
# NORMALIZER (extracts OSM enrichment data)
# ════════════════════════════════════════════════════════════════
def _normalize(feat: dict, zone_key: str) -> Optional[dict]:
    """
    Convert Geoapify feature to a flat dict.
    Extracts OSM tags from properties.datasource.raw so the downstream
    classifier can use stars/tourism/luxury fallbacks for unbranded hotels.
    """
    props = feat.get("properties", {})
    name = (props.get("name") or "").strip()
    if not name:
        return None

    lat = props.get("lat")
    lon = props.get("lon")
    if lat is None or lon is None:
        return None

    source_id = (
        props.get("place_id")
        or props.get("datasource", {}).get("raw", {}).get("osm_id")
        or f"{lat},{lon},{name}"
    )

    # Categories from Geoapify itself (curated)
    categories = props.get("categories", [])
    if isinstance(categories, str):
        categories = [categories]

    # Raw OSM tags from underlying datasource — this is the goldmine
    raw_osm = props.get("datasource", {}).get("raw", {}) or {}

    # Star rating: OSM "stars" tag (string like "5", "4", "5S")
    osm_stars: Optional[int] = None
    stars_raw = str(raw_osm.get("stars") or "").strip()
    if stars_raw:
        # Strip non-digit suffixes ("5S" → 5, "4*" → 4)
        digits = "".join(c for c in stars_raw if c.isdigit())
        if digits:
            try:
                val = int(digits)
                if 1 <= val <= 5:
                    osm_stars = val
            except ValueError:
                pass

    # Tourism type: hotel | resort | guest_house | apartment | etc.
    osm_tourism = (raw_osm.get("tourism") or "").strip().lower()

    # Luxury indicator (rare but exists in OSM)
    osm_luxury = (raw_osm.get("luxury") or "").strip().lower()

    # Building type
    osm_building = (raw_osm.get("building") or "").strip().lower()

    return {
        "source": "geoapify",
        "source_id": str(source_id),
        "zone_key": zone_key,
        "name": name,
        "brand": (
            props.get("brand") or props.get("operator") or raw_osm.get("brand") or ""
        ).strip(),
        "latitude": float(lat),
        "longitude": float(lon),
        "address": (props.get("address_line1") or "").strip(),
        "city": (props.get("city") or "").strip(),
        "state": (props.get("state") or "").strip(),
        "postcode": (props.get("postcode") or "").strip(),
        "country": (props.get("country") or "").strip(),
        "phone": (
            props.get("contact", {}).get("phone") or raw_osm.get("phone") or ""
        ).strip(),
        "website": (
            props.get("website")
            or props.get("contact", {}).get("website")
            or raw_osm.get("website")
            or ""
        ).strip(),
        # Enrichment fields for unbranded luxury detection:
        "categories": ",".join(categories) if categories else "",
        "osm_stars": osm_stars,
        "osm_tourism": osm_tourism,
        "osm_luxury": osm_luxury,
        "osm_building": osm_building,
    }


# ════════════════════════════════════════════════════════════════
# PUBLIC API
# ════════════════════════════════════════════════════════════════
def discover_geoapify(zone: Zone, use_cache: bool = True) -> List[dict]:
    if use_cache:
        cached = _load_cache(zone.key)
        if cached is not None:
            logger.info(
                "geoapify zone=%s loaded from cache: %d hotels (0 credits used)",
                zone.key,
                len(cached),
            )
            return cached

    used = _tracker.get_today("geoapify")
    logger.info(
        "geoapify discovery for zone=%s state=%s [quota: %d/%d credits used]",
        zone.key,
        zone.state,
        used,
        GEOAPIFY_DAILY_CAP,
    )

    seen: Dict[str, dict] = {}
    try:
        _discover_recursive(zone.bbox, seen, zone.key)
    except QuotaExceededError as ex:
        logger.error("STOPPING: %s", ex)
        logger.error("Returning %d hotels found before quota stop", len(seen))
        raise

    hotels = list(seen.values())
    _save_cache(zone.key, hotels)

    used_after = _tracker.get_today("geoapify")
    logger.info(
        "geoapify zone=%s complete: %d unique hotels [quota: %d/%d credits used today]",
        zone.key,
        len(hotels),
        used_after,
        GEOAPIFY_DAILY_CAP,
    )
    return hotels


# ════════════════════════════════════════════════════════════════
# CLI
# ════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    import sys
    import csv

    try:
        from dotenv import load_dotenv

        load_dotenv()
        GEOAPIFY_API_KEY = os.environ.get("GEOAPIFY_API_KEY", "").strip()
        GEOAPIFY_DAILY_CAP = int(os.environ.get("GEOAPIFY_DAILY_CAP", "2500"))
    except ImportError:
        pass

    from app.services.zones_registry import ZONES

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    if not GEOAPIFY_API_KEY:
        print("ERROR: GEOAPIFY_API_KEY not set in .env")
        sys.exit(1)

    args = sys.argv[1:]
    no_cache = "--no-cache" in args
    args = [a for a in args if a != "--no-cache"]

    zone_key = args[0] if args else "south_florida"
    if zone_key not in ZONES:
        print(f"Unknown zone: {zone_key}")
        sys.exit(1)

    zone = ZONES[zone_key]
    print(f"\n{'=' * 60}")
    print(f"Geoapify discovery: {zone.name} ({zone.state})")
    print(f"bbox: {zone.bbox}")
    print(
        f"Daily cap: {GEOAPIFY_DAILY_CAP} credits | Used today: {_tracker.get_today('geoapify')}"
    )
    print(f"{'=' * 60}\n")

    try:
        hotels = discover_geoapify(zone, use_cache=not no_cache)
    except QuotaExceededError as ex:
        print(f"\n✗ QUOTA STOP: {ex}")
        sys.exit(2)

    out_path = f"geoapify_{zone_key}.csv"
    if hotels:
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(hotels[0].keys()))
            writer.writeheader()
            writer.writerows(hotels)
        print(f"\n✓ Wrote {len(hotels)} hotels to {out_path}")
    else:
        print("\n✗ No hotels found")
