"""
SMART LEAD HUNTER — Master Discovery Pipeline
==============================================
End-to-end discovery → filter → enrich → match → report → sync workflow.

CLI USAGE:
    python -m app.services.pipeline south_florida          # one zone, dry run
    python -m app.services.pipeline south_florida --sync   # one zone, write to DB
    python -m app.services.pipeline FL                      # entire state, dry run
    python -m app.services.pipeline FL --sync               # entire state, write to DB
    python -m app.services.pipeline FL --no-cache           # ignore cache, refetch
    python -m app.services.pipeline FL --clear-cache        # wipe cache then run
    python -m app.services.pipeline CA --sync --enrich      # use Gemini on unknowns

WORKFLOW:
    1. DISCOVER  via Geoapify (cached + safety guards)
    2. FILTER    multi-strategy classifier (brand match + name patterns)
    3. ENRICH    (--enrich only) — Gemini classifies unknowns
    4. MATCH     fuzzy name + geo distance against existing DB
    5. REPORT    per-zone CSV
    6. SYNC      (--sync only) — insert NEW leads
"""

import argparse
import csv
import logging
import math
import re
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import or_, and_, create_engine
from sqlalchemy.orm import sessionmaker

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

from app.config import settings
from app.models.existing_hotel import ExistingHotel
from app.services.scorer import (
    get_brand_tier,
    get_brand_tier_name,
    EXISTING_CLIENT_BRANDS,
)
from app.services.sources.geoapify import (
    discover_geoapify,
    QuotaExceededError,
    clear_cache,
)
from app.services.zones_registry import ZONES, Zone, zones_by_state

logger = logging.getLogger(__name__)

# ════════════════════════════════════════════════════════════════
# SYNC DB SESSION
# ════════════════════════════════════════════════════════════════
_sync_url = settings.database_url.replace("postgresql+asyncpg://", "postgresql://")
_sync_engine = create_engine(_sync_url, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=_sync_engine, autocommit=False, autoflush=False)


# ════════════════════════════════════════════════════════════════
# CONFIG
# ════════════════════════════════════════════════════════════════
NAME_MATCH_THRESHOLD = 85
GEO_MATCH_METERS = 200
REPORTS_DIR = Path("reports")

PRIORITY_ORDER = {"high": 0, "medium": 1, "low": 2}
KEEP_TIER_NUMS = {1, 2, 3, 4}

_STATE_CODE_TO_NAME = {
    "AL": "Alabama",
    "AK": "Alaska",
    "AZ": "Arizona",
    "AR": "Arkansas",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DE": "Delaware",
    "DC": "District of Columbia",
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
    "BS": "Bahamas",
    "JM": "Jamaica",
    "DO": "Dominican Republic",
    "PR": "Puerto Rico",
    "CY": "Cayman Islands",
    "TC": "Turks and Caicos",
    "BM": "Bermuda",
    "VI": "US Virgin Islands",
    "VG": "British Virgin Islands",
    "BB": "Barbados",
    "AW": "Aruba",
    "CW": "Curaçao",
    "LC": "Saint Lucia",
    "AG": "Antigua and Barbuda",
    "AI": "Anguilla",
    "KN": "St. Kitts and Nevis",
    "SX": "St. Martin / Sint Maarten",
    "GD": "Grenada",
    "DM": "Dominica",
    "TT": "Trinidad and Tobago",
    "VC": "St. Vincent & Grenadines",
}


def _state_full_name(code: str) -> str:
    return _STATE_CODE_TO_NAME.get((code or "").upper(), code or "")


# ════════════════════════════════════════════════════════════════
# NAME PATTERN HEURISTICS
# ════════════════════════════════════════════════════════════════
BUDGET_NAME_PATTERNS = [
    r"\bmotel\b",
    r"\bmotor\s+inn\b",
    r"\bmotor\s+lodge\b",
    r"\bextended\s*stay\b",
    r"\bvalue\s+(inn|lodge|place)\b",
    r"\bbudget\s+(inn|lodge|hotel|motel)\b",
    r"\beconomy\s+(inn|lodge)\b",
    r"\bsuper\s*8\b",
    r"\b(rodeway|travelodge|knight)\b",
    r"\bdays\s+inn\b",
    r"\bred\s+roof\b",
    r"\bmotel\s*6\b",
    r"\bstudio\s*6\b",
    r"\bmicrotel\b",
    r"\bhostel\b",
    r"\bbed\s*&?\s*breakfast\b",
    r"\bb\s*&\s*b\b",
    r"\bcabins?\b",
    r"\bcamp(ground|ing|site)\b",
    r"\brv\s+park\b",
    r"\btrailer\s+park\b",
    r"\bguest\s*house\b",
    r"\binn\s*&\s*suites?\b",
]

LUXURY_NAME_PATTERNS = [
    r"\bresort\b",
    r"\bspa\b",
    r"\bboutique\b",
    r"\bmanor\b",
    r"\bestate\b",
    r"\bvilla\b",
    r"\bvillas\b",
    r"\bretreat\b",
    r"\bmansion\b",
    r"\bchateau\b",
    r"\bcastle\b",
    r"\bcountry\s+club\b",
    r"\binn\s+at\b",
    r"\blodge\s+at\b",
    r"\b(the\s+)?ranch\b",
    r"\bhacienda\b",
    r"\bcasa\s+(del|de\s+la|del\s+mar)\b",
    r"\bauberge\b",
    r"\bgrand\s+hotel\b",
    r"\bpalazzo\b",
    r"\bpalace\s+hotel\b",
]

_BUDGET_RE = re.compile("|".join(BUDGET_NAME_PATTERNS), re.IGNORECASE)
_LUXURY_RE = re.compile("|".join(LUXURY_NAME_PATTERNS), re.IGNORECASE)


def _name_matches_budget(name: str) -> bool:
    return bool(_BUDGET_RE.search(name or ""))


def _name_matches_luxury(name: str) -> bool:
    return bool(_LUXURY_RE.search(name or ""))


# ════════════════════════════════════════════════════════════════
# UTILS
# ════════════════════════════════════════════════════════════════
def _haversine_meters(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6_371_000
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def _print_header(title: str, char: str = "=") -> None:
    print(char * 64)
    print(title)
    print(char * 64)


# ════════════════════════════════════════════════════════════════
# MULTI-STRATEGY TIER CLASSIFIER (rule-based)
# ════════════════════════════════════════════════════════════════
def _classify_hotel_tier(hotel: dict) -> Tuple[int, str, str, str]:
    """
    Returns: (tier_num, tier_label, tier_db_name, classification_method)
    """
    name = hotel.get("name", "") or ""
    brand = hotel.get("brand", "") or ""

    if _name_matches_budget(name):
        return 5, "Budget (name pattern)", "tier5_skip", "budget_keyword"

    candidates = []
    n_tier, n_label, _ = get_brand_tier(name)
    if n_tier > 0:
        candidates.append((n_tier, n_label, "name_brand"))
    if brand:
        b_tier, b_label, _ = get_brand_tier(brand)
        if b_tier > 0:
            candidates.append((b_tier, b_label, "brand_field"))

    branded_keepers = [c for c in candidates if c[0] in KEEP_TIER_NUMS]
    if branded_keepers:
        best = min(branded_keepers, key=lambda c: c[0])
        return best[0], best[1], get_brand_tier_name(best[0]), best[2]

    if any(c[0] == 5 for c in candidates):
        return 5, "Budget/Skip", "tier5_skip", "name_brand_skip"

    stars = hotel.get("osm_stars")
    if stars and isinstance(stars, int):
        if stars >= 5:
            return 2, "Luxury (OSM 5★)", "tier2_luxury", "osm_stars_5"
        if stars == 4:
            return 4, "Upscale (OSM 4★)", "tier4_upscale", "osm_stars_4"
        return 0, "Below 4★ (OSM)", "unknown", "osm_stars_low"

    if (hotel.get("osm_luxury") or "").lower() == "yes":
        return 2, "Luxury (OSM tag)", "tier2_luxury", "osm_luxury_tag"

    tourism = (hotel.get("osm_tourism") or "").lower()
    categories = (hotel.get("categories") or "").lower()
    if tourism == "resort" or "accommodation.resort" in categories:
        return 4, "Upscale (resort)", "tier4_upscale", "resort_fallback"

    if _name_matches_luxury(name):
        return 4, "Upscale (name pattern)", "tier4_upscale", "luxury_keyword"

    return 0, "Unknown", "unknown", "unknown"


def _is_existing_client_brand(hotel: dict) -> bool:
    name_lower = (hotel.get("name", "") or "").lower()
    brand_lower = (hotel.get("brand", "") or "").lower()
    combined = f"{name_lower} {brand_lower}"
    return any(b in combined for b in EXISTING_CLIENT_BRANDS)


# ════════════════════════════════════════════════════════════════
# STEP 1: DISCOVER
# ════════════════════════════════════════════════════════════════
def step_discover(zone: Zone, use_cache: bool = True) -> List[dict]:
    print("\n[1/6] Discovering via Geoapify...")
    hotels = discover_geoapify(zone, use_cache=use_cache)
    print(f"      → {len(hotels)} raw hotels found")
    return hotels


# ════════════════════════════════════════════════════════════════
# STEP 2: FILTER (returns kept + unknowns separately for enrichment)
# ════════════════════════════════════════════════════════════════
def step_filter(
    raw_hotels: List[dict],
) -> Tuple[List[dict], List[dict], Dict[str, int]]:
    """
    Returns: (kept_hotels, unknown_hotels, tier_counts)
    Unknown hotels can be passed to step_enrich for Gemini classification.
    """
    print("\n[2/6] Filtering through multi-strategy classifier...")
    kept = []
    unknowns = []
    tier_counts: Dict[str, int] = {
        "tier1_ultra_luxury": 0,
        "tier2_luxury": 0,
        "tier3_upper_upscale": 0,
        "tier4_upscale": 0,
        "skipped_budget": 0,
        "skipped_unknown": 0,
    }
    method_counts: Dict[str, int] = {}

    for h in raw_hotels:
        tier_num, tier_label, tier_db, method = _classify_hotel_tier(h)

        if tier_num in KEEP_TIER_NUMS:
            h["brand_tier"] = tier_db
            h["tier_num"] = tier_num
            h["tier_label"] = tier_label
            h["classification_method"] = method
            kept.append(h)
            tier_counts[tier_db] += 1
            method_counts[method] = method_counts.get(method, 0) + 1
        elif tier_num == 5:
            tier_counts["skipped_budget"] += 1
        else:
            tier_counts["skipped_unknown"] += 1
            unknowns.append(h)

    print(
        f"      → {len(kept)} kept, {len(unknowns)} unknown, {tier_counts['skipped_budget']} budget (out of {len(raw_hotels)})"
    )
    for tier, count in tier_counts.items():
        print(f"        {tier:25} {count}")
    if method_counts:
        print("      Classification methods (kept):")
        for method in sorted(method_counts.keys()):
            print(f"        {method:25} {method_counts[method]}")
    return kept, unknowns, tier_counts


# ════════════════════════════════════════════════════════════════
# STEP 3: ENRICH WITH GEMINI (optional)
# ════════════════════════════════════════════════════════════════
def step_enrich(unknowns: List[dict]) -> List[dict]:
    """
    Send unknowns to Gemini for classification.
    Returns the subset that Gemini classified as 4★+ with high confidence.
    """
    if not unknowns:
        print("\n[3/6] Enrichment skipped — no unknowns to classify")
        return []

    print(f"\n[3/6] Enriching {len(unknowns)} unknowns with Gemini...")

    try:
        from app.services.gemini_classifier import (
            classify_unknowns,
            TIER_DB_TO_LABEL,
            TIER_DB_TO_NUM,
        )
    except ImportError as ex:
        print(f"      ✗ Gemini classifier not available: {ex}")
        print("      Run: pip install google-cloud-aiplatform")
        return []
    except Exception as ex:
        print(f"      ✗ Gemini init failed: {ex}")
        return []

    try:
        results = classify_unknowns(unknowns)
    except Exception as ex:
        print(f"      ✗ Gemini classification failed: {ex}")
        traceback.print_exc()
        return []

    enriched = []
    for h in unknowns:
        sid = h.get("source_id", "")
        if sid in results:
            tier_db, conf, reasoning = results[sid]
            h["brand_tier"] = tier_db
            h["tier_num"] = TIER_DB_TO_NUM.get(tier_db, 0)
            h["tier_label"] = TIER_DB_TO_LABEL.get(tier_db, "Gemini")
            h["classification_method"] = (
                f"gemini_{tier_db.replace('tier', 't').replace('_', '')}"
            )
            h["gemini_confidence"] = conf
            h["gemini_reasoning"] = reasoning
            enriched.append(h)

    print(f"      → {len(enriched)} unknowns recovered as 4★+ via Gemini")
    return enriched


# ════════════════════════════════════════════════════════════════
# STEP 4: LOAD EXISTING FROM DB
# ════════════════════════════════════════════════════════════════
def step_load_existing(zone: Zone) -> List[ExistingHotel]:
    print("\n[4/6] Loading existing hotels from database...")
    db = SessionLocal()
    try:
        s, w, n, e = zone.bbox
        rows = (
            db.query(ExistingHotel)
            .filter(
                or_(
                    ExistingHotel.zone == zone.name,
                    ExistingHotel.zone == zone.key,
                    and_(
                        ExistingHotel.latitude.between(s, n),
                        ExistingHotel.longitude.between(w, e),
                    ),
                )
            )
            .all()
        )
    finally:
        db.close()

    clients = sum(1 for r in rows if r.is_client)
    prospects = len(rows) - clients
    print(f"      → {len(rows)} existing hotels in this zone")
    print(f"        clients:    {clients}")
    print(f"        prospects:  {prospects}")
    return rows


# ════════════════════════════════════════════════════════════════
# STEP 5: MATCH
# ════════════════════════════════════════════════════════════════
def step_match(discovered: List[dict], existing: List[ExistingHotel]) -> List[dict]:
    print(
        f"\n[5/6] Matching (fuzzy name ≥{NAME_MATCH_THRESHOLD}% + geo ≤{GEO_MATCH_METERS}m)..."
    )

    try:
        from rapidfuzz import fuzz
    except ImportError:
        print("      ✗ rapidfuzz not installed. Run: pip install rapidfuzz")
        sys.exit(3)

    existing_indexed = [
        e for e in existing if e.latitude is not None and e.longitude is not None
    ]

    classified = []
    counts = {"CLIENT": 0, "PROSPECT": 0, "NEW": 0, "NEW_CLIENT_BRAND": 0}

    for d in discovered:
        d_lat, d_lon, d_name = d["latitude"], d["longitude"], d["name"]
        best_match: Optional[ExistingHotel] = None
        best_score = 0
        best_distance = float("inf")

        for e in existing_indexed:
            distance = _haversine_meters(d_lat, d_lon, e.latitude, e.longitude)
            if distance > GEO_MATCH_METERS:
                continue
            score = fuzz.token_set_ratio(d_name, e.name or "")
            if score >= NAME_MATCH_THRESHOLD and score > best_score:
                best_match = e
                best_score = score
                best_distance = distance

        if best_match is None:
            cls = "NEW"
            d["matched_db_id"] = None
            d["matched_db_name"] = None
            d["match_score"] = None
            d["match_distance_m"] = None
            if _is_existing_client_brand(d):
                d["client_brand_hint"] = True
                counts["NEW_CLIENT_BRAND"] += 1
            else:
                d["client_brand_hint"] = False
        else:
            cls = "CLIENT" if best_match.is_client else "PROSPECT"
            d["matched_db_id"] = best_match.id
            d["matched_db_name"] = best_match.name
            d["match_score"] = best_score
            d["match_distance_m"] = round(best_distance, 1)
            d["client_brand_hint"] = False

        d["classification"] = cls
        counts[cls] += 1
        classified.append(d)

    print(f"      → CLIENT match:        {counts['CLIENT']}")
    print(f"      → PROSPECT match:      {counts['PROSPECT']}")
    print(f"      → NEW leads:           {counts['NEW']}")
    if counts["NEW_CLIENT_BRAND"]:
        print(
            f"        ↳ likely client brands missing from DB: {counts['NEW_CLIENT_BRAND']}"
        )
    return classified


# ════════════════════════════════════════════════════════════════
# STEP 6: REPORT
# ════════════════════════════════════════════════════════════════
def step_report(zone: Zone, classified: List[dict]) -> None:
    print("\n[6/6] Writing reports...")
    REPORTS_DIR.mkdir(exist_ok=True)

    if not classified:
        print("      ✗ Nothing to write")
        return

    fieldnames = list(classified[0].keys())
    full_path = REPORTS_DIR / f"match_report_{zone.key}.csv"
    new_path = REPORTS_DIR / f"new_leads_{zone.key}.csv"

    with open(full_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(classified)
    print(f"      ✓ {full_path}  ({len(classified)} rows)")

    new_leads = [c for c in classified if c["classification"] == "NEW"]
    with open(new_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(new_leads)
    print(f"      ✓ {new_path}  ({len(new_leads)} new leads)")


# ════════════════════════════════════════════════════════════════
# OPTIONAL: SYNC TO DB
# ════════════════════════════════════════════════════════════════
def step_sync(zone: Zone, classified: List[dict]) -> Tuple[int, int]:
    new_leads = [c for c in classified if c["classification"] == "NEW"]
    if not new_leads:
        return 0, 0

    print(f"\n[+] Syncing {len(new_leads)} NEW leads to database...")
    db = SessionLocal()
    inserted = 0
    skipped = 0
    try:
        for lead in new_leads:
            exists = (
                db.query(ExistingHotel)
                .filter(
                    ExistingHotel.name == lead["name"],
                    ExistingHotel.latitude == lead["latitude"],
                    ExistingHotel.longitude == lead["longitude"],
                )
                .first()
            )
            if exists:
                skipped += 1
                continue

            # Mark Gemini-classified leads with a distinct data_source
            method = lead.get("classification_method", "")
            data_source = (
                "geoapify_gemini"
                if method.startswith("gemini_")
                else "geoapify_discovery"
            )

            row = ExistingHotel(
                name=lead["name"],
                brand=lead.get("brand") or None,
                brand_tier=lead.get("brand_tier"),
                address=lead.get("address") or None,
                city=lead.get("city") or None,
                state=_state_full_name(zone.state),
                country=lead.get("country") or "US",
                zip_code=lead.get("postcode") or None,
                latitude=lead["latitude"],
                longitude=lead["longitude"],
                phone=lead.get("phone") or None,
                website=lead.get("website") or None,
                is_client=False,
                data_source=data_source,
                status="new",
                zone=zone.name,
                last_verified_at=datetime.now(timezone.utc),
            )
            db.add(row)
            inserted += 1

        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

    print(f"      ✓ Inserted {inserted} new leads (skipped {skipped} duplicates)")
    return inserted, skipped


# ════════════════════════════════════════════════════════════════
# RUN ONE ZONE
# ════════════════════════════════════════════════════════════════
def run_zone(
    zone: Zone,
    sync: bool = False,
    use_cache: bool = True,
    enrich: bool = False,
) -> Optional[Dict]:
    _print_header(f"ZONE: {zone.name} ({zone.state}) | priority={zone.priority}")
    print(f"bbox: {zone.bbox}")
    if enrich:
        print("Gemini enrichment: ON")

    try:
        raw = step_discover(zone, use_cache=use_cache)
        kept, unknowns, tier_counts = step_filter(raw)

        # Optional Gemini enrichment of unknowns
        enriched = []
        if enrich and unknowns:
            enriched = step_enrich(unknowns)
            kept.extend(enriched)
        elif not enrich:
            print("\n[3/6] Enrichment disabled (use --enrich to recover unknowns)")

        existing = step_load_existing(zone)
        classified = step_match(kept, existing)
        step_report(zone, classified)

        inserted = 0
        skipped = 0
        if sync:
            inserted, skipped = step_sync(zone, classified)

        counts = {"CLIENT": 0, "PROSPECT": 0, "NEW": 0}
        client_brand_hints = 0
        for c in classified:
            counts[c["classification"]] += 1
            if c.get("client_brand_hint"):
                client_brand_hints += 1

        return {
            "zone_key": zone.key,
            "zone_name": zone.name,
            "state": zone.state,
            "priority": zone.priority,
            "raw": len(raw),
            "filtered": len(kept),
            "enriched": len(enriched),
            "existing": len(existing),
            "client": counts["CLIENT"],
            "prospect": counts["PROSPECT"],
            "new": counts["NEW"],
            "client_brand_hints": client_brand_hints,
            "inserted": inserted,
            "skipped": skipped,
            "tier_breakdown": tier_counts,
        }
    except QuotaExceededError as ex:
        print(f"\n✗ QUOTA EXCEEDED: {ex}")
        raise
    except Exception as ex:
        print(f"\n✗ ZONE FAILED: {zone.key}: {ex}")
        traceback.print_exc()
        return None


# ════════════════════════════════════════════════════════════════
# RUN ENTIRE STATE
# ════════════════════════════════════════════════════════════════
def run_state(
    state_code: str,
    sync: bool = False,
    use_cache: bool = True,
    enrich: bool = False,
) -> List[Dict]:
    zones = zones_by_state(state_code)
    if not zones:
        print(f"No zones found for state: {state_code}")
        return []

    zones.sort(key=lambda z: PRIORITY_ORDER.get(z.priority, 9))

    _print_header(
        f"STATE PIPELINE: {state_code} | {len(zones)} zones | enrich={enrich}"
    )
    for z in zones:
        print(f"  {z.priority:6}  {z.key:25} {z.name}")
    print()

    results: List[Dict] = []
    for i, zone in enumerate(zones, 1):
        print(f"\n\n>>> Zone {i}/{len(zones)} <<<")
        try:
            result = run_zone(zone, sync=sync, use_cache=use_cache, enrich=enrich)
            if result:
                results.append(result)
        except QuotaExceededError:
            print(f"\nStopping state run after zone {i}/{len(zones)} due to quota.")
            break

    print("\n\n")
    _print_header(f"STATE SUMMARY: {state_code}", "═")

    if not results:
        print("No zones completed successfully.")
        return results

    totals = {
        "raw": 0,
        "filtered": 0,
        "enriched": 0,
        "existing": 0,
        "client": 0,
        "prospect": 0,
        "new": 0,
        "inserted": 0,
        "client_brand_hints": 0,
    }
    for r in results:
        for k in totals:
            totals[k] += r.get(k, 0)

    print(
        f"\n{'Zone':<25} {'Raw':>6} {'4★+':>6} {'+AI':>5} {'Exist':>6} {'Cli':>5} {'Pros':>5} {'NEW':>5}"
    )
    print("-" * 70)
    for r in results:
        print(
            f"{r['zone_name']:<25} {r['raw']:>6} {r['filtered']:>6} {r.get('enriched', 0):>5} "
            f"{r['existing']:>6} {r['client']:>5} {r['prospect']:>5} {r['new']:>5}"
        )
    print("-" * 70)
    print(
        f"{'TOTAL':<25} {totals['raw']:>6} {totals['filtered']:>6} {totals['enriched']:>5} "
        f"{totals['existing']:>6} {totals['client']:>5} {totals['prospect']:>5} {totals['new']:>5}"
    )

    if totals["client_brand_hints"]:
        print(
            f"\n⚠  {totals['client_brand_hints']} NEW leads match existing-client brand patterns"
        )

    if sync:
        print(f"\n→ Inserted into DB: {totals['inserted']}")

    REPORTS_DIR.mkdir(exist_ok=True)
    state_report_path = REPORTS_DIR / f"state_summary_{state_code}.csv"
    with open(state_report_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[k for k in results[0].keys() if k != "tier_breakdown"],
        )
        w.writeheader()
        for r in results:
            row = {k: v for k, v in r.items() if k != "tier_breakdown"}
            w.writerow(row)
    print(f"\n✓ State summary: {state_report_path}")

    return results


# ════════════════════════════════════════════════════════════════
# API WRAPPERS
# ════════════════════════════════════════════════════════════════
def get_zones_for_api(state: Optional[str] = None) -> List[Dict[str, Any]]:
    if state:
        zones = zones_by_state(state)
    else:
        zones = list(ZONES.values())
    return [
        {
            "key": z.key,
            "name": z.name,
            "state": z.state,
            "priority": z.priority,
            "description": z.description,
            "bbox": list(z.bbox),
        }
        for z in zones
    ]


def run_zone_for_api(
    zone_key: str, sync: bool = True, enrich: bool = False
) -> Dict[str, Any]:
    if zone_key not in ZONES:
        raise ValueError(f"Unknown zone: {zone_key}")
    zone = ZONES[zone_key]
    result = run_zone(zone, sync=sync, use_cache=True, enrich=enrich)
    if result is None:
        return {"status": "failed", "zone": zone_key, "zone_name": zone.name}
    return {
        "status": "complete",
        "zone": result["zone_key"],
        "zone_name": result["zone_name"],
        "state": result["state"],
        "priority": result["priority"],
        "total_from_geoapify": result["raw"],
        "qualified_4star_plus": result["filtered"],
        "enriched_by_gemini": result.get("enriched", 0),
        "tier_breakdown": result["tier_breakdown"],
        "existing_in_db": result["existing"],
        "matched_clients": result["client"],
        "matched_prospects": result["prospect"],
        "new_leads": result["new"],
        "client_brand_hints": result["client_brand_hints"],
        "saved_new": result["inserted"],
        "skipped_duplicates": result["skipped"],
    }


def run_state_for_api(
    state_code: str, sync: bool = True, enrich: bool = False
) -> Dict[str, Any]:
    state_code = state_code.upper()
    results = run_state(state_code, sync=sync, use_cache=True, enrich=enrich)
    if not results:
        return {
            "status": "no_zones",
            "state": state_code,
            "zones_processed": 0,
            "totals": {},
            "zones": [],
        }
    totals = {
        "raw": 0,
        "filtered": 0,
        "enriched": 0,
        "existing": 0,
        "client": 0,
        "prospect": 0,
        "new": 0,
        "inserted": 0,
        "client_brand_hints": 0,
    }
    for r in results:
        for k in totals:
            totals[k] += r.get(k, 0)
    return {
        "status": "complete",
        "state": state_code,
        "zones_processed": len(results),
        "totals": {
            "total_from_geoapify": totals["raw"],
            "qualified_4star_plus": totals["filtered"],
            "enriched_by_gemini": totals["enriched"],
            "existing_in_db": totals["existing"],
            "matched_clients": totals["client"],
            "matched_prospects": totals["prospect"],
            "new_leads": totals["new"],
            "client_brand_hints": totals["client_brand_hints"],
            "saved_new": totals["inserted"],
        },
        "zones": [
            {
                "zone": r["zone_key"],
                "zone_name": r["zone_name"],
                "priority": r["priority"],
                "total_from_geoapify": r["raw"],
                "qualified_4star_plus": r["filtered"],
                "enriched_by_gemini": r.get("enriched", 0),
                "existing_in_db": r["existing"],
                "matched_clients": r["client"],
                "matched_prospects": r["prospect"],
                "new_leads": r["new"],
                "saved_new": r["inserted"],
            }
            for r in results
        ],
    }


# ════════════════════════════════════════════════════════════════
# CLI
# ════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    parser = argparse.ArgumentParser(
        description="Smart Lead Hunter — discovery pipeline"
    )
    parser.add_argument("target", help="Zone key or state code")
    parser.add_argument(
        "--sync", action="store_true", help="Insert NEW leads into ExistingHotel table"
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Ignore disk cache and refetch from Geoapify",
    )
    parser.add_argument(
        "--clear-cache", action="store_true", help="Wipe cache before running"
    )
    parser.add_argument(
        "--enrich",
        action="store_true",
        help="Use Gemini to classify unknown hotels (Vertex AI)",
    )
    args = parser.parse_args()

    if args.clear_cache:
        clear_cache()
        print("Cache cleared.")

    target = args.target.strip()
    use_cache = not args.no_cache
    is_state = len(target) == 2 and target.isalpha()

    if is_state:
        run_state(
            target.upper(), sync=args.sync, use_cache=use_cache, enrich=args.enrich
        )
    elif target in ZONES:
        result = run_zone(
            ZONES[target], sync=args.sync, use_cache=use_cache, enrich=args.enrich
        )
        if result:
            print(f"\n→ NEW leads: {result['new']}")
            if not args.sync:
                print("  [dry-run] Use --sync to insert into DB.")
    else:
        upper = target.upper()
        if len(upper) == 2 and zones_by_state(upper):
            run_state(upper, sync=args.sync, use_cache=use_cache, enrich=args.enrich)
        else:
            print(f"Unknown target: {target}")
            sys.exit(1)
