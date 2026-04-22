"""
Geocode Verify — spot-check existing potential_leads coords.

Samples N leads that CURRENTLY HAVE coords, re-runs them through Geoapify,
and reports:
  - Exact match (within 100m)
  - Close match (within 5km — same neighborhood)
  - Far match (5-50km — same city, different address)
  - Very far match (>50km — different city, definitively wrong)

This tells us whether the CURRENT coords can be trusted before we decide
to backfill more leads with the same method.

Usage:
    python -m scripts.verify_geocodes                 # random 20 leads
    python -m scripts.verify_geocodes --sample 40     # larger sample
    python -m scripts.verify_geocodes --clustered     # only check clustered coord groups
    python -m scripts.verify_geocodes --by-id 1240,342,519  # specific leads

Interpretation:
  ≥80% exact/close match → existing data is reliable, safe to backfill
  <50% exact/close match → geocoding method is broken, fix before backfilling
  High "far" or "very far" → city-center fallback or state/country mismatch
"""

import argparse
import asyncio
import math
import sys
from collections import Counter
from pathlib import Path

# Bootstrap sys.path
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from sqlalchemy import text  # noqa: E402
from app.database import async_session  # noqa: E402
from app.services.lead_geo_enrichment import geocode_hotel  # noqa: E402


PACING_SECONDS = 0.25  # 4 req/sec


def haversine_km(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Distance between two coords in kilometers."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlng / 2) ** 2
    )
    c = 2 * math.asin(math.sqrt(a))
    return R * c


def classify(distance_km: float) -> str:
    if distance_km < 0.1:
        return "exact"        # within 100m
    if distance_km < 5.0:
        return "close"        # within 5km — same neighborhood
    if distance_km < 50.0:
        return "far"          # 5-50km — same city different address
    return "very_far"         # >50km — wrong city or state


async def fetch_sample(session, sample_size: int, clustered_only: bool = False, by_ids: list = None) -> list:
    """Get a sample of leads with existing coords to verify."""
    if by_ids:
        ids_str = ",".join(str(int(i)) for i in by_ids)
        query = f"""
            SELECT id, hotel_name, city, state, country, latitude, longitude
            FROM potential_leads
            WHERE id IN ({ids_str})
              AND latitude IS NOT NULL AND longitude IS NOT NULL
        """
        r = await session.execute(text(query))
        return r.fetchall()

    if clustered_only:
        # Find coords shared by 3+ leads and pick one from each cluster
        query = """
            WITH clustered AS (
                SELECT latitude, longitude, COUNT(*) as cnt
                FROM potential_leads
                WHERE latitude IS NOT NULL AND longitude IS NOT NULL
                GROUP BY latitude, longitude
                HAVING COUNT(*) >= 3
            )
            SELECT DISTINCT ON (p.latitude, p.longitude)
                p.id, p.hotel_name, p.city, p.state, p.country,
                p.latitude, p.longitude
            FROM potential_leads p
            JOIN clustered c ON p.latitude = c.latitude AND p.longitude = c.longitude
            ORDER BY p.latitude, p.longitude, p.id
        """
        r = await session.execute(text(query))
        rows = r.fetchall()
        return rows[:sample_size] if sample_size else rows

    # Random sample
    query = f"""
        SELECT id, hotel_name, city, state, country, latitude, longitude
        FROM potential_leads
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
          AND status NOT IN ('deleted', 'rejected')
          AND hotel_name IS NOT NULL AND hotel_name != ''
        ORDER BY RANDOM()
        LIMIT {int(sample_size)}
    """
    r = await session.execute(text(query))
    return r.fetchall()


async def verify(sample_size: int, clustered_only: bool, by_ids: list) -> None:
    header = "GEOCODE VERIFICATION"
    if clustered_only:
        header += " (CLUSTERED COORDS)"
    elif by_ids:
        header += f" (specific IDs: {by_ids})"
    else:
        header += f" (random sample of {sample_size})"

    print("=" * 100)
    print(header)
    print("=" * 100)

    async with async_session() as session:
        leads = await fetch_sample(session, sample_size, clustered_only, by_ids)

    if not leads:
        print("\nNo leads match the criteria.")
        return

    print(f"\nChecking {len(leads)} leads...")
    print(f"Pacing: {PACING_SECONDS}s between calls\n")

    results = []
    buckets = Counter()

    for i, row in enumerate(leads, 1):
        (lead_id, hotel_name, city, state, country, db_lat, db_lng) = row
        db_lat, db_lng = float(db_lat), float(db_lng)

        try:
            fresh = await geocode_hotel(
                hotel_name=hotel_name or "",
                city=city or "",
                state=state or "",
                country=country or "",
            )
        except Exception as ex:
            fresh = None
            print(f"  [{i:>2}/{len(leads)}] id={lead_id:>4}  {hotel_name[:40]:<42}  ERROR: {ex}")
            buckets["error"] += 1
            results.append({"row": row, "fresh": None, "distance_km": None, "class": "error"})
            await asyncio.sleep(PACING_SECONDS)
            continue

        if fresh is None:
            buckets["no_match"] += 1
            cls = "no_match"
            print(
                f"  [{i:>2}/{len(leads)}] id={lead_id:>4}  {hotel_name[:40]:<42}  "
                f"DB=({db_lat:.4f}, {db_lng:.4f})  FRESH=none  → NO MATCH NOW"
            )
            results.append({"row": row, "fresh": None, "distance_km": None, "class": cls})
        else:
            fresh_lat, fresh_lng = fresh
            distance = haversine_km(db_lat, db_lng, fresh_lat, fresh_lng)
            cls = classify(distance)
            buckets[cls] += 1
            flag = {"exact": "✓", "close": "○", "far": "△", "very_far": "✗"}[cls]
            print(
                f"  [{i:>2}/{len(leads)}] id={lead_id:>4}  {hotel_name[:40]:<42}  "
                f"DB=({db_lat:.4f}, {db_lng:.4f})  FRESH=({fresh_lat:.4f}, {fresh_lng:.4f})  "
                f"{flag} {cls:<8} Δ {distance:.1f}km"
            )
            results.append({"row": row, "fresh": fresh, "distance_km": distance, "class": cls})

        await asyncio.sleep(PACING_SECONDS)

    # ── SUMMARY ──
    total = len(leads)
    print("\n" + "=" * 100)
    print("SUMMARY")
    print("=" * 100)
    print(f"Total checked: {total}")
    print(f"  ✓ Exact      (<100m):     {buckets['exact']:>3} ({100*buckets['exact']/total:>3.0f}%)")
    print(f"  ○ Close      (<5km):      {buckets['close']:>3} ({100*buckets['close']/total:>3.0f}%)")
    print(f"  △ Far        (5-50km):    {buckets['far']:>3} ({100*buckets['far']/total:>3.0f}%)")
    print(f"  ✗ Very far   (>50km):     {buckets['very_far']:>3} ({100*buckets['very_far']/total:>3.0f}%)")
    print(f"  · No match now:           {buckets['no_match']:>3} ({100*buckets['no_match']/total:>3.0f}%)")
    print(f"  ! Error:                  {buckets['error']:>3} ({100*buckets['error']/total:>3.0f}%)")

    reliable = buckets["exact"] + buckets["close"]
    pct_reliable = 100 * reliable / total
    print()
    if pct_reliable >= 80:
        verdict = "✅ RELIABLE"
        action = "Existing coords look good. Safe to backfill missing ones with same method."
    elif pct_reliable >= 50:
        verdict = "⚠️ MIXED"
        action = "Existing coords are partly reliable. Backfill OK but re-check clustered coords separately."
    else:
        verdict = "🚨 UNRELIABLE"
        action = "Existing coords are mostly wrong. DO NOT backfill — fix geocoding method first."

    print(f"{verdict}: {reliable}/{total} reliable ({pct_reliable:.0f}%)")
    print(f"→ {action}")

    if buckets["very_far"] > 0:
        print(f"\n🚨 Suspicious 'very far' mismatches (>50km apart):")
        for r in results:
            if r["class"] == "very_far":
                row = r["row"]
                (lid, name, city, state, country, db_lat, db_lng) = row
                fresh = r["fresh"]
                print(
                    f"   id={lid}: {name}"
                    f"\n      DB says:    ({db_lat:.4f}, {db_lng:.4f})"
                    f"\n      Fresh says: ({fresh[0]:.4f}, {fresh[1]:.4f})"
                    f"\n      → {r['distance_km']:.0f} km apart — one of these is wrong"
                )


def main():
    parser = argparse.ArgumentParser(description="Verify existing potential_leads geocodes")
    parser.add_argument(
        "--sample",
        type=int,
        default=20,
        help="Random sample size (default 20)",
    )
    parser.add_argument(
        "--clustered",
        action="store_true",
        help="Only verify clustered coord groups (3+ leads sharing coords)",
    )
    parser.add_argument(
        "--by-id",
        type=str,
        default=None,
        help="Comma-separated lead IDs to verify specifically",
    )
    args = parser.parse_args()

    by_ids = None
    if args.by_id:
        by_ids = [int(x) for x in args.by_id.split(",") if x.strip()]

    asyncio.run(verify(args.sample, args.clustered, by_ids))


if __name__ == "__main__":
    main()
