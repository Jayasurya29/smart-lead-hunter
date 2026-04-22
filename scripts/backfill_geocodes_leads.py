"""
Geocode Backfill — populate missing lat/lng on potential_leads.

Targets rows where:
  - latitude IS NULL OR longitude IS NULL
  - status NOT IN ('deleted', 'rejected') — only worth geocoding active leads

Uses existing app.services.lead_geo_enrichment.geocode_hotel() which:
  - Uses Geoapify structured search (city + state + country, not free text)
  - Validates coords against state bounding boxes (prevents Rogers MN → Rogers AR)
  - Returns None if Geoapify can't confidently resolve the address

What this script does NOT do:
  - Does NOT re-geocode rows that ALREADY have coords (even if clustered)
    → use audit_geocodes.py output to identify clustered groups and
      target those separately if you want un-clustering
  - Does NOT touch sap_clients (separate script needed)
  - Does NOT use billing addresses (per memory: "SAP billing addresses
    must never be used for geocoding")

Usage:
    python -m scripts.backfill_geocodes_leads
    python -m scripts.backfill_geocodes_leads --dry-run
    python -m scripts.backfill_geocodes_leads --limit 50

Flags:
    --dry-run  : report what WOULD be geocoded without making API calls
    --limit N  : only process first N rows (good for testing)

Rate limiting:
    Geoapify allows ~5 req/sec on free tier, higher on paid. We pace to
    4 req/sec to stay well under limits. ~400 leads = ~2 minutes.

Cost:
    Each geocode = 15 Geoapify credits. 136 leads → ~2,040 credits.
    Current pricing: credits are cheap (free tier = 3,000/day).
"""

import argparse
import asyncio
import sys
from collections import Counter
from pathlib import Path

# Bootstrap sys.path so `app.*` imports work
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from sqlalchemy import text  # noqa: E402
from app.database import async_session  # noqa: E402
from app.services.lead_geo_enrichment import geocode_hotel  # noqa: E402
from app.services.utils import local_now  # noqa: E402


PACING_SECONDS = 0.25  # 4 req/sec — comfortable margin under Geoapify limits


async def fetch_leads_needing_geocode(session, limit: int = None) -> list:
    """Fetch leads missing lat OR lng, ordered by most recent."""
    query = """
        SELECT id, hotel_name, city, state, country
        FROM potential_leads
        WHERE (latitude IS NULL OR longitude IS NULL)
          AND status NOT IN ('deleted', 'rejected')
          AND hotel_name IS NOT NULL
          AND hotel_name != ''
        ORDER BY updated_at DESC NULLS LAST, id DESC
    """
    if limit:
        query += f" LIMIT {int(limit)}"

    r = await session.execute(text(query))
    return r.fetchall()


async def backfill(dry_run: bool = False, limit: int = None) -> None:
    print("=" * 70)
    print(f"GEOCODE BACKFILL — potential_leads {'(DRY RUN)' if dry_run else ''}")
    print("=" * 70)

    async with async_session() as session:
        leads = await fetch_leads_needing_geocode(session, limit=limit)

        if not leads:
            print("\nNo leads need geocoding. Either all have coords, or DB is empty.")
            return

        print(f"\nLeads needing geocode: {len(leads)}")
        print(f"Pacing: {PACING_SECONDS}s between calls (~{int(1/PACING_SECONDS)} req/sec)")
        print(f"Est. time: {len(leads) * PACING_SECONDS / 60:.1f} minutes")
        print()

        if dry_run:
            print("DRY RUN — showing first 20:")
            for lead in leads[:20]:
                print(f"  id={lead[0]:>4}  {lead[1][:50]:<52} [{lead[2] or '?'}, {lead[3] or '?'}, {lead[4] or '?'}]")
            print(f"\n(… {max(0, len(leads) - 20)} more would be processed)")
            return

        # ── RUN THE BACKFILL ──
        stats = Counter()
        stats["total"] = len(leads)
        results_batch = []  # accumulate updates, commit in batches of 25

        for i, lead in enumerate(leads, start=1):
            lead_id, hotel_name, city, state, country = lead
            try:
                coords = await geocode_hotel(
                    hotel_name=hotel_name or "",
                    city=city or "",
                    state=state or "",
                    country=country or "",
                )
                if coords:
                    lat, lng = coords
                    results_batch.append((lead_id, lat, lng))
                    stats["geocoded"] += 1
                    status = f"✓ ({lat:.4f}, {lng:.4f})"
                else:
                    stats["no_match"] += 1
                    status = "✗ no confident match"
            except Exception as ex:
                stats["errors"] += 1
                status = f"! error: {ex}"

            print(f"  [{i:>3}/{len(leads)}] id={lead_id:>4}  {hotel_name[:45]:<47}  {status}")

            # Commit every 25 updates to avoid losing progress on interrupt
            if len(results_batch) >= 25:
                await _commit_batch(session, results_batch)
                results_batch.clear()

            await asyncio.sleep(PACING_SECONDS)

        # Final batch
        if results_batch:
            await _commit_batch(session, results_batch)

        # ── SUMMARY ──
        print("\n" + "=" * 70)
        print("BACKFILL COMPLETE")
        print("=" * 70)
        print(f"Total processed:   {stats['total']}")
        print(f"Geocoded:          {stats['geocoded']:>4} ({100 * stats['geocoded'] / stats['total']:.0f}%)")
        print(f"No confident match: {stats['no_match']:>4} ({100 * stats['no_match'] / stats['total']:.0f}%)")
        print(f"Errors:            {stats['errors']:>4}")
        print()
        if stats["geocoded"]:
            print(f"✓ {stats['geocoded']} leads now have coords. Re-run audit_geocodes.py to verify.")


async def _commit_batch(session, batch: list[tuple]) -> None:
    """Commit a batch of (id, lat, lng) tuples to the DB."""
    if not batch:
        return
    now = local_now()
    for lead_id, lat, lng in batch:
        await session.execute(
            text("""
                UPDATE potential_leads
                SET latitude = :lat, longitude = :lng, updated_at = :now
                WHERE id = :id
            """),
            {"id": lead_id, "lat": lat, "lng": lng, "now": now},
        )
    await session.commit()


def main():
    parser = argparse.ArgumentParser(description="Backfill missing geocodes on potential_leads")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would be geocoded without making API calls",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Only process first N leads (good for testing)",
    )
    args = parser.parse_args()
    asyncio.run(backfill(dry_run=args.dry_run, limit=args.limit))


if __name__ == "__main__":
    main()
