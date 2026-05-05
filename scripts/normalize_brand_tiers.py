"""
SMART LEAD HUNTER — Brand Tier Normalization (one-shot)
=========================================================

Problem
-------
Earlier code paths (Smart Fill grounding prompts, lead_data_enrichment
validators, _apply_enrichment_to_lead apply layer, rescore VALID_TIERS,
existing_hotel_scorer's defensive map) accepted non-canonical 7-tier
values:

    tier5_upper_midscale, tier6_midscale, tier7_economy

JA Uniforms only targets 4-star+ properties — the canonical brand_tier
set is 5-tier:

    tier1_ultra_luxury, tier2_luxury, tier3_upper_upscale,
    tier4_upscale, tier5_skip

This script normalizes any non-canonical values found in the live DB
to `tier5_skip`. It does NOT delete those rows — sales reviews them and
either re-classifies (if the lead is actually upscale+) or rejects (if
correctly mid-market).

What this script does
---------------------
1. Reports how many rows in `potential_leads` and `existing_hotels` have
   a non-canonical tier value.
2. With `--apply`, rewrites them to `tier5_skip` and adds a note in the
   row's `notes` field so sales sees what happened.
3. Does NOT run automatically — you invoke it from PowerShell once after
   deploying the patch.

Usage
-----
    # Dry run (default — reports counts, makes no changes)
    python -m scripts.normalize_brand_tiers

    # Apply the rewrite
    python -m scripts.normalize_brand_tiers --apply

Created: 2026-05-05 (audit fix for bug #7)
"""
from __future__ import annotations

import argparse
import asyncio
import logging
from datetime import datetime, timezone

from sqlalchemy import select

from app.database import async_session
from app.models.potential_lead import PotentialLead
from app.models.existing_hotel import ExistingHotel

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

NON_CANONICAL = {"tier5_upper_midscale", "tier6_midscale", "tier7_economy"}
NORMALIZED_TO = "tier5_skip"


async def _normalize_table(model, table_label: str, apply: bool) -> dict:
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    note_marker = (
        f"[brand-tier normalized {stamp}]: original tier was non-canonical, "
        f"mapped to {NORMALIZED_TO} per canonical 5-tier system"
    )

    async with async_session() as session:
        rows = (
            await session.execute(
                select(model).where(model.brand_tier.in_(list(NON_CANONICAL)))
            )
        ).scalars().all()

        result = {
            "table": table_label,
            "rows_found": len(rows),
            "rows_changed": 0,
            "by_tier": {},
        }

        for r in rows:
            result["by_tier"][r.brand_tier] = result["by_tier"].get(r.brand_tier, 0) + 1

        if not apply:
            return result

        # Write phase — append the note and rewrite the tier
        for r in rows:
            old_tier = r.brand_tier
            r.brand_tier = NORMALIZED_TO
            existing_notes = (r.notes or "").strip()
            new_notes = (
                f"{existing_notes}\n{note_marker} (was {old_tier!r})"
                if existing_notes
                else f"{note_marker} (was {old_tier!r})"
            )
            r.notes = new_notes
            result["rows_changed"] += 1

        await session.commit()

    return result


async def main():
    parser = argparse.ArgumentParser(
        description="Normalize non-canonical brand_tier values to tier5_skip."
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply changes. Without this flag, the script only reports counts.",
    )
    args = parser.parse_args()

    print("=" * 72)
    print("Brand-tier normalization (canonical 5-tier system)")
    print(f"Mode: {'APPLY' if args.apply else 'DRY RUN'}")
    print("=" * 72)

    pl = await _normalize_table(PotentialLead, "potential_leads", args.apply)
    eh = await _normalize_table(ExistingHotel, "existing_hotels", args.apply)

    for r in (pl, eh):
        print(
            f"\n{r['table']}: {r['rows_found']} non-canonical row(s) found"
            f"{' — ' + str(r['rows_changed']) + ' rewritten' if args.apply else ''}"
        )
        for tier, count in sorted(r["by_tier"].items()):
            print(f"   {tier}: {count}")

    print("\n" + "=" * 72)
    if not args.apply and (pl["rows_found"] or eh["rows_found"]):
        print("Re-run with --apply to rewrite these rows to tier5_skip.")
    elif args.apply:
        print("Done. Sales should review tier5_skip rows for re-classification.")
    else:
        print("No non-canonical tier values found. Database is clean.")
    print("=" * 72)


if __name__ == "__main__":
    asyncio.run(main())
