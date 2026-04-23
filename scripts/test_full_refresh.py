"""
Full Refresh CLI — runs Smart Fill / Full Refresh from the command line with
verbose stage-by-stage logging. Lets you watch the pipeline work without the
UI's noise.

Usage:
    python -m scripts.test_full_refresh --lead-id 1252
    python -m scripts.test_full_refresh --lead-id 1252 --mode smart
    python -m scripts.test_full_refresh --lead-id 1252 --dry-run

Flags:
    --lead-id N   : which lead to refresh (required)
    --mode        : "full" (default) | "smart" (only missing fields)
    --dry-run     : run enrichment but DO NOT save to DB — just show result
    --quiet       : skip the big "BEFORE → AFTER" diff output

What you see:
    1. Current DB state (before)
    2. Classification call (project_type)
    3. Queries being built + searched
    4. Gemini extraction raw response
    5. Field-by-field mapping
    6. New DB state (after)
    7. Diff summary
"""

import argparse
import asyncio
import logging
import os
import sys
from pathlib import Path

# Bootstrap sys.path
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from sqlalchemy import text, select  # noqa: E402
from app.database import async_session  # noqa: E402
from app.models.potential_lead import PotentialLead  # noqa: E402
from app.services.lead_data_enrichment import enrich_lead_data  # noqa: E402
from app.services.utils import get_timeline_label, local_now  # noqa: E402


# Verbose logging — show everything from our enrichment modules
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s:%(lineno)d | %(message)s",
    datefmt="%H:%M:%S",
)
# Quiet noisy 3rd-party loggers
for noisy in ("httpx", "httpcore", "sqlalchemy", "asyncio"):
    logging.getLogger(noisy).setLevel(logging.WARNING)


# Fields we care about showing in before/after diff
TRACKED_FIELDS = [
    "hotel_name", "status", "brand", "brand_tier", "project_type",
    "opening_date", "timeline_label", "room_count",
    "city", "state", "country",
    "management_company", "owner", "developer",
    "description",
]


def _snapshot(lead) -> dict:
    """Take a snapshot of the lead's current trackable field values."""
    return {f: getattr(lead, f, None) for f in TRACKED_FIELDS}


def _print_header(text_str: str) -> None:
    print()
    print("=" * 80)
    print(f"  {text_str}")
    print("=" * 80)


def _print_snapshot(label: str, snap: dict) -> None:
    print(f"\n── {label} ──")
    for field in TRACKED_FIELDS:
        val = snap.get(field)
        if val is None or val == "":
            display = "(empty)"
        elif isinstance(val, str) and len(val) > 80:
            display = val[:77] + "..."
        else:
            display = repr(val) if isinstance(val, str) else str(val)
        print(f"  {field:<22} = {display}")


def _print_diff(before: dict, after: dict) -> None:
    print("\n── CHANGES ──")
    changed = False
    for field in TRACKED_FIELDS:
        b, a = before.get(field), after.get(field)
        if b != a:
            changed = True
            b_display = "(empty)" if b in (None, "") else repr(b)
            a_display = "(empty)" if a in (None, "") else repr(a)
            print(f"  {field:<22}:  {b_display}")
            print(f"  {' ' * 22}→  {a_display}")
    if not changed:
        print("  (no fields changed)")


async def run_refresh(lead_id: int, mode: str, dry_run: bool, quiet: bool) -> None:
    async with async_session() as session:
        # ── STEP 1: Load lead ──
        result = await session.execute(
            select(PotentialLead).where(PotentialLead.id == lead_id)
        )
        lead = result.scalar_one_or_none()
        if not lead:
            print(f"❌ Lead {lead_id} not found")
            return

        _print_header(f"FULL REFRESH TEST — Lead {lead_id}: {lead.hotel_name}")
        before = _snapshot(lead)
        _print_snapshot("BEFORE (current DB state)", before)
        print(f"\nMode: {mode}   Dry-run: {dry_run}")

        # ── STEP 2: Run enrichment (verbose logs from inside) ──
        _print_header("ENRICHMENT PIPELINE")
        print("(Watch for: [Classify] → [Queries] → [Search] → [Extract] → [Map])\n")

        try:
            enriched = await enrich_lead_data(
                hotel_name=lead.hotel_name,
                city=lead.city or "",
                state=lead.state or "",
                brand=lead.brand or "",
                current_opening_date=lead.opening_date or "",
                current_brand_tier=lead.brand_tier or "",
                current_room_count=lead.room_count or 0,
                mode=mode,
            )
        except Exception as ex:
            print(f"\n❌ Enrichment crashed: {ex}")
            import traceback
            traceback.print_exc()
            return

        # ── STEP 3: Show what enrichment returned ──
        _print_header("ENRICHMENT RESULT")
        changes = enriched.get("changes", []) or []
        print(f"Confidence:  {enriched.get('confidence', '?')}")
        print(f"Source URL:  {enriched.get('source_url', '(none)')}")
        print(f"Changes:     {changes if changes else '(none)'}")
        if not changes:
            print("\n⚠️  Enrichment returned no changes. Nothing to save.")
            return

        print("\nReturned fields:")
        for k, v in enriched.items():
            if k in ("changes", "confidence", "source_url"):
                continue
            display = repr(v) if isinstance(v, str) and len(v) < 100 else str(v)[:100]
            print(f"  {k:<22} = {display}")

        # ── STEP 4: Apply to DB (or skip if dry-run) ──
        if dry_run:
            print("\n[DRY RUN] Skipping DB update.")
            return

        _print_header("APPLYING TO DB")

        # Minimal write loop — mimics scraping.py logic
        REOPENING_TYPES = {
            "renovation", "rebrand", "reopening",
            "conversion", "ownership_change",
        }
        proj_type = (enriched.get("project_type") or "").strip().lower()
        is_live_reopening = (
            proj_type in REOPENING_TYPES
            and enriched.get("reopening_date")
        )

        # project_type
        if proj_type and (mode == "full" or not lead.project_type):
            lead.project_type = proj_type

        # already_opened handling
        if enriched.get("already_opened") and not is_live_reopening:
            lead.status = "expired"
            lead.opening_date = enriched.get("opened_date", lead.opening_date)
            lead.timeline_label = "EXPIRED"
            print("  Path: already_opened (not a live reopening) → status=expired")
        elif is_live_reopening:
            reopening = enriched["reopening_date"]
            lead.opening_date = reopening
            lead.timeline_label = get_timeline_label(reopening)
            lead.project_type = proj_type
            if lead.status == "expired" and lead.timeline_label not in ("EXPIRED", "LATE"):
                lead.status = "new"
                print(f"  Path: live reopening → un-expired, status=new, date={reopening}")
            else:
                print(f"  Path: live reopening → date={reopening}, timeline={lead.timeline_label}")

        # opening_date (if not already handled by reopening path)
        if "opening_date" in enriched and not is_live_reopening:
            lead.opening_date = enriched["opening_date"]
            lead.timeline_label = get_timeline_label(enriched["opening_date"])
            if lead.timeline_label == "EXPIRED":
                lead.status = "expired"

        # Scalar fields — respect user edits
        INVALID_SENTINELS = {"", "unknown", "none", "n/a", "tbd"}
        VALID_TIERS = {
            "tier1_ultra_luxury", "tier2_luxury", "tier3_upper_upscale",
            "tier4_upscale", "tier5_upper_midscale", "tier6_midscale",
            "tier7_economy",
        }
        if "brand_tier" in enriched:
            new_tier = (enriched["brand_tier"] or "").strip().lower()
            current_tier = (lead.brand_tier or "").strip().lower()
            if new_tier in VALID_TIERS:
                lead.brand_tier = enriched["brand_tier"]
            elif current_tier in INVALID_SENTINELS and new_tier:
                lead.brand_tier = enriched["brand_tier"]
        if "room_count" in enriched and enriched["room_count"]:
            try:
                rc = int(enriched["room_count"])
                if rc > 0 and (mode == "full" or not lead.room_count):
                    lead.room_count = rc
            except (TypeError, ValueError):
                pass
        if "brand" in enriched:
            nb = (enriched["brand"] or "").strip()
            if nb and nb.lower() not in INVALID_SENTINELS:
                lead.brand = nb
        if "city" in enriched and (mode == "full" or not lead.city):
            lead.city = enriched["city"]
        if "state" in enriched and (mode == "full" or not lead.state):
            lead.state = enriched["state"]
        if "country" in enriched and (mode == "full" or not lead.country):
            lead.country = enriched["country"]
        if "description" in enriched and (mode == "full" or not lead.description):
            lead.description = enriched["description"]

        # Entity fields (management_company / owner / developer)
        for entity_field in ("management_company", "owner", "developer"):
            if entity_field in enriched:
                val = enriched[entity_field]
                if val and (mode == "full" or not getattr(lead, entity_field)):
                    setattr(lead, entity_field, val)

        # former_names
        if "former_names" in enriched:
            lead.former_names = enriched["former_names"]

        lead.updated_at = local_now()
        await session.commit()
        print("  ✅ Committed to DB")

        # ── STEP 5: Show after state + diff ──
        await session.refresh(lead)
        after = _snapshot(lead)
        if not quiet:
            _print_snapshot("AFTER (new DB state)", after)
            _print_diff(before, after)

        _print_header("DONE")


def main():
    parser = argparse.ArgumentParser(description="Run Full Refresh from CLI with verbose logs")
    parser.add_argument("--lead-id", type=int, required=True, help="Lead ID to refresh")
    parser.add_argument("--mode", choices=("full", "smart"), default="full")
    parser.add_argument("--dry-run", action="store_true", help="Don't save to DB")
    parser.add_argument("--quiet", action="store_true", help="Skip before/after diff")
    args = parser.parse_args()

    asyncio.run(run_refresh(
        lead_id=args.lead_id,
        mode=args.mode,
        dry_run=args.dry_run,
        quiet=args.quiet,
    ))


if __name__ == "__main__":
    main()
