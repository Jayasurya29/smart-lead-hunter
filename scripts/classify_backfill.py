"""
scripts/classify_backfill.py
============================
Quota-paced classification for the backfill's flood of new contacts.

The backfill creates tens of thousands of contacts. The normal classify pass
(`run_tier1(only_unknown=True)`) throws them all at Vertex in fast waves and
slams into the per-minute quota (the 429s you saw at ~2,760 calls). This wrapper
classifies in small chunks with a cooldown between each, and backs off when a
chunk looks quota-starved — so it grinds through 30k+ contacts overnight without
burning thousands of failed calls.

It reuses the SAME tested classifier (`run_tier1`), just paced from the outside.
Fully resumable: `only_unknown` means each chunk picks up whatever's still
unclassified, so you can Ctrl+C and re-run anytime.

Run as a standalone script — NOT Celery — alongside / after the backfill.

USAGE (run from repo root, venv active)
---------------------------------------
  python scripts\\classify_backfill.py                       # defaults: 300/chunk, 25s rest
  python scripts\\classify_backfill.py --chunk 200 --sleep 40   # gentler on quota
  python scripts\\classify_backfill.py --chunk 500 --sleep 15   # faster if quota allows
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


async def _count_unknown() -> int:
    from sqlalchemy import text
    from app.database import async_session

    async with async_session() as s:
        return (
            await s.execute(
                text(
                    "SELECT count(*) FROM contacts "
                    "WHERE contact_category IS NULL OR contact_category = 'unknown'"
                )
            )
        ).scalar() or 0


async def _run(chunk: int, sleep_s: float, max_sleep: float) -> None:
    from app.services.contact_tier1_enrichment import run_tier1

    remaining = await _count_unknown()
    print(f"Unclassified contacts to start: {remaining:,}\n")
    if not remaining:
        print("Nothing to classify. Done.")
        return

    totals: dict = {}
    done = 0
    cur_sleep = sleep_s
    t0 = time.time()
    i = 0
    stall = 0
    while True:
        i += 1
        try:
            summary = await run_tier1(only_unknown=True, limit=chunk)
        except Exception as e:  # noqa: BLE001 - keep grinding
            print(f"  chunk {i}: error ({str(e)[:100]}) — backing off {cur_sleep:.0f}s")
            await asyncio.sleep(cur_sleep)
            cur_sleep = min(max_sleep, cur_sleep * 1.5)
            continue

        scanned = summary.get("scanned", 0) or 0
        if scanned == 0:
            break
        enriched_n = summary.get("enriched", 0) or 0
        done += enriched_n
        # Stall guard: rows scanned but none classified means the same
        # un-classifiable row(s) keep being re-selected (e.g. a grounded
        # profile the signals pass skips). Stop instead of spinning.
        if enriched_n == 0:
            stall += 1
            if stall >= 3:
                print(f"  [stall] {scanned} row(s) scanned but not "
                      f"classifiable after {stall} tries — stopping. "
                      f"Set these few manually (or use the Junk button).")
                break
            await asyncio.sleep(2)
            continue
        stall = 0
        for k, v in (summary.get("by_category") or {}).items():
            totals[k] = totals.get(k, 0) + v
        sent = summary.get("sent_to_llm", 0) or 0
        # If the chunk was mostly LLM and likely quota-pressured, ease off a bit;
        # otherwise relax back toward the base rest.
        cur_sleep = min(max_sleep, cur_sleep * 1.3) if sent >= chunk * 0.9 else sleep_s
        print(
            f"  chunk {i}: scanned={scanned} enriched={summary.get('enriched', 0)} "
            f"by_category={summary.get('by_category')}  (rest {cur_sleep:.0f}s)"
        )
        await asyncio.sleep(cur_sleep)

    mins = (time.time() - t0) / 60.0
    print(
        f"\nDone in {mins:.1f} min — classified {done:,} contacts. "
        f"Totals by category: {totals}"
    )
    left = await _count_unknown()
    print(f"Still unclassified: {left:,}" + ("" if left else "  ✓ all clear"))


def main() -> int:
    ap = argparse.ArgumentParser(description="Quota-paced bulk classifier for backfilled contacts.")
    ap.add_argument("--chunk", type=int, default=300, help="contacts per chunk (default 300)")
    ap.add_argument("--sleep", type=float, default=25.0, help="base rest between chunks, seconds (default 25)")
    ap.add_argument("--max-sleep", type=float, default=120.0, help="cap on backoff rest, seconds (default 120)")
    args = ap.parse_args()
    try:
        asyncio.run(_run(args.chunk, args.sleep, args.max_sleep))
    except KeyboardInterrupt:
        print("\nInterrupted — safe to re-run anytime; it resumes on what's left.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
