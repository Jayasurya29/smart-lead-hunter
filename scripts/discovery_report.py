"""
Query Intelligence Report
==========================
Print a summary of discovery query performance: top performers, junk queries,
retirement candidates, paused retries.

Usage:
    python -m scripts.discovery_report           # full report
    python -m scripts.discovery_report --top 30  # top 30 queries
    python -m scripts.discovery_report --junk    # only show junk
    python -m scripts.discovery_report --csv     # CSV output for spreadsheet

This reads from the `discovery_query_stats` table populated by the discovery
engine (scripts/discover_sources.py). If the table is empty, run discovery
at least once first.
"""

import argparse
import asyncio
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from sqlalchemy import select, desc, func  # noqa: E402

from app.database import async_session  # noqa: E402
from app.models.discovery_query_stat import DiscoveryQueryStat  # noqa: E402
from app.services.query_intelligence import build_report  # noqa: E402


async def print_full_report(top_n: int):
    async with async_session() as session:
        report = await build_report(session, top_n=top_n)
        print(report)


async def print_csv():
    async with async_session() as session:
        result = await session.execute(
            select(DiscoveryQueryStat).order_by(
                desc(DiscoveryQueryStat.total_new_leads),
                desc(DiscoveryQueryStat.total_new_sources),
            )
        )
        rows = result.scalars().all()

    print(
        "query_text,status,total_runs,total_new_sources,total_new_leads,"
        "consecutive_zero_runs,first_run_at,last_run_at,last_success_at"
    )
    for s in rows:
        # Escape quotes for CSV
        q = (s.query_text or "").replace('"', '""')
        print(
            f'"{q}",{s.status},{s.total_runs},{s.total_new_sources},'
            f"{s.total_new_leads},{s.consecutive_zero_runs},"
            f"{s.first_run_at or ''},{s.last_run_at or ''},"
            f"{s.last_success_at or ''}"
        )


async def print_junk_only():
    async with async_session() as session:
        result = await session.execute(
            select(DiscoveryQueryStat)
            .where(DiscoveryQueryStat.status == "junk")
            .order_by(desc(DiscoveryQueryStat.total_runs))
        )
        junk = result.scalars().all()

    if not junk:
        print("No junk queries yet — nothing has been retired.")
        return

    print("═" * 70)
    print(f"  JUNK QUERIES ({len(junk)}) — skipped on normal runs")
    print("═" * 70)
    for s in junk:
        retry = s.paused_until.strftime("%Y-%m-%d") if s.paused_until else "soon"
        print(f"  [{s.total_runs} runs, retry after {retry}]")
        print(f"    {s.query_text}")
        print()


def main():
    parser = argparse.ArgumentParser(description="Discovery query intelligence report")
    parser.add_argument("--top", type=int, default=15, help="Show top N queries")
    parser.add_argument(
        "--junk", action="store_true", help="Only show junk queries"
    )
    parser.add_argument(
        "--csv", action="store_true", help="CSV output (pipe to file for Excel)"
    )
    args = parser.parse_args()

    if args.csv:
        asyncio.run(print_csv())
    elif args.junk:
        asyncio.run(print_junk_only())
    else:
        asyncio.run(print_full_report(args.top))


if __name__ == "__main__":
    main()
