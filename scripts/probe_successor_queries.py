#!/usr/bin/env python3
r"""probe_successor_queries.py -- see EXACTLY what each successor query returns,
so we can tweak before deploying. READ-ONLY, no LLM, no writes.

Usage:
    python scripts/probe_successor_queries.py --org "Conrad Fort Lauderdale Beach" --title "F&B Manager"
"""
from __future__ import annotations
import argparse, asyncio, sys
from pathlib import Path
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _dept_for(title: str) -> str:
    t = title.lower()
    if any(k in t for k in ("f&b", "food", "beverage", "culinary", "restaurant")):
        return "food beverage director OR manager OR head"
    if any(k in t for k in ("human res", "hr", "people", "talent")):
        return "human resources director OR manager OR head"
    if any(k in t for k in ("sales", "revenue", "commercial")):
        return "director of sales OR revenue manager"
    if "general manager" in t or "gm" in t:
        return "general manager"
    return f"{title} OR director"


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--org", required=True)
    ap.add_argument("--title", required=True)
    args = ap.parse_args()
    from app.services.contact_enrichment import _search_serper

    dept = _dept_for(args.title)
    queries = {
        "Q1 appointment-news": f'"{args.org}" "{args.title}" appointed OR named OR "new" OR announces 2025 OR 2026',
        "Q2 current": f'"{args.org}" {args.title} current 2026',
        "Q3 dept-leadership": f'"{args.org}" {dept} linkedin',
    }
    for label, q in queries.items():
        print(f"\n===== {label}\nQUERY: {q}")
        try:
            res = await _search_serper(q, max_results=6)
        except Exception as e:
            print(f"  ERROR: {e}"); continue
        if not res:
            print("  (no results)")
        for r in res:
            print(f"  - {r.get('title','')[:70]}")
            print(f"      {r.get('snippet','')[:120]}")
            print(f"      {r.get('url','')}")


if __name__ == "__main__":
    asyncio.run(main())
