#!/usr/bin/env python3
"""check_enriched.py — eyeball the most recently enriched contacts.

Read-only. Uses the app's own DB connection (the same one smart_fill and the
API use), so no psql / DATABASE_URL juggling needed.

    python scripts/check_enriched.py            # last 25 enriched
    python scripts/check_enriched.py --limit 50
    python scripts/check_enriched.py --linkedin # only show ones with a LinkedIn URL
"""

import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

from sqlalchemy import text  # noqa: E402

from app.database import async_session  # noqa: E402


def _trim(v, n):
    s = "" if v is None else str(v)
    s = s.replace("\n", " ").strip()
    return s if len(s) <= n else s[: n - 1] + "…"


async def run(limit: int, linkedin_only: bool) -> None:
    where = "enriched_at IS NOT NULL"
    if linkedin_only:
        where += " AND COALESCE(linkedin_url,'') <> ''"
    sql = text(
        f"""
        SELECT id, COALESCE(display_name, first_name || ' ' || last_name, email) AS who,
               organization, inferred_role, seniority, contact_category,
               procurement_priority, enrichment_confidence,
               CASE WHEN COALESCE(linkedin_url,'') <> '' THEN 'yes' ELSE '-' END AS li,
               to_char(enriched_at, 'MM-DD HH24:MI') AS enriched
        FROM contacts WHERE {where}
        ORDER BY enriched_at DESC LIMIT :lim
        """
    )
    async with async_session() as session:
        rows = (await session.execute(sql, {"lim": limit})).all()

    if not rows:
        print("No enriched contacts found yet.")
        return

    cols = [
        ("id", 6), ("who", 24), ("organization", 26), ("inferred_role", 26),
        ("seniority", 10), ("cat", 9), ("pri", 5), ("conf", 5), ("li", 3),
        ("enriched", 11),
    ]
    header = "  ".join(name.ljust(w) for name, w in cols)
    print("\n" + header)
    print("  ".join("-" * w for _, w in cols))
    for r in rows:
        conf = "" if r.enrichment_confidence is None else f"{float(r.enrichment_confidence):.2f}"
        line = "  ".join([
            _trim(r.id, 6).ljust(6),
            _trim(r.who, 24).ljust(24),
            _trim(r.organization, 26).ljust(26),
            _trim(r.inferred_role, 26).ljust(26),
            _trim(r.seniority, 10).ljust(10),
            _trim(r.contact_category, 9).ljust(9),
            _trim(r.procurement_priority, 5).ljust(5),
            conf.ljust(5),
            _trim(r.li, 3).ljust(3),
            _trim(r.enriched, 11).ljust(11),
        ])
        print(line)
    print(f"\n{len(rows)} row(s).  'li' = has LinkedIn URL.  Open any on the "
          "Contacts page to verify the human matches.\n")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--limit", type=int, default=25)
    p.add_argument("--linkedin", action="store_true", help="Only rows with a LinkedIn URL")
    args = p.parse_args()
    asyncio.run(run(args.limit, args.linkedin))


if __name__ == "__main__":
    main()
