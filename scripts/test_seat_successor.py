#!/usr/bin/env python3
r"""test_seat_successor.py -- prove Phase 3 'fill the seat' (READ-ONLY).

For confirmed movers (contacts that have a `former` affiliation written by the
Where-now Apply flow), ask find_seat_successor: who holds that vacated role at
the property now? Prints the finding. Writes NOTHING.

USAGE:
    python scripts/test_seat_successor.py                 # auto-pick movers from DB
    python scripts/test_seat_successor.py --n 5
    python scripts/test_seat_successor.py --org "The Tides Inn" --title "Human Resources Manager"
    python scripts/test_seat_successor.py --ids 3491,1234

A mover's vacated seat = (former affiliation's account_name) + (the title the
contact held). We read the title from the contacts row; the former org from
contact_affiliations(relationship='former').
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from sqlalchemy import text  # noqa: E402

from app.services.current_employer import find_seat_successor  # noqa: E402


async def _movers_from_db(limit: int, ids: list[int] | None) -> list[dict]:
    from app.database import async_session

    rows: list[dict] = []
    async with async_session() as s:
        if ids:
            q = text(
                "SELECT c.id, c.title, c.organization AS new_org, "
                "  COALESCE(NULLIF(TRIM(CONCAT_WS(' ', c.first_name, c.last_name)), ''), c.display_name, '') AS holder, "
                "  a.account_name AS former_org "
                "FROM contacts c "
                "JOIN contact_affiliations a "
                "  ON a.person_id = c.id AND a.person_type = 'contact' "
                " AND a.relationship = 'former' "
                "WHERE c.id = ANY(:ids)"
            )
            res = await s.execute(q, {"ids": ids})
        else:
            q = text(
                "SELECT c.id, c.title, c.organization AS new_org, "
                "  COALESCE(NULLIF(TRIM(CONCAT_WS(' ', c.first_name, c.last_name)), ''), c.display_name, '') AS holder, "
                "  a.account_name AS former_org "
                "FROM contacts c "
                "JOIN contact_affiliations a "
                "  ON a.person_id = c.id AND a.person_type = 'contact' "
                " AND a.relationship = 'former' "
                "WHERE a.account_name IS NOT NULL AND a.account_name <> '' "
                "ORDER BY a.created_at DESC NULLS LAST "
                "LIMIT :lim"
            )
            res = await s.execute(q, {"lim": limit})
        for r in res:
            rows.append(
                {
                    "id": r.id,
                    "title": (r.title or "").strip(),
                    "new_org": (r.new_org or "").strip(),
                    "former_org": (r.former_org or "").strip(),
                    "holder": (r.holder or "").strip(),
                }
            )
    return rows


async def _run(args) -> None:
    if args.org and args.title:
        cases = [
            {
                "id": None,
                "title": args.title,
                "new_org": "",
                "former_org": args.org,
                "holder": args.former,
            }
        ]
    else:
        ids = [int(x) for x in args.ids.split(",")] if args.ids else None
        cases = await _movers_from_db(args.n, ids)

    if not cases:
        print("No confirmed movers found (no contacts with a 'former' affiliation).")
        print("Tip: run Where-now -> Apply on a stale contact first, or pass --org/--title.")
        return

    print(f"Probing {len(cases)} vacated seat(s). READ-ONLY -- no writes.\n")
    for c in cases:
        former_holder = c.get("holder", "")
        org, title = c["former_org"], c["title"]
        hdr = f"id={c['id']}" if c["id"] else "(manual)"
        who = f" (vacated by {former_holder})" if former_holder else ""
        print(f"--- {hdr}  seat: {title or '(no title on file)'} @ {org}{who}")
        if not title:
            print("    SKIP: contact has no title on file -- cannot identify the seat.\n")
            continue
        try:
            res = await find_seat_successor(org=org, title=title, former_holder=former_holder)
        except Exception as e:
            print(f"    ERROR: {e}\n")
            continue
        if res.get("found"):
            print(f"    SUCCESSOR: {res['successor_name']}  ({res['successor_title']})")
            print(f"    evidence:  {res['evidence']}")
            if res.get("citations"):
                print(f"    source:    {res['citations'][0]}")
        else:
            print(f"    no clear successor found.  (model said: {res.get('evidence') or 'n/a'})")
        print()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=5, help="how many movers to probe (default 5)")
    ap.add_argument("--ids", default="", help="comma-separated contact ids")
    ap.add_argument("--org", default="", help="manual: vacated org (use with --title)")
    ap.add_argument("--title", default="", help="manual: vacated title (use with --org)")
    ap.add_argument("--former", default="", help="manual: name of the person who left (improves accuracy)")
    args = ap.parse_args()
    asyncio.run(_run(args))
    return 0


if __name__ == "__main__":
    sys.exit(main())
