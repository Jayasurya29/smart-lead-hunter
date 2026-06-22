#!/usr/bin/env python3
r"""test_successor_apply_dryrun.py -- prove the Phase 3 Apply fork (NO WRITES).

For each confirmed mover (contact with a 'former' affiliation), this:
  1. resolves the vacated seat (former org + title + who left),
  2. finds the successor via find_seat_successor (Serper+Gemini),
  3. resolves whether the former property is a known existing_hotel / lead,
  4. PRINTS the exact action it WOULD take:
       - STUB:  create/merge a lead_contact for the successor at that property
                (+ a successor_of affiliation link back to the mover)
       - NOTE:  property not in pipeline -> record finding as a note/edge on
                the original contact only (no orphan stub; XOR constraint).
       - DEDUP: successor already exists at the property -> would merge, not create.

Writes NOTHING. This is the proof step before wiring the endpoint.

USAGE:
    python scripts/test_successor_apply_dryrun.py
    python scripts/test_successor_apply_dryrun.py --n 10
    python scripts/test_successor_apply_dryrun.py --ids 3491
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


def _norm_name(s: str) -> str:
    """Mirror the lead-gen person-name normalization closely enough for dedup preview."""
    s = (s or "").lower().strip()
    for ch in (",", ".", "'", '"'):
        s = s.replace(ch, " ")
    # drop common credential suffixes
    toks = [t for t in s.split() if t not in {"jr", "sr", "ii", "iii", "shrm", "cp", "mba", "phd"}]
    return " ".join(toks)


async def _movers(limit: int, ids: list[int] | None) -> list[dict]:
    from app.database import async_session

    out: list[dict] = []
    async with async_session() as s:
        base = (
            "SELECT c.id, "
            "  COALESCE(NULLIF(TRIM(CONCAT_WS(' ', c.first_name, c.last_name)), ''), c.display_name, '') AS holder, "
            "  c.title, c.organization AS new_org, a.account_name AS former_org "
            "FROM contacts c "
            "JOIN contact_affiliations a "
            "  ON a.person_id = c.id AND a.person_type='contact' AND a.relationship='former' "
            "WHERE a.account_name IS NOT NULL AND a.account_name <> '' "
        )
        if ids:
            res = await s.execute(text(base + "AND c.id = ANY(:ids)"), {"ids": ids})
        else:
            res = await s.execute(
                text(base + "ORDER BY a.created_at DESC NULLS LAST LIMIT :lim"), {"lim": limit}
            )
        for r in res:
            out.append(
                {
                    "id": r.id,
                    "holder": (r.holder or "").strip(),
                    "title": (r.title or "").strip(),
                    "new_org": (r.new_org or "").strip(),
                    "former_org": (r.former_org or "").strip(),
                }
            )
    return out


async def _resolve_property(session, org: str) -> tuple[str | None, int | None]:
    """Return ('existing_hotel', id) | ('lead', id) | (None, None)."""
    r = await session.execute(
        text("SELECT id FROM existing_hotels WHERE lower(name)=lower(:o) LIMIT 1"), {"o": org}
    )
    hid = r.scalar()
    if hid:
        return "existing_hotel", hid
    r = await session.execute(
        text("SELECT id FROM potential_leads WHERE lower(hotel_name)=lower(:o) LIMIT 1"), {"o": org}
    )
    lid = r.scalar()
    if lid:
        return "lead", lid
    return None, None


async def _existing_stub(session, parent_kind: str, parent_id: int, succ_name: str) -> dict | None:
    col = "existing_hotel_id" if parent_kind == "existing_hotel" else "lead_id"
    r = await session.execute(
        text(f"SELECT id, name, title FROM lead_contacts WHERE {col} = :pid"), {"pid": parent_id}
    )
    target = _norm_name(succ_name)
    for row in r:
        if _norm_name(row.name) == target:
            return {"id": row.id, "name": row.name, "title": row.title}
    return None


async def _run(args) -> None:
    from app.database import async_session

    cases = await _movers(args.n, [int(x) for x in args.ids.split(",")] if args.ids else None)
    if not cases:
        print("No confirmed movers (no 'former' affiliations). Run Where-now -> Apply first.")
        return

    print(f"DRY-RUN over {len(cases)} mover(s). NO WRITES.\n")
    async with async_session() as s:
        for c in cases:
            org, title, holder = c["former_org"], c["title"], c["holder"]
            print(f"=== contact id={c['id']}  {holder or '(name?)'}  ::  seat '{title or '(no title)'}' @ {org}")
            print(f"    (they moved to: {c['new_org'] or '(unknown)'})")

            if not title:
                print("    -> SKIP: no title on file; cannot identify the vacated seat.\n")
                continue

            res = await find_seat_successor(org=org, title=title, former_holder=holder)
            if not res.get("found"):
                print(f"    -> NO SUCCESSOR FOUND (model: {res.get('evidence') or 'n/a'})")
                print("       would record nothing.\n")
                continue

            succ = res["successor_name"]
            succ_title = res["successor_title"]
            print(f"    successor: {succ}  ({succ_title})")
            src = res["citations"][0] if res.get("citations") else "serper"

            kind, pid = await _resolve_property(s, org)
            # Always: a successor_of link on the original contact (warm path).
            print(f"    WOULD LINK: contact_affiliations row on id={c['id']}:")
            print(f"       relationship='successor', account_name='{succ}', "
                  f"notes='{succ} now holds {title} at {org} (replaced {holder or 'prior contact'})', "
                  f"source='seat_successor'")

            if kind is None:
                print(f"    WOULD NOTE ONLY: '{org}' is NOT a known hotel/lead -> no stub created "
                      f"(lead_id XOR existing_hotel_id forbids an orphan). Finding kept as the link above.\n")
                continue

            dup = await _existing_stub(s, kind, pid, succ)
            if dup:
                print(f"    WOULD MERGE (dedup): successor already exists as lead_contact "
                      f"id={dup['id']} '{dup['name']}' at {kind} {pid} -> fill empty fields, no new row.\n")
            else:
                print(f"    WOULD CREATE STUB: lead_contact at {kind} {pid}: "
                      f"name='{succ}', title='{succ_title}', organization='{org}', "
                      f"found_via='successor_discovery', confidence='low', "
                      f"source_detail='Replaced {holder or 'prior contact'}; via {src}'.\n")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=10)
    ap.add_argument("--ids", default="")
    args = ap.parse_args()
    asyncio.run(_run(args))
    return 0


if __name__ == "__main__":
    sys.exit(main())
