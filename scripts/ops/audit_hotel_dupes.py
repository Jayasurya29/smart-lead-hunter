"""
audit_hotel_dupes.py
====================
Read-only. Finds hotel/account names that are the SAME property split by a
filler word (Resort, Hotel, Spa...) or word-order, across BOTH contacts'
organizations AND the existing_hotels table.

    python -m scripts.ops.audit_hotel_dupes
    python -m scripts.ops.audit_hotel_dupes --table hotels   # existing_hotels only
    python -m scripts.ops.audit_hotel_dupes --table contacts # contact orgs only

Groups names by a FILLER-STRIPPED key: lowercase, drop punctuation, drop the
generic property nouns (resort/hotel/spa/suites/inn/club/collection/residences)
and articles, keep every distinguishing word. Names that collapse to the same
key but are spelled differently are candidate duplicates.

Guard: if stripping leaves < 2 meaningful words, we keep the original words
(so 'Hotel California' != 'California Hotel', and 'PGA Resort' != 'PG').

Nothing is written — this sizes the problem and lists the groups.
"""

import argparse
import asyncio
import re
import sys
from collections import defaultdict

from sqlalchemy import text

from app.database import async_session

FILLER = {"resort", "resorts", "hotel", "hotels", "spa", "spas", "suites",
          "inn", "inns", "club", "clubs", "collection", "villas", "residences",
          "lodge", "lodges", "the", "and", "a", "at", "by", "of", "an"}


def fkey(name: str) -> str:
    s = re.sub(r"[.,'\"&]+", " ", (name or "").strip().lower())
    words = [w for w in re.split(r"\s+", s) if w]
    kept = [w for w in words if w not in FILLER]
    if len(kept) < 2:
        kept = [w for w in words if w != "the"]
    return "".join(kept)


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--table", choices=["hotels", "contacts", "both"], default="both")
    ap.add_argument("--min", type=int, default=2)
    args = ap.parse_args()

    names: list[str] = []
    async with async_session() as s:
        if args.table in ("hotels", "both"):
            rows = (await s.execute(text(
                "SELECT DISTINCT hotel_name FROM existing_hotels "
                "WHERE hotel_name IS NOT NULL"))).all()
            names += [r[0] for r in rows]
        if args.table in ("contacts", "both"):
            rows = (await s.execute(text(
                "SELECT DISTINCT organization FROM contacts "
                "WHERE organization IS NOT NULL "
                "AND COALESCE(manual_category,contact_category,'')!='junk'"))).all()
            names += [r[0] for r in rows]

    names = sorted(set(n.strip() for n in names if n and n.strip()))
    groups = defaultdict(set)
    for n in names:
        k = fkey(n)
        if k:
            groups[k].add(n)

    dupes = {k: v for k, v in groups.items() if len(v) >= args.min}

    print(f"\nHOTEL DUPLICATE-NAME AUDIT  ({len(names):,} distinct names, "
          f"table={args.table})\n" + "=" * 72)
    print(f"\n  groups that are the same property spelled differently: {len(dupes)}")
    total_extra = sum(len(v) - 1 for v in dupes.values())
    print(f"  redundant name variants to collapse: {total_extra}")

    print("\n  the splits:\n")
    for k, variants in sorted(dupes.items(), key=lambda x: -len(x[1]))[:60]:
        vs = sorted(variants, key=len, reverse=True)
        print(f"    {vs[0]}")
        for v in vs[1:]:
            print(f"        = {v}")

    print("\n" + "=" * 72)
    print("  Each group's LONGEST spelling would become the canonical name.")
    print("  Read-only — nothing written.\n")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except KeyboardInterrupt:
        sys.exit(130)
