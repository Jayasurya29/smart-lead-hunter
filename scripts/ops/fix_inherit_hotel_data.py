"""
fix_inherit_hotel_data.py
=========================
Enriches contacts by copying data from the hotel they are linked to. FREE —
no API, no LLM, just a JOIN on matched_hotel_id.

    python -m scripts.ops.fix_inherit_hotel_data            # DRY RUN
    python -m scripts.ops.fix_inherit_hotel_data --apply    # write

WHAT IT COPIES (into EMPTY contact fields only)
  brand_tier   ALWAYS safe. A person at a Ritz-Carlton is ultra-luxury tier
               whether they are the GM or a regional VP.
  address      PROPERTY-LEVEL people only (GM, front desk, dept heads). A
  city         regional/corporate contact linked to a hotel sits at HQ, not
  state        the property, so the hotel's street address would be WRONG for
  country      them. Corporate titles are detected and skipped for these.
  brand        safe — the brand of the linked property.

WHY THIS IS SAFE
  - Fill-empty only: never overwrites an existing contact value.
  - Address/location gated on title so corporate people don't get a property
    address.
  - Only touches contacts already linked to a hotel (matched_hotel_id).
  - Dry run writes nothing.
"""

import argparse
import asyncio
import re
import sys
from collections import Counter

from sqlalchemy import text

from app.database import async_session

CORPORATE = re.compile(r"regional|corporate|area\b|vice president|\bvp\b|above property|multi|"
                       r"\bdivision(al)?\b|enterprise|group director", re.I)

SQL = """
    SELECT c.id, c.title,
           c.brand_tier AS c_tier, c.address AS c_addr, c.city AS c_city,
           c.state AS c_state, c.country AS c_country, c.organization AS c_org,
           h.brand_tier AS h_tier, h.brand AS h_brand, h.address AS h_addr,
           h.city AS h_city, h.state AS h_state, h.country AS h_country,
           h.hotel_name AS h_name
    FROM contacts c
    JOIN existing_hotels h ON h.id = c.matched_hotel_id
    WHERE COALESCE(c.manual_category, c.contact_category,'') != 'junk'
"""


def _blank(v):
    return not (v or "").strip() if isinstance(v, str) else v is None


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    async with async_session() as s:
        rows = [dict(r._mapping) for r in (await s.execute(text(SQL))).all()]

    fills = Counter()
    updates = []  # (id, {field: value})
    for r in rows:
        u = {}
        # brand tier — always safe
        if _blank(r["c_tier"]) and r["h_tier"]:
            u["brand_tier"] = r["h_tier"]
            fills["brand_tier"] += 1
        # location — property-level only
        is_corporate = bool(r["title"] and CORPORATE.search(r["title"]))
        if not is_corporate:
            for cf, hf, col in (("c_addr", "h_addr", "address"),
                                ("c_city", "h_city", "city"),
                                ("c_state", "h_state", "state"),
                                ("c_country", "h_country", "country")):
                if _blank(r[cf]) and (r[hf] or "").strip():
                    u[col] = r[hf]
                    fills[col] += 1
        if u:
            updates.append((r["id"], u))

    print(f"\nHOTEL-DATA INHERITANCE  ({'APPLYING' if args.apply else 'DRY RUN'})\n" + "=" * 66)
    print(f"  linked contacts scanned : {len(rows):,}")
    print(f"  contacts to enrich      : {len(updates):,}")
    print("\n  fields that would be filled:")
    for f, n in fills.most_common():
        print(f"    {f:<12} {n:>6,}")

    print("\n  sample:")
    for _id, u in updates[:15]:
        print(f"    #{_id}: " + ", ".join(f"{k}={str(v)[:20]}" for k, v in u.items()))

    if not args.apply:
        print("\n  Dry run — nothing written. Re-run with --apply.\n")
        return 0

    async with async_session() as s:
        for cid, u in updates:
            sets, params = [], {"id": cid}
            for k, v in u.items():
                sets.append(f"{k} = :{k}")
                params[k] = v
            sets.append("updated_at = NOW()")
            await s.execute(text(f"UPDATE contacts SET {', '.join(sets)} WHERE id = :id"), params)
        await s.commit()
    print(f"\n  ENRICHED {len(updates):,} contacts from their linked hotel.\n")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except KeyboardInterrupt:
        sys.exit(130)
