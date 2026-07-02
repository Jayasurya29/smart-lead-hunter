"""Backfill `tier` on news-attached lead_contacts that predate tier-at-attach.

Contacts added from news before the tier-classification change have tier=NULL,
so _compute_priority() mis-ranks them (a GM sits at P3 instead of P1). This runs
each such title through the SAME canonical classifier the live attach path now
uses, and fills tier where it's missing. Only touches found_via in
('news','news_move') rows with a NULL/empty tier — never overwrites a set tier.

    python scripts/ops/backfill_news_contact_tiers.py            # dry-run
    python scripts/ops/backfill_news_contact_tiers.py --apply
"""
import argparse
import asyncio
import os
import sys
from collections import Counter

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from sqlalchemy import text  # noqa: E402

from app.database import async_session  # noqa: E402
from app.services.news_actions import _tier_for_title  # noqa: E402


async def main():
    ap = argparse.ArgumentParser(description="Backfill tier on news lead_contacts")
    ap.add_argument("--apply", action="store_true", help="write changes")
    args = ap.parse_args()

    async with async_session() as db:
        rows = (await db.execute(text(
            "SELECT id, name, title FROM lead_contacts "
            "WHERE found_via IN ('news','news_move') "
            "AND (tier IS NULL OR tier = '') AND title IS NOT NULL "
            "ORDER BY id"
        ))).mappings().all()

        out: Counter = Counter()
        changed = []
        for r in rows:
            tier = _tier_for_title(r["title"])
            if not tier:
                out["unplaceable"] += 1
                continue
            out[tier] += 1
            changed.append((r["id"], r["name"], r["title"], tier))
            if args.apply:
                await db.execute(
                    text("UPDATE lead_contacts SET tier=:t WHERE id=:i"),
                    {"t": tier, "i": r["id"]},
                )
        if args.apply:
            await db.commit()

    tag = "APPLIED" if args.apply else "DRY-RUN (nothing written)"
    print(f"\n=== BACKFILL NEWS CONTACT TIERS — {tag} ===")
    print(f"  candidates (news contacts, tier empty): {len(rows)}")
    for cid, name, title, tier in changed:
        print(f"  [{cid:>5}] {name[:28]:<28} {(title or '')[:26]:<26} -> {tier}")
    print("  summary:")
    for k, v in out.most_common():
        print(f"    {k:<22} {v}")
    if not args.apply:
        print("\n  Re-run with --apply to write these tiers.")


if __name__ == "__main__":
    asyncio.run(main())
