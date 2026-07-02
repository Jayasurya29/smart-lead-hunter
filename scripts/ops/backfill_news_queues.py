"""
backfill_news_queues.py  --  populate the news action queues from the
hotel_news rows we ALREADY have (so you can review the backlog now, without
waiting for the next scan). Dry-run by default; --apply writes.

Uses the exact same gate/self-match logic as the live scan (app.services.
news_actions), so the backfill and the scan agree.

USAGE (repo root, venv active, DATABASE_URL set)
------------------------------------------------
  python scripts/ops/backfill_news_queues.py            # dry-run (counts only)
  python scripts/ops/backfill_news_queues.py --days 90
  python scripts/ops/backfill_news_queues.py --apply    # actually queue them
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from collections import Counter
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

from sqlalchemy import text  # noqa: E402

from app.database import async_session  # noqa: E402
from app.services.lead_factory import _normalize_for_dedup  # noqa: E402
from app.services.news_actions import (  # noqa: E402
    TARGET_REGIONS,
    _looks_like_person,
    is_self_match,
    qualifies_as_lead,
    queue_existing_contact,
    queue_new_hotel,
    queue_person_flag,
)


async def _pipeline_keys(db):
    keys = set()
    for tbl, filt in (("existing_hotels", ""),
                      ("potential_leads", " WHERE status NOT IN ('rejected','duplicate')")):
        rows = (await db.execute(text(f"SELECT hotel_name FROM {tbl}{filt}"))).scalars().all()
        for n in rows:
            keys.add(_normalize_for_dedup(n or ""))
    keys.discard("")
    return keys


async def main():
    ap = argparse.ArgumentParser(description="Backfill news queues from hotel_news")
    ap.add_argument("--days", type=int, default=90)
    ap.add_argument("--apply", action="store_true", help="write (default: dry-run)")
    args = ap.parse_args()

    async with async_session() as db:
        pk = await _pipeline_keys(db)
        news = (await db.execute(text(
            "SELECT id, url, source, category, vertical, region, hotel_name, "
            "brand, person_name, person_title, luxury, relationship_hits "
            "FROM hotel_news WHERE created_at > NOW() - make_interval(days => :d) "
            "ORDER BY created_at DESC"
        ), {"d": args.days})).mappings().all()

        hotel_out: Counter = Counter()
        person_out: Counter = Counter()
        contact_out: Counter = Counter()

        for n in news:
            hn = (n["hotel_name"] or "").strip()
            in_pipe = bool(hn) and _normalize_for_dedup(hn) in pk
            if args.apply:
                r = await queue_new_hotel(
                    db, news_id=n["id"], hotel_name=hn, brand=n["brand"],
                    region=n["region"], vertical=n["vertical"],
                    category=n["category"], luxury=n["luxury"],
                    source=n["source"], url=n["url"], in_pipeline=in_pipe,
                )
                hotel_out[r] += 1
            else:
                # simulate the gate without writing
                if len(_normalize_for_dedup(hn)) < 4:
                    hotel_out["skipped_no_name"] += 1
                elif in_pipe:
                    hotel_out["skipped_in_pipeline"] += 1
                elif qualifies_as_lead(vertical=n["vertical"], region=n["region"],
                                       category=n["category"], luxury=n["luxury"]):
                    hotel_out["queued"] += 1
                else:
                    hotel_out["skipped_unqualified"] += 1

            hits = n["relationship_hits"] or []
            if n["person_name"] and " " in (n["person_name"] or "") and hits:
                if args.apply:
                    r = await queue_person_flag(
                        db, news_id=n["id"], person_name=n["person_name"],
                        person_title=n["person_title"], new_hotel=hn or None,
                        new_org=n["brand"], hits=hits, region=n["region"],
                    )
                    person_out[r] += 1
                elif (n["region"] or "other").lower() not in TARGET_REGIONS:
                    person_out["skipped_out_of_region"] += 1
                else:
                    non_self = any(
                        not is_self_match(h.get("account") or h.get("organization"), hn)
                        for h in hits
                    )
                    person_out["queued" if non_self else "skipped_self_match"] += 1

            # existing-hotel case: a person named at a hotel we ALREADY own, whom
            # we DON'T already know (no relationship hit) -> queue as a new contact
            # for that existing account.
            if in_pipe and not hits and _looks_like_person(n["person_name"]):
                if args.apply:
                    r = await queue_existing_contact(
                        db, news_id=n["id"], hotel_name=hn,
                        person_name=n["person_name"], person_title=n["person_title"],
                        region=n["region"], category=n["category"],
                    )
                    contact_out[r] += 1
                elif (n["region"] or "other").lower() not in TARGET_REGIONS:
                    contact_out["skipped_out_of_region"] += 1
                else:
                    contact_out["candidate"] += 1

        if args.apply:
            await db.commit()

    tag = "APPLIED" if args.apply else "DRY-RUN (nothing written)"
    print(f"\n=== BACKFILL NEWS QUEUES — {tag} ===")
    print(f"  stories scanned: {len(news)}")
    print("  hotel queue:")
    for k, v in hotel_out.most_common():
        print(f"    {k:<24} {v}")
    print("  person review:")
    for k, v in person_out.most_common():
        print(f"    {k:<24} {v}")
    print("  contact review (existing hotels):")
    for k, v in contact_out.most_common():
        print(f"    {k:<24} {v}")
    if not args.apply:
        print("\n  Re-run with --apply to write these into the queues.")
    else:
        print("\n  Done. Review with: python scripts/ops/review_news_queue.py --list")
    print()


if __name__ == "__main__":
    asyncio.run(main())
