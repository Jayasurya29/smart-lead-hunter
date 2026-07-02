"""
review_news_queue.py  --  work the news action queues by hand.

  --list                 show pending hotel candidates + person job-change flags
  --approve ID           approve a queued hotel -> creates a lead (save_lead_to_db,
                         which dedups; auto_smart_fill enriches it next morning)
  --reject  ID           reject a queued hotel
  --action  ID           mark a person flag actioned (you followed up)
  --dismiss ID           dismiss a person flag

Same review pattern as review_pending_names.py / review_pending_moves.py.

USAGE (repo root, venv active, DATABASE_URL set)
  python scripts/ops/review_news_queue.py --list
  python scripts/ops/review_news_queue.py --approve 12
  python scripts/ops/review_news_queue.py --dismiss 4
"""

from __future__ import annotations

import argparse
import asyncio
import sys
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
from app.services.news_actions import (  # noqa: E402
    apply_person_move,
    approve_contact,
    approve_lead,
    list_actions,
    looks_like_real_hotel,
    reject_lead,
    reopen_person,
    reset_stuck_leads,
    revert_action,
    set_contact_status,
    set_person_status,
)


async def show_list(db):
    hotels = (await db.execute(text(
        "SELECT id, hotel_name, category, region, vertical, luxury, source "
        "FROM news_lead_queue WHERE status='pending' ORDER BY luxury DESC, id"
    ))).mappings().all()
    people = (await db.execute(text(
        "SELECT id, person_name, person_title, new_hotel, match_strength, "
        "known_account FROM news_person_review WHERE status='pending' ORDER BY id"
    ))).mappings().all()
    contacts = (await db.execute(text(
        "SELECT id, person_name, person_title, hotel_name, account_type "
        "FROM news_contact_review WHERE status='pending' ORDER BY id"
    ))).mappings().all()

    print(f"\n=== PENDING HOTEL LEADS ({len(hotels)}) — approve to add to pipeline ===")
    for h in hotels:
        lux = "★" if h["luxury"] else " "
        print(f"  [{h['id']:>4}] {lux} {(h['hotel_name'] or '')[:46]:<46} "
              f"{(h['category'] or ''):<16} {(h['region'] or ''):<9} {h['vertical'] or ''}")
    print(f"\n=== PENDING PERSON FLAGS ({len(people)}) — known people who moved ===")
    for p in people:
        print(f"  [{p['id']:>4}] {(p['person_name'] or '')[:22]:<22} — "
              f"{(p['person_title'] or 'role?')[:20]:<20} @ {(p['new_hotel'] or '?')[:24]:<24} "
              f"[{p['match_strength']}: known from {(p['known_account'] or '?')[:22]}]")
    print(f"\n=== PENDING CONTACT REVIEWS ({len(contacts)}) — new person at a hotel we own ===")
    for c in contacts:
        print(f"  [{c['id']:>4}] {(c['person_name'] or '')[:22]:<22} — "
              f"{(c['person_title'] or 'role?')[:20]:<20} @ {(c['hotel_name'] or '?')[:30]:<30} "
              f"[{c['account_type']}]")
    print("\n  approve/reject a hotel  : --approve ID / --reject ID")
    print("  action/dismiss a flag   : --action ID / --dismiss ID")
    print("  add/skip a contact      : --add-contact ID / --skip-contact ID\n")


async def prune_bad(db):
    """Auto-reject queued hotels whose extracted name is junk (fragments,
    zoos, descriptor blobs) — the name-quality gate applied retroactively."""
    rows = (await db.execute(text(
        "SELECT id, hotel_name FROM news_lead_queue WHERE status='pending'"
    ))).mappings().all()
    bad = [r for r in rows if not looks_like_real_hotel(r["hotel_name"])]
    for r in bad:
        await reject_lead(db, r["id"])
    print(f"pruned {len(bad)} junk-name rows:")
    for r in bad:
        print(f"  rejected [{r['id']}] {r['hotel_name']}")
    if not bad:
        print("  (none — queue names look clean)")


async def show_log(db, days: int):
    rows = await list_actions(db, days=days)
    print(f"\n=== NEWS ACTIVITY — last {days} day(s) ({len(rows)} actions) ===")
    if not rows:
        print("  (nothing logged in this window)")
        return
    for r in rows:
        ts = r["created_at"].strftime("%m-%d %H:%M") if r["created_at"] else "?"
        flag = "  [REVERTED]" if r["reverted"] else ""
        print(f"  [{r['id']:>4}] {ts}  {r['action']:<13} {(r['summary'] or '')[:70]}{flag}")
    print("\n  revert a bad one:  --revert ID\n")


async def main():
    ap = argparse.ArgumentParser(description="Review the news action queues")
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--log", action="store_true", help="show the activity log")
    ap.add_argument("--days", type=int, default=7, help="window for --log")
    ap.add_argument("--revert", type=int, help="undo a logged action by id")
    ap.add_argument("--prune", action="store_true", help="auto-reject junk-name hotels")
    ap.add_argument("--reset-stuck", action="store_true",
                    help="re-open hotels marked approved but with no lead created")
    ap.add_argument("--approve", type=int)
    ap.add_argument("--reject", type=int)
    ap.add_argument("--action", type=int)
    ap.add_argument("--reopen", type=int, help="reset a person flag to pending")
    ap.add_argument("--dismiss", type=int)
    ap.add_argument("--add-contact", type=int, dest="add_contact",
                    help="attach a queued contact to its existing hotel")
    ap.add_argument("--skip-contact", type=int, dest="skip_contact",
                    help="dismiss a queued contact review")
    args = ap.parse_args()

    async with async_session() as db:
        if args.log:
            await show_log(db, args.days)
        elif args.revert is not None:
            r = await revert_action(db, args.revert)
            print(f"revert #{args.revert}: {r}")
        elif args.prune:
            await prune_bad(db)
        elif args.reset_stuck:
            n = await reset_stuck_leads(db)
            print(f"reset {n} stuck (approved-but-no-lead) row(s) back to pending")
        elif args.approve is not None:
            r = await approve_lead(db, args.approve)
            print(f"approve #{args.approve}: {r}")
        elif args.reject is not None:
            ok = await reject_lead(db, args.reject)
            print(f"reject #{args.reject}: {'done' if ok else 'not found / not pending'}")
        elif args.action is not None:
            r = await apply_person_move(db, args.action)
            print(f"action #{args.action}: {r}")
        elif args.reopen is not None:
            ok = await reopen_person(db, args.reopen)
            print(f"reopen #{args.reopen}: {'done — now pending' if ok else 'not found'}")
        elif args.dismiss is not None:
            ok = await set_person_status(db, args.dismiss, "dismissed")
            print(f"dismiss #{args.dismiss}: {'done' if ok else 'not found / not pending'}")
        elif args.add_contact is not None:
            r = await approve_contact(db, args.add_contact)
            print(f"add-contact #{args.add_contact}: {r}")
        elif args.skip_contact is not None:
            ok = await set_contact_status(db, args.skip_contact, "dismissed")
            print(f"skip-contact #{args.skip_contact}: {'done' if ok else 'not found / not pending'}")
        else:
            await show_list(db)


if __name__ == "__main__":
    asyncio.run(main())
