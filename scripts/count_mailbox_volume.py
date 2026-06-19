"""
scripts/count_mailbox_volume.py
===============================
STEP ZERO for the one-time historical backfill. Counts how many real messages
each active JA mailbox holds since a given date — WITHOUT fetching any message
bodies. It only pages `messages.list` (cheap: ~5 quota units/page, 500 ids/page)
and tallies IDs, so it's the safest possible probe before committing to the
heavy get-based pull.

Scope matches the daily sync exactly: `-in:chats -in:spam -in:trash` (real kept
mail only — no Google Chat, no spam folder, no deleted mail). That keeps the
number honest: it reflects what we'd actually ingest, not 40k spam messages
we'd throw away.

USAGE (run from repo root, venv active)
---------------------------------------
  python scripts\\count_mailbox_volume.py                      # all mailboxes, since 2020-01-01
  python scripts\\count_mailbox_volume.py --since 2020-01-01   # explicit date
  python scripts\\count_mailbox_volume.py --mailbox sales@jauniforms.com   # probe ONE first
  python scripts\\count_mailbox_volume.py --limit-pages 4      # quick quota probe (caps pages/mbx)

Output: a per-mailbox table (count + page calls + time) and a grand total, plus
a rough estimate of the get-phase runtime at ~7 msg/s.
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from googleapiclient.errors import HttpError  # noqa: E402

PAGE_SIZE = 500  # Gmail max per messages.list page


def _count_one(gmail, mailbox: str, query: str, limit_pages: int | None) -> dict:
    """Page messages.list, tally ids only. No messages.get → no payload cost."""
    count = 0
    pages = 0
    page_token = None
    t0 = time.time()
    error = None
    while True:
        try:
            resp = (
                gmail.users()
                .messages()
                .list(
                    userId="me",
                    q=query,
                    maxResults=PAGE_SIZE,
                    pageToken=page_token,
                )
                .execute()
            )
        except HttpError as e:
            error = str(e)[:160]
            break
        count += len(resp.get("messages", []))
        pages += 1
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
        if limit_pages and pages >= limit_pages:
            error = f"stopped at --limit-pages={limit_pages} (count is a floor)"
            break
    return {
        "mailbox": mailbox,
        "count": count,
        "pages": pages,
        "seconds": round(time.time() - t0, 1),
        "error": error,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Count messages per JA mailbox since a date (list-only).")
    ap.add_argument("--since", default="2020-01-01", help="YYYY-MM-DD (default 2020-01-01)")
    ap.add_argument("--mailbox", default="", help="probe a single mailbox instead of all")
    ap.add_argument(
        "--limit-pages",
        type=int,
        default=0,
        help="cap pages per mailbox for a fast quota probe (0 = no cap, true total)",
    )
    args = ap.parse_args()

    # Gmail wants after:YYYY/MM/DD
    since_q = args.since.replace("-", "/")
    query = f"after:{since_q} -in:chats -in:spam -in:trash"
    limit_pages = args.limit_pages or None

    from app.services.inbox_sync import _gmail
    from app.services.mailbox_discovery import list_active_mailboxes

    if args.mailbox:
        mailboxes = [args.mailbox]
    else:
        print("Listing active mailboxes...")
        mailboxes = list_active_mailboxes()
    print(f"Counting {len(mailboxes)} mailbox(es), query: {query!r}\n")

    rows = []
    grand_total = 0
    for i, mb in enumerate(mailboxes, 1):
        try:
            gmail = _gmail(mb)
            r = _count_one(gmail, mb, query, limit_pages)
        except Exception as e:  # noqa: BLE001 - report and continue
            r = {"mailbox": mb, "count": 0, "pages": 0, "seconds": 0.0, "error": str(e)[:160]}
        rows.append(r)
        grand_total += r["count"]
        flag = f"  ⚠ {r['error']}" if r["error"] else ""
        print(f"  [{i:>2}/{len(mailboxes)}] {mb:<40} {r['count']:>8,} msgs  "
              f"({r['pages']} pages, {r['seconds']}s){flag}")

    rows.sort(key=lambda x: x["count"], reverse=True)
    print("\n" + "=" * 64)
    print("BY VOLUME (largest first):")
    for r in rows:
        print(f"  {r['mailbox']:<40} {r['count']:>8,}")
    print("=" * 64)
    print(f"  {'GRAND TOTAL':<40} {grand_total:>8,} messages")

    # Rough get-phase runtime at the ~7 msg/s wall the sync code notes.
    secs = grand_total / 7.0
    hrs = secs / 3600.0
    print(f"\nEstimated get-phase runtime @ ~7 msg/s: ~{hrs:,.1f} hours "
          f"({hrs / 7:.1f} working days @ 7h/day).")
    print("This is LIST-only — no bodies fetched, no contacts touched, nothing written.")
    if limit_pages:
        print(f"\nNOTE: --limit-pages={limit_pages} was set, so counts are FLOORS, not totals.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
