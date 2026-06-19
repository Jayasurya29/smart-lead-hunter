"""
scripts/backfill_mailbox.py
===========================
ONE-TIME historical contact backfill — manual, one mailbox at a time. NOT a
Celery task: you run it by hand and watch it. It drives the SAME tested
extraction pipeline (`sync_mailbox`) in resumable, count-once batches.

How it stays correct (Option 1):
  - Run `--reset-counts` ONCE before starting. It zeroes contacts.interaction_count
    so the backfill rebuilds every count from scratch.
  - Each message is gated on its RFC Message-ID via the synced_messages ledger,
    so every real email is counted exactly once — across mailboxes AND across
    re-runs. Re-running a mailbox (or a crashed batch) never double-counts.
  - Per-mailbox window checkpoint (backfill_progress) means a re-run resumes
    where it stopped.

Daily Celery sync keeps running normally and is unaffected (it passes no ledger
and no id-override, so its code path is unchanged; the backfill preserves its
Gmail history cursor).

USAGE (run from repo root, venv active — do NOT run inside Celery)
------------------------------------------------------------------
  # 0. ONE TIME, before the first mailbox — rebuilds counts from scratch:
  python scripts\\backfill_mailbox.py --reset-counts

  # 1. PROBE the goldmine safely — oldest window only, tiny, real but small:
  python scripts\\backfill_mailbox.py ugarcia@jauniforms.com --probe

  # 2. Full mailbox (ugarcia first), 10k batches, resumable:
  python scripts\\backfill_mailbox.py ugarcia@jauniforms.com --since 2020-01-01 --batch 10000

  # see what WOULD be pulled, no fetch/write:
  python scripts\\backfill_mailbox.py ugarcia@jauniforms.com --dry-run

Re-run the same command anytime to resume; finished windows are skipped.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from sqlalchemy import text  # noqa: E402
from googleapiclient.errors import HttpError  # noqa: E402

WINDOW_DAYS = 30  # list ids in 30-day slices, oldest → newest
LIST_PAGE = 500
_SCORE = False  # per-batch buying-signal scoring; OFF for backfill (--score to enable)


def _windows(since: date, until: date, win_days: int = WINDOW_DAYS):
    """Yield (start, end) date slices oldest→newest covering [since, until]."""
    cur = since
    while cur < until:
        nxt = min(until, cur + timedelta(days=win_days))
        yield cur, nxt
        cur = nxt


def _list_window_ids(gmail, after: date, before: date) -> list[str]:
    """All message ids in [after, before), real mail only (sync-matching)."""
    q = (
        f"after:{after.strftime('%Y/%m/%d')} "
        f"before:{before.strftime('%Y/%m/%d')} "
        "-in:chats -in:spam -in:trash"
    )
    ids: list[str] = []
    page_token = None
    while True:
        try:
            resp = (
                gmail.users()
                .messages()
                .list(userId="me", q=q, maxResults=LIST_PAGE, pageToken=page_token)
                .execute()
            )
        except HttpError as e:
            print(f"    ! list error [{q}]: {str(e)[:120]}")
            break
        ids.extend(m["id"] for m in resp.get("messages", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return ids


def _chunks(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


async def _reset_counts() -> None:
    from app.database import async_session

    print("Clean-start for the whole backfill. This:")
    print("  - zeroes contacts.interaction_count for the WHOLE table")
    print("  - empties the count-once ledger (synced_messages)")
    print("  - clears all per-mailbox progress (backfill_progress)")
    print("Names/orgs/categories/person_id and comm-dates are NOT touched.")
    print("Counts read low until the backfill finishes, then become exact.")
    print("Run this ONCE, before the first mailbox.\n")
    confirm = input("Type RESET to proceed: ").strip()
    if confirm != "RESET":
        print("Aborted — nothing changed.")
        return
    async with async_session() as s:
        res = await s.execute(text("UPDATE contacts SET interaction_count = 0"))
        await s.execute(text("TRUNCATE synced_messages"))
        await s.execute(text("TRUNCATE backfill_progress"))
        await s.commit()
        print(f"Done — interaction_count zeroed on {res.rowcount or 0} contacts; "
              "ledger + progress cleared. Ready for a clean backfill.")


async def _progress_before(mailbox: str):
    from app.database import async_session

    async with async_session() as s:
        row = (
            await s.execute(
                text("SELECT window_done_before FROM backfill_progress WHERE mailbox = :m"),
                {"m": mailbox},
            )
        ).first()
    return row[0] if row else None


async def _checkpoint(mailbox: str, done_before: date, status: str) -> None:
    from app.database import async_session

    async with async_session() as s:
        await s.execute(
            text(
                "INSERT INTO backfill_progress (mailbox, window_done_before, status, updated_at) "
                "VALUES (:m, :d, :st, now()) "
                "ON CONFLICT (mailbox) DO UPDATE SET "
                "window_done_before = EXCLUDED.window_done_before, "
                "status = EXCLUDED.status, updated_at = now()"
            ),
            {"m": mailbox, "d": done_before, "st": status},
        )
        await s.commit()


async def _run(mailbox: str, since: date, batch: int, probe: bool, dry_run: bool) -> None:
    from app.database import async_session
    from app.services.inbox_sync import _gmail, sync_mailbox
    from app.services.backfill_ledger import BackfillLedger

    gmail = _gmail(mailbox)
    until = date.today() + timedelta(days=1)
    resume = None if (probe or dry_run) else await _progress_before(mailbox)
    if resume:
        print(f"Resuming {mailbox}: windows ending on/before {resume} already done.\n")

    total_seen = total_counted = 0
    for ws, we in _windows(since, until):
        if resume and we <= resume:
            continue
        ids = _list_window_ids(gmail, ws, we)
        print(f"  window {ws} .. {we}: {len(ids):,} ids")
        if dry_run:
            total_seen += len(ids)
            if probe:
                break
            continue
        for bi, batch_ids in enumerate(_chunks(ids, batch), 1):
            async with async_session() as s:
                ledger = BackfillLedger(s)
                stats = await sync_mailbox(
                    mailbox,
                    s,
                    message_ids_override=batch_ids,
                    ledger=ledger,
                    score_buying_signal=_SCORE,
                )
            lg = ledger.stats()
            total_seen += len(batch_ids)
            total_counted += lg["new"]
            print(
                f"    batch {bi} ({len(batch_ids):,}): "
                f"counted={lg['new']:,} dup-skipped={lg['dup']:,} "
                f"new_contacts={stats.get('new_contacts', 0)} "
                f"updated={stats.get('updated_contacts', 0)}"
            )
        if not dry_run:
            await _checkpoint(mailbox, we, "running")
        if probe:
            print("\n[PROBE] stopped after the first (oldest) window.")
            break

    if not dry_run and not probe:
        await _checkpoint(mailbox, until, "done")
    print(
        f"\n{mailbox}: {'DRY-RUN ' if dry_run else ''}seen={total_seen:,} "
        f"counted-once={total_counted:,}"
        + ("  (dry-run: nothing fetched/written)" if dry_run else "")
    )


def main() -> int:
    ap = argparse.ArgumentParser(description="One-time manual contact backfill (one mailbox).")
    ap.add_argument("mailbox", nargs="?", default="", help="mailbox to backfill, e.g. ugarcia@jauniforms.com")
    ap.add_argument("--since", default="2020-01-01", help="YYYY-MM-DD (default 2020-01-01)")
    ap.add_argument("--batch", type=int, default=10000, help="messages per batch (default 10000)")
    ap.add_argument("--probe", action="store_true", help="oldest window only — small, real, safe")
    ap.add_argument("--dry-run", action="store_true", help="list/count only — no fetch, no writes")
    ap.add_argument("--reset-counts", action="store_true", help="ONE-TIME: zero interaction_count, then exit")
    ap.add_argument("--score", action="store_true", help="also score buying-signal per batch (off by default — does it once, paced, afterward)")
    args = ap.parse_args()

    global _SCORE
    _SCORE = args.score

    if args.reset_counts:
        asyncio.run(_reset_counts())
        return 0
    if not args.mailbox:
        ap.error("a mailbox is required (or use --reset-counts)")
    since = datetime.strptime(args.since, "%Y-%m-%d").date()
    asyncio.run(_run(args.mailbox, since, args.batch, args.probe, args.dry_run))
    return 0


if __name__ == "__main__":
    sys.exit(main())
