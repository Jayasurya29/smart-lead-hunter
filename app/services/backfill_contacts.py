"""scripts/backfill_contacts.py

Manual one-time backfill of historical contacts from a mailbox.

The daily Celery task (sync_inbox_contacts at 9:45 AM) only does
incremental delta syncs — yesterday's emails only. This script is for
loading deeper history into the `contacts` table on demand.

Uses the same extraction + dedup logic as the Celery task
(app/services/inbox_sync.sync_mailbox), so contacts loaded by this
script merge cleanly with anything the daily task wrote.

After this script runs, it stamps the mailbox_sync_state with the
current Gmail historyId — so the daily task picks up cleanly the
next morning without re-processing anything.

Usage
-----
# Backfill last 45 days for one mailbox
python scripts/backfill_contacts.py --mailbox ugarcia@jauniforms.com --days 45

# Backfill last 90 days for ALL active @jauniforms.com mailboxes
python scripts/backfill_contacts.py --all --days 90

# Backfill a full year for one mailbox
python scripts/backfill_contacts.py --mailbox menchu@jauniforms.com --days 365

# List which mailboxes would be synced (no DB writes)
python scripts/backfill_contacts.py --list-mailboxes
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path
from typing import Any

# Ensure project root is on sys.path so `from app...` works regardless
# of where this script is invoked from.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.database import async_session  # noqa: E402
from app.services.inbox_sync import sync_mailbox  # noqa: E402
from app.services.mailbox_discovery import list_active_mailboxes  # noqa: E402


def _print_banner(text: str, char: str = "=") -> None:
    bar = char * 70
    print(f"\n{bar}\n  {text}\n{bar}")


def _print_result(mailbox: str, days: int, result: dict[str, Any]) -> None:
    status = result.get("status", "?")
    icon = "✓" if status == "success" else "✗"
    print(
        f"\n  {icon} {mailbox} (last {days}d)\n"
        f"     Status:           {status}\n"
        f"     Messages scanned: {result.get('messages_scanned', 0):,}\n"
        f"     Contacts found:   {result.get('contacts_found', 0):,}\n"
        f"     New contacts:     {result.get('new_contacts', 0):,}\n"
        f"     Updated contacts: {result.get('updated_contacts', 0):,}\n"
        f"     Upsert errors:    {result.get('errors', 0):,}"
    )
    if result.get("error"):
        print(f"     ERROR: {result['error']}")


async def backfill_one(mailbox: str, days: int) -> dict[str, Any]:
    """Backfill a single mailbox. Returns the sync result dict."""
    _print_banner(f"BACKFILL: {mailbox} (last {days} days)")
    async with async_session() as session:
        result = await sync_mailbox(
            mailbox,
            session,
            force_full_scan=True,  # always full scan for manual backfill
            scan_days_override=days,  # custom backfill window
        )
    _print_result(mailbox, days, result)
    return result


async def backfill_all(mailboxes: list[str], days: int) -> list[dict[str, Any]]:
    """Backfill multiple mailboxes sequentially."""
    results: list[dict[str, Any]] = []
    for mailbox in mailboxes:
        try:
            r = await backfill_one(mailbox, days)
            results.append(r)
        except Exception as e:
            print(f"\n  ✗ {mailbox} CRASHED: {type(e).__name__}: {e}")
            results.append(
                {
                    "mailbox": mailbox,
                    "status": "crashed",
                    "error": str(e),
                }
            )
    return results


def _print_summary(results: list[dict[str, Any]]) -> None:
    totals = {
        "messages_scanned": 0,
        "contacts_found": 0,
        "new_contacts": 0,
        "updated_contacts": 0,
        "errors": 0,
    }
    ok_count = 0
    fail_count = 0
    for r in results:
        if r.get("status") == "success":
            ok_count += 1
        else:
            fail_count += 1
        for k in totals:
            totals[k] += int(r.get(k) or 0)

    _print_banner("BACKFILL SUMMARY", char="=")
    print(f"  Mailboxes attempted: {len(results)}")
    print(f"  Mailboxes succeeded: {ok_count}")
    print(f"  Mailboxes failed:    {fail_count}")
    print(f"  Total messages:      {totals['messages_scanned']:,}")
    print(f"  Total contacts:      {totals['contacts_found']:,}")
    print(f"  New rows in DB:      {totals['new_contacts']:,}")
    print(f"  Updated rows in DB:  {totals['updated_contacts']:,}")
    print(f"  Upsert errors:       {totals['errors']:,}")
    print()


async def main() -> int:
    parser = argparse.ArgumentParser(
        description="Manual contact backfill for SLH contacts table",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--mailbox",
        help="Single mailbox to backfill (e.g. ugarcia@jauniforms.com)",
    )
    group.add_argument(
        "--all",
        action="store_true",
        help="Backfill ALL active @jauniforms.com mailboxes",
    )
    group.add_argument(
        "--list-mailboxes",
        action="store_true",
        help="List discoverable mailboxes (no DB writes)",
    )

    parser.add_argument(
        "--days",
        type=int,
        default=45,
        help="How many days back to scan (default: 45)",
    )
    parser.add_argument(
        "--yes",
        "-y",
        action="store_true",
        help="Skip confirmation prompt when using --all",
    )

    args = parser.parse_args()

    # ── Mode: just list mailboxes ────────────────────────────────────
    if args.list_mailboxes:
        print("\nDiscovering active mailboxes via Directory API...")
        try:
            mailboxes = list_active_mailboxes()
        except Exception as e:
            print(f"\n✗ Discovery failed: {type(e).__name__}: {e}")
            return 1
        print(f"\nFound {len(mailboxes)} active mailbox(es):")
        for m in mailboxes:
            print(f"  - {m}")
        print()
        return 0

    # ── Mode: single mailbox ─────────────────────────────────────────
    if args.mailbox:
        if not args.mailbox.endswith("@jauniforms.com"):
            print(f"✗ Mailbox must end with @jauniforms.com: {args.mailbox}")
            return 1
        result = await backfill_one(args.mailbox, args.days)
        _print_summary([result])
        return 0 if result.get("status") == "success" else 1

    # ── Mode: all mailboxes ──────────────────────────────────────────
    if args.all:
        print("\nDiscovering active mailboxes via Directory API...")
        try:
            mailboxes = list_active_mailboxes()
        except Exception as e:
            print(f"\n✗ Discovery failed: {type(e).__name__}: {e}")
            return 1

        if not mailboxes:
            print("\n✗ No active mailboxes found. Aborting.")
            return 1

        print(f"\nWill backfill {len(mailboxes)} mailbox(es), {args.days} days each:")
        for m in mailboxes:
            print(f"  - {m}")

        if not args.yes:
            confirm = input("\nProceed? [y/N]: ").strip().lower()
            if confirm != "y":
                print("Aborted.")
                return 1

        results = await backfill_all(mailboxes, args.days)
        _print_summary(results)

        fail_count = sum(1 for r in results if r.get("status") != "success")
        return 0 if fail_count == 0 else 1

    return 0


if __name__ == "__main__":
    try:
        rc = asyncio.run(main())
        sys.exit(rc)
    except KeyboardInterrupt:
        print("\n⏹  Interrupted")
        sys.exit(130)
    except Exception as e:
        print(f"\n✗ FATAL: {type(e).__name__}: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
