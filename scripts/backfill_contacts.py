#!/usr/bin/env python3
"""scripts/backfill_contacts.py — Manual historical Gmail contact backfill.

Usage:
    # Preview which mailboxes would be synced
    python scripts/backfill_contacts.py --list-mailboxes

    # Backfill ONE mailbox, last 2 days
    python scripts/backfill_contacts.py --mailbox ugarcia@jauniforms.com --days 2

    # Backfill ALL active mailboxes, last 90 days
    python scripts/backfill_contacts.py --all --days 90

    # Skip confirmation prompt
    python scripts/backfill_contacts.py --all --days 90 --yes

This is a MANUAL tool — it does NOT run automatically. The daily Celery
task (sync_inbox_contacts at 9:45 AM) handles incremental deltas. Use
this script for initial historical loads or catch-up after outages.

Each mailbox is synced in its own DB session so a failure in one
mailbox doesn't roll back others.
"""

import argparse
import asyncio
import logging
import sys
import time
from pathlib import Path

# Ensure project root is on sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Load .env if available
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

from app.database import async_session
from app.services.inbox_sync import sync_mailbox
from app.services.mailbox_discovery import list_active_mailboxes

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
# Suppress noisy httpx/httpcore per-request logs
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

logger = logging.getLogger("backfill_contacts")


def _fmt_elapsed(seconds: float) -> str:
    """Format seconds into human-readable string."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes = int(seconds // 60)
    secs = seconds % 60
    if minutes < 60:
        return f"{minutes}m {secs:.0f}s"
    hours = minutes // 60
    mins = minutes % 60
    return f"{hours}h {mins}m {secs:.0f}s"


async def backfill_one(mailbox: str, days: int) -> dict:
    """Run a full-scan sync for one mailbox."""
    t0 = time.time()
    logger.info(f"Starting backfill: {mailbox} ({days} days)")
    async with async_session() as session:
        stats = await sync_mailbox(
            mailbox,
            session,
            force_full_scan=True,
            scan_days_override=days,
        )
    elapsed = time.time() - t0
    logger.info(
        f"Done: {mailbox} — "
        f"scanned={stats.get('messages_scanned', 0)}, "
        f"contacts={stats.get('contacts_found', 0)}, "
        f"new={stats.get('new_contacts', 0)}, "
        f"updated={stats.get('updated_contacts', 0)}, "
        f"errors={stats.get('errors', 0)} "
        f"[{_fmt_elapsed(elapsed)}]"
    )
    return stats


async def main():
    parser = argparse.ArgumentParser(
        description="Backfill Gmail contacts from JA Uniforms mailboxes"
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--mailbox",
        type=str,
        help="Single mailbox to backfill (e.g. ugarcia@jauniforms.com)",
    )
    group.add_argument(
        "--all",
        action="store_true",
        help="Backfill ALL active mailboxes",
    )
    group.add_argument(
        "--list-mailboxes",
        action="store_true",
        help="List active mailboxes and exit",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=45,
        help="How many days back to scan (default: 45)",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Skip confirmation prompt for --all",
    )

    args = parser.parse_args()

    # ── List mailboxes ──
    if args.list_mailboxes:
        mailboxes = list_active_mailboxes()
        print(f"\nActive JA Uniforms mailboxes ({len(mailboxes)}):")
        for mb in mailboxes:
            print(f"  {mb}")
        return

    # ── Single mailbox ──
    if args.mailbox:
        t0 = time.time()
        stats = await backfill_one(args.mailbox, args.days)
        elapsed = time.time() - t0
        print(f"\n✅ Backfill complete for {args.mailbox} [{_fmt_elapsed(elapsed)}]")
        print(f"   Messages scanned: {stats.get('messages_scanned', 0)}")
        print(f"   Contacts found:   {stats.get('contacts_found', 0)}")
        print(f"   New contacts:     {stats.get('new_contacts', 0)}")
        print(f"   Updated:          {stats.get('updated_contacts', 0)}")
        print(f"   Errors:           {stats.get('errors', 0)}")
        return

    # ── All mailboxes ──
    if args.all:
        mailboxes = list_active_mailboxes()
        print(f"\nWill backfill {len(mailboxes)} mailboxes, {args.days} days each:")
        for mb in mailboxes:
            print(f"  {mb}")

        if not args.yes:
            confirm = input("\nProceed? [y/N] ").strip().lower()
            if confirm not in ("y", "yes"):
                print("Aborted.")
                return

        print()
        t0_total = time.time()
        totals = {
            "messages_scanned": 0,
            "contacts_found": 0,
            "new_contacts": 0,
            "updated_contacts": 0,
            "errors": 0,
            "mailboxes_ok": 0,
            "mailboxes_failed": 0,
        }

        for i, mb in enumerate(mailboxes, 1):
            print(f"[{i}/{len(mailboxes)}] {mb}")
            try:
                stats = await backfill_one(mb, args.days)
                totals["messages_scanned"] += stats.get("messages_scanned", 0)
                totals["contacts_found"] += stats.get("contacts_found", 0)
                totals["new_contacts"] += stats.get("new_contacts", 0)
                totals["updated_contacts"] += stats.get("updated_contacts", 0)
                totals["errors"] += stats.get("errors", 0)
                totals["mailboxes_ok"] += 1
            except Exception as e:
                logger.error(f"Failed to backfill {mb}: {e}", exc_info=True)
                totals["mailboxes_failed"] += 1

        elapsed_total = time.time() - t0_total
        print(f"\n{'═' * 50}")
        print(f"✅ Backfill complete [{_fmt_elapsed(elapsed_total)}]")
        print(f"   Mailboxes OK:     {totals['mailboxes_ok']}")
        print(f"   Mailboxes failed: {totals['mailboxes_failed']}")
        print(f"   Messages scanned: {totals['messages_scanned']}")
        print(f"   Contacts found:   {totals['contacts_found']}")
        print(f"   New contacts:     {totals['new_contacts']}")
        print(f"   Updated:          {totals['updated_contacts']}")
        print(f"   Errors:           {totals['errors']}")


if __name__ == "__main__":
    asyncio.run(main())
