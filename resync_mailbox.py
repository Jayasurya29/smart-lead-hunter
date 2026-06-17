#!/usr/bin/env python3
"""resync_mailbox.py -- backfill ONE mailbox back to a given DATE, with LOGGING.

The normal sync only scans the last ~2 days. This does a wide-window pull for a
specific mailbox back to a date you name, to populate the communication dates
(first_message_at / last_inbound_at / last_outbound_at) from history.

This version FIXES the "looks frozen" problem:
  - turns ON console logging so sync_mailbox's own progress lines print (you will
    see "N messages to process", the windowed-backfill total, "N contacts after
    merge", rejections, and the final summary)
  - runs a heartbeat that prints elapsed time every 15s so you know it is alive
    even during the long silent message-fetch phase

Usage (PowerShell, repo root, venv active):
    python resync_mailbox.py aarencibia@jauniforms.com 2023/01/01
    python resync_mailbox.py aarencibia@jauniforms.com 1300        # days also ok
"""
import asyncio
import logging
import sys
import threading
import time
from datetime import date, datetime

# --- turn on logging BEFORE importing app modules so their loggers inherit it ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)

from app.database import async_session          # noqa: E402
from app.services.inbox_sync import sync_mailbox  # noqa: E402


def _to_days(arg: str) -> int:
    arg = arg.strip()
    if "/" in arg or "-" in arg:
        sep = "/" if "/" in arg else "-"
        d = datetime.strptime(arg, f"%Y{sep}%m{sep}%d").date()
        days = (date.today() - d).days
        if days < 1:
            print(f"date {arg} is not in the past")
            sys.exit(1)
        return days
    return int(arg)


class _Heartbeat(threading.Thread):
    """Prints an alive-tick every `every` seconds until stopped."""

    def __init__(self, label: str, every: int = 15):
        super().__init__(daemon=True)
        self.label = label
        self.every = every
        self._stop = threading.Event()

    def run(self):
        t0 = time.time()
        while not self._stop.wait(self.every):
            mins = (time.time() - t0) / 60
            print(f"    ...still working on {self.label} ({mins:.1f} min elapsed)",
                  flush=True)

    def stop(self):
        self._stop.set()


async def _run(mailbox: str, days: int, since_label: str):
    hb = _Heartbeat(mailbox)
    hb.start()
    t0 = time.time()
    try:
        async with async_session() as session:
            print(f"resync: {mailbox} -- scanning back to {since_label} ({days} days).\n"
                  f"        progress logs below; heartbeat every 15s.\n", flush=True)
            result = await sync_mailbox(
                mailbox, session, force_full_scan=True, scan_days_override=days
            )
    finally:
        hb.stop()
    print(f"\ndone in {(time.time() - t0) / 60:.1f} min:")
    for k in ("mailbox", "messages_scanned", "contacts_found",
              "new_contacts", "updated_contacts", "status", "error"):
        if k in result:
            print(f"  {k}: {result[k]}")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("usage: python resync_mailbox.py <mailbox> <YYYY/MM/DD | days>")
        print("   e.g. python resync_mailbox.py aarencibia@jauniforms.com 2023/01/01")
        sys.exit(1)
    mb = sys.argv[1]
    raw = sys.argv[2]
    d = _to_days(raw)
    label = raw if ("/" in raw or "-" in raw) else f"{d} days ago"
    asyncio.run(_run(mb, d, label))
