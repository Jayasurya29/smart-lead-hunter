#!/usr/bin/env python3
"""count_mailbox_volume.py -- ACCURATE per-mailbox message count since a date.

Gmail has no count endpoint, and resultSizeEstimate is unreliable junk (it
returns a near-constant page-based guess, NOT a real total). The only honest
count is to page through messages.list following nextPageToken and tally the
IDs. This does exactly that -- IDs only (no message bodies, no DB writes), so it
is the lightest accurate count possible.

It also serves as a real quota probe: paging the IDs of the biggest mailbox is a
genuine burst of list calls. If THIS trips 429s, the full drain needs to go slow.

Usage (PowerShell, repo root, venv active):
    python count_mailbox_volume.py 2020/01/01
    python count_mailbox_volume.py 2020/01/01 --batch 10000
    python count_mailbox_volume.py 2020/01/01 --only apagan@jauniforms.com
    python count_mailbox_volume.py 2010/01/01 --before 2023/01/01 --only ugarcia@jauniforms.com
"""
import argparse
import sys
import time

from googleapiclient.errors import HttpError

from app.services.inbox_sync import _gmail
from app.services.mailbox_discovery import list_active_mailboxes


def _count(mailbox: str, after: str, before: str = "") -> int:
    """True count of messages for the date window by paging IDs (500/page)."""
    gmail = _gmail(mailbox)
    q = f"after:{after} -in:chats -in:spam -in:trash"
    if before:
        q = f"after:{after} before:{before} -in:chats -in:spam -in:trash"
    total = 0
    page_token = None
    while True:
        resp = (
            gmail.users()
            .messages()
            .list(userId="me", q=q, maxResults=500, pageToken=page_token)
            .execute()
        )
        total += len(resp.get("messages", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return total


def main(after: str, batch: int, only: str, before: str):
    try:
        mailboxes = [only] if only else list_active_mailboxes()
    except Exception as e:
        print(f"FATAL: could not list mailboxes: {e}")
        sys.exit(2)

    _win = f"{after} -> {before}" if before else f"since {after}"
    print(f"counting messages {_win} across {len(mailboxes)} mailbox(es) "
          f"(batch size {batch}) -- paging real IDs, this takes a bit\n")
    grand = 0
    rows = []
    t0 = time.time()
    for i, mb in enumerate(mailboxes, 1):
        try:
            n = _count(mb, after, before)
        except HttpError as e:
            print(f"[{i}/{len(mailboxes)}] {mb:<40} ERROR {e}")
            rows.append((mb, -1, 0))
            continue
        except Exception as e:
            print(f"[{i}/{len(mailboxes)}] {mb:<40} ERROR {e}")
            rows.append((mb, -1, 0))
            continue
        batches = (n + batch - 1) // batch if n > 0 else 0
        grand += n
        rows.append((mb, n, batches))
        print(f"[{i}/{len(mailboxes)}] {mb:<40} {n:>7} msgs  -> {batches} batch(es)", flush=True)

    print(f"\n--- summary ({time.time() - t0:.0f}s) ---")
    big = [r for r in rows if r[1] > batch]
    print(f"total mailboxes: {len(rows)}")
    print(f"REAL total messages since {after}: {grand}")
    print(f"total batches needed (sum): {sum(r[2] for r in rows if r[1] > 0)}")
    if big:
        print("high-volume mailboxes (> "
              f"{batch}):")
        for m, n, b in sorted(big, key=lambda x: -x[1]):
            print(f"    {m:<40} {n:>7} msgs -> {b} batches")
    else:
        print(f"no mailbox exceeds {batch} -- each fits in a single batch.")
    errs = [r[0] for r in rows if r[1] == -1]
    if errs:
        print(f"errored (quota/auth -- note for the real run): {errs}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("after", nargs="?", default="2025/01/01", help="YYYY/MM/DD")
    ap.add_argument("--batch", type=int, default=10000)
    ap.add_argument("--only", default="", help="count just one mailbox")
    ap.add_argument("--before", default="", help="upper date bound YYYY/MM/DD")
    a = ap.parse_args()
    main(a.after, a.batch, a.only, a.before)
