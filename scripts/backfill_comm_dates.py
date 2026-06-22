#!/usr/bin/env python3
r"""backfill_comm_dates.py -- populate comm-dates WITHOUT counting.

WHY: contacts have first_message_at / last_inbound_at / last_outbound_at, but
those columns were added (migration 040, 2026-06-16) AFTER the count-once
ledger (synced_messages) was already full. inbox_sync folds dates only for
messages that pass the ledger gate (new, uncounted). So any contact whose
messages were ALL counted before the date code existed has interaction_count
set but NULL dates -- ~8k such contacts.

A normal re-backfill cannot fix this: every message is a ledger dup, so it's
skipped before the date fold. This script re-reads the mailbox windows and
writes ONLY the date columns via a direct, count-safe UPDATE:

    last_inbound_at  = GREATEST(existing, this message, NULL-safe)
    last_outbound_at = GREATEST(existing, this message, NULL-safe)
    first_message_at = LEAST(existing, this message, NULL-safe)

It NEVER touches interaction_count, synced_messages, source_mailboxes, names,
orgs, or runs discovery/enrichment. Idempotent: GREATEST/LEAST only moves dates
outward, so re-running converges (safe to resume / re-run any mailbox).

USAGE:
    python scripts/backfill_comm_dates.py <mailbox>@jauniforms.com
    python scripts/backfill_comm_dates.py <mailbox>@jauniforms.com --since 2015-01-01
    python scripts/backfill_comm_dates.py <mailbox>@jauniforms.com --dry-run   # counts only, no writes

Direction (matches inbox_sync exactly): a message is INBOUND when the From
address is NOT on an OWN domain (the external person wrote to us); OUTBOUND
when JA wrote. We fold the message's send time into EVERY external participant
on the message (From + To + Cc + Bcc + Reply-To), so a person CC'd on a thread
still carries the right timeline. Own-domain addresses are skipped (they are
not contacts). format=metadata keeps the Gmail fetch cheap (headers + date,
no bodies / attachments).
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

from googleapiclient.errors import HttpError  # noqa: E402
from sqlalchemy import text  # noqa: E402

from app.services.inbox_sync import (  # noqa: E402
    OWN_DOMAINS,
    _dt_fromtimestamp_utc,
    _extract_emails,
    _gmail,
)

WINDOW_DAYS = 30
LIST_PAGE = 500
GET_CHUNK = 100  # messages fetched (metadata) per inner batch before a DB flush
_META_HEADERS = ["From", "To", "Cc", "Bcc", "Reply-To"]


def _windows(since: date, until: date, win_days: int = WINDOW_DAYS):
    cur = since
    while cur < until:
        nxt = min(until, cur + timedelta(days=win_days))
        yield cur, nxt
        cur = nxt


def _list_window_ids(gmail, after: date, before: date) -> list[str]:
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


def _domain(email: str) -> str:
    return email.split("@")[-1].lower() if "@" in email else ""


def _fetch_meta(gmail, msg_id: str) -> dict | None:
    """internalDate + the participant headers only (no body)."""
    try:
        return (
            gmail.users()
            .messages()
            .get(
                userId="me",
                id=msg_id,
                format="metadata",
                metadataHeaders=_META_HEADERS,
            )
            .execute()
        )
    except HttpError as e:
        print(f"    ! get error {msg_id}: {str(e)[:100]}")
        return None


def _fold(acc: dict[str, dict], email: str, msg_dt, inbound: bool) -> None:
    """Fold one message date into a participant's running min/max (NULL-safe)."""
    if not email or msg_dt is None:
        return
    rec = acc.setdefault(email, {"in": None, "out": None, "first": None})
    if rec["first"] is None or msg_dt < rec["first"]:
        rec["first"] = msg_dt
    if inbound:
        if rec["in"] is None or msg_dt > rec["in"]:
            rec["in"] = msg_dt
    else:
        if rec["out"] is None or msg_dt > rec["out"]:
            rec["out"] = msg_dt


def _process_message(meta: dict, acc: dict[str, dict]) -> None:
    headers = {
        h["name"]: h.get("value", "")
        for h in (meta.get("payload", {}).get("headers") or [])
    }
    msg_dt = None
    try:
        _id = meta.get("internalDate")
        if _id:
            msg_dt = _dt_fromtimestamp_utc(int(_id) / 1000)
    except Exception:
        msg_dt = None
    if msg_dt is None:
        return

    from_emails = _extract_emails(headers.get("From", ""))
    from_e = from_emails[0].lower() if from_emails else ""
    from_domain = _domain(from_e)
    inbound = bool(from_e) and from_domain not in OWN_DOMAINS

    seen: set[str] = set()
    for hdr in _META_HEADERS:
        for e in _extract_emails(headers.get(hdr, "")):
            e = e.lower()
            if e in seen:
                continue
            seen.add(e)
            if not e or _domain(e) in OWN_DOMAINS:
                continue  # own-domain addresses are not contacts
            _fold(acc, e, msg_dt, inbound)


async def _apply_dates(acc: dict[str, dict]) -> int:
    """Direct, count-safe UPDATE of date columns only. Returns rows touched."""
    if not acc:
        return 0
    from app.database import async_session

    touched = 0
    async with async_session() as s:
        for email, rec in acc.items():
            res = await s.execute(
                text(
                    "UPDATE contacts SET "
                    "  first_message_at = LEAST("
                    "     COALESCE(first_message_at, :first), COALESCE(:first, first_message_at)), "
                    "  last_inbound_at = GREATEST("
                    "     COALESCE(last_inbound_at, :lin), COALESCE(:lin, last_inbound_at)), "
                    "  last_outbound_at = GREATEST("
                    "     COALESCE(last_outbound_at, :lout), COALESCE(:lout, last_outbound_at)) "
                    "WHERE email = :email"
                ),
                {
                    "email": email,
                    "first": rec["first"],
                    "lin": rec["in"],
                    "lout": rec["out"],
                },
            )
            touched += res.rowcount or 0
        await s.commit()
    return touched


async def _run(mailbox: str, since: date, dry_run: bool) -> None:
    gmail = _gmail(mailbox)
    until = date.today() + timedelta(days=1)
    print(f"Date-only backfill for {mailbox} (since {since}). Counts are NOT touched.\n")

    grand_msgs = grand_touched = 0
    for ws, we in _windows(since, until):
        ids = _list_window_ids(gmail, ws, we)
        if not ids:
            continue
        print(f"  window {ws} .. {we}: {len(ids):,} ids")
        if dry_run:
            grand_msgs += len(ids)
            continue
        for batch in _chunks(ids, GET_CHUNK):
            acc: dict[str, dict] = {}
            for mid in batch:
                meta = _fetch_meta(gmail, mid)
                if meta:
                    _process_message(meta, acc)
            touched = await _apply_dates(acc)
            grand_msgs += len(batch)
            grand_touched += touched
        print(f"    contacts dated so far: {grand_touched:,}")

    if dry_run:
        print(f"\n[DRY-RUN] would scan {grand_msgs:,} messages. No writes made.")
    else:
        print(
            f"\nDone. Scanned {grand_msgs:,} messages; "
            f"applied date updates {grand_touched:,} times "
            f"(a contact on many messages is updated many times -- GREATEST/LEAST converge)."
        )


def main() -> int:
    ap = argparse.ArgumentParser(description="Populate contact comm-dates without counting.")
    ap.add_argument("mailbox", help="e.g. ugarcia@jauniforms.com")
    ap.add_argument("--since", default="2014-01-01", help="YYYY-MM-DD oldest window (default 2014-01-01)")
    ap.add_argument("--dry-run", action="store_true", help="list message counts only; no Gmail fetch, no writes")
    args = ap.parse_args()
    try:
        since = datetime.strptime(args.since, "%Y-%m-%d").date()
    except ValueError:
        print("ERROR: --since must be YYYY-MM-DD")
        return 2
    asyncio.run(_run(args.mailbox, since, args.dry_run))
    return 0


if __name__ == "__main__":
    sys.exit(main())
