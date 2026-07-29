"""
backfill_comm_dates.py  —  BATCHED comm-date backfill
=====================================================
Populates first_message_at / last_inbound_at / last_outbound_at on existing
contacts from real Gmail message headers. Direction tracking went live
~2026-06-23; everything older shows no dates. This rebuilds the timeline.

FAST: fetches message metadata in BATCHES of 100 (one HTTP round-trip per 100
messages) instead of one call per message — roughly 50-100x faster than a
naive per-message loop.

    python scripts/ops/backfill_comm_dates.py --dry-run
    python scripts/ops/backfill_comm_dates.py --dry-run --since 2024-01-01
    python scripts/ops/backfill_comm_dates.py --mailbox salesorders@jauniforms.com
    python scripts/ops/backfill_comm_dates.py --since 2024-01-01          # writes

WHAT IT READS
  Headers only (From/To/Cc, internalDate). No bodies, no contact scraping,
  no Gemini, no grounding, no Serper. Free Gmail quota; runtime is the cost.

DIRECTION (same rule as the live sync)
  A message is INBOUND when its From is NOT a JA domain (they wrote to us),
  OUTBOUND when JA wrote. For each external participant:
    inbound msg   -> stamp last_inbound_at on the sender; Cc bystanders get
                     first_message_at only (dates the relationship, not a reply)
    outbound msg  -> stamp last_outbound_at on every external recipient

WRITE (fill-forward, never clobbers fresher live-sync data)
  first_message_at = LEAST(existing, found)
  last_inbound_at  = GREATEST(existing, found)
  last_outbound_at = GREATEST(existing, found)

  --dry-run writes nothing and reports counts.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from sqlalchemy import text  # noqa: E402

from app.database import async_session  # noqa: E402
from app.services.inbox_sync import (  # noqa: E402
    OWN_DOMAINS,
    _extract_emails,
    _gmail,
)
from app.services.mailbox_discovery import get_mailboxes_cached  # noqa: E402

LIST_PAGE = 500          # ids per list() page
BATCH_SIZE = 25         # messages per batch get() — Gmail's max is 100
MAX_RETRIES = 4


def _hdr(headers, name):
    for h in headers:
        if h.get("name", "").lower() == name.lower():
            return h.get("value") or ""
    return ""


def _dt_from_ms(ms):
    try:
        return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc)
    except Exception:
        return None


def _fold(acc, email, dt, kind):
    """Fold one dated observation into acc[email]."""
    e = acc[email]
    if e.get("first") is None or dt < e["first"]:
        e["first"] = dt
    if kind == "in" and (e.get("last_in") is None or dt > e["last_in"]):
        e["last_in"] = dt
    if kind == "out" and (e.get("last_out") is None or dt > e["last_out"]):
        e["last_out"] = dt


def _process_message(msg, acc):
    """Fold one fetched message's headers into the accumulator."""
    dt = _dt_from_ms(msg.get("internalDate"))
    if dt is None:
        return
    headers = msg.get("payload", {}).get("headers", [])
    from_emails = _extract_emails(_hdr(headers, "From"))
    from_e = from_emails[0].lower() if from_emails else ""
    from_dom = from_e.split("@", 1)[1] if "@" in from_e else ""
    inbound = bool(from_e) and from_dom not in OWN_DOMAINS

    if inbound:
        _fold(acc, from_e, dt, "in")
        for h in ("To", "Cc"):
            for e in _extract_emails(_hdr(headers, h)):
                e = e.lower()
                if "@" in e and e.split("@", 1)[1] not in OWN_DOMAINS:
                    _fold(acc, e, dt, "first_only")
    else:
        for h in ("To", "Cc"):
            for e in _extract_emails(_hdr(headers, h)):
                e = e.lower()
                if "@" in e and e.split("@", 1)[1] not in OWN_DOMAINS:
                    _fold(acc, e, dt, "out")


def _scan_mailbox(mailbox, since, stats, acc, verbose):
    """One pass over a mailbox using BATCHED metadata gets."""
    svc = _gmail(mailbox)
    q = f"after:{since.replace('-', '/')}" if since else None

    # 1) collect all message ids (cheap, ~500 per call)
    ids = []
    page = None
    while True:
        resp = svc.users().messages().list(
            userId="me", maxResults=LIST_PAGE, pageToken=page, q=q
        ).execute()
        ids.extend(m["id"] for m in resp.get("messages", []))
        page = resp.get("nextPageToken")
        if not page:
            break
    print(f"    {mailbox}: {len(ids):,} messages to scan", flush=True)
    if not ids:
        return

    # 2) fetch metadata in batches of BATCH_SIZE
    def _cb(_rid, response, exception):
        if exception is not None:
            stats["msg_errors"] += 1
            return
        stats["messages"] += 1
        _process_message(response, acc)

    for i in range(0, len(ids), BATCH_SIZE):
        chunk = ids[i:i + BATCH_SIZE]
        for attempt in range(MAX_RETRIES):
            batch = svc.new_batch_http_request(callback=_cb)
            for mid in chunk:
                batch.add(svc.users().messages().get(
                    userId="me", id=mid, format="metadata",
                    metadataHeaders=["From", "To", "Cc", "Date"],
                ))
            try:
                batch.execute()
                break
            except Exception as e:
                if attempt == MAX_RETRIES - 1:
                    stats["batch_errors"] += 1
                    if verbose:
                        print(f"      batch failed: {str(e)[:60]}", flush=True)
                else:
                    time.sleep(2 ** attempt)  # backoff on rate limit
        time.sleep(0.4)
        done = min(i + BATCH_SIZE, len(ids))
        if verbose or done % 2000 < BATCH_SIZE:
            print(f"      …{done:,}/{len(ids):,}  ({stats['messages']:,} ok)", flush=True)


async def _write(acc, dry_run):
    out = {"matched": 0, "updated": 0}
    emails = list(acc.keys())
    if not emails:
        return out
    async with async_session() as session:
        # match in chunks to keep the ANY() list sane
        matched_ids = []
        for i in range(0, len(emails), 5000):
            batch = emails[i:i + 5000]
            rows = (await session.execute(
                text("SELECT id, lower(email) AS em FROM contacts "
                     "WHERE lower(email) = ANY(:ems)"),
                {"ems": batch},
            )).all()
            matched_ids.extend((r.id, r.em) for r in rows)
        out["matched"] = len(matched_ids)
        if dry_run:
            return out
        for cid, em in matched_ids:
            a = acc[em]
            await session.execute(
                text("UPDATE contacts SET "
                     "first_message_at = LEAST(COALESCE(first_message_at, :f), COALESCE(:f, first_message_at)), "
                     "last_inbound_at  = GREATEST(COALESCE(last_inbound_at, :i), COALESCE(:i, last_inbound_at)), "
                     "last_outbound_at = GREATEST(COALESCE(last_outbound_at, :o), COALESCE(:o, last_outbound_at)) "
                     "WHERE id = :id"),
                {"id": cid, "f": a.get("first"), "i": a.get("last_in"), "o": a.get("last_out")},
            )
            out["updated"] += 1
        await session.commit()
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="2023-01-01", help="YYYY-MM-DD floor")
    ap.add_argument("--mailbox", action="append", help="limit to mailbox(es)")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--verbose", action="store_true", help="log every batch")
    args = ap.parse_args()

    boxes = args.mailbox or get_mailboxes_cached()
    print(f"Comm-date backfill — {len(boxes)} mailbox(es), since {args.since}"
          f"{' [DRY RUN]' if args.dry_run else ''}, batched {BATCH_SIZE}/call\n")

    acc = defaultdict(dict)
    stats = {"messages": 0, "msg_errors": 0, "batch_errors": 0}
    t0 = time.time()
    for mb in boxes:
        print(f"  scanning {mb} …", flush=True)
        try:
            _scan_mailbox(mb, args.since, stats, acc, args.verbose)
        except Exception as e:
            print(f"  !! {mb}: {str(e)[:80]}")
    dt = time.time() - t0
    print(f"\nscanned {stats['messages']:,} messages in {dt:,.0f}s "
          f"({stats['msg_errors']} msg errors, {stats['batch_errors']} batch errors); "
          f"{len(acc):,} distinct external addresses")

    res = asyncio.run(_write(acc, args.dry_run))
    print(f"contacts matched: {res['matched']:,}"
          + ("" if args.dry_run else f" · updated: {res['updated']:,}"))
    if args.dry_run:
        print("dry run — nothing written. Re-run without --dry-run to apply.")


if __name__ == "__main__":
    main()
