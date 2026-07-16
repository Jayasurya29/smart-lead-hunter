"""backfill_comm_dates.py — one-shot Gmail METADATA backfill for
first_message_at / last_inbound_at / last_outbound_at on existing contacts.

Why: direction tracking went live ~2026-06-23; everything older shows
"No data yet". This re-LISTS old messages (headers only — From/To/Cc/Date,
no bodies, no contact scraping) and stamps the dates. Free Gmail quota;
runtime is the only cost.

Safety:
  - fill-only: LEAST/GREATEST against existing values, never clobbers
    fresher data written by the live sync
  - only touches emails that already exist in contacts
  - --dry-run prints counts, writes nothing

Usage (repo root, venv):
  python scripts/ops/backfill_comm_dates.py --dry-run
  python scripts/ops/backfill_comm_dates.py --since 2023-01-01
  python scripts/ops/backfill_comm_dates.py --mailbox it@jauniforms.com --dry-run
"""

from __future__ import annotations

import argparse
import asyncio
import sys
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

BATCH_PAGE = 500  # gmail list page size


def _hdr(headers: list[dict], name: str) -> str:
    for h in headers:
        if h.get("name", "").lower() == name.lower():
            return h.get("value") or ""
    return ""


def _dt_from_ms(internal_ms: str | int | None):
    try:
        return datetime.fromtimestamp(int(internal_ms) / 1000, tz=timezone.utc)
    except Exception:
        return None


def _scan_mailbox(mailbox: str, since: str | None, stats: dict, acc: dict) -> None:
    """One pass over the mailbox: every message's From/To/Cc headers fold into
    acc[email] = {first, last_in, last_out}. Direction per participant:
    the From of an external message = inbound for that person; recipients of
    OUR outbound = outbound for them; external Cc bystanders date the
    relationship only (first)."""
    svc = _gmail(mailbox)
    q = f"after:{since.replace('-', '/')}" if since else None
    page = None
    while True:
        req = (
            svc.users()
            .messages()
            .list(userId="me", maxResults=BATCH_PAGE, pageToken=page, q=q)
        )
        resp = req.execute()
        ids = [m["id"] for m in resp.get("messages", [])]
        if not ids:
            break
        for mid in ids:
            try:
                msg = (
                    svc.users()
                    .messages()
                    .get(
                        userId="me",
                        id=mid,
                        format="metadata",
                        metadataHeaders=["From", "To", "Cc", "Date"],
                    )
                    .execute()
                )
            except Exception:
                stats["msg_errors"] += 1
                continue
            stats["messages"] += 1
            dt = _dt_from_ms(msg.get("internalDate"))
            if dt is None:
                continue
            headers = msg.get("payload", {}).get("headers", [])
            from_emails = _extract_emails(_hdr(headers, "From"))
            from_e = from_emails[0].lower() if from_emails else ""
            from_dom = from_e.split("@", 1)[1] if "@" in from_e else ""
            msg_inbound = bool(from_e) and from_dom not in OWN_DOMAINS

            def _touch(email: str, kind: str):
                e = acc[email]
                if e.get("first") is None or dt < e["first"]:
                    e["first"] = dt
                if kind == "in" and (e.get("last_in") is None or dt > e["last_in"]):
                    e["last_in"] = dt
                if kind == "out" and (e.get("last_out") is None or dt > e["last_out"]):
                    e["last_out"] = dt

            if msg_inbound:
                _touch(from_e, "in")
                # external Cc bystanders on an inbound msg: relationship date only
                for h in ("To", "Cc"):
                    for e in _extract_emails(_hdr(headers, h)):
                        e = e.lower()
                        if "@" in e and e.split("@", 1)[1] not in OWN_DOMAINS:
                            _touch(e, "first_only")
            else:
                # our outbound: every external recipient got written-to
                for h in ("To", "Cc"):
                    for e in _extract_emails(_hdr(headers, h)):
                        e = e.lower()
                        if "@" in e and e.split("@", 1)[1] not in OWN_DOMAINS:
                            _touch(e, "out")
        page = resp.get("nextPageToken")
        if not page:
            break
        if stats["messages"] % 5000 < BATCH_PAGE:
            print(f"    …{stats['messages']:,} messages", flush=True)


async def _write(acc: dict, dry_run: bool) -> dict:
    out = {"matched": 0, "updated": 0}
    emails = list(acc.keys())
    if not emails:
        return out
    async with async_session() as session:
        rows = (
            await session.execute(
                text("SELECT id, lower(email) AS em FROM contacts WHERE lower(email) = ANY(:ems)"),
                {"ems": emails},
            )
        ).all()
        out["matched"] = len(rows)
        if dry_run:
            return out
        for r in rows:
            a = acc[r.em]
            await session.execute(
                text(
                    "UPDATE contacts SET "
                    "first_message_at = LEAST(COALESCE(first_message_at, :f), :f), "
                    "last_inbound_at  = GREATEST(COALESCE(last_inbound_at, :i), COALESCE(:i, last_inbound_at)), "
                    "last_outbound_at = GREATEST(COALESCE(last_outbound_at, :o), COALESCE(:o, last_outbound_at)) "
                    "WHERE id = :id"
                ),
                {
                    "id": r.id,
                    "f": a.get("first"),
                    "i": a.get("last_in"),
                    "o": a.get("last_out"),
                },
            )
            out["updated"] += 1
        await session.commit()
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="2023-01-01", help="YYYY-MM-DD floor (default 2023-01-01)")
    ap.add_argument("--mailbox", action="append", help="limit to specific mailbox(es)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    boxes = args.mailbox or get_mailboxes_cached()
    print(f"Backfilling comm dates from {len(boxes)} mailbox(es), since {args.since}"
          f"{' [DRY RUN]' if args.dry_run else ''}")

    acc: dict = defaultdict(dict)
    stats = {"messages": 0, "msg_errors": 0}
    for mb in boxes:
        print(f"  scanning {mb} …", flush=True)
        try:
            _scan_mailbox(mb, args.since, stats, acc)
        except Exception as e:
            print(f"  !! {mb}: {e}")
    print(f"scanned {stats['messages']:,} messages ({stats['msg_errors']} errors); "
          f"{len(acc):,} distinct external addresses")

    res = asyncio.run(_write(acc, args.dry_run))
    print(f"contacts matched: {res['matched']:,}"
          + ("" if args.dry_run else f" · updated: {res['updated']:,}"))
    if args.dry_run:
        print("dry run — nothing written. Re-run without --dry-run to apply.")


if __name__ == "__main__":
    main()
