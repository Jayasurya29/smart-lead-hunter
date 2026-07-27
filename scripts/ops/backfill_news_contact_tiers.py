"""
backfill_comm_dates.py
======================
Populates first_message_at / last_inbound_at / last_outbound_at for contacts
that have interactions but no message dates — the records where an early sync
or bulk import counted messages without stamping their timeline (e.g. Sheila:
interaction_count 14, all three date columns NULL).

    python -m scripts.ops.backfill_comm_dates              # DRY RUN, 50
    python -m scripts.ops.backfill_comm_dates --limit 1000
    python -m scripts.ops.backfill_comm_dates --limit 1000 --apply

For each such contact it searches the top mailboxes for messages the address
appears on (from: OR to:), reads each message's real internalDate + From
header, and derives:
  first_message_at  = earliest message date seen
  last_inbound_at   = latest message FROM the contact (external -> us)
  last_outbound_at  = latest message TO the contact (us -> them)

Direction uses the SAME rule as the live sync: a message is inbound when its
From is NOT a JA domain.

WHY IT MATTERS
  With real dates, the freshness filter finally works: genuine recent repliers
  move to 'fresh', long-dead threads (Sheila's 2020-2022 mail) move to 'stale'
  where they belong. It does NOT invent freshness — it tells the truth.

  It does NOT detect that a person changed jobs (Sheila left in 2022); a stale
  date flags the risk, but only email verification catches a dead address.

SAFETY
  - Writes ONLY the three date columns, only when currently NULL (fill-empty).
  - Never touches category, name, or any other field.
  - Dry run writes nothing.
COST: Gmail reads only. NO Gemini, NO grounding, NO Serper. Cheapest backfill.
"""

import argparse
import asyncio
import sys
from datetime import datetime, timezone

from sqlalchemy import text

from app.database import async_session
from app.services.inbox_sync import _extract_emails, _gmail
from app.services.mailbox_discovery import list_active_mailboxes

OWN_DOMAINS = {"jauniforms.com", "jauniforms.org", "ja-uniforms.com", "ja-uniforms.org"}
MAX_MAILBOXES = 6
MAX_MSGS = 8

SQL = """
    SELECT id, email
    FROM contacts
    WHERE interaction_count >= 1
      AND first_message_at IS NULL
      AND COALESCE(manual_category, contact_category,'') != 'junk'
      AND email LIKE '%@%'
    ORDER BY interaction_count DESC
    LIMIT :lim
"""


def _dt(ms):
    try:
        return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc)
    except Exception:
        return None


async def gather_dates(gmail_clients, email):
    """Return (first, last_inbound, last_outbound) from the contact's mail."""
    first = last_in = last_out = None
    for gmail in gmail_clients:
        try:
            resp = gmail.users().messages().list(
                userId="me", q=f"(from:{email} OR to:{email})", maxResults=MAX_MSGS
            ).execute()
        except Exception:
            continue
        for m in resp.get("messages", []):
            try:
                msg = gmail.users().messages().get(
                    userId="me", id=m["id"], format="metadata",
                    metadataHeaders=["From"]
                ).execute()
            except Exception:
                continue
            dt = _dt(msg.get("internalDate"))
            if not dt:
                continue
            hdrs = {h["name"]: h["value"] for h in msg.get("payload", {}).get("headers", [])}
            frm = _extract_emails(hdrs.get("From", ""))
            frm_e = frm[0].lower() if frm else ""
            frm_dom = frm_e.split("@")[-1] if "@" in frm_e else ""
            inbound = bool(frm_e) and frm_dom not in OWN_DOMAINS
            if first is None or dt < first:
                first = dt
            if inbound:
                if last_in is None or dt > last_in:
                    last_in = dt
            else:
                if last_out is None or dt > last_out:
                    last_out = dt
    return first, last_in, last_out


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=50)
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    async with async_session() as s:
        rows = [dict(r._mapping) for r in (await s.execute(text(SQL), {"lim": args.limit})).all()]

    ordered = list_active_mailboxes()[:MAX_MAILBOXES]
    clients = []
    for mb in ordered:
        try:
            clients.append(_gmail(mb))
        except Exception:
            pass

    print(f"\nCOMM-DATE BACKFILL  ({'APPLYING' if args.apply else 'DRY RUN'})  "
          f"{len(rows)} contacts, {len(clients)} inboxes\n" + "=" * 68)

    found = 0
    updates = []
    for r in rows:
        first, lin, lout = await gather_dates(clients, r["email"])
        print(f"    ...checked {r['email'][:44]}", flush=True)
        if first or lin or lout:
            found += 1
            updates.append((r["id"], first, lin, lout))

    print(f"\n  recovered dates for : {found}/{len(rows)}")
    print("\n  sample:")
    for _id, f, i, o in updates[:15]:
        fs = f.date().isoformat() if f else "-"
        isr = i.date().isoformat() if i else "-"
        os_ = o.date().isoformat() if o else "-"
        print(f"    #{_id}: first={fs}  last_in={isr}  last_out={os_}")

    if not args.apply:
        print("\n  Dry run — nothing written. --apply to write the dates.\n")
        return 0

    async with async_session() as s:
        for cid, f, i, o in updates:
            await s.execute(
                text("UPDATE contacts SET "
                     "first_message_at = COALESCE(first_message_at, :f), "
                     "last_inbound_at  = COALESCE(last_inbound_at, :i), "
                     "last_outbound_at = COALESCE(last_outbound_at, :o), "
                     "updated_at = NOW() WHERE id = :id"),
                {"f": f, "i": i, "o": o, "id": cid},
            )
        await s.commit()
    print(f"\n  WROTE dates to {len(updates)} contacts.\n")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except KeyboardInterrupt:
        sys.exit(130)
