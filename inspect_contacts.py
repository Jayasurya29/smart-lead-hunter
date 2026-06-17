#!/usr/bin/env python3
"""inspect_contacts.py -- READ-ONLY snapshot of the contacts table.

Pure SELECT. No writes, no Gmail/Gemini calls, no cost. Shows:
  - total contacts
  - how many have the new comm-date fields populated (did a post-patch sync run?)
  - how many have secondary_email (former-email feature)
  - a sample of recently-seen contacts with their real comm dates, so you can
    eyeball whether the dates look right before any big backfill.

Usage (PowerShell, repo root, venv active):
    python inspect_contacts.py
"""
import asyncio

from sqlalchemy import text

from app.database import async_session


async def main():
    async with async_session() as s:
        total = (await s.execute(text("SELECT count(*) FROM contacts"))).scalar()
        with_first = (await s.execute(text(
            "SELECT count(*) FROM contacts WHERE first_message_at IS NOT NULL"))).scalar()
        with_in = (await s.execute(text(
            "SELECT count(*) FROM contacts WHERE last_inbound_at IS NOT NULL"))).scalar()
        with_out = (await s.execute(text(
            "SELECT count(*) FROM contacts WHERE last_outbound_at IS NOT NULL"))).scalar()
        with_sec = (await s.execute(text(
            "SELECT count(*) FROM contacts WHERE secondary_email IS NOT NULL"))).scalar()

        print("=== contacts snapshot (read-only) ===")
        print(f"total contacts:                 {total}")
        print(f"  with first_message_at:        {with_first}")
        print(f"  with last_inbound_at:         {with_in}")
        print(f"  with last_outbound_at:        {with_out}")
        print(f"  with secondary_email:         {with_sec}")
        if with_first == 0 and with_in == 0 and with_out == 0:
            print("\n  -> NO comm dates populated yet. No sync has run since the")
            print("     patches went live (or the columns aren't being written).")
        else:
            print("\n  -> comm dates ARE populating. A post-patch sync has run.")

        print("\n=== 10 most-recently-seen contacts (with comm dates) ===")
        rows = (await s.execute(text(
            "SELECT email, organization, last_seen, first_message_at, "
            "last_inbound_at, last_outbound_at "
            "FROM contacts ORDER BY last_seen DESC NULLS LAST LIMIT 10"))).mappings().all()
        for r in rows:
            print(f"\n  {r['email']}  ({r['organization'] or '-'})")
            print(f"    last_seen (sync):   {r['last_seen']}")
            print(f"    first_message_at:   {r['first_message_at']}")
            print(f"    last_inbound_at:    {r['last_inbound_at']}")
            print(f"    last_outbound_at:   {r['last_outbound_at']}")

        # spot-check the people from the Brendan thread if present
        print("\n=== spot-check apagan / brendan / ritzcarlton (if present) ===")
        chk = (await s.execute(text(
            "SELECT email, organization, first_message_at, last_inbound_at, "
            "last_outbound_at, secondary_email FROM contacts "
            "WHERE email ILIKE '%apagan%' OR email ILIKE '%brendan%' "
            "OR email ILIKE '%payze%' OR organization ILIKE '%ritz%' "
            "OR organization ILIKE '%regis%' LIMIT 10"))).mappings().all()
        if not chk:
            print("  (none found)")
        for r in chk:
            print(f"  {r['email']} ({r['organization'] or '-'}) "
                  f"in={r['last_inbound_at']} out={r['last_outbound_at']} "
                  f"sec={r['secondary_email'] or '-'}")


if __name__ == "__main__":
    asyncio.run(main())
