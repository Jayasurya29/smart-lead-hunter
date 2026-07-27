"""
check_mailboxes.py
==================
Read-only. Asks Google Workspace which mailboxes it can see right now, and
compares that against what the database has been syncing.

    python -m scripts.ops.check_mailboxes

Settles one question: are the stalled mailboxes missing because Google no
longer lists them (account suspended/deleted, or the new admin account cannot
see them), or because sync is failing on them?
"""

import asyncio
import sys

from sqlalchemy import text

from app.database import async_session


async def main() -> int:
    print("\nMAILBOX CHECK\n" + "=" * 66)

    # ── what Google says today ──
    try:
        from app.services.mailbox_discovery import (
            DOMAIN_ADMIN_EMAIL,
            list_active_mailboxes,
        )
        print(f"\nImpersonating: {DOMAIN_ADMIN_EMAIL}")
        live = set(list_active_mailboxes())
        print(f"Google lists {len(live)} active mailboxes right now.")
    except Exception as e:
        print(f"\nCOULD NOT REACH GOOGLE: {e}")
        print("\nIf this failed, the service account or its delegation is the problem —")
        print("that alone would stop every sync, so check it before anything else.")
        return 1

    # ── what the DB has ──
    async with async_session() as s:
        rows = (
            await s.execute(
                text(
                    "SELECT mailbox, "
                    "ROUND(EXTRACT(EPOCH FROM (NOW()-last_synced_at))/3600) AS hrs "
                    "FROM mailbox_sync_state ORDER BY 2 DESC NULLS FIRST"
                )
            )
        ).all()

    known = {r.mailbox: (float(r.hrs) if r.hrs is not None else 99999) for r in rows}

    stalled = {m: h for m, h in known.items() if h > 48}
    print(f"\nDatabase has {len(known)} mailbox rows, {len(stalled)} stalled (>48h).\n")

    if stalled:
        print("STALLED MAILBOXES — why:\n")
        for m, h in sorted(stalled.items(), key=lambda x: -x[1]):
            days = h / 24
            if m in live:
                why = "STILL ACTIVE IN GOOGLE -> sync is failing on this one. Real problem."
            elif m.lower() in live:
                why = "case duplicate — Google has the lowercase version. Safe to delete."
            else:
                why = "Google does NOT list it — suspended, deleted, or not visible to this admin."
            print(f"  {m:<38} {days:>5.0f}d   {why}")

    # ── mailboxes Google has that we have never synced ──
    never = sorted(live - set(known))
    if never:
        print(f"\nGOOGLE HAS {len(never)} MAILBOX(ES) WE HAVE NEVER SYNCED:\n")
        for m in never[:30]:
            print(f"  {m}")
        print("\n  These will be picked up on the next sync automatically.")

    print("\n" + "=" * 66)
    print("  Read-only. Nothing written.\n")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except KeyboardInterrupt:
        sys.exit(130)
