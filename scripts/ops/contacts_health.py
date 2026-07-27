"""
contacts_health.py
==================
Read-only. Tells you in plain language whether the contacts pipeline is
actually working. Nothing is written, ever.

    python -m scripts.ops.contacts_health

WHY THIS EXISTS
  daily_health_check watches scraping sources only. Nothing watched contacts.
  That is how three failures survived unnoticed:
    - Gmail sync silently dropped messages (history pagination)
    - contact-to-hotel name matching never worked at all
    - classification stopped running on 2026-06-11 and nobody knew for 6 weeks

  Every check below would have caught one of those the next day.

Each line ends in OK, WARN or PROBLEM. PROBLEM means something is broken now.
"""

import asyncio
import sys
from datetime import datetime, timezone

from sqlalchemy import text

from app.database import async_session

PROBLEMS: list[str] = []
WARNINGS: list[str] = []


def verdict(label, value, ok_if, warn_if=None, note=""):
    """Print one check line with a plain-English verdict."""
    if ok_if:
        tag = "OK"
    elif warn_if:
        tag = "WARN"
        WARNINGS.append(label)
    else:
        tag = "PROBLEM"
        PROBLEMS.append(label)
    pad = "." * max(2, 44 - len(label))
    print(f"  {label} {pad} {str(value):>12}   {tag}")
    if note and tag != "OK":
        print(f"        -> {note}")


async def one(s, sql, params=None):
    r = (await s.execute(text(sql), params or {})).first()
    return r[0] if r else 0


async def main() -> int:
    now = datetime.now(timezone.utc)
    print(f"\nCONTACTS PIPELINE HEALTH  —  {now:%Y-%m-%d %H:%M} UTC")
    print("=" * 74)

    async with async_session() as s:

        # ── 1. Is the mailbox sync running? ──────────────────────────
        print("\n1. EMAIL SYNC — is it still pulling mail?")
        rows = (
            await s.execute(
                text(
                    "SELECT mailbox, last_synced_at, "
                    "EXTRACT(EPOCH FROM (NOW() - last_synced_at))/3600 AS hrs "
                    "FROM mailbox_sync_state ORDER BY last_synced_at NULLS FIRST"
                )
            )
        ).all()
        if not rows:
            verdict("mailbox_sync_state rows", 0, False, note="No sync has ever run.")
        for r in rows:
            hrs = float(r.hrs or 9999)
            verdict(
                f"{r.mailbox[:34]} last sync",
                f"{hrs:.0f}h ago",
                hrs <= 30,
                hrs <= 96,
                note="This mailbox has stopped syncing. Check the worker and beat.",
            )

        # ── 2. Are new contacts arriving? ────────────────────────────
        print("\n2. NEW CONTACTS — is anything coming in?")
        d1 = await one(s, "SELECT COUNT(*) FROM contacts WHERE created_at > NOW() - INTERVAL '1 day'")
        d7 = await one(s, "SELECT COUNT(*) FROM contacts WHERE created_at > NOW() - INTERVAL '7 days'")
        d30 = await one(s, "SELECT COUNT(*) FROM contacts WHERE created_at > NOW() - INTERVAL '30 days'")
        verdict("added in last 24h", f"{d1:,}", d1 > 0, True)
        verdict("added in last 7 days", f"{d7:,}", d7 > 0, d30 > 0,
                note="No new contacts in a week. Sync is running but finding nothing.")
        verdict("added in last 30 days", f"{d30:,}", d30 > 0, False,
                note="Nothing new in a month. The pipeline is dead.")

        # ── 3. Is classification keeping up? ─────────────────────────
        print("\n3. CATEGORIZING — buyer / seller / junk being assigned?")
        uncat = await one(
            s,
            "SELECT COUNT(*) FROM contacts WHERE approval_status='pending' "
            "AND (contact_category IS NULL OR contact_category='unknown')",
        )
        verdict("waiting to be categorized", f"{uncat:,}", uncat < 200, uncat < 1500,
                note="Backlog is growing. classify_pending_contacts is not running.")
        newuncat = await one(
            s,
            "SELECT COUNT(*) FROM contacts WHERE created_at > NOW() - INTERVAL '3 days' "
            "AND (contact_category IS NULL OR contact_category='unknown')",
        )
        verdict("new but still uncategorized (3d)", f"{newuncat:,}", newuncat < 50, newuncat < 300,
                note="Recent contacts are not being categorized. The chain is broken again.")

        # ── 4. Is autolink keeping up? ───────────────────────────────
        print("\n4. ACCOUNT LINKING — contacts attached to hotels?")
        linked = await one(
            s, "SELECT COUNT(*) FROM contacts WHERE matched_hotel_id IS NOT NULL "
               "OR matched_lead_id IS NOT NULL")
        verdict("contacts linked to an account", f"{linked:,}", linked > 0, False,
                note="Nothing is linked. run_autolink is not running.")

        # ── 5. Is the work queue actually being consumed? ────────────
        print("\n5. TASK QUEUE — are jobs being picked up?")
        try:
            import redis
            from app.tasks.celery_app import celery_app

            r = redis.from_url(celery_app.conf.broker_url)
            for q in ("celery", "maintenance", "scraping", "crm"):
                depth = r.llen(q)
                if q == "celery":
                    verdict("default queue 'celery' (unconsumed)", depth, depth == 0, depth < 5,
                            note="Jobs are landing on a queue no worker reads. "
                                 "They will never run. Route them to 'maintenance'.")
                else:
                    verdict(f"queue '{q}' backlog", depth, depth < 50, depth < 500,
                            note="Jobs are piling up. Worker may be down.")
        except Exception as e:
            verdict("queue check", "failed", False, note=f"Could not read Redis: {e}")

        # ── 6. Data quality on what we have ──────────────────────────
        print("\n6. DATA QUALITY")
        total = await one(s, "SELECT COUNT(*) FROM contacts")
        noname = await one(
            s, "SELECT COUNT(*) FROM contacts WHERE COALESCE(NULLIF(TRIM(first_name),''),"
               "NULLIF(TRIM(display_name),'')) IS NULL AND email LIKE '%@%'")
        junk = await one(s, "SELECT COUNT(*) FROM contacts WHERE COALESCE(manual_category,contact_category)='junk'")
        print(f"  total contacts .............................. {total:>12,}")
        verdict("no name at all", f"{noname:,}",
                total and noname / total < 0.15, total and noname / total < 0.30,
                note="A large share cannot be personalised in any mailing.")
        verdict("marked junk", f"{junk:,}",
                total and junk / total < 0.25, total and junk / total < 0.45,
                note="Junk rate is high — the filters may be too aggressive.")

    # ── verdict ──────────────────────────────────────────────────────
    print("\n" + "=" * 74)
    if PROBLEMS:
        print(f"  {len(PROBLEMS)} PROBLEM(S) — something is broken right now:\n")
        for p in PROBLEMS:
            print(f"     - {p}")
    if WARNINGS:
        print(f"\n  {len(WARNINGS)} warning(s) — worth watching:\n")
        for w in WARNINGS:
            print(f"     - {w}")
    if not PROBLEMS and not WARNINGS:
        print("  Everything healthy.")
    print("\n  Run this after every change, and once a week regardless.")
    print("  Read-only — nothing was written.\n")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except KeyboardInterrupt:
        sys.exit(130)
