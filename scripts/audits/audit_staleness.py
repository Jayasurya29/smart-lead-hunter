"""
audit_staleness.py  —  READ-ONLY (no writes, no API calls)
=========================================================
Profiles how OLD your contacts' last real interaction is, so we can draw a
defensible stale/fresh line instead of guessing a number.

"Last interaction" = the most recent of last_inbound_at / last_outbound_at
(falling back to last_seen). Buckets the population by age and shows, for the
sales-relevant slice (buyers, real names, real hotels, not role inboxes), how
many fall in each band — and how many have NO usable date at all (which is its
own problem: we can't judge their freshness).

Run from repo root, venv active, DATABASE_URL set:
    python audit_staleness.py
"""

import asyncio

from sqlalchemy import text

from app.database import async_session

# the "worth keeping current" slice — the sales-relevant contacts
TARGET = (
    "contact_category = 'buyer' "
    "AND (first_name IS NOT NULL OR display_name IS NOT NULL) "
    "AND organization IS NOT NULL AND email LIKE '%@%'"
)

# most recent real touch; coalesce inbound/outbound, fall back to last_seen
LAST = "GREATEST(COALESCE(last_inbound_at, last_outbound_at, last_seen), "
LAST += "COALESCE(last_outbound_at, last_inbound_at, last_seen))"


async def main() -> None:
    async with async_session() as s:
        async def scalar(q, **p):
            return (await s.execute(text(q), p)).scalar() or 0

        total = await scalar(f"SELECT COUNT(*) FROM contacts WHERE {TARGET}")
        print("=" * 70)
        print(f" STALENESS PROFILE — sales-relevant contacts: {total:,}")
        print("=" * 70)

        # coverage: do we even HAVE a real interaction date?
        have_inbound = await scalar(f"SELECT COUNT(*) FROM contacts WHERE {TARGET} AND last_inbound_at IS NOT NULL")
        have_any = await scalar(
            f"SELECT COUNT(*) FROM contacts WHERE {TARGET} "
            "AND (last_inbound_at IS NOT NULL OR last_outbound_at IS NOT NULL)"
        )
        print("\n1) date coverage (can we even judge freshness?)")
        print(f"   have last_inbound_at        : {have_inbound:,}  ({have_inbound/total*100:.0f}%)")
        print(f"   have inbound OR outbound    : {have_any:,}  ({have_any/total*100:.0f}%)")
        print(f"   NO interaction date at all  : {total-have_any:,}  ({(total-have_any)/total*100:.0f}%)")
        print("   ^ contacts with no date can't be aged -- they need separate handling.")

        print("\n2) age of last interaction (of those WITH a date)")
        bands = [
            ("<= 90 days   (fresh)", "now() - interval '90 days'", None),
            ("90d - 6 mo", "now() - interval '180 days'", "now() - interval '90 days'"),
            ("6 mo - 1 yr", "now() - interval '365 days'", "now() - interval '180 days'"),
            ("1 - 2 yr     (stale)", "now() - interval '730 days'", "now() - interval '365 days'"),
            ("> 2 yr       (very stale)", None, "now() - interval '730 days'"),
        ]
        for label, newer_than, older_than in bands:
            conds = [TARGET, "(last_inbound_at IS NOT NULL OR last_outbound_at IS NOT NULL)"]
            if newer_than:
                conds.append(f"{LAST} > {newer_than}")
            if older_than:
                conds.append(f"{LAST} <= {older_than}")
            n = await scalar(f"SELECT COUNT(*) FROM contacts WHERE {' AND '.join(conds)}")
            bar = "█" * min(50, n // max(1, total // 100))
            print(f"   {label:<26} {n:>6,}  {bar}")

        # actionable: how many would a given threshold flag for check-status?
        print("\n3) how many would each stale-threshold flag for re-check?")
        for days in (180, 270, 365, 540, 730):
            n = await scalar(
                f"SELECT COUNT(*) FROM contacts WHERE {TARGET} "
                f"AND (last_inbound_at IS NOT NULL OR last_outbound_at IS NOT NULL) "
                f"AND {LAST} <= now() - interval '{days} days'"
            )
            print(f"   older than {days:>3}d : {n:>6,} contacts to check")

        print("\n" + "=" * 70)
        print(" Pick the threshold where the count is meaningful but affordable.")
        print(" The 'no date' bucket is separate: those need a freshness signal")
        print(" (e.g. a one-time check) since we can't age them.")
        print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
