"""
audit_mailer_readiness.py
=========================
Read-only. Tells you exactly who is mailable for the catalog campaign, by
grading every non-junk contact against real send-readiness criteria — not just
the stored category.

    python -m scripts.ops.audit_mailer_readiness
    python -m scripts.ops.audit_mailer_readiness --sample 15

For a catalog blast to hotel buyers you need contacts that are:
  - category buyer (not seller/competitor/personal/junk)
  - a real deliverable email (not a role-noise or fake address)
  - ideally a real person name (for personalisation), though role inboxes
    like purchasing@ are still valid catalog recipients
  - NOT a JA-internal address
  - NOT a supplier/competitor hiding in buyers

Output = a clean count of MAILABLE contacts, plus the exact buckets that need
fixing before send.
"""

import argparse
import asyncio
import re
import sys
from collections import Counter

from sqlalchemy import text

from app.database import async_session

try:
    from app.services.client_resolver import VENDOR_SEEDS
except Exception:
    VENDOR_SEEDS = set()

FREEMAIL = {"gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "aol.com",
            "icloud.com", "me.com", "live.com", "msn.com"}
ROLE = re.compile(r"^(info|sales|admin|office|contact|hello|team|marketing|hr|"
                  r"purchasing|procurement|reservations|frontdesk|fd|accounting|"
                  r"ap|ar|billing|payables|receivables|noreply|no-reply)([.\-_]|@|$)", re.I)

SQL = """
    SELECT id, email, first_name, last_name, display_name, organization,
           COALESCE(manual_category, contact_category,'') AS cat,
           matched_hotel_id
    FROM contacts
    WHERE email LIKE '%@%'
"""


def is_vendor(org):
    o = (org or "").lower()
    return bool(o) and any(v in o for v in VENDOR_SEEDS)


def has_name(r):
    return bool((r["first_name"] or r["display_name"] or "").strip())


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", type=int, default=10)
    args = ap.parse_args()

    async with async_session() as s:
        rows = [dict(r._mapping) for r in (await s.execute(text(SQL))).all()]
    total = len(rows)

    cats = Counter(r["cat"] or "(none)" for r in rows)
    print(f"\nMAILER READINESS  ({total:,} contacts)\n" + "=" * 68)
    print("\nCATEGORY BREAKDOWN:")
    for k, v in cats.most_common():
        print(f"  {k:<14} {v:>7,}  ({100.0*v/total:.0f}%)")

    buyers = [r for r in rows if r["cat"] == "buyer"]
    print(f"\nGRADING THE {len(buyers):,} BUYERS FOR SEND-READINESS:")

    mailable = []
    problems = {
        "supplier in buyers": [],
        "freemail (personal, not business)": [],
        "role/no-reply inbox": [],
        "no name (blast-only, no personalisation)": [],
    }
    for r in buyers:
        dom = r["email"].split("@")[-1].lower()
        local = r["email"].split("@")[0].lower()
        if is_vendor(r["organization"]):
            problems["supplier in buyers"].append(r)
            continue
        if dom in FREEMAIL:
            problems["freemail (personal, not business)"].append(r)
        if ROLE.match(local):
            problems["role/no-reply inbox"].append(r)
        if not has_name(r):
            problems["no name (blast-only, no personalisation)"].append(r)
        mailable.append(r)

    named = [r for r in mailable if has_name(r)]
    print(f"\n  MAILABLE (buyer, real business email)  : {len(mailable):,}")
    print(f"    of those, with a person name          : {len(named):,}")
    print(f"    role/generic inbox (still valid)       : {len(mailable)-len(named):,}")

    print("\n  NEEDS ATTENTION before send:")
    for label, items in problems.items():
        print(f"    {label:<42} {len(items):>6,}")

    # what's sitting in other categories that might be miscategorised buyers
    junk_buyers = [r for r in rows if r["cat"] == "junk" and r["matched_hotel_id"]]
    print(f"\n  possible buyers stuck in junk (linked to a hotel): {len(junk_buyers):,}")

    if args.sample:
        S = args.sample
        if problems["supplier in buyers"]:
            print(f"\n  suppliers mislabelled as buyers (sample {S}):")
            for r in problems["supplier in buyers"][:S]:
                print(f"     {r['email'][:40]:<42} {r['organization']}")
        if problems["freemail (personal, not business)"]:
            print(f"\n  freemail buyers — personal addresses (sample {S}):")
            for r in problems["freemail (personal, not business)"][:S]:
                print(f"     {r['email'][:40]:<42} {r['organization'] or ''}")

    print("\n" + "=" * 68)
    print("  Read-only. This is your pre-send worklist.\n")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except KeyboardInterrupt:
        sys.exit(130)
