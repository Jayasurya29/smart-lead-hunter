"""Classify shared mailboxes by FUNCTION (2026-06-08).

After fix_shared_mailboxes flagged is_shared_mailbox=TRUE, this splits them:
  - purchasing@/procurement@  -> category 'buyer'  (real buying channel)
  - accounting@/ap@/frontdesk@/admin@/hr@/... -> category 'operational',
    opportunity cleared (billing/ops — never a sales prospect)

How big CRMs model this: a department inbox is account infrastructure,
not a person. Operational ones drop out of the prospect/people view;
buying ones stay reachable on the sell side.

The DRY RUN also prints the contact_category column type + any CHECK
constraint + distinct opportunity values, so we know BEFORE --apply
whether 'operational' needs a migration or writes as free text.

Run:
    python classify_mailbox_function.py            # dry-run + schema probe
    python classify_mailbox_function.py --apply
"""

import asyncio
import re
import sys

from sqlalchemy import text

from app.database import async_session

# buying-side signals in the local part
_BUYING = re.compile(r"purchas|procure|sourcing|buyer|supply", re.I)


async def main(apply: bool) -> None:
    async with async_session() as db:
        # ── schema probe (so we never blind-write an invalid enum) ──
        col_type = (
            await db.execute(
                text(
                    "SELECT data_type FROM information_schema.columns "
                    "WHERE table_name='contacts' AND column_name='contact_category'"
                )
            )
        ).scalar()
        checks = (
            (
                await db.execute(
                    text(
                        "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
                        "WHERE conrelid='contacts'::regclass AND contype='c' "
                        "AND pg_get_constraintdef(oid) ILIKE '%category%'"
                    )
                )
            )
            .scalars()
            .all()
        )
        opp_vals = (
            (
                await db.execute(
                    text(
                        "SELECT DISTINCT opportunity_level FROM contacts "
                        "WHERE opportunity_level IS NOT NULL"
                    )
                )
            )
            .scalars()
            .all()
        )
        print(f"contact_category type: {col_type}")
        print(f"category CHECK constraints: {checks or 'none (free text)'}")
        print(f"distinct opportunity_level values: {sorted(v for v in opp_vals)}\n")

        rows = (
            await db.execute(
                text(
                    "SELECT id, email, inferred_role, contact_category, "
                    "       opportunity_level, organization "
                    "FROM contacts WHERE is_shared_mailbox = TRUE"
                )
            )
        ).mappings().all()

        buying, operational = [], []
        for r in rows:
            local = (r["email"] or "").split("@", 1)[0]
            (buying if _BUYING.search(local) else operational).append(r)

        print(f"BUYING channels -> category 'buyer' ({len(buying)}):")
        for r in buying:
            print(f"  #{r['id']:>5}  {r['email']:<42} (was '{r['contact_category']}')  "
                  f"{r['organization'] or '-'}")

        print(f"\nOPERATIONAL -> category 'operational', opportunity cleared ({len(operational)}):")
        for r in operational:
            print(f"  #{r['id']:>5}  {r['email']:<42} (was '{r['contact_category']}'"
                  f" / opp '{r['opportunity_level']}')  {r['organization'] or '-'}")

        if not apply:
            print("\nDRY RUN — re-run with --apply to write.")
            print("If the type above is USER-DEFINED (an enum) with a CHECK that "
                  "lacks 'operational', tell me and I'll ship a 1-line migration first.")
            return

        for r in buying:
            await db.execute(
                text("UPDATE contacts SET contact_category = 'buyer' WHERE id = :cid"),
                {"cid": r["id"]},
            )
        for r in operational:
            await db.execute(
                text(
                    "UPDATE contacts SET contact_category = 'operational', "
                    "opportunity_level = NULL WHERE id = :cid"
                ),
                {"cid": r["id"]},
            )
        await db.commit()
        print(f"\nAPPLIED: {len(buying)} buying, {len(operational)} operational.")


if __name__ == "__main__":
    asyncio.run(main(apply="--apply" in sys.argv))
