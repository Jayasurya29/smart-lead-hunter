"""
review_pending_names.py
=====================
The human side of the name-review queue. The resolver parks candidates here when
a web result's SURNAME + org match the email but the FIRST INITIAL doesn't
(r.brady@marriott.com -> "Lauren Brady": Brady + Marriott match, but L != R).
These are namesake-risky to auto-write, occasionally legitimate (nicknames,
middle names, surname-first email schemes), so you decide.

    python -m scripts.ops.review_pending_names                  # list pending
    python -m scripts.ops.review_pending_names --approve "3,7"  # write those names
    python -m scripts.ops.review_pending_names --reject "1,2"   # discard

Approving writes first/last/display_name onto the contact (enrichment_source=
'name_review_approved'). Rejecting closes the candidate so it won't resurface.

Run from repo root, venv active, DATABASE_URL set.
"""

import asyncio
import sys
from datetime import datetime, timezone

from sqlalchemy import text

from app.database import async_session


def _ids(flag):
    if flag in sys.argv:
        try:
            return [int(x) for x in sys.argv[sys.argv.index(flag) + 1].split(",")
                    if x.strip().isdigit()]
        except Exception:
            return []
    return []


APPROVE = _ids("--approve")
REJECT = _ids("--reject")


async def main() -> None:
    async with async_session() as s:
        if APPROVE:
            n = 0
            for pid in APPROVE:
                row = (await s.execute(text(
                    "SELECT contact_id, candidate_name FROM pending_names "
                    "WHERE id=:id AND status='pending'"), {"id": pid})).mappings().one_or_none()
                if not row:
                    print(f"  #{pid}: not a pending candidate, skipped")
                    continue
                parts = (row["candidate_name"] or "").split()
                if len(parts) < 2:
                    print(f"  #{pid}: candidate '{row['candidate_name']}' not splittable, skipped")
                    continue
                fn, ln = parts[0], parts[-1]
                await s.execute(text(
                    "UPDATE contacts SET first_name=:fn, last_name=:ln, "
                    "display_name=:dn, enrichment_source='name_review_approved', "
                    "updated_at=:now WHERE id=:cid"),
                    {"fn": fn, "ln": ln, "dn": f"{fn} {ln}",
                     "now": datetime.now(timezone.utc), "cid": row["contact_id"]})
                await s.execute(text(
                    "UPDATE pending_names SET status='approved', reviewed_at=:now WHERE id=:id"),
                    {"now": datetime.now(timezone.utc), "id": pid})
                n += 1
                print(f"  approved #{pid}: contact {row['contact_id']} -> {fn} {ln}")
            await s.commit()
            print(f"\n  DONE — wrote {n} approved name(s).")
            if not REJECT:
                return

        if REJECT:
            res = await s.execute(text(
                "UPDATE pending_names SET status='rejected', reviewed_at=:now "
                "WHERE id = ANY(:ids) AND status='pending'"),
                {"now": datetime.now(timezone.utc), "ids": REJECT})
            await s.commit()
            print(f"  rejected {res.rowcount} candidate(s): {REJECT}")
            return

        rows = (await s.execute(text(
            "SELECT id, contact_id, email, org, candidate_name, created_at "
            "FROM pending_names WHERE status='pending' ORDER BY created_at DESC"
        ))).mappings().all()
        print("=" * 78)
        print(f" PENDING NAMES — awaiting review: {len(rows)}")
        print(" Surname + org matched the email but the first initial didn't.")
        print(" Approve real ones (nicknames/middle names), reject namesakes.")
        print("=" * 78)
        for r in rows:
            print(f"  #{r['id']:<5} {r['email']:<38} ?-> {r['candidate_name']}")
            print(f"         (org: {r['org'] or '?'})")
        print("\n  --approve \"id,id\"  writes the name   |   --reject \"id,id\"  discards")


if __name__ == "__main__":
    asyncio.run(main())
