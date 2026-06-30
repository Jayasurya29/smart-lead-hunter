    """
review_pending_moves.py
======================
The human side of the move-review queue. Unverified moves (contacts with no
LinkedIn slug, where the lookup can't prove it's the right person) are parked in
pending_moves instead of being written. This tool lets you review them.

    python review_pending_moves.py                 # list pending candidates
    python review_pending_moves.py --approve "1,5" # apply those moves (re-file +
                                                   #   former affiliation)
    python review_pending_moves.py --reject "2,3"  # mark rejected (no change)

Approving does exactly what a verified move does: sets the contact's
organization to the new employer and records the old org as a 'former'
affiliation (preserving the stale email in notes). Rejecting just closes the
candidate so it won't resurface.

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
                    "SELECT contact_id, email, from_org, to_org, to_title "
                    "FROM pending_moves WHERE id=:id AND status='pending'"),
                    {"id": pid})).mappings().one_or_none()
                if not row:
                    print(f"  #{pid}: not a pending candidate, skipped")
                    continue
                cid, from_org, to_org = row["contact_id"], row["from_org"], row["to_org"]
                # re-file the contact to the new employer
                await s.execute(text(
                    "UPDATE contacts SET organization=:org, title=COALESCE(NULLIF(:t,''), title), "
                    "enrichment_source='grounded', updated_at=:now WHERE id=:id"),
                    {"org": to_org, "t": row["to_title"] or "", "now": datetime.now(timezone.utc), "id": cid})
                # record the old org as 'former' (idempotent on account_name)
                exists = (await s.execute(text(
                    "SELECT 1 FROM contact_affiliations WHERE person_type='contact' "
                    "AND person_id=:id AND relationship='former' "
                    "AND lower(COALESCE(account_name,''))=lower(:nm) LIMIT 1"),
                    {"id": cid, "nm": from_org})).one_or_none()
                if not exists and from_org:
                    await s.execute(text(
                        "INSERT INTO contact_affiliations (person_type, person_id, "
                        "account_type, account_name, relationship, source, confidence, "
                        "notes, created_at, updated_at) VALUES ('contact', :id, "
                        "'management_company', :nm, 'former', 'review_approved', 0.7, "
                        ":notes, :now, :now)"),
                        {"id": cid, "nm": from_org,
                         "notes": f"Moved to {to_org} (human-approved from review queue)"
                                  + (f" | former_email={row['email']}" if row["email"] else ""),
                         "now": datetime.now(timezone.utc)})
                await s.execute(text(
                    "UPDATE pending_moves SET status='approved', reviewed_at=:now WHERE id=:id"),
                    {"now": datetime.now(timezone.utc), "id": pid})
                n += 1
                print(f"  approved #{pid}: {from_org!r} -> {to_org!r} (contact {cid})")
            await s.commit()
            print(f"\n  DONE — applied {n} approved move(s).")
            if not REJECT:
                return

        if REJECT:
            res = await s.execute(text(
                "UPDATE pending_moves SET status='rejected', reviewed_at=:now "
                "WHERE id = ANY(:ids) AND status='pending'"),
                {"now": datetime.now(timezone.utc), "ids": REJECT})
            await s.commit()
            print(f"  rejected {res.rowcount} candidate(s): {REJECT}")
            return

        rows = (await s.execute(text(
            "SELECT id, contact_id, email, name, from_org, to_org, to_title, created_at "
            "FROM pending_moves WHERE status='pending' ORDER BY created_at DESC"
        ))).mappings().all()
        print("=" * 78)
        print(f" PENDING MOVES — awaiting review: {len(rows)}")
        print(" These had NO LinkedIn slug to verify the person. Approve real ones,")
        print(" reject namesake/work-history noise.")
        print("=" * 78)
        for r in rows:
            print(f"  #{r['id']:<5} {r['name'] or '?':<22} {r['from_org'] or '?':<26} -> {r['to_org']}")
            print(f"         {r['email']}  ({r['to_title'] or 'no title'})")
        print("\n  --approve \"id,id\"  applies the move   |   --reject \"id,id\"  discards")


if __name__ == "__main__":
    asyncio.run(main())
