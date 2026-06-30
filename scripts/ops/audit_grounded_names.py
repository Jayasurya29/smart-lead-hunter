"""
audit_grounded_names.py
======================
Re-checks every name-resolver write (enrichment_source='grounded_name') against
the CURRENT gate and sorts them into THREE buckets:

  KEEP     - current gate still accepts (good write, untouched)
  ROLLBACK - a REAL person on a person-shaped email, but the resolved name is
             wrong/unverifiable -> clear the name only; the contact stays a
             valid (now unnamed) contact, eligible for a clean re-resolve
  JUNK     - NOT a person (billing portal, company-as-person, role/system) ->
             mark manual_category='junk' so the resolver, check-status, and
             outreach all skip it forever (stops wasting Serper+LLM money)

DRY-RUN by default. --apply commits both the junk marks and the name clears.

Run from repo root, venv active, DATABASE_URL set:
    python audit_grounded_names.py
    python audit_grounded_names.py --apply
"""

import asyncio
import re
import sys

from sqlalchemy import text

from app.database import async_session
# reuse the SAME gate + word/domain lists the resolver uses (single source)
from resolve_contact_names import (
    _looks_like_person, _resolve_against_email, _COMPANY_WORDS, _BILLING_DOMAINS,
)

APPLY = "--apply" in sys.argv


def _is_junk_write(email: str, name: str) -> bool:
    """A written name that isn't a person at all: billing-portal address, or a
    company/brand name (Intuit Security, Google Cloud, Graduate Richmond)."""
    domain = email.split("@", 1)[1].lower() if "@" in email else ""
    if any(b in domain for b in _BILLING_DOMAINS):
        return True
    toks = [re.sub(r"[^a-z]", "", t.lower()) for t in re.split(r"[\s'\-]+", name) if t]
    if any(t in _COMPANY_WORDS for t in toks):
        return True
    if not _looks_like_person(email):
        return True
    return False


async def main() -> None:
    async with async_session() as s:
        rows = (await s.execute(text(
            "SELECT id, email, first_name, last_name, display_name "
            "FROM contacts WHERE enrichment_source='grounded_name' "
            "AND (first_name IS NOT NULL OR display_name IS NOT NULL)"
        ))).mappings().all()

        keep, rollback, junk = [], [], []
        for r in rows:
            email = r["email"] or ""
            name = " ".join(x for x in (r["first_name"], r["last_name"]) if x).strip() \
                or (r["display_name"] or "")
            if email and _looks_like_person(email) and _resolve_against_email(email, [name]):
                keep.append((r["id"], email, name))
            elif _is_junk_write(email, name):
                junk.append((r["id"], email, name))
            else:
                rollback.append((r["id"], email, name))

        print("=" * 70)
        print(f" GROUNDED-NAME AUDIT  ({'APPLY' if APPLY else 'DRY-RUN'})")
        print(f" total: {len(rows)}  keep: {len(keep)}  "
              f"rollback(real, clear name): {len(rollback)}  junk(mark junk): {len(junk)}")
        print("=" * 70)
        print("\n JUNK -> manual_category='junk' (never resolve/contact again):")
        for cid, email, name in junk:
            print(f"   #{cid:<7} {email:<42} {name}")
        print("\n ROLLBACK -> clear name, keep as a valid unnamed contact:")
        for cid, email, name in rollback:
            print(f"   #{cid:<7} {email:<42} {name}")

        if not APPLY:
            print("\n  DRY-RUN — nothing changed.")
            print(f"  --apply would: mark {len(junk)} junk, clear {len(rollback)} names.")
            return

        nj = nr = 0
        for cid, _e, _n in junk:
            await s.execute(text(
                "UPDATE contacts SET manual_category='junk', first_name=NULL, "
                "last_name=NULL, display_name=NULL, enrichment_source=NULL WHERE id=:id"),
                {"id": cid})
            nj += 1
        for cid, _e, _n in rollback:
            await s.execute(text(
                "UPDATE contacts SET first_name=NULL, last_name=NULL, "
                "display_name=NULL, enrichment_source=NULL WHERE id=:id"),
                {"id": cid})
            nr += 1
        await s.commit()
        print(f"\n  DONE — marked {nj} junk, cleared {nr} names "
              f"(rolled-back contacts kept, eligible for clean re-resolve).")


if __name__ == "__main__":
    asyncio.run(main())
