"""
audit_name_quality.py
=====================
Confirms (and sizes) the "display_name is actually the domain/email" bug.

The People API path stores Google's synthesized displayName verbatim, and
_apply_name_gate bails early when there's no first/last — so nameless
contacts can keep a display_name that is just their email or bare domain,
which then renders as the person's name in the UI.

Run from repo root, venv active, DATABASE_URL set:
    python audit_name_quality.py
    python audit_name_quality.py jbell@premiumparking.com   # inspect one row
"""

import asyncio
import sys

from sqlalchemy import text

from app.database import async_session


async def main() -> None:
    target = sys.argv[1].strip() if len(sys.argv) > 1 else "jbell@premiumparking.com"

    async with async_session() as s:
        print("=" * 64)
        print(f" RAW ROW for {target}")
        print("=" * 64)
        row = (
            await s.execute(
                text(
                    "SELECT display_name, first_name, last_name, organization, "
                    "contact_category, category_source, enrichment_source "
                    "FROM contacts WHERE email ILIKE :e LIMIT 1"
                ),
                {"e": target},
            )
        ).first()
        if not row:
            print("  (not found)")
        else:
            dn, fn, ln, org, cat, csrc, esrc = row
            print(f"  display_name     : {dn!r}")
            print(f"  first_name       : {fn!r}")
            print(f"  last_name        : {ln!r}")
            print(f"  organization     : {org!r}")
            print(f"  contact_category : {cat!r}  (source: {csrc!r})")
            print(f"  enrichment_source: {esrc!r}")
            dom = target.split("@")[-1]
            if dn and (dn.lower() == dom or "@" in dn or dn.lower() == target):
                print("  >>> display_name IS the email/domain — confirms the bug.")

        total = (await s.execute(text("SELECT COUNT(*) FROM contacts"))).scalar() or 0

        # blast radius: how many contacts have a junk display_name?
        n_email = (
            await s.execute(
                text("SELECT COUNT(*) FROM contacts WHERE display_name LIKE '%@%'")
            )
        ).scalar() or 0
        # display_name == its own email domain, no space (bare-domain-as-name)
        n_domain = (
            await s.execute(
                text(
                    "SELECT COUNT(*) FROM contacts "
                    "WHERE display_name IS NOT NULL "
                    "AND position(' ' in display_name) = 0 "
                    "AND display_name LIKE '%.%' "
                    "AND email ILIKE ('%@' || display_name)"
                )
            )
        ).scalar() or 0

        print("\n" + "=" * 64)
        print(" BLAST RADIUS")
        print("=" * 64)
        print(f"  total contacts                         : {total:,}")
        print(f"  display_name contains '@' (full email) : {n_email:,}")
        print(f"  display_name == its bare email domain  : {n_domain:,}")
        print(f"  => junk-name contacts (approx)         : {n_email + n_domain:,}")

        print("\n  samples (domain-as-name):")
        rows = (
            await s.execute(
                text(
                    "SELECT display_name, email, organization FROM contacts "
                    "WHERE display_name IS NOT NULL "
                    "AND position(' ' in display_name) = 0 "
                    "AND display_name LIKE '%.%' "
                    "AND email ILIKE ('%@' || display_name) LIMIT 10"
                )
            )
        ).all()
        for dn, em, org in rows:
            print(f"    name={dn!r:<32} email={em:<34} org={org!r}")

        print("=" * 64)
        print(" If the counts are non-trivial, the fix is two-fold: (1) null a")
        print(" display_name that equals the email/domain at ingest, and (2) make")
        print(" _apply_name_gate sanitize display_name even when first/last are empty.")
        print("=" * 64)


if __name__ == "__main__":
    asyncio.run(main())
