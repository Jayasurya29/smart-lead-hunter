#!/usr/bin/env python3
"""backfill_company_coverage.py -- portfolio coverage for management/parking cos.

Many inbox contacts sit on a management/parking-operator domain (TownePark, SP+,
Metropolis, LAZ ...). They can't link to ONE hotel -- the person covers the
operator's whole portfolio. This writes a 'covers' affiliation at the
management_company level (account_type='management_company', scope='portfolio')
for a CURATED set of operator domains. Brand HQ (hilton/marriott) and
schools/freemail are intentionally excluded.

DRY-RUN by default. --apply to write. Idempotent (ON CONFLICT DO NOTHING).
Usage:  python scripts/backfill_company_coverage.py [--apply]
"""

import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

from sqlalchemy import text  # noqa: E402

from app.database import async_session  # noqa: E402

# Curated operator domain -> canonical management/parking company name.
COMPANY_BY_DOMAIN = {
    "townepark.com": "Towne Park",
    "spplus.com": "SP+",
    "metropolis.io": "Metropolis",
    "parkingmgt.com": "Parking Management Company",
    "reefparking.com": "REEF",
    "lazparking.com": "LAZ Parking",
    "denisonparking.com": "Denison Parking",
    "kwpmc.com": "KW Property Management",
    "schultehospitality.com": "Schulte Hospitality Group",
    "oasismarinas.com": "Oasis Marinas",
    "loewshotels.com": "Loews Hotels",
    "southbeachgroup.com": "South Beach Group",
    "rosenhotels.com": "Rosen Hotels & Resorts",
    "pyramidglobal.com": "Pyramid Global Hospitality",
    "reimaginedparking.com": "Reimagined Parking",
    "sedanos.com": "Sedano's",
    "compass-usa.com": "Compass Group",
}


async def main(args):
    async with async_session() as s:
        rows = (
            await s.execute(
                text(
                    "SELECT id, split_part(email,'@',2) AS domain FROM contacts "
                    "WHERE matched_hotel_id IS NULL AND matched_lead_id IS NULL "
                    "AND COALESCE(contact_category,'') <> 'junk' AND email LIKE '%@%'"
                )
            )
        ).all()

        per_company = {}
        for r in rows:
            company = COMPANY_BY_DOMAIN.get((r.domain or "").lower())
            if not company:
                continue
            per_company[company] = per_company.get(company, 0) + 1
            if args.apply:
                await s.execute(
                    text(
                        "INSERT INTO contact_affiliations (person_type, person_id, account_type, "
                        "account_name, relationship, scope, source, confidence, notes, created_at, updated_at) "
                        "VALUES ('contact', :pid, 'management_company', :nm, 'covers', 'portfolio', "
                        ":src, 0.9, :notes, NOW(), NOW()) "
                        "ON CONFLICT DO NOTHING"
                    ),
                    {"pid": r.id, "nm": company, "src": "matched", "notes": f"Portfolio coverage ({company})"},
                )
        if args.apply:
            await s.commit()

    total = sum(per_company.values())
    mode = "APPLIED" if args.apply else "DRY-RUN (no writes)"
    print(f"\n{mode}: {total} contacts -> management-company portfolio coverage")
    for c, n in sorted(per_company.items(), key=lambda x: -x[1]):
        print(f"  {n:>5}  {c}")
    if not args.apply and total:
        print("\nRe-run with --apply to write.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    asyncio.run(main(ap.parse_args()))
