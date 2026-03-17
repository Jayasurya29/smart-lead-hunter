"""
One-time backfill: Compute timeline_label for all existing leads.

Run after migration 002_add_timeline_label:
    python backfill_timeline.py

This uses the Python get_timeline_label() function for accurate parsing
of dates like "Q2 2026", "Spring 2027", "March 2026", bare "2026", etc.
"""

import asyncio
import os
import sys

from dotenv import load_dotenv

load_dotenv()

# Ensure app is importable
sys.path.insert(0, os.path.dirname(__file__))

from sqlalchemy import select, update
from app.database import async_session
from app.models.potential_lead import PotentialLead
from app.services.utils import get_timeline_label


async def backfill():
    async with async_session() as session:
        result = await session.execute(
            select(PotentialLead.id, PotentialLead.opening_date)
        )
        rows = result.all()

        total = len(rows)
        updated = 0
        counts = {}

        for lead_id, opening_date in rows:
            label = get_timeline_label(opening_date or "")
            counts[label] = counts.get(label, 0) + 1

            await session.execute(
                update(PotentialLead)
                .where(PotentialLead.id == lead_id)
                .values(timeline_label=label)
            )
            updated += 1

            if updated % 100 == 0:
                print(f"  Processed {updated}/{total}...")

        await session.commit()

    print(f"\nBackfill complete: {updated} leads updated")
    print("Distribution:")
    for label in ("HOT", "URGENT", "WARM", "COOL", "LATE", "EXPIRED", "TBD"):
        print(f"  {label}: {counts.get(label, 0)}")


if __name__ == "__main__":
    print("Backfilling timeline_label for all leads...")
    asyncio.run(backfill())
