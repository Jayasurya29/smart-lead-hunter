# -*- coding: utf-8 -*-
"""
BULK RESCORE ALL LEADS
=======================
Rescores all active leads using enriched contacts from lead_contacts table.
Shows before/after comparison.

Usage:
    python rescore_all.py
"""

import asyncio
import logging

logging.basicConfig(level=logging.INFO, format="%(message)s")


async def run():
    from sqlalchemy import select
    from app.database import async_session
    from app.models.potential_lead import PotentialLead
    from app.services.rescore import rescore_lead

    print("=" * 65)
    print("  BULK RESCORE - Using Enriched Contacts")
    print("=" * 65)

    async with async_session() as s:
        # Load all active leads
        result = await s.execute(
            select(PotentialLead)
            .where(PotentialLead.status != "deleted")
            .order_by(PotentialLead.lead_score.desc())
        )
        leads = result.scalars().all()
        print(f"\n  Rescoring {len(leads)} leads...\n")

        changed = 0
        increased = 0
        decreased = 0
        biggest_jump = ("", 0)

        for lead in leads:
            score_result = await rescore_lead(lead.id, s)
            if not score_result:
                continue

            old = score_result["old_score"]
            new = score_result["new_score"]
            diff = score_result["change"]
            contacts = score_result["contacts_found"]
            hs = score_result["hotel_specific"]
            tier = score_result["contact_tier"]

            if diff != 0:
                changed += 1
                arrow = "+" if diff > 0 else ""
                if diff > 0:
                    increased += 1
                else:
                    decreased += 1
                if diff > biggest_jump[1]:
                    biggest_jump = (lead.hotel_name, diff)

                print(
                    f"  {arrow}{diff:3d} | {old:2d} -> {new:2d} | "
                    f"{contacts}c ({hs}hs) | {lead.hotel_name[:40]}"
                )
                if contacts > 0:
                    print(f"       {tier}")

        await s.commit()

        print(f"\n{'=' * 65}")
        print("  RESULTS:")
        print(f"  Total leads:  {len(leads)}")
        print(f"  Changed:      {changed}")
        print(f"  Increased:    {increased}")
        print(f"  Decreased:    {decreased}")
        if biggest_jump[1] > 0:
            print(f"  Biggest jump: +{biggest_jump[1]} ({biggest_jump[0][:40]})")
        print(f"{'=' * 65}")


if __name__ == "__main__":
    asyncio.run(run())
