# -*- coding: utf-8 -*-
"""
BULK CONTACT ENRICHMENT
========================
Enriches leads that have no contacts, in priority order: URGENT > HOT > WARM > COOL > TBD.
Uses the same enrichment + save logic as the dashboard endpoint.

Usage:
    python bulk_enrich.py              # Enrich all unenriched leads
    python bulk_enrich.py --limit 10   # Enrich first 10 only
    python bulk_enrich.py --timeline URGENT  # Only URGENT leads
"""
import asyncio
import argparse
import os
import sys
import time
import logging

sys.path.insert(0, os.getcwd())
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# Timeline priority order
TIMELINE_PRIORITY = ['URGENT', 'HOT', 'WARM', 'COOL', 'TBD']


async def get_unenriched_leads(timeline_filter=None, limit=None):
    """Get leads with no contacts in lead_contacts table, ordered by priority."""
    from app.database import async_session
    from sqlalchemy import text

    timeline_clause = ""
    if timeline_filter:
        timeline_clause = f"AND pl.timeline_label = '{timeline_filter.upper()}'"

    limit_clause = f"LIMIT {limit}" if limit else ""

    query = f"""
        SELECT pl.id, pl.hotel_name, pl.brand, pl.city, pl.state, pl.country,
               pl.management_company, pl.opening_date, pl.lead_score, pl.timeline_label
        FROM potential_leads pl
        LEFT JOIN lead_contacts lc ON lc.lead_id = pl.id
        WHERE pl.status = 'new'
        {timeline_clause}
        GROUP BY pl.id
        HAVING COUNT(lc.id) = 0
        ORDER BY
            CASE pl.timeline_label
                WHEN 'URGENT' THEN 1
                WHEN 'HOT' THEN 2
                WHEN 'WARM' THEN 3
                WHEN 'COOL' THEN 4
                ELSE 5
            END,
            pl.lead_score DESC
        {limit_clause}
    """

    async with async_session() as session:
        rows = (await session.execute(text(query))).fetchall()

    return [dict(zip(
        ['id', 'hotel_name', 'brand', 'city', 'state', 'country',
         'management_company', 'opening_date', 'lead_score', 'timeline_label'],
        row
    )) for row in rows]


async def save_enrichment(lead_id, hotel_name, enrichment_result):
    """Save enrichment results to PotentialLead + LeadContact tables.
    Replicates the logic from app/routes/contacts.py."""
    from app.database import async_session
    from sqlalchemy import select
    from app.models.potential_lead import PotentialLead
    from app.models.lead_contact import LeadContact
    from app.services.utils import local_now, normalize_hotel_name
    from app.services.rescore import rescore_lead

    async with async_session() as session:
        lead_result = await session.execute(
            select(PotentialLead).where(PotentialLead.id == lead_id)
        )
        lead = lead_result.scalar_one_or_none()
        if not lead:
            return {"status": "error", "message": "Lead not found"}

        updated_fields = []

        # Update lead fields (only fill empty)
        if enrichment_result.management_company and not lead.management_company:
            lead.management_company = enrichment_result.management_company
            updated_fields.append("management_company")
        if enrichment_result.developer and not lead.developer:
            lead.developer = enrichment_result.developer
            updated_fields.append("developer")

        if enrichment_result.best_contact:
            bc = enrichment_result.best_contact
            if bc.get("name") and not lead.contact_name:
                lead.contact_name = bc["name"]
                updated_fields.append("contact_name")
            if bc.get("title") and not lead.contact_title:
                lead.contact_title = bc["title"]
                updated_fields.append("contact_title")
            if bc.get("email") and not lead.contact_email:
                lead.contact_email = bc["email"]
                updated_fields.append("contact_email")
            if bc.get("phone") and not lead.contact_phone:
                lead.contact_phone = bc["phone"]
                updated_fields.append("contact_phone")

        lead.updated_at = local_now()

        # Rescore with new contact data
        await rescore_lead(lead_id, session)

        # Save contacts to lead_contacts table
        contacts_saved = 0
        if enrichment_result.contacts:
            existing_contacts = await session.execute(
                select(LeadContact).where(LeadContact.lead_id == lead_id)
            )
            existing_names = {
                normalize_hotel_name(c.name)
                for c in existing_contacts.scalars().all()
            }

            for i, c in enumerate(enrichment_result.contacts):
                name = c.get("name", "").strip()
                if not name:
                    continue

                normalized_name = normalize_hotel_name(name)
                if normalized_name in existing_names:
                    continue

                contact = LeadContact(
                    lead_id=lead_id,
                    name=c["name"],
                    title=c.get("title"),
                    email=c.get("email"),
                    phone=c.get("phone"),
                    linkedin=c.get("linkedin"),
                    organization=c.get("organization"),
                    scope=c.get("scope", "unknown"),
                    confidence=c.get(
                        "_validation_confidence", c.get("confidence", "medium")
                    ),
                    tier=c.get("_buyer_tier"),
                    score=c.get("_validation_score", 0),
                    is_primary=(i == 0),
                    found_via=", ".join(enrichment_result.layers_tried)
                    if enrichment_result.layers_tried
                    else "web_search",
                    source_detail=c.get(
                        "confidence_note", c.get("_validation_reason", "")
                    ),
                    evidence_url=c.get("source"),
                    last_enriched_at=local_now(),
                )
                session.add(contact)
                contacts_saved += 1

        await session.commit()

    return {
        "status": "enriched" if contacts_saved > 0 else "no_contacts",
        "contacts_saved": contacts_saved,
        "updated_fields": updated_fields,
    }


async def main():
    parser = argparse.ArgumentParser(description="Bulk contact enrichment")
    parser.add_argument("--limit", type=int, default=None, help="Max leads to enrich")
    parser.add_argument("--timeline", type=str, default=None, help="Filter by timeline: URGENT, HOT, WARM, COOL, TBD")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be enriched without running")
    args = parser.parse_args()

    from app.services.contact_enrichment import enrich_lead_contacts

    leads = await get_unenriched_leads(timeline_filter=args.timeline, limit=args.limit)

    print(f"\n{'=' * 60}")
    print(f"BULK CONTACT ENRICHMENT")
    print(f"{'=' * 60}")
    print(f"  Leads to enrich: {len(leads)}")
    if args.timeline:
        print(f"  Timeline filter: {args.timeline}")
    if args.limit:
        print(f"  Limit: {args.limit}")

    # Show breakdown
    timeline_counts = {}
    for lead in leads:
        tl = lead['timeline_label'] or 'TBD'
        timeline_counts[tl] = timeline_counts.get(tl, 0) + 1
    print(f"\n  Breakdown:")
    for tl in TIMELINE_PRIORITY:
        if tl in timeline_counts:
            print(f"    {timeline_counts[tl]:>3}x {tl}")
    print()

    if args.dry_run:
        print("DRY RUN - listing leads that would be enriched:\n")
        for lead in leads:
            print(f"  [{lead['id']}] {lead['hotel_name']} | {lead['timeline_label']} | score: {lead['lead_score']} | {lead['city']}, {lead['state']}")
        return

    # Enrich each lead
    total = len(leads)
    success = 0
    no_contacts = 0
    errors = 0
    total_contacts = 0
    start_time = time.time()

    for idx, lead in enumerate(leads):
        lid = lead['id']
        name = lead['hotel_name']
        tl = lead['timeline_label']
        elapsed = time.time() - start_time
        rate = (idx / elapsed * 60) if elapsed > 0 and idx > 0 else 0

        print(f"\n[{idx+1}/{total}] [{tl}] {name} ({lead['city']}, {lead['state']}) score:{lead['lead_score']}")
        if idx > 0:
            eta_min = (total - idx) / rate if rate > 0 else 0
            print(f"  Rate: {rate:.1f}/min | ETA: {eta_min:.0f} min remaining")

        try:
            result = await enrich_lead_contacts(
                lead_id=lid,
                hotel_name=name,
                brand=lead['brand'],
                city=lead['city'],
                state=lead['state'],
                country=lead['country'],
                management_company=lead['management_company'],
                opening_date=lead['opening_date'],
            )

            if result and result.contacts:
                save_result = await save_enrichment(lid, name, result)
                contacts_count = save_result.get('contacts_saved', 0)
                total_contacts += contacts_count
                success += 1
                contact_names = [c.get('name', '?') for c in result.contacts[:3]]
                print(f"  ENRICHED: {contacts_count} contacts saved -> {', '.join(contact_names)}")
            else:
                no_contacts += 1
                print(f"  NO CONTACTS FOUND (layers: {result.layers_tried if result else 'none'})")

        except Exception as e:
            errors += 1
            print(f"  ERROR: {str(e)[:100]}")

    elapsed = time.time() - start_time

    print(f"\n{'=' * 60}")
    print(f"BULK ENRICHMENT COMPLETE")
    print(f"{'=' * 60}")
    print(f"  Total processed:  {total}")
    print(f"  Enriched:         {success}")
    print(f"  No contacts:      {no_contacts}")
    print(f"  Errors:           {errors}")
    print(f"  Contacts saved:   {total_contacts}")
    print(f"  Time:             {elapsed/60:.1f} min")
    print(f"  Rate:             {total/(elapsed/60):.1f} leads/min")

asyncio.run(main())
