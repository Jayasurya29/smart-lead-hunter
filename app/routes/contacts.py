"""Contact management and lead enrichment endpoints.

FIX: All contact CRUD endpoints now use Depends(get_db) for proper lifecycle.
FIX: Enrichment errors return proper HTTP status codes (not 200).
FIX: Removed phantom contact_linkedin reference.
NOTE: enrich_lead() intentionally uses manual sessions because the enrichment
      network calls take 10-30s — we don't want to hold a DB connection that long.
"""

import logging
import os

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db, async_session
from app.models import PotentialLead
from app.models.lead_contact import LeadContact
from app.services.rescore import rescore_lead
from app.services.utils import local_now, normalize_hotel_name
from app.shared import require_ajax

logger = logging.getLogger(__name__)

router = APIRouter()


# ═══════════════════════════════════════════════════════════════
# ENRICHMENT
# ═══════════════════════════════════════════════════════════════


@router.post("/api/dashboard/leads/{lead_id}/enrich", tags=["Dashboard"])
async def enrich_lead(lead_id: int, _csrf=Depends(require_ajax)):
    """Enrich a lead with contact information via web search."""
    # Session 1: Read lead data (extract to local vars, then close)
    async with async_session() as session:
        result = await session.execute(
            select(PotentialLead).where(PotentialLead.id == lead_id)
        )
        lead = result.scalar_one_or_none()
        if not lead:
            raise HTTPException(status_code=404, detail="Lead not found")

        hotel_name = lead.hotel_name
        brand = lead.brand or ""
        city = lead.city or ""
        state = lead.state or ""
        country = lead.country or "USA"
        management_company = lead.management_company or ""
        opening_date = lead.opening_date or ""
        # FIX C-04: Capture updated_at for optimistic lock check
        lead_updated_at = lead.updated_at

    # Run enrichment (network calls — can take 10-30 seconds)
    try:
        from app.services.contact_enrichment import ContactEnrichmentEngine

        engine = ContactEnrichmentEngine(
            serper_api_key=os.getenv("SERPER_API_KEY", ""),
            apollo_api_key=os.getenv("APOLLO_API_KEY", ""),
        )

        enrichment_result = await engine.enrich_lead(
            hotel_name=hotel_name,
            brand=brand,
            city=city,
            state=state,
            country=country,
            management_company=management_company,
            opening_date=opening_date,
        )
    except Exception as e:
        logger.error(f"Enrichment failed for lead {lead_id}: {e}", exc_info=True)
        return JSONResponse(
            content={
                "status": "error",
                "message": f"Enrichment failed: {str(e)[:200]}",
            },
            status_code=502,
        )

    # Session 2: Save results (with optimistic lock check)
    try:
        async with async_session() as session:
            lead_result = await session.execute(
                select(PotentialLead).where(PotentialLead.id == lead_id)
            )
            lead = lead_result.scalar_one_or_none()
            if not lead:
                return JSONResponse(
                    content={
                        "status": "error",
                        "message": "Lead not found after enrichment",
                    },
                    status_code=404,
                )

            # FIX C-04: Optimistic lock — warn if lead was edited during enrichment
            # Only fill empty fields (never overwrite), so concurrent edits are safe
            # for fields that were already populated. Log a warning for awareness.
            if (
                lead_updated_at
                and lead.updated_at
                and lead.updated_at > lead_updated_at
            ):
                logger.warning(
                    f"Lead {lead_id} was modified during enrichment "
                    f"(before={lead_updated_at}, now={lead.updated_at}). "
                    f"Only filling empty fields to avoid overwriting edits."
                )

            updated_fields = []

            if enrichment_result.management_company and not lead.management_company:
                lead.management_company = enrichment_result.management_company
                updated_fields.append("management_company")
            if enrichment_result.developer and not lead.developer:
                lead.developer = enrichment_result.developer
                updated_fields.append("developer")
            if enrichment_result.owner and not lead.owner:
                lead.owner = enrichment_result.owner
                updated_fields.append("owner")

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

            save_result = {
                "status": "enriched" if updated_fields else "no_new_data",
                "updated_fields": updated_fields,
            }

            # Save contacts to lead_contacts table
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
                        # Update existing contact with new data
                        ec_result = await session.execute(
                            select(LeadContact).where(
                                LeadContact.lead_id == lead_id,
                                LeadContact.name == name,
                            )
                        )
                        ec = ec_result.scalar_one_or_none()
                        if ec:
                            filled = []
                            if not ec.email and c.get("email"):
                                ec.email = c["email"]
                                filled.append("email")
                            if not ec.phone and c.get("phone"):
                                ec.phone = c["phone"]
                                filled.append("phone")
                            if not ec.linkedin and c.get("linkedin"):
                                ec.linkedin = c["linkedin"]
                                filled.append("linkedin")
                            if not ec.title and c.get("title"):
                                ec.title = c["title"]
                                filled.append("title")
                            if not ec.organization and c.get("organization"):
                                ec.organization = c["organization"]
                                filled.append("organization")
                            if not ec.evidence_url and c.get("source"):
                                ec.evidence_url = c["source"]
                                filled.append("evidence_url")
                            if filled:
                                logger.info(
                                    f"Updated {ec.name}: filled {', '.join(filled)}"
                                )
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

            await session.commit()

        return {
            "status": save_result["status"],
            "lead_id": lead_id,
            "hotel_name": hotel_name,
            "contacts_found": len(enrichment_result.contacts),
            "best_contact": enrichment_result.best_contact,
            "management_company": enrichment_result.management_company,
            "developer": enrichment_result.developer,
            "layers_tried": enrichment_result.layers_tried,
            "sources_used": enrichment_result.sources_used,
            "updated_fields": save_result.get("updated_fields", []),
            "errors": enrichment_result.errors,
        }

    except Exception as e:
        logger.error(
            f"Failed to save enrichment for lead {lead_id}: {e}", exc_info=True
        )
        return JSONResponse(
            content={"status": "error", "message": f"Failed to save: {str(e)[:200]}"},
            status_code=500,
        )


# ═══════════════════════════════════════════════════════════════
# CONTACT MANAGEMENT
# ═══════════════════════════════════════════════════════════════


@router.get("/api/dashboard/leads/{lead_id}/contacts")
async def list_contacts(lead_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(LeadContact)
        .where(LeadContact.lead_id == lead_id)
        .order_by(
            LeadContact.is_saved.desc(),
            LeadContact.is_primary.desc(),
            LeadContact.score.desc(),
        )
    )
    return [c.to_dict() for c in result.scalars().all()]


@router.post("/api/dashboard/leads/{lead_id}/contacts/{contact_id}/save")
async def save_contact(
    lead_id: int,
    contact_id: int,
    db: AsyncSession = Depends(get_db),
    _csrf=Depends(require_ajax),
):
    result = await db.execute(
        select(LeadContact).where(
            LeadContact.id == contact_id, LeadContact.lead_id == lead_id
        )
    )
    contact = result.scalar_one_or_none()
    if not contact:
        raise HTTPException(status_code=404, detail="Contact not found")
    contact.is_saved = True
    contact.updated_at = local_now()
    await db.commit()
    return {"status": "saved", "contact_id": contact_id}


@router.post("/api/dashboard/leads/{lead_id}/contacts/{contact_id}/unsave")
async def unsave_contact(
    lead_id: int,
    contact_id: int,
    db: AsyncSession = Depends(get_db),
    _csrf=Depends(require_ajax),
):
    result = await db.execute(
        select(LeadContact).where(
            LeadContact.id == contact_id, LeadContact.lead_id == lead_id
        )
    )
    contact = result.scalar_one_or_none()
    if not contact:
        raise HTTPException(status_code=404, detail="Contact not found")
    contact.is_saved = False
    contact.updated_at = local_now()
    await db.commit()
    return {"status": "unsaved", "contact_id": contact_id}


@router.delete("/api/dashboard/leads/{lead_id}/contacts/{contact_id}")
async def delete_contact(
    lead_id: int,
    contact_id: int,
    db: AsyncSession = Depends(get_db),
    _csrf=Depends(require_ajax),
):
    result = await db.execute(
        select(LeadContact).where(
            LeadContact.id == contact_id, LeadContact.lead_id == lead_id
        )
    )
    contact = result.scalar_one_or_none()
    if not contact:
        raise HTTPException(status_code=404, detail="Contact not found")
    await db.delete(contact)
    # Flush the delete so rescore sees the updated contact count
    await db.flush()
    try:
        await rescore_lead(lead_id, db)
    except Exception:
        pass
    await db.commit()
    return {"status": "deleted", "contact_id": contact_id}


@router.post("/api/dashboard/leads/{lead_id}/contacts/{contact_id}/set-primary")
async def set_primary_contact(
    lead_id: int,
    contact_id: int,
    db: AsyncSession = Depends(get_db),
    _csrf=Depends(require_ajax),
):
    await db.execute(
        update(LeadContact)
        .where(LeadContact.lead_id == lead_id)
        .values(is_primary=False)
    )
    result = await db.execute(
        select(LeadContact).where(
            LeadContact.id == contact_id, LeadContact.lead_id == lead_id
        )
    )
    contact = result.scalar_one_or_none()
    if not contact:
        raise HTTPException(status_code=404, detail="Contact not found")
    contact.is_primary = True
    contact.is_saved = True
    contact.updated_at = local_now()
    lead_result = await db.execute(
        select(PotentialLead).where(PotentialLead.id == lead_id)
    )
    lead = lead_result.scalar_one_or_none()
    if lead:
        lead.contact_name = contact.name
        lead.contact_title = contact.title
        lead.contact_email = contact.email
        lead.contact_phone = contact.phone
        # NOTE: contact_linkedin column does not exist on PotentialLead.
        # LinkedIn is stored on the LeadContact record only.
        lead.updated_at = local_now()
    await db.commit()
    return {"status": "primary_set", "contact_id": contact_id}
