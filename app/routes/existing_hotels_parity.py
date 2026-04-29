"""
Existing Hotels — Parity Routes
================================

Endpoints split out from existing_hotels.py to keep that file from growing.

  POST /api/existing-hotels/{hotel_id}/contacts/{contact_id}/toggle-scope
       Mirrors routes/contacts.toggle_contact_scope (the lead-side
       endpoint). Same 5 valid scopes, same apply_score_to_contact
       rescore.

  POST /api/existing-hotels/{hotel_id}/contacts/{contact_id}/enrich-email
       Mirrors routes/contacts.enrich_contact_email_route — manual Wiza
       email lookup. 2 credits per found email, LinkedIn-required, syncs
       to hotel.contact_email if is_primary.

  POST /api/existing-hotels/{hotel_id}/rescore
       Manually trigger a rescore using the Option B account-fit scorer.
       Cheap (<1ms, no external calls). Useful when the user manually
       edits brand_tier / room_count / hotel_type / zone in the Edit tab
       and wants the score to refresh without re-running Smart Fill.

Wire up in main.py:
    from app.routes.existing_hotels_parity import router as eh_parity_router
    app.include_router(eh_parity_router)
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.shared import require_ajax
from app.models.lead_contact import LeadContact
from app.models.existing_hotel import ExistingHotel
from app.services.utils import local_now

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/existing-hotels", tags=["existing-hotels-parity"])


# ─────────────────────────────────────────────────────────────────────────────
# Scope toggle
# ─────────────────────────────────────────────────────────────────────────────


@router.post("/{hotel_id}/contacts/{contact_id}/toggle-scope")
async def toggle_hotel_contact_scope(
    hotel_id: int,
    contact_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    _csrf=Depends(require_ajax),
):
    body = await request.json()
    new_scope = body.get("scope", "")

    _VALID_SCOPES = (
        "hotel_specific",
        "chain_area",
        "management_corporate",
        "chain_corporate",
        "owner",
    )
    if new_scope not in _VALID_SCOPES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid scope. Must be one of: {', '.join(_VALID_SCOPES)}",
        )

    result = await db.execute(
        select(LeadContact).where(
            LeadContact.id == contact_id,
            LeadContact.existing_hotel_id == hotel_id,
        )
    )
    contact = result.scalar_one_or_none()
    if not contact:
        raise HTTPException(status_code=404, detail="Contact not found")

    contact.scope = new_scope

    from app.services.contact_scoring import apply_score_to_contact

    apply_score_to_contact(
        contact,
        title=contact.title,
        scope=new_scope,
        strategist_priority=contact.strategist_priority,
    )
    contact.updated_at = local_now()

    if contact.is_primary:
        hotel_result = await db.execute(
            select(ExistingHotel).where(ExistingHotel.id == hotel_id)
        )
        hotel = hotel_result.scalar_one_or_none()
        if hotel:
            hotel.contact_name = contact.name
            hotel.contact_title = contact.title
            hotel.contact_email = contact.email
            hotel.contact_phone = contact.phone
            hotel.updated_at = local_now()

    await db.commit()
    return {"status": "updated", "scope": new_scope, "score": contact.score}


# ─────────────────────────────────────────────────────────────────────────────
# Wiza email lookup — single contact
# ─────────────────────────────────────────────────────────────────────────────


@router.post("/{hotel_id}/contacts/{contact_id}/enrich-email")
async def enrich_hotel_contact_email(
    hotel_id: int,
    contact_id: int,
    db: AsyncSession = Depends(get_db),
    _csrf=Depends(require_ajax),
):
    from app.services.wiza_enrichment import enrich_contact_email

    result = await db.execute(
        select(LeadContact).where(
            LeadContact.id == contact_id,
            LeadContact.existing_hotel_id == hotel_id,
        )
    )
    contact = result.scalar_one_or_none()
    if not contact:
        raise HTTPException(status_code=404, detail="Contact not found")

    if not contact.linkedin:
        raise HTTPException(
            status_code=422,
            detail="Contact has no LinkedIn URL — required for Wiza enrichment",
        )

    wiza_result = await enrich_contact_email(
        linkedin_url=contact.linkedin,
        contact_name=contact.name,
    )

    if not wiza_result:
        return {
            "status": "not_found",
            "contact_id": contact_id,
            "message": "Wiza could not find an email for this contact",
        }

    contact.email = wiza_result["email"]
    contact.found_via = f"wiza_{wiza_result['email_status']}"
    contact.updated_at = local_now()

    if contact.is_primary:
        hotel_res = await db.execute(
            select(ExistingHotel).where(ExistingHotel.id == hotel_id)
        )
        hotel = hotel_res.scalar_one_or_none()
        if hotel:
            hotel.contact_email = contact.email
            hotel.updated_at = local_now()

    await db.commit()

    return {
        "status": "found",
        "contact_id": contact_id,
        "email": wiza_result["email"],
        "email_status": wiza_result["email_status"],
        "confidence": wiza_result["confidence"],
        "credits_used": wiza_result.get("credits_used", 2),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Rescore — manual trigger of the Option B scorer
# ─────────────────────────────────────────────────────────────────────────────


@router.post("/{hotel_id}/rescore")
async def rescore_existing_hotel(
    hotel_id: int,
    db: AsyncSession = Depends(get_db),
    _csrf=Depends(require_ajax),
):
    """Recompute lead_score + score_breakdown using the current row's
    brand_tier, zone, country, room_count, hotel_type values.

    Use cases:
      - User manually edits a scoring field in the Edit tab and wants
        the score to refresh without running Smart Fill.
      - Score weights changed (after re-deploy, the user can hit this
        endpoint instead of running the bulk script for a single row).

    No external calls, no Gemini, no Serper. Pure function. Cheap.
    """
    result = await db.execute(select(ExistingHotel).where(ExistingHotel.id == hotel_id))
    hotel = result.scalar_one_or_none()
    if not hotel:
        raise HTTPException(status_code=404, detail="Hotel not found")

    from app.services.existing_hotel_scorer import apply_score_to_hotel

    score, breakdown = apply_score_to_hotel(hotel)
    hotel.updated_at = local_now()
    await db.commit()

    return {
        "status": "rescored",
        "hotel_id": hotel_id,
        "lead_score": score,
        "score_breakdown": breakdown,
    }
