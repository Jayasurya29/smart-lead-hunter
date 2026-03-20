"""Lead CRUD endpoints + JSON API actions with full CRM logic."""

import logging
import os
from datetime import timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import get_db
from app.models import PotentialLead
from app.models.lead_contact import LeadContact
from app.schemas import (
    LeadCreate,
    LeadUpdate,
    LeadResponse,
    LeadListResponse,
)
from app.services.lead_factory import save_lead_to_db
from app.services.utils import local_now
from app.services.audit import log_action
from app.shared import (
    apply_lead_filters,
    paginate_leads,
    lead_list_response,
    escape_like,
)

logger = logging.getLogger(__name__)

router = APIRouter()


# A-01: Lightweight user email extractor from JWT cookie (no DB lookup)
def _get_user_email(request: Request) -> str:
    """Extract user email from JWT cookie for audit logging."""
    cookie = request.cookies.get("slh_session", "")
    if not cookie:
        return "unknown"
    try:
        from jose import jwt as jose_jwt

        secret = (
            os.getenv("JWT_SECRET_KEY", "")
            or "dev-only-insecure-key-do-not-use-in-production"
        )
        payload = jose_jwt.decode(cookie, secret, algorithms=["HS256"])
        return payload.get("email", "unknown")
    except Exception:
        return "unknown"


# -----------------------------------------------------------------------------
# Lead List (full filtering for React frontend)
# -----------------------------------------------------------------------------


@router.get("/leads", response_model=LeadListResponse, tags=["Leads"])
async def list_leads(
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
    status: Optional[str] = None,
    min_score: Optional[int] = None,
    state: Optional[str] = None,
    location_type: Optional[str] = None,
    brand_tier: Optional[str] = None,
    search: Optional[str] = None,
    timeline: Optional[str] = None,
    year: Optional[str] = None,
    added: Optional[str] = None,
    sort: Optional[str] = "score_desc",
    location: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
):
    """List leads with full filtering, pagination, and sorting."""
    query = select(PotentialLead)
    count_query = select(func.count(PotentialLead.id))

    query, count_query = apply_lead_filters(
        query,
        count_query,
        status=status,
        min_score=min_score,
        state=state,
        location_type=location_type,
        brand_tier=brand_tier,
        search=search,
    )

    now = local_now()
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)

    # Timeline filter (uses precomputed column)
    if timeline and timeline in (
        "hot",
        "urgent",
        "warm",
        "cool",
        "late",
        "expired",
        "tbd",
    ):
        label = timeline.upper()
        query = query.where(PotentialLead.timeline_label == label)
        count_query = count_query.where(PotentialLead.timeline_label == label)

    # Year filter
    if year:
        safe_year = escape_like(year)
        query = query.where(PotentialLead.opening_date.ilike(f"%{safe_year}%"))
        count_query = count_query.where(
            PotentialLead.opening_date.ilike(f"%{safe_year}%")
        )

    # Added date filter
    if added:
        if added == "today":
            cutoff = today_start
        elif added == "this_week":
            cutoff = today_start - timedelta(days=now.weekday())
        elif added == "last_7":
            cutoff = today_start - timedelta(days=7)
        elif added == "last_30":
            cutoff = today_start - timedelta(days=30)
        else:
            cutoff = None
        if cutoff:
            query = query.where(PotentialLead.created_at >= cutoff)
            count_query = count_query.where(PotentialLead.created_at >= cutoff)

    # Location filter
    if location:
        from app.config.locations import (
            SOUTH_FLORIDA_CITIES,
            CARIBBEAN_COUNTRIES,
            SOUTHEAST_STATES,
            MOUNTAIN_STATES,
            NORTHEAST_STATES,
            DC_NAMES,
            MIDWEST_STATES,
            PACIFIC_NW_STATES,
            LAS_VEGAS_CITIES,
            NEW_ORLEANS_CITIES,
            HAWAII_CITIES,
        )

        loc_filter = None
        if location == "south_florida":
            loc_filter = func.lower(PotentialLead.city).in_(SOUTH_FLORIDA_CITIES)
        elif location == "rest_florida":
            loc_filter = (func.lower(PotentialLead.state) == "florida") & ~func.lower(
                PotentialLead.city
            ).in_(SOUTH_FLORIDA_CITIES)
        elif location == "caribbean":
            loc_filter = func.lower(PotentialLead.country).in_(CARIBBEAN_COUNTRIES)
        elif location == "california":
            loc_filter = func.lower(PotentialLead.state) == "california"
        elif location == "new_york":
            loc_filter = func.lower(PotentialLead.state) == "new york"
        elif location == "texas":
            loc_filter = func.lower(PotentialLead.state) == "texas"
        elif location == "southeast":
            loc_filter = func.lower(PotentialLead.state).in_(SOUTHEAST_STATES)
        elif location == "mountain":
            loc_filter = func.lower(PotentialLead.state).in_(MOUNTAIN_STATES)
        elif location == "northeast":
            loc_filter = func.lower(PotentialLead.state).in_(NORTHEAST_STATES)
        elif location == "dc":
            loc_filter = func.lower(PotentialLead.city).in_(DC_NAMES) | func.lower(
                PotentialLead.state
            ).in_(DC_NAMES)
        elif location == "midwest":
            loc_filter = func.lower(PotentialLead.state).in_(MIDWEST_STATES)
        elif location == "pacific_nw":
            loc_filter = func.lower(PotentialLead.state).in_(PACIFIC_NW_STATES)
        elif location == "las_vegas":
            loc_filter = func.lower(PotentialLead.city).in_(LAS_VEGAS_CITIES)
        elif location == "new_orleans":
            loc_filter = func.lower(PotentialLead.city).in_(NEW_ORLEANS_CITIES)
        elif location == "hawaii":
            loc_filter = (func.lower(PotentialLead.state) == "hawaii") | func.lower(
                PotentialLead.city
            ).in_(HAWAII_CITIES)

        if loc_filter is not None:
            query = query.where(loc_filter)
            count_query = count_query.where(loc_filter)

    # Sort — supports both old keys and new frontend keys
    sort_map = {
        # Original keys
        "newest": PotentialLead.created_at.desc().nullslast(),
        "oldest": PotentialLead.created_at.asc().nullslast(),
        "score_desc": PotentialLead.lead_score.desc().nullslast(),
        "score_asc": PotentialLead.lead_score.asc().nullslast(),
        "name_asc": PotentialLead.hotel_name.asc().nullslast(),
        "opening": PotentialLead.opening_date.asc().nullslast(),
        # New keys from React frontend
        "name_desc": PotentialLead.hotel_name.desc().nullslast(),
        "opening_asc": PotentialLead.opening_date.asc().nullslast(),
        "opening_desc": PotentialLead.opening_date.desc().nullslast(),
        "tier_asc": PotentialLead.brand_tier.asc().nullslast(),
        "tier_desc": PotentialLead.brand_tier.desc().nullslast(),
        "time_asc": PotentialLead.timeline_label.asc().nullslast(),
        "time_desc": PotentialLead.timeline_label.desc().nullslast(),
        "location_asc": PotentialLead.city.asc().nullslast(),
        "location_desc": PotentialLead.city.desc().nullslast(),
    }
    order_by = sort_map.get(
        sort or "score_desc", PotentialLead.lead_score.desc().nullslast()
    )

    leads, total, pages = await paginate_leads(
        db, query, count_query, page, per_page, order_by=order_by
    )
    return lead_list_response(leads, total, page, per_page, pages)


# -----------------------------------------------------------------------------
# Shortcut Lead Lists
# -----------------------------------------------------------------------------


@router.get("/leads/hot", response_model=LeadListResponse, tags=["Leads"])
async def get_hot_leads(
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    """Get hot leads (score >= config threshold)"""
    where = [
        PotentialLead.lead_score >= settings.hot_lead_threshold,
        PotentialLead.status == "new",
    ]
    query = select(PotentialLead).where(*where)
    count_query = select(func.count(PotentialLead.id)).where(*where)
    leads, total, pages = await paginate_leads(
        db, query, count_query, page, per_page, order_by=PotentialLead.lead_score.desc()
    )
    return lead_list_response(leads, total, page, per_page, pages)


@router.get("/leads/florida", response_model=LeadListResponse, tags=["Leads"])
async def get_florida_leads(
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    """Get Florida leads"""
    query = select(PotentialLead).where(PotentialLead.location_type == "florida")
    count_query = select(func.count(PotentialLead.id)).where(
        PotentialLead.location_type == "florida"
    )
    leads, total, pages = await paginate_leads(
        db,
        query,
        count_query,
        page,
        per_page,
        order_by=PotentialLead.lead_score.desc().nullslast(),
    )
    return lead_list_response(leads, total, page, per_page, pages)


@router.get("/leads/caribbean", response_model=LeadListResponse, tags=["Leads"])
async def get_caribbean_leads(
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    """Get Caribbean leads"""
    query = select(PotentialLead).where(PotentialLead.location_type == "caribbean")
    count_query = select(func.count(PotentialLead.id)).where(
        PotentialLead.location_type == "caribbean"
    )
    leads, total, pages = await paginate_leads(
        db,
        query,
        count_query,
        page,
        per_page,
        order_by=PotentialLead.lead_score.desc().nullslast(),
    )
    return lead_list_response(leads, total, page, per_page, pages)


# -----------------------------------------------------------------------------
# Single Lead CRUD
# -----------------------------------------------------------------------------


@router.get("/leads/{lead_id}", response_model=LeadResponse, tags=["Leads"])
async def get_lead(lead_id: int, db: AsyncSession = Depends(get_db)):
    """Get a single lead by ID"""
    result = await db.execute(select(PotentialLead).where(PotentialLead.id == lead_id))
    lead = result.scalar_one_or_none()
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")
    return LeadResponse.model_validate(lead)


@router.post("/leads", response_model=LeadResponse, tags=["Leads"])
async def create_lead(lead_data: LeadCreate, db: AsyncSession = Depends(get_db)):
    """Create a new lead manually."""
    lead_dict = lead_data.model_dump()
    lead_dict["source_site"] = lead_dict.get("source_site") or "manual"

    result = await save_lead_to_db(lead_dict, db, commit=True)

    if result["status"] == "skipped":
        raise HTTPException(status_code=422, detail=result["reason"])
    if result["status"] in ("duplicate", "enriched"):
        raise HTTPException(
            status_code=409,
            detail=f"A lead with a similar name already exists (ID: {result['id']})",
        )

    lead = (
        await db.execute(select(PotentialLead).where(PotentialLead.id == result["id"]))
    ).scalar_one()
    logger.info(
        f"Created lead: {lead.hotel_name} (ID: {lead.id}, Score: {lead.lead_score})"
    )
    return LeadResponse.model_validate(lead)


@router.patch("/leads/{lead_id}", response_model=LeadResponse, tags=["Leads"])
async def update_lead(
    lead_id: int, updates: LeadUpdate, db: AsyncSession = Depends(get_db)
):
    """Update a lead"""
    result = await db.execute(select(PotentialLead).where(PotentialLead.id == lead_id))
    lead = result.scalar_one_or_none()
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")

    update_data = updates.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(lead, field, value)

    lead.updated_at = local_now()
    await db.commit()
    await db.refresh(lead)
    logger.info(f"Updated lead: {lead.hotel_name} (ID: {lead.id})")
    return LeadResponse.model_validate(lead)


@router.post("/leads/{lead_id}/approve", response_model=LeadResponse, tags=["Leads"])
async def approve_lead(lead_id: int, db: AsyncSession = Depends(get_db)):
    """Legacy endpoint — redirects to the full API version with CRM logic."""
    # FIX M-01: Old version called crm.push_lead() which doesn't exist.
    # Delegate to the proper API endpoint that checks contacts + pushes to Insightly.
    return await api_approve_lead(lead_id, db)


@router.post("/leads/{lead_id}/reject", response_model=LeadResponse, tags=["Leads"])
async def reject_lead(
    lead_id: int,
    reason: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    """Legacy endpoint — redirects to the full API version with CRM cleanup."""
    # FIX M-01: Old version didn't clean up Insightly leads on reject.
    return await api_reject_lead(lead_id, reason, db)


@router.delete("/leads/{lead_id}", tags=["Leads"])
async def delete_lead(lead_id: int, db: AsyncSession = Depends(get_db)):
    """Hard-delete a lead"""
    result = await db.execute(select(PotentialLead).where(PotentialLead.id == lead_id))
    lead = result.scalar_one_or_none()
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")

    hotel_name = lead.hotel_name
    await db.delete(lead)
    await db.commit()
    return {"message": f"Lead deleted: {hotel_name}", "id": lead_id}


# -----------------------------------------------------------------------------
# JSON API Lead Actions (React frontend — with full CRM logic)
# -----------------------------------------------------------------------------


@router.post(
    "/api/leads/{lead_id}/approve", response_model=LeadResponse, tags=["Leads"]
)
async def api_approve_lead(
    lead_id: int, request: Request = None, db: AsyncSession = Depends(get_db)
):
    """Approve — checks contacts, pushes to Insightly CRM, returns JSON."""
    user_email = _get_user_email(request) if request else "system"
    result = await db.execute(select(PotentialLead).where(PotentialLead.id == lead_id))
    lead = result.scalar_one_or_none()
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")

    contacts_result = await db.execute(
        select(LeadContact)
        .where(LeadContact.lead_id == lead_id)
        .order_by(LeadContact.score.desc())
    )
    contacts = [c.to_dict() for c in contacts_result.scalars().all()]
    if not contacts:
        raise HTTPException(
            status_code=422, detail="Enrich first — no contacts to push to CRM"
        )

    lead.status = "approved"
    lead.updated_at = local_now()

    from app.services.insightly import get_insightly_client

    crm = get_insightly_client()
    crm_error = None
    if crm.enabled and not lead.insightly_id:
        try:
            pushed = await crm.push_contacts_as_leads(
                contacts=contacts,
                hotel_name=lead.hotel_name,
                brand=lead.brand or "",
                brand_tier=lead.brand_tier or "",
                city=lead.city or "",
                state=lead.state or "",
                country=lead.country or "USA",
                opening_date=lead.opening_date or "",
                room_count=lead.room_count or 0,
                lead_score=lead.lead_score or 0,
                description=lead.description or "",
                source_url=lead.source_url or "",
                management_company=lead.management_company or "",
                developer=lead.developer or "",
                owner=lead.owner or "",
                slh_lead_id=lead.id,
            )
            successful = [p for p in pushed if p[1]]
            if successful:
                lead.insightly_id = successful[0][1]
                lead.sync_error = None
                logger.info(
                    f"Insightly: pushed {len(successful)} contacts for {lead.hotel_name}"
                )
            else:
                crm_error = "CRM push returned no successful records"
                lead.sync_error = crm_error
                logger.warning(f"Insightly: push returned empty for {lead.hotel_name}")
        except Exception as e:
            crm_error = f"CRM sync failed: {str(e)[:100]}"
            lead.sync_error = crm_error
            logger.error(f"Insightly: push failed for {lead.hotel_name}: {e}")

    # A-01: Audit log
    await log_action(
        session=db,
        action="approve",
        lead=lead,
        user_email=user_email,
        detail=f"Contacts: {len(contacts)}"
        + (f", CRM error: {crm_error}" if crm_error else ""),
    )

    await db.commit()
    await db.refresh(lead)

    response = LeadResponse.model_validate(lead)
    if crm_error:
        return JSONResponse(
            content={**response.model_dump(mode="json"), "crm_warning": crm_error},
            status_code=200,
        )
    return response


@router.post("/api/leads/{lead_id}/reject", response_model=LeadResponse, tags=["Leads"])
async def api_reject_lead(
    lead_id: int,
    reason: Optional[str] = Query(None),
    request: Request = None,
    db: AsyncSession = Depends(get_db),
):
    """Reject — cleans up Insightly, returns JSON."""
    user_email = _get_user_email(request) if request else "system"
    result = await db.execute(select(PotentialLead).where(PotentialLead.id == lead_id))
    lead = result.scalar_one_or_none()
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")

    old_status = lead.status
    lead.status = "rejected"
    lead.rejection_reason = reason
    lead.notes = f"{lead.notes or ''}\nRejected: {reason or 'No reason given'}".strip()
    lead.updated_at = local_now()

    if lead.insightly_id:
        from app.services.insightly import get_insightly_client

        crm = get_insightly_client()
        if crm.enabled:
            await crm.delete_leads_by_slh_id(lead.id)
        lead.insightly_id = None

    await log_action(
        session=db,
        action="reject",
        lead=lead,
        user_email=user_email,
        old_values={"status": old_status},
        new_values={"status": "rejected", "reason": reason},
        detail=reason,
    )

    await db.commit()
    await db.refresh(lead)
    return LeadResponse.model_validate(lead)


@router.post(
    "/api/leads/{lead_id}/restore", response_model=LeadResponse, tags=["Leads"]
)
async def api_restore_lead(
    lead_id: int, request: Request = None, db: AsyncSession = Depends(get_db)
):
    """Restore — cleans up Insightly, returns JSON."""
    user_email = _get_user_email(request) if request else "system"
    result = await db.execute(select(PotentialLead).where(PotentialLead.id == lead_id))
    lead = result.scalar_one_or_none()
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")

    old_status = lead.status
    lead.status = "new"
    lead.rejection_reason = None
    lead.updated_at = local_now()

    if lead.insightly_id:
        from app.services.insightly import get_insightly_client

        crm = get_insightly_client()
        if crm.enabled:
            await crm.delete_leads_by_slh_id(lead.id)
        lead.insightly_id = None

    await log_action(
        session=db,
        action="restore",
        lead=lead,
        user_email=user_email,
        old_values={"status": old_status},
        new_values={"status": "new"},
    )

    await db.commit()
    await db.refresh(lead)
    return LeadResponse.model_validate(lead)


@router.post("/api/leads/{lead_id}/delete", tags=["Leads"])
async def api_soft_delete_lead(
    lead_id: int, request: Request = None, db: AsyncSession = Depends(get_db)
):
    """Soft-delete — returns JSON."""
    user_email = _get_user_email(request) if request else "system"
    result = await db.execute(select(PotentialLead).where(PotentialLead.id == lead_id))
    lead = result.scalar_one_or_none()
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")

    await log_action(
        session=db,
        action="delete",
        lead=lead,
        user_email=user_email,
    )

    lead.status = "deleted"
    lead.updated_at = local_now()
    await db.commit()
    return {"status": "deleted", "id": lead_id}
