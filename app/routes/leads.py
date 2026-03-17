"""Lead CRUD endpoints + JSON API actions with full CRM logic."""

import logging
from datetime import timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
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
from app.shared import (
    apply_lead_filters,
    paginate_leads,
    lead_list_response,
    escape_like,
)

logger = logging.getLogger(__name__)

router = APIRouter()


# -----------------------------------------------------------------------------
# Lead List (full filtering for React + HTMX)
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
        south_fl_cities = [
            "miami",
            "miami beach",
            "fort lauderdale",
            "hallandale beach",
            "west palm beach",
            "palm beach",
            "boca raton",
            "hollywood",
            "deerfield beach",
            "delray beach",
            "aventura",
            "coral gables",
            "key west",
            "key biscayne",
            "sweetwater",
            "doral",
            "hialeah",
            "homestead",
            "sunny isles beach",
            "surfside",
            "bal harbour",
            "north miami",
            "north miami beach",
            "miami gardens",
            "miami lakes",
            "coconut grove",
            "pompano beach",
            "lauderdale by the sea",
            "plantation",
            "weston",
            "davie",
            "sunrise",
            "pembroke pines",
            "miramar",
            "cooper city",
            "boynton beach",
            "jupiter",
            "riviera beach",
            "lake worth",
            "naples",
            "bonita springs",
            "marco island",
            "fort myers",
            "cape coral",
            "sarasota",
            "clearwater",
            "st. petersburg",
            "st petersburg",
        ]
        caribbean_countries = [
            "dominican republic",
            "bahamas",
            "jamaica",
            "cayman islands",
            "barbados",
            "aruba",
            "turks & caicos islands",
            "turks and caicos",
            "saint lucia",
            "st. lucia",
            "curacao",
            "u.s. virgin islands",
            "antigua and barbuda",
            "trinidad and tobago",
            "puerto rico",
        ]
        southeast_states = [
            "georgia",
            "tennessee",
            "south carolina",
            "north carolina",
            "alabama",
            "mississippi",
            "arkansas",
            "virginia",
        ]
        mountain_states = [
            "utah",
            "wyoming",
            "idaho",
            "colorado",
            "montana",
            "arizona",
            "new mexico",
        ]

        loc_filter = None
        if location == "south_florida":
            loc_filter = func.lower(PotentialLead.city).in_(south_fl_cities)
        elif location == "rest_florida":
            loc_filter = (func.lower(PotentialLead.state) == "florida") & ~func.lower(
                PotentialLead.city
            ).in_(south_fl_cities)
        elif location == "caribbean":
            loc_filter = func.lower(PotentialLead.country).in_(caribbean_countries)
        elif location == "california":
            loc_filter = func.lower(PotentialLead.state) == "california"
        elif location == "new_york":
            loc_filter = func.lower(PotentialLead.state) == "new york"
        elif location == "texas":
            loc_filter = func.lower(PotentialLead.state) == "texas"
        elif location == "southeast":
            loc_filter = func.lower(PotentialLead.state).in_(southeast_states)
        elif location == "mountain":
            loc_filter = func.lower(PotentialLead.state).in_(mountain_states)

        if loc_filter is not None:
            query = query.where(loc_filter)
            count_query = count_query.where(loc_filter)

    # Sort
    sort_map = {
        "newest": PotentialLead.created_at.desc().nullslast(),
        "oldest": PotentialLead.created_at.asc().nullslast(),
        "score_desc": PotentialLead.lead_score.desc().nullslast(),
        "score_asc": PotentialLead.lead_score.asc().nullslast(),
        "name_asc": PotentialLead.hotel_name.asc(),
        "opening": PotentialLead.opening_date.asc().nullslast(),
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
    """Approve a lead (simple REST — single contact push)."""
    result = await db.execute(select(PotentialLead).where(PotentialLead.id == lead_id))
    lead = result.scalar_one_or_none()
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")

    lead.status = "approved"
    lead.updated_at = local_now()

    from app.services.insightly import get_insightly_client

    crm = get_insightly_client()
    if crm.enabled and not lead.insightly_id:
        lead_data = {
            "hotel_name": lead.hotel_name,
            "brand": lead.brand,
            "brand_tier": lead.brand_tier,
            "city": lead.city,
            "state": lead.state,
            "country": lead.country or "USA",
            "opening_date": lead.opening_date,
            "room_count": lead.room_count or 0,
            "lead_score": lead.lead_score or 0,
            "description": lead.description,
            "source_url": lead.source_url,
            "management_company": lead.management_company,
            "developer": lead.developer,
            "owner": lead.owner,
            "status": "approved",
            "id": lead.id,
        }
        crm_result = await crm.push_lead(lead_data)
        if crm_result:
            lead.insightly_id = crm_result.get("RECORD_ID")

    await db.commit()
    await db.refresh(lead)
    return LeadResponse.model_validate(lead)


@router.post("/leads/{lead_id}/reject", response_model=LeadResponse, tags=["Leads"])
async def reject_lead(
    lead_id: int,
    reason: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    """Reject a lead"""
    result = await db.execute(select(PotentialLead).where(PotentialLead.id == lead_id))
    lead = result.scalar_one_or_none()
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")

    lead.status = "rejected"
    lead.rejection_reason = reason
    lead.notes = f"{lead.notes or ''}\nRejected: {reason or 'No reason given'}".strip()
    lead.updated_at = local_now()
    await db.commit()
    await db.refresh(lead)
    return LeadResponse.model_validate(lead)


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
async def api_approve_lead(lead_id: int, db: AsyncSession = Depends(get_db)):
    """Approve — checks contacts, pushes to Insightly CRM, returns JSON."""
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
    if crm.enabled and not lead.insightly_id:
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
            logger.info(
                f"Insightly: pushed {len(successful)} contacts for {lead.hotel_name}"
            )

    await db.commit()
    await db.refresh(lead)
    return LeadResponse.model_validate(lead)


@router.post("/api/leads/{lead_id}/reject", response_model=LeadResponse, tags=["Leads"])
async def api_reject_lead(
    lead_id: int,
    reason: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    """Reject — cleans up Insightly, returns JSON."""
    result = await db.execute(select(PotentialLead).where(PotentialLead.id == lead_id))
    lead = result.scalar_one_or_none()
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")

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

    await db.commit()
    await db.refresh(lead)
    return LeadResponse.model_validate(lead)


@router.post(
    "/api/leads/{lead_id}/restore", response_model=LeadResponse, tags=["Leads"]
)
async def api_restore_lead(lead_id: int, db: AsyncSession = Depends(get_db)):
    """Restore — cleans up Insightly, returns JSON."""
    result = await db.execute(select(PotentialLead).where(PotentialLead.id == lead_id))
    lead = result.scalar_one_or_none()
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")

    lead.status = "new"
    lead.rejection_reason = None
    lead.updated_at = local_now()

    if lead.insightly_id:
        from app.services.insightly import get_insightly_client

        crm = get_insightly_client()
        if crm.enabled:
            await crm.delete_leads_by_slh_id(lead.id)
        lead.insightly_id = None

    await db.commit()
    await db.refresh(lead)
    return LeadResponse.model_validate(lead)


@router.post("/api/leads/{lead_id}/delete", tags=["Leads"])
async def api_soft_delete_lead(lead_id: int, db: AsyncSession = Depends(get_db)):
    """Soft-delete — returns JSON."""
    result = await db.execute(select(PotentialLead).where(PotentialLead.id == lead_id))
    lead = result.scalar_one_or_none()
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")

    lead.status = "deleted"
    lead.updated_at = local_now()
    await db.commit()
    return {"status": "deleted", "id": lead_id}
