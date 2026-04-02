"""Lead CRUD endpoints — list, detail, create, update."""

import logging
from datetime import timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import get_db
from app.models import PotentialLead
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
        "revenue_high": PotentialLead.revenue_opening.desc().nullslast(),
        "revenue_low": PotentialLead.revenue_opening.asc().nullslast(),
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


# NOTE: All lead actions (approve, reject, restore, delete) are handled
# exclusively by /api/dashboard/leads/ routes in dashboard.py.
# The frontend uses those endpoints. Legacy /leads/{id}/approve etc. and
# /api/leads/{id}/approve have been removed to eliminate dangerous
# three-way route divergence (audit trail was missing from dashboard routes).
