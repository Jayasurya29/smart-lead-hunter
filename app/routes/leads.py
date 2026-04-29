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
    per_page: int = Query(20, ge=1, le=500),
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
    has_coords: Optional[str] = None,
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

    # has_coords filter — used by map to get only geocoded leads
    if has_coords == "true":
        query = query.where(
            PotentialLead.latitude.isnot(None),
            PotentialLead.longitude.isnot(None),
        )
        count_query = count_query.where(
            PotentialLead.latitude.isnot(None),
            PotentialLead.longitude.isnot(None),
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
    per_page: int = Query(20, ge=1, le=500),
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
    per_page: int = Query(20, ge=1, le=500),
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
    per_page: int = Query(20, ge=1, le=500),
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


# ══════════════════════════════════════════════════════════════════
# EXCEL EXPORT
# ══════════════════════════════════════════════════════════════════


@router.get("/leads/export", tags=["Leads"])
async def export_leads_excel(
    db: AsyncSession = Depends(get_db),
    status: Optional[str] = Query(
        None, description="Filter by status (new, approved, etc)"
    ),
    timeline: Optional[str] = Query(
        None, description="Filter by timeline label (HOT, URGENT, WARM, COOL)"
    ),
    tier: Optional[str] = Query(None, description="Filter by brand tier"),
    location: Optional[str] = Query(None, description="Filter by city/state"),
    search: Optional[str] = Query(None, description="Search hotel name/brand/city"),
    min_score: Optional[int] = Query(None, description="Minimum lead score"),
):
    """Export New Hotels (potential_leads) to a polished 2-sheet Excel workbook.

    Sheet 1 — Call List (sales outreach view, color-coded, autofiltered)
    Sheet 2 — Summary / Score Distribution (pivot-friendly aggregations)

    Filters mirror the Dashboard UI so an exported file matches what the
    user sees on screen.
    """
    import io
    from datetime import date
    from fastapi.responses import StreamingResponse
    from sqlalchemy import or_

    from app.models.lead_contact import LeadContact
    from app.services.excel_export import build_workbook

    # ── Build query ──
    q = select(PotentialLead)
    if status:
        statuses = [s.strip() for s in status.split(",")]
        q = q.where(PotentialLead.status.in_(statuses))
    else:
        q = q.where(PotentialLead.status.notin_(["rejected", "expired", "deleted"]))

    if timeline:
        labels = [t.strip().upper() for t in timeline.split(",")]
        q = q.where(PotentialLead.timeline_label.in_(labels))
    if tier:
        q = q.where(PotentialLead.brand_tier == tier)
    if min_score:
        q = q.where(PotentialLead.lead_score >= min_score)
    if location:
        loc_lower = f"%{location.lower()}%"
        q = q.where(
            or_(
                PotentialLead.city.ilike(loc_lower),
                PotentialLead.state.ilike(loc_lower),
                PotentialLead.country.ilike(loc_lower),
            )
        )
    if search:
        search_lower = f"%{search.lower()}%"
        q = q.where(
            or_(
                PotentialLead.hotel_name.ilike(search_lower),
                PotentialLead.brand.ilike(search_lower),
                PotentialLead.city.ilike(search_lower),
                PotentialLead.state.ilike(search_lower),
            )
        )

    q = q.order_by(PotentialLead.lead_score.desc().nullslast())
    result = await db.execute(q)
    leads = list(result.scalars().all())

    # ── Fetch all contacts grouped by lead ──
    primary_contacts: dict[int, LeadContact] = {}
    if leads:
        lead_ids = [lead.id for lead in leads]
        cq = await db.execute(
            select(LeadContact)
            .where(LeadContact.lead_id.in_(lead_ids))
            .order_by(LeadContact.score.desc().nullslast())
        )
        all_contacts = list(cq.scalars().all())
        contacts_by_lead: dict[int, list] = {}
        for c in all_contacts:
            contacts_by_lead.setdefault(c.lead_id, []).append(c)
        for lead in leads:
            cs = contacts_by_lead.get(lead.id, [])
            # Attach the full list for "Total Contacts" column
            setattr(lead, "_export_all_contacts", cs)
            # Pick primary: explicitly-flagged, else top-scored
            primary = next((c for c in cs if c.is_primary), None) or (
                cs[0] if cs else None
            )
            if primary:
                primary_contacts[lead.id] = primary

    # ── Determine the tab label for the title banner ──
    if status and "new" in status:
        tab_label = "Pipeline"
    elif status and "approved" in status:
        tab_label = "Approved"
    elif status and "rejected" in status:
        tab_label = "Rejected"
    else:
        tab_label = "All Active"

    # ── Build workbook ──
    xlsx_bytes = build_workbook(
        leads,
        primary_contacts,
        kind="new",
        tab_label=tab_label,
    )

    filename = (
        f"JA_NewHotels_{tab_label.replace(' ', '')}_" f"{date.today().isoformat()}.xlsx"
    )
    return StreamingResponse(
        io.BytesIO(xlsx_bytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


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

    # Auto-recalculate revenue if any revenue-relevant field was changed
    revenue_fields = {
        "room_count",
        "brand_tier",
        "brand",
        "hotel_type",
        "city",
        "state",
        "country",
    }
    if update_data.keys() & revenue_fields:
        try:
            from app.services.revenue_updater import update_lead_revenue

            await update_lead_revenue(lead_id)
            logger.info(f"Revenue recalculated for lead {lead_id} after field update")
        except Exception as e:
            logger.warning(f"Revenue recalc failed for lead {lead_id}: {e}")

    return LeadResponse.model_validate(lead)


# NOTE: All lead actions (approve, reject, restore, delete) are handled
# exclusively by /api/dashboard/leads/ routes in dashboard.py.
# The frontend uses those endpoints. Legacy /leads/{id}/approve etc. and
# /api/leads/{id}/approve have been removed to eliminate dangerous
# three-way route divergence (audit trail was missing from dashboard routes).


# ══════════════════════════════════════════════════════════════════
# GEO ENRICHMENT ROUTES
# ══════════════════════════════════════════════════════════════════


@router.post("/leads/{lead_id}/enrich-geo", tags=["Leads"])
async def enrich_lead_geo_route(
    lead_id: int,
    db: AsyncSession = Depends(get_db),
):
    """
    Trigger website discovery + geocoding for a single lead.
    Updates hotel_website, latitude, longitude in place.
    """
    from app.services.lead_geo_enrichment import enrich_lead_geo
    from sqlalchemy import update as sql_update

    result = await db.execute(select(PotentialLead).where(PotentialLead.id == lead_id))
    lead = result.scalar_one_or_none()
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")

    geo = await enrich_lead_geo(
        hotel_name=lead.hotel_name,
        city=lead.city,
        state=lead.state,
        country=lead.country,
        brand=lead.brand,
        existing_website=lead.hotel_website,
        address=lead.address,
        zip_code=lead.zip_code,
    )
    await db.execute(
        sql_update(PotentialLead)
        .where(PotentialLead.id == lead_id)
        .values(
            latitude=geo.get("latitude") or lead.latitude,
            longitude=geo.get("longitude") or lead.longitude,
            hotel_website=geo.get("hotel_website") or lead.hotel_website,
            website_verified=geo.get("website_verified") or lead.website_verified,
        )
    )
    await db.commit()

    return {
        "id": lead_id,
        "hotel_name": lead.hotel_name,
        "latitude": geo.get("latitude"),
        "longitude": geo.get("longitude"),
        "hotel_website": geo.get("hotel_website"),
        "website_verified": geo.get("website_verified"),
    }


@router.post("/leads/bulk-enrich-geo", tags=["Leads"])
async def bulk_enrich_geo(
    db: AsyncSession = Depends(get_db),
    limit: int = Query(50, description="Max leads to process"),
    force: bool = Query(False, description="Re-enrich leads that already have coords"),
):
    """
    Backfill geocoords + websites for all existing leads that are missing them.
    Processes up to `limit` leads per call. Run multiple times to backfill all.
    """
    from app.services.lead_geo_enrichment import enrich_lead_geo
    from sqlalchemy import update as sql_update, or_

    # Find leads missing coords (or force re-enrich all)
    query = select(PotentialLead).where(
        PotentialLead.status.notin_(["rejected", "expired"])
    )
    if not force:
        query = query.where(
            or_(
                PotentialLead.latitude.is_(None),
                PotentialLead.hotel_website.is_(None),
            )
        )
    query = query.order_by(PotentialLead.lead_score.desc()).limit(limit)

    result = await db.execute(query)
    leads = result.scalars().all()

    updated = 0
    failed = 0

    for lead in leads:
        try:
            geo = await enrich_lead_geo(
                hotel_name=lead.hotel_name,
                city=lead.city,
                state=lead.state,
                country=lead.country,
                brand=lead.brand,
                existing_website=lead.hotel_website,
            )
            try:
                await db.execute(
                    sql_update(PotentialLead)
                    .where(PotentialLead.id == lead.id)
                    .values(
                        latitude=geo.get("latitude") or lead.latitude,
                        longitude=geo.get("longitude") or lead.longitude,
                        hotel_website=geo.get("hotel_website") or lead.hotel_website,
                        website_verified=geo.get("website_verified")
                        or lead.website_verified,
                    )
                )
                await db.commit()
                updated += 1
                logger.info(f"Geo enriched [{updated}/{len(leads)}]: {lead.hotel_name}")
            except Exception as db_err:
                await db.rollback()  # Reset transaction so next lead can proceed
                failed += 1
                logger.warning(
                    f"Geo enrichment DB save failed for {lead.hotel_name}: {db_err}"
                )
        except Exception as e:
            await db.rollback()
            failed += 1
            logger.warning(f"Geo enrichment failed for {lead.hotel_name}: {e}")

    return {
        "processed": len(leads),
        "updated": updated,
        "failed": failed,
        "message": "Run again to process more leads"
        if len(leads) == limit
        else "All leads processed",
    }


# ══════════════════════════════════════════════════════════════════
# EXCEL EXPORT
# ══════════════════════════════════════════════════════════════════
