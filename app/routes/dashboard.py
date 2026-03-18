"""HTMX Dashboard page, partials, and lead actions."""

import logging
import os
from datetime import timedelta
from typing import Optional

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from sqlalchemy import select, func, case, or_
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models import PotentialLead, Source
from app.models.lead_contact import LeadContact
from app.services.rescore import rescore_lead
from app.services.utils import local_now, normalize_hotel_name
from app.shared import (
    templates,
    escape_like,
    require_ajax,
    checked_json,
    apply_lead_filters,
    paginate_leads,
    get_dashboard_stats,
)

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/dashboard", response_class=HTMLResponse, tags=["Dashboard"])
async def dashboard_page(
    request: Request,
    tab: str = "pipeline",
    page: int = 1,
    search: str = "",
    score: str = "",
    location: str = "",
    tier: str = "",
    sort: str = "score_desc",
    added: str = "",
    db: AsyncSession = Depends(get_db),
):
    """Dashboard page with Pipeline/Approved/Rejected tabs"""
    # Map tab to status
    tab_status_map = {
        "pipeline": "new",
        "approved": "approved",
        "deleted": "deleted",
        "rejected": "rejected",
    }
    status = tab_status_map.get(tab, "new")

    # Base query
    query = select(PotentialLead).where(PotentialLead.status == status)
    now = local_now()
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)

    # Filters (Audit Fix #6: escape LIKE wildcards in search input)
    if search:
        safe_search = escape_like(search)
        search_term = f"%{safe_search}%"
        query = query.where(
            or_(
                PotentialLead.hotel_name.ilike(search_term),
                PotentialLead.city.ilike(search_term),
                PotentialLead.brand.ilike(search_term),
                PotentialLead.state.ilike(search_term),
            )
        )
    if score in ("hot", "urgent", "warm", "cool", "late", "expired", "tbd"):
        query = query.where(PotentialLead.timeline_label == score.upper())

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

        if location == "south_florida":
            query = query.where(func.lower(PotentialLead.city).in_(south_fl_cities))
        elif location == "rest_florida":
            query = query.where(
                func.lower(PotentialLead.state) == "florida",
                ~func.lower(PotentialLead.city).in_(south_fl_cities),
            )
        elif location == "caribbean":
            query = query.where(
                func.lower(PotentialLead.country).in_(caribbean_countries)
            )
        elif location == "california":
            query = query.where(func.lower(PotentialLead.state) == "california")
        elif location == "new_york":
            query = query.where(func.lower(PotentialLead.state) == "new york")
        elif location == "texas":
            query = query.where(func.lower(PotentialLead.state) == "texas")
        elif location == "southeast":
            query = query.where(func.lower(PotentialLead.state).in_(southeast_states))
        elif location == "mountain":
            query = query.where(func.lower(PotentialLead.state).in_(mountain_states))
    if added:
        if added == "this_week":
            week_start = today_start - timedelta(days=now.weekday())
            query = query.where(PotentialLead.created_at >= week_start)
        elif added == "last_7":
            query = query.where(
                PotentialLead.created_at >= today_start - timedelta(days=7)
            )
        elif added == "last_30":
            query = query.where(
                PotentialLead.created_at >= today_start - timedelta(days=30)
            )
    if tier:
        query = query.where(PotentialLead.brand_tier == tier)

    # Order — support sort parameter
    if sort == "newest":
        query = query.order_by(PotentialLead.created_at.desc().nullslast())
    elif sort == "oldest":
        query = query.order_by(PotentialLead.created_at.asc().nullslast())
    elif sort == "score_asc":
        query = query.order_by(PotentialLead.lead_score.asc().nullslast())
    elif sort == "name_asc":
        query = query.order_by(PotentialLead.hotel_name.asc())
    elif sort == "opening":
        query = query.order_by(PotentialLead.opening_date.asc().nullslast())
    else:
        query = query.order_by(PotentialLead.lead_score.desc().nullslast())

    # Pagination
    per_page = 25
    offset = (page - 1) * per_page

    # Get total count for pagination
    count_query = select(func.count()).select_from(query.subquery())
    total_result = await db.execute(count_query)
    total_count = total_result.scalar() or 0
    total_pages = max(1, (total_count + per_page - 1) // per_page)

    # Get leads
    result = await db.execute(query.offset(offset).limit(per_page))
    leads = result.scalars().all()

    # Audit Fix M-01 + P-04: Single query, portable case() syntax
    _tab_counts_r = await db.execute(
        select(
            func.sum(case((PotentialLead.status == "new", 1), else_=0)).label("new"),
            func.sum(case((PotentialLead.status == "approved", 1), else_=0)).label(
                "approved"
            ),
            func.sum(case((PotentialLead.status == "rejected", 1), else_=0)).label(
                "rejected"
            ),
            func.sum(case((PotentialLead.status == "deleted", 1), else_=0)).label(
                "deleted"
            ),
        )
    )
    _tc = _tab_counts_r.one()

    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "leads": leads,
            "active_tab": tab,
            "current_page": page,
            "total_pages": total_pages,
            "pipeline_count": _tc.new or 0,
            "approved_count": _tc.approved or 0,
            "rejected_count": _tc.rejected or 0,
            "deleted_count": _tc.deleted or 0,
            "total_count": total_count,
            "api_auth_key": os.getenv("API_AUTH_KEY", ""),
        },
    )


@router.get("/api/dashboard/stats", response_class=HTMLResponse, tags=["Dashboard"])
async def dashboard_stats_partial(request: Request, db: AsyncSession = Depends(get_db)):
    """HTMX partial: Stats cards"""
    stats = await get_dashboard_stats(db)

    return templates.TemplateResponse(request, "partials/stats.html", {"stats": stats})


@router.get("/api/dashboard/leads", response_class=HTMLResponse, tags=["Dashboard"])
async def dashboard_leads_partial(
    request: Request,
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
    search: Optional[str] = None,
    status: Optional[str] = None,
    min_score: Optional[int] = None,
    location_type: Optional[str] = None,
    brand_tier: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
):
    """HTMX partial: Lead list with filtering and pagination"""
    query = select(PotentialLead)
    count_query = select(func.count(PotentialLead.id))

    query, count_query = apply_lead_filters(
        query,
        count_query,
        status=status,
        min_score=min_score,
        location_type=location_type,
        brand_tier=brand_tier,
        search=search,
    )

    leads, total, pages = await paginate_leads(db, query, count_query, page, per_page)

    return templates.TemplateResponse(
        request,
        "partials/lead_list.html",
        {
            "leads": leads,
            "pagination": {
                "total": total,
                "page": page,
                "per_page": per_page,
                "pages": pages,
            },
        },
    )


@router.get(
    "/api/dashboard/leads/{lead_id}", response_class=HTMLResponse, tags=["Dashboard"]
)
async def dashboard_lead_detail_partial(
    request: Request, lead_id: int, db: AsyncSession = Depends(get_db)
):
    """HTMX partial: Lead detail panel"""
    result = await db.execute(select(PotentialLead).where(PotentialLead.id == lead_id))
    lead = result.scalar_one_or_none()

    if not lead:
        return HTMLResponse(
            content='<div class="p-6 text-center text-red-500">Lead not found</div>',
            status_code=404,
        )

    return templates.TemplateResponse(
        request, "partials/lead_detail.html", {"lead": lead}
    )


@router.get(
    "/api/dashboard/leads/{lead_id}/row",
    response_class=HTMLResponse,
    tags=["Dashboard"],
)
async def dashboard_lead_row_partial(
    request: Request, lead_id: int, db: AsyncSession = Depends(get_db)
):
    """HTMX partial: Return single lead row (for refresh after edit)"""
    result = await db.execute(select(PotentialLead).where(PotentialLead.id == lead_id))
    lead = result.scalar_one_or_none()

    if not lead:
        return HTMLResponse(content="", status_code=404)

    return templates.TemplateResponse(request, "partials/lead_row.html", {"lead": lead})


@router.patch("/api/dashboard/leads/{lead_id}/edit", tags=["Dashboard"])
async def dashboard_edit_lead(
    lead_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    _csrf=Depends(require_ajax),
):
    """Edit lead fields from the detail panel"""
    data = await checked_json(request)

    # ── Input validation ──
    import re as _re

    _email_re = _re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
    _valid_tiers = {
        "tier1_ultra_luxury",
        "tier2_luxury",
        "tier3_upper_upscale",
        "tier4_upscale",
        "tier5_skip",
        "unknown",
        "",
    }
    errors = []

    if "hotel_name" in data:
        name = str(data["hotel_name"]).strip() if data["hotel_name"] else ""
        if not name:
            errors.append("Hotel name cannot be empty")
        elif len(name) > 255:
            errors.append("Hotel name must be 255 characters or fewer")

    if "contact_email" in data and data["contact_email"]:
        email = str(data["contact_email"]).strip()
        if email and not _email_re.match(email):
            errors.append(f"Invalid email format: {email}")

    if "room_count" in data and data["room_count"] is not None:
        try:
            rc = int(data["room_count"])
            if rc < 0:
                errors.append("Room count cannot be negative")
        except (ValueError, TypeError):
            errors.append("Room count must be a number")

    if "brand_tier" in data and data["brand_tier"]:
        if str(data["brand_tier"]).strip() not in _valid_tiers:
            errors.append(f"Invalid brand tier: {data['brand_tier']}")

    # Cap string field lengths
    for field, max_len in [
        ("city", 100),
        ("state", 100),
        ("country", 100),
        ("brand", 100),
        ("contact_name", 200),
        ("contact_title", 100),
        ("contact_phone", 50),
        ("management_company", 200),
        ("developer", 200),
        ("owner", 200),
        ("opening_date", 50),
    ]:
        if field in data and data[field] and len(str(data[field])) > max_len:
            errors.append(f"{field} must be {max_len} characters or fewer")

    for field, max_len in [("description", 5000), ("notes", 5000)]:
        if field in data and data[field] and len(str(data[field])) > max_len:
            errors.append(f"{field} must be {max_len} characters or fewer")

    if errors:
        return JSONResponse(content={"detail": "; ".join(errors)}, status_code=422)

    result = await db.execute(select(PotentialLead).where(PotentialLead.id == lead_id))
    lead = result.scalar_one_or_none()

    if not lead:
        return JSONResponse(content={"detail": "Lead not found"}, status_code=404)

    # Editable fields whitelist
    editable_fields = [
        "hotel_name",
        "brand",
        "brand_tier",
        "hotel_type",
        "city",
        "state",
        "country",
        "opening_date",
        "room_count",
        "management_company",
        "developer",
        "owner",
        "contact_name",
        "contact_title",
        "contact_email",
        "contact_phone",
        "description",
        "notes",
    ]

    for field in editable_fields:
        if field in data:
            value = data[field]
            # Convert empty strings to None
            if value == "" or value is None:
                setattr(lead, field, None)
            elif field == "room_count":
                try:
                    setattr(lead, field, int(value) if value else None)
                except (ValueError, TypeError):
                    pass  # Skip invalid room_count
            else:
                setattr(lead, field, str(value))

    # Audit Fix: Keep normalized name in sync when hotel_name changes
    if "hotel_name" in data and data["hotel_name"]:
        lead.hotel_name_normalized = normalize_hotel_name(data["hotel_name"])

    # Keep timeline_label in sync when opening_date changes
    if "opening_date" in data:
        from app.services.utils import get_timeline_label

        lead.timeline_label = get_timeline_label(data["opening_date"] or "")

    # Audit Fix 5b: Wrap room_count safely
    # Rescore lead after edits
    tier_points_map = {
        "tier1_ultra_luxury": 25,
        "tier2_luxury": 20,
        "tier3_upper_upscale": 15,
        "tier4_upscale": 10,
        "tier5_skip": 0,
        "unknown": 0,
    }
    scoring_fields = {
        "hotel_name",
        "brand",
        "city",
        "state",
        "country",
        "opening_date",
        "room_count",
        "description",
    }
    scoring_changed = any(f in data for f in scoring_fields)

    if scoring_changed:
        # Full rescore with enriched contacts
        await db.flush()
        await rescore_lead(lead.id, db)
        if "brand_tier" in data and data["brand_tier"]:
            auto_points = (lead.score_breakdown or {}).get("brand", {}).get("points", 0)
            manual_points = tier_points_map.get(data["brand_tier"], 0)
            lead.lead_score = lead.lead_score - auto_points + manual_points
            lead.brand_tier = data["brand_tier"]
    elif "brand_tier" in data and data["brand_tier"]:
        old_points = tier_points_map.get(lead.brand_tier or "unknown", 0)
        new_points = tier_points_map.get(data["brand_tier"], 0)
        lead.lead_score = (lead.lead_score or 0) - old_points + new_points
        lead.brand_tier = data["brand_tier"]

    lead.updated_at = local_now()
    await db.commit()
    await db.refresh(lead)
    return JSONResponse(
        content={
            "status": "ok",
            "id": lead.id,
            "new_score": lead.lead_score,
            "new_tier": lead.brand_tier,
        }
    )


@router.post(
    "/api/dashboard/leads/{lead_id}/approve",
    response_class=HTMLResponse,
    tags=["Dashboard"],
)
async def dashboard_approve_lead(
    request: Request,
    lead_id: int,
    db: AsyncSession = Depends(get_db),
    _csrf=Depends(require_ajax),
):
    """HTMX: Approve lead and return updated row"""
    result = await db.execute(select(PotentialLead).where(PotentialLead.id == lead_id))
    lead = result.scalar_one_or_none()

    if not lead:
        return HTMLResponse(content="Lead not found", status_code=404)

    # Block approve if no contacts — must enrich first

    contacts_result = await db.execute(
        select(LeadContact)
        .where(LeadContact.lead_id == lead_id)
        .order_by(LeadContact.score.desc())
    )
    contacts = [c.to_dict() for c in contacts_result.scalars().all()]
    if not contacts:
        return HTMLResponse(
            content='<div class="text-red-600 text-sm font-medium p-2">Enrich first — no contacts to push to CRM</div>',
            status_code=200,
        )

    lead.status = "approved"
    lead.updated_at = local_now()

    # Push contacts as Insightly Leads
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
            lead.insightly_id = successful[0][1]  # Store first Lead ID as reference
            logger.info(
                f"Insightly: pushed {len(successful)} contacts for "
                f"{lead.hotel_name} → Lead IDs: {[p[1] for p in successful]}"
            )
        else:
            logger.warning(f"Insightly: failed to push contacts for {lead.hotel_name}")

    await db.commit()
    await db.refresh(lead)

    logger.info(f"Dashboard: Approved lead {lead.hotel_name} (ID: {lead.id})")

    return templates.TemplateResponse(request, "partials/lead_row.html", {"lead": lead})


@router.post(
    "/api/dashboard/leads/{lead_id}/reject",
    response_class=HTMLResponse,
    tags=["Dashboard"],
)
async def dashboard_reject_lead(
    request: Request,
    lead_id: int,
    reason: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
    _csrf=Depends(require_ajax),
):
    """HTMX: Reject lead and return updated row"""
    result = await db.execute(select(PotentialLead).where(PotentialLead.id == lead_id))
    lead = result.scalar_one_or_none()

    if not lead:
        return HTMLResponse(content="Lead not found", status_code=404)

    lead.status = "rejected"
    lead.rejection_reason = reason
    lead.notes = f"{lead.notes or ''}\nRejected: {reason or 'No reason given'}".strip()
    lead.updated_at = local_now()

    # Remove from Insightly if previously pushed
    if lead.insightly_id:
        from app.services.insightly import get_insightly_client

        crm = get_insightly_client()
        if crm.enabled:
            deleted = await crm.delete_leads_by_slh_id(lead.id)
            logger.info(f"Insightly: deleted {deleted} leads for {lead.hotel_name}")
        lead.insightly_id = None

    await db.commit()
    await db.refresh(lead)

    logger.info(
        f"Dashboard: Rejected lead {lead.hotel_name} (ID: {lead.id}, Reason: {reason})"
    )

    return templates.TemplateResponse(request, "partials/lead_row.html", {"lead": lead})


@router.post(
    "/api/dashboard/leads/{lead_id}/restore",
    response_class=HTMLResponse,
    tags=["Dashboard"],
)
async def dashboard_restore_lead(
    request: Request,
    lead_id: int,
    db: AsyncSession = Depends(get_db),
    _csrf=Depends(require_ajax),
):
    result = await db.execute(select(PotentialLead).where(PotentialLead.id == lead_id))
    lead = result.scalar_one_or_none()
    if not lead:
        return HTMLResponse(
            content="<div class='text-red-500 p-2'>Lead not found</div>",
            status_code=404,
        )
    lead.status = "new"
    lead.rejection_reason = None
    lead.updated_at = local_now()

    # Remove from Insightly if previously pushed
    if lead.insightly_id:
        from app.services.insightly import get_insightly_client

        crm = get_insightly_client()
        if crm.enabled:
            deleted = await crm.delete_leads_by_slh_id(lead.id)
            logger.info(f"Insightly: deleted {deleted} leads for {lead.hotel_name}")
        lead.insightly_id = None

    await db.commit()
    await db.refresh(lead)
    return templates.TemplateResponse(request, "partials/lead_row.html", {"lead": lead})


@router.post(
    "/api/dashboard/leads/{lead_id}/delete",
    response_class=HTMLResponse,
    tags=["Dashboard"],
)
async def dashboard_delete_lead(
    request: Request,
    lead_id: int,
    db: AsyncSession = Depends(get_db),
    _csrf=Depends(require_ajax),
):
    """Soft-delete a lead (can be restored from Deleted tab)"""
    result = await db.execute(select(PotentialLead).where(PotentialLead.id == lead_id))
    lead = result.scalar_one_or_none()

    if not lead:
        return HTMLResponse(
            content="<div class='text-red-500 p-2'>Lead not found</div>",
            status_code=404,
        )

    lead.status = "deleted"

    lead.updated_at = local_now()

    await db.commit()

    # Return empty response so HTMX removes the row from the current tab
    return HTMLResponse(content="", status_code=200)


@router.get("/api/dashboard/sources/list", tags=["Dashboard"])
async def dashboard_sources_list(db: AsyncSession = Depends(get_db)):
    """Return all sources with metadata for scrape modal source selection."""

    result = await db.execute(
        select(Source)
        .where(Source.is_active.is_(True))
        .order_by(Source.priority.desc(), Source.name)
    )
    sources = result.scalars().all()

    now = local_now()

    # Build category counts
    cat_counts = {}
    cat_labels = {
        "chain_newsroom": "🏨 Chain Newsrooms",
        "luxury_independent": "💎 Luxury & Independent",
        "aggregator": "📰 Aggregators",
        "industry": "🏗️ Industry",
        "florida": "🌴 Florida",
        "caribbean": "🏝️ Caribbean",
        "travel_pub": "✈️ Travel Pubs",
        "pr_wire": "📡 PR Wire",
    }

    all_sources = []
    due_sources = []

    # Frequency → hours threshold
    freq_hours = {
        "daily": 20,
        "every_3_days": 68,
        "twice_weekly": 96,
        "weekly": 160,
        "monthly": 720,
    }

    for src in sources:
        # Count categories
        cat_counts[src.source_type] = cat_counts.get(src.source_type, 0) + 1

        # Gold URL count
        gold_urls = src.gold_urls or {} if hasattr(src, "gold_urls") else {}
        active_gold = sum(1 for m in gold_urls.values() if m.get("miss_streak", 0) < 3)

        source_data = {
            "id": src.id,
            "name": src.name,
            "type": src.source_type,
            "priority": src.priority,
            "frequency": src.scrape_frequency or "daily",
            "health": src.health_status or "new",
            "leads": src.leads_found or 0,
            "gold_count": active_gold,
            "last_scraped": src.last_scraped_at.isoformat()
            if src.last_scraped_at
            else None,
        }
        all_sources.append(source_data)

        # Check if due for scraping
        freq = src.scrape_frequency or "daily"
        threshold = freq_hours.get(freq, 160)  # default weekly

        is_due = False
        reason = ""

        if not src.last_scraped_at:
            is_due = True
            reason = "Never scraped"
        else:
            hours_since = (now - src.last_scraped_at).total_seconds() / 3600
            if hours_since >= threshold:
                is_due = True
                reason = f"{freq} (last: {hours_since:.0f}h ago)"

        if is_due:
            # Determine scrape mode for this source
            scrape_mode = "discover" if active_gold == 0 else "gold"
            needs_discovery = True
            if hasattr(src, "last_discovery_at") and src.last_discovery_at:
                interval = getattr(src, "discovery_interval_days", 7) or 7
                needs_discovery = (now - src.last_discovery_at) > timedelta(
                    days=interval
                )

            if needs_discovery:
                scrape_mode = "discover"

            due_sources.append(
                {
                    **source_data,
                    "reason": reason,
                    "mode": scrape_mode,
                }
            )

    categories = [
        {"type": t, "label": cat_labels.get(t, t), "count": c}
        for t, c in sorted(cat_counts.items())
    ]

    return {
        "sources": all_sources,
        "due_sources": due_sources,
        "categories": categories,
        "total": len(all_sources),
        "total_due": len(due_sources),
    }
