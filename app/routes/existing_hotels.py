"""
Existing Hotels API — Phase 2
==============================
CRUD + filtering for the existing hotels prospecting database.
"""

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy import select, func, case, or_
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models.existing_hotel import ExistingHotel
from app.shared import require_ajax, escape_like

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/existing-hotels", tags=["Existing Hotels"])


# ── GET /api/existing-hotels — List with filtering + pagination ──


@router.get("")
async def list_existing_hotels(
    page: int = Query(1, ge=1),
    per_page: int = Query(25, ge=1, le=100),
    search: Optional[str] = None,
    state: Optional[str] = None,
    city: Optional[str] = None,
    brand_tier: Optional[str] = None,
    chain: Optional[str] = None,
    is_client: Optional[str] = None,
    status: Optional[str] = None,
    has_contact: Optional[str] = None,
    zone: Optional[str] = None,
    sort: str = "name_az",
    db: AsyncSession = Depends(get_db),
):
    """List existing hotels with filtering and pagination."""
    query = select(ExistingHotel)
    count_query = select(func.count(ExistingHotel.id))

    # Filters
    if search:
        safe = escape_like(search)
        search_filter = or_(
            ExistingHotel.name.ilike(f"%{safe}%"),
            ExistingHotel.city.ilike(f"%{safe}%"),
            ExistingHotel.brand.ilike(f"%{safe}%"),
            ExistingHotel.chain.ilike(f"%{safe}%"),
            ExistingHotel.state.ilike(f"%{safe}%"),
        )
        query = query.where(search_filter)
        count_query = count_query.where(search_filter)

    if state:
        safe = escape_like(state)
        query = query.where(ExistingHotel.state.ilike(f"%{safe}%"))
        count_query = count_query.where(ExistingHotel.state.ilike(f"%{safe}%"))

    if city:
        safe = escape_like(city)
        query = query.where(ExistingHotel.city.ilike(f"%{safe}%"))
        count_query = count_query.where(ExistingHotel.city.ilike(f"%{safe}%"))

    if brand_tier:
        query = query.where(ExistingHotel.brand_tier == brand_tier)
        count_query = count_query.where(ExistingHotel.brand_tier == brand_tier)

    if chain:
        safe = escape_like(chain)
        query = query.where(ExistingHotel.chain.ilike(f"%{safe}%"))
        count_query = count_query.where(ExistingHotel.chain.ilike(f"%{safe}%"))

    if is_client == "true":
        query = query.where(ExistingHotel.is_client.is_(True))
        count_query = count_query.where(ExistingHotel.is_client.is_(True))
    elif is_client == "false":
        query = query.where(ExistingHotel.is_client.is_(False))
        count_query = count_query.where(ExistingHotel.is_client.is_(False))

    if status:
        query = query.where(ExistingHotel.status == status)
        count_query = count_query.where(ExistingHotel.status == status)

    if zone:
        query = query.where(ExistingHotel.zone == zone)
        count_query = count_query.where(ExistingHotel.zone == zone)

    if has_contact == "true":
        query = query.where(ExistingHotel.gm_name.isnot(None))
        count_query = count_query.where(ExistingHotel.gm_name.isnot(None))
    elif has_contact == "false":
        query = query.where(ExistingHotel.gm_name.is_(None))
        count_query = count_query.where(ExistingHotel.gm_name.is_(None))

    # Sorting
    sort_map = {
        "name_az": ExistingHotel.name.asc(),
        "name_za": ExistingHotel.name.desc(),
        "city_az": ExistingHotel.city.asc(),
        "city_za": ExistingHotel.city.desc(),
        "state_az": ExistingHotel.state.asc(),
        "newest": ExistingHotel.created_at.desc(),
        "oldest": ExistingHotel.created_at.asc(),
        "revenue_high": ExistingHotel.revenue_annual.desc().nullslast(),
        "revenue_low": ExistingHotel.revenue_annual.asc().nullslast(),
    }
    order = sort_map.get(sort, ExistingHotel.name.asc())
    query = query.order_by(order)

    # Pagination
    total = (await db.execute(count_query)).scalar() or 0
    pages = (total + per_page - 1) // per_page if total > 0 else 1
    offset = (page - 1) * per_page
    query = query.offset(offset).limit(per_page)

    result = await db.execute(query)
    hotels = [h.to_dict() for h in result.scalars().all()]

    return {
        "hotels": hotels,
        "total": total,
        "page": page,
        "per_page": per_page,
        "pages": pages,
    }


# ── GET /api/existing-hotels/stats — Dashboard stats ──


@router.get("/stats")
async def existing_hotels_stats(db: AsyncSession = Depends(get_db)):
    """Get stats for the existing hotels database."""
    result = await db.execute(
        select(
            func.count(ExistingHotel.id).label("total"),
            func.sum(case((ExistingHotel.is_client.is_(True), 1), else_=0)).label(
                "clients"
            ),
            func.sum(case((ExistingHotel.is_client.is_(False), 1), else_=0)).label(
                "prospects"
            ),
            func.sum(case((ExistingHotel.latitude.isnot(None), 1), else_=0)).label(
                "geocoded"
            ),
            func.sum(case((ExistingHotel.gm_name.isnot(None), 1), else_=0)).label(
                "with_contact"
            ),
            func.sum(case((ExistingHotel.brand_tier.isnot(None), 1), else_=0)).label(
                "with_tier"
            ),
            func.sum(case((ExistingHotel.pushed_to_map.is_(True), 1), else_=0)).label(
                "on_map"
            ),
        )
    )
    r = result.one()

    # Tier breakdown
    tier_result = await db.execute(
        select(
            ExistingHotel.brand_tier,
            func.count(ExistingHotel.id),
        )
        .where(ExistingHotel.brand_tier.isnot(None))
        .group_by(ExistingHotel.brand_tier)
    )
    tiers = {row[0]: row[1] for row in tier_result.fetchall()}

    # Top states
    state_result = await db.execute(
        select(
            ExistingHotel.state,
            func.count(ExistingHotel.id),
        )
        .where(ExistingHotel.state.isnot(None))
        .group_by(ExistingHotel.state)
        .order_by(func.count(ExistingHotel.id).desc())
        .limit(10)
    )
    top_states = [{"state": row[0], "count": row[1]} for row in state_result.fetchall()]

    # Zone breakdown
    zone_result = await db.execute(
        select(
            ExistingHotel.zone,
            func.count(ExistingHotel.id),
        )
        .where(ExistingHotel.zone.isnot(None))
        .group_by(ExistingHotel.zone)
        .order_by(func.count(ExistingHotel.id).desc())
    )
    zones = [{"zone": row[0], "count": row[1]} for row in zone_result.fetchall()]

    return {
        "total": r.total or 0,
        "clients": r.clients or 0,
        "prospects": r.prospects or 0,
        "geocoded": r.geocoded or 0,
        "with_contact": r.with_contact or 0,
        "with_tier": r.with_tier or 0,
        "on_map": r.on_map or 0,
        "tiers": tiers,
        "top_states": top_states,
        "zones": zones,
    }


# ── GET /api/existing-hotels/map-data — Geocoded hotels for map ──


@router.get("/map-data")
async def map_data(
    is_client: Optional[str] = None,
    brand_tier: Optional[str] = None,
    state: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
):
    """Get geocoded hotels for map display. Returns minimal data for performance."""
    query = select(
        ExistingHotel.id,
        ExistingHotel.name,
        ExistingHotel.brand,
        ExistingHotel.brand_tier,
        ExistingHotel.city,
        ExistingHotel.state,
        ExistingHotel.latitude,
        ExistingHotel.longitude,
        ExistingHotel.is_client,
        ExistingHotel.room_count,
        ExistingHotel.phone,
        ExistingHotel.zone,
        ExistingHotel.revenue_annual,
    ).where(ExistingHotel.latitude.isnot(None))

    if is_client == "true":
        query = query.where(ExistingHotel.is_client.is_(True))
    elif is_client == "false":
        query = query.where(ExistingHotel.is_client.is_(False))

    if brand_tier:
        query = query.where(ExistingHotel.brand_tier == brand_tier)

    if state:
        query = query.where(ExistingHotel.state.ilike(f"%{escape_like(state)}%"))

    result = await db.execute(query)
    rows = result.fetchall()

    return [
        {
            "id": r[0],
            "name": r[1],
            "brand": r[2],
            "brand_tier": r[3],
            "city": r[4],
            "state": r[5],
            "lat": r[6],
            "lng": r[7],
            "is_client": r[8],
            "room_count": r[9],
            "phone": r[10],
            "zone": r[11],
            "revenue_annual": r[12],
        }
        for r in rows
    ]


# ── GET /api/existing-hotels/{id} — Single hotel detail ──


@router.get("/{hotel_id}")
async def get_existing_hotel(hotel_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(ExistingHotel).where(ExistingHotel.id == hotel_id))
    hotel = result.scalar_one_or_none()
    if not hotel:
        raise HTTPException(status_code=404, detail="Hotel not found")
    return hotel.to_dict()


# ── PATCH /api/existing-hotels/{id} — Edit hotel ──


@router.patch("/{hotel_id}")
async def update_existing_hotel(
    hotel_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    _csrf=Depends(require_ajax),
):
    body = await request.json()
    result = await db.execute(select(ExistingHotel).where(ExistingHotel.id == hotel_id))
    hotel = result.scalar_one_or_none()
    if not hotel:
        raise HTTPException(status_code=404, detail="Hotel not found")

    allowed = {
        "name",
        "brand",
        "chain",
        "brand_tier",
        "address",
        "city",
        "state",
        "country",
        "zip_code",
        "latitude",
        "longitude",
        "room_count",
        "phone",
        "website",
        "property_type",
        "gm_name",
        "gm_title",
        "gm_email",
        "gm_phone",
        "gm_linkedin",
        "is_client",
        "sap_bp_code",
        "client_notes",
        "status",
        "zone",
        "rejection_reason",
        "lead_score",
        "revenue_opening",
        "revenue_annual",
    }
    for field, value in body.items():
        if field in allowed:
            setattr(hotel, field, value)

    await db.commit()
    return hotel.to_dict()


# ── POST /api/existing-hotels — Create manually ──


@router.post("")
async def create_existing_hotel(
    request: Request,
    db: AsyncSession = Depends(get_db),
    _csrf=Depends(require_ajax),
):
    body = await request.json()
    name = (body.get("name") or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="Hotel name is required")

    hotel = ExistingHotel(
        name=name,
        brand=body.get("brand"),
        chain=body.get("chain"),
        brand_tier=body.get("brand_tier"),
        address=body.get("address"),
        city=body.get("city"),
        state=body.get("state"),
        country=body.get("country", "US"),
        zip_code=body.get("zip_code"),
        latitude=body.get("latitude"),
        longitude=body.get("longitude"),
        room_count=body.get("room_count"),
        phone=body.get("phone"),
        website=body.get("website"),
        is_client=body.get("is_client", False),
        data_source="manual",
        status="new",
    )
    db.add(hotel)
    await db.commit()
    return hotel.to_dict()


# ── POST /api/existing-hotels/{id}/approve — Push to Insightly ──


@router.post("/{hotel_id}/approve")
async def approve_existing_hotel(
    hotel_id: int,
    db: AsyncSession = Depends(get_db),
    _csrf=Depends(require_ajax),
):
    result = await db.execute(select(ExistingHotel).where(ExistingHotel.id == hotel_id))
    hotel = result.scalar_one_or_none()
    if not hotel:
        raise HTTPException(status_code=404, detail="Hotel not found")

    # Push to Insightly
    try:
        from app.services.insightly import get_insightly_client

        crm = get_insightly_client()
        insightly_lead = await crm.create_lead(
            {
                "FIRST_NAME": hotel.gm_name.split()[0]
                if hotel.gm_name and " " in hotel.gm_name
                else (hotel.gm_name or "Hotel"),
                "LAST_NAME": hotel.gm_name.split()[-1]
                if hotel.gm_name and " " in hotel.gm_name
                else hotel.name[:50],
                "ORGANISATION_NAME": hotel.name,
                "TITLE": hotel.gm_title or "",
                "EMAIL": hotel.gm_email or "",
                "PHONE": hotel.gm_phone or hotel.phone or "",
                "LEAD_SOURCE_ID": 3859952,
                "CUSTOMFIELDS": [
                    {"FIELD_NAME": "SLH_Lead_ID__c", "FIELD_VALUE": f"EH-{hotel.id}"},
                    {"FIELD_NAME": "SLH_Type__c", "FIELD_VALUE": "Existing Hotel"},
                ],
            }
        )
        hotel.insightly_id = insightly_lead.get("LEAD_ID")
    except Exception as e:
        logger.error(f"Insightly push failed for existing hotel {hotel_id}: {e}")

    hotel.status = "approved"
    await db.commit()
    return hotel.to_dict()


# ── POST /api/existing-hotels/{id}/reject ──


@router.post("/{hotel_id}/reject")
async def reject_existing_hotel(
    hotel_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    _csrf=Depends(require_ajax),
):
    body = (
        await request.json()
        if request.headers.get("content-type") == "application/json"
        else {}
    )
    result = await db.execute(select(ExistingHotel).where(ExistingHotel.id == hotel_id))
    hotel = result.scalar_one_or_none()
    if not hotel:
        raise HTTPException(status_code=404, detail="Hotel not found")

    hotel.status = "rejected"
    hotel.rejection_reason = body.get("reason", "")
    await db.commit()
    return hotel.to_dict()


# ── POST /api/existing-hotels/{id}/restore ──


@router.post("/{hotel_id}/restore")
async def restore_existing_hotel(
    hotel_id: int,
    db: AsyncSession = Depends(get_db),
    _csrf=Depends(require_ajax),
):
    result = await db.execute(select(ExistingHotel).where(ExistingHotel.id == hotel_id))
    hotel = result.scalar_one_or_none()
    if not hotel:
        raise HTTPException(status_code=404, detail="Hotel not found")

    # If approved + in Insightly, delete from CRM
    if hotel.status == "approved" and hotel.insightly_id:
        try:
            from app.services.insightly import get_insightly_client

            crm = get_insightly_client()
            await crm.delete_lead(hotel.insightly_id)
            hotel.insightly_id = None
        except Exception as e:
            logger.error(f"Insightly delete failed for existing hotel {hotel_id}: {e}")

    hotel.status = "new"
    hotel.rejection_reason = None
    await db.commit()
    return hotel.to_dict()


# ── POST /api/existing-hotels/export-csv — Export for Atlist ──


@router.post("/export-csv")
async def export_csv(
    request: Request,
    db: AsyncSession = Depends(get_db),
    _csrf=Depends(require_ajax),
):
    """Export hotels as CSV for Atlist upload."""
    body = await request.json()
    is_client = body.get("is_client")

    query = select(ExistingHotel).where(ExistingHotel.latitude.isnot(None))

    if is_client == "true":
        query = query.where(ExistingHotel.is_client.is_(True))
    elif is_client == "false":
        query = query.where(ExistingHotel.is_client.is_(False))

    result = await db.execute(query)
    hotels = result.scalars().all()

    rows = []
    for h in hotels:
        tier_label = (h.brand_tier or "").replace("_", " ").title()
        notes = f"Brand: {h.brand or 'Independent'} | Tier: {tier_label}"
        if h.room_count:
            notes += f" | Rooms: {h.room_count}"
        if h.phone:
            notes += f" | Phone: {h.phone}"
        if h.gm_name:
            notes += f" | GM: {h.gm_name}"

        group = "Client" if h.is_client else "Prospect"
        tags = ",".join(filter(None, [h.state, h.brand, tier_label]))

        rows.append(
            {
                "Name": h.name,
                "Address": f"{h.address or ''}, {h.city or ''}, {h.state or ''} {h.zip_code or ''}".strip(
                    ", "
                ),
                "Latitude": h.latitude,
                "Longitude": h.longitude,
                "Notes": notes,
                "Group": group,
                "Tags": tags,
            }
        )

    return {"rows": rows, "count": len(rows)}
