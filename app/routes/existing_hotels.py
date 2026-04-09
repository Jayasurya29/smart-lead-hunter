"""
Existing Hotels API
====================
CRUD + filtering for the existing hotels prospecting database.

Discovery endpoints have been migrated to use app.services.pipeline (Geoapify).
The old hotel_discovery.py and chain_discovery.py modules are deleted.
"""

import asyncio
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


# ════════════════════════════════════════════════════════════════
# LIST + FILTER
# ════════════════════════════════════════════════════════════════
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


# ════════════════════════════════════════════════════════════════
# STATS
# ════════════════════════════════════════════════════════════════
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

    tier_result = await db.execute(
        select(ExistingHotel.brand_tier, func.count(ExistingHotel.id))
        .where(ExistingHotel.brand_tier.isnot(None))
        .group_by(ExistingHotel.brand_tier)
    )
    tiers = {row[0]: row[1] for row in tier_result.fetchall()}

    # All states/regions — merge full registry (US + Caribbean) with DB counts
    # so even empty regions appear in the dropdown for discovery
    from app.services.zones_registry import ZONES as REGISTRY_ZONES

    state_result = await db.execute(
        select(ExistingHotel.state, func.count(ExistingHotel.id))
        .where(ExistingHotel.state.isnot(None))
        .group_by(ExistingHotel.state)
    )
    db_state_counts = {row[0]: row[1] for row in state_result.fetchall()}

    # Get one representative zone per state code → state display name
    REGION_NAMES = {
        # US states
        "AL": "Alabama",
        "AK": "Alaska",
        "AZ": "Arizona",
        "AR": "Arkansas",
        "CA": "California",
        "CO": "Colorado",
        "CT": "Connecticut",
        "DE": "Delaware",
        "DC": "District of Columbia",
        "FL": "Florida",
        "GA": "Georgia",
        "HI": "Hawaii",
        "ID": "Idaho",
        "IL": "Illinois",
        "IN": "Indiana",
        "IA": "Iowa",
        "KS": "Kansas",
        "KY": "Kentucky",
        "LA": "Louisiana",
        "ME": "Maine",
        "MD": "Maryland",
        "MA": "Massachusetts",
        "MI": "Michigan",
        "MN": "Minnesota",
        "MS": "Mississippi",
        "MO": "Missouri",
        "MT": "Montana",
        "NE": "Nebraska",
        "NV": "Nevada",
        "NH": "New Hampshire",
        "NJ": "New Jersey",
        "NM": "New Mexico",
        "NY": "New York",
        "NC": "North Carolina",
        "ND": "North Dakota",
        "OH": "Ohio",
        "OK": "Oklahoma",
        "OR": "Oregon",
        "PA": "Pennsylvania",
        "RI": "Rhode Island",
        "SC": "South Carolina",
        "SD": "South Dakota",
        "TN": "Tennessee",
        "TX": "Texas",
        "UT": "Utah",
        "VT": "Vermont",
        "VA": "Virginia",
        "WA": "Washington",
        "WV": "West Virginia",
        "WI": "Wisconsin",
        "WY": "Wyoming",
        # Caribbean (note: KY here is Kentucky, but Cayman zones have key prefix ky_)
        # We resolve by walking the registry instead of hardcoding region names
    }
    # Build region list from registry — handles both US states and Caribbean countries
    seen_states = set()
    all_regions = []
    for z in REGISTRY_ZONES.values():
        if z.state in seen_states:
            continue
        seen_states.add(z.state)
        # For Caribbean (ISO country codes), use the zone name as the region name
        # because there's only one zone per Caribbean country
        # For US states (state codes), use the official state name from REGION_NAMES
        is_caribbean = z.key.startswith(
            (
                "bs_",
                "jm_",
                "do_",
                "pr_",
                "ky_",
                "tc_",
                "bm_",
                "vi_",
                "vg_",
                "bb_",
                "aw_",
                "cw_",
                "lc_",
                "ag_",
                "ai_",
                "kn_",
                "sx_",
                "gd_",
                "dm_",
                "tt_",
                "vc_",
            )
        )
        region_name = z.name if is_caribbean else REGION_NAMES.get(z.state, z.state)
        all_regions.append(
            {
                "state": region_name,
                "code": z.state,
                "count": db_state_counts.get(region_name, 0),
                "is_caribbean": is_caribbean,
            }
        )

    # Include any orphan DB states not in the registry
    seen_names = {r["state"] for r in all_regions}
    for db_name, count in db_state_counts.items():
        if db_name not in seen_names:
            all_regions.append(
                {
                    "state": db_name,
                    "code": None,
                    "count": count,
                    "is_caribbean": False,
                }
            )

    # Sort: count desc, then name asc
    all_regions.sort(key=lambda r: (-r["count"], r["state"]))
    top_states = all_regions

    # Zone breakdown — merge DB counts with the full 126-zone registry
    # so empty zones still appear in the dropdown
    from app.services.zones_registry import ZONES as REGISTRY_ZONES

    zone_result = await db.execute(
        select(
            ExistingHotel.zone,
            func.count(ExistingHotel.id),
        )
        .where(ExistingHotel.zone.isnot(None))
        .group_by(ExistingHotel.zone)
    )
    db_counts = {row[0]: row[1] for row in zone_result.fetchall()}

    # Build the full list: every registry zone + any orphaned zones in DB
    zones = []
    seen_names = set()
    for z in REGISTRY_ZONES.values():
        zones.append(
            {
                "zone": z.name,
                "key": z.key,
                "state": z.state,
                "priority": z.priority,
                "count": db_counts.get(z.name, 0),
            }
        )
        seen_names.add(z.name)

    # Include any zone in DB that doesn't match the registry (legacy/orphaned data)
    for db_name, count in db_counts.items():
        if db_name not in seen_names:
            zones.append(
                {
                    "zone": db_name,
                    "key": None,
                    "state": None,
                    "priority": None,
                    "count": count,
                }
            )

    # Sort: count desc, then name asc (so populated zones float to top per state)
    zones.sort(key=lambda z: (-z["count"], z["zone"]))

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


# ════════════════════════════════════════════════════════════════
# MAP DATA
# ════════════════════════════════════════════════════════════════
@router.get("/map-data")
async def map_data(
    is_client: Optional[str] = None,
    brand_tier: Optional[str] = None,
    state: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
):
    """Get geocoded hotels for map display."""
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


# ════════════════════════════════════════════════════════════════
# DISCOVERY ENDPOINTS (powered by pipeline.py + Geoapify)
# ════════════════════════════════════════════════════════════════
@router.get("/discover/zones")
async def list_discovery_zones(
    state: Optional[str] = Query(
        None, description="Optional 2-letter state filter (e.g. FL)"
    ),
):
    """
    List available discovery zones.
    Pass ?state=FL to filter to one state, or omit for all 126 national zones.
    """
    from app.services.pipeline import get_zones_for_api

    return {"zones": get_zones_for_api(state=state)}


@router.post("/discover/{zone_key}")
async def run_zone_discovery(zone_key: str, enrich: bool = True):
    """
    Discover hotels in a specific zone via Geoapify pipeline.
    Workflow: Geoapify → tier filter → match against DB → insert NEW leads.
    """
    from app.services.pipeline import run_zone_for_api
    from app.services.zones_registry import ZONES

    if zone_key not in ZONES:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown zone: {zone_key}",
        )

    try:
        result = await asyncio.to_thread(run_zone_for_api, zone_key, True, enrich)
        return result
    except Exception as e:
        logger.error(f"Discovery failed for zone {zone_key}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Discovery failed: {str(e)[:200]}")


@router.post("/discover/state/{state_code}")
async def run_state_discovery(state_code: str, enrich: bool = True):
    """
    Discover hotels across all zones in a state.
    Example: POST /api/existing-hotels/discover/state/FL
    """
    from app.services.pipeline import run_state_for_api
    from app.services.zones_registry import zones_by_state

    state_code = state_code.upper()
    if not zones_by_state(state_code):
        raise HTTPException(status_code=400, detail=f"No zones for state: {state_code}")

    try:
        result = await asyncio.to_thread(run_state_for_api, state_code, True, enrich)
        return result
    except Exception as e:
        logger.error(f"State discovery failed for {state_code}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Discovery failed: {str(e)[:200]}")


@router.post("/discover/all")
async def run_all_discovery():
    """
    Backwards-compat endpoint: runs all Florida zones.
    For other states, use POST /discover/state/{state_code}
    """
    from app.services.pipeline import run_state_for_api

    try:
        result = await asyncio.to_thread(run_state_for_api, "FL", True)
        return result
    except Exception as e:
        logger.error(f"Full FL discovery failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Discovery failed: {str(e)[:200]}")


# ════════════════════════════════════════════════════════════════
# DETAIL / EDIT / CREATE
# ════════════════════════════════════════════════════════════════
@router.get("/{hotel_id}")
async def get_existing_hotel(hotel_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(ExistingHotel).where(ExistingHotel.id == hotel_id))
    hotel = result.scalar_one_or_none()
    if not hotel:
        raise HTTPException(status_code=404, detail="Hotel not found")
    return hotel.to_dict()


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


# ════════════════════════════════════════════════════════════════
# APPROVE / REJECT / RESTORE
# ════════════════════════════════════════════════════════════════
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


# ════════════════════════════════════════════════════════════════
# EXPORT CSV
# ════════════════════════════════════════════════════════════════
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
