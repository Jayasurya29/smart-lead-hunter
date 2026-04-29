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
        # Search BOTH old (name, website) and new (hotel_name, hotel_website)
        # column names. Backfill from migration 018 keeps both populated, but
        # any field updates after that will only land in the canonical column.
        search_filter = or_(
            ExistingHotel.hotel_name.ilike(f"%{safe}%"),
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
        # A contact exists if EITHER the legacy gm_name or the canonical
        # contact_name is populated.
        query = query.where(
            or_(
                ExistingHotel.contact_name.isnot(None),
                ExistingHotel.gm_name.isnot(None),
            )
        )
        count_query = count_query.where(
            or_(
                ExistingHotel.contact_name.isnot(None),
                ExistingHotel.gm_name.isnot(None),
            )
        )
    elif has_contact == "false":
        query = query.where(
            ExistingHotel.contact_name.is_(None),
            ExistingHotel.gm_name.is_(None),
        )
        count_query = count_query.where(
            ExistingHotel.contact_name.is_(None),
            ExistingHotel.gm_name.is_(None),
        )

    sort_map = {
        # name_az/za sort by canonical hotel_name. Backfill from migration
        # 018 ensures every row has hotel_name populated.
        "name_az": ExistingHotel.hotel_name.asc(),
        "name_za": ExistingHotel.hotel_name.desc(),
        "city_az": ExistingHotel.city.asc(),
        "city_za": ExistingHotel.city.desc(),
        "state_az": ExistingHotel.state.asc(),
        "newest": ExistingHotel.created_at.desc(),
        "oldest": ExistingHotel.created_at.asc(),
        "revenue_high": ExistingHotel.revenue_annual.desc().nullslast(),
        "revenue_low": ExistingHotel.revenue_annual.asc().nullslast(),
        # Score (Option B account-fit). NULL scores sink to the bottom on
        # both directions so unscored rows don't pollute the top of either
        # ranking. The rescore script populates lead_score on every row,
        # but defensive nullslast() handles future rows that haven't been
        # rescored yet.
        "score_high": ExistingHotel.lead_score.desc().nullslast(),
        "score_low": ExistingHotel.lead_score.asc().nullslast(),
        # Brand tier. SQL string sort works because the canonical_tier
        # values ("tier1_ultra_luxury" → "tier4_upscale") sort
        # alphabetically in the right order: tier1 < tier2 < tier3 < tier4.
        "tier_asc": ExistingHotel.brand_tier.asc().nullslast(),
        "tier_desc": ExistingHotel.brand_tier.desc().nullslast(),
        # Location: state then city, both ascending/descending together.
        # Matches the frontend's display "city, state" so users see
        # adjacent rows clustered by state.
        "location_az": ExistingHotel.state.asc().nullslast(),
        "location_za": ExistingHotel.state.desc().nullslast(),
        # Opening date — for existing_hotels this is the (historical)
        # open date. NULLs go last on both directions.
        "opening_soon": ExistingHotel.opening_date.asc().nullslast(),
        "opening_late": ExistingHotel.opening_date.desc().nullslast(),
    }
    order = sort_map.get(sort, ExistingHotel.hotel_name.asc())
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


# ════════════════════════════════════════════════════════════════
# CONTACTS — list contacts attached to an existing hotel (Path Y)
# ════════════════════════════════════════════════════════════════
# Mirrors /api/dashboard/leads/{lead_id}/contacts but filters by
# existing_hotel_id (the dual-FK introduced in migration 018). The CHECK
# constraint on lead_contacts guarantees a contact has exactly one parent,
# so a contact returned here will NOT appear in any potential_lead's list.
#
# Same response shape as the potential-lead version — the frontend can
# reuse its existing ContactsTab component once we wire it through.
#
# This is the minimum needed to prove dual-FK works end-to-end. The next
# steps (Smart Fill SSE, Run Enrichment SSE, contact CRUD endpoints) will
# follow the same pattern as their potential-lead siblings.
@router.get("/{hotel_id}/contacts")
async def list_contacts_for_hotel(hotel_id: int, db: AsyncSession = Depends(get_db)):
    """List contacts attached to an existing hotel.

    Returns contacts in the same priority order used for potential_leads:
      1. is_primary first
      2. strategist_priority (P1/P2/P3/P4) ascending — NULL last
      3. scope (hotel_specific > chain_area > others)
      4. score descending
      5. is_saved descending
    """
    # Local imports — keep the module's top-level imports tight.
    from app.models.lead_contact import LeadContact
    from sqlalchemy import case

    # Verify the hotel exists — return 404 not empty list, so the frontend
    # can distinguish "no contacts found" from "wrong hotel id".
    hotel_check = await db.execute(
        select(ExistingHotel.id).where(ExistingHotel.id == hotel_id)
    )
    if hotel_check.scalar_one_or_none() is None:
        raise HTTPException(status_code=404, detail="Hotel not found")

    result = await db.execute(
        select(LeadContact)
        .where(LeadContact.existing_hotel_id == hotel_id)
        .order_by(
            LeadContact.is_primary.desc(),
            LeadContact.strategist_priority.asc().nullslast(),
            case(
                (LeadContact.scope == "hotel_specific", 0),
                (LeadContact.scope == "chain_area", 1),
                else_=2,
            ),
            LeadContact.score.desc(),
            LeadContact.is_saved.desc(),
        )
    )
    contacts = [c.to_dict() for c in result.scalars().all()]

    # Same priority-label re-sort the leads endpoint does — keeps UI ordering
    # identical between the two property kinds.
    _PRI_RANK = {"P1": 0, "P2": 1, "P3": 2, "P4": 3}
    contacts.sort(
        key=lambda c: (
            0 if c.get("is_primary") else 1,
            _PRI_RANK.get(c.get("priority_label", "P4"), 4),
            -(c.get("score") or 0),
            0 if c.get("is_saved") else 1,
        )
    )
    return contacts


# ════════════════════════════════════════════════════════════════
# RUN ENRICHMENT — SSE pipeline (Path Y → 1A, 2026-04-28)
# ════════════════════════════════════════════════════════════════
# Mirrors the potential_leads enrichment pipeline. Same fan-out
# architecture, same 11 stages, same Stop button behavior. The
# job registry in contacts.py is keyed by (parent_kind, parent_id)
# so the same machinery serves both kinds of property.
#
# Phase B early-rejection (residences_only) is skipped for existing
# hotels — they're already operating, can't be rejected as "not real".
# Everything else (Iter 1-6 + persist) is identical.
@router.get("/{hotel_id}/enrich-stream")
async def enrich_hotel_stream(hotel_id: int, request: Request):
    """SSE stream of enrichment progress for an existing hotel."""
    import asyncio
    import json
    import time
    from fastapi.responses import StreamingResponse

    # Reuse the job registry + factory from contacts.py — single source
    # of truth for both lead and hotel enrichment lifecycles.
    from app.routes.contacts import _jobs, _start_enrichment_job
    from app.database import async_session

    key = ("hotel", hotel_id)
    existing = _jobs.get(key)
    if existing is not None:
        job = existing
        logger.info(
            f"Attaching new watcher to running enrichment for hotel {hotel_id} "
            f"(now {len(job.subscribers) + 1} watcher(s))"
        )
    else:
        # First request for this hotel — verify it exists, build facts.
        async with async_session() as session:
            result = await session.execute(
                select(ExistingHotel).where(ExistingHotel.id == hotel_id)
            )
            hotel = result.scalar_one_or_none()
            if not hotel:
                raise HTTPException(status_code=404, detail="Hotel not found")

            # Build the facts dict the enrichment pipeline needs. Existing
            # hotels don't have timeline_label (per migration 018) — pass
            # empty string. opening_date IS retained as historical info.
            lead_facts = {
                "hotel_name": hotel.hotel_name or hotel.name,
                "brand": hotel.brand or "",
                "city": hotel.city or "",
                "state": hotel.state or "",
                "country": hotel.country or "USA",
                "management_company": hotel.management_company or "",
                "opening_date": hotel.opening_date or "",
                "timeline_label": "",  # not applicable to open hotels
                "description": hotel.description or "",
                "project_type_str": hotel.hotel_type or hotel.property_type or "",
                "search_name": getattr(hotel, "search_name", None) or "",
                "former_names": getattr(hotel, "former_names", None) or [],
            }

        job = await _start_enrichment_job("hotel", hotel_id, lead_facts)
        logger.info(f"Started new enrichment job for hotel {hotel_id}")

    # Subscribe this connection
    sub_queue: asyncio.Queue = asyncio.Queue(maxsize=100)
    job.subscribers.add(sub_queue)

    # Replay current state to this subscriber immediately so a reconnecting
    # user sees real progress (Iter 3 · 36% · 3min) on first paint, not a
    # blank "Connecting..." card.
    #
    # IMPORTANT: refresh elapsed_s BEFORE replaying. The stored
    # current_event has elapsed_s from when it was first emitted (could be
    # 30 seconds ago if Iter 3's queries are slow). If we replay it as-is,
    # the frontend's anchor calculation puts the job-start time at "30s
    # ago" — and the timer ticks 30, 31, 32 instead of the real 60, 61, 62.
    # Recompute elapsed_s against job.started_at so the anchor is correct.
    if job.current_event is not None:
        replay = dict(job.current_event)
        if "elapsed_s" in replay:
            replay["elapsed_s"] = round(time.monotonic() - job.started_at, 1)
        try:
            sub_queue.put_nowait(replay)
        except asyncio.QueueFull:
            pass

    async def event_stream():
        yield f'data: {json.dumps({"type": "started", "total": 11})}\n\n'
        try:
            while True:
                if await request.is_disconnected():
                    logger.info(
                        f"Watcher disconnected from hotel {hotel_id} enrichment; "
                        f"background task continues. "
                        f"({len(job.subscribers) - 1} watcher(s) remain)"
                    )
                    return
                try:
                    event = await asyncio.wait_for(sub_queue.get(), timeout=10.0)
                except asyncio.TimeoutError:
                    yield f'data: {json.dumps({"type": "ping"})}\n\n'
                    if job.task.done() and sub_queue.empty():
                        return
                    continue
                yield f"data: {json.dumps(event)}\n\n"
                if event["type"] in ("complete", "error"):
                    return
        except asyncio.CancelledError:
            logger.info(
                f"SSE stream cancelled for hotel {hotel_id}; "
                f"background task continues."
            )
            raise
        except Exception as e:
            logger.error(f"Enrich stream error (hotel {hotel_id}): {e}")
            try:
                yield f'data: {json.dumps({"type": "error", "message": str(e)[:200]})}\n\n'
            except Exception:
                pass
        finally:
            job.subscribers.discard(sub_queue)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-transform",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


@router.post("/{hotel_id}/enrich-cancel")
async def enrich_hotel_cancel(hotel_id: int, _csrf=Depends(require_ajax)):
    """Cancel an in-flight enrichment for an existing hotel."""
    from app.routes.contacts import _jobs

    job = _jobs.get(("hotel", hotel_id))
    if job is None:
        return {"cancelled": False, "reason": "no_active_job"}
    job.task.cancel()
    logger.info(f"User cancelled enrichment for hotel {hotel_id}")
    return {"cancelled": True}


@router.get("/{hotel_id}/enrich-status")
async def enrich_hotel_status(hotel_id: int):
    """Polling endpoint — is enrichment running for this hotel?"""
    import time
    from app.routes.contacts import _jobs

    job = _jobs.get(("hotel", hotel_id))
    if job is None:
        return {"running": False}
    return {
        "running": True,
        "current_event": job.current_event,
        "watchers": len(job.subscribers),
        "elapsed_s": round(time.monotonic() - job.started_at, 1),
    }


# ════════════════════════════════════════════════════════════════
# CONTACT CRUD — save/unsave/delete/edit/set-primary
# ════════════════════════════════════════════════════════════════
# Mirrors the potential_leads contact endpoints. Filter by
# existing_hotel_id (dual-FK from migration 018). All endpoints check
# the contact's existing_hotel_id matches the URL hotel_id — prevents
# cross-hotel contact tampering.
@router.post("/{hotel_id}/contacts/{contact_id}/save")
async def save_hotel_contact(
    hotel_id: int,
    contact_id: int,
    db: AsyncSession = Depends(get_db),
    _csrf=Depends(require_ajax),
):
    from app.models.lead_contact import LeadContact
    from app.services.utils import local_now

    result = await db.execute(
        select(LeadContact).where(
            LeadContact.id == contact_id,
            LeadContact.existing_hotel_id == hotel_id,
        )
    )
    contact = result.scalar_one_or_none()
    if not contact:
        raise HTTPException(status_code=404, detail="Contact not found")
    contact.is_saved = True
    contact.updated_at = local_now()
    await db.commit()
    return {"status": "saved", "contact_id": contact_id}


@router.post("/{hotel_id}/contacts/{contact_id}/unsave")
async def unsave_hotel_contact(
    hotel_id: int,
    contact_id: int,
    db: AsyncSession = Depends(get_db),
    _csrf=Depends(require_ajax),
):
    from app.models.lead_contact import LeadContact
    from app.services.utils import local_now

    result = await db.execute(
        select(LeadContact).where(
            LeadContact.id == contact_id,
            LeadContact.existing_hotel_id == hotel_id,
        )
    )
    contact = result.scalar_one_or_none()
    if not contact:
        raise HTTPException(status_code=404, detail="Contact not found")
    contact.is_saved = False
    contact.updated_at = local_now()
    await db.commit()
    return {"status": "unsaved", "contact_id": contact_id}


@router.delete("/{hotel_id}/contacts/{contact_id}")
async def delete_hotel_contact(
    hotel_id: int,
    contact_id: int,
    db: AsyncSession = Depends(get_db),
    _csrf=Depends(require_ajax),
):
    from app.models.lead_contact import LeadContact

    result = await db.execute(
        select(LeadContact).where(
            LeadContact.id == contact_id,
            LeadContact.existing_hotel_id == hotel_id,
        )
    )
    contact = result.scalar_one_or_none()
    if not contact:
        raise HTTPException(status_code=404, detail="Contact not found")
    await db.delete(contact)
    await db.commit()
    return {"status": "deleted", "contact_id": contact_id}


@router.post("/{hotel_id}/contacts/{contact_id}/set-primary")
async def set_hotel_primary_contact(
    hotel_id: int,
    contact_id: int,
    db: AsyncSession = Depends(get_db),
    _csrf=Depends(require_ajax),
):
    """Mark a contact as primary for this hotel. Unsets any other primary."""
    from app.models.lead_contact import LeadContact
    from app.services.utils import local_now

    # Unset any current primary
    others = await db.execute(
        select(LeadContact).where(
            LeadContact.existing_hotel_id == hotel_id,
            LeadContact.is_primary.is_(True),
        )
    )
    for c in others.scalars().all():
        c.is_primary = False

    # Set the new primary
    target = await db.execute(
        select(LeadContact).where(
            LeadContact.id == contact_id,
            LeadContact.existing_hotel_id == hotel_id,
        )
    )
    contact = target.scalar_one_or_none()
    if not contact:
        raise HTTPException(status_code=404, detail="Contact not found")
    contact.is_primary = True
    contact.updated_at = local_now()

    # Sync flat contact_* fields on hotel for fast list-page display
    hotel_result = await db.execute(
        select(ExistingHotel).where(ExistingHotel.id == hotel_id)
    )
    hotel = hotel_result.scalar_one_or_none()
    if hotel:
        hotel.contact_name = contact.name
        hotel.contact_title = contact.title
        hotel.contact_email = contact.email
        hotel.contact_phone = contact.phone

    await db.commit()
    return {"status": "primary_set", "contact_id": contact_id}


@router.patch("/{hotel_id}/contacts/{contact_id}")
async def update_hotel_contact(
    hotel_id: int,
    contact_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    _csrf=Depends(require_ajax),
):
    """Edit a contact. Same allowed-fields list as potential_leads variant."""
    from app.models.lead_contact import LeadContact
    from app.services.utils import local_now

    body = await request.json()
    result = await db.execute(
        select(LeadContact).where(
            LeadContact.id == contact_id,
            LeadContact.existing_hotel_id == hotel_id,
        )
    )
    contact = result.scalar_one_or_none()
    if not contact:
        raise HTTPException(status_code=404, detail="Contact not found")

    allowed = {
        "name",
        "title",
        "email",
        "phone",
        "linkedin",
        "organization",
        "evidence_url",
    }
    for fld, value in body.items():
        if fld in allowed:
            setattr(contact, fld, value)
    contact.updated_at = local_now()
    await db.commit()
    return contact.to_dict()


@router.post("/{hotel_id}/contacts")
async def add_hotel_contact(
    hotel_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    _csrf=Depends(require_ajax),
):
    """Manually add a contact to an existing hotel."""
    from app.models.lead_contact import LeadContact
    from app.services.utils import local_now

    # Verify hotel exists
    hotel_check = await db.execute(
        select(ExistingHotel.id).where(ExistingHotel.id == hotel_id)
    )
    if hotel_check.scalar_one_or_none() is None:
        raise HTTPException(status_code=404, detail="Hotel not found")

    body = await request.json()
    name = (body.get("name") or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="name is required")

    contact = LeadContact(
        existing_hotel_id=hotel_id,
        name=name,
        title=body.get("title"),
        email=body.get("email"),
        phone=body.get("phone"),
        linkedin=body.get("linkedin"),
        organization=body.get("organization"),
        scope=body.get("scope", "hotel_specific"),
        confidence="manual",
        is_saved=True,
        found_via="manual",
        last_enriched_at=local_now(),
    )
    db.add(contact)
    await db.commit()
    await db.refresh(contact)
    return contact.to_dict()


# ════════════════════════════════════════════════════════════════
# SMART FILL — SSE pipeline for AI-powered field filling
# ════════════════════════════════════════════════════════════════
# Mirrors /api/leads/{id}/smart-fill-stream from scraping.py. Reuses
# the same enrich_lead_data service — only the parent kind differs.
# Result-application logic mirrors _apply_enrichment_to_lead in
# scraping.py BUT skips timeline_label (existing_hotels don't carry it).
@router.get("/{hotel_id}/smart-fill-stream")
async def smart_fill_hotel_stream(
    hotel_id: int,
    request: Request,
    mode: str = Query("smart", pattern="^(smart|full)$"),
):
    """SSE stream of Smart Fill progress for an existing hotel.

    mode=smart: only fills fields that are currently empty
    mode=full:  re-fills every field (Full Refresh)
    """
    import asyncio
    import json
    import time
    from fastapi.responses import StreamingResponse
    from app.database import async_session
    from app.services.lead_data_enrichment import enrich_lead_data

    # Verify hotel exists + load facts
    async with async_session() as session:
        result = await session.execute(
            select(ExistingHotel).where(ExistingHotel.id == hotel_id)
        )
        hotel = result.scalar_one_or_none()
        if not hotel:
            raise HTTPException(status_code=404, detail="Hotel not found")

        # Snapshot what enrich_lead_data needs. Only the keyword args its
        # signature actually accepts — passing extras would crash with a
        # TypeError. The function reference: hotel_name, city, state,
        # country, brand, current_opening_date, current_brand_tier,
        # current_room_count, mode, search_name, progress_callback.
        hotel_snapshot = {
            "hotel_name": hotel.hotel_name or hotel.name or "",
            "city": hotel.city or "",
            "state": hotel.state or "",
            "country": hotel.country or "USA",
            "brand": hotel.brand or "",
            "current_opening_date": hotel.opening_date or "",
            "current_brand_tier": hotel.brand_tier or "",
            "current_room_count": hotel.room_count or 0,
            "search_name": getattr(hotel, "search_name", None) or "",
        }

    event_queue: asyncio.Queue = asyncio.Queue(maxsize=100)
    start_time = time.monotonic()

    async def progress_callback(stage: int, total: int, label: str):
        elapsed = round(time.monotonic() - start_time, 1)
        pct = min(100, round((stage / total) * 100))
        await event_queue.put(
            {
                "type": "stage",
                "stage": stage,
                "total": total,
                "label": label,
                "pct": pct,
                "elapsed_s": elapsed,
            }
        )

    async def run_smart_fill():
        try:
            enriched = await enrich_lead_data(
                **hotel_snapshot,
                mode=mode,
                progress_callback=progress_callback,
            )

            # Apply enriched fields to the hotel row.
            # The enrich_lead_data result dict has fields at the TOP LEVEL
            # (e.g. enriched["opening_date"], enriched["brand_tier"]) — NOT
            # nested under an "enriched" key. The full set of possible keys:
            #   project_type, opening_date, reopening_date, opened_date,
            #   already_opened, brand_tier, room_count, brand, city, state,
            #   country, description, address, zip_code, official_name,
            #   search_name, former_names, management_company, owner,
            #   developer, source_url, confidence, changes
            #
            # Mirrors _apply_enrichment_to_lead from scraping.py but SKIPS
            # timeline_label (existing_hotels don't have that column —
            # migration 018 deliberately omits it for already-opened hotels).
            applied: list[str] = []
            async with async_session() as apply_session:
                ah_result = await apply_session.execute(
                    select(ExistingHotel).where(ExistingHotel.id == hotel_id)
                )
                ah = ah_result.scalar_one_or_none()
                if ah and enriched and enriched.get("changes"):
                    _INVALID = {"", "unknown", "none", "n/a", "na", "tbd"}
                    _VALID_TIERS = {
                        "tier1_ultra_luxury",
                        "tier2_luxury",
                        "tier3_upper_upscale",
                        "tier4_upscale",
                        "tier5_skip",
                    }

                    # project_type — historical metadata, also useful on
                    # existing hotels (was it new build / renovation /
                    # rebrand). Always update if mode=full or empty.
                    if enriched.get("project_type") and (
                        mode == "full" or not ah.project_type
                    ):
                        ah.project_type = enriched["project_type"]
                        applied.append("project_type")

                    # opening_date — historical fact, but still apply
                    # the regression guard to reject less-specific values
                    # (e.g. "September 2024" → "2024" should be rejected).
                    if enriched.get("opening_date"):
                        from app.services.utils import should_accept_opening_date

                        accept, reason = should_accept_opening_date(
                            ah.opening_date, enriched["opening_date"]
                        )
                        if accept:
                            ah.opening_date = enriched["opening_date"]
                            try:
                                import re

                                m = re.search(r"(20\d{2})", enriched["opening_date"])
                                if m:
                                    ah.opening_year = int(m.group(1))
                            except Exception:
                                pass
                            applied.append("opening_date")
                        else:
                            logger.info(
                                f"Smart Fill REJECTED opening_date update for "
                                f"existing_hotel #{hotel_id}: {reason}"
                            )

                    if enriched.get("brand_tier"):
                        new_tier = enriched["brand_tier"].strip().lower()
                        current_tier = (ah.brand_tier or "").strip().lower()
                        if new_tier in _VALID_TIERS:
                            ah.brand_tier = enriched["brand_tier"]
                            applied.append("brand_tier")
                        elif current_tier in _INVALID and new_tier:
                            ah.brand_tier = enriched["brand_tier"]
                            applied.append("brand_tier")

                    if "room_count" in enriched:
                        try:
                            new_rc = int(enriched.get("room_count") or 0)
                        except (TypeError, ValueError):
                            new_rc = 0
                        if new_rc > 0 and (
                            mode == "full" or not ah.room_count or ah.room_count <= 0
                        ):
                            ah.room_count = new_rc
                            applied.append("room_count")

                    # hotel_type — feeds the Option B scorer (10 pts).
                    # In smart mode, fills only if current is empty or a
                    # generic freeform value. In full mode, always
                    # overwrite when a valid enum value is returned.
                    if enriched.get("hotel_type"):
                        new_ht = (enriched.get("hotel_type") or "").strip().lower()
                        _VALID_HT = {
                            "resort",
                            "all_inclusive",
                            "boutique",
                            "hotel",
                            "lodge",
                            "inn",
                        }
                        if new_ht in _VALID_HT:
                            current_ht = (ah.hotel_type or "").strip().lower()
                            ambiguous = (
                                current_ht == ""
                                or current_ht == "hotel"
                                or current_ht not in _VALID_HT
                            )
                            if mode == "full" or ambiguous:
                                ah.hotel_type = new_ht
                                ah.property_type = new_ht  # legacy mirror
                                applied.append("hotel_type")

                    if enriched.get("brand"):
                        new_brand = enriched["brand"].strip()
                        if new_brand and new_brand.lower() not in _INVALID:
                            if mode == "full" or not (ah.brand or "").strip():
                                ah.brand = new_brand
                                applied.append("brand")

                    # Location + descriptive fields — fill empties or full mode
                    for field in (
                        "city",
                        "state",
                        "country",
                        "description",
                        "address",
                        "zip_code",
                        "management_company",
                        "owner",
                        "developer",
                    ):
                        if enriched.get(field) and (
                            mode == "full" or not getattr(ah, field, None)
                        ):
                            setattr(ah, field, enriched[field])
                            applied.append(field)

                    # Name intelligence — always update when present
                    if enriched.get("official_name"):
                        ah.hotel_name = enriched["official_name"]
                        ah.name = enriched["official_name"]  # legacy backfill
                        applied.append("hotel_name")
                    if enriched.get("search_name"):
                        ah.search_name = enriched["search_name"]
                        applied.append("search_name")
                    if enriched.get("former_names"):
                        ah.former_names = enriched["former_names"]
                        applied.append("former_names")

                    # ── Auto-rescore (Option B, account fit) ────────
                    # Any time Smart Fill changes brand_tier, room_count,
                    # hotel_type, or zone, the score should refresh so
                    # the Pipeline ranking stays accurate. We rescore on
                    # ANY apply (cheap pure function — sub-millisecond)
                    # so we don't have to enumerate which fields matter.
                    if applied:
                        try:
                            from app.services.existing_hotel_scorer import (
                                apply_score_to_hotel,
                            )

                            new_score, _bd = apply_score_to_hotel(ah)
                            applied.append(f"lead_score={new_score}")
                        except Exception as _score_err:
                            logger.warning(
                                f"Smart Fill auto-rescore failed for "
                                f"existing_hotel #{hotel_id}: {_score_err}"
                            )

                    await apply_session.commit()

            # ── Revenue auto-update — runs in its own session AFTER
            # the Smart Fill commit. Smart Fill may have just filled
            # in room_count, brand_tier, or hotel_type — the three
            # inputs the revenue calculator needs. If revenue was NULL
            # before and the inputs are now present, it'll populate.
            # If inputs changed, revenue refreshes accordingly.
            if applied:
                try:
                    from app.services.revenue_updater import update_hotel_revenue

                    op, an = await update_hotel_revenue(hotel_id)
                    if op is not None:
                        applied.append(f"revenue_annual={int(an):,}")
                except Exception as _rev_err:
                    logger.warning(
                        f"Smart Fill revenue update failed for "
                        f"existing_hotel #{hotel_id}: {_rev_err}"
                    )

            duration = round(time.monotonic() - start_time, 1)
            await event_queue.put(
                {
                    "type": "complete",
                    "pct": 100,
                    "elapsed_s": duration,
                    "summary": {
                        "fields_filled": applied,
                        "duration_s": duration,
                        "mode": mode,
                        "confidence": enriched.get("confidence") if enriched else None,
                    },
                }
            )
        except Exception as e:
            logger.exception(f"Smart Fill failed for hotel {hotel_id}: {e}")
            await event_queue.put(
                {
                    "type": "error",
                    "message": f"Smart Fill failed: {str(e)[:200]}",
                }
            )

    task = asyncio.create_task(run_smart_fill())

    async def event_stream():
        yield f'data: {json.dumps({"type": "started", "total": 8})}\n\n'
        try:
            while True:
                if await request.is_disconnected():
                    return
                try:
                    event = await asyncio.wait_for(event_queue.get(), timeout=10.0)
                except asyncio.TimeoutError:
                    yield f'data: {json.dumps({"type": "ping"})}\n\n'
                    if task.done() and event_queue.empty():
                        return
                    continue
                yield f"data: {json.dumps(event)}\n\n"
                if event["type"] in ("complete", "error"):
                    return
        except asyncio.CancelledError:
            raise

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-transform",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


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

    # Allowed fields for PATCH. Includes BOTH legacy names (name, website,
    # property_type, gm_*) and new canonical names (hotel_name, hotel_website,
    # hotel_type, contact_*) — frontend can send either during the migration
    # transition. Legacy `phone` was dropped in migration 018.
    allowed = {
        # Legacy names — still accepted for backwards compat
        "name",
        "website",
        "property_type",
        "gm_name",
        "gm_title",
        "gm_email",
        "gm_phone",
        "gm_linkedin",
        # Canonical names (matches potential_leads + existing_hotel.py model)
        "hotel_name",
        "hotel_website",
        "hotel_type",
        "contact_name",
        "contact_title",
        "contact_email",
        "contact_phone",
        # Common identity / location
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
        "zone",
        "location_type",
        "website_verified",
        # Property details
        "room_count",
        "opening_date",
        "opening_year",
        "project_type",
        "description",
        "key_insights",
        # Stakeholders
        "management_company",
        "developer",
        "owner",
        # Name intelligence
        "search_name",
        "former_names",
        # Client / SAP fields
        "is_client",
        "sap_bp_code",
        "client_notes",
        # Workflow + scoring
        "status",
        "rejection_reason",
        "lead_score",
        "score_breakdown",
        "estimated_revenue",
        "revenue_opening",
        "revenue_annual",
        "notes",
        "data_source",
    }
    for fld, value in body.items():
        if fld in allowed:
            setattr(hotel, fld, value)

    await db.commit()
    return hotel.to_dict()


@router.post("")
async def create_existing_hotel(
    request: Request,
    db: AsyncSession = Depends(get_db),
    _csrf=Depends(require_ajax),
):
    body = await request.json()
    # Accept either legacy "name" or canonical "hotel_name". Same for
    # website/hotel_website and property_type/hotel_type.
    name = (body.get("hotel_name") or body.get("name") or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="Hotel name is required")

    hotel = ExistingHotel(
        # Canonical name (post-018). Also populate the legacy `name` column
        # for backwards compat with any code still reading it.
        hotel_name=name,
        name=name,
        brand=body.get("brand"),
        chain=body.get("chain"),
        brand_tier=body.get("brand_tier"),
        address=body.get("address"),
        city=body.get("city"),
        state=body.get("state"),
        country=body.get("country", "USA"),
        zip_code=body.get("zip_code"),
        latitude=body.get("latitude"),
        longitude=body.get("longitude"),
        room_count=body.get("room_count"),
        # Both canonical and legacy website fields populated for parity
        hotel_website=body.get("hotel_website") or body.get("website"),
        website=body.get("website") or body.get("hotel_website"),
        hotel_type=body.get("hotel_type") or body.get("property_type"),
        property_type=body.get("property_type") or body.get("hotel_type"),
        is_client=body.get("is_client", False),
        data_source=body.get("data_source", "manual"),
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
