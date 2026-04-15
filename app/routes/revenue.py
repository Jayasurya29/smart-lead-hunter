"""
SMART LEAD HUNTER — Revenue Calculator API
============================================
Endpoints for calculating uniform revenue potential.

Usage:
    from app.routes.revenue import router as revenue_router
    app.include_router(revenue_router)
"""

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import select

from app.database import async_session
from app.models.potential_lead import PotentialLead
from app.services.revenue_calculator import (
    TIERS,
    calculate_annual_recurring,
    calculate_new_opening,
    calculate_rebrand,
    detect_tier_from_brand,
    get_all_climates,
    get_all_tiers,
    get_property_types,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/revenue", tags=["revenue"])


# ─── Request Schemas ─────────────────────────────────────────────────────────


class RevenueCalculateRequest(BaseModel):
    """Manual revenue calculation request."""

    rooms: int = Field(..., ge=1, le=10000, description="Number of rooms")
    tier: str = Field(
        ..., description="Tier key: ultra_luxury, luxury, upper_upscale, upscale"
    )
    property_type: str = Field(
        default="resort",
        description="city, resort, convention, all_inclusive, theme_park, boutique",
    )
    location: str = Field(
        default="South Florida", description="Location name or SLH dropdown value"
    )
    fb_outlets: int = Field(default=0, ge=0, le=30, description="Number of F&B outlets")
    lead_type: str = Field(
        default="new_opening", description="new_opening, annual_recurring, rebrand"
    )
    ja_actual: Optional[float] = Field(
        default=None, description="JA's actual SAP revenue for wallet share calc"
    )
    rebrand_pct: Optional[float] = Field(
        default=0.70, ge=0.0, le=1.0, description="Rebrand replacement % (0.6-0.8)"
    )


# ─── Endpoints ───────────────────────────────────────────────────────────────


@router.post("/calculate")
async def calculate_revenue(req: RevenueCalculateRequest):
    """
    Calculate revenue potential from manual inputs.
    Returns full breakdown with staffing, costs, and JA addressable.
    """
    if req.tier not in TIERS:
        raise HTTPException(
            400, f"Invalid tier: {req.tier}. Valid: {list(TIERS.keys())}"
        )

    if req.lead_type == "new_opening":
        result = calculate_new_opening(
            rooms=req.rooms,
            tier_key=req.tier,
            property_type=req.property_type,
            location=req.location,
            fb_outlets=req.fb_outlets,
            ja_actual=req.ja_actual,
        )
    elif req.lead_type == "annual_recurring":
        result = calculate_annual_recurring(
            rooms=req.rooms,
            tier_key=req.tier,
            property_type=req.property_type,
            location=req.location,
            fb_outlets=req.fb_outlets,
            ja_actual=req.ja_actual,
        )
    elif req.lead_type == "rebrand":
        result = calculate_rebrand(
            rooms=req.rooms,
            tier_key=req.tier,
            property_type=req.property_type,
            location=req.location,
            fb_outlets=req.fb_outlets,
            ja_actual=req.ja_actual,
            rebrand_pct=req.rebrand_pct or 0.70,
        )
    else:
        raise HTTPException(
            400,
            f"Invalid lead_type: {req.lead_type}. Valid: new_opening, annual_recurring, rebrand",
        )

    return result.to_dict()


@router.get("/estimate/lead/{lead_id}")
async def estimate_lead_revenue(lead_id: int):
    """
    Auto-calculate revenue potential for an existing SLH lead.
    Pulls room_count, brand, location from the lead record.
    Returns estimate if enough data available, or missing fields list.
    """
    async with async_session() as session:
        result = await session.execute(
            select(PotentialLead).where(PotentialLead.id == lead_id)
        )
        lead = result.scalar_one_or_none()

    if not lead:
        raise HTTPException(404, f"Lead {lead_id} not found")

    # Check what data we have
    missing = []
    if not lead.room_count:
        missing.append("room_count")

    # Auto-detect tier from brand or use existing brand_tier
    tier_key = None
    if lead.brand_tier:
        # Map SLH tier format to calculator format
        tier_map = {
            "tier1_ultra_luxury": "ultra_luxury",
            "tier2_luxury": "luxury",
            "tier3_upper_upscale": "upper_upscale",
            "tier4_upscale": "upscale",
        }
        tier_key = tier_map.get(lead.brand_tier)

    if not tier_key and lead.brand:
        tier_key = detect_tier_from_brand(lead.brand)

    if not tier_key:
        missing.append("brand_tier (cannot determine tier from brand)")

    # Resolve location
    location = _resolve_lead_location(lead)

    # Map hotel_type to property_type
    property_type = _map_hotel_type(lead.hotel_type)

    if missing:
        return {
            "status": "incomplete",
            "lead_id": lead_id,
            "hotel_name": lead.hotel_name,
            "missing_fields": missing,
            "available": {
                "room_count": lead.room_count,
                "brand": lead.brand,
                "brand_tier": lead.brand_tier,
                "detected_tier": tier_key,
                "location": location,
                "property_type": property_type,
                "hotel_type": lead.hotel_type,
            },
            "message": "Cannot calculate revenue potential without required fields. "
            "Please add room count and/or brand tier to this lead.",
        }

    # Calculate new opening (SLH leads are new hotel openings)
    opening = calculate_new_opening(
        rooms=lead.room_count,
        tier_key=tier_key,
        property_type=property_type,
        location=location,
        fb_outlets=0,  # Unknown for new leads
    )

    # Also calculate annual recurring for comparison
    annual = calculate_annual_recurring(
        rooms=lead.room_count,
        tier_key=tier_key,
        property_type=property_type,
        location=location,
        fb_outlets=0,
    )

    # Persist to DB so the pipeline table shows the value without a separate bulk update
    try:
        from sqlalchemy import update as sql_update

        async with async_session() as session:
            await session.execute(
                sql_update(PotentialLead)
                .where(PotentialLead.id == lead_id)
                .values(
                    revenue_opening=round(opening.ja_addressable),
                    revenue_annual=round(annual.ja_addressable),
                )
            )
            await session.commit()
    except Exception as e:
        logger.warning(f"Could not persist revenue for lead {lead_id}: {e}")

    return {
        "status": "success",
        "lead_id": lead_id,
        "hotel_name": lead.hotel_name,
        "brand": lead.brand,
        "detected_tier": tier_key,
        "tier_label": TIERS[tier_key]["label"],
        "location": location,
        "property_type": property_type,
        "rooms": lead.room_count,
        "new_opening": opening.to_dict(),
        "annual_recurring": annual.to_dict(),
    }


@router.get("/tiers")
async def list_tiers():
    """Return all tier configurations for frontend dropdowns."""
    return {"tiers": get_all_tiers()}


@router.get("/climates")
async def list_climates():
    """Return all climate configurations for frontend dropdowns."""
    return {"climates": get_all_climates()}


@router.get("/property-types")
async def list_property_types():
    """Return valid property types for frontend dropdowns."""
    return {"property_types": get_property_types()}


@router.get("/quick-estimate")
async def quick_estimate(
    rooms: int = Query(..., ge=1, le=10000),
    tier: str = Query(...),
    location: str = Query(default="South Florida"),
    property_type: str = Query(default="resort"),
):
    """
    Quick GET-based estimate — for embedding in lead table tooltips.
    Returns just the key numbers, not full breakdown.
    """
    if tier not in TIERS:
        raise HTTPException(400, f"Invalid tier: {tier}")

    opening = calculate_new_opening(rooms, tier, property_type, location)
    annual = calculate_annual_recurring(rooms, tier, property_type, location)

    return {
        "rooms": rooms,
        "tier": tier,
        "tier_label": TIERS[tier]["label"],
        "location": location,
        "opening_estimate": round(opening.ja_addressable),
        "annual_estimate": round(annual.ja_addressable),
        "total_staff": opening.total_staff,
        "uniformed_staff": opening.uniformed_staff,
    }


# ─── Helper Functions ────────────────────────────────────────────────────────
@router.post("/bulk-update")
async def bulk_update():
    """Recalculate revenue for all leads missing it."""
    from app.services.revenue_updater import bulk_update_revenue

    result = await bulk_update_revenue()
    return result


@router.post("/bulk-update/force")
async def bulk_update_force():
    """Recalculate revenue for ALL leads."""
    from app.services.revenue_updater import bulk_update_revenue

    result = await bulk_update_revenue(force=True)
    return result


@router.post("/update/{lead_id}")
async def update_single(lead_id: int):
    """Recalculate revenue for a single lead."""
    from app.services.revenue_updater import update_lead_revenue

    opening, annual = await update_lead_revenue(lead_id)
    return {"lead_id": lead_id, "revenue_opening": opening, "revenue_annual": annual}


def _resolve_lead_location(lead) -> str:
    """Build a location string from lead fields for climate resolution."""
    # Try location_type first (SLH's own classification)
    if lead.location_type:
        loc_map = {
            "florida": "South Florida",
            "caribbean": "Caribbean",
            "usa": "Southeast",  # Default for generic USA
            "international": "Caribbean",  # Most international leads are Caribbean
        }
        location = loc_map.get(lead.location_type, "Southeast")

        # Refine if we have state info
        if lead.state:
            state_upper = lead.state.upper().strip()
            if state_upper in ("FL", "FLORIDA"):
                # Check if South FL or Orlando area
                city_lower = (lead.city or "").lower()
                if any(
                    c in city_lower
                    for c in ["miami", "fort lauderdale", "palm beach", "boca", "key"]
                ):
                    return "South Florida"
                if any(
                    c in city_lower
                    for c in ["orlando", "tampa", "jacksonville", "kissimmee"]
                ):
                    return "Rest of Florida"
                return "South Florida"  # Default FL to South FL
            if state_upper in ("NY", "NEW YORK"):
                return "New York"
            if state_upper in ("TX", "TEXAS"):
                return "Texas"
            if state_upper in ("CA", "CALIFORNIA"):
                return "California"
            if state_upper in ("HI", "HAWAII"):
                return "Hawaii"
            if state_upper in ("NV", "NEVADA"):
                return "Las Vegas"
            if state_upper in ("LA", "LOUISIANA"):
                return "New Orleans"
            if state_upper in ("CO", "COLORADO", "UT", "UTAH", "MT", "MONTANA"):
                return "Mountain West"
            if state_upper in ("WA", "WASHINGTON", "OR", "OREGON"):
                return "Pacific Northwest"
            if state_upper in ("DC",):
                return "Washington DC"
            # Northeast states
            if state_upper in (
                "MA",
                "CT",
                "NJ",
                "PA",
                "RI",
                "NH",
                "VT",
                "ME",
                "MD",
                "DE",
            ):
                return "Northeast"
            # Southeast states
            if state_upper in ("GA", "SC", "NC", "TN", "VA", "AL", "MS", "KY", "WV"):
                return "Southeast"
            # Midwest
            if state_upper in (
                "IL",
                "OH",
                "MI",
                "IN",
                "WI",
                "MN",
                "IA",
                "MO",
                "KS",
                "NE",
                "ND",
                "SD",
            ):
                return "Midwest"
            # Southwest
            if state_upper in ("AZ", "NM"):
                return "Texas"  # Similar climate profile

        return location

    # Fallback: build from city + state + country
    parts = []
    if lead.city:
        parts.append(lead.city)
    if lead.state:
        parts.append(lead.state)
    if lead.country and lead.country != "USA":
        parts.append(lead.country)

    return ", ".join(parts) if parts else "Southeast"


def _map_hotel_type(hotel_type: Optional[str]) -> str:
    """Map SLH hotel_type to revenue calculator property_type."""
    if not hotel_type:
        return "resort"  # Default

    ht_lower = hotel_type.lower().strip()

    if "all" in ht_lower and "incl" in ht_lower:
        return "all_inclusive"
    if "resort" in ht_lower:
        return "resort"
    if "convention" in ht_lower or "conference" in ht_lower:
        return "convention"
    if "boutique" in ht_lower:
        return "boutique"
    if "theme" in ht_lower or "park" in ht_lower:
        return "theme_park"
    if "hotel" in ht_lower or "city" in ht_lower:
        return "city"

    return "resort"  # Default for unknown types
