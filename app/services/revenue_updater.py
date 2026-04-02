"""
SMART LEAD HUNTER — Revenue Auto-Updater
==========================================
Calculates and stores revenue_opening + revenue_annual on lead records.
Called after: lead creation, smart fill, manual edit, enrichment.

Usage:
    from app.services.revenue_updater import update_lead_revenue, bulk_update_revenue
    await update_lead_revenue(lead_id)        # Single lead
    await bulk_update_revenue()               # All leads missing revenue
"""

import logging
from sqlalchemy import select, update, and_

from app.database import async_session
from app.models.potential_lead import PotentialLead
from app.services.revenue_calculator import (
    calculate_annual_recurring,
    calculate_new_opening,
    detect_tier_from_brand,
)

logger = logging.getLogger(__name__)

# Map SLH tier format to calculator format
TIER_MAP = {
    "tier1_ultra_luxury": "ultra_luxury",
    "tier2_luxury": "luxury",
    "tier3_upper_upscale": "upper_upscale",
    "tier4_upscale": "upscale",
}


def _resolve_tier(lead) -> str | None:
    """Get calculator tier key from lead data."""
    if lead.brand_tier:
        tier_key = TIER_MAP.get(lead.brand_tier)
        if tier_key:
            return tier_key
    if lead.brand:
        return detect_tier_from_brand(lead.brand)
    return None


def _resolve_location(lead) -> str:
    """Build location string for climate resolution."""
    if lead.state:
        s = lead.state.upper().strip()
        city = (lead.city or "").lower()
        if s in ("FL", "FLORIDA"):
            if any(
                c in city
                for c in ["miami", "fort lauderdale", "palm beach", "boca", "key"]
            ):
                return "South Florida"
            if any(c in city for c in ["orlando", "tampa", "jacksonville"]):
                return "Rest of Florida"
            return "South Florida"
        if s in ("NY", "NEW YORK"):
            return "New York"
        if s in ("TX", "TEXAS"):
            return "Texas"
        if s in ("CA", "CALIFORNIA"):
            return "California"
        if s in ("HI", "HAWAII"):
            return "Hawaii"
        if s in ("NV", "NEVADA"):
            return "Las Vegas"
        if s in ("LA", "LOUISIANA"):
            return "New Orleans"
        if s in ("CO", "COLORADO", "UT", "UTAH", "MT", "MONTANA"):
            return "Mountain West"
        if s in ("WA", "WASHINGTON", "OR", "OREGON"):
            return "Pacific Northwest"
        if s in ("DC",):
            return "Washington DC"
        if s in ("MA", "CT", "NJ", "PA", "RI", "NH", "VT", "ME", "MD", "DE"):
            return "Northeast"
        if s in ("GA", "SC", "NC", "TN", "VA", "AL", "MS", "KY", "WV"):
            return "Southeast"
        if s in ("IL", "OH", "MI", "IN", "WI", "MN", "IA", "MO"):
            return "Midwest"
        if s in ("AZ", "NM"):
            return "Texas"

    if lead.location_type:
        loc_map = {
            "florida": "South Florida",
            "caribbean": "Caribbean",
            "usa": "Southeast",
            "international": "Caribbean",
        }
        return loc_map.get(lead.location_type, "Southeast")

    # Fallback: build from city/country
    if lead.country and lead.country != "USA":
        return "Caribbean"
    return "Southeast"


def _resolve_property_type(hotel_type: str | None) -> str:
    """Map hotel_type to calculator property_type."""
    if not hotel_type:
        return "resort"
    ht = hotel_type.lower()
    if "all" in ht and "incl" in ht:
        return "all_inclusive"
    if "resort" in ht:
        return "resort"
    if "convention" in ht or "conference" in ht:
        return "convention"
    if "boutique" in ht:
        return "boutique"
    if "theme" in ht or "park" in ht:
        return "theme_park"
    if "hotel" in ht or "city" in ht:
        return "city"
    return "resort"


def calculate_for_lead(lead) -> tuple[float | None, float | None]:
    """
    Calculate revenue for a lead object. Returns (opening, annual) or (None, None).
    Pure function — no DB access.
    """
    if not lead.room_count:
        return None, None

    tier_key = _resolve_tier(lead)
    if not tier_key:
        return None, None

    location = _resolve_location(lead)
    prop_type = _resolve_property_type(lead.hotel_type)

    try:
        opening = calculate_new_opening(
            rooms=lead.room_count,
            tier_key=tier_key,
            property_type=prop_type,
            location=location,
        )
        annual = calculate_annual_recurring(
            rooms=lead.room_count,
            tier_key=tier_key,
            property_type=prop_type,
            location=location,
        )
        return round(opening.ja_addressable), round(annual.ja_addressable)
    except Exception as e:
        logger.warning(f"Revenue calc failed for lead {lead.id}: {e}")
        return None, None


async def update_lead_revenue(lead_id: int) -> tuple[float | None, float | None]:
    """
    Calculate and store revenue for a single lead.
    Returns (opening, annual) values.
    """
    async with async_session() as session:
        result = await session.execute(
            select(PotentialLead).where(PotentialLead.id == lead_id)
        )
        lead = result.scalar_one_or_none()
        if not lead:
            return None, None

        opening, annual = calculate_for_lead(lead)

        # Only update if values changed
        if opening != lead.revenue_opening or annual != lead.revenue_annual:
            await session.execute(
                update(PotentialLead)
                .where(PotentialLead.id == lead_id)
                .values(revenue_opening=opening, revenue_annual=annual)
            )
            await session.commit()
            logger.info(
                f"Revenue updated for lead {lead_id} ({lead.hotel_name}): "
                f"opening=${opening:,}"
                if opening
                else "N/A" f", annual=${annual:,}"
                if annual
                else "N/A"
            )

        return opening, annual


async def bulk_update_revenue(force: bool = False) -> dict:
    """
    Calculate revenue for all leads that are missing it (or all if force=True).
    Returns summary stats.
    """
    async with async_session() as session:
        if force:
            result = await session.execute(select(PotentialLead))
        else:
            result = await session.execute(
                select(PotentialLead).where(
                    and_(
                        PotentialLead.revenue_opening.is_(None),
                        PotentialLead.room_count.isnot(None),
                    )
                )
            )
        leads = result.scalars().all()

    updated = 0
    skipped = 0
    failed = 0

    for lead in leads:
        opening, annual = calculate_for_lead(lead)
        if opening is not None:
            async with async_session() as session:
                await session.execute(
                    update(PotentialLead)
                    .where(PotentialLead.id == lead.id)
                    .values(revenue_opening=opening, revenue_annual=annual)
                )
                await session.commit()
            updated += 1
        elif lead.room_count and _resolve_tier(lead):
            failed += 1
        else:
            skipped += 1

    logger.info(
        f"Bulk revenue update: {updated} updated, {skipped} skipped (missing data), {failed} failed"
    )
    return {
        "updated": updated,
        "skipped": skipped,
        "failed": failed,
        "total": len(leads),
    }
