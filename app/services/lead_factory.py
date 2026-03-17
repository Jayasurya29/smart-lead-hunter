"""
SMART LEAD HUNTER - Lead Factory
=================================
Single entry point for ALL lead creation, regardless of source.
Ensures every lead gets: normalization, scoring, dedup check, enrichment.

Used by:
- POST /leads (manual API)
- orchestrator.save_leads_to_database (pipeline)
- scraping_tasks._save_lead_impl (Celery)
"""

import logging
import re
from typing import Dict, Optional, Tuple

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.potential_lead import PotentialLead
from app.services.utils import normalize_hotel_name, local_now, get_timeline_label
from app.services.scorer import calculate_lead_score
from app.config.intelligence_config import SCORE_HOT_THRESHOLD, SCORE_WARM_THRESHOLD

logger = logging.getLogger(__name__)


def extract_year(date_str: Optional[str]) -> Optional[int]:
    """Extract year from opening date string like 'Q3 2027' or '2026'."""
    if not date_str:
        return None
    import re

    match = re.search(r"20\d{2}", str(date_str))
    return int(match.group()) if match else None


# Patterns that indicate article titles / market summaries, not real hotels
_JUNK_PATTERNS = [
    re.compile(r"^\d+ new hotels? (in|for|forecasted|opening)", re.IGNORECASE),
    re.compile(r"hotels? forecasted for", re.IGNORECASE),
    re.compile(r"hotels? opening in 20\d{2}", re.IGNORECASE),
    re.compile(r"hotels? in \d{4}", re.IGNORECASE),
    re.compile(r"hotel construction", re.IGNORECASE),
    re.compile(r"hotel pipeline", re.IGNORECASE),
    re.compile(r"hotel forecast", re.IGNORECASE),
    re.compile(r"new openings for 20\d{2}", re.IGNORECASE),
]


def prepare_lead(
    lead_dict: Dict,
) -> Tuple[Optional[PotentialLead], Optional[str], Dict]:
    """
    Normalize, score, and build a PotentialLead from any source.

    Returns:
        (lead_obj, skip_reason, score_result)
        - lead_obj: PotentialLead ready for DB insert (or None if skipped)
        - skip_reason: str if skipped, None if valid
        - score_result: full scoring dict for reference
    """
    hotel_name = (lead_dict.get("hotel_name") or "").strip()
    if not hotel_name:
        return None, "No hotel name", {}

    # Reject article titles / market summaries
    for pattern in _JUNK_PATTERNS:
        if pattern.search(hotel_name):
            return None, f"Article title, not a hotel: {hotel_name}", {}

    # 1. NORMALIZE
    normalized = normalize_hotel_name(hotel_name)

    # 2. SCORE (also determines brand_tier, location_type, opening_year, should_save)
    score_result = calculate_lead_score(
        hotel_name=hotel_name,
        city=lead_dict.get("city"),
        state=lead_dict.get("state"),
        country=lead_dict.get("country", "USA"),
        opening_date=lead_dict.get("opening_date"),
        room_count=lead_dict.get("room_count"),
        contact_name=lead_dict.get("contact_name"),
        contact_email=lead_dict.get("contact_email"),
        contact_phone=lead_dict.get("contact_phone"),
        brand=lead_dict.get("brand"),
    )

    # 3. FILTER — skip budget brands
    if not score_result.get("should_save", True):
        return None, score_result.get("skip_reason", "Filtered"), score_result

    # 4. DETERMINE FINAL SCORE
    # Prefer pipeline qualification_score if available, fall back to calculated
    pipeline_score = lead_dict.get("qualification_score") or lead_dict.get("lead_score")
    final_score = pipeline_score if pipeline_score else score_result["total_score"]

    # 5. PARSE ROOM COUNT
    room_count = None
    try:
        room_count = int(float(lead_dict.get("room_count", 0) or 0))
        if room_count == 0:
            room_count = None
    except (ValueError, TypeError):
        pass

    # 6. BUILD LEAD
    lead = PotentialLead(
        hotel_name=hotel_name,
        hotel_name_normalized=normalized,
        brand=lead_dict.get("brand") or None,
        brand_tier=score_result.get("brand_tier"),
        hotel_type=lead_dict.get("property_type") or lead_dict.get("hotel_type"),
        hotel_website=lead_dict.get("hotel_website"),
        city=lead_dict.get("city"),
        state=lead_dict.get("state"),
        country=lead_dict.get("country", "USA"),
        location_type=score_result.get("location_type"),
        opening_date=lead_dict.get("opening_date"),
        opening_year=score_result.get("opening_year")
        or extract_year(lead_dict.get("opening_date")),
        timeline_label=get_timeline_label(lead_dict.get("opening_date") or ""),
        room_count=room_count,
        contact_name=lead_dict.get("contact_name"),
        contact_title=lead_dict.get("contact_title"),
        contact_email=lead_dict.get("contact_email"),
        contact_phone=lead_dict.get("contact_phone"),
        description=lead_dict.get("key_insights") or lead_dict.get("description"),
        key_insights=lead_dict.get("key_insights"),
        management_company=lead_dict.get("management_company"),
        developer=lead_dict.get("developer"),
        owner=lead_dict.get("owner"),
        source_url=lead_dict.get("source_url"),
        source_site=lead_dict.get("source_name")
        or lead_dict.get("source_site")
        or "manual",
        lead_score=final_score,
        score_breakdown=score_result.get("breakdown", {}),
        status="new",
        raw_data=lead_dict.get("raw_data"),
        scraped_at=local_now(),
        created_at=local_now(),
        updated_at=local_now(),
    )

    return lead, None, score_result


def enrich_existing_lead(existing: PotentialLead, lead_dict: Dict) -> bool:
    """
    Enrich an existing lead with new/better data from a duplicate extraction.
    Returns True if any fields were updated.
    """
    enriched = False

    # Fill empty fields with new data
    enrichment_fields = {
        "brand": lead_dict.get("brand"),
        "city": lead_dict.get("city"),
        "state": lead_dict.get("state"),
        "country": lead_dict.get("country"),
        "opening_date": lead_dict.get("opening_date"),
        "room_count": lead_dict.get("room_count"),
        "contact_name": lead_dict.get("contact_name"),
        "contact_title": lead_dict.get("contact_title"),
        "contact_email": lead_dict.get("contact_email"),
        "contact_phone": lead_dict.get("contact_phone"),
        "description": lead_dict.get("key_insights") or lead_dict.get("description"),
        "hotel_type": lead_dict.get("property_type") or lead_dict.get("hotel_type"),
    }

    for field, new_val in enrichment_fields.items():
        if not new_val:
            continue
        old_val = getattr(existing, field, None)
        if not old_val:
            setattr(existing, field, new_val)
            enriched = True
        elif field == "description" and len(str(new_val)) > len(str(old_val)):
            setattr(existing, field, new_val)
            enriched = True
        elif field == "opening_date" and len(str(new_val)) > len(str(old_val)):
            # "March 2026" is more specific than "2026"
            setattr(existing, field, new_val)
            existing.timeline_label = get_timeline_label(str(new_val))
            enriched = True
        elif field == "room_count" and not old_val and new_val:
            setattr(existing, field, new_val)
            enriched = True
        elif (
            field == "room_count"
            and old_val
            and new_val
            and int(new_val) > 0
            and int(old_val) == 0
        ):
            setattr(existing, field, new_val)
            enriched = True

    # Track source URLs
    new_source_url = lead_dict.get("source_url")
    if new_source_url:
        existing_urls = existing.source_urls or []
        if new_source_url not in existing_urls:
            existing.source_urls = existing_urls + [new_source_url]
            enriched = True

        # Track what this source extracted
        extractions = dict(existing.source_extractions or {})
        if new_source_url not in extractions:
            extractions[new_source_url] = {
                k: v
                for k, v in {
                    "hotel_name": lead_dict.get("hotel_name"),
                    "brand": lead_dict.get("brand"),
                    "city": lead_dict.get("city"),
                    "state": lead_dict.get("state"),
                    "country": lead_dict.get("country"),
                    "opening_date": lead_dict.get("opening_date"),
                    "room_count": lead_dict.get("room_count"),
                    "contact_name": lead_dict.get("contact_name"),
                    "contact_email": lead_dict.get("contact_email"),
                    "contact_phone": lead_dict.get("contact_phone"),
                    "key_insights": lead_dict.get("key_insights"),
                    "source_name": lead_dict.get("source_name")
                    or lead_dict.get("source_site"),
                }.items()
                if v
            }
            existing.source_extractions = extractions
            enriched = True

    if enriched:
        # Recalculate timeline_label if opening_date was set or updated
        if existing.opening_date:
            existing.timeline_label = get_timeline_label(existing.opening_date)
        existing.updated_at = local_now()

    return enriched


async def save_lead_to_db(
    lead_dict: Dict,
    session: AsyncSession,
    commit: bool = True,
) -> Dict:
    """
    Full pipeline: normalize → dedup → enrich OR score → save.
    Single entry point for ALL lead saves.

    Returns:
        {"status": "saved"|"duplicate"|"enriched"|"skipped", "id": int|None, "reason": str|None}
    """
    hotel_name = (lead_dict.get("hotel_name") or "").strip()
    if not hotel_name:
        return {"status": "skipped", "id": None, "reason": "No hotel name"}

    normalized = normalize_hotel_name(hotel_name)

    # DEDUP CHECK
    result = await session.execute(
        select(PotentialLead).where(PotentialLead.hotel_name_normalized == normalized)
    )
    existing = result.scalars().first()

    if existing:
        # Enrich existing lead with new data
        enriched = enrich_existing_lead(existing, lead_dict)
        if enriched:
            logger.info(f"   🔄 Enriched: {hotel_name}")
        if commit:
            await session.commit()
        return {
            "status": "enriched" if enriched else "duplicate",
            "id": existing.id,
            "reason": "Already exists",
        }

    # PREPARE NEW LEAD (normalize + score + filter + build)
    lead, skip_reason, score_result = prepare_lead(lead_dict)

    if lead is None:
        logger.info(f"   ⏭️ Skipped: {hotel_name} - {skip_reason}")
        return {"status": "skipped", "id": None, "reason": skip_reason}

    # SAVE
    session.add(lead)
    if commit:
        await session.commit()
        await session.refresh(lead)

    quality = (
        "🔴 HOT"
        if lead.lead_score >= SCORE_HOT_THRESHOLD
        else "🟠 WARM"
        if lead.lead_score >= SCORE_WARM_THRESHOLD
        else "🔵 COOL"
    )
    logger.info(f"   {quality} [{lead.lead_score}] {hotel_name}")

    return {"status": "saved", "id": lead.id, "reason": None}


async def save_leads_batch(
    lead_dicts: list,
    session: AsyncSession,
) -> Dict:
    """
    Save a batch of leads through the full pipeline.
    Wraps each lead in a savepoint for isolation.

    Returns:
        {"saved": int, "duplicates": int, "enriched": int, "skipped": int, "errors": int}
    """
    saved = 0
    duplicates = 0
    enriched = 0
    skipped = 0
    errors = 0

    for lead_dict in lead_dicts:
        try:
            async with session.begin_nested():
                result = await save_lead_to_db(lead_dict, session, commit=False)

            status = result["status"]
            if status == "saved":
                saved += 1
            elif status == "duplicate":
                duplicates += 1
            elif status == "enriched":
                enriched += 1
                duplicates += 1  # Count enriched as duplicate for backward compat
            elif status == "skipped":
                skipped += 1

        except Exception as e:
            logger.error(f"   ❌ Error: {lead_dict.get('hotel_name', 'unknown')}: {e}")
            errors += 1

    await session.commit()

    logger.info(
        f"\n✅ SAVED: {saved} | Duplicates: {duplicates} | Skipped: {skipped} | Errors: {errors}"
    )
    return {
        "saved": saved,
        "duplicates": duplicates,
        "enriched": enriched,
        "skipped": skipped,
        "errors": errors,
    }
