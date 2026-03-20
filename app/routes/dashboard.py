"""Dashboard API routes — lead actions, sources list."""

import logging
from datetime import timedelta
from typing import Optional

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import JSONResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models import PotentialLead, Source
from app.models.lead_contact import LeadContact
from app.services.rescore import rescore_lead
from app.services.utils import local_now, normalize_hotel_name
from app.shared import (
    require_ajax,
    checked_json,
)

logger = logging.getLogger(__name__)

router = APIRouter()


# ═══════════════════════════════════════════════════════════════
#  EDIT LEAD
# ═══════════════════════════════════════════════════════════════


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
            if value == "" or value is None:
                setattr(lead, field, None)
            elif field == "room_count":
                try:
                    setattr(lead, field, int(value) if value else None)
                except (ValueError, TypeError):
                    pass
            else:
                setattr(lead, field, str(value))

    # Keep normalized name in sync
    if "hotel_name" in data and data["hotel_name"]:
        lead.hotel_name_normalized = normalize_hotel_name(data["hotel_name"])

    # Keep timeline_label in sync
    if "opening_date" in data:
        from app.services.utils import get_timeline_label

        lead.timeline_label = get_timeline_label(data["opening_date"] or "")

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


# ═══════════════════════════════════════════════════════════════
#  APPROVE
# ═══════════════════════════════════════════════════════════════


@router.post("/api/dashboard/leads/{lead_id}/approve", tags=["Dashboard"])
async def dashboard_approve_lead(
    lead_id: int,
    db: AsyncSession = Depends(get_db),
    _csrf=Depends(require_ajax),
):
    """Approve lead — push contacts to Insightly CRM"""
    result = await db.execute(select(PotentialLead).where(PotentialLead.id == lead_id))
    lead = result.scalar_one_or_none()

    if not lead:
        return JSONResponse(content={"detail": "Lead not found"}, status_code=404)

    # Block approve if no contacts
    contacts_result = await db.execute(
        select(LeadContact)
        .where(LeadContact.lead_id == lead_id)
        .order_by(LeadContact.score.desc())
    )
    contacts = [c.to_dict() for c in contacts_result.scalars().all()]
    if not contacts:
        return JSONResponse(
            content={"detail": "Enrich first — no contacts to push to CRM"},
            status_code=400,
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
            lead.insightly_id = successful[0][1]
            logger.info(
                f"Insightly: pushed {len(successful)} contacts for "
                f"{lead.hotel_name} -> Lead IDs: {[p[1] for p in successful]}"
            )
        else:
            logger.warning(f"Insightly: failed to push contacts for {lead.hotel_name}")

    await db.commit()
    await db.refresh(lead)

    logger.info(f"Dashboard: Approved lead {lead.hotel_name} (ID: {lead.id})")

    return {
        "status": "approved",
        "id": lead.id,
        "insightly_id": lead.insightly_id,
        "contacts_pushed": len(contacts),
    }


# ═══════════════════════════════════════════════════════════════
#  REJECT
# ═══════════════════════════════════════════════════════════════


@router.post("/api/dashboard/leads/{lead_id}/reject", tags=["Dashboard"])
async def dashboard_reject_lead(
    lead_id: int,
    reason: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
    _csrf=Depends(require_ajax),
):
    """Reject lead — remove from Insightly if previously pushed"""
    result = await db.execute(select(PotentialLead).where(PotentialLead.id == lead_id))
    lead = result.scalar_one_or_none()

    if not lead:
        return JSONResponse(content={"detail": "Lead not found"}, status_code=404)

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

    return {"status": "rejected", "id": lead.id}


# ═══════════════════════════════════════════════════════════════
#  RESTORE
# ═══════════════════════════════════════════════════════════════


@router.post("/api/dashboard/leads/{lead_id}/restore", tags=["Dashboard"])
async def dashboard_restore_lead(
    lead_id: int,
    db: AsyncSession = Depends(get_db),
    _csrf=Depends(require_ajax),
):
    """Restore rejected/deleted lead back to pipeline"""
    result = await db.execute(select(PotentialLead).where(PotentialLead.id == lead_id))
    lead = result.scalar_one_or_none()

    if not lead:
        return JSONResponse(content={"detail": "Lead not found"}, status_code=404)

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

    return {"status": "restored", "id": lead.id}


# ═══════════════════════════════════════════════════════════════
#  DELETE (soft)
# ═══════════════════════════════════════════════════════════════


@router.post("/api/dashboard/leads/{lead_id}/delete", tags=["Dashboard"])
async def dashboard_delete_lead(
    lead_id: int,
    db: AsyncSession = Depends(get_db),
    _csrf=Depends(require_ajax),
):
    """Soft-delete a lead (can be restored from Deleted tab)"""
    result = await db.execute(select(PotentialLead).where(PotentialLead.id == lead_id))
    lead = result.scalar_one_or_none()

    if not lead:
        return JSONResponse(content={"detail": "Lead not found"}, status_code=404)

    lead.status = "deleted"
    lead.updated_at = local_now()

    await db.commit()

    return {"status": "deleted", "id": lead.id}


# ═══════════════════════════════════════════════════════════════
#  SOURCES LIST
# ═══════════════════════════════════════════════════════════════


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

    cat_counts = {}
    cat_labels = {
        "chain_newsroom": "\U0001f3e8 Chain Newsrooms",
        "luxury_independent": "\U0001f48e Luxury & Independent",
        "aggregator": "\U0001f4f0 Aggregators",
        "industry": "\U0001f3d7\ufe0f Industry",
        "florida": "\U0001f334 Florida",
        "caribbean": "\U0001f3d6\ufe0f Caribbean",
        "travel_pub": "\u2708\ufe0f Travel Pubs",
        "pr_wire": "\U0001f4e1 PR Wire",
    }

    all_sources = []
    due_sources = []

    freq_hours = {
        "daily": 20,
        "every_3_days": 68,
        "twice_weekly": 96,
        "weekly": 160,
        "monthly": 720,
    }

    for src in sources:
        cat_counts[src.source_type] = cat_counts.get(src.source_type, 0) + 1

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

        freq = src.scrape_frequency or "daily"
        threshold = freq_hours.get(freq, 160)

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
