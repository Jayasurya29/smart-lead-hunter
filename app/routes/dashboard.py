"""Dashboard API routes — lead actions, sources list."""

import logging
import os
from datetime import timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models import PotentialLead, Source
from app.models.lead_contact import LeadContact
from app.services.rescore import rescore_lead
from app.services.utils import local_now, normalize_hotel_name
from app.services.audit import log_action
from app.shared import (
    require_ajax,
    checked_json,
    _get_redis,
)

# FIX M-13: Reuse schema constants instead of redefining them
from app.schemas import VALID_BRAND_TIERS, _EMAIL_RE

logger = logging.getLogger(__name__)

router = APIRouter()


# ── Shared helper: extract user email from JWT cookie for audit logs ──
def _get_user_email(request: Request) -> str:
    """Extract user email from JWT cookie without DB lookup."""
    cookie = request.cookies.get("slh_session", "")
    if not cookie:
        return "unknown"
    try:
        from jose import jwt as jose_jwt

        secret = (
            os.getenv("JWT_SECRET_KEY", "")
            or "dev-only-insecure-key-do-not-use-in-production"
        )
        payload = jose_jwt.decode(cookie, secret, algorithms=["HS256"])
        return payload.get("email", "unknown")
    except Exception:
        return "unknown"


async def _invalidate_stats_cache():
    """Delete cached dashboard stats so next poll gets fresh data."""
    r = await _get_redis()
    if r:
        try:
            await r.delete("slh:dashboard_stats")
        except Exception:
            pass


# FIX M-13: Validation config for dashboard edit — compiled once at import,
# not per-request. Field length caps in one place.
_EDIT_STRING_LIMITS = {
    "city": 100,
    "state": 100,
    "country": 100,
    "brand": 100,
    "contact_name": 200,
    "contact_title": 100,
    "contact_phone": 50,
    "management_company": 200,
    "developer": 200,
    "owner": 200,
    "opening_date": 50,
}
_EDIT_LONG_LIMITS = {"description": 5000, "notes": 5000}


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
    try:
        data = await checked_json(request)
    except HTTPException:
        raise
    except Exception:
        return JSONResponse(content={"detail": "Invalid request body"}, status_code=400)

    # ── Input validation (FIX M-13: uses shared constants from schemas.py) ──
    errors = []

    if "hotel_name" in data:
        name = str(data["hotel_name"]).strip() if data["hotel_name"] else ""
        if not name:
            errors.append("Hotel name cannot be empty")
        elif len(name) > 255:
            errors.append("Hotel name must be 255 characters or fewer")

    if "contact_email" in data and data["contact_email"]:
        email = str(data["contact_email"]).strip()
        if email and not _EMAIL_RE.match(email):
            errors.append(f"Invalid email format: {email}")

    if "room_count" in data and data["room_count"] is not None:
        try:
            rc = int(data["room_count"])
            if rc < 0:
                errors.append("Room count cannot be negative")
        except (ValueError, TypeError):
            errors.append("Room count must be a number")

    if "brand_tier" in data and data["brand_tier"]:
        if str(data["brand_tier"]).strip() not in VALID_BRAND_TIERS:
            errors.append(f"Invalid brand tier: {data['brand_tier']}")

    for field, max_len in _EDIT_STRING_LIMITS.items():
        if field in data and data[field] and len(str(data[field])) > max_len:
            errors.append(f"{field} must be {max_len} characters or fewer")

    for field, max_len in _EDIT_LONG_LIMITS.items():
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
        # Web + geo fields — added 2026-04-29 so sales can fix bad data
        # Smart Fill picked up. Without these in the whitelist the PATCH
        # silently dropped them and the wrong values stuck.
        "hotel_website",
        "address",
        "zip_code",
        "source_url",
        "latitude",
        "longitude",
    ]

    # Numeric fields that need float conversion (vs default str cast).
    _FLOAT_FIELDS = {"latitude", "longitude"}

    # A-01: Capture old values for audit trail
    old_values = {}
    for field in editable_fields:
        if field in data:
            old_val = getattr(lead, field, None)
            old_values[field] = old_val

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
            elif field in _FLOAT_FIELDS:
                # latitude / longitude — coerce to float, skip on garbage
                try:
                    setattr(lead, field, float(value))
                except (ValueError, TypeError):
                    pass
            else:
                setattr(lead, field, str(value))

    # Keep normalized name in sync
    if "hotel_name" in data and data["hotel_name"]:
        lead.hotel_name_normalized = normalize_hotel_name(data["hotel_name"])

    # Keep timeline_label in sync — and reconcile status / auto-transfer
    # when opening_date changes. Manual edits must follow the same
    # lifecycle rules as Smart Fill: a lead whose date crosses into the
    # EXPIRED bucket (past or <3mo future) belongs in existing_hotels,
    # not potential_leads. A lead whose date moves OUT of EXPIRED back
    # into an active bucket should un-expire so it reappears in Pipeline.
    opening_date_changed = "opening_date" in data
    needs_transfer = False
    if opening_date_changed:
        from app.services.utils import get_timeline_label

        new_label = get_timeline_label(data["opening_date"] or "")
        lead.timeline_label = new_label
        if new_label == "EXPIRED":
            lead.status = "expired"
            needs_transfer = True  # fire transfer_lead after commit
            logger.info(
                f"Dashboard edit auto-expired lead #{lead_id} "
                f"{lead.hotel_name!r}: opening_date={data['opening_date']!r}"
            )
        elif (
            new_label in ("URGENT", "HOT", "WARM", "COOL", "TBD")
            and lead.status == "expired"
        ):
            lead.status = "new"
            logger.info(
                f"Dashboard edit un-expired lead #{lead_id} "
                f"{lead.hotel_name!r}: opening_date={data['opening_date']!r} "
                f"({new_label} bucket)"
            )

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
        # FIX H-05: Refresh lead after rescore — lead_score/score_breakdown may be stale
        await db.refresh(lead)
        if "brand_tier" in data and data["brand_tier"]:
            auto_points = (lead.score_breakdown or {}).get("brand", {}).get("points", 0)
            manual_points = tier_points_map.get(data["brand_tier"], 0)
            lead.lead_score = lead.lead_score - auto_points + manual_points
            lead.brand_tier = data["brand_tier"]
            # Keep score_breakdown in sync with the manual override so the
            # UI "Why this score?" reflects the true tier points — not the
            # stale auto-calculated ones.
            bd = dict(lead.score_breakdown or {})
            brand_bd = dict(bd.get("brand") or {})
            brand_bd["points"] = manual_points
            brand_bd["tier"] = data["brand_tier"]
            brand_bd["manual_override"] = True
            bd["brand"] = brand_bd
            lead.score_breakdown = bd
    elif "brand_tier" in data and data["brand_tier"]:
        old_points = tier_points_map.get(lead.brand_tier or "unknown", 0)
        new_points = tier_points_map.get(data["brand_tier"], 0)
        lead.lead_score = (lead.lead_score or 0) - old_points + new_points
        lead.brand_tier = data["brand_tier"]
        # Same fix when no other scoring fields changed — still sync the
        # breakdown so the UI reflects the manual tier override.
        bd = dict(lead.score_breakdown or {})
        brand_bd = dict(bd.get("brand") or {})
        brand_bd["points"] = new_points
        brand_bd["tier"] = data["brand_tier"]
        brand_bd["manual_override"] = True
        bd["brand"] = brand_bd
        lead.score_breakdown = bd

    lead.updated_at = local_now()

    # A-01: Audit log for edits — only log fields that actually changed
    new_values = {k: data[k] for k in old_values if data.get(k) != old_values[k]}
    if new_values:
        changed_old = {k: old_values[k] for k in new_values}
        # Extract user email from JWT cookie
        user_email = "unknown"
        cookie = request.cookies.get("slh_session", "")
        if cookie:
            try:
                from jose import jwt as jose_jwt
                import os

                secret = (
                    os.getenv("JWT_SECRET_KEY", "")
                    or "dev-only-insecure-key-do-not-use-in-production"
                )
                payload = jose_jwt.decode(cookie, secret, algorithms=["HS256"])
                user_email = payload.get("email", "unknown")
            except Exception:
                pass
        await log_action(
            session=db,
            action="edit",
            lead=lead,
            user_email=user_email,
            old_values=changed_old,
            new_values=new_values,
        )

    await db.commit()
    await db.refresh(lead)

    # ─── Auto-transfer to existing_hotels if opening_date hit EXPIRED ──
    # Mirrors the Smart Fill SSE handler so manual dashboard edits
    # behave identically. transfer_lead runs in its own session because
    # it commits internally and hard-deletes the source row.
    if needs_transfer:
        try:
            from app.database import async_session
            from app.services.lead_transfer import transfer_lead

            async with async_session() as transfer_session:
                tr = await transfer_lead(lead_id, transfer_session, commit=True)
            if tr.get("status") in ("transferred", "merged"):
                eh_id = tr.get("existing_hotel_id")
                logger.info(
                    f"Dashboard edit auto-{tr['status']} lead #{lead_id} "
                    f"→ existing_hotel #{eh_id}"
                )
                return JSONResponse(
                    content={
                        "status": "transferred",
                        "id": lead_id,
                        "existing_hotel_id": eh_id,
                        "transfer_status": tr["status"],
                    }
                )
        except Exception as e:
            logger.warning(f"Dashboard edit auto-transfer failed for #{lead_id}: {e}")

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
    request: Request,
    db: AsyncSession = Depends(get_db),
    _csrf=Depends(require_ajax),
):
    """Approve lead — push contacts to Insightly CRM"""
    user_email = _get_user_email(request)
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

    old_status = lead.status
    lead.status = "approved"
    lead.updated_at = local_now()

    # Push contacts as Insightly Leads
    from app.services.insightly import get_insightly_client

    crm = get_insightly_client()
    crm_error = None
    if crm.enabled and not lead.insightly_id:
        try:
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
                lead.insightly_lead_ids = [p[1] for p in successful]
                lead.sync_error = None
                logger.info(
                    f"Insightly: pushed {len(successful)} contacts for "
                    f"{lead.hotel_name} -> Lead IDs: {[p[1] for p in successful]}"
                )
            else:
                crm_error = "CRM push returned no successful records"
                lead.sync_error = crm_error
                logger.warning(f"Insightly: push returned empty for {lead.hotel_name}")
        except Exception as e:
            crm_error = f"CRM sync failed: {str(e)[:100]}"
            lead.sync_error = crm_error
            logger.error(f"Insightly: push failed for {lead.hotel_name}: {e}")

    # Audit log
    await log_action(
        session=db,
        action="approve",
        lead=lead,
        user_email=user_email,
        old_values={"status": old_status},
        new_values={"status": "approved"},
        detail=f"Contacts: {len(contacts)}"
        + (f", CRM error: {crm_error}" if crm_error else ""),
    )

    await db.commit()
    await db.refresh(lead)
    await _invalidate_stats_cache()

    logger.info(f"Dashboard: Approved lead {lead.hotel_name} (ID: {lead.id})")

    resp = {
        "status": "approved",
        "id": lead.id,
        "insightly_id": lead.insightly_id,
        "contacts_pushed": len(contacts),
    }
    if crm_error:
        resp["crm_warning"] = crm_error
    return resp


# ═══════════════════════════════════════════════════════════════
#  REJECT
# ═══════════════════════════════════════════════════════════════


@router.post("/api/dashboard/leads/{lead_id}/reject", tags=["Dashboard"])
async def dashboard_reject_lead(
    lead_id: int,
    request: Request,
    reason: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
    _csrf=Depends(require_ajax),
):
    """Reject lead — remove from Insightly if previously pushed"""
    user_email = _get_user_email(request)
    result = await db.execute(select(PotentialLead).where(PotentialLead.id == lead_id))
    lead = result.scalar_one_or_none()

    if not lead:
        return JSONResponse(content={"detail": "Lead not found"}, status_code=404)

    old_status = lead.status
    lead.status = "rejected"
    lead.rejection_reason = reason
    lead.notes = f"{lead.notes or ''}\nRejected: {reason or 'No reason given'}".strip()
    lead.updated_at = local_now()

    # Remove from Insightly if previously pushed — use stored IDs (fast path)
    if lead.insightly_id or lead.insightly_lead_ids:
        from app.services.insightly import get_insightly_client

        crm = get_insightly_client()
        if crm.enabled:
            stored_ids = lead.insightly_lead_ids or []
            if stored_ids:
                deleted = await crm.delete_leads_by_ids(stored_ids)
            else:
                # Fallback for leads pushed before insightly_lead_ids existed
                deleted = await crm.delete_leads_by_slh_id(lead.id)
            logger.info(f"Insightly: deleted {deleted} leads for {lead.hotel_name}")
        lead.insightly_id = None
        lead.insightly_lead_ids = []

    await log_action(
        session=db,
        action="reject",
        lead=lead,
        user_email=user_email,
        old_values={"status": old_status},
        new_values={"status": "rejected", "reason": reason},
        detail=reason,
    )

    await db.commit()
    await db.refresh(lead)
    await _invalidate_stats_cache()

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
    request: Request,
    db: AsyncSession = Depends(get_db),
    _csrf=Depends(require_ajax),
):
    """Restore rejected/deleted lead back to pipeline"""
    user_email = _get_user_email(request)
    result = await db.execute(select(PotentialLead).where(PotentialLead.id == lead_id))
    lead = result.scalar_one_or_none()

    if not lead:
        return JSONResponse(content={"detail": "Lead not found"}, status_code=404)

    old_status = lead.status
    lead.status = "new"
    lead.rejection_reason = None
    lead.updated_at = local_now()

    # Remove from Insightly if previously pushed — use stored IDs (fast path)
    if lead.insightly_id or lead.insightly_lead_ids:
        from app.services.insightly import get_insightly_client

        crm = get_insightly_client()
        if crm.enabled:
            stored_ids = lead.insightly_lead_ids or []
            if stored_ids:
                deleted = await crm.delete_leads_by_ids(stored_ids)
            else:
                # Fallback for leads pushed before insightly_lead_ids existed
                deleted = await crm.delete_leads_by_slh_id(lead.id)
            logger.info(f"Insightly: deleted {deleted} leads for {lead.hotel_name}")
        lead.insightly_id = None
        lead.insightly_lead_ids = []

    await log_action(
        session=db,
        action="restore",
        lead=lead,
        user_email=user_email,
        old_values={"status": old_status},
        new_values={"status": "new"},
    )

    await db.commit()
    await db.refresh(lead)
    await _invalidate_stats_cache()

    return {"status": "restored", "id": lead.id}


# ═══════════════════════════════════════════════════════════════
#  DELETE (soft)
# ═══════════════════════════════════════════════════════════════


@router.post("/api/dashboard/leads/{lead_id}/delete", tags=["Dashboard"])
async def dashboard_delete_lead(
    lead_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    _csrf=Depends(require_ajax),
):
    """Soft-delete a lead (can be restored from Deleted tab)"""
    user_email = _get_user_email(request)
    result = await db.execute(select(PotentialLead).where(PotentialLead.id == lead_id))
    lead = result.scalar_one_or_none()

    if not lead:
        return JSONResponse(content={"detail": "Lead not found"}, status_code=404)

    await log_action(
        session=db,
        action="delete",
        lead=lead,
        user_email=user_email,
    )

    lead.status = "deleted"
    lead.updated_at = local_now()

    await db.commit()
    await _invalidate_stats_cache()

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


# ═══════════════════════════════════════════════════════════════
#  A-03: BATCH CONTACT COUNTS (eliminates N+1 queries)
# ═══════════════════════════════════════════════════════════════


@router.get("/api/dashboard/leads/contact-counts", tags=["Dashboard"])
async def batch_contact_counts(
    ids: str = Query("", description="Comma-separated lead IDs"),
    db: AsyncSession = Depends(get_db),
):
    """Return {lead_id: contact_count} for a batch of leads in one query.

    Frontend calls this once per page load with visible lead IDs,
    instead of N separate /contacts requests.

    Usage: GET /api/dashboard/leads/contact-counts?ids=1,2,3,45,67
    """
    if not ids.strip():
        return {}

    try:
        lead_ids = [int(x.strip()) for x in ids.split(",") if x.strip().isdigit()]
    except ValueError:
        return {}

    if not lead_ids or len(lead_ids) > 200:
        return {}

    from sqlalchemy import func as sqlfunc

    result = await db.execute(
        select(
            LeadContact.lead_id,
            sqlfunc.count(LeadContact.id).label("count"),
        )
        .where(LeadContact.lead_id.in_(lead_ids))
        .group_by(LeadContact.lead_id)
    )

    return {row.lead_id: row.count for row in result.all()}


# ═══════════════════════════════════════════════════════════════
#  A-01: AUDIT LOG VIEWER
# ═══════════════════════════════════════════════════════════════


@router.get("/api/dashboard/audit-log", tags=["Dashboard"])
async def get_audit_log(
    lead_id: Optional[int] = None,
    action: Optional[str] = None,
    limit: int = Query(50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    """View audit trail — filterable by lead or action type."""
    from app.models.audit_log import AuditLog

    query = select(AuditLog).order_by(AuditLog.created_at.desc())

    if lead_id:
        query = query.where(AuditLog.lead_id == lead_id)
    if action:
        query = query.where(AuditLog.action == action)

    query = query.limit(limit)
    result = await db.execute(query)
    return [row.to_dict() for row in result.scalars().all()]
