"""Inbox Contact Sync — API routes (Phase 3).

Thin wrappers around app.services.contact_dedup for the `contacts` table
populated by Gmail signature extraction (inbox_sync / backfill_contacts).

Endpoints:
  GET    /api/inbox-contacts              — list with filters + pagination
  GET    /api/inbox-contacts/stats        — header bar counts
  GET    /api/inbox-contacts/{id}         — single contact detail
  POST   /api/inbox-contacts/{id}/approve — change status → approved
  DELETE /api/inbox-contacts/{id}         — hard delete (reject)
  POST   /api/inbox-contacts/{id}/push-to-insightly — push to CRM + stamp
  POST   /api/inbox-contacts/{id}/match-lead  — link to potential_lead
  POST   /api/inbox-contacts/{id}/match-hotel — link to existing_hotel
  POST   /api/inbox-contacts/sync         — manual trigger Celery task
  POST   /api/inbox-contacts/bulk-approve — approve multiple contacts at once

NOTE: Route prefix is /api/inbox-contacts (not /api/contacts) to avoid
collision with the existing contacts.py routes which serve lead_contacts
enrichment at /api/dashboard/leads/{id}/enrich*.
"""

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.shared import require_ajax
from app.services.contact_dedup import (
    get_contact_by_id,
    get_contact_stats,
    list_contacts,
    update_approval_status,
    delete_contact,
    mark_pushed_to_insightly,
    link_to_lead,
    link_to_hotel,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Inbox Contacts"])


# ──────────────────────────────────────────────────────────────────────
# Helpers — serialise datetime/date/array fields for JSON
# ──────────────────────────────────────────────────────────────────────


def _serialize_contact(row: dict) -> dict:
    """Ensure all values in a contact dict are JSON-safe."""
    out = {}
    for k, v in row.items():
        if hasattr(v, "isoformat"):
            out[k] = v.isoformat()
        elif isinstance(v, list):
            out[k] = v
        else:
            out[k] = v
    return out


# ──────────────────────────────────────────────────────────────────────
# Request bodies (Pydantic models)
# ──────────────────────────────────────────────────────────────────────


class MatchLeadBody(BaseModel):
    lead_id: Optional[int] = None


class MatchHotelBody(BaseModel):
    hotel_id: Optional[int] = None


class PushToInsightlyBody(BaseModel):
    insightly_contact_id: str


class BulkApproveBody(BaseModel):
    ids: list[int]


# ──────────────────────────────────────────────────────────────────────
# READ endpoints
# ──────────────────────────────────────────────────────────────────────


@router.get("/api/inbox-contacts/stats")
async def inbox_contacts_stats(db: AsyncSession = Depends(get_db)):
    """Header bar counts — total, P1-P4, pending/approved/pushed, etc."""
    stats = await get_contact_stats(db)
    return stats


@router.get("/api/inbox-contacts")
async def inbox_contacts_list(
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=500),
    procurement_priority: Optional[str] = None,
    contact_category: Optional[str] = None,
    approval_status: Optional[str] = None,
    brand_tier: Optional[str] = None,
    gpo: Optional[str] = None,
    source_mailbox: Optional[str] = None,
    has_signature: Optional[bool] = None,
    organization: Optional[str] = None,
    search: Optional[str] = None,
    matched_only: Optional[bool] = None,
    order_by: Optional[str] = "priority_score",
    db: AsyncSession = Depends(get_db),
):
    """List contacts with filters and pagination.

    Returns: { items: [...], total: N, page: N, per_page: N, pages: N }
    """
    limit = per_page
    offset = (page - 1) * per_page

    try:
        rows, total = await list_contacts(
            db,
            procurement_priority=procurement_priority,
            contact_category=contact_category,
            approval_status=approval_status,
            brand_tier=brand_tier,
            gpo=gpo,
            source_mailbox=source_mailbox,
            has_signature=has_signature,
            organization=organization,
            search=search,
            matched_only=matched_only,
            limit=limit,
            offset=offset,
            order_by=order_by or "priority_score",
        )
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))

    pages = max(1, (total + per_page - 1) // per_page)
    return {
        "items": [_serialize_contact(r) for r in rows],
        "total": total,
        "page": page,
        "per_page": per_page,
        "pages": pages,
    }


@router.get("/api/inbox-contacts/{contact_id}")
async def inbox_contact_detail(
    contact_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Single contact detail by id."""
    row = await get_contact_by_id(db, contact_id)
    if not row:
        raise HTTPException(status_code=404, detail="Contact not found")
    return _serialize_contact(row)


# ──────────────────────────────────────────────────────────────────────
# WRITE endpoints (CSRF-protected)
# ──────────────────────────────────────────────────────────────────────


@router.post("/api/inbox-contacts/{contact_id}/approve")
async def inbox_contact_approve(
    contact_id: int,
    db: AsyncSession = Depends(get_db),
    _csrf=Depends(require_ajax),
):
    """Approve a pending contact."""
    try:
        result = await update_approval_status(db, contact_id, "approved")
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    if not result:
        raise HTTPException(status_code=404, detail="Contact not found")
    await db.commit()
    return _serialize_contact(result)


@router.post("/api/inbox-contacts/bulk-approve")
async def inbox_contacts_bulk_approve(
    body: BulkApproveBody,
    db: AsyncSession = Depends(get_db),
    _csrf=Depends(require_ajax),
):
    """Approve multiple contacts at once."""
    approved = []
    failed = []
    for cid in body.ids:
        try:
            result = await update_approval_status(db, cid, "approved")
            if result:
                approved.append(cid)
            else:
                failed.append({"id": cid, "reason": "not_found"})
        except Exception as e:
            failed.append({"id": cid, "reason": str(e)[:100]})
    await db.commit()
    return {"approved": len(approved), "failed": failed}


@router.delete("/api/inbox-contacts/{contact_id}")
async def inbox_contact_delete(
    contact_id: int,
    db: AsyncSession = Depends(get_db),
    _csrf=Depends(require_ajax),
):
    """Hard delete a contact (Reject button)."""
    deleted = await delete_contact(db, contact_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Contact not found")
    await db.commit()
    return {"deleted": True, "id": contact_id}


@router.post("/api/inbox-contacts/{contact_id}/push-to-insightly")
async def inbox_contact_push_to_insightly(
    contact_id: int,
    body: PushToInsightlyBody,
    db: AsyncSession = Depends(get_db),
    _csrf=Depends(require_ajax),
):
    """Push an approved contact to Insightly CRM and stamp."""
    # Verify contact exists and is approved
    existing = await get_contact_by_id(db, contact_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Contact not found")
    if existing["approval_status"] not in ("approved", "pushed_to_insightly"):
        raise HTTPException(
            status_code=422,
            detail=f"Contact must be approved first (current: {existing['approval_status']})",
        )

    result = await mark_pushed_to_insightly(db, contact_id, body.insightly_contact_id)
    if not result:
        raise HTTPException(status_code=404, detail="Contact not found after update")
    await db.commit()
    return _serialize_contact(result)


@router.post("/api/inbox-contacts/{contact_id}/match-lead")
async def inbox_contact_match_lead(
    contact_id: int,
    body: MatchLeadBody,
    db: AsyncSession = Depends(get_db),
    _csrf=Depends(require_ajax),
):
    """Link (or unlink) a contact to a potential_lead."""
    result = await link_to_lead(db, contact_id, body.lead_id)
    if not result:
        raise HTTPException(status_code=404, detail="Contact not found")
    await db.commit()
    return _serialize_contact(result)


@router.post("/api/inbox-contacts/{contact_id}/match-hotel")
async def inbox_contact_match_hotel(
    contact_id: int,
    body: MatchHotelBody,
    db: AsyncSession = Depends(get_db),
    _csrf=Depends(require_ajax),
):
    """Link (or unlink) a contact to an existing_hotel."""
    result = await link_to_hotel(db, contact_id, body.hotel_id)
    if not result:
        raise HTTPException(status_code=404, detail="Contact not found")
    await db.commit()
    return _serialize_contact(result)


@router.post("/api/inbox-contacts/sync")
async def inbox_contacts_manual_sync(
    _csrf=Depends(require_ajax),
):
    """Manually trigger the Celery contact sync task.

    Returns immediately — the sync runs in the background.
    """
    try:
        from app.tasks.autonomous_tasks import sync_inbox_contacts

        task = sync_inbox_contacts.delay()
        return {
            "status": "queued",
            "task_id": task.id,
            "message": "Contact sync task queued. Check Celery logs for progress.",
        }
    except Exception as e:
        logger.error(f"Failed to queue contact sync: {e}", exc_info=True)
        return JSONResponse(
            status_code=502,
            content={
                "status": "error",
                "message": f"Failed to queue task: {str(e)[:200]}",
            },
        )
