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
from app.services.contacts_export import build_contacts_xlsx
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


@router.get("/api/inbox-contacts/export.xlsx")
async def export_inbox_contacts_xlsx(db: AsyncSession = Depends(get_db)):
    """Download the contacts directory as a polished .xlsx (trash excluded)."""
    from fastapi.responses import StreamingResponse
    from io import BytesIO
    from datetime import datetime

    data = await build_contacts_xlsx(db)
    fname = f"contacts_{datetime.now():%Y-%m-%d}.xlsx"
    return StreamingResponse(
        BytesIO(data),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


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

    # PERF 2026-06-03: list payloads carried each contact's FULL sync_history
    # (up to 50 events × ~5,600 contacts ≈ multi-MB initial load across the
    # 12 parallel page fetches). The UI's Engagement card only shows the last
    # 3 events — trim here. The single-contact detail endpoint below still
    # returns the full history.
    for r in rows:
        h = r.get("sync_history")
        if isinstance(h, list) and len(h) > 3:
            r["sync_history"] = h[-3:]

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


class ContactUpdateBody(BaseModel):
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    display_name: Optional[str] = None
    title: Optional[str] = None
    organization: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    linkedin_url: Optional[str] = None


@router.patch("/api/inbox-contacts/{contact_id}")
async def inbox_contact_update(
    contact_id: int,
    body: ContactUpdateBody,
    db: AsyncSession = Depends(get_db),
    _csrf=Depends(require_ajax),
):
    """Edit a contact's basic fields from the UI. Only fields present in the
    body are changed; pass "" to clear one. Names/orgs/emails/phones/LinkedIn
    were previously uneditable -- enrichment mistakes were permanent."""
    from sqlalchemy import text as _sql

    allowed = {
        "first_name",
        "last_name",
        "display_name",
        "title",
        "organization",
        "email",
        "phone",
        "linkedin_url",
    }
    changes = {
        k: (v.strip() if isinstance(v, str) else v)
        for k, v in body.model_dump(exclude_unset=True).items()
        if k in allowed
    }
    # [no_null_email] never null the NOT NULL email column via an inline edit.
    # An empty email in the PATCH is dropped (no-op) rather than written as NULL
    # (which raised NotNullViolationError and 500'd the drawer).
    if "email" in changes and not (changes["email"] or "").strip():
        changes.pop("email", None)
    if not changes:
        raise HTTPException(status_code=400, detail="no editable fields provided")
    if "email" in changes and changes["email"] and "@" not in changes["email"]:
        raise HTTPException(status_code=422, detail="email must contain @")
    if (
        "linkedin_url" in changes
        and changes["linkedin_url"]
        and "linkedin.com" not in changes["linkedin_url"].lower()
    ):
        raise HTTPException(status_code=422, detail="linkedin_url must be a linkedin.com URL")

    # [patch_inbox_edit_dup_email] An email edit that collides with another
    # contact's UNIQUE email would raise a raw UniqueViolation -> ugly 500.
    # Pre-check (case-insensitive) and return a clean 409 naming the holder.
    if changes.get("email"):
        dup = (
            await db.execute(
                _sql(
                    "SELECT id, COALESCE(NULLIF(TRIM(display_name), ''), email) AS who "
                    "FROM contacts WHERE lower(email) = lower(:e) AND id <> :id LIMIT 1"
                ),
                {"e": changes["email"], "id": contact_id},
            )
        ).first()
        if dup is not None:
            raise HTTPException(
                status_code=409,
                detail=f"Another contact ({dup.who}) already uses {changes['email']}.",
            )

    sets = ", ".join(f"{k} = :{k}" for k in changes)
    res = await db.execute(
        _sql(f"UPDATE contacts SET {sets}, updated_at = NOW() WHERE id = :id RETURNING id"),
        {**{k: (v or None) for k, v in changes.items()}, "id": contact_id},
    )
    if res.first() is None:
        raise HTTPException(status_code=404, detail="Contact not found")
    await db.commit()
    row = await get_contact_by_id(db, contact_id)
    return _serialize_contact(row)


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


async def _write_coverage_edge(db, contact_id, account_type, account_id):
    """Mirror an inbox-contact <-> property match into a 'covers' affiliation
    so the coverage card fills in (parity with lead-gen). Scoped to
    source='matched': re-matching replaces only the matched edge, leaving
    enrichment/portfolio coverage intact. account_id None -> clear it.
    [patch_inbox_coverage_edge]"""
    from sqlalchemy import text as _sql

    await db.execute(
        _sql(
            "DELETE FROM contact_affiliations WHERE person_type='contact' "
            "AND person_id=:pid AND relationship='covers' "
            "AND account_type=:at AND source='matched'"
        ),
        {"pid": contact_id, "at": account_type},
    )
    if not account_id:
        return
    table = "existing_hotels" if account_type == "existing_hotel" else "potential_leads"
    nm = (
        await db.execute(_sql(f"SELECT hotel_name FROM {table} WHERE id=:id"), {"id": account_id})
    ).scalar()
    if not nm:
        return
    await db.execute(  # [patch_coverage_edge_accountid]
        _sql(
            "INSERT INTO contact_affiliations (person_type, person_id, account_type, account_id, "
            "account_name, relationship, source, confidence, notes, created_at, updated_at) "
            "VALUES ('contact', :pid, :at, :aid, :nm, 'covers', 'matched', 0.9, :notes, NOW(), NOW())"
        ),
        {
            "pid": contact_id,
            "at": account_type,
            "aid": account_id,
            "nm": nm,
            "notes": "Coverage from contact-to-property match",
        },
    )


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
    await _write_coverage_edge(
        db, contact_id, "potential_lead", body.lead_id
    )  # [patch_inbox_coverage_edge]
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
    await _write_coverage_edge(
        db, contact_id, "existing_hotel", body.hotel_id
    )  # [patch_inbox_coverage_edge]
    await db.commit()
    return _serialize_contact(result)


_AUTOLINK_SKIP_DOMAINS = {
    "gmail.com",
    "yahoo.com",
    "outlook.com",
    "hotmail.com",
    "aol.com",
    "icloud.com",
    "me.com",
    "live.com",
    "msn.com",
    "comcast.net",
    "bellsouth.net",
    "att.net",
    "marriott.com",
    "hilton.com",
    "hyatt.com",
    "ihg.com",
    "wyndham.com",
    "accor.com",
}


async def _auto_resolve_property(db, domain, org):
    """Resolve an inbox contact to ONE hotel/lead by domain then exact name.
    Returns (account_type, account_id, account_name, method) or None.
    [patch_inbox_autolink]"""
    from sqlalchemy import text as _sql
    from app.services.org_normalize import normalize_organization

    async def _single(q, params):
        rows = (await db.execute(_sql(q), params)).all()
        return rows[0] if len(rows) == 1 else None

    domain = (domain or "").strip().lower()
    if domain and domain not in _AUTOLINK_SKIP_DOMAINS:
        pat = f"%{domain}%"
        r = await _single(
            "SELECT id, hotel_name FROM existing_hotels WHERE hotel_website ILIKE :p", {"p": pat}
        )
        if r:
            return ("existing_hotel", r.id, r.hotel_name, "domain")
        r = await _single(
            "SELECT id, hotel_name FROM potential_leads WHERE hotel_website ILIKE :p AND status <> 'rejected'",
            {"p": pat},
        )
        if r:
            return ("potential_lead", r.id, r.hotel_name, "domain")

    nrm = normalize_organization(org or "")
    if nrm:
        r = await _single(
            "SELECT id, hotel_name FROM existing_hotels WHERE hotel_name_normalized = :n",
            {"n": nrm},
        )
        if r:
            return ("existing_hotel", r.id, r.hotel_name, "name")
        r = await _single(
            "SELECT id, hotel_name FROM potential_leads WHERE hotel_name_normalized = :n AND status <> 'rejected'",
            {"n": nrm},
        )
        if r:
            return ("potential_lead", r.id, r.hotel_name, "name")
    return None


@router.post("/api/contacts/{contact_id}/auto-link")
async def inbox_contact_auto_link(
    contact_id: int,
    db: AsyncSession = Depends(get_db),
    _csrf=Depends(require_ajax),
):
    """Auto-resolve an inbox contact's org/domain to a hotel/lead and link it
    (matched FK + coverage edge). No confident single match -> no change.
    [patch_inbox_autolink]"""
    c = await get_contact_by_id(db, contact_id)
    if not c:
        raise HTTPException(status_code=404, detail="Contact not found")
    email = c.get("email") or ""
    domain = email.split("@")[-1] if "@" in email else ""
    hit = await _auto_resolve_property(db, domain, c.get("organization") or "")
    if not hit:
        return {"linked": False, "note": "no confident single match"}
    account_type, account_id, account_name, method = hit
    if account_type == "existing_hotel":
        await link_to_hotel(db, contact_id, account_id)
    else:
        await link_to_lead(db, contact_id, account_id)
    await _write_coverage_edge(db, contact_id, account_type, account_id)
    await db.commit()
    return {
        "linked": True,
        "account_type": account_type,
        "account_id": account_id,
        "account_name": account_name,
        "method": method,
    }


@router.post("/api/inbox-contacts/classify")
async def inbox_contacts_manual_classify(
    _csrf=Depends(require_ajax),
):
    """Manually trigger tier-1 classification over still-uncategorized
    contacts — the "Classify now" button on the Contacts page's
    Uncategorized facet.

    Returns immediately; the task runs in the background on the SAME
    "maintenance" queue the scheduled sync->classify chain uses (a bare
    .delay() would land on the default queue the worker doesn't consume).
    """
    try:
        from app.tasks.autonomous_tasks import classify_pending_contacts

        task = classify_pending_contacts.apply_async(queue="maintenance")
        return {
            "status": "queued",
            "task_id": task.id,
            "message": "Classification queued. Counts refresh as it runs.",
        }
    except Exception as e:
        logger.error(f"Failed to queue classification: {e}", exc_info=True)
        return JSONResponse(
            status_code=502,
            content={
                "status": "error",
                "message": f"Failed to queue task: {str(e)[:200]}",
            },
        )


@router.post("/api/inbox-contacts/sync")
async def inbox_contacts_manual_sync(
    _csrf=Depends(require_ajax),
):
    """Manually trigger the Celery contact sync task.

    Returns immediately — the sync runs in the background.
    """
    try:
        from app.tasks.autonomous_tasks import sync_inbox_contacts

        # Route to the SAME queue the beat schedule uses for this task
        # (celery_app.py: sync-inbox-contacts -> queue "maintenance"). A
        # bare .delay() goes to the default "celery" queue, which the worker
        # (-Q scraping,maintenance,crm) does not consume — so the manual job
        # would sit unrun and the button would look dead.
        task = sync_inbox_contacts.apply_async(queue="maintenance")
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
