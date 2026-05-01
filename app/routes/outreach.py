"""Outreach API — generate, list, approve, reject, manual-edit, mark-sent.

Routes:
  POST   /api/outreach/generate           — start pipeline (returns id)
  GET    /api/outreach/generate-stream    — SSE progress stream variant
  GET    /api/outreach                    — paginated list with filters
  GET    /api/outreach/{id}               — single research record
  PATCH  /api/outreach/{id}               — manual edit (subject/body/etc)
  POST   /api/outreach/{id}/approve       — flip status to 'approved'
  POST   /api/outreach/{id}/reject        — flip status to 'rejected'
  POST   /api/outreach/{id}/mark-sent     — flip status to 'sent' (manual)
  POST   /api/outreach/{id}/sequence      — generate 3-touch follow-up

Phase 1 — no automated email send. The /mark-sent endpoint is what
sales clicks AFTER they've copy-pasted the email into Gmail/Outlook
and sent it themselves.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from sqlalchemy import select, func, desc
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db, async_session
from app.models.research_history import ResearchHistory
from app.models import PotentialLead
from app.models.existing_hotel import ExistingHotel
from app.models.lead_contact import LeadContact
from app.routes.auth import get_current_user

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/outreach", tags=["Outreach"])


# ─────────────────────────────────────────────────────────────────────────────
# Schemas
# ─────────────────────────────────────────────────────────────────────────────


class GenerateRequest(BaseModel):
    """Either pass a contact_id (preferred — pulls everything from DB) or
    raw fields for ad-hoc research not tied to an existing contact."""

    # DB-linked path
    contact_id: Optional[int] = None
    parent_kind: Optional[str] = None  # "lead" | "existing_hotel"
    parent_id: Optional[int] = None

    # Manual path (only used when contact_id is None)
    contact_name: Optional[str] = None
    contact_title: Optional[str] = None
    hotel_name: Optional[str] = None
    hotel_location: Optional[str] = None
    linkedin_url: Optional[str] = None
    email: Optional[str] = None


class UpdateRequest(BaseModel):
    """Manual edit before the rep sends. Only these fields are mutable
    after generation. Everything else stays as the agents produced it
    so we can audit changes vs original."""

    email_subject: Optional[str] = None
    email_body: Optional[str] = None
    linkedin_message: Optional[str] = None
    approval_notes: Optional[str] = None


class RejectRequest(BaseModel):
    feedback: Optional[str] = None


# ─────────────────────────────────────────────────────────────────────────────
# Resolver — turn a contact_id (+ parent kind/id) into the inputs the
# pipeline needs. Falls back to manual fields if contact_id is None.
# ─────────────────────────────────────────────────────────────────────────────


async def _resolve_inputs(
    db: AsyncSession, req: GenerateRequest
) -> tuple[dict, Optional[LeadContact], Optional[int], Optional[int]]:
    """Return (pipeline_inputs, contact_orm, lead_id, existing_hotel_id).

    pipeline_inputs is the kwargs dict for build_initial_state(). It
    includes BOTH the explicit contact + hotel fields AND the SLH-known
    context (brand_tier, project_type, timeline_label, room_count,
    is_client, slh_lead_score, etc.) so the v2 agents don't waste
    tokens re-deriving things SLH already knows."""

    if req.contact_id:
        contact_q = await db.execute(
            select(LeadContact).where(LeadContact.id == req.contact_id)
        )
        contact = contact_q.scalar_one_or_none()
        if not contact:
            raise HTTPException(404, f"Contact {req.contact_id} not found")

        # Resolve parent — explicit args take precedence
        lead_id = req.parent_id if req.parent_kind == "lead" else contact.lead_id
        eh_id = (
            req.parent_id
            if req.parent_kind == "existing_hotel"
            else contact.existing_hotel_id
        )

        # Build SLH context block from whichever parent applies
        hotel_name = ""
        hotel_location = ""
        slh_ctx: dict = {}
        if lead_id:
            l_q = await db.execute(
                select(PotentialLead).where(PotentialLead.id == lead_id)
            )
            lead = l_q.scalar_one_or_none()
            if lead:
                hotel_name = lead.hotel_name or ""
                parts = [lead.city, lead.state]
                hotel_location = ", ".join([p for p in parts if p])
                slh_ctx = {
                    "brand": lead.brand or "",
                    "brand_tier": lead.brand_tier or "",
                    "project_type": getattr(lead, "project_type", None) or "",
                    "timeline_label": lead.timeline_label or "",
                    "opening_date": lead.opening_date or "",
                    "room_count": lead.room_count or None,
                    "hotel_type": getattr(lead, "hotel_type", None) or "",
                    "zone": getattr(lead, "zone", None) or "",
                    "is_client": False,  # potential_leads are by definition prospects
                    "slh_lead_score": lead.lead_score or None,
                }
        elif eh_id:
            h_q = await db.execute(
                select(ExistingHotel).where(ExistingHotel.id == eh_id)
            )
            hotel = h_q.scalar_one_or_none()
            if hotel:
                hotel_name = (hotel.hotel_name or hotel.name) or ""
                parts = [hotel.city, hotel.state]
                hotel_location = ", ".join([p for p in parts if p])
                slh_ctx = {
                    "brand": hotel.brand or "",
                    "brand_tier": hotel.brand_tier or "",
                    "project_type": getattr(hotel, "project_type", None) or "",
                    "timeline_label": "",  # existing_hotels don't carry timeline
                    "opening_date": hotel.opening_date or "",
                    "room_count": hotel.room_count or None,
                    "hotel_type": getattr(hotel, "hotel_type", None) or "",
                    "zone": getattr(hotel, "zone", None) or "",
                    "is_client": bool(getattr(hotel, "is_client", False)),
                    "slh_lead_score": hotel.lead_score or None,
                }

        if not hotel_name:
            raise HTTPException(
                400, "Could not resolve hotel name from contact's parent record"
            )

        # Pull contact's priority + scope (computed by SLH's contact scorer)
        contact_priority = ""
        try:
            priority, _reason = contact._compute_priority()
            contact_priority = priority or ""
        except Exception:
            pass

        inputs = {
            "contact_name": contact.name or "",
            "contact_title": contact.title or "",
            "hotel_name": hotel_name,
            "hotel_location": hotel_location,
            "linkedin_url": contact.linkedin or "",
            "email": contact.email or "",
            "contact_priority": contact_priority,
            "contact_scope": contact.scope or "",
            **slh_ctx,
        }
        return inputs, contact, lead_id, eh_id

    # Manual path — caller provided raw fields, no SLH context
    if not req.contact_name or not req.hotel_name:
        raise HTTPException(
            400,
            "Provide either contact_id, OR contact_name + hotel_name",
        )

    inputs = {
        "contact_name": req.contact_name,
        "contact_title": req.contact_title or "",
        "hotel_name": req.hotel_name,
        "hotel_location": req.hotel_location or "",
        "linkedin_url": req.linkedin_url or "",
        "email": req.email or "",
    }
    return inputs, None, None, None


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline runner — runs LangGraph in a thread (LangGraph is sync)
# ─────────────────────────────────────────────────────────────────────────────


def _run_pipeline_blocking(inputs: dict) -> dict:
    """Synchronous pipeline invocation. Called from a thread executor."""
    from app.services.outreach.graph import build_graph, build_initial_state

    pipeline = build_graph()
    state = build_initial_state(**inputs)
    return pipeline.invoke(state)


async def _run_pipeline(inputs: dict) -> dict:
    """Async wrapper — offloads the synchronous pipeline to a worker thread."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _run_pipeline_blocking, inputs)


def _state_to_db_kwargs(
    state: dict,
    contact: Optional[LeadContact],
    lead_id: Optional[int],
    eh_id: Optional[int],
) -> dict:
    """Convert pipeline state dict to ResearchHistory column kwargs.

    Note: v2 produces richer payloads (follow_up_sequence is a list of
    dicts, fit_breakdown is a dict, etc.) that the existing DB schema
    doesn't have dedicated columns for. We flatten to strings for the
    legacy ARRAY(Text) columns and stash extras in approval_notes.
    """
    # Flatten follow_up_sequence from list-of-dicts → list-of-strings
    raw_followups = state.get("follow_up_sequence") or []
    followups_flat: list[str] = []
    for touch in raw_followups:
        if isinstance(touch, dict):
            day = touch.get("day", "?")
            t_type = touch.get("type", "")
            scheduled = touch.get("scheduled_for", "")
            headline = touch.get("headline", "")
            followups_flat.append(f"Day +{day} — {t_type} — {scheduled} — {headline}")
        elif isinstance(touch, str):
            followups_flat.append(touch)

    return {
        "lead_id": lead_id,
        "existing_hotel_id": eh_id,
        "lead_contact_id": contact.id if contact else None,
        "contact_name": state.get("contact_name") or "",
        "contact_title": state.get("contact_title") or "",
        "hotel_name": state.get("hotel_name") or "",
        "hotel_location": state.get("hotel_location") or "",
        "linkedin_url": state.get("linkedin_url") or "",
        "email": state.get("email") or "",
        "company_summary": state.get("company_summary") or "",
        "contact_summary": state.get("contact_summary") or "",
        "pain_points": state.get("pain_points") or [],
        "signals": state.get("signals") or [],
        "outreach_angle": state.get("outreach_angle") or "",
        "personalization_hook": state.get("personalization_hook") or "",
        # Use SLH-known brand_tier first, fall back to Gemini's inferred tier
        "hotel_tier": (
            state.get("brand_tier") or state.get("hotel_tier_inferred") or ""
        ),
        "hiring_signals": state.get("hiring_signals") or [],
        "recent_news": state.get("recent_news") or [],
        "fit_score": state.get("fit_score") or 0,
        "value_props": state.get("value_props") or [],
        "email_subject": state.get("email_subject") or "",
        "email_body": state.get("email_body") or "",
        "linkedin_message": state.get("linkedin_message") or "",
        "quality_approved": state.get("quality_approved") or False,
        "quality_feedback": state.get("quality_feedback") or "",
        "send_time": state.get("send_time") or "",
        "follow_up_sequence": followups_flat,
        "approval_status": "pending",
        "research_confidence": state.get("research_confidence") or None,
        "sources": state.get("sources") or [],
    }


# ─────────────────────────────────────────────────────────────────────────────
# POST /generate — non-streaming (one request, blocks until done)
# ─────────────────────────────────────────────────────────────────────────────


# ─────────────────────────────────────────────────────────────────────────────
# Picker autocomplete — search across BOTH potential_leads + existing_hotels
# so the composer's "Find hotel" search box can autocomplete from any lead.
# Then "Get contacts" loads everyone tied to that hotel so user can click
# one and auto-fill the form.
# ─────────────────────────────────────────────────────────────────────────────


@router.get("/search-hotels")
async def search_hotels(
    q: str = Query(..., min_length=2),
    limit: int = Query(20, ge=1, le=50),
    db: AsyncSession = Depends(get_db),
):
    """Autocomplete for the picker. Returns up to `limit` matches from
    BOTH potential_leads and existing_hotels, ranked by score desc.
    Each hit is tagged with `kind` so the frontend knows which detail
    endpoint to hit."""
    s = f"%{q.lower()}%"

    # Search potential_leads
    pl_q = (
        select(
            PotentialLead.id,
            PotentialLead.hotel_name,
            PotentialLead.brand,
            PotentialLead.city,
            PotentialLead.state,
            PotentialLead.country,
            PotentialLead.lead_score,
        )
        .where(
            (func.lower(PotentialLead.hotel_name).ilike(s))
            | (func.lower(PotentialLead.brand).ilike(s))
        )
        .where(PotentialLead.status.notin_(["deleted", "rejected"]))
        .order_by(PotentialLead.lead_score.desc().nullslast())
        .limit(limit)
    )
    pl_rows = (await db.execute(pl_q)).all()

    # Search existing_hotels
    eh_q = (
        select(
            ExistingHotel.id,
            ExistingHotel.hotel_name,
            ExistingHotel.name,
            ExistingHotel.brand,
            ExistingHotel.city,
            ExistingHotel.state,
            ExistingHotel.country,
            ExistingHotel.lead_score,
        )
        .where(
            (func.lower(ExistingHotel.hotel_name).ilike(s))
            | (func.lower(ExistingHotel.name).ilike(s))
            | (func.lower(ExistingHotel.brand).ilike(s))
        )
        .order_by(ExistingHotel.lead_score.desc().nullslast())
        .limit(limit)
    )
    eh_rows = (await db.execute(eh_q)).all()

    results = []
    for r in pl_rows:
        results.append(
            {
                "kind": "lead",
                "id": r.id,
                "hotel_name": r.hotel_name,
                "brand": r.brand,
                "location": ", ".join(p for p in [r.city, r.state] if p) or "—",
                "country": r.country,
                "score": r.lead_score or 0,
            }
        )
    for r in eh_rows:
        results.append(
            {
                "kind": "existing_hotel",
                "id": r.id,
                "hotel_name": r.hotel_name or r.name or "(no name)",
                "brand": r.brand,
                "location": ", ".join(p for p in [r.city, r.state] if p) or "—",
                "country": r.country,
                "score": r.lead_score or 0,
            }
        )

    # Sort combined results by score desc, cap to limit
    results.sort(key=lambda x: x["score"], reverse=True)
    return {"results": results[:limit]}


@router.get("/hotel-contacts")
async def hotel_contacts(
    kind: str = Query(..., pattern="^(lead|existing_hotel)$"),
    parent_id: int = Query(...),
    db: AsyncSession = Depends(get_db),
):
    """List contacts attached to a specific hotel (lead or existing_hotel).
    Used by the composer after the user picks a hotel — surfaces every
    contact so they can pick one click → auto-populated form.

    Also returns a hotel summary so the form can display location +
    score + brand alongside the contact list."""
    if kind == "lead":
        h_q = await db.execute(
            select(PotentialLead).where(PotentialLead.id == parent_id)
        )
        hotel = h_q.scalar_one_or_none()
        if not hotel:
            raise HTTPException(404, "Lead not found")
        hotel_dict = {
            "kind": "lead",
            "id": hotel.id,
            "hotel_name": hotel.hotel_name,
            "brand": hotel.brand,
            "city": hotel.city,
            "state": hotel.state,
            "country": hotel.country,
            "score": hotel.lead_score or 0,
        }
        c_q = await db.execute(
            select(LeadContact)
            .where(LeadContact.lead_id == parent_id)
            .order_by(
                LeadContact.is_primary.desc().nullslast(),
                LeadContact.score.desc().nullslast(),
            )
        )
    else:  # existing_hotel
        h_q = await db.execute(
            select(ExistingHotel).where(ExistingHotel.id == parent_id)
        )
        hotel = h_q.scalar_one_or_none()
        if not hotel:
            raise HTTPException(404, "Existing hotel not found")
        hotel_dict = {
            "kind": "existing_hotel",
            "id": hotel.id,
            "hotel_name": hotel.hotel_name or hotel.name,
            "brand": hotel.brand,
            "city": hotel.city,
            "state": hotel.state,
            "country": hotel.country,
            "score": hotel.lead_score or 0,
        }
        c_q = await db.execute(
            select(LeadContact)
            .where(LeadContact.existing_hotel_id == parent_id)
            .order_by(
                LeadContact.is_primary.desc().nullslast(),
                LeadContact.score.desc().nullslast(),
            )
        )

    contacts = c_q.scalars().all()
    return {
        "hotel": hotel_dict,
        "contacts": [
            {
                "id": c.id,
                "name": c.name,
                "title": c.title,
                "email": c.email,
                "phone": c.phone,
                "linkedin": c.linkedin,
                "is_primary": c.is_primary,
                "score": c.score or 0,
                "scope": c.scope,
            }
            for c in contacts
        ],
    }


@router.post("/generate")
async def generate(
    req: GenerateRequest,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    inputs, contact, lead_id, eh_id = await _resolve_inputs(db, req)
    # Sender first name comes from the logged-in user — never hardcoded.
    inputs["sender_first_name"] = current_user.get("first_name") or ""
    state = await _run_pipeline(inputs)

    record = ResearchHistory(**_state_to_db_kwargs(state, contact, lead_id, eh_id))
    db.add(record)
    await db.commit()
    await db.refresh(record)
    return record.to_dict()


# ─────────────────────────────────────────────────────────────────────────────
# GET /generate-stream — SSE: progress events while the agents run
#
# Uses the same job-with-subscribers pattern as Smart Fill so a client
# disconnect doesn't kill the run, and reconnecting clients see real
# progress instead of a stuck "Connecting..." state.
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class _OutreachJob:
    job_key: str
    inputs: dict
    contact_id: Optional[int]
    lead_id: Optional[int]
    eh_id: Optional[int]
    task: Optional[asyncio.Task]
    started_at: float
    current_event: Optional[dict] = None
    result_id: Optional[int] = None
    subscribers: set = field(default_factory=set)


# Keyed by a deterministic job_key (contact_id OR a hash of the manual
# inputs) so two browser tabs / navigate-away-and-back attach to the
# same run instead of starting duplicates.
_outreach_jobs: dict[str, _OutreachJob] = {}


def _job_key(req: GenerateRequest) -> str:
    if req.contact_id:
        return f"contact:{req.contact_id}"
    # Manual entry — hash the identifying fields
    raw = f"{req.contact_name}|{req.hotel_name}|{req.email or ''}".lower()
    return f"manual:{raw}"


async def _start_outreach_job(
    job_key: str,
    inputs: dict,
    contact: Optional[LeadContact],
    lead_id: Optional[int],
    eh_id: Optional[int],
) -> _OutreachJob:
    job = _OutreachJob(
        job_key=job_key,
        inputs=inputs,
        contact_id=contact.id if contact else None,
        lead_id=lead_id,
        eh_id=eh_id,
        task=None,
        started_at=time.monotonic(),
    )

    # Synthetic stage events — the LangGraph agents don't natively
    # emit progress, so we push markers around their invocations.
    # Real-time per-agent progress would require instrumenting
    # researcher.py etc., which is bigger surgery; for now the user
    # sees five clear stages that align with the agent boundaries.
    STAGES = [
        (1, "Researching hotel + contact (6 parallel searches)..."),
        (2, "Analyzing fit + value props..."),
        (3, "Drafting email + LinkedIn message..."),
        (4, "Quality-checking the draft..."),
        (5, "Scheduling + follow-up sequence..."),
    ]
    TOTAL = len(STAGES)

    def emit(event: dict):
        job.current_event = event
        for q in list(job.subscribers):
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                pass

    async def run():
        try:
            # Single-shot pipeline runs end-to-end; we emit synthetic
            # progress markers at expected agent boundaries. They
            # advance via timer rather than agent callbacks because
            # LangGraph doesn't expose mid-run hooks.
            emit(
                {
                    "type": "stage",
                    "stage": 1,
                    "total": TOTAL,
                    "label": STAGES[0][1],
                    "pct": 10,
                    "elapsed_s": round(time.monotonic() - job.started_at, 1),
                }
            )

            # Run the actual pipeline (blocking → thread)
            loop = asyncio.get_event_loop()

            # Heartbeat task: pulse stages 2..5 at progressive %
            async def heartbeat():
                progressions = [(2, 30), (3, 55), (4, 75), (5, 90)]
                # Wait at least 5 sec before advancing to stage 2 so
                # we don't blast all stages instantly on a fast run
                start_delay = 5.0
                step_delay = 12.0
                await asyncio.sleep(start_delay)
                for stage, pct in progressions:
                    emit(
                        {
                            "type": "stage",
                            "stage": stage,
                            "total": TOTAL,
                            "label": STAGES[stage - 1][1],
                            "pct": pct,
                            "elapsed_s": round(time.monotonic() - job.started_at, 1),
                        }
                    )
                    await asyncio.sleep(step_delay)

            heartbeat_task = asyncio.create_task(heartbeat())
            try:
                state = await loop.run_in_executor(None, _run_pipeline_blocking, inputs)
            finally:
                heartbeat_task.cancel()

            # Persist
            async with async_session() as session:
                contact_obj = None
                if job.contact_id:
                    cq = await session.execute(
                        select(LeadContact).where(LeadContact.id == job.contact_id)
                    )
                    contact_obj = cq.scalar_one_or_none()
                record = ResearchHistory(
                    **_state_to_db_kwargs(state, contact_obj, job.lead_id, job.eh_id)
                )
                session.add(record)
                await session.commit()
                await session.refresh(record)
                job.result_id = record.id
                result_dict = record.to_dict()

            duration = round(time.monotonic() - job.started_at, 1)
            emit(
                {
                    "type": "complete",
                    "pct": 100,
                    "elapsed_s": duration,
                    "research_id": job.result_id,
                    "result": result_dict,
                }
            )

        except asyncio.CancelledError:
            emit({"type": "error", "message": "Outreach generation cancelled"})
            raise
        except Exception as e:
            logger.exception(f"Outreach generation failed for {job.job_key}: {e}")
            emit({"type": "error", "message": f"Generation failed: {str(e)[:200]}"})

    job.task = asyncio.create_task(run())

    def _cleanup(_t):
        _outreach_jobs.pop(job_key, None)

    job.task.add_done_callback(_cleanup)
    _outreach_jobs[job_key] = job
    return job


@router.get("/generate-stream")
async def generate_stream(
    request: Request,
    contact_id: Optional[int] = None,
    parent_kind: Optional[str] = None,
    parent_id: Optional[int] = None,
    contact_name: Optional[str] = None,
    contact_title: Optional[str] = None,
    hotel_name: Optional[str] = None,
    hotel_location: Optional[str] = None,
    linkedin_url: Optional[str] = None,
    email: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    """SSE stream — same job-with-subscribers pattern as Smart Fill."""
    req = GenerateRequest(
        contact_id=contact_id,
        parent_kind=parent_kind,
        parent_id=parent_id,
        contact_name=contact_name,
        contact_title=contact_title,
        hotel_name=hotel_name,
        hotel_location=hotel_location,
        linkedin_url=linkedin_url,
        email=email,
    )
    key = _job_key(req)

    existing = _outreach_jobs.get(key)
    if existing is not None:
        job = existing
    else:
        inputs, contact, lead_id, eh_id = await _resolve_inputs(db, req)
        # Sender first name comes from the logged-in user — never hardcoded.
        inputs["sender_first_name"] = current_user.get("first_name") or ""
        job = await _start_outreach_job(key, inputs, contact, lead_id, eh_id)

    sub_queue: asyncio.Queue = asyncio.Queue(maxsize=100)
    job.subscribers.add(sub_queue)

    if job.current_event is not None:
        replay = dict(job.current_event)
        if "elapsed_s" in replay:
            replay["elapsed_s"] = round(time.monotonic() - job.started_at, 1)
        try:
            sub_queue.put_nowait(replay)
        except asyncio.QueueFull:
            pass

    async def event_stream():
        yield f"data: {json.dumps({'type': 'started', 'total': 5})}\n\n"
        try:
            while True:
                if await request.is_disconnected():
                    return
                try:
                    event = await asyncio.wait_for(sub_queue.get(), timeout=10.0)
                except asyncio.TimeoutError:
                    yield f"data: {json.dumps({'type': 'ping'})}\n\n"
                    if job.task and job.task.done() and sub_queue.empty():
                        return
                    continue
                yield f"data: {json.dumps(event)}\n\n"
                if event["type"] in ("complete", "error"):
                    return
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


# ─────────────────────────────────────────────────────────────────────────────
# GET / — paginated list (for the Outreach tab)
# ─────────────────────────────────────────────────────────────────────────────


@router.get("")
async def list_outreach(
    page: int = Query(1, ge=1),
    per_page: int = Query(25, ge=1, le=200),
    status: Optional[str] = Query(None, description="pending|approved|rejected|sent"),
    search: Optional[str] = Query(None),
    min_score: Optional[int] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    q = select(ResearchHistory)
    cnt_q = select(func.count(ResearchHistory.id))

    if status:
        statuses = [s.strip() for s in status.split(",")]
        q = q.where(ResearchHistory.approval_status.in_(statuses))
        cnt_q = cnt_q.where(ResearchHistory.approval_status.in_(statuses))
    if min_score is not None:
        q = q.where(ResearchHistory.fit_score >= min_score)
        cnt_q = cnt_q.where(ResearchHistory.fit_score >= min_score)
    if search:
        s = f"%{search.lower()}%"
        q = q.where(
            (func.lower(ResearchHistory.contact_name).ilike(s))
            | (func.lower(ResearchHistory.hotel_name).ilike(s))
            | (func.lower(ResearchHistory.email_subject).ilike(s))
        )
        cnt_q = cnt_q.where(
            (func.lower(ResearchHistory.contact_name).ilike(s))
            | (func.lower(ResearchHistory.hotel_name).ilike(s))
            | (func.lower(ResearchHistory.email_subject).ilike(s))
        )

    q = (
        q.order_by(desc(ResearchHistory.created_at))
        .offset((page - 1) * per_page)
        .limit(per_page)
    )

    rows = (await db.execute(q)).scalars().all()
    total = (await db.execute(cnt_q)).scalar() or 0

    return {
        "rows": [r.to_dict() for r in rows],
        "total": total,
        "page": page,
        "per_page": per_page,
        "pages": (total + per_page - 1) // per_page if total else 1,
    }


@router.get("/stats")
async def outreach_stats(db: AsyncSession = Depends(get_db)):
    """Counts by status for the Outreach tab badges."""
    rows = (
        await db.execute(
            select(
                ResearchHistory.approval_status,
                func.count(ResearchHistory.id),
            ).group_by(ResearchHistory.approval_status)
        )
    ).all()
    counts = {"pending": 0, "approved": 0, "rejected": 0, "sent": 0}
    for status, n in rows:
        counts[status] = n
    counts["total"] = sum(counts.values())
    return counts


# ─────────────────────────────────────────────────────────────────────────────
# GET /{id} / PATCH / approve / reject / mark-sent
# ─────────────────────────────────────────────────────────────────────────────


@router.get("/{research_id}")
async def get_outreach(research_id: int, db: AsyncSession = Depends(get_db)):
    rec = (
        await db.execute(
            select(ResearchHistory).where(ResearchHistory.id == research_id)
        )
    ).scalar_one_or_none()
    if not rec:
        raise HTTPException(404, "Research not found")
    return rec.to_dict()


@router.patch("/{research_id}")
async def update_outreach(
    research_id: int,
    body: UpdateRequest,
    db: AsyncSession = Depends(get_db),
):
    rec = (
        await db.execute(
            select(ResearchHistory).where(ResearchHistory.id == research_id)
        )
    ).scalar_one_or_none()
    if not rec:
        raise HTTPException(404, "Research not found")

    data = body.model_dump(exclude_unset=True)
    for field_name, val in data.items():
        if val is not None:
            setattr(rec, field_name, val)
    await db.commit()
    await db.refresh(rec)
    return rec.to_dict()


@router.post("/{research_id}/approve")
async def approve_outreach(research_id: int, db: AsyncSession = Depends(get_db)):
    rec = (
        await db.execute(
            select(ResearchHistory).where(ResearchHistory.id == research_id)
        )
    ).scalar_one_or_none()
    if not rec:
        raise HTTPException(404, "Research not found")
    rec.approval_status = "approved"
    await db.commit()
    await db.refresh(rec)
    return rec.to_dict()


@router.post("/{research_id}/reject")
async def reject_outreach(
    research_id: int,
    body: RejectRequest,
    db: AsyncSession = Depends(get_db),
):
    rec = (
        await db.execute(
            select(ResearchHistory).where(ResearchHistory.id == research_id)
        )
    ).scalar_one_or_none()
    if not rec:
        raise HTTPException(404, "Research not found")
    rec.approval_status = "rejected"
    if body.feedback:
        rec.approval_notes = body.feedback
    await db.commit()
    await db.refresh(rec)
    return rec.to_dict()


@router.post("/{research_id}/mark-sent")
async def mark_sent(research_id: int, db: AsyncSession = Depends(get_db)):
    """Phase 1: rep manually marks the email as sent after pasting it
    into Gmail/Outlook and clicking Send there. We just record the
    timestamp; we don't actually send anything from SLH."""
    from datetime import datetime, timezone

    rec = (
        await db.execute(
            select(ResearchHistory).where(ResearchHistory.id == research_id)
        )
    ).scalar_one_or_none()
    if not rec:
        raise HTTPException(404, "Research not found")
    rec.approval_status = "sent"
    rec.sent_at = datetime.now(timezone.utc)
    await db.commit()
    await db.refresh(rec)
    return rec.to_dict()


@router.post("/{research_id}/revert-to-pending")
async def revert_to_pending(research_id: int, db: AsyncSession = Depends(get_db)):
    """Move a sent / rejected / approved outreach back to Pending Review.

    Use cases:
      - rep accidentally clicked Reject and wants to reconsider
      - rep marked Sent but actually didn't send → wants to send for real now
      - manager wants to re-review a previously approved outreach

    Clears sent_at if it was set, but preserves approval_notes / rejection
    feedback so the audit trail isn't lost."""
    rec = (
        await db.execute(
            select(ResearchHistory).where(ResearchHistory.id == research_id)
        )
    ).scalar_one_or_none()
    if not rec:
        raise HTTPException(404, "Research not found")
    rec.approval_status = "pending"
    rec.sent_at = None
    await db.commit()
    await db.refresh(rec)
    return rec.to_dict()


@router.post("/{research_id}/sequence")
async def generate_sequence(research_id: int, db: AsyncSession = Depends(get_db)):
    """Generate a 3-touch follow-up sequence using Gemini.

    Ported from PitchIQ. Returns a JSON array of touches; UI shows them
    inline so sales can copy/paste each follow-up at the right time.
    """
    rec = (
        await db.execute(
            select(ResearchHistory).where(ResearchHistory.id == research_id)
        )
    ).scalar_one_or_none()
    if not rec:
        raise HTTPException(404, "Research not found")

    from app.services.outreach.config import get_llm
    from langchain_core.messages import HumanMessage
    import re

    prompt = f"""You are a B2B sales expert. Generate a 3-touch email sequence for hotel uniform sales.

Contact: {rec.contact_name} — {rec.contact_title or ''}
Hotel: {rec.hotel_name}
Original email: {rec.email_body or ''}
Pain points: {rec.pain_points or []}
Value props: {rec.value_props or []}

Return ONLY this JSON, no markdown:
{{
  "touches": [
    {{"day": 0, "type": "Intro", "subject": "{rec.email_subject or ''}", "body": "{rec.email_body or ''}"}},
    {{"day": 5, "type": "Value Hook", "subject": "...", "body": "..."}},
    {{"day": 12, "type": "Breakup", "subject": "...", "body": "..."}}
  ]
}}

Touch 1: Use the original email exactly as provided
Touch 2: Lead with a specific value prop, reference their pain point
Touch 3: Short breakup email, 3-4 lines max, create gentle urgency
"""
    response = get_llm().invoke([HumanMessage(content=prompt)])
    text = response.content.strip()
    text = re.sub(r"```json|```", "", text).strip()
    try:
        sequence = json.loads(text)
    except json.JSONDecodeError:
        raise HTTPException(500, "LLM returned invalid JSON; please retry")
    return sequence
