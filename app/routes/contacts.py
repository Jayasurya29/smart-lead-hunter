"""Contact management and lead enrichment endpoints.

FIX: All contact CRUD endpoints now use Depends(get_db) for proper lifecycle.
FIX: Enrichment errors return proper HTTP status codes (not 200).
FIX: Removed phantom contact_linkedin reference.
NOTE: enrich_lead() intentionally uses manual sessions because the enrichment
      network calls take 10-30s — we don't want to hold a DB connection that long.
"""

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass, field

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from sqlalchemy import case, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import get_db, async_session
from app.models import PotentialLead
from app.models.lead_contact import LeadContact
from app.services.rescore import rescore_lead
from app.services.utils import local_now, normalize_hotel_name
from app.shared import require_ajax

logger = logging.getLogger(__name__)

router = APIRouter()


# ═══════════════════════════════════════════════════════════════
# ENRICHMENT
# ═══════════════════════════════════════════════════════════════


@router.post("/api/dashboard/leads/{lead_id}/enrich", tags=["Dashboard"])
async def enrich_lead(lead_id: int, _csrf=Depends(require_ajax)):
    """Enrich a lead with contact information via web search."""
    # Session 1: Read lead data (extract to local vars, then close)
    async with async_session() as session:
        result = await session.execute(
            select(PotentialLead).where(PotentialLead.id == lead_id)
        )
        lead = result.scalar_one_or_none()
        if not lead:
            raise HTTPException(status_code=404, detail="Lead not found")

        hotel_name = lead.hotel_name
        brand = lead.brand or ""
        city = lead.city or ""
        state = lead.state or ""
        country = lead.country or "USA"
        management_company = lead.management_company or ""
        opening_date = lead.opening_date or ""
        timeline_label = lead.timeline_label or ""
        description = lead.description or ""
        project_type_str = lead.hotel_type or ""
        search_name = getattr(lead, "search_name", None) or ""
        former_names = getattr(lead, "former_names", None) or []
        # FIX C-04: Capture updated_at for optimistic lock check
        lead_updated_at = lead.updated_at

    # Run enrichment (network calls — can take 10-30 seconds)
    try:
        from app.services.contact_enrichment import enrich_lead_contacts

        enrichment_result = await enrich_lead_contacts(
            lead_id=lead_id,
            hotel_name=hotel_name,
            brand=brand,
            city=city,
            state=state,
            country=country,
            management_company=management_company,
            opening_date=opening_date,
            timeline_label=timeline_label,
            description=description,
            project_type_str=project_type_str,
            search_name=search_name,
            former_names=former_names,
        )
    except Exception as e:
        logger.error(f"Enrichment failed for lead {lead_id}: {e}", exc_info=True)
        return JSONResponse(
            content={
                "status": "error",
                "message": f"Enrichment failed: {str(e)[:200]}",
            },
            status_code=502,
        )

    # Phase B: if researcher flagged this lead as residences_only (or similar),
    # mark it as rejected in the DB so it doesn't re-trigger enrichment
    if enrichment_result.should_reject:
        try:
            async with async_session() as session:
                lead_result = await session.execute(
                    select(PotentialLead).where(PotentialLead.id == lead_id)
                )
                lead = lead_result.scalar_one_or_none()
                if lead:
                    lead.status = "rejected"
                    lead.rejection_reason = (
                        enrichment_result.rejection_reason or "auto_reject"
                    )[:100]  # VARCHAR(100) guard
                    await session.commit()
                    logger.info(
                        f"Lead {lead_id} auto-rejected: "
                        f"{enrichment_result.rejection_reason}"
                    )
            return JSONResponse(
                content={
                    "status": "rejected",
                    "message": f"Lead auto-rejected: {enrichment_result.rejection_reason}",
                    "reason": enrichment_result.rejection_reason,
                },
                status_code=200,
            )
        except Exception as e:
            logger.error(
                f"Failed to mark lead {lead_id} as rejected: {e}", exc_info=True
            )
            # Fall through — still return the empty contacts result rather than 500

    # Session 2: Save results (with optimistic lock check)
    try:
        async with async_session() as session:
            lead_result = await session.execute(
                select(PotentialLead).where(PotentialLead.id == lead_id)
            )
            lead = lead_result.scalar_one_or_none()
            if not lead:
                return JSONResponse(
                    content={
                        "status": "error",
                        "message": "Lead not found after enrichment",
                    },
                    status_code=404,
                )

            # FIX C-04: Optimistic lock — warn if lead was edited during enrichment
            # Only fill empty fields (never overwrite), so concurrent edits are safe
            # for fields that were already populated. Log a warning for awareness.
            if (
                lead_updated_at
                and lead.updated_at
                and lead.updated_at > lead_updated_at
            ):
                logger.warning(
                    f"Lead {lead_id} was modified during enrichment "
                    f"(before={lead_updated_at}, now={lead.updated_at}). "
                    f"Only filling empty fields to avoid overwriting edits."
                )

            updated_fields = []

            if enrichment_result.management_company and not lead.management_company:
                lead.management_company = enrichment_result.management_company
                updated_fields.append("management_company")
            if enrichment_result.developer and not lead.developer:
                lead.developer = enrichment_result.developer
                updated_fields.append("developer")
            if getattr(enrichment_result, "owner", None) and not lead.owner:
                lead.owner = enrichment_result.owner
                updated_fields.append("owner")

            if enrichment_result.best_contact:
                bc = enrichment_result.best_contact
                if bc.get("name") and not lead.contact_name:
                    lead.contact_name = bc["name"]
                    updated_fields.append("contact_name")
                if bc.get("title") and not lead.contact_title:
                    lead.contact_title = bc["title"]
                    updated_fields.append("contact_title")
                if bc.get("email") and not lead.contact_email:
                    lead.contact_email = bc["email"]
                    updated_fields.append("contact_email")
                if bc.get("phone") and not lead.contact_phone:
                    lead.contact_phone = bc["phone"]
                    updated_fields.append("contact_phone")

            lead.updated_at = local_now()

            # Rescore with new contact data
            await rescore_lead(lead_id, session)

            save_result = {
                "status": "enriched" if updated_fields else "no_new_data",
                "updated_fields": updated_fields,
            }

            # Save contacts to lead_contacts table
            if enrichment_result.contacts:
                existing_contacts = await session.execute(
                    select(LeadContact).where(LeadContact.lead_id == lead_id)
                )
                existing_names = {
                    normalize_hotel_name(c.name)
                    for c in existing_contacts.scalars().all()
                }

                for i, c in enumerate(enrichment_result.contacts):
                    name = c.get("name", "").strip()
                    if not name:
                        continue

                    normalized_name = normalize_hotel_name(name)
                    if normalized_name in existing_names:
                        # Update existing contact with new data
                        ec_result = await session.execute(
                            select(LeadContact).where(
                                LeadContact.lead_id == lead_id,
                                LeadContact.name == name,
                            )
                        )
                        ec = ec_result.scalar_one_or_none()
                        if ec:
                            filled = []
                            if not ec.email and c.get("email"):
                                ec.email = c["email"]
                                filled.append("email")
                            if not ec.phone and c.get("phone"):
                                ec.phone = c["phone"]
                                filled.append("phone")
                            if not ec.linkedin and c.get("linkedin"):
                                ec.linkedin = c["linkedin"]
                                filled.append("linkedin")
                            if not ec.title and c.get("title"):
                                ec.title = c["title"]
                                filled.append("title")
                            if not ec.organization and c.get("organization"):
                                ec.organization = c["organization"]
                                filled.append("organization")
                            if not ec.evidence_url and c.get("source"):
                                ec.evidence_url = c["source"]
                                filled.append("evidence_url")
                            # Strategist verdict always refreshes (not fill-empty)
                            if c.get("_final_priority"):
                                if ec.strategist_priority != c["_final_priority"]:
                                    filled.append("strategist_priority")
                                ec.strategist_priority = c["_final_priority"]
                            if c.get("_final_reasoning"):
                                ec.strategist_reasoning = c["_final_reasoning"]
                            # ── Always refresh classification fields on
                            #    re-enrichment (bug fix 2026-04-22). Previously
                            #    these stayed frozen from the first insert —
                            #    so Elie Khoury kept score=5 from April 16
                            #    even after today's pipeline scored her P1
                            #    (floor=28). Mismatch between strategist_priority
                            #    (refreshed) and score (stuck) made the UI
                            #    show "P1 / 5 LOW" instead of "P1 / 28 HIGH".
                            new_score = c.get("_validation_score")
                            if new_score is not None and new_score != ec.score:
                                filled.append(f"score({ec.score}->{new_score})")
                                ec.score = new_score
                            new_tier = c.get("_buyer_tier")
                            if new_tier and new_tier != ec.tier:
                                filled.append("tier")
                                ec.tier = new_tier
                            new_confidence = c.get("_validation_confidence") or c.get(
                                "confidence"
                            )
                            if new_confidence and new_confidence != ec.confidence:
                                filled.append("confidence")
                                ec.confidence = new_confidence
                            # Scope may shift if Iter 6 or verifier
                            # reclassified (e.g. chain_area -> owner)
                            new_scope = c.get("scope")
                            if new_scope and new_scope != ec.scope:
                                filled.append(f"scope({ec.scope}->{new_scope})")
                                ec.scope = new_scope
                            # Merge new evidence items (dedupe by URL)
                            new_evidence = c.get("_evidence_items") or []
                            if new_evidence:
                                existing_ev = ec.evidence or []
                                existing_urls = {
                                    e.get("source_url")
                                    for e in existing_ev
                                    if isinstance(e, dict)
                                }
                                added = 0
                                for ev in new_evidence:
                                    if ev.get("source_url") not in existing_urls:
                                        existing_ev.append(ev)
                                        existing_urls.add(ev.get("source_url"))
                                        added += 1
                                if added:
                                    try:
                                        from app.services.source_tier import (
                                            trust_score as _ts,
                                        )

                                        existing_ev.sort(
                                            key=lambda e: (
                                                -_ts(e.get("trust_tier", "unknown")),
                                                -(e.get("source_year") or 0),
                                            )
                                        )
                                    except Exception:
                                        pass
                                    ec.evidence = existing_ev[:8]
                                    filled.append(f"evidence(+{added})")
                            ec.last_enriched_at = local_now()
                            # source_detail refreshes when new evidence arrives
                            new_detail = c.get("source_detail")
                            if new_detail and new_detail != ec.source_detail:
                                ec.source_detail = new_detail
                                filled.append("source_detail")
                            if filled:
                                logger.info(
                                    f"Updated {ec.name}: filled {', '.join(filled)}"
                                )
                        continue

                    contact = LeadContact(
                        lead_id=lead_id,
                        name=c["name"],
                        title=c.get("title"),
                        email=c.get("email"),
                        phone=c.get("phone"),
                        linkedin=c.get("linkedin"),
                        organization=c.get("organization"),
                        scope=c.get("scope", "unknown"),
                        confidence=c.get(
                            "_validation_confidence", c.get("confidence", "medium")
                        ),
                        tier=c.get("_buyer_tier"),
                        score=c.get("_validation_score", 0),
                        # Unified scoring breakdown (migration 013)
                        score_breakdown=c.get("_score_breakdown"),
                        # Evidence array captured during snippet extraction (migration 014).
                        # Without this line, new contacts insert with evidence=NULL even
                        # though the capture ran — which is what happened on Hyatt Centric
                        # run: the 4 survivors showed Ev=0 in the DB despite [EVIDENCE]
                        # log lines showing items captured.
                        evidence=c.get("_evidence_items") or None,
                        # Iter 6 strategist verdict — the authoritative priority
                        strategist_priority=c.get("_final_priority"),
                        strategist_reasoning=c.get("_final_reasoning"),
                        is_primary=(i == 0),
                        found_via=", ".join(enrichment_result.layers_tried)
                        if enrichment_result.layers_tried
                        else "web_search",
                        source_detail=c.get(
                            "source_detail",
                            c.get("confidence_note", c.get("_validation_reason", "")),
                        ),
                        evidence_url=c.get("source"),
                        last_enriched_at=local_now(),
                    )
                    session.add(contact)

            await session.commit()

        return {
            "status": save_result["status"],
            "lead_id": lead_id,
            "hotel_name": hotel_name,
            "contacts_found": len(enrichment_result.contacts),
            "best_contact": enrichment_result.best_contact,
            "management_company": enrichment_result.management_company,
            "developer": enrichment_result.developer,
            "layers_tried": enrichment_result.layers_tried,
            "sources_used": enrichment_result.sources_used,
            "updated_fields": save_result.get("updated_fields", []),
            "errors": enrichment_result.errors,
        }

    except Exception as e:
        logger.error(
            f"Failed to save enrichment for lead {lead_id}: {e}", exc_info=True
        )
        return JSONResponse(
            content={"status": "error", "message": f"Failed to save: {str(e)[:200]}"},
            status_code=500,
        )


# ═══════════════════════════════════════════════════════════════
# CONTACT MANAGEMENT
# ═══════════════════════════════════════════════════════════════


@router.get("/api/dashboard/leads/{lead_id}/contacts")
async def list_contacts(lead_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(LeadContact)
        .where(LeadContact.lead_id == lead_id)
        .order_by(
            LeadContact.is_primary.desc(),
            # Strategist priority (P1/P2/P3/P4) takes precedence when set.
            # Postgres sorts NULL last by default — contacts without a
            # strategist verdict fall to the bottom of their is_primary group.
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

    # Re-sort using the computed priority_label (P1 → P4) so the sales team
    # always sees the highest-priority contacts first. Falls back to score
    # within the same priority bucket.
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


@router.post("/api/dashboard/leads/{lead_id}/contacts/{contact_id}/save")
async def save_contact(
    lead_id: int,
    contact_id: int,
    db: AsyncSession = Depends(get_db),
    _csrf=Depends(require_ajax),
):
    result = await db.execute(
        select(LeadContact).where(
            LeadContact.id == contact_id, LeadContact.lead_id == lead_id
        )
    )
    contact = result.scalar_one_or_none()
    if not contact:
        raise HTTPException(status_code=404, detail="Contact not found")
    contact.is_saved = True
    contact.updated_at = local_now()

    # NOTE: Wiza email enrichment is intentionally NOT auto-triggered on Save.
    # Wiza costs 2 credits per found email — we only spend those when the user
    # explicitly clicks the "Find Email" button for a specific contact they
    # actually plan to outreach. Saving a contact should be free.

    await db.commit()
    return {
        "status": "saved",
        "contact_id": contact_id,
    }


@router.post("/api/dashboard/leads/{lead_id}/contacts/{contact_id}/unsave")
async def unsave_contact(
    lead_id: int,
    contact_id: int,
    db: AsyncSession = Depends(get_db),
    _csrf=Depends(require_ajax),
):
    result = await db.execute(
        select(LeadContact).where(
            LeadContact.id == contact_id, LeadContact.lead_id == lead_id
        )
    )
    contact = result.scalar_one_or_none()
    if not contact:
        raise HTTPException(status_code=404, detail="Contact not found")
    contact.is_saved = False
    contact.updated_at = local_now()
    await db.commit()
    return {"status": "unsaved", "contact_id": contact_id}


@router.delete("/api/dashboard/leads/{lead_id}/contacts/{contact_id}")
async def delete_contact(
    lead_id: int,
    contact_id: int,
    db: AsyncSession = Depends(get_db),
    _csrf=Depends(require_ajax),
):
    result = await db.execute(
        select(LeadContact).where(
            LeadContact.id == contact_id, LeadContact.lead_id == lead_id
        )
    )
    contact = result.scalar_one_or_none()
    if not contact:
        raise HTTPException(status_code=404, detail="Contact not found")
    await db.delete(contact)
    # Flush the delete so rescore sees the updated contact count
    await db.flush()
    try:
        await rescore_lead(lead_id, db)
    except Exception:
        pass
    await db.commit()
    return {"status": "deleted", "contact_id": contact_id}


@router.patch("/api/dashboard/leads/{lead_id}/contacts/{contact_id}")
async def update_contact(
    lead_id: int,
    contact_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    _csrf=Depends(require_ajax),
):
    body = await request.json()
    result = await db.execute(
        select(LeadContact).where(
            LeadContact.id == contact_id, LeadContact.lead_id == lead_id
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
    # Sync primary contact changes back to lead record
    if contact.is_primary:
        lead_result = await db.execute(
            select(PotentialLead).where(PotentialLead.id == lead_id)
        )
        lead = lead_result.scalar_one_or_none()
        if lead:
            lead.contact_name = contact.name
            lead.contact_title = contact.title
            lead.contact_email = contact.email
            lead.contact_phone = contact.phone
            lead.updated_at = local_now()

    # Rescore contact via unified scoring module (single source of truth).
    # The edit may have changed title AND/OR scope implicitly (e.g. setting
    # a title that clearly puts them in a different tier). We always
    # rescore using current scope + current strategist_priority so the
    # priority floor is respected — editing a P1 contact's email should
    # NEVER drop their score to 5.
    from app.services.contact_scoring import apply_score_to_contact

    apply_score_to_contact(
        contact,
        title=contact.title,
        scope=contact.scope,
        strategist_priority=contact.strategist_priority,
    )
    contact.updated_at = local_now()
    await db.flush()
    try:
        await rescore_lead(lead_id, db)
    except Exception:
        pass
    await db.commit()
    return {"status": "updated", "contact_id": contact_id, "score": contact.score}


@router.post("/api/dashboard/leads/{lead_id}/contacts/{contact_id}/toggle-scope")
async def toggle_contact_scope(
    lead_id: int,
    contact_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    _csrf=Depends(require_ajax),
):
    body = await request.json()
    new_scope = body.get("scope", "")
    # All scope values the enrichment pipeline + Iter 6 verifier can produce.
    # Previously only accepted {hotel_specific, chain_area, chain_corporate},
    # rejecting management_corporate (added for operator corporate like
    # Crescent/Aimbridge/Highgate) and owner (added for check-writers
    # like Dr. Chaudhuri/KPC Development). Sales flipping a contact to
    # these scopes via UI would get 400 errors.
    _VALID_SCOPES = (
        "hotel_specific",
        "chain_area",
        "management_corporate",
        "chain_corporate",
        "owner",
    )
    if new_scope not in _VALID_SCOPES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid scope. Must be one of: {', '.join(_VALID_SCOPES)}",
        )
    result = await db.execute(
        select(LeadContact).where(
            LeadContact.id == contact_id, LeadContact.lead_id == lead_id
        )
    )
    contact = result.scalar_one_or_none()
    if not contact:
        raise HTTPException(status_code=404, detail="Contact not found")
    contact.scope = new_scope

    # Rescore via unified scoring module (single source of truth).
    # Previously had a broken tier check — hardcoded "TIER1_HSKP" /
    # "TIER2_PURCH" strings that never matched the real enum values
    # ("TIER1_UNIFORM_DIRECT" / "TIER2_PURCHASING"), meaning scope toggles
    # on tier-1/2 contacts silently did nothing. Now it always rescores.
    from app.services.contact_scoring import apply_score_to_contact

    apply_score_to_contact(
        contact,
        title=contact.title,
        scope=new_scope,
        strategist_priority=contact.strategist_priority,
    )
    contact.updated_at = local_now()
    await db.flush()
    try:
        await rescore_lead(lead_id, db)
    except Exception:
        pass
    await db.commit()
    return {"status": "updated", "scope": new_scope, "score": contact.score}


@router.post("/api/dashboard/leads/{lead_id}/contacts/add")
async def add_contact(
    lead_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    _csrf=Depends(require_ajax),
):
    """Manually add a contact to a lead."""
    body = await request.json()
    name = (body.get("name") or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="Contact name is required")

    # Check lead exists
    lead_result = await db.execute(
        select(PotentialLead).where(PotentialLead.id == lead_id)
    )
    lead = lead_result.scalar_one_or_none()
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")

    # Score the contact via unified scoring module (single source of truth)
    from app.services.contact_scoring import score_contact

    title = (body.get("title") or "").strip()
    scope = body.get("scope", "hotel_specific")
    _VALID_SCOPES = (
        "hotel_specific",
        "chain_area",
        "management_corporate",
        "chain_corporate",
        "owner",
        "unknown",
    )
    if scope not in _VALID_SCOPES:
        scope = "unknown"

    score_result = score_contact(
        title=title,
        scope=scope,
        strategist_priority=None,  # Manual add — no strategist verdict yet
    )

    contact = LeadContact(
        lead_id=lead_id,
        name=name,
        title=title or None,
        email=(body.get("email") or "").strip() or None,
        phone=(body.get("phone") or "").strip() or None,
        linkedin=(body.get("linkedin") or "").strip() or None,
        organization=(body.get("organization") or "").strip() or None,
        scope=scope,
        confidence=score_result["confidence"],
        tier=score_result["tier"],
        score=score_result["score"],
        score_breakdown=score_result["breakdown"],
        is_primary=False,
        is_saved=True,
        found_via="manual",
        source_detail="Manually added",
        evidence_url=(body.get("evidence_url") or "").strip() or None,
        last_enriched_at=local_now(),
    )
    db.add(contact)

    # Update lead primary contact if none exists
    if not lead.contact_name:
        lead.contact_name = name
        lead.contact_title = title or None
        lead.contact_email = (body.get("email") or "").strip() or None
        lead.contact_phone = (body.get("phone") or "").strip() or None

    await db.flush()
    try:
        await rescore_lead(lead_id, db)
    except Exception:
        pass
    await db.commit()
    return {"status": "created", "contact_id": contact.id, "score": contact.score}


@router.post("/api/dashboard/leads/{lead_id}/contacts/{contact_id}/set-primary")
async def set_primary_contact(
    lead_id: int,
    contact_id: int,
    db: AsyncSession = Depends(get_db),
    _csrf=Depends(require_ajax),
):
    await db.execute(
        update(LeadContact)
        .where(LeadContact.lead_id == lead_id)
        .values(is_primary=False)
    )
    result = await db.execute(
        select(LeadContact).where(
            LeadContact.id == contact_id, LeadContact.lead_id == lead_id
        )
    )
    contact = result.scalar_one_or_none()
    if not contact:
        raise HTTPException(status_code=404, detail="Contact not found")
    contact.is_primary = True
    contact.is_saved = True
    contact.updated_at = local_now()
    lead_result = await db.execute(
        select(PotentialLead).where(PotentialLead.id == lead_id)
    )
    lead = lead_result.scalar_one_or_none()
    if lead:
        lead.contact_name = contact.name
        lead.contact_title = contact.title
        lead.contact_email = contact.email
        lead.contact_phone = contact.phone
        # NOTE: contact_linkedin column does not exist on PotentialLead.
        # LinkedIn is stored on the LeadContact record only.
        lead.updated_at = local_now()
    await db.commit()
    return {"status": "primary_set", "contact_id": contact_id}


# ═══════════════════════════════════════════════════════════════
# WIZA EMAIL ENRICHMENT
# ═══════════════════════════════════════════════════════════════


@router.post("/api/dashboard/leads/{lead_id}/contacts/{contact_id}/enrich-email")
async def enrich_contact_email_route(
    lead_id: int,
    contact_id: int,
    db: AsyncSession = Depends(get_db),
    _csrf=Depends(require_ajax),
):
    """
    Manually trigger Wiza email enrichment for a specific contact.
    Requires contact to have a LinkedIn URL. Costs 2 Wiza credits if email found.
    Failed lookups are free.
    """
    from app.services.wiza_enrichment import enrich_contact_email

    result = await db.execute(
        select(LeadContact).where(
            LeadContact.id == contact_id, LeadContact.lead_id == lead_id
        )
    )
    contact = result.scalar_one_or_none()
    if not contact:
        raise HTTPException(status_code=404, detail="Contact not found")

    if not contact.linkedin:
        raise HTTPException(
            status_code=422,
            detail="Contact has no LinkedIn URL — required for Wiza enrichment",
        )

    wiza_result = await enrich_contact_email(
        linkedin_url=contact.linkedin,
        contact_name=contact.name,
    )

    if not wiza_result:
        return {
            "status": "not_found",
            "contact_id": contact_id,
            "message": "Wiza could not find an email for this contact",
        }

    # Save email to contact
    contact.email = wiza_result["email"]
    contact.found_via = f"wiza_{wiza_result['email_status']}"
    contact.updated_at = local_now()

    # Sync to lead primary contact if applicable
    if contact.is_primary:
        lead_res = await db.execute(
            select(PotentialLead).where(PotentialLead.id == lead_id)
        )
        lead = lead_res.scalar_one_or_none()
        if lead:
            lead.contact_email = contact.email
            lead.updated_at = local_now()

    await db.commit()

    return {
        "status": "found",
        "contact_id": contact_id,
        "email": wiza_result["email"],
        "email_status": wiza_result["email_status"],
        "confidence": wiza_result["confidence"],
        "credits_used": wiza_result.get("credits_used", 2),
    }


# Hard safety limits for bulk Wiza enrichment — enforced regardless
# of what the caller passes. Prevents runaway drain.
BULK_ENRICH_MAX_LIMIT = 10  # Hard cap: never process more than 10 per call
BULK_ENRICH_DEFAULT_LIMIT = 5  # Default if caller doesn't specify
BULK_ENRICH_MIN_CREDITS = 30  # Refuse to run if fewer than this remain
BULK_ENRICH_ABORT_CREDITS = 20  # Mid-batch abort if we drop below this


@router.post("/api/contacts/bulk-enrich-email")
async def bulk_enrich_emails(
    db: AsyncSession = Depends(get_db),
    limit: int = BULK_ENRICH_DEFAULT_LIMIT,
):
    """
    Bulk enrich emails for saved contacts that have a LinkedIn URL but no email.

    SAFETY (2026-04-22 — tightened after 2 credits/email discovery):
      - Default limit: 5 contacts (max 10 credits if all hit)
      - Hard cap: 10 contacts per call (max 20 credits) — enforced even if
        the caller passes limit=999
      - Requires 30+ credits to start (prevents draining to zero)
      - Aborts mid-batch if credits drop below 20 during the run

    Cost: 2 Wiza credits per found email (failed lookups are free).
    Worst case with limit=10 and all hit: 20 credits.

    Run the endpoint multiple times to process more saved contacts in
    controlled batches instead of one big drain.
    """
    from app.services.wiza_enrichment import enrich_contact_email, check_wiza_credits

    # ── Clamp requested limit to hard cap ────────────────────────
    original_limit = limit
    limit = max(1, min(limit, BULK_ENRICH_MAX_LIMIT))
    if limit != original_limit:
        logger.info(
            f"Bulk enrich: clamped limit {original_limit} -> {limit} "
            f"(hard cap {BULK_ENRICH_MAX_LIMIT})"
        )

    # ── Pre-flight credit check ──────────────────────────────────
    credits = await check_wiza_credits()
    credits_remaining = (credits or {}).get("credits_remaining")
    if credits_remaining is not None and credits_remaining < BULK_ENRICH_MIN_CREDITS:
        return {
            "status": "insufficient_credits",
            "credits_remaining": credits_remaining,
            "message": (
                f"Low Wiza credits ({credits_remaining}) — "
                f"need at least {BULK_ENRICH_MIN_CREDITS} to run a bulk batch. "
                "Purchase more at wiza.co/app/settings/api"
            ),
        }

    # Find saved contacts with LinkedIn but no email
    result = await db.execute(
        select(LeadContact)
        .where(
            LeadContact.is_saved.is_(True),
            LeadContact.linkedin.isnot(None),
            LeadContact.linkedin != "",
            LeadContact.email.is_(None),
        )
        .order_by(LeadContact.score.desc())
        .limit(limit)
    )
    contacts = result.scalars().all()

    if not contacts:
        return {
            "status": "complete",
            "processed": 0,
            "found": 0,
            "not_found": 0,
            "message": "No contacts need email enrichment",
        }

    found = 0
    not_found = 0
    errors = 0
    credits_used_total = 0
    aborted_reason = None

    for i, contact in enumerate(contacts):
        # ── Mid-batch abort: re-check credits every 3 contacts ───
        # Guards against exhausting to zero during a runaway batch.
        if i > 0 and i % 3 == 0:
            mid_check = await check_wiza_credits()
            mid_remaining = (mid_check or {}).get("credits_remaining")
            if mid_remaining is not None and mid_remaining < BULK_ENRICH_ABORT_CREDITS:
                aborted_reason = (
                    f"credits dropped to {mid_remaining} "
                    f"(below abort threshold {BULK_ENRICH_ABORT_CREDITS})"
                )
                logger.warning(f"Bulk Wiza aborted: {aborted_reason}")
                break

        try:
            wiza_result = await enrich_contact_email(
                linkedin_url=contact.linkedin,
                contact_name=contact.name,
            )
            if wiza_result:
                contact.email = wiza_result["email"]
                contact.found_via = f"wiza_{wiza_result['email_status']}"
                contact.updated_at = local_now()
                credits_used_total += wiza_result.get("credits_used", 2)

                # Sync to lead primary contact if applicable
                if contact.is_primary:
                    lead_res = await db.execute(
                        select(PotentialLead).where(PotentialLead.id == contact.lead_id)
                    )
                    lead = lead_res.scalar_one_or_none()
                    if lead:
                        lead.contact_email = contact.email
                        lead.updated_at = local_now()

                await db.commit()
                found += 1
                logger.info(
                    f"Bulk Wiza [{found + not_found}/{len(contacts)}]: "
                    f"{contact.name} → {wiza_result['email']} "
                    f"(running total: {credits_used_total} credits)"
                )
            else:
                not_found += 1
        except Exception as e:
            errors += 1
            logger.warning(f"Bulk Wiza error for {contact.name}: {e}")

    return {
        "status": "aborted" if aborted_reason else "complete",
        "processed": found + not_found + errors,
        "total_queued": len(contacts),
        "found": found,
        "not_found": not_found,
        "errors": errors,
        "credits_used": credits_used_total,  # Actual credits spent (2 per found)
        "aborted_reason": aborted_reason,
        "message": (
            f"Bulk batch aborted: {aborted_reason}"
            if aborted_reason
            else "Run again to process more"
            if len(contacts) == limit
            else "All contacts processed"
        ),
    }


@router.get("/api/contacts/wiza-credits")
async def get_wiza_credits():
    """Check remaining Wiza credit balance."""
    from app.services.wiza_enrichment import check_wiza_credits

    api_key = os.getenv("WIZA_API_KEY", "")
    if not api_key or api_key == "your-wiza-api-key-here":
        return {
            "configured": False,
            "message": "Add WIZA_API_KEY to your .env file",
        }

    credits = await check_wiza_credits()
    if credits is None:
        return {
            "configured": True,
            "credits_remaining": None,
            "error": "API call failed",
        }

    return {
        "configured": True,
        "credits_remaining": credits.get("credits_remaining"),
    }


# ═══════════════════════════════════════════════════════════════
# ENRICH CONTACTS — SSE STREAM WITH PROGRESS (multi-watcher version)
# ═══════════════════════════════════════════════════════════════
# Design: a single background TASK runs per lead. Each SSE connection is
# a SUBSCRIBER that receives events from the task's progress callback.
# Multiple subscribers per task are supported — when the user navigates
# away and comes back (or opens the lead in two browser tabs), the new
# connection ATTACHES to the running task instead of starting a new one.
#
# Why this matters: previously, navigating away → coming back triggered
# a fresh enrichment that doubled Serper + Gemini cost. The 409-rejection
# guard prevented the duplicate but left the user staring at a stuck
# "Connecting..." card with no way to see real progress. Now the second
# connection IS the way to see real progress.
#
# Lifecycle:
#   1. POST-equivalent click → if no job for lead_id: create EnrichmentJob,
#      start task. If job exists: just attach a watcher.
#   2. progress_callback (from contact_enrichment.py) fires → fan-out the
#      event to ALL subscribers of that lead's job. Also save to
#      `current_event` so future watchers can replay it on attach.
#   3. New SSE connection → adds queue to subscribers. If job has a
#      `current_event` (meaning the task already started), push it
#      immediately so user sees CURRENT state (Iter 3 · 36% · 3min)
#      instead of a fresh "Iter 1" or "Connecting..."
#   4. SSE disconnects → remove watcher's queue from subscribers. Task
#      keeps running. Job stays alive.
#   5. Task finishes → emit final complete/error event to all current
#      subscribers, then `_jobs.pop(lead_id)` via add_done_callback.
#   6. Cancel endpoint → user clicks Stop → task.cancel() → done_callback
#      cleans up.
#
# In-memory only. Server restart drops all jobs (acceptable — a restart
# disrupts in-flight enrichments anyway and the work that was done is
# lost regardless).


@dataclass
class EnrichmentJob:
    """A single enrichment task + its watchers.

    Path Y → 1A (2026-04-28): job is keyed by composite (parent_kind,
    parent_id) so a single registry serves both potential_leads and
    existing_hotels. Fan-out, cancel, and replay-on-attach work the
    same regardless of parent kind.

    parent_kind:
      "lead"  → potential_leads (original pipeline)
      "hotel" → existing_hotels (added 2026-04-28)
    """

    parent_kind: str  # "lead" | "hotel"
    parent_id: int
    task: asyncio.Task
    started_at: float
    # Most recent event emitted by the task. New SSE connections receive
    # this immediately so they see current state instead of starting empty.
    current_event: dict | None = None
    # One queue per active SSE connection watching this job. Events are
    # fanned out to every queue. Queues are removed on client disconnect.
    subscribers: set[asyncio.Queue] = field(default_factory=set)


# Single source of truth for active enrichments. Module-level so all
# routes (start + cancel + status) share the same view.
# Key: (parent_kind, parent_id). Same lead_id and hotel_id never collide.
_jobs: dict[tuple[str, int], EnrichmentJob] = {}


async def _start_enrichment_job(
    parent_kind: str, parent_id: int, lead_facts: dict
) -> EnrichmentJob:
    """Create and register a new EnrichmentJob.

    Args:
        parent_kind: "lead" or "hotel"
        parent_id: PotentialLead.id or ExistingHotel.id
        lead_facts: dict of facts to feed enrich_lead_contacts (hotel_name,
                    brand, city, etc.). Built by the calling route.

    Caller must verify no existing job for this (parent_kind, parent_id)
    before calling. Returns the registered job with task already started.
    """
    key = (parent_kind, parent_id)
    job = EnrichmentJob(
        parent_kind=parent_kind,
        parent_id=parent_id,
        task=None,  # filled in below — needs `job` reference inside task
        started_at=time.monotonic(),
    )

    async def progress_callback(stage: int, total: int, label: str):
        """Fan-out a stage event to all current subscribers + save as current."""
        elapsed = round(time.monotonic() - job.started_at, 1)
        pct = min(100, round((stage / total) * 100))
        event = {
            "type": "stage",
            "stage": stage,
            "total": total,
            "label": label,
            "pct": pct,
            "elapsed_s": elapsed,
        }
        job.current_event = event
        # Fan-out (non-blocking — if a subscriber's queue is full, drop)
        for q in list(job.subscribers):
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                pass

    def emit_terminal(event: dict):
        """Push a complete/error event to all subscribers. Sets current_event."""
        job.current_event = event
        for q in list(job.subscribers):
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                pass

    async def run_enrichment():
        """Run the 11-stage enrichment + persist + emit complete/error.

        Branches by parent_kind for the two operations that differ:
          - Phase B rejection update (different model class)
          - Persist call (passes the right kw-arg)
        Everything else (the actual enrich_lead_contacts pipeline) is
        identical between leads and hotels — both pass hotel_name + city
        + brand etc. through lead_facts.
        """
        try:
            from app.services.contact_enrichment import (
                enrich_lead_contacts,
                persist_enrichment_contacts,
            )

            # ── Run the 11-stage enrichment pipeline ──
            # The function still takes lead_id as its first arg — that's
            # just an internal identifier for logging/idempotency keys
            # inside contact_enrichment, NOT a DB foreign key. Passing
            # parent_id works for both kinds; the function never queries
            # potential_leads with it.
            #
            # is_existing_hotel triggers lean-mode optimizations inside
            # iterative_researcher: skips Iter 1 (Discovery) entirely +
            # gates Iter 3 (Corporate Hunt) on having found 2+ property
            # decision-makers. See enrich_lead_contacts docstring for
            # the full lean-mode contract.
            enrichment_result = await enrich_lead_contacts(
                lead_id=parent_id,
                **lead_facts,
                progress_callback=progress_callback,
                is_existing_hotel=(parent_kind == "hotel"),
            )

            # ── Phase B early-rejection (residences_only, etc.) ──
            # Only applies to potential_leads — existing_hotels are already
            # operating, can't be rejected as "not a real hotel".
            if (
                getattr(enrichment_result, "should_reject", False)
                and parent_kind == "lead"
            ):
                try:
                    async with async_session() as rej_session:
                        rej_result = await rej_session.execute(
                            select(PotentialLead).where(PotentialLead.id == parent_id)
                        )
                        rej_lead = rej_result.scalar_one_or_none()
                        if rej_lead:
                            rej_lead.status = "rejected"
                            rej_lead.rejection_reason = (
                                enrichment_result.rejection_reason or "auto_reject"
                            )[:100]
                            await rej_session.commit()
                except Exception as ex:
                    logger.error(
                        f"Failed to mark {parent_kind} {parent_id} rejected: {ex}"
                    )

                duration = round(time.monotonic() - job.started_at, 1)
                emit_terminal(
                    {
                        "type": "complete",
                        "pct": 100,
                        "elapsed_s": duration,
                        "summary": {
                            "contacts_saved": 0,
                            "contacts_rejected": 0,
                            "duration_s": duration,
                            "should_reject": True,
                            "rejection_reason": enrichment_result.rejection_reason,
                        },
                    }
                )
                return

            # ── Persist to DB ──
            # persist_enrichment_contacts (refactored Path Y) accepts EITHER
            # lead_id OR existing_hotel_id. We pass the right one based on
            # parent_kind. The function sets the correct FK on lead_contacts
            # rows and updates flat fields on the right parent table.
            persist_summary: dict = {"contacts_added": 0, "contacts_updated": 0}
            try:
                async with async_session() as persist_session:
                    if parent_kind == "lead":
                        persist_summary = await persist_enrichment_contacts(
                            lead_id=parent_id,
                            enrichment_result=enrichment_result,
                            session=persist_session,
                        )
                    else:
                        persist_summary = await persist_enrichment_contacts(
                            existing_hotel_id=parent_id,
                            enrichment_result=enrichment_result,
                            session=persist_session,
                        )
                    await persist_session.commit()
            except Exception as ex:
                logger.exception(f"Persist failed for {parent_kind} {parent_id}: {ex}")
                emit_terminal(
                    {
                        "type": "error",
                        "message": f"Save failed: {str(ex)[:200]}",
                    }
                )
                return

            duration = round(time.monotonic() - job.started_at, 1)
            contacts_added = persist_summary.get("contacts_added", 0)
            contacts_updated = persist_summary.get("contacts_updated", 0)
            total_saved = contacts_added + contacts_updated

            emit_terminal(
                {
                    "type": "complete",
                    "pct": 100,
                    "elapsed_s": duration,
                    "summary": {
                        "contacts_saved": total_saved,
                        "contacts_added": contacts_added,
                        "contacts_updated": contacts_updated,
                        "duration_s": duration,
                        "should_reject": False,
                        "rejection_reason": None,
                    },
                }
            )

        except asyncio.CancelledError:
            # User clicked Stop. Emit a clean cancel event so subscribers
            # know it was deliberate, not an error.
            duration = round(time.monotonic() - job.started_at, 1)
            emit_terminal(
                {
                    "type": "error",
                    "message": "Enrichment cancelled by user",
                    "cancelled": True,
                    "elapsed_s": duration,
                }
            )
            raise  # re-raise so task.cancelled() returns True
        except Exception as e:
            logger.exception(f"Enrichment failed for {parent_kind} {parent_id}: {e}")
            emit_terminal(
                {
                    "type": "error",
                    "message": f"Enrichment failed: {str(e)[:200]}",
                }
            )

    # Create + register task. We can't pass `job` to create_task before it's
    # built, but inside `run_enrichment` we close over `job` from outer scope.
    job.task = asyncio.create_task(run_enrichment())

    # Cleanup: remove job from registry when task finishes.
    def _on_done(_t):
        _jobs.pop(key, None)
        logger.info(f"Enrichment job removed for {parent_kind} {parent_id}")

    job.task.add_done_callback(_on_done)

    _jobs[key] = job
    return job


@router.get(
    "/api/dashboard/leads/{lead_id}/enrich-stream",
    tags=["Dashboard"],
)
async def enrich_lead_stream(lead_id: int, request: Request):
    """SSE stream of enrichment progress.

    Behavior:
      - First connection for a lead → starts the enrichment task.
      - Subsequent connections (navigate away + back, second tab, etc.) →
        attach as additional watchers of the SAME running task. Receive
        the current stage event immediately so progress bar shows real
        state, not "Iter 1" or "Connecting..."

    Event shapes:
      {"type": "stage", "stage": 3, "total": 11, "label": "...", "pct": 27, "elapsed_s": 12.3}
      {"type": "complete", "pct": 100, "summary": {"contacts_saved": 6, ...}}
      {"type": "error", "message": "...", "cancelled": True}  # user clicked Stop
    """
    from fastapi.responses import StreamingResponse

    # ── Get-or-create the job for this lead ──
    # Composite key (parent_kind, parent_id) — see _jobs definition.
    key = ("lead", lead_id)
    existing = _jobs.get(key)
    if existing is not None:
        # Job already running — attach as additional watcher.
        job = existing
        logger.info(
            f"Attaching new watcher to running enrichment for lead {lead_id} "
            f"(now {len(job.subscribers) + 1} watcher(s))"
        )
    else:
        # First request for this lead — verify lead exists, then start task.
        async with async_session() as session:
            result = await session.execute(
                select(PotentialLead).where(PotentialLead.id == lead_id)
            )
            lead = result.scalar_one_or_none()
            if not lead:
                raise HTTPException(status_code=404, detail="Lead not found")

            lead_facts = {
                "hotel_name": lead.hotel_name,
                "brand": lead.brand or "",
                "city": lead.city or "",
                "state": lead.state or "",
                "country": lead.country or "USA",
                "management_company": lead.management_company or "",
                "opening_date": lead.opening_date or "",
                "timeline_label": lead.timeline_label or "",
                "description": lead.description or "",
                "project_type_str": lead.hotel_type or "",
                "search_name": getattr(lead, "search_name", None) or "",
                "former_names": getattr(lead, "former_names", None) or [],
            }

        job = await _start_enrichment_job("lead", lead_id, lead_facts)
        logger.info(f"Started new enrichment job for lead {lead_id}")

    # ── Subscribe this connection to the job ──
    # bounded queue so a stalled subscriber can't OOM the server.
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
    # Recompute against job.started_at so the anchor reflects real elapsed.
    if job.current_event is not None:
        replay = dict(job.current_event)
        if "elapsed_s" in replay:
            replay["elapsed_s"] = round(time.monotonic() - job.started_at, 1)
        try:
            sub_queue.put_nowait(replay)
        except asyncio.QueueFull:
            pass

    async def event_stream():
        # Hello event — tells the UI the connection is live + total stages.
        yield f'data: {json.dumps({"type": "started", "total": 11})}\n\n'

        try:
            while True:
                # Client disconnected — DO NOT cancel the task, just stop streaming.
                if await request.is_disconnected():
                    logger.info(
                        f"Watcher disconnected from lead {lead_id} enrichment; "
                        f"background task continues. "
                        f"({len(job.subscribers) - 1} watcher(s) remain)"
                    )
                    return

                try:
                    event = await asyncio.wait_for(sub_queue.get(), timeout=10.0)
                except asyncio.TimeoutError:
                    yield f'data: {json.dumps({"type": "ping"})}\n\n'
                    # If task is finished and queue empty, exit cleanly.
                    if job.task.done() and sub_queue.empty():
                        return
                    continue

                yield f"data: {json.dumps(event)}\n\n"

                # Exit on terminal events.
                if event["type"] in ("complete", "error"):
                    return

        except asyncio.CancelledError:
            logger.info(
                f"SSE stream cancelled for lead {lead_id}; "
                f"background task continues."
            )
            raise
        except Exception as e:
            logger.error(f"Enrich stream error (lead {lead_id}): {e}")
            try:
                yield f'data: {json.dumps({"type": "error", "message": str(e)[:200]})}\n\n'
            except Exception:
                pass
        finally:
            # Unsubscribe THIS connection — the task + other subscribers
            # are unaffected.
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


@router.post(
    "/api/dashboard/leads/{lead_id}/enrich-cancel",
    tags=["Dashboard"],
)
async def enrich_lead_cancel(lead_id: int):
    """Cancel an in-flight enrichment for a lead.

    User clicked the Stop button. We cancel the background task, which
    triggers asyncio.CancelledError inside run_enrichment, which emits
    a clean error event to all watchers, which closes their progress
    cards. Returns 200 even if no job exists (idempotent).
    """
    job = _jobs.get(("lead", lead_id))
    if job is None:
        return {"cancelled": False, "reason": "no_active_job"}

    job.task.cancel()
    logger.info(f"User cancelled enrichment for lead {lead_id}")
    return {"cancelled": True}


@router.get(
    "/api/dashboard/leads/{lead_id}/enrich-status",
    tags=["Dashboard"],
)
async def enrich_lead_status(lead_id: int):
    """Cheap polling endpoint — is an enrichment running for this lead?

    Used by the frontend to know whether to show the Run Enrichment
    button or attach to a running job. Frontend can poll this on lead
    detail mount before opening an SSE.
    """
    job = _jobs.get(("lead", lead_id))
    if job is None:
        return {"running": False}
    return {
        "running": True,
        "current_event": job.current_event,
        "watchers": len(job.subscribers),
        "elapsed_s": round(time.monotonic() - job.started_at, 1),
    }
