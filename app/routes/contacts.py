"""Contact management and lead enrichment endpoints.

FIX: All contact CRUD endpoints now use Depends(get_db) for proper lifecycle.
FIX: Enrichment errors return proper HTTP status codes (not 200).
FIX: Removed phantom contact_linkedin reference.
NOTE: enrich_lead() intentionally uses manual sessions because the enrichment
      network calls take 10-30s — we don't want to hold a DB connection that long.
"""

import logging
import os

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

    # Auto-enrich email via Wiza if contact has LinkedIn but no email
    wiza_result = None
    if contact.linkedin and not contact.email:
        try:
            from app.services.wiza_enrichment import enrich_contact_email

            wiza_result = await enrich_contact_email(
                linkedin_url=contact.linkedin,
                contact_name=contact.name,
            )
            if wiza_result:
                contact.email = wiza_result["email"]
                contact.found_via = f"wiza_{wiza_result['email_status']}"
                # If this is the primary contact, sync email back to lead
                if contact.is_primary:
                    lead_res = await db.execute(
                        select(PotentialLead).where(PotentialLead.id == lead_id)
                    )
                    lead = lead_res.scalar_one_or_none()
                    if lead:
                        lead.contact_email = contact.email
        except Exception as e:
            logger.warning(
                f"Wiza enrichment failed on save for contact {contact_id}: {e}"
            )

    await db.commit()
    return {
        "status": "saved",
        "contact_id": contact_id,
        "email_found": wiza_result["email"] if wiza_result else None,
        "email_status": wiza_result["email_status"] if wiza_result else None,
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
    for field, value in body.items():
        if field in allowed:
            setattr(contact, field, value)
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

    # Rescore contact based on updated title/scope
    from app.config.sap_title_classifier import title_classifier

    if contact.title:
        classification = title_classifier.classify(contact.title)
        scope = contact.scope or "unknown"
        if scope == "hotel_specific":
            contact.score = 30 if classification.tier.value <= 5 else 8
            contact.confidence = "high"
        elif scope == "chain_area":
            contact.score = 12 if classification.tier.value <= 5 else 5
            contact.confidence = "medium"
        else:
            contact.score = 5
            contact.confidence = "low"
        contact.tier = classification.tier.name
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
    if new_scope not in ("hotel_specific", "chain_area", "chain_corporate"):
        raise HTTPException(status_code=400, detail="Invalid scope")
    result = await db.execute(
        select(LeadContact).where(
            LeadContact.id == contact_id, LeadContact.lead_id == lead_id
        )
    )
    contact = result.scalar_one_or_none()
    if not contact:
        raise HTTPException(status_code=404, detail="Contact not found")
    contact.scope = new_scope
    # Rescore: base score depends on scope
    if contact.tier in (
        "TIER1_HSKP",
        "TIER2_PURCH",
        "TIER3_GM_OPS",
        "TIER4_FB",
        "TIER5_HR",
    ):
        if new_scope == "hotel_specific":
            contact.score = 30
            contact.confidence = "high"
        elif new_scope == "chain_area":
            contact.score = 12
            contact.confidence = "medium"
        else:
            contact.score = 5
            contact.confidence = "low"

    # Preserve strategist priority floor — P1=28, P2=18, P3=10, P4=2
    # Without this, toggling scope on a P1 contact drops 28→12
    _PRIORITY_FLOOR = {"P1": 28, "P2": 18, "P3": 10, "P4": 2}
    if contact.strategist_priority and contact.strategist_priority in _PRIORITY_FLOOR:
        floor = _PRIORITY_FLOOR[contact.strategist_priority]
        if contact.score < floor:
            contact.score = floor
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

    # Score the contact
    from app.config.sap_title_classifier import title_classifier

    title = (body.get("title") or "").strip()
    scope = body.get("scope", "hotel_specific")
    score = 5
    confidence = "low"
    tier_name = "UNKNOWN"
    if title:
        classification = title_classifier.classify(title)
        tier_name = classification.tier.name
        if scope == "hotel_specific":
            score = 30 if classification.tier.value <= 5 else 8
            confidence = "high"
        elif scope == "chain_area":
            score = 12 if classification.tier.value <= 5 else 5
            confidence = "medium"

    contact = LeadContact(
        lead_id=lead_id,
        name=name,
        title=title or None,
        email=(body.get("email") or "").strip() or None,
        phone=(body.get("phone") or "").strip() or None,
        linkedin=(body.get("linkedin") or "").strip() or None,
        organization=(body.get("organization") or "").strip() or None,
        scope=scope,
        confidence=confidence,
        tier=tier_name,
        score=score,
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
    return {"status": "created", "contact_id": contact.id, "score": score}


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
    Requires contact to have a LinkedIn URL. Costs 1 Wiza credit if email found.
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
        "credits_used": wiza_result.get("credits_used", 1),
    }


@router.post("/api/contacts/bulk-enrich-email")
async def bulk_enrich_emails(
    db: AsyncSession = Depends(get_db),
    limit: int = 50,
):
    """
    Bulk enrich emails for all saved contacts that have a LinkedIn URL but no email.
    Processes up to `limit` contacts per call. Run multiple times to process all.

    Cost: up to `limit` Wiza credits (only charged when email is found).
    """
    from app.services.wiza_enrichment import enrich_contact_email, check_wiza_credits

    # Check credits first
    credits = await check_wiza_credits()
    if credits and credits.get("credits_remaining", 999) < 5:
        return {
            "status": "insufficient_credits",
            "credits_remaining": credits.get("credits_remaining"),
            "message": "Low Wiza credits — purchase more at wiza.co/app/settings/api",
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

    for contact in contacts:
        try:
            wiza_result = await enrich_contact_email(
                linkedin_url=contact.linkedin,
                contact_name=contact.name,
            )
            if wiza_result:
                contact.email = wiza_result["email"]
                contact.found_via = f"wiza_{wiza_result['email_status']}"
                contact.updated_at = local_now()

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
                    f"Bulk Wiza [{found + not_found}/{len(contacts)}]: {contact.name} → {wiza_result['email']}"
                )
            else:
                not_found += 1
        except Exception as e:
            errors += 1
            logger.warning(f"Bulk Wiza error for {contact.name}: {e}")

    return {
        "status": "complete",
        "processed": len(contacts),
        "found": found,
        "not_found": not_found,
        "errors": errors,
        "credits_used": found,
        "message": "Run again to process more"
        if len(contacts) == limit
        else "All contacts processed",
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
