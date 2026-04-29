"""
Smart Lead Hunter — Lead Transfer Service
==========================================

Manual transfer of a potential_lead → existing_hotels. Triggered from the
PowerShell script `scripts/transfer_to_existing.py` once you've finished
smart-fill / contact enrichment / verification on the lead and decided
it's ready to graduate (lead has opened, or is within the 0-3 month
opening window).

Operation
---------
1. SELECT the potential_lead.
2. INSERT a row into existing_hotels with the same fields (schema parity
   from migration 018) — status='new' so it lands on the Existing Hotels
   Pipeline tab for sales review under the new-scoring lens.
3. UPDATE lead_contacts.lead_id=NULL, existing_hotel_id=N (atomic flip,
   CHECK constraint guarantees integrity, no copy needed).
4. SCORE the new row using existing_hotel_scorer (Option B — account fit).
   The lead's old `lead_score` and `timeline_label` are intentionally
   discarded. Existing-hotel scoring uses a different lens and we want
   the new row to arrive on the Pipeline tab pre-scored under that lens.
5. DELETE the potential_lead. Hard delete — there's no Expired tab, no
   audit zombie, no "→EH#nnn" annotation.

Score handling
--------------
On transfer:
  - lead_score → recomputed via score_existing_hotel(new_row)
  - score_breakdown → recomputed
  - timeline_label → DROPPED (existing hotels don't use timeline labels;
                              they're operating, not pre-opening)

This means a lead that was scoring 85 on the new-hotels side might score
65 as an existing hotel — that's CORRECT. The lenses measure different
things (timeline urgency vs account fit) and they shouldn't agree.

Dedup
-----
Light check — if an existing_hotel with the same `hotel_name_normalized`
already exists, we MERGE the lead's enrichment data into it (only fills
NULLs, preserves manual edits) and re-parent the contacts. Then we
RESCORE the merged row (because the merge may have filled fields that
affect the score). Then still hard-delete the source lead.

Created: 2026-04-28
"""

from __future__ import annotations

import logging
from typing import Optional

from sqlalchemy import select, update as sql_update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.potential_lead import PotentialLead
from app.models.existing_hotel import ExistingHotel
from app.models.lead_contact import LeadContact
from app.services.utils import normalize_hotel_name, local_now
from app.services.existing_hotel_scorer import apply_score_to_hotel

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Dedup — does this lead already exist as an existing_hotel?
# ─────────────────────────────────────────────────────────────────────────────


async def _find_existing_hotel_match(
    lead: PotentialLead,
    session: AsyncSession,
) -> Optional[ExistingHotel]:
    """Find an existing-hotel match for this lead via hotel_name_normalized.

    Returns None if no match. Falls back to a name+location loose match
    for rows imported pre-018 that may have NULL hotel_name_normalized.
    """
    if not lead.hotel_name:
        return None

    normalized = lead.hotel_name_normalized or normalize_hotel_name(lead.hotel_name)

    if normalized:
        result = await session.execute(
            select(ExistingHotel).where(
                ExistingHotel.hotel_name_normalized == normalized
            )
        )
        match = result.scalars().first()
        if match:
            return match

    # Loose name match for legacy rows
    name_lower = lead.hotel_name.lower().strip()
    if name_lower:
        from sqlalchemy import func

        result = await session.execute(
            select(ExistingHotel).where(
                func.lower(func.coalesce(ExistingHotel.hotel_name, ExistingHotel.name))
                == name_lower
            )
        )
        candidates = result.scalars().all()
        for c in candidates:
            if (
                (lead.city and c.city and lead.city.lower() == (c.city or "").lower())
                or (
                    lead.state
                    and c.state
                    and lead.state.lower() == (c.state or "").lower()
                )
                or (
                    lead.country
                    and c.country
                    and lead.country.lower() == (c.country or "").lower()
                )
            ):
                return c
        if len(candidates) == 1:
            return candidates[0]

    return None


# ─────────────────────────────────────────────────────────────────────────────
# Build a fresh ExistingHotel from a lead
# ─────────────────────────────────────────────────────────────────────────────


def _build_existing_hotel_from_lead(lead: PotentialLead) -> ExistingHotel:
    """Field-for-field copy from PotentialLead → ExistingHotel.

    Schema parity from migration 018 makes this a clean copy. We do NOT
    carry over:

      - timeline_label  → not on existing_hotels by design (post-opening
                          hotels don't have urgency labels)
      - lead_score      → will be recomputed via Option B scorer below
      - score_breakdown → tied to the lead-side scoring model, will be
                          rebuilt by the new model
      - estimated_revenue → keep, since the revenue calculator works the
                            same on either side (it's based on rooms +
                            tier + climate, not opening-related)
    """
    return ExistingHotel(
        # Identity (canonical + legacy mirrors)
        hotel_name=lead.hotel_name,
        name=lead.hotel_name,
        hotel_name_normalized=lead.hotel_name_normalized,
        brand=lead.brand,
        chain=lead.chain,
        brand_tier=lead.brand_tier,
        hotel_type=lead.hotel_type,
        property_type=lead.hotel_type,
        hotel_website=lead.hotel_website,
        website=lead.hotel_website,
        # Location
        address=lead.address,
        city=lead.city,
        state=lead.state,
        country=lead.country,
        zip_code=lead.zip_code,
        location_type=lead.location_type,
        latitude=lead.latitude,
        longitude=lead.longitude,
        zone=lead.zone,
        website_verified=lead.website_verified,
        # Primary contact (single-row flat snapshot — full contact list
        # comes through the FK flip below)
        contact_name=lead.contact_name,
        contact_title=lead.contact_title,
        contact_email=lead.contact_email,
        contact_phone=lead.contact_phone,
        gm_name=lead.contact_name,
        gm_title=lead.contact_title,
        gm_email=lead.contact_email,
        gm_phone=lead.contact_phone,
        # Hotel details (opening_date / project_type retained as historical)
        opening_date=lead.opening_date,
        opening_year=lead.opening_year,
        project_type=lead.project_type,
        room_count=lead.room_count,
        revenue_opening=lead.revenue_opening,
        revenue_annual=lead.revenue_annual,
        estimated_revenue=lead.estimated_revenue,
        description=lead.description,
        key_insights=lead.key_insights,
        # Stakeholders
        management_company=lead.management_company,
        developer=lead.developer,
        owner=lead.owner,
        # Name intelligence
        search_name=lead.search_name,
        former_names=lead.former_names,
        # Scoring — set to None here, populated by apply_score_to_hotel()
        # below in transfer_lead() before commit.
        lead_score=None,
        score_breakdown=None,
        # Source provenance
        data_source=lead.data_source or "transferred_from_lead",
        source_id=lead.source_id,
        source_url=lead.source_url,
        source_site=lead.source_site,
        source_urls=lead.source_urls,
        source_extractions=lead.source_extractions,
        scraped_at=lead.scraped_at,
        last_verified_at=local_now(),
        # Workflow — fresh row, sales reviews on the Pipeline tab
        status="new",
        notes=lead.notes,
        # Defaults
        is_client=False,
        pushed_to_map=False,
        # Timestamps
        created_at=local_now(),
        updated_at=local_now(),
    )


def _enrich_existing_from_lead(existing: ExistingHotel, lead: PotentialLead) -> None:
    """Merge path: fill any NULL fields on `existing` from the lead.

    Preserves manual edits on the existing row — only writes if the
    target field is empty. Source URLs are unioned. Source extractions
    are dict-merged with existing taking priority on duplicate keys.

    Note: scores get recomputed by the caller after this returns.
    """
    fields_to_check = [
        "brand",
        "chain",
        "brand_tier",
        "hotel_type",
        "address",
        "zip_code",
        "latitude",
        "longitude",
        "zone",
        "opening_date",
        "opening_year",
        "project_type",
        "room_count",
        "description",
        "key_insights",
        "management_company",
        "developer",
        "owner",
        "hotel_website",
        "search_name",
        "revenue_opening",
        "revenue_annual",
        "estimated_revenue",
        "contact_name",
        "contact_title",
        "contact_email",
        "contact_phone",
    ]
    for field in fields_to_check:
        existing_val = getattr(existing, field, None)
        lead_val = getattr(lead, field, None)
        if not existing_val and lead_val:
            setattr(existing, field, lead_val)

    # Union source URLs
    if lead.source_urls:
        existing_urls = list(existing.source_urls or [])
        for url in lead.source_urls:
            if url and url not in existing_urls:
                existing_urls.append(url)
        existing.source_urls = existing_urls

    # Merge source extractions — existing wins on conflict
    if lead.source_extractions:
        merged = dict(lead.source_extractions or {})
        merged.update(existing.source_extractions or {})
        existing.source_extractions = merged

    existing.last_verified_at = local_now()
    existing.updated_at = local_now()


# ─────────────────────────────────────────────────────────────────────────────
# Contact re-parenting via FK flip
# ─────────────────────────────────────────────────────────────────────────────


async def _migrate_contacts(
    lead_id: int,
    existing_hotel_id: int,
    session: AsyncSession,
) -> int:
    """Atomically re-parent all contacts from lead → existing_hotel."""
    result = await session.execute(
        sql_update(LeadContact)
        .where(LeadContact.lead_id == lead_id)
        .values(lead_id=None, existing_hotel_id=existing_hotel_id)
    )
    return result.rowcount or 0


# ─────────────────────────────────────────────────────────────────────────────
# Main API — single-lead transfer
# ─────────────────────────────────────────────────────────────────────────────


async def transfer_lead(
    lead_id: int,
    session: AsyncSession,
    *,
    commit: bool = True,
) -> dict:
    """Transfer one potential_lead to existing_hotels and DELETE the lead.

    Returns:
        {
          "status":            "transferred" | "merged" | "not_found",
          "lead_id":           <int>,
          "existing_hotel_id": <int> | None,
          "contacts_migrated": <int>,
          "score":             <int> | None,
          "reason":            <str>,
        }
    """
    result = await session.execute(
        select(PotentialLead).where(PotentialLead.id == lead_id)
    )
    lead = result.scalar_one_or_none()
    if not lead:
        return {
            "status": "not_found",
            "lead_id": lead_id,
            "existing_hotel_id": None,
            "contacts_migrated": 0,
            "score": None,
            "reason": f"Lead {lead_id} not found",
        }

    hotel_name = lead.hotel_name or f"<lead #{lead_id}>"
    existing_match = await _find_existing_hotel_match(lead, session)

    if existing_match:
        # MERGE path — enrich existing, rescore, re-parent, delete lead
        eh_id = existing_match.id
        _enrich_existing_from_lead(existing_match, lead)

        # Rescore — merge may have filled fields that affect the score
        score, _breakdown = apply_score_to_hotel(existing_match)

        contacts_migrated = await _migrate_contacts(lead.id, eh_id, session)
        await session.delete(lead)
        if commit:
            await session.commit()

        # Revenue calc — merge may have filled rooms/tier/type that
        # enable revenue computation that wasn't possible before.
        try:
            from app.services.revenue_updater import update_hotel_revenue

            await update_hotel_revenue(eh_id)
        except Exception as e:
            logger.warning(
                f"Revenue calc failed for merged existing_hotel " f"#{eh_id}: {e}"
            )

        logger.info(
            f"   ✓ Merged lead #{lead_id} '{hotel_name}' into existing_hotel "
            f"#{eh_id} → score={score} ({contacts_migrated} contact(s) "
            f"re-parented, lead deleted)"
        )
        return {
            "status": "merged",
            "lead_id": lead_id,
            "existing_hotel_id": eh_id,
            "contacts_migrated": contacts_migrated,
            "score": score,
            "reason": f"Merged into existing_hotel #{eh_id}",
        }

    # CREATE path — new existing_hotel row
    new_hotel = _build_existing_hotel_from_lead(lead)

    # Score the new row using Option B (account fit) — leaves the lead's
    # old prospect-side score behind on purpose. We're now scoring under
    # a different lens.
    score, _breakdown = apply_score_to_hotel(new_hotel)

    session.add(new_hotel)
    await session.flush()  # need new_hotel.id for FK flip

    contacts_migrated = await _migrate_contacts(lead.id, new_hotel.id, session)
    await session.delete(lead)

    if commit:
        await session.commit()

    # Revenue calc — runs after commit so the row exists with its final
    # ID. Uses a separate session inside update_hotel_revenue. Failures
    # don't abort the transfer; revenue can be backfilled later.
    try:
        from app.services.revenue_updater import update_hotel_revenue

        await update_hotel_revenue(new_hotel.id)
    except Exception as e:
        logger.warning(
            f"Revenue calc failed for new existing_hotel #{new_hotel.id} "
            f"after transfer: {e}"
        )

    logger.info(
        f"   ✓ Transferred lead #{lead_id} '{hotel_name}' → existing_hotel "
        f"#{new_hotel.id} score={score} ({contacts_migrated} contact(s), "
        f"lead deleted)"
    )
    return {
        "status": "transferred",
        "lead_id": lead_id,
        "existing_hotel_id": new_hotel.id,
        "contacts_migrated": contacts_migrated,
        "score": score,
        "reason": f"Created existing_hotel #{new_hotel.id}",
    }


# ─────────────────────────────────────────────────────────────────────────────
# Bulk API — used only by the manual PowerShell script
# ─────────────────────────────────────────────────────────────────────────────


async def transfer_leads_by_ids(
    lead_ids: list[int],
    session: AsyncSession,
) -> dict:
    """Transfer a specific list of leads. Per-lead errors don't abort
    the batch.
    """
    transferred = 0
    merged = 0
    errors = 0
    not_found = 0
    contacts_total = 0
    results: list[dict] = []

    for lid in lead_ids:
        try:
            r = await transfer_lead(lid, session, commit=False)
            results.append(r)
            status = r["status"]
            if status == "transferred":
                transferred += 1
                contacts_total += r["contacts_migrated"]
            elif status == "merged":
                merged += 1
                contacts_total += r["contacts_migrated"]
            elif status == "not_found":
                not_found += 1
        except Exception as e:
            logger.exception(f"transfer_lead({lid}) failed")
            errors += 1
            try:
                await session.rollback()
            except Exception:
                pass
            results.append(
                {
                    "status": "error",
                    "lead_id": lid,
                    "existing_hotel_id": None,
                    "contacts_migrated": 0,
                    "score": None,
                    "reason": str(e)[:200],
                }
            )

    try:
        await session.commit()
    except Exception as e:
        logger.error(f"final commit failed: {e}")
        await session.rollback()
        errors += 1

    return {
        "transferred": transferred,
        "merged": merged,
        "not_found": not_found,
        "errors": errors,
        "contacts_migrated": contacts_total,
        "total": len(lead_ids),
        "results": results,
    }
