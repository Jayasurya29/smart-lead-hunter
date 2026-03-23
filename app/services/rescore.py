# -*- coding: utf-8 -*-
"""
LEAD RESCORE SERVICE
=====================
Rescores leads using FRESH scoring + enriched contact data.

Every rescore:
1. Calls calculate_lead_score() fresh (brand, location, timing, rooms, etc.)
2. Replaces flat contact score with enriched contacts from lead_contacts table
3. Updates the lead's score, breakdown, and tier

Contact scoring (12 pts max):
- Hotel-specific + high confidence + decision-maker = 12 pts
- Hotel-specific + high confidence = 10 pts
- Hotel-specific + medium confidence = 8 pts
- Hotel-specific + low confidence = 6 pts
- Chain-area contact = 4 pts
- Has email = +2 bonus (capped at 12)
- No contacts = 0 pts
"""

import logging
from typing import Dict, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession


logger = logging.getLogger(__name__)

# Decision-maker titles for uniform purchasing
DECISION_MAKER_TITLES = [
    "general manager",
    "hotel manager",
    "director of housekeeping",
    "executive housekeeper",
    "director of purchasing",
    "purchasing manager",
    "director of human resources",
    "hr director",
    "director of operations",
    "director of rooms",
    "director of food",
    "director of f&b",
    "food & beverage director",
    "food and beverage director",
    "director of engineering",
    "rooms division manager",
    "executive director",
    "managing director",
    "resort manager",
    "property manager",
    "director of people",
]


def _is_decision_maker(title: str) -> bool:
    if not title:
        return False
    title_lower = title.lower()
    return any(dm in title_lower for dm in DECISION_MAKER_TITLES)


def score_enriched_contacts(contacts: list) -> Dict:
    """Score contacts from the lead_contacts table."""
    if not contacts:
        return {
            "points": 0,
            "tier": "No Contacts - Research Needed",
            "detail": {
                "total_contacts": 0,
                "hotel_specific": 0,
                "chain_area": 0,
                "has_decision_maker": False,
                "has_email": False,
                "best_contact": None,
            },
        }

    points = 0
    hotel_specific = []
    chain_area = []
    has_email = False
    has_decision_maker = False
    best_contact = None
    best_score = 0

    for contact in contacts:
        scope = getattr(contact, "scope", "unknown") or "unknown"
        confidence = getattr(contact, "confidence", "medium") or "medium"
        title = getattr(contact, "title", "") or ""
        email = getattr(contact, "email", "") or ""
        name = getattr(contact, "name", "") or ""

        if email and "@" in email:
            has_email = True

        is_dm = _is_decision_maker(title)
        if is_dm:
            has_decision_maker = True

        contact_score = 0
        if scope == "hotel_specific":
            hotel_specific.append(contact)
            if confidence == "high":
                contact_score = 10
                if is_dm:
                    contact_score = 12
            elif confidence == "medium":
                contact_score = 8
            else:
                contact_score = 6
        elif scope == "chain_area":
            chain_area.append(contact)
            contact_score = 4

        if contact_score > best_score:
            best_score = contact_score
            best_contact = f"{name} - {title}" if title else name

    points = best_score
    if has_email and points < 12:
        points = min(points + 2, 12)

    if points >= 10:
        tier = "Sales-Ready (hotel-specific decision-maker)"
    elif points >= 8:
        tier = "Strong Contact (hotel-specific)"
    elif points >= 6:
        tier = "Good Contact (hotel-specific, low confidence)"
    elif points >= 4:
        tier = "Chain Contact Only"
    elif points > 0:
        tier = "Weak Contact"
    else:
        tier = "No Contacts - Research Needed"

    return {
        "points": points,
        "tier": tier,
        "detail": {
            "total_contacts": len(contacts),
            "hotel_specific": len(hotel_specific),
            "chain_area": len(chain_area),
            "has_decision_maker": has_decision_maker,
            "has_email": has_email,
            "best_contact": best_contact,
        },
    }


async def rescore_lead(lead_id: int, session: AsyncSession) -> Optional[Dict]:
    """
    Full rescore: fresh calculate_lead_score + enriched contacts overlay.
    """
    from app.models.potential_lead import PotentialLead
    from app.models.lead_contact import LeadContact
    from app.services.scorer import calculate_lead_score

    # Load lead
    result = await session.execute(
        select(PotentialLead).where(PotentialLead.id == lead_id)
    )
    lead = result.scalar_one_or_none()
    if not lead:
        return None

    old_score = lead.lead_score or 0

    # 1. FRESH full score from current lead fields
    score_result = calculate_lead_score(
        hotel_name=lead.hotel_name or "",
        city=lead.city or "",
        state=lead.state or "",
        country=lead.country or "USA",
        opening_date=lead.opening_date or "",
        room_count=lead.room_count or 0,
        description=lead.description or "",
        brand=lead.brand or "",
    )

    # If lead would be filtered (budget/international/expired), keep existing
    if not score_result.get("should_save", True):
        return {
            "old_score": old_score,
            "new_score": old_score,
            "change": 0,
            "contact_tier": "N/A",
            "contact_points": 0,
            "contacts_found": 0,
            "hotel_specific": 0,
            "changed": False,
        }

    # 2. Load enriched contacts
    contacts_result = await session.execute(
        select(LeadContact).where(LeadContact.lead_id == lead_id)
    )
    contacts = contacts_result.scalars().all()

    # 3. Score contacts from DB
    contact_result = score_enriched_contacts(contacts)

    # 4. Calculate new total: fresh base score - old contact points + enriched contact points
    base_score = score_result["total_score"]
    old_contact_points = score_result["breakdown"].get("contact", {}).get("points", 0)
    new_score = base_score - old_contact_points + contact_result["points"]
    new_score = min(new_score, 100)

    # 5. Build updated breakdown
    new_breakdown = score_result["breakdown"]
    new_breakdown["contact"] = contact_result

    # 6. Update lead
    lead.lead_score = new_score
    lead.score_breakdown = new_breakdown

    # Update brand_tier and location_type from fresh score
    if score_result.get("brand_tier"):
        lead.brand_tier = score_result["brand_tier"]
    if score_result.get("location_type"):
        lead.location_type = score_result["location_type"]

    # Recalculate timeline label from opening date
    from app.services.utils import get_timeline_label

    lead.timeline_label = get_timeline_label(lead.opening_date or "")

    # NOTE: Score tier is derived from timeline_label (already set above)
    # and lead_score. No separate column needed — the frontend reads
    # lead_score + thresholds directly.

    return {
        "old_score": old_score,
        "new_score": new_score,
        "change": new_score - old_score,
        "contact_tier": contact_result["tier"],
        "contact_points": contact_result["points"],
        "contacts_found": contact_result["detail"]["total_contacts"],
        "hotel_specific": contact_result["detail"]["hotel_specific"],
        "changed": new_score != old_score,
    }


async def rescore_all_leads(session: AsyncSession) -> Dict:
    """Rescore all active leads."""
    from app.models.potential_lead import PotentialLead

    result = await session.execute(
        select(PotentialLead.id).where(PotentialLead.status != "deleted")
    )
    lead_ids = [row[0] for row in result.all()]

    total = len(lead_ids)
    changed = 0
    increased = 0
    decreased = 0
    total_change = 0

    for lead_id in lead_ids:
        score_result = await rescore_lead(lead_id, session)
        if score_result and score_result["changed"]:
            changed += 1
            total_change += score_result["change"]
            if score_result["change"] > 0:
                increased += 1
            else:
                decreased += 1

    await session.commit()

    return {
        "total": total,
        "changed": changed,
        "increased": increased,
        "decreased": decreased,
        "avg_change": round(total_change / max(changed, 1), 1),
    }
