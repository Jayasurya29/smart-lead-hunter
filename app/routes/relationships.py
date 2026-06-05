"""Relationship triangulation endpoints (2026-06-05) — standalone router
so the existing contacts route files stay untouched.

  GET /api/dashboard/leads/{lead_id}/contacts/{contact_id}/relationships
  GET /api/dashboard/leads/{lead_id}/contact-relationships
"""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models.lead_contact import LeadContact

router = APIRouter()


@router.get("/api/dashboard/leads/{lead_id}/contacts/{contact_id}/relationships")
async def contact_relationships(lead_id: int, contact_id: int, db: AsyncSession = Depends(get_db)):
    """Everywhere we already know this person from — inbox archive +
    other pipeline accounts. Email-exact = certainty; name-match =
    job-change candidate to verify."""
    from app.services.relationship_intel import find_known_relationships

    lc = (
        await db.execute(
            select(LeadContact).where(LeadContact.id == contact_id, LeadContact.lead_id == lead_id)
        )
    ).scalar_one_or_none()
    if not lc:
        raise HTTPException(status_code=404, detail="Contact not found")
    hits = await find_known_relationships(
        db,
        name=lc.name,
        email=lc.email,
        exclude_lead_id=lead_id,
        exclude_lead_contact_id=lc.id,
    )
    return {"contact_id": contact_id, "name": lc.name, "relationships": hits}


@router.get("/api/dashboard/leads/{lead_id}/contact-relationships")
async def lead_contact_relationships(lead_id: int, db: AsyncSession = Depends(get_db)):
    """Bulk triangulation for a lead's contacts — powers the
    'we know this person' badge."""
    from app.services.relationship_intel import find_known_relationships

    rows = (
        (
            await db.execute(
                select(LeadContact)
                .where(LeadContact.lead_id == lead_id)
                .order_by(LeadContact.is_saved.desc(), LeadContact.score.desc())
                .limit(25)
            )
        )
        .scalars()
        .all()
    )
    out = []
    for lc in rows:
        hits = await find_known_relationships(
            db,
            name=lc.name,
            email=lc.email,
            exclude_lead_id=lead_id,
            exclude_lead_contact_id=lc.id,
        )
        if hits:
            out.append(
                {
                    "contact_id": lc.id,
                    "name": lc.name,
                    "is_saved": bool(lc.is_saved),
                    "relationships": hits,
                }
            )
    return {"lead_id": lead_id, "contacts_with_relationships": out}
