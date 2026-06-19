"""Learning-junk-system endpoints.

Lets reps curate junk and have the system learn from it:
  - junk / restore a single contact (manual_category override)
  - bulk junk
  - junk / un-junk a whole domain (auto-junk rule + flips existing contacts)
  - list the junk-domain rules
  - suggest domains the rep keeps junking by hand (Tier 3)

Effective category = COALESCE(manual_category, contact_category), so anything
junked here drops out of the real contact count immediately.
"""

import logging

from fastapi import APIRouter, Depends, HTTPException, Request

from app.database import async_session
from app.services import junk_rules
from app.shared import require_ajax

logger = logging.getLogger(__name__)
router = APIRouter()


@router.post("/api/contacts/{contact_id}/junk", tags=["Contacts"])
async def junk_one(contact_id: int, _csrf=Depends(require_ajax)):
    """Mark ONE contact as junk (rep override; reversible via /unjunk)."""
    async with async_session() as s:
        ok = await junk_rules.junk_contact(s, contact_id)
    if not ok:
        raise HTTPException(status_code=404, detail="contact not found")
    return {"id": contact_id, "manual_category": "junk"}


@router.post("/api/contacts/{contact_id}/unjunk", tags=["Contacts"])
async def unjunk_one(contact_id: int, _csrf=Depends(require_ajax)):
    """Restore ONE contact (clears the rep's junk override → AI category)."""
    async with async_session() as s:
        ok = await junk_rules.unjunk_contact(s, contact_id)
    if not ok:
        raise HTTPException(status_code=404, detail="contact not found")
    return {"id": contact_id, "manual_category": None}


@router.post("/api/contacts/junk-bulk", tags=["Contacts"])
async def junk_bulk(request: Request, _csrf=Depends(require_ajax)):
    """Mark many contacts junk at once. Body: {"ids": [1,2,3]}."""
    body = await request.json()
    ids = body.get("ids") or []
    if not isinstance(ids, list) or not all(isinstance(i, int) for i in ids):
        raise HTTPException(status_code=400, detail="ids must be a list of integers")
    async with async_session() as s:
        n = await junk_rules.junk_contacts_bulk(s, ids)
    return {"junked": n}


@router.post("/api/contacts/junk-domain", tags=["Contacts"])
async def junk_domain_add(request: Request, _csrf=Depends(require_ajax)):
    """Junk a whole domain. Body: {"domain": "mariana.com", "reason": "..."}.

    Adds the auto-junk rule AND flips every existing contact from that domain to
    junk. Future contacts auto-junk in Pass 1 (no LLM)."""
    body = await request.json()
    domain = (body.get("domain") or "").strip()
    if not domain:
        raise HTTPException(status_code=400, detail="domain required")
    try:
        async with async_session() as s:
            result = await junk_rules.junk_domain(
                s, domain, added_by=body.get("added_by"), reason=body.get("reason")
            )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return result


@router.post("/api/contacts/unjunk-domain", tags=["Contacts"])
async def junk_domain_remove(request: Request, _csrf=Depends(require_ajax)):
    """Remove a domain from auto-junk and release its contacts.
    Body: {"domain": "mariana.com"}."""
    body = await request.json()
    domain = (body.get("domain") or "").strip()
    if not domain:
        raise HTTPException(status_code=400, detail="domain required")
    async with async_session() as s:
        result = await junk_rules.unjunk_domain(s, domain)
    return result


@router.get("/api/contacts/junk-domains", tags=["Contacts"])
async def junk_domains_list():
    """List the rep-curated auto-junk domains."""
    async with async_session() as s:
        return {"domains": await junk_rules.list_junk_domains(s)}


@router.get("/api/contacts/junk-domain-suggestions", tags=["Contacts"])
async def junk_domain_suggest(threshold: int = 3):
    """Domains the rep has manually junked >= threshold times — offer a
    one-click 'junk the whole domain'."""
    async with async_session() as s:
        return {"suggestions": await junk_rules.junk_domain_suggestions(s, threshold)}
