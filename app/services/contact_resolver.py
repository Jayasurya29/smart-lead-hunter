"""contact_resolver.py -- unify inbox (contacts) and lead-gen (lead_contacts)
contacts behind one read/write, so the tool endpoints (find-linkedin,
find-current-employer, find-successor, enrich-deep) work for BOTH.

ID convention (matches the frontend LEAD_ID_OFFSET): an incoming id >= the
offset is a lead_contact; subtract the offset for the real lead_contacts.id.
Anything below is an inbox contacts.id.

Field mapping:
  contacts:       first_name/last_name/display_name, linkedin_url, organization
  lead_contacts:  name (single),                      linkedin,     organization
"""

from __future__ import annotations

from sqlalchemy import text

LEAD_ID_OFFSET = 10_000_000


def is_lead_id(cid: int) -> bool:
    return cid >= LEAD_ID_OFFSET


def real_id(cid: int) -> int:
    return cid - LEAD_ID_OFFSET if is_lead_id(cid) else cid


async def resolve_contact(session, cid: int) -> dict | None:
    """Return a normalized contact dict or None.
    Keys: name, org, email, linkedin, title, _table, _id (real), _is_lead.
    """
    if is_lead_id(cid):
        rid = real_id(cid)
        r = (
            await session.execute(
                text(
                    "SELECT id, name, title, organization, email, linkedin "
                    "FROM lead_contacts WHERE id = :id"
                ),
                {"id": rid},
            )
        ).first()
        if not r:
            return None
        return {
            "name": (r.name or "").strip(),
            "org": (r.organization or "").strip(),
            "email": (r.email or "").strip(),
            "linkedin": (r.linkedin or "").strip(),
            "title": (r.title or "").strip(),
            "_table": "lead_contacts",
            "_id": rid,
            "_is_lead": True,
        }
    r = (
        await session.execute(
            text(
                "SELECT id, first_name, last_name, display_name, title, "
                "organization, email, linkedin_url "
                "FROM contacts WHERE id = :id"
            ),
            {"id": cid},
        )
    ).first()
    if not r:
        return None
    name = f"{r.first_name or ''} {r.last_name or ''}".strip() or (r.display_name or "").strip()
    return {
        "name": name,
        "org": (r.organization or "").strip(),
        "email": (r.email or "").strip(),
        "linkedin": (r.linkedin_url or "").strip(),
        "title": (r.title or "").strip(),
        "_table": "contacts",
        "_id": cid,
        "_is_lead": False,
    }


# map a logical field -> (contacts column, lead_contacts column)
_FIELD_MAP = {
    "linkedin": ("linkedin_url", "linkedin"),
    "title": ("title", "title"),
    "organization": ("organization", "organization"),
    "email": ("email", "email"),
}


async def write_contact_field(session, cid: int, field: str, value) -> bool:
    """Write one logical field to the correct table/column. Returns True on hit."""
    cols = _FIELD_MAP.get(field)
    if not cols:
        return False
    if is_lead_id(cid):
        col = cols[1]
        rid = real_id(cid)
        res = await session.execute(
            text(f"UPDATE lead_contacts SET {col} = :v, updated_at = NOW() WHERE id = :id"),
            {"v": value, "id": rid},
        )
    else:
        col = cols[0]
        res = await session.execute(
            text(f"UPDATE contacts SET {col} = :v, updated_at = NOW() WHERE id = :id"),
            {"v": value, "id": cid},
        )
    return res.rowcount > 0
