#!/usr/bin/env python3
r"""patch_endpoint_leadcontact_edit.py -- editable lead-gen contacts.

PROBLEM: the only lead-contact PATCH (update_contact) filters on
LeadContact.lead_id == lead_id, so contacts attached to an EXISTING HOTEL
(lead_id NULL, existing_hotel_id set -- e.g. successor stubs like Laura Doner,
and discovered hotel contacts like Chiara Chappaz) have NO edit path. The UI
gates their fields read-only as a result.

FIX: add ONE parent-agnostic endpoint keyed on the lead_contact id alone:
    PATCH /api/lead-contacts/{contact_id}
Same allowed fields + rescore as update_contact. Works for both lead_id and
existing_hotel_id rows. Placed right after the GET /api/lead-contacts handler.

Anchored, idempotent, .bak, py_compile auto-restore.
"""
import py_compile
import shutil
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent
TARGET = REPO / "app" / "routes" / "contacts.py"
MARK = "# [patch_endpoint_leadcontact_edit]"


def main() -> int:
    if not TARGET.exists():
        print(f"ERROR: {TARGET} not found")
        return 1
    src = TARGET.read_text(encoding="utf-8")
    if MARK in src:
        print("Already patched. No-op.")
        return 0

    # Anchor: the final return of list_lead_contacts.
    anchor = (
        '    return {"items": items, "total": total, "page": page, "per_page": per_page, "pages": pages}\n'
    )
    if anchor not in src:
        print("ERROR: anchor (end of list_lead_contacts) not found. Aborting.")
        return 2
    if src.count(anchor) != 1:
        print(f"ERROR: anchor not unique ({src.count(anchor)}). Aborting.")
        return 2

    block = anchor + (
        "\n"
        f'@router.patch("/api/lead-contacts/{{contact_id}}")  {MARK}\n'
        "async def update_lead_contact_any(\n"
        "    contact_id: int,\n"
        "    request: Request,\n"
        "    db: AsyncSession = Depends(get_db),\n"
        "    _csrf=Depends(require_ajax),\n"
        "):\n"
        '    """Edit a lead-generator contact by its id, regardless of parent\n'
        "    (works for both lead_id and existing_hotel_id rows). Mirrors the\n"
        "    allowed fields + rescore of the lead-scoped update_contact, so\n"
        "    successor stubs and existing-hotel contacts are editable too.\n"
        '    """\n'
        "    body = await request.json()\n"
        "    contact = (\n"
        "        await db.execute(select(LeadContact).where(LeadContact.id == contact_id))\n"
        "    ).scalar_one_or_none()\n"
        "    if not contact:\n"
        '        raise HTTPException(status_code=404, detail="Contact not found")\n'
        "    allowed = {\n"
        '        "name", "title", "email", "secondary_email", "phone",\n'
        '        "linkedin", "organization", "evidence_url",\n'
        "    }\n"
        "    for fld, value in body.items():\n"
        "        if fld in allowed:\n"
        "            setattr(contact, fld, value)\n"
        "    # keep the parent lead's headline contact in sync when primary\n"
        "    if contact.is_primary and contact.lead_id:\n"
        "        lead = (\n"
        "            await db.execute(select(PotentialLead).where(PotentialLead.id == contact.lead_id))\n"
        "        ).scalar_one_or_none()\n"
        "        if lead:\n"
        "            lead.contact_name = contact.name\n"
        "            lead.contact_title = contact.title\n"
        "            lead.contact_email = contact.email\n"
        "            lead.contact_phone = contact.phone\n"
        "            lead.updated_at = local_now()\n"
        "    from app.services.contact_scoring import apply_score_to_contact\n"
        "\n"
        "    apply_score_to_contact(\n"
        "        contact,\n"
        "        title=contact.title,\n"
        "        scope=contact.scope,\n"
        "        strategist_priority=contact.strategist_priority,\n"
        "    )\n"
        "    contact.updated_at = local_now()\n"
        "    await db.flush()\n"
        "    if contact.lead_id:\n"
        "        try:\n"
        "            await rescore_lead(contact.lead_id, db)\n"
        "        except Exception:\n"
        "            pass\n"
        "    await db.commit()\n"
        '    return {"status": "updated", "contact_id": contact_id, "score": contact.score}\n'
    )

    out = src.replace(anchor, block, 1)
    bak = TARGET.with_suffix(TARGET.suffix + ".bak")
    shutil.copy2(TARGET, bak)
    TARGET.write_text(out, encoding="utf-8")
    try:
        py_compile.compile(str(TARGET), doraise=True)
    except py_compile.PyCompileError as e:
        shutil.copy2(bak, TARGET)
        print(f"ERROR: py_compile failed, restored:\n{e}")
        return 3
    print("PATCHED OK -> app/routes/contacts.py")
    print("  + PATCH /api/lead-contacts/{contact_id}  (parent-agnostic edit)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
