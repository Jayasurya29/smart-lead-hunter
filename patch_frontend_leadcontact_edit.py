#!/usr/bin/env python3
r"""patch_frontend_leadcontact_edit.py -- edit lead-gen contacts in the drawer.

Wires the new PATCH /api/lead-contacts/{id} into the UI so lead-generator
contacts (successor stubs, discovered-hotel contacts) are editable -- name,
title, email, phone, linkedin, organization -- just like inbox contacts.

  1. api/inboxContacts.ts  -> updateLeadContact(realId, fields)  [lead schema:
     name / linkedin / etc.]
  2. hooks/useInboxContacts.ts -> useUpdateLeadContact()  (invalidates lead list)
  3. pages/ContactsPage.tsx -> InfoRow gains an optional leadId; when set, save()
     routes to the lead endpoint with the real (un-offset) id and maps field
     names (first/last/display -> name, linkedin_url -> linkedin). Call sites
     pass leadId for lead-gen rows (previously they passed contactId=undefined,
     which made the row read-only).

Anchored, idempotent, .bak each. JSX/TS comments single-level.
Built against the CURRENT pasted ContactsPage.tsx.
"""
import shutil
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent
API = REPO / "frontend" / "src" / "api" / "inboxContacts.ts"
HOOK = REPO / "frontend" / "src" / "hooks" / "useInboxContacts.ts"
PAGE = REPO / "frontend" / "src" / "pages" / "ContactsPage.tsx"
MARK = "patch_frontend_leadcontact_edit"


def _edit(path: Path, edits, guard) -> int:
    src = path.read_text(encoding="utf-8")
    if guard in src:
        print(f"  {path.name}: already patched")
        return 0
    out = src
    for i, (a, b) in enumerate(edits, 1):
        if a not in out:
            print(f"  ERROR {path.name}: anchor {i} not found")
            return 2
        if out.count(a) != 1:
            print(f"  ERROR {path.name}: anchor {i} not unique ({out.count(a)})")
            return 2
        out = out.replace(a, b, 1)
    shutil.copy2(path, path.with_suffix(path.suffix + ".bak"))
    path.write_text(out, encoding="utf-8")
    print(f"  PATCHED {path.name}")
    return 0


def main() -> int:
    if not (API.exists() and HOOK.exists() and PAGE.exists()):
        print("ERROR: target file(s) missing")
        return 1

    # 1) api fn (lead schema field names)
    api_edits = [(
        "export async function updateInboxContact(\n"
        "  id: number,\n"
        "  fields: ContactEditFields,\n"
        "): Promise<InboxContact> {\n"
        "  const { data } = await api.patch<InboxContact>(`/api/inbox-contacts/${id}`, fields)\n"
        "  return data\n"
        "}\n",
        "export async function updateInboxContact(\n"
        "  id: number,\n"
        "  fields: ContactEditFields,\n"
        "): Promise<InboxContact> {\n"
        "  const { data } = await api.patch<InboxContact>(`/api/inbox-contacts/${id}`, fields)\n"
        "  return data\n"
        "}\n"
        "\n"
        f"// {MARK}: lead-gen contacts use the lead schema (name / linkedin)\n"
        "export interface LeadContactEditFields {\n"
        "  name?: string\n"
        "  title?: string\n"
        "  organization?: string\n"
        "  email?: string\n"
        "  secondary_email?: string\n"
        "  phone?: string\n"
        "  linkedin?: string\n"
        "}\n"
        "export async function updateLeadContact(\n"
        "  realId: number,\n"
        "  fields: LeadContactEditFields,\n"
        "): Promise<unknown> {\n"
        "  const { data } = await api.patch(`/api/lead-contacts/${realId}`, fields)\n"
        "  return data\n"
        "}\n",
    )]
    if _edit(API, api_edits, MARK) != 0:
        return 2

    # 2) hook
    hook_edits = []
    hook_src = HOOK.read_text(encoding="utf-8")
    if "updateLeadContact" not in hook_src.split("export function useUpdateInboxContact")[0]:
        hook_edits.append((
            "  updateInboxContact,\n",
            "  updateInboxContact,\n  updateLeadContact,\n",
        ))
    hook_edits.append((
        "export function useUpdateInboxContact() {\n",
        f"// {MARK}\n"
        "export function useUpdateLeadContact() {\n"
        "  const qc = useQueryClient()\n"
        "  return useMutation({\n"
        "    mutationFn: ({ realId, fields }: { realId: number; fields: import('../api/inboxContacts').LeadContactEditFields }) =>\n"
        "      updateLeadContact(realId, fields),\n"
        "    onSuccess: () => { qc.invalidateQueries({ queryKey: ['lead-contacts', 'all'] }) },\n"
        "  })\n"
        "}\n"
        "\n"
        "export function useUpdateInboxContact() {\n",
    ))
    if _edit(HOOK, hook_edits, MARK) != 0:
        return 2

    # 3) page: import hook, InfoRow gains leadId + routes save(), call sites pass leadId
    page_edits = []
    page_edits.append((
        "  useUpdateInboxContact,\n",
        "  useUpdateInboxContact,\n  useUpdateLeadContact,\n",
    ))
    # InfoRow signature: add leadId prop
    page_edits.append((
        "function InfoRow({ icon, label, value, mono, href, editField, contactId, placeholder }: {\n"
        "  icon: React.ReactNode; label: string; value: string | null | undefined; mono?: boolean; href?: string\n"
        "  editField?: string  // contacts column to edit; '__name__' edits first+last+display together\n"
        "  contactId?: number  // omit (lead-gen offset ids) to make the row copy-only\n"
        "  placeholder?: string\n"
        "}) {\n"
        "  const updMut = useUpdateInboxContact()\n",
        "function InfoRow({ icon, label, value, mono, href, editField, contactId, leadId, placeholder }: {\n"
        "  icon: React.ReactNode; label: string; value: string | null | undefined; mono?: boolean; href?: string\n"
        "  editField?: string  // contacts column to edit; '__name__' edits first+last+display together\n"
        "  contactId?: number  // inbox-contact id -> edits via /api/inbox-contacts\n"
        f"  leadId?: number     // {MARK}: real lead_contact id -> edits via /api/lead-contacts\n"
        "  placeholder?: string\n"
        "}) {\n"
        "  const updMut = useUpdateInboxContact()\n"
        "  const leadMut = useUpdateLeadContact()\n",
    ))
    # canEdit: allow when either id present
    page_edits.append((
        "  const canEdit = !!editField && !!contactId\n",
        "  const canEdit = !!editField && (!!contactId || !!leadId)\n",
    ))
    # save(): route to lead endpoint when leadId set
    page_edits.append((
        "    const fields = editField === '__name__'\n"
        "      ? (() => { const p = newVal.split(/\\s+/); return { first_name: p[0] || '', last_name: p.slice(1).join(' ') || '', display_name: newVal } })()\n"
        "      : { [editField!]: newVal }\n"
        "    try { await updMut.mutateAsync({ id: contactId!, fields: fields as any }) }\n"
        "    catch { /* error surfaced by the api interceptor; field reverts on refetch */ }\n",
        f"    // {MARK}: lead-gen rows -> lead endpoint, mapping to the lead schema\n"
        "    if (leadId) {\n"
        "      const lf: any = editField === '__name__' ? { name: newVal }\n"
        "        : editField === 'linkedin_url' ? { linkedin: newVal }\n"
        "        : { [editField!]: newVal }\n"
        "      try { await leadMut.mutateAsync({ realId: leadId, fields: lf }) }\n"
        "      catch { /* surfaced by interceptor; reverts on refetch */ }\n"
        "      return\n"
        "    }\n"
        "    const fields = editField === '__name__'\n"
        "      ? (() => { const p = newVal.split(/\\s+/); return { first_name: p[0] || '', last_name: p.slice(1).join(' ') || '', display_name: newVal } })()\n"
        "      : { [editField!]: newVal }\n"
        "    try { await updMut.mutateAsync({ id: contactId!, fields: fields as any }) }\n"
        "    catch { /* error surfaced by the api interceptor; field reverts on refetch */ }\n",
    ))

    rc = _edit(PAGE, page_edits, MARK)
    if rc != 0:
        return 2

    # 4) call sites: pass leadId for lead-gen rows. Each InfoRow in the Contact +
    #    Hospitality cards currently has:
    #      contactId={sourceOf(contact) !== 'lead_generator' ? contact.id : undefined}
    #    Add leadId for the lead case. Do this as a blanket replace of that exact
    #    expression (appears on name/email/phone/linkedin/organization rows).
    page_src2 = PAGE.read_text(encoding="utf-8")
    old_expr = "contactId={sourceOf(contact) !== 'lead_generator' ? contact.id : undefined}"
    new_expr = (
        "contactId={sourceOf(contact) !== 'lead_generator' ? contact.id : undefined}\n"
        "              leadId={sourceOf(contact) === 'lead_generator' || contact.id >= LEAD_ID_OFFSET "
        "? (contact.id >= LEAD_ID_OFFSET ? contact.id - LEAD_ID_OFFSET : contact.id) : undefined}"
    )
    n = page_src2.count(old_expr)
    if n == 0:
        print("  WARN: no InfoRow call sites matched -- lead rows may stay read-only.")
    else:
        page_src2 = page_src2.replace(old_expr, new_expr)
        PAGE.write_text(page_src2, encoding="utf-8")
        print(f"  wired leadId on {n} InfoRow call site(s)")

    print("\nDone. Now: cd frontend && npm run build")
    return 0


if __name__ == "__main__":
    sys.exit(main())
