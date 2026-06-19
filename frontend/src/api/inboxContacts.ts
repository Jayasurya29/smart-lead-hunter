import api from './client'

/* ════════════════════════════════════════
   TYPES
   ════════════════════════════════════════ */

export interface InboxContact {
  id: number
  email: string
  first_name: string | null
  last_name: string | null
  display_name: string | null
  title: string | null
  organization: string | null
  phone: string | null
  address: string | null
  linkedin_url: string | null
  org_source: string | null
  has_signature: boolean
  confidence: number | null
  parent_company: string | null
  brand_tier: string | null
  operating_model: string | null
  gpo: string | null
  procurement_priority: string
  priority_reason: string | null
  contact_category: string | null
  manual_category: string | null
  category_source: string | null
  inferred_role: string | null
  seniority: string | null
  department: string | null
  is_decision_maker: boolean | null
  background: string | null
  enrichment_source: string | null
  enrichment_confidence: number | null
  opportunity_level: string | null
  opportunity_score: number | null
  buying_signal_score: number | null
  buying_signal_stage: string | null
  buying_signal_reason: string | null
  buying_signal_deal: string | null
  buying_signal_label: string | null
  buying_signal_team: Array<{ name: string | null; email: string; org: string | null }> | null
  buying_signal_products: string | null
  management_company: string | null
  interaction_count: number
  source_mailboxes: string[]
  first_seen: string | null
  last_seen: string | null
  first_message_at: string | null
  last_inbound_at: string | null
  last_outbound_at: string | null
  secondary_email: string | null
  approval_status: string
  insightly_contact_id: string | null
  pushed_to_insightly_at: string | null
  matched_lead_id: number | null
  matched_hotel_id: number | null
  sync_history: Array<{ action: string; mailbox: string | null; ts: string }> | null
  created_at: string
  updated_at: string
}

export interface InboxContactStats {
  total: number
  p1: number
  p2: number
  p3: number
  p4: number
  p_unknown: number
  pending: number
  approved: number
  pushed_to_insightly: number
  new_today: number
  with_signature: number
  with_phone: number
  buyer: number
  seller: number
  competitor: number
  personal: number
  junk: number
  uncategorized: number
  decision_makers: number
  last_sync_at: string | null
}

export interface InboxContactListResponse {
  items: InboxContact[]
  total: number
  page: number
  per_page: number
  pages: number
}

export interface InboxContactFilters {
  page?: number
  per_page?: number
  procurement_priority?: string
  contact_category?: string
  approval_status?: string
  brand_tier?: string
  gpo?: string
  source_mailbox?: string
  has_signature?: boolean
  organization?: string
  search?: string
  matched_only?: boolean
  order_by?: string
}

/* ════════════════════════════════════════
   API CALLS
   ════════════════════════════════════════ */

export async function fetchInboxContacts(
  filters: InboxContactFilters = {},
): Promise<InboxContactListResponse> {
  const params = new URLSearchParams()
  if (filters.page)                   params.set('page', String(filters.page))
  if (filters.per_page)               params.set('per_page', String(filters.per_page))
  if (filters.procurement_priority)   params.set('procurement_priority', filters.procurement_priority)
  if (filters.contact_category)       params.set('contact_category', filters.contact_category)
  if (filters.approval_status)        params.set('approval_status', filters.approval_status)
  if (filters.brand_tier)             params.set('brand_tier', filters.brand_tier)
  if (filters.gpo)                    params.set('gpo', filters.gpo)
  if (filters.source_mailbox)         params.set('source_mailbox', filters.source_mailbox)
  if (filters.has_signature != null)  params.set('has_signature', String(filters.has_signature))
  if (filters.organization)           params.set('organization', filters.organization)
  if (filters.search)                 params.set('search', filters.search)
  if (filters.matched_only != null)   params.set('matched_only', String(filters.matched_only))
  if (filters.order_by)               params.set('order_by', filters.order_by)

  const { data } = await api.get<InboxContactListResponse>(`/api/inbox-contacts?${params}`)
  return data
}

/**
 * Lead-generator contacts (lead_contacts joined to their potential lead /
 * existing hotel), served by the backend already shaped like InboxContact
 * with source='lead_generator' + explicit account_type / lifecycle_stage.
 */
export async function fetchLeadContacts(page = 1, per_page = 500): Promise<InboxContactListResponse> {
  const { data } = await api.get<InboxContactListResponse>(`/api/lead-contacts?page=${page}&per_page=${per_page}`)
  return data
}

export async function fetchInboxContactStats(): Promise<InboxContactStats> {
  const { data } = await api.get<InboxContactStats>('/api/inbox-contacts/stats')
  return data
}

export async function fetchInboxContact(id: number): Promise<InboxContact> {
  const { data } = await api.get<InboxContact>(`/api/inbox-contacts/${id}`)
  return data
}

export async function approveInboxContact(id: number): Promise<InboxContact> {
  const { data } = await api.post<InboxContact>(`/api/inbox-contacts/${id}/approve`)
  return data
}

export async function bulkApproveInboxContacts(ids: number[]): Promise<{ approved: number; failed: Array<{ id: number; reason: string }> }> {
  const { data } = await api.post('/api/inbox-contacts/bulk-approve', { ids })
  return data
}

export async function deleteInboxContact(id: number): Promise<void> {
  await api.delete(`/api/inbox-contacts/${id}`)
}

export async function matchInboxContactToLead(id: number, leadId: number | null): Promise<InboxContact> {
  const { data } = await api.post<InboxContact>(`/api/inbox-contacts/${id}/match-lead`, { lead_id: leadId })
  return data
}

export async function matchInboxContactToHotel(id: number, hotelId: number | null): Promise<InboxContact> {
  const { data } = await api.post<InboxContact>(`/api/inbox-contacts/${id}/match-hotel`, { hotel_id: hotelId })
  return data
}

export async function triggerInboxSync(): Promise<{ status: string; task_id?: string; message: string }> {
  const { data } = await api.post('/api/inbox-contacts/sync')
  return data
}

export interface DeepEnrichResult {
  contact_id: number
  name: string
  role: string | null
  seniority: string | null
  department: string | null
  is_decision_maker: boolean | null
  background: string | null
  found_email: string | null
  confidence: number
  sources_used: number
}

export interface FindLinkedinResult {
  found: boolean
  linkedin_url: string | null
  note?: string
}

export async function findContactLinkedin(id: number): Promise<FindLinkedinResult> {
  const { data } = await api.post<FindLinkedinResult>(
    `/api/contacts/${id}/find-linkedin`,
  )
  return data
}

export async function deepEnrichContact(
  id: number,
  findEmail = false,
): Promise<DeepEnrichResult> {
  const { data } = await api.post<DeepEnrichResult>(
    `/api/contacts/${id}/enrich-deep?find_email=${findEmail}`,
  )
  return data
}

/* [patch_frontend_current_employer] */
export interface CurrentEmployerResult {
  found: boolean
  relationship?: 'same' | 'moved' | 'unknown'
  moved?: boolean
  same?: boolean
  current_employer?: string
  current_title?: string
  company_domain?: string
  citations?: string[]
  source?: string
  on_file_org?: string
  mode?: 'preview' | 'apply'
  // apply-mode (deep-enrich) extras:
  background?: string | null
  role?: string | null
  employer_changed?: boolean
  former_employer?: string | null
  secondary_email?: string | null
  note?: string
}

export async function findCurrentEmployer(
  id: number,
  opts: { apply?: boolean; useWiza?: boolean; findEmail?: boolean } = {},
): Promise<CurrentEmployerResult> {
  const qs = new URLSearchParams()
  if (opts.apply) qs.set('apply', 'true')
  if (opts.useWiza) qs.set('use_wiza', 'true')
  if (opts.findEmail) qs.set('find_email', 'true')
  const { data } = await api.post<CurrentEmployerResult>(
    `/api/contacts/${id}/find-current-employer?${qs.toString()}`,
  )
  return data
}

export interface ContactEditFields {
  first_name?: string
  last_name?: string
  display_name?: string
  title?: string
  organization?: string
  email?: string
  phone?: string
  linkedin_url?: string
}

export async function updateInboxContact(
  id: number,
  fields: ContactEditFields,
): Promise<InboxContact> {
  const { data } = await api.patch<InboxContact>(`/api/inbox-contacts/${id}`, fields)
  return data
}

/* ── learning junk system ── */

export async function junkContact(id: number): Promise<void> {
  await api.post(`/api/contacts/${id}/junk`)
}

export async function unjunkContact(id: number): Promise<void> {
  await api.post(`/api/contacts/${id}/unjunk`)
}

export async function junkContactsBulk(ids: number[]): Promise<{ junked: number }> {
  const { data } = await api.post('/api/contacts/junk-bulk', { ids })
  return data
}

export async function junkDomain(
  domain: string,
  reason?: string,
): Promise<{ domain: string; existing_contacts: number; flipped_to_junk: number }> {
  const { data } = await api.post('/api/contacts/junk-domain', { domain, reason })
  return data
}

export async function unjunkDomain(domain: string): Promise<{ domain: string; restored: number }> {
  const { data } = await api.post('/api/contacts/unjunk-domain', { domain })
  return data
}

export interface JunkDomainRule {
  domain: string
  added_at: string | null
  added_by: string | null
  reason: string | null
  contacts_at_add: number
}

export async function fetchJunkDomains(): Promise<{ domains: JunkDomainRule[] }> {
  const { data } = await api.get('/api/contacts/junk-domains')
  return data
}

export async function fetchJunkSuggestions(
  threshold = 3,
): Promise<{ suggestions: Array<{ domain: string; manual_junked: number }> }> {
  const { data } = await api.get(`/api/contacts/junk-domain-suggestions?threshold=${threshold}`)
  return data
}
