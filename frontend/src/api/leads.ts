import api from './client'
import type { Lead, LeadListResponse, DashboardStats, Contact, SourcesListResponse } from './types'

/* ════════════════════════════════════════
   LEADS
   ════════════════════════════════════════ */

export interface LeadFilters {
  page?: number
  per_page?: number
  status?: string
  search?: string
  min_score?: number
  location?: string
  brand_tier?: string
  timeline?: string
  year?: string
  added?: string
  sort?: string
}

/* Map frontend sort keys → backend sort keys */
const SORT_MAP: Record<string, string> = {
  score_high:   'score_desc',
  score_low:    'score_asc',
  hotel_az:     'name_asc',
  hotel_za:     'name_desc',
  newest:       'newest',
  oldest:       'oldest',
  opening_soon: 'opening_asc',
  opening_late: 'opening_desc',
  tier_asc:     'tier_asc',
  tier_desc:    'tier_desc',
  time_asc:     'time_asc',
  time_desc:    'time_desc',
  location_az:  'location_asc',
  location_za:  'location_desc',
}

export async function fetchLeads(filters: LeadFilters = {}): Promise<LeadListResponse> {
  const params = new URLSearchParams()
  if (filters.page)       params.set('page', String(filters.page))
  if (filters.per_page)   params.set('per_page', String(filters.per_page))
  if (filters.status)     params.set('status', filters.status)
  if (filters.search)     params.set('search', filters.search)
  if (filters.min_score)  params.set('min_score', String(filters.min_score))
  if (filters.location)   params.set('location', filters.location)
  if (filters.brand_tier) params.set('brand_tier', filters.brand_tier)
  if (filters.timeline)   params.set('timeline', filters.timeline)
  if (filters.year)       params.set('year', filters.year)
  if (filters.added)      params.set('added', filters.added)
  if (filters.sort)       params.set('sort', SORT_MAP[filters.sort] || filters.sort)

  const { data } = await api.get<LeadListResponse>(`/leads?${params}`)
  return data
}

export async function fetchLead(id: number): Promise<Lead> {
  const { data } = await api.get<Lead>(`/leads/${id}`)
  return data
}

/* ════════════════════════════════════════
   LEAD ACTIONS
   ════════════════════════════════════════ */

export async function approveLead(id: number): Promise<void> {
  await api.post(`/api/dashboard/leads/${id}/approve`)
}

export async function rejectLead(id: number, reason?: string): Promise<void> {
  const params = reason ? `?reason=${encodeURIComponent(reason)}` : ''
  await api.post(`/api/dashboard/leads/${id}/reject${params}`)
}

export async function restoreLead(id: number): Promise<void> {
  await api.post(`/api/dashboard/leads/${id}/restore`)
}

export async function deleteLead(id: number): Promise<void> {
  await api.post(`/api/dashboard/leads/${id}/delete`)
}

export async function editLead(id: number, fields: Partial<Lead>): Promise<any> {
  const { data } = await api.patch(`/api/dashboard/leads/${id}/edit`, fields)
  return data
}

export async function enrichLead(id: number): Promise<any> {
  const { data } = await api.post(`/api/dashboard/leads/${id}/enrich`)
  return data
}

/* ════════════════════════════════════════
   STATS
   ════════════════════════════════════════ */

export async function fetchStats(): Promise<DashboardStats> {
  const { data } = await api.get<DashboardStats>('/stats')
  return data
}

/* ════════════════════════════════════════
   CONTACTS
   ════════════════════════════════════════ */

export async function fetchContacts(leadId: number): Promise<Contact[]> {
  const { data } = await api.get<Contact[]>(`/api/dashboard/leads/${leadId}/contacts`)
  return Array.isArray(data) ? data : []
}

export async function saveContact(leadId: number, contactId: number): Promise<void> {
  await api.post(`/api/dashboard/leads/${leadId}/contacts/${contactId}/save`)
}

export async function unsaveContact(leadId: number, contactId: number): Promise<void> {
  await api.post(`/api/dashboard/leads/${leadId}/contacts/${contactId}/unsave`)
}

export async function deleteContact(leadId: number, contactId: number): Promise<void> {
  await api.delete(`/api/dashboard/leads/${leadId}/contacts/${contactId}`)
}

export async function setPrimaryContact(leadId: number, contactId: number): Promise<void> {
  await api.post(`/api/dashboard/leads/${leadId}/contacts/${contactId}/set-primary`)
}

export async function addContact(leadId: number, data: Record<string, string>): Promise<any> {
  const { data: result } = await api.post(`/api/dashboard/leads/${leadId}/contacts/add`, data)
  return result
}

export async function updateContact(leadId: number, contactId: number, data: Record<string, string>): Promise<void> {
  await api.patch(`/api/dashboard/leads/${leadId}/contacts/${contactId}`, data)
}

export async function toggleContactScope(leadId: number, contactId: number, scope: string): Promise<any> {
  const { data } = await api.post(`/api/dashboard/leads/${leadId}/contacts/${contactId}/toggle-scope`, { scope })
  return data
}
/* ════════════════════════════════════════
   SCRAPE
   ════════════════════════════════════════ */

export async function triggerScrape(mode: string, sourceIds: number[] = []): Promise<any> {
  const { data } = await api.post('/api/dashboard/scrape', { mode, source_ids: sourceIds })
  return data
}

export async function cancelScrape(scrapeId: string): Promise<void> {
  await api.post(`/api/dashboard/scrape/cancel/${scrapeId}`)
}

/* ════════════════════════════════════════
   URL EXTRACT
   ════════════════════════════════════════ */

export async function triggerExtractUrl(url: string): Promise<any> {
  const { data } = await api.post('/api/dashboard/extract-url', { url })
  return data
}

/* ════════════════════════════════════════
   DISCOVERY
   ════════════════════════════════════════ */

export async function triggerDiscovery(
  mode: string = 'full',
  extractLeads: boolean = true,
  extraPayload?: Record<string, unknown>,
): Promise<any> {
  const payload = {
    mode,
    extract_leads: extractLeads,
    ...(extraPayload || {}),
  }
  const { data } = await api.post('/api/dashboard/discovery/start', payload)
  return data
}

export async function cancelDiscovery(discoveryId: string): Promise<any> {
  const { data } = await api.post('/api/dashboard/discovery/cancel', { discovery_id: discoveryId })
  return data
}

export async function checkDiscoveryStatus(discoveryId: string): Promise<any> {
  const { data } = await api.get(`/api/dashboard/discovery/status?discovery_id=${discoveryId}`)
  return data
}

/* ════════════════════════════════════════
   SOURCES
   ════════════════════════════════════════ */

export async function fetchSources(): Promise<any> {
  const { data } = await api.get('/api/dashboard/sources/list')
  return data
}
