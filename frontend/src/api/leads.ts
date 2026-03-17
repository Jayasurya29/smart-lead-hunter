import api from './client'
import type { Lead, LeadListResponse, DashboardStats, Contact } from './types'

// ── Leads ──

export interface LeadFilters {
  page?: number
  per_page?: number
  status?: string
  search?: string
  min_score?: number
  location_type?: string
  brand_tier?: string
  timeline?: string
  year?: string
  added?: string
  sort?: string
}

export async function fetchLeads(filters: LeadFilters = {}): Promise<LeadListResponse> {
  const params = new URLSearchParams()
  if (filters.page) params.set('page', String(filters.page))
  if (filters.per_page) params.set('per_page', String(filters.per_page))
  if (filters.status) params.set('status', filters.status)
  if (filters.search) params.set('search', filters.search)
  if (filters.min_score) params.set('min_score', String(filters.min_score))
  if (filters.location_type) params.set('location_type', filters.location_type)
  if (filters.brand_tier) params.set('brand_tier', filters.brand_tier)
  if (filters.timeline) params.set('timeline', filters.timeline)
  if (filters.year) params.set('year', filters.year)
  if (filters.added) params.set('added', filters.added)
  if (filters.sort) params.set('sort', filters.sort)

  const res = await api.get<LeadListResponse>(`/leads?${params}`)
  return res.data
}

export async function fetchLead(id: number): Promise<Lead> {
  const { data } = await api.get<Lead>(`/leads/${id}`)
  return data
}

// REST JSON endpoints (not the HTMX /api/dashboard/ ones that return HTML)
export async function approveLead(id: number): Promise<Lead> {
  const { data } = await api.post<Lead>(`/leads/${id}/approve`)
  return data
}

export async function rejectLead(id: number, reason?: string): Promise<Lead> {
  const params = reason ? `?reason=${encodeURIComponent(reason)}` : ''
  const { data } = await api.post<Lead>(`/leads/${id}/reject${params}`)
  return data
}

export async function restoreLead(id: number): Promise<Lead> {
  // Uses the HTMX endpoint — but we Accept JSON via client interceptor
  const { data } = await api.post(`/api/dashboard/leads/${id}/restore`)
  return data
}

export async function deleteLead(id: number): Promise<void> {
  // Soft-delete (moves to "deleted" tab, can be restored)
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

// ── Stats ──
export async function fetchStats(): Promise<DashboardStats> {
  const { data } = await api.get<DashboardStats>('/stats')
  return data
}

// ── Contacts ──

export async function fetchContacts(leadId: number): Promise<Contact[]> {
  const { data } = await api.get<Contact[]>(`/api/dashboard/leads/${leadId}/contacts`)
  return data
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

// ── Scrape ──

export async function triggerScrape(mode: string = 'full', sourceIds: number[] = []): Promise<any> {
  const { data } = await api.post('/api/dashboard/scrape', { mode, source_ids: sourceIds })
  return data
}

// ── Discovery ──

export async function triggerDiscovery(queries: number = 10): Promise<any> {
  const { data } = await api.post('/api/dashboard/discovery/start', { mode: 'full', extract_leads: true, queries })
  return data
}

// ── Extract URL ──

export async function triggerExtractUrl(url: string): Promise<any> {
  const { data } = await api.post('/api/dashboard/extract-url', { url })
  return data
}

// ── Sources ──
export async function fetchSources(): Promise<any> {
  const { data } = await api.get('/api/dashboard/sources/list')
  return data
}