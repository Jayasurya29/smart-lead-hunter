import api from './client'
import type { Lead, LeadListResponse, DashboardStats, Contact, SourcesListResponse } from './types'

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
  Object.entries(filters).forEach(([key, val]) => {
    if (val !== undefined && val !== null && val !== '') {
      params.set(key, String(val))
    }
  })
  const { data } = await api.get<LeadListResponse>(`/leads?${params}`)
  return data
}

export async function fetchLead(id: number): Promise<Lead> {
  const { data } = await api.get<Lead>(`/leads/${id}`)
  return data
}

// Uses REST JSON endpoints that return LeadResponse
export async function approveLead(id: number): Promise<Lead> {
  const { data } = await api.post<Lead>(`/leads/${id}/approve`)
  return data
}

export async function rejectLead(id: number, reason?: string): Promise<Lead> {
  const params = reason ? `?reason=${encodeURIComponent(reason)}` : ''
  const { data } = await api.post<Lead>(`/leads/${id}/reject${params}`)
  return data
}

// Restore uses PATCH on the REST endpoint to set status back to "new"
export async function restoreLead(id: number): Promise<Lead> {
  const { data } = await api.patch<Lead>(`/leads/${id}`, { status: 'new', rejection_reason: null })
  return data
}

// Soft-delete via PATCH (set status to "deleted")
export async function deleteLead(id: number): Promise<Lead> {
  const { data } = await api.patch<Lead>(`/leads/${id}`, { status: 'deleted' })
  return data
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

export async function triggerScrape(mode: string, sourceIds: number[] = []): Promise<{ scrape_id: string }> {
  const { data } = await api.post('/api/dashboard/scrape', { mode, source_ids: sourceIds })
  return data
}

export async function cancelScrape(scrapeId: string): Promise<void> {
  await api.post(`/api/dashboard/scrape/cancel/${scrapeId}`)
}

// ── Extract URL ──

export async function triggerExtractUrl(url: string): Promise<{ extract_id: string }> {
  const { data } = await api.post('/api/dashboard/extract-url', { url })
  return data
}

// ── Discovery ──

export async function triggerDiscovery(mode: string = 'full', extractLeads: boolean = true): Promise<{ discovery_id: string }> {
  const { data } = await api.post('/api/dashboard/discovery/start', { mode, extract_leads: extractLeads })
  return data
}

// ── Sources ──

export async function fetchSources(): Promise<SourcesListResponse> {
  const { data } = await api.get<SourcesListResponse>('/api/dashboard/sources/list')
  return data
}

// ── SSE Stream Helper ──

export function createSSEStream(path: string): EventSource {
  const token = localStorage.getItem('slh_token')
  const sep = path.includes('?') ? '&' : '?'
  const url = token ? `${path}${sep}api_key=${token}` : path
  return new EventSource(url)
}
