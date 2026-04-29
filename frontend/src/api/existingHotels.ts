/**
 * Existing Hotels API
 * ===================
 * Mirrors api/leads.ts but targets /api/existing-hotels. Same data shapes,
 * same operations — schema parity from migration 018 means we can re-use
 * Lead and Contact types directly.
 *
 * NOTE: there is intentionally no `transfer` helper here. Transfers from
 * potential_leads → existing_hotels are operator-driven via the
 * scripts/transfer_to_existing.py PowerShell script, not a UI button.
 */
import api from './client'
import type { Lead, Contact } from './types'

/* ────────────────────────────────────────────────────────
   Types — same shape as Lead, plus client-specific fields
   ──────────────────────────────────────────────────────── */

export interface ExistingHotel extends Lead {
  is_client: boolean
  sap_bp_code: string | null
  client_notes: string | null
  pushed_to_map: boolean
  data_source: string | null
  zone: string | null
  former_names: string[] | null
  search_name: string | null
  last_verified_at: string | null
}

export interface ExistingHotelListResponse {
  hotels: ExistingHotel[]
  total: number
  page: number
  per_page: number
  pages: number
}

export interface ExistingHotelStats {
  total: number
  clients: number
  prospects: number
  geocoded: number
  with_contact: number
  with_tier: number
  on_map: number
  tiers: Record<string, number>
  top_states: { state: string; code?: string; count: number; is_caribbean?: boolean }[]
  zones: { zone: string; key?: string | null; state?: string | null; priority?: string | null; count: number }[]
}

export interface ExistingHotelFilters {
  page?: number
  per_page?: number
  status?: string
  search?: string
  state?: string
  city?: string
  brand_tier?: string
  chain?: string
  is_client?: string
  has_contact?: string
  zone?: string
  sort?: string
}

/* ────────────────────────────────────────────────────────
   List + stats
   ──────────────────────────────────────────────────────── */

export async function fetchExistingHotels(
  filters: ExistingHotelFilters = {},
): Promise<ExistingHotelListResponse> {
  const params = new URLSearchParams()
  if (filters.page)        params.set('page', String(filters.page))
  if (filters.per_page)    params.set('per_page', String(filters.per_page))
  if (filters.status)      params.set('status', filters.status)
  if (filters.search)      params.set('search', filters.search)
  if (filters.state)       params.set('state', filters.state)
  if (filters.city)        params.set('city', filters.city)
  if (filters.brand_tier)  params.set('brand_tier', filters.brand_tier)
  if (filters.chain)       params.set('chain', filters.chain)
  if (filters.is_client)   params.set('is_client', filters.is_client)
  if (filters.has_contact) params.set('has_contact', filters.has_contact)
  if (filters.zone)        params.set('zone', filters.zone)
  if (filters.sort)        params.set('sort', filters.sort)

  const { data } = await api.get<ExistingHotelListResponse>(
    `/api/existing-hotels?${params}`,
  )
  return data
}

export async function fetchExistingHotel(id: number): Promise<ExistingHotel> {
  const { data } = await api.get<ExistingHotel>(`/api/existing-hotels/${id}`)
  return data
}

export async function fetchExistingHotelStats(): Promise<ExistingHotelStats> {
  const { data } = await api.get<ExistingHotelStats>('/api/existing-hotels/stats')
  return data
}

/* ────────────────────────────────────────────────────────
   Workflow actions
   ──────────────────────────────────────────────────────── */

export async function approveExistingHotel(id: number): Promise<void> {
  await api.post(`/api/existing-hotels/${id}/approve`)
}

export async function rejectExistingHotel(id: number, reason?: string): Promise<void> {
  await api.post(`/api/existing-hotels/${id}/reject`, { reason: reason || '' })
}

export async function restoreExistingHotel(id: number): Promise<void> {
  await api.post(`/api/existing-hotels/${id}/restore`)
}

export async function editExistingHotel(
  id: number, fields: Partial<ExistingHotel>,
): Promise<ExistingHotel> {
  const { data } = await api.patch(`/api/existing-hotels/${id}`, fields)
  return data
}

/* ────────────────────────────────────────────────────────
   Contacts
   ──────────────────────────────────────────────────────── */

export async function fetchHotelContacts(hotelId: number): Promise<Contact[]> {
  const { data } = await api.get<Contact[]>(`/api/existing-hotels/${hotelId}/contacts`)
  return Array.isArray(data) ? data : []
}

export async function saveHotelContact(hotelId: number, contactId: number): Promise<void> {
  await api.post(`/api/existing-hotels/${hotelId}/contacts/${contactId}/save`)
}

export async function unsaveHotelContact(hotelId: number, contactId: number): Promise<void> {
  await api.post(`/api/existing-hotels/${hotelId}/contacts/${contactId}/unsave`)
}

export async function deleteHotelContact(hotelId: number, contactId: number): Promise<void> {
  await api.delete(`/api/existing-hotels/${hotelId}/contacts/${contactId}`)
}

export async function setPrimaryHotelContact(hotelId: number, contactId: number): Promise<void> {
  await api.post(`/api/existing-hotels/${hotelId}/contacts/${contactId}/set-primary`)
}

export async function updateHotelContact(
  hotelId: number, contactId: number, fields: Record<string, string>,
): Promise<void> {
  await api.patch(`/api/existing-hotels/${hotelId}/contacts/${contactId}`, fields)
}

export async function addHotelContact(
  hotelId: number, fields: Record<string, string>,
): Promise<any> {
  const { data } = await api.post(`/api/existing-hotels/${hotelId}/contacts`, fields)
  return data
}

export async function toggleHotelContactScope(
  hotelId: number, contactId: number, scope: string,
): Promise<any> {
  const { data } = await api.post(
    `/api/existing-hotels/${hotelId}/contacts/${contactId}/toggle-scope`,
    { scope },
  )
  return data
}

/* Wiza email lookup — single contact. Mirrors enrichContactEmail in api/leads. */
export async function enrichHotelContactEmail(
  hotelId: number, contactId: number,
): Promise<{ status: 'found' | 'not_found'; email?: string; email_status?: string; confidence?: string; credits_used?: number; message?: string }> {
  const { data } = await api.post(
    `/api/existing-hotels/${hotelId}/contacts/${contactId}/enrich-email`,
    {},
  )
  return data
}

/* ────────────────────────────────────────────────────────
   Enrichment status
   ──────────────────────────────────────────────────────── */

export async function getHotelEnrichmentStatus(hotelId: number): Promise<any> {
  const { data } = await api.get(`/api/existing-hotels/${hotelId}/enrich-status`)
  return data
}

/* Get current Smart Fill (in-flight) status for an existing hotel.
   Used on hotel detail mount to decide whether to attach to a running
   Smart Fill or show the idle Smart Fill button. */
export async function getHotelSmartFillStatus(hotelId: number): Promise<{ running: boolean; mode: string | null }> {
  const { data } = await api.get(`/api/existing-hotels/${hotelId}/smart-fill-status`)
  return data
}

export async function cancelHotelEnrichment(hotelId: number): Promise<any> {
  const { data } = await api.post(`/api/existing-hotels/${hotelId}/enrich-cancel`)
  return data
}
