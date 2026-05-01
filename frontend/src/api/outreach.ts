import api from '@/api/client'

export type ApprovalStatus = 'pending' | 'approved' | 'rejected' | 'sent'

export interface ResearchSource {
  url: string
  title: string
  snippet: string
  category: string
}

export interface ResearchRecord {
  id: number
  lead_id: number | null
  existing_hotel_id: number | null
  lead_contact_id: number | null
  contact_name: string
  contact_title: string | null
  hotel_name: string
  hotel_location: string | null
  linkedin_url: string | null
  email: string | null
  company_summary: string | null
  contact_summary: string | null
  pain_points: string[]
  signals: string[]
  outreach_angle: string | null
  personalization_hook: string | null
  hotel_tier: string | null
  hiring_signals: string[]
  recent_news: string[]
  fit_score: number | null
  value_props: string[]
  email_subject: string | null
  email_body: string | null
  linkedin_message: string | null
  quality_approved: boolean | null
  quality_feedback: string | null
  send_time: string | null
  follow_up_sequence: string[]
  approval_status: ApprovalStatus
  approval_notes: string | null
  research_confidence: 'high' | 'medium' | 'low' | null
  sources: ResearchSource[]
  created_at: string | null
  updated_at: string | null
  sent_at: string | null
}

export interface OutreachListResponse {
  rows: ResearchRecord[]
  total: number
  page: number
  per_page: number
  pages: number
}

export interface OutreachStats {
  pending: number
  approved: number
  rejected: number
  sent: number
  total: number
}

export interface GenerateRequest {
  contact_id?: number
  parent_kind?: 'lead' | 'existing_hotel'
  parent_id?: number
  // Manual entry path (only when contact_id is missing)
  contact_name?: string
  contact_title?: string
  hotel_name?: string
  hotel_location?: string
  linkedin_url?: string
  email?: string
}

export async function generateOutreach(req: GenerateRequest): Promise<ResearchRecord> {
  const { data } = await api.post('/api/outreach/generate', req)
  return data
}

export async function listOutreach(params: {
  page?: number
  per_page?: number
  status?: string
  search?: string
  min_score?: number
}): Promise<OutreachListResponse> {
  const { data } = await api.get('/api/outreach', { params })
  return data
}

export async function getOutreachStats(): Promise<OutreachStats> {
  const { data } = await api.get('/api/outreach/stats')
  return data
}

export async function getOutreach(id: number): Promise<ResearchRecord> {
  const { data } = await api.get(`/api/outreach/${id}`)
  return data
}

export async function updateOutreach(
  id: number,
  patch: { email_subject?: string; email_body?: string; linkedin_message?: string; approval_notes?: string },
): Promise<ResearchRecord> {
  const { data } = await api.patch(`/api/outreach/${id}`, patch)
  return data
}

export async function approveOutreach(id: number): Promise<ResearchRecord> {
  const { data } = await api.post(`/api/outreach/${id}/approve`)
  return data
}

export async function rejectOutreach(id: number, feedback?: string): Promise<ResearchRecord> {
  const { data } = await api.post(`/api/outreach/${id}/reject`, { feedback })
  return data
}

export async function markSent(id: number): Promise<ResearchRecord> {
  const { data } = await api.post(`/api/outreach/${id}/mark-sent`)
  return data
}

export async function revertToPending(id: number): Promise<ResearchRecord> {
  const { data } = await api.post(`/api/outreach/${id}/revert-to-pending`)
  return data
}

export interface SequenceTouch {
  day: number
  type: string
  subject: string
  body: string
}

export async function generateSequence(id: number): Promise<{ touches: SequenceTouch[] }> {
  const { data } = await api.post(`/api/outreach/${id}/sequence`)
  return data
}

/* ════════════════════════════════════════════════════════════════════
   Picker — autocomplete hotels + load their contacts so the composer
   can auto-fill all fields when the user picks a contact.
   ════════════════════════════════════════════════════════════════════ */

export type HotelKind = 'lead' | 'existing_hotel'

export interface HotelSearchHit {
  kind: HotelKind
  id: number
  hotel_name: string
  brand: string | null
  location: string
  country: string | null
  score: number
}

export async function searchHotelsForOutreach(query: string): Promise<HotelSearchHit[]> {
  if (!query || query.trim().length < 2) return []
  const { data } = await api.get('/api/outreach/search-hotels', {
    params: { q: query.trim(), limit: 20 },
  })
  return data.results || []
}

export interface HotelSummary {
  kind: HotelKind
  id: number
  hotel_name: string
  brand: string | null
  city: string | null
  state: string | null
  country: string | null
  score: number
}

export interface HotelContact {
  id: number
  name: string
  title: string | null
  email: string | null
  phone: string | null
  linkedin: string | null
  is_primary: boolean
  score: number
  scope: string | null
}

export async function getHotelContactsForOutreach(
  kind: HotelKind,
  parentId: number,
): Promise<{ hotel: HotelSummary; contacts: HotelContact[] }> {
  const { data } = await api.get('/api/outreach/hotel-contacts', {
    params: { kind, parent_id: parentId },
  })
  return data
}
