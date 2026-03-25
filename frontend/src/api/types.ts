/* ── Leads ── */

export interface Lead {
  id: number
  hotel_name: string | null
  name?: string | null
  brand: string | null
  brand_name?: string | null
  brand_tier: string | null
  hotel_type: string | null
  hotel_website: string | null
  city: string | null
  state: string | null
  country: string | null
  location_type: string | null
  opening_date: string | null
  opening_year: number | null
  room_count: number | null
  lead_score: number | null
  timeline_label: string | null
  status: string
  management_company: string | null
  developer: string | null
  owner: string | null
  contact_name: string | null
  contact_title: string | null
  contact_email: string | null
  contact_phone: string | null
  description: string | null
  key_insights: string | null
  notes: string | null
  rejection_reason: string | null
  source_url: string | null
  source_site: string | null
  source_urls: string[] | null
  source_extractions: Record<string, any> | null
  score_breakdown: Record<string, any> | null
  sources?: any
  source_detail?: string | null
  insightly_id: number | null
  created_at: string
  updated_at: string | null
}

export interface LeadListResponse {
  leads: Lead[]
  total: number
  page: number
  per_page: number
  pages: number  // FIX H-10: was "total_pages" but backend sends "pages"
}

/* ── Stats ── */

export interface DashboardStats {
  total_leads: number
  new_leads: number
  approved_leads: number
  pending_leads: number
  rejected_leads: number
  hot_leads: number
  urgent_leads: number
  warm_leads: number
  cool_leads: number
  total_sources: number
  active_sources: number
  healthy_sources: number
  leads_today: number
  leads_this_week: number
  expired_leads: number
}

/* ── Contacts ── */

export interface Contact {
  id: number
  lead_id: number
  name: string
  title: string | null
  email: string | null
  phone: string | null
  linkedin: string | null
  organization: string | null
  scope: string | null
  confidence: string | null
  tier: string | null
  score: number
  is_primary: boolean
  is_saved: boolean
  source_detail: string | null
  found_via: string | null
  evidence_url: string | null
}

/* ── Sources ── */

export interface Source {
  id: number
  name: string
  url: string
  is_active: boolean
  gold_url_count: number
  last_scraped_at: string | null
}

export interface SourcesListResponse {
  sources: Source[]
}

/* ── Auth ── */

export interface User {
  id: number
  first_name: string
  last_name: string
  email: string
  role: 'sales' | 'admin'
  is_active: boolean
  last_login: string | null
  created_at: string | null
}

export interface AuthResponse {
  access_token: string
  token_type: string
}

/* ── Shared ── */

export type LeadTab = 'pipeline' | 'approved' | 'rejected' | 'expired'
