// Types matching FastAPI Pydantic schemas

export interface Lead {
  id: number
  hotel_name: string
  brand: string | null
  brand_tier: string | null
  hotel_type: string | null
  hotel_website: string | null
  city: string | null
  state: string | null
  country: string | null
  location_type: string | null
  contact_name: string | null
  contact_title: string | null
  contact_email: string | null
  contact_phone: string | null
  opening_date: string | null
  room_count: number | null
  description: string | null
  key_insights: string | null
  management_company: string | null
  developer: string | null
  owner: string | null
  lead_score: number | null
  score_breakdown: Record<string, any> | null
  status: string
  source_url: string | null
  source_site: string | null
  rejection_reason: string | null
  notes: string | null
  insightly_id: number | null
  created_at: string
  updated_at: string | null
}

export interface LeadListResponse {
  leads: Lead[]
  total: number
  page: number
  per_page: number
  pages: number
}

export interface Source {
  id: number
  name: string
  base_url: string
  source_type: string | null
  priority: number | null
  is_active: boolean
  last_scraped_at: string | null
  leads_found: number
  health_status: string | null
  gold_count?: number
}

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
}

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
}

export interface User {
  id: number
  email: string
  full_name: string
  role: 'admin' | 'manager' | 'viewer'
}

export interface AuthResponse {
  access_token: string
  token_type: string
}

export type LeadTab = 'pipeline' | 'approved' | 'rejected' | 'deleted'
