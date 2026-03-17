// Types matching FastAPI backend response shapes exactly

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
  contact_linkedin: string | null
  opening_date: string | null
  opening_year: number | null
  room_count: number | null
  description: string | null
  key_insights: string | null
  management_company: string | null
  developer: string | null
  owner: string | null
  lead_score: number | null
  score_breakdown: Record<string, any> | null
  estimated_revenue: number | null
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

// Matches the /api/dashboard/sources/list response shape exactly
export interface SourceInfo {
  id: number
  name: string
  type: string           // backend sends "type" not "source_type"
  priority: number
  frequency: string
  health: string         // backend sends "health" not "health_status"
  leads: number          // backend sends "leads" not "leads_found"
  gold_count: number
  last_scraped: string | null  // backend sends "last_scraped" not "last_scraped_at"
  // Only on due_sources:
  reason?: string
  mode?: string
}

export interface SourcesListResponse {
  sources: SourceInfo[]
  due_sources: SourceInfo[]
  categories: { type: string; label: string; count: number }[]
  total: number
  total_due: number
}

// Matches GET /stats response (StatsResponse pydantic model)
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
  found_via: string | null
  source_detail: string | null
  evidence_url: string | null
}

export type LeadTab = 'pipeline' | 'approved' | 'rejected' | 'deleted'

// SSE event from backend scrape/extract/discovery streams
export interface SSEEvent {
  type: 'started' | 'info' | 'source_start' | 'source_complete' | 'url_error' | 'leads_found' | 'complete' | 'error' | 'cancelled' | 'phase' | 'stats' | 'success' | 'warning'
  message?: string
  scrape_id?: string
  source?: string
  current?: number
  total?: number
  pages?: number
  stats?: Record<string, any>
  duration_seconds?: number
}
