import api from './client'

// ── Types ──

export interface RevenueEstimate {
  lead_type: string
  tier: string
  tier_label: string
  property_type: string
  location: string
  rooms: number
  staff_per_room: number
  base_staff: number
  seasonal_staff: number
  total_staff: number
  uniformed_staff: number
  uniformed_pct: number
  cost_per_employee: number
  climate_factor: number
  fb_multiplier: number
  garment_pct: number
  total_budget: number
  ja_addressable: number
  ja_actual: number | null
  wallet_share: number | null
  gap: number | null
  peak_months: number
  seasonal_surge_pct: number
  fb_outlets: number
}

export interface LeadRevenueResponse {
  status: 'success' | 'incomplete'
  lead_id: number
  hotel_name: string
  brand?: string
  detected_tier?: string
  tier_label?: string
  location?: string
  property_type?: string
  rooms?: number
  new_opening?: RevenueEstimate
  annual_recurring?: RevenueEstimate
  // When incomplete
  missing_fields?: string[]
  available?: Record<string, unknown>
  message?: string
}

export interface QuickEstimate {
  rooms: number
  tier: string
  tier_label: string
  location: string
  opening_estimate: number
  annual_estimate: number
  total_staff: number
  uniformed_staff: number
}

export interface TierConfig {
  key: string
  label: string
  adr_min: number
  staff_per_room: Record<string, number>
  annual_cost_per_employee: number
  initial_kit_cost: number
  garment_pct: number
}

export interface ClimateConfig {
  key: string
  label: string
  factor: number
  peak_months: number
  seasonal_surge: number
}

// ── API Functions ──

export async function fetchLeadRevenue(leadId: number) {
  const { data } = await api.get<LeadRevenueResponse>(`/revenue/estimate/lead/${leadId}`)
  return data
}

export async function fetchQuickEstimate(params: {
  rooms: number
  tier: string
  location?: string
  property_type?: string
}) {
  const { data } = await api.get<QuickEstimate>('/revenue/quick-estimate', { params })
  return data
}

export async function calculateRevenue(params: {
  rooms: number
  tier: string
  property_type?: string
  location?: string
  fb_outlets?: number
  lead_type?: string
  ja_actual?: number
}) {
  const { data } = await api.post<RevenueEstimate>('/revenue/calculate', params)
  return data
}

export async function fetchTiers() {
  const { data } = await api.get<{ tiers: TierConfig[] }>('/revenue/tiers')
  return data.tiers
}

export async function fetchClimates() {
  const { data } = await api.get<{ climates: ClimateConfig[] }>('/revenue/climates')
  return data.climates
}

export async function fetchPropertyTypes() {
  const { data } = await api.get<{ property_types: string[] }>('/revenue/property-types')
  return data.property_types
}
