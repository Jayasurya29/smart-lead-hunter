import api from './client'

// ── Types ──

export interface SAPClient {
  id: number
  customer_code: string
  customer_name: string
  customer_group: string | null
  customer_type: string
  is_hotel: boolean
  phone: string | null
  email: string | null
  contact_person: string | null
  street: string | null
  city: string | null
  state: string | null
  zip_code: string | null
  country: string | null
  revenue_current_year: number
  revenue_last_year: number
  revenue_lifetime: number
  total_invoices: number
  customer_since: string | null
  last_order_date: string | null
  days_since_last_order: number | null
  churn_risk: string
  revenue_trend: string
  sales_rep: string | null
  brand: string | null
  brand_tier: string | null
  room_count: number | null
  matched_lead_id: number | null
  notes: string | null
  created_at: string | null
}

export interface SAPClientListResponse {
  clients: SAPClient[]
  total: number
  page: number
  per_page: number
  total_pages: number
}

export interface SAPFilters {
  groups: string[]
  states: string[]
  sales_reps: string[]
  customer_types: string[]
}

export interface SAPImportSummary {
  total_clients: number
  hotel_clients: number
  non_hotel_clients: number
  churn_breakdown: Record<string, number>
  revenue: {
    lifetime: number
    current_year: number
    last_year: number
  }
  top_groups: Array<{
    group: string
    client_count: number
    total_revenue: number
  }>
}

export interface ParetoItem {
  rank: number
  id: number
  customer_code: string
  customer_name: string
  customer_group: string | null
  revenue_lifetime: number
  revenue_current_year: number
  revenue_last_year: number
  sales_rep: string | null
  state: string | null
  is_hotel: boolean
  pct_of_total: number
  cumulative_pct: number
}

export interface ChurnRiskResponse {
  at_risk_clients: SAPClient[]
  total_at_risk: number
  total_revenue_at_risk: number
}

// ── API Functions ──

export interface SAPClientParams {
  page?: number
  per_page?: number
  search?: string
  group?: string
  state?: string
  sales_rep?: string
  is_hotel?: boolean
  customer_type?: string
  churn_risk?: string
  sort_by?: string
  sort_dir?: 'asc' | 'desc'
  min_revenue?: number
}

export async function fetchSAPClients(params: SAPClientParams = {}) {
  const { data } = await api.get<SAPClientListResponse>('/sap/clients', { params })
  return data
}

export async function fetchSAPClient(id: number) {
  const { data } = await api.get<SAPClient>(`/sap/clients/${id}`)
  return data
}

export async function fetchSAPFilters() {
  const { data } = await api.get<SAPFilters>('/sap/filters')
  return data
}

export async function fetchSAPSummary() {
  const { data } = await api.get<SAPImportSummary>('/sap/import/summary')
  return data
}

export async function fetchPareto(params: { is_hotel?: boolean; state?: string } = {}) {
  const { data } = await api.get<{ total_revenue: number; total_clients: number; pareto: ParetoItem[] }>(
    '/sap/analytics/pareto',
    { params },
  )
  return data
}

export async function fetchChurnRisk(params: { is_hotel?: boolean; min_revenue?: number } = {}) {
  const { data } = await api.get<ChurnRiskResponse>('/sap/analytics/churn-risk', { params })
  return data
}

export async function fetchBrandPenetration() {
  const { data } = await api.get<{
    brands: Array<{
      group: string
      client_count: number
      total_revenue: number
      avg_revenue: number
      revenue_ytd: number
      states: string[]
      state_count: number
    }>
  }>('/sap/analytics/brand-penetration')
  return data
}

export async function fetchGeoDistribution() {
  const { data } = await api.get<{
    states: Array<{
      state: string
      client_count: number
      total_revenue: number
      revenue_ytd: number
    }>
  }>('/sap/analytics/geo')
  return data
}

export async function importSAPCSV(file: File) {
  const formData = new FormData()
  formData.append('file', file)
  const { data } = await api.post('/sap/import', formData, {
    headers: { 'Content-Type': 'multipart/form-data' },
  })
  return data
}
