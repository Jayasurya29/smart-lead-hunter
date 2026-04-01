import { useQuery } from '@tanstack/react-query'
import type { SAPClientParams } from '@/api/sap'

// Direct fetch that uses cookies — bypasses axios token issues
async function sapFetch(path: string) {
  const res = await fetch(`/api/sap${path}`, { credentials: 'include' })
  if (!res.ok) throw new Error(`SAP API ${res.status}: ${res.statusText}`)
  return res.json()
}

export function useSAPClients(params: SAPClientParams = {}) {
  const query = new URLSearchParams()
  Object.entries(params).forEach(([k, v]) => {
    if (v != null && String(v) !== '') query.set(k, String(v))
  })
  const qs = query.toString() ? `?${query.toString()}` : ''
  return useQuery({
    queryKey: ['sap-clients', params],
    queryFn: () => sapFetch(`/clients${qs}`),
    staleTime: 60_000,
  })
}

export function useSAPClient(id: number | null) {
  return useQuery({
    queryKey: ['sap-client', id],
    queryFn: () => sapFetch(`/clients/${id}`),
    enabled: !!id,
  })
}

export function useSAPFilters() {
  return useQuery({
    queryKey: ['sap-filters'],
    queryFn: () => sapFetch('/filters'),
    staleTime: 300_000,
  })
}

export function useSAPSummary() {
  return useQuery({
    queryKey: ['sap-summary'],
    queryFn: () => sapFetch('/import/summary'),
    staleTime: 60_000,
  })
}

export function usePareto(params: { is_hotel?: boolean; state?: string } = {}) {
  const query = new URLSearchParams()
  Object.entries(params).forEach(([k, v]) => {
    if (v != null && String(v) !== '') query.set(k, String(v))
  })
  const qs = query.toString() ? `?${query.toString()}` : ''
  return useQuery({
    queryKey: ['sap-pareto', params],
    queryFn: () => sapFetch(`/analytics/pareto${qs}`),
    staleTime: 120_000,
  })
}

export function useChurnRisk(params: { is_hotel?: boolean; min_revenue?: number } = {}) {
  const query = new URLSearchParams()
  Object.entries(params).forEach(([k, v]) => {
    if (v != null && String(v) !== '') query.set(k, String(v))
  })
  const qs = query.toString() ? `?${query.toString()}` : ''
  return useQuery({
    queryKey: ['sap-churn', params],
    queryFn: () => sapFetch(`/analytics/churn-risk${qs}`),
    staleTime: 120_000,
  })
}

export function useBrandPenetration() {
  return useQuery({
    queryKey: ['sap-brands'],
    queryFn: () => sapFetch('/analytics/brand-penetration'),
    staleTime: 120_000,
  })
}
