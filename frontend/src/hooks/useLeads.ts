import { useQuery, useMutation, useQueryClient, type QueryClient } from '@tanstack/react-query'
import api from '@/api/client'
import {
  fetchLeads, fetchLead, fetchStats, fetchContacts,
  approveLead, rejectLead, restoreLead, deleteLead, enrichLead,
} from '@/api/leads'
import type { LeadTab } from '@/api/types'

const STATUS_MAP: Record<LeadTab, string> = {
  pipeline: 'new',
  approved: 'approved',
  rejected: 'rejected',
  expired:  'expired',
}

// AUDIT 2026-05-05 (bug #13): Centralized invalidation helper.
// Every mutation that can transition or delete a lead must invalidate:
//   - ['leads']                        — list pages
//   - ['lead', id]                     — detail panel
//   - ['contacts', id]                 — contact tab (enrichment changes)
//   - ['stats']                        — sidebar count widget
//   - ['map-leads']                    — Map tab marker layer
//   - ['map-data']                     — Map tab unified layer (mixes EH)
//   - ['existing-hotels']              — auto-transfer creates EH rows
//   - ['existing-hotels-stats']        — EH counts widget
// Calling all of these on success keeps every page consistent without
// waiting for the 30s poll, especially after a transferred response
// where the lead has moved tables.
function invalidateLeadEverywhere(qc: QueryClient, id?: number) {
  qc.invalidateQueries({ queryKey: ['leads'] })
  qc.invalidateQueries({ queryKey: ['stats'] })
  qc.invalidateQueries({ queryKey: ['map-leads'] })
  qc.invalidateQueries({ queryKey: ['map-data'] })
  qc.invalidateQueries({ queryKey: ['existing-hotels'] })
  qc.invalidateQueries({ queryKey: ['existing-hotels-stats'] })
  if (id != null) {
    qc.invalidateQueries({ queryKey: ['lead', id] })
    qc.invalidateQueries({ queryKey: ['contacts', id] })
  }
}

export function useLeads(
  tab: LeadTab,
  page: number = 1,
  search: string = '',
  filters: Record<string, string> = {},
  per_page: number = 25,
) {
  return useQuery({
    queryKey: ['leads', tab, page, search, filters, per_page],
    queryFn: () => fetchLeads({
      status: STATUS_MAP[tab],
      page,
      per_page,
      search: search || undefined,
      location:   filters.location || undefined,
      brand_tier: filters.tier || undefined,
      timeline:   filters.timeline || undefined,
      year:       filters.year || undefined,
      added:      filters.added || undefined,
      sort:       filters.sort || undefined,
    }),
    refetchInterval: 30_000,
    refetchIntervalInBackground: false,
    staleTime: 10_000,
  })
}

export function useLead(id: number | null) {
  return useQuery({
    queryKey: ['lead', id],
    queryFn: () => fetchLead(id!),
    enabled: !!id,
  })
}

export function useStats() {
  return useQuery({
    queryKey: ['stats'],
    queryFn: fetchStats,
    refetchInterval: 30_000,
    refetchIntervalInBackground: false,
    staleTime: 10_000,
  })
}

export function useContacts(leadId: number | null) {
  return useQuery({
    queryKey: ['contacts', leadId],
    queryFn: () => fetchContacts(leadId!),
    enabled: !!leadId,
  })
}

export function useApproveLead() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (id: number) => approveLead(id),
    onSuccess: (_data, id) => invalidateLeadEverywhere(qc, id),
  })
}

export function useRejectLead() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: ({ id, reason }: { id: number; reason?: string }) => rejectLead(id, reason),
    onSuccess: (_data, vars) => invalidateLeadEverywhere(qc, vars.id),
  })
}

export function useRestoreLead() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (id: number) => restoreLead(id),
    onSuccess: (_data, id) => invalidateLeadEverywhere(qc, id),
  })
}

export function useDeleteLead() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (id: number) => deleteLead(id),
    onSuccess: (_data, id) => invalidateLeadEverywhere(qc, id),
  })
}

export function useSmartFill() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: async ({ id, mode = 'smart' }: { id: number; mode?: 'smart' | 'full' }) => {
      const { data } = await api.post(`/api/leads/${id}/smart-fill`, { mode })
      return data
    },
    onSuccess: (_data, vars) => invalidateLeadEverywhere(qc, vars.id),
  })
}

export function useEnrichLead() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (id: number) => enrichLead(id),
    onSuccess: (_data, id) => invalidateLeadEverywhere(qc, id),
  })
}
