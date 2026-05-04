import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
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
    refetchIntervalInBackground: false,  // FIX L-09: stop polling in background tabs
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
    refetchIntervalInBackground: false,  // FIX L-09: stop polling in background tabs
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
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['leads'] })
      qc.invalidateQueries({ queryKey: ['stats'] })
      qc.invalidateQueries({ queryKey: ['map-leads'] })
      qc.invalidateQueries({ queryKey: ['map-data'] })
    },
  })
}

export function useRejectLead() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: ({ id, reason }: { id: number; reason?: string }) => rejectLead(id, reason),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['leads'] })
      qc.invalidateQueries({ queryKey: ['stats'] })
      qc.invalidateQueries({ queryKey: ['map-leads'] })
    },
  })
}

export function useRestoreLead() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (id: number) => restoreLead(id),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['leads'] })
      qc.invalidateQueries({ queryKey: ['stats'] })
      qc.invalidateQueries({ queryKey: ['map-leads'] })
    },
  })
}

export function useDeleteLead() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (id: number) => deleteLead(id),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['leads'] })
      qc.invalidateQueries({ queryKey: ['stats'] })
      qc.invalidateQueries({ queryKey: ['map-leads'] })
    },
  })
}

export function useSmartFill() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: async ({ id, mode = 'smart' }: { id: number; mode?: 'smart' | 'full' }) => {
      const { data } = await api.post(`/api/leads/${id}/smart-fill`, { mode })
      return data
    },
    onSuccess: (_data, vars) => {
      qc.invalidateQueries({ queryKey: ['lead', vars.id] })
      qc.invalidateQueries({ queryKey: ['leads'] })
      qc.invalidateQueries({ queryKey: ['map-leads'] })
    },
  })
}

export function useEnrichLead() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (id: number) => enrichLead(id),
    onSuccess: (_data, id) => {
      qc.invalidateQueries({ queryKey: ['contacts', id] })
      qc.invalidateQueries({ queryKey: ['lead', id] })
      qc.invalidateQueries({ queryKey: ['leads'] })
      qc.invalidateQueries({ queryKey: ['stats'] })
      qc.invalidateQueries({ queryKey: ['map-leads'] })
    },
  })
}
