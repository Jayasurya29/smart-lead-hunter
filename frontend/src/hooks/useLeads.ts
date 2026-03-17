import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { fetchLeads, fetchLead, fetchStats, fetchContacts, approveLead, rejectLead, restoreLead, deleteLead, enrichLead } from '@/api/leads'
import type { LeadFilters } from '@/api/leads'
import type { LeadTab } from '@/api/types'

const STATUS_MAP: Record<LeadTab, string> = {
  pipeline: 'new',
  approved: 'approved',
  rejected: 'rejected',
  deleted: 'deleted',
}

export interface LeadFilterState {
  timeline: string
  location: string
  tier: string
  year: string
  added: string
  sort: string
}

// ── Lead list with full filter support ──
export function useLeads(tab: LeadTab, page: number = 1, search: string = '', filters: LeadFilterState = { timeline: '', location: '', tier: '', year: '', added: '', sort: 'newest' }) {
  return useQuery({
    queryKey: ['leads', tab, page, search, filters],
    queryFn: () => fetchLeads({
      status: STATUS_MAP[tab],
      page,
      per_page: 25,
      search: search || undefined,
      location_type: filters.location || undefined,
      brand_tier: filters.tier || undefined,
      timeline: filters.timeline || undefined,
      year: filters.year || undefined,
      added: filters.added || undefined,
      sort: filters.sort || undefined,
    }),
    refetchInterval: 30_000,
    staleTime: 10_000,
  })
}

// ── Single lead ──
export function useLead(id: number | null) {
  return useQuery({
    queryKey: ['lead', id],
    queryFn: () => fetchLead(id!),
    enabled: !!id,
  })
}

// ── Dashboard stats ──
export function useStats() {
  return useQuery({
    queryKey: ['stats'],
    queryFn: fetchStats,
    refetchInterval: 30_000,
    staleTime: 10_000,
  })
}

// ── Contacts ──
export function useContacts(leadId: number | null) {
  return useQuery({
    queryKey: ['contacts', leadId],
    queryFn: () => fetchContacts(leadId!),
    enabled: !!leadId,
  })
}

// ── Mutations ──

export function useApproveLead() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (id: number) => approveLead(id),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['leads'] })
      qc.invalidateQueries({ queryKey: ['stats'] })
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
    },
  })
}

export function useEnrichLead() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (id: number) => enrichLead(id),
    onSuccess: (_data, id) => {
      qc.invalidateQueries({ queryKey: ['lead', id] })
      qc.invalidateQueries({ queryKey: ['contacts', id] })
      qc.invalidateQueries({ queryKey: ['leads'] })
    },
  })
}
