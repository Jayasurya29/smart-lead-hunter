import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  fetchLeads, fetchLead, fetchStats, fetchContacts, fetchSources,
  approveLead, rejectLead, restoreLead, deleteLead, enrichLead,
  saveContact, unsaveContact, deleteContact, setPrimaryContact,
} from '@/api/leads'
import type { LeadFilters } from '@/api/leads'
import type { LeadTab, SourcesListResponse } from '@/api/types'

const STATUS_MAP: Record<LeadTab, string> = {
  pipeline: 'new',
  approved: 'approved',
  rejected: 'rejected',
  deleted: 'deleted',
}

export function useLeads(tab: LeadTab, page: number, search: string, filters: { timeline?: string; location?: string; tier?: string; year?: string; added?: string; sort?: string }) {
  return useQuery({
    queryKey: ['leads', tab, page, search, filters],
    queryFn: () => fetchLeads({
      status: STATUS_MAP[tab],
      page,
      per_page: 25,
      search: search || undefined,
      // FIX C-03: Send as "location" param (not location_type).
      // The backend now accepts "location" with values like south_florida,
      // rest_florida, etc. and does city-level filtering internally.
      ...(filters.location ? { location: filters.location } : {}),
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

export function useSources() {
  return useQuery<SourcesListResponse>({
    queryKey: ['sources'],
    queryFn: fetchSources,
    staleTime: 60_000,
  })
}

// ── Mutations ──

function useLeadMutation<T = number>(
  fn: (arg: T) => Promise<any>,
  extraInvalidate?: string[][]
) {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: fn,
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['leads'] })
      qc.invalidateQueries({ queryKey: ['stats'] })
      extraInvalidate?.forEach(key => qc.invalidateQueries({ queryKey: key }))
    },
  })
}

export function useApproveLead() {
  return useLeadMutation((id: number) => approveLead(id))
}

export function useRejectLead() {
  return useLeadMutation(({ id, reason }: { id: number; reason?: string }) => rejectLead(id, reason))
}

export function useRestoreLead() {
  return useLeadMutation((id: number) => restoreLead(id))
}

export function useDeleteLead() {
  return useLeadMutation((id: number) => deleteLead(id))
}

export function useEnrichLead() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (id: number) => enrichLead(id),
    onSuccess: (_data, id) => {
      qc.invalidateQueries({ queryKey: ['lead', id] })
      qc.invalidateQueries({ queryKey: ['contacts', id] })
      qc.invalidateQueries({ queryKey: ['leads'] })
      qc.invalidateQueries({ queryKey: ['stats'] })
    },
  })
}

export function useSaveContact() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: ({ leadId, contactId }: { leadId: number; contactId: number }) =>
      saveContact(leadId, contactId),
    onSuccess: (_data, { leadId }) => {
      qc.invalidateQueries({ queryKey: ['contacts', leadId] })
    },
  })
}

export function useUnsaveContact() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: ({ leadId, contactId }: { leadId: number; contactId: number }) =>
      unsaveContact(leadId, contactId),
    onSuccess: (_data, { leadId }) => {
      qc.invalidateQueries({ queryKey: ['contacts', leadId] })
    },
  })
}

export function useDeleteContact() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: ({ leadId, contactId }: { leadId: number; contactId: number }) =>
      deleteContact(leadId, contactId),
    onSuccess: (_data, { leadId }) => {
      qc.invalidateQueries({ queryKey: ['contacts', leadId] })
      qc.invalidateQueries({ queryKey: ['lead', leadId] })
    },
  })
}

export function useSetPrimaryContact() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: ({ leadId, contactId }: { leadId: number; contactId: number }) =>
      setPrimaryContact(leadId, contactId),
    onSuccess: (_data, { leadId }) => {
      qc.invalidateQueries({ queryKey: ['contacts', leadId] })
      qc.invalidateQueries({ queryKey: ['lead', leadId] })
    },
  })
}