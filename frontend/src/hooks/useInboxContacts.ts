import { useQuery, useMutation, useQueryClient, type QueryClient } from '@tanstack/react-query'
import {
  fetchInboxContacts,
  fetchLeadContacts,
  fetchInboxContactStats,
  approveInboxContact,
  bulkApproveInboxContacts,
  deleteInboxContact,
  triggerInboxSync,
  deepEnrichContact,
  findCurrentEmployer,
  findSuccessor,
  findContactLinkedin,
  updateInboxContact,
  updateLeadContact,
  junkContact,
  unjunkContact,
  junkDomain,
  type InboxContactFilters,
} from '@/api/inboxContacts'

/**
 * Invalidate all inbox-contact related queries after a mutation.
 */
export function invalidateInboxContacts(qc: QueryClient) {
  qc.invalidateQueries({ queryKey: ['inbox-contacts'] })
  qc.invalidateQueries({ queryKey: ['inbox-contacts-stats'] })
}

export function useInboxContacts(filters: InboxContactFilters = {}) {
  return useQuery({
    queryKey: ['inbox-contacts', filters],
    queryFn: () => fetchInboxContacts(filters),
    refetchInterval: 30_000,
    refetchIntervalInBackground: false,
    staleTime: 10_000,
  })
}

/**
 * Load the ENTIRE contacts table (all pages) so the account-grouped view and
 * its header/scope counts are computed over every contact — not just a 200-row
 * slice. Fetches page 1 to learn the page count, then pulls the rest in
 * parallel. Cached for 60s. Filtering/sorting/grouping happen client-side in
 * the page, so this hook takes no filters beyond sort order.
 */
export function useAllInboxContacts(orderBy = 'priority_score') {
  return useQuery({
    queryKey: ['inbox-contacts', 'all', orderBy],
    queryFn: async () => {
      const per_page = 500
      const first = await fetchInboxContacts({ page: 1, per_page, order_by: orderBy })
      const pages = Math.min(first.pages || 1, 80) // safety cap ~40k (post-2025 backfill)
      const items = [...first.items]
      if (pages > 1) {
        const rest = await Promise.all(
          Array.from({ length: pages - 1 }, (_, i) =>
            fetchInboxContacts({ page: i + 2, per_page, order_by: orderBy }),
          ),
        )
        for (const r of rest) items.push(...r.items)
      }
      return { items, total: first.total }
    },
    staleTime: 60_000,
    refetchInterval: false,
    refetchOnWindowFocus: false,
  })
}

/**
 * Load ALL lead-generator contacts (every page) for the unified directory.
 * Same paginate-all pattern as useAllInboxContacts. If the endpoint is
 * unavailable the query just errors and the page degrades to inbox-only.
 */
export function useAllLeadContacts() {
  return useQuery({
    queryKey: ['lead-contacts', 'all'],
    queryFn: async () => {
      const per_page = 500
      const first = await fetchLeadContacts(1, per_page)
      const pages = Math.min(first.pages || 1, 80) // safety cap ~40k (lead rows)
      const items = [...first.items]
      if (pages > 1) {
        const rest = await Promise.all(
          Array.from({ length: pages - 1 }, (_, i) => fetchLeadContacts(i + 2, per_page)),
        )
        for (const r of rest) items.push(...r.items)
      }
      return { items, total: first.total }
    },
    staleTime: 60_000,
    refetchInterval: false,
    refetchOnWindowFocus: false,
  })
}

export function useInboxContactStats() {
  return useQuery({
    queryKey: ['inbox-contacts-stats'],
    queryFn: fetchInboxContactStats,
    refetchInterval: 30_000,
    refetchIntervalInBackground: false,
    staleTime: 10_000,
  })
}

export function useApproveInboxContact() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (id: number) => approveInboxContact(id),
    onSuccess: () => invalidateInboxContacts(qc),
  })
}

export function useBulkApproveInboxContacts() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (ids: number[]) => bulkApproveInboxContacts(ids),
    onSuccess: () => invalidateInboxContacts(qc),
  })
}

export function useDeleteInboxContact() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (id: number) => deleteInboxContact(id),
    onSuccess: () => invalidateInboxContacts(qc),
  })
}

export function useTriggerInboxSync() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: () => triggerInboxSync(),
    onSuccess: () => {
      // Delay refetch slightly — task is async
      setTimeout(() => invalidateInboxContacts(qc), 2000)
    },
  })
}

export function useFindLinkedin() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (id: number) => findContactLinkedin(id),
    onSuccess: () => invalidateInboxContacts(qc),
  })
}

// patch_frontend_leadcontact_edit
export function useUpdateLeadContact() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: ({ realId, fields }: { realId: number; fields: import('../api/inboxContacts').LeadContactEditFields }) =>
      updateLeadContact(realId, fields),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['lead-contacts', 'all'] }) },
  })
}

export function useUpdateInboxContact() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: ({ id, fields }: { id: number; fields: import('../api/inboxContacts').ContactEditFields }) =>
      updateInboxContact(id, fields),
    onSuccess: () => invalidateInboxContacts(qc),
  })
}

export function useDeepEnrichContact() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: ({ id, findEmail }: { id: number; findEmail?: boolean }) =>
      deepEnrichContact(id, findEmail),
    onSuccess: () => invalidateInboxContacts(qc),
  })
}

/* [patch_frontend_current_employer] */
export function useFindCurrentEmployer() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: ({ id, apply, useWiza, findEmail }: {
      id: number; apply?: boolean; useWiza?: boolean; findEmail?: boolean
    }) => findCurrentEmployer(id, { apply, useWiza, findEmail }),
    onSuccess: (_d, vars) => { if (vars.apply) invalidateInboxContacts(qc) },
  })
}

// /* [patch_frontend_find_successor] */
export function useFindSuccessor() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: ({ id, apply }: { id: number; apply?: boolean }) =>
      findSuccessor(id, { apply }),
    onSuccess: (_d, vars) => { if (vars.apply) invalidateInboxContacts(qc) },
  })
}

export function useJunkContact() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (id: number) => junkContact(id),
    onSuccess: () => invalidateInboxContacts(qc),
  })
}

export function useUnjunkContact() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (id: number) => unjunkContact(id),
    onSuccess: () => invalidateInboxContacts(qc),
  })
}

export function useJunkDomain() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: ({ domain, reason }: { domain: string; reason?: string }) => junkDomain(domain, reason),
    onSuccess: () => invalidateInboxContacts(qc),
  })
}
