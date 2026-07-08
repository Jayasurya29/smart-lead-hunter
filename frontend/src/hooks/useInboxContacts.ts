import { useQuery, useMutation, useQueryClient, type QueryClient } from '@tanstack/react-query'
import {
  fetchAllInboxContacts,
  fetchInboxContacts,
  fetchLeadContacts,
  fetchInboxContactStats,
  approveInboxContact,
  bulkApproveInboxContacts,
  deleteInboxContact,
  triggerInboxSync,
  deepEnrichContact,
  findContactEmail,
  findCurrentEmployer,
  findSuccessor,
  findContactLinkedin,
  updateInboxContact,
  updateLeadContact,
  junkContact,
  junkContactsBulk,
  mergeContacts,
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

// [contacts_perf] Patch cached rows in place instead of refetching the 32k /all
// payload after every trash/restore/approve — the refetch+regroup was the
// visible hitch. Stats stay invalidated (cheap); list data is edited surgically.
function patchCachedContacts(qc: QueryClient, ids: number[], patch: Record<string, unknown>) {
  const idset = new Set(ids)
  qc.setQueriesData({ queryKey: ['inbox-contacts'] }, (old: any) => {
    if (!old?.items) return old
    let touched = false
    const items = old.items.map((c: any) => {
      if (idset.has(c.id)) { touched = true; return { ...c, ...patch } }
      return c
    })
    return touched ? { ...old, items } : old
  })
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
export function useAllInboxContacts(orderBy = 'priority_score', search = '') {
  const term = (search || '').trim()
  return useQuery({
    // [server_side_search] cache per search term; a term-specific query never
    // collides with the full-browse cache.
    queryKey: ['inbox-contacts', 'all', orderBy, term],
    queryFn: async () => {
      const per_page = 500
      // [server_side_search] when searching, let the DB match and return only
      // the hits -- ONE request, no 87-page load of the whole table.
      if (term) {
        const res = await fetchInboxContacts({ page: 1, per_page, order_by: orderBy, search: term })
        return { items: res.items, total: res.total }
      }
      // [contacts_perf] browse path: ONE gzip'd request for the whole table
      // (was 87 parallel 500-row pages). Old fan-out kept as fallback so the
      // page still works against a backend that predates /all.
      try {
        const all = await fetchAllInboxContacts(orderBy)
        return { items: all.items, total: all.total }
      } catch {
        /* fall through to legacy fan-out */
      }
      const first = await fetchInboxContacts({ page: 1, per_page, order_by: orderBy })
      const pages = Math.min(first.pages || 1, 200) // ~100k headroom
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
      // [contacts_load_cap] same headroom as the inbox list (see above).
      const pages = Math.min(first.pages || 1, 200) // ~100k headroom (lead rows)
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
    onSuccess: (_d, ids) => patchCachedContacts(qc, ids, { approval_status: 'approved' }),
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
    onSuccess: (_d, vars) => patchCachedContacts(qc, [vars.id], vars.fields as Record<string, unknown>),
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

// [find_email_only] email-ONLY lookup; refreshes the list so the new email shows.
export function useFindContactEmail() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (id: number) => findContactEmail(id),
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
    mutationFn: ({ id, apply, previewSeatOrg, previewSeatTitle }:
      { id: number; apply?: boolean; previewSeatOrg?: string; previewSeatTitle?: string }) =>
      findSuccessor(id, { apply, previewSeatOrg, previewSeatTitle }),
    onSuccess: (_d, vars) => { if (vars.apply) invalidateInboxContacts(qc) },
  })
}

export function useJunkContact() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (id: number) => junkContact(id),
    onSuccess: (_d, id) => patchCachedContacts(qc, [id], { manual_category: 'junk' }),
  })
}

export function useUnjunkContact() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (id: number) => unjunkContact(id),
    onSuccess: (_d, id) => patchCachedContacts(qc, [id], { manual_category: null }),
  })
}

// [trash_ux] bulk trash / restore for the Contacts grid selection bar
export function useBulkJunkContacts() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (ids: number[]) => junkContactsBulk(ids),
    onSuccess: (_d, ids) => patchCachedContacts(qc, ids, { manual_category: 'junk' }),
  })
}

export function useBulkUnjunkContacts() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: async (ids: number[]) => { await Promise.all(ids.map((id) => unjunkContact(id))) },
    onSuccess: (_d, ids) => patchCachedContacts(qc, ids, { manual_category: null }),
  })
}

// [category_ux] bulk-set the human category override. '' clears back to auto.
export function useBulkSetCategory() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: async ({ ids, category }: { ids: number[]; category: string }) => {
      await Promise.all(ids.map((id) => updateInboxContact(id, { manual_category: category })))
    },
    onSuccess: (_d, vars) =>
      patchCachedContacts(qc, vars.ids, { manual_category: vars.category || null }),
  })
}

// [merge_ux] commit a merge (preview is called directly, no cache to touch)
export function useMergeContacts() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: ({ primaryId, mergeId }: { primaryId: number; mergeId: number }) =>
      mergeContacts(primaryId, mergeId),
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
