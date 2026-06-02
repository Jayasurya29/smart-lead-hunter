import { useQuery, useMutation, useQueryClient, type QueryClient } from '@tanstack/react-query'
import {
  fetchInboxContacts,
  fetchInboxContactStats,
  approveInboxContact,
  bulkApproveInboxContacts,
  deleteInboxContact,
  triggerInboxSync,
  deepEnrichContact,
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

export function useDeepEnrichContact() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: ({ id, findEmail }: { id: number; findEmail?: boolean }) =>
      deepEnrichContact(id, findEmail),
    onSuccess: () => invalidateInboxContacts(qc),
  })
}
