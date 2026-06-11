import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  fetchPersonAffiliations,
  fetchAccountCoverage,
  saveLeadContact,
} from '@/api/affiliations'

/** Employer + resolved coverage for one person. Lazy: only fires when a real
 *  personType + non-negative id are supplied (i.e. when a drawer is open). */
export function usePersonAffiliations(
  personType: 'contact' | 'lead_contact' | null,
  personId: number | null,
) {
  return useQuery({
    queryKey: ['affiliations', 'person', personType, personId],
    queryFn: () => fetchPersonAffiliations(personType!, personId!),
    enabled: !!personType && personId != null && personId >= 0,
    staleTime: 60_000,
    refetchOnWindowFocus: false,
  })
}

/** Everyone who covers one hotel/lead (direct edges + portfolio buyers). */
export function useAccountCoverage(
  accountType: 'existing_hotel' | 'potential_lead' | null,
  accountId: number | null,
) {
  return useQuery({
    queryKey: ['affiliations', 'account', accountType, accountId],
    queryFn: () => fetchAccountCoverage(accountType!, accountId!),
    enabled: !!accountType && accountId != null && accountId >= 0,
    staleTime: 60_000,
    refetchOnWindowFocus: false,
  })
}

/** Verify (save) a lead contact, then refresh coverage + the contacts list so
 *  the now-verified person becomes openable. */
export function useSaveLeadContact() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (contactId: number) => saveLeadContact(contactId),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['affiliations'] })
      qc.invalidateQueries({ queryKey: ['lead-contacts'] })
    },
  })
}
