import { useQuery, useQueryClient } from '@tanstack/react-query'

async function fetchLeadRevenue(leadId: number) {
  const res = await fetch(`/revenue/estimate/lead/${leadId}`, {
    credentials: 'include',
  })
  if (!res.ok) throw new Error('Revenue fetch failed')
  return res.json()
}

export function useLeadRevenue(leadId: number | null) {
  const qc = useQueryClient()

  return useQuery({
    queryKey: ['revenue', 'lead', leadId],
    queryFn: async () => {
      const data = await fetchLeadRevenue(leadId!)
      // Revenue was persisted to DB — refresh the leads list so table shows updated value
      if (data.status === 'success') {
        qc.invalidateQueries({ queryKey: ['leads'] })
      }
      return data
    },
    enabled: !!leadId,
    staleTime: 5 * 60 * 1000,
    retry: false,
  })
}
