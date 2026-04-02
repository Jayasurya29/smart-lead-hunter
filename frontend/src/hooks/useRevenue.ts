import { useQuery } from '@tanstack/react-query'

async function fetchLeadRevenue(leadId: number) {
  const res = await fetch(`/revenue/estimate/lead/${leadId}`, {
    credentials: 'include',
  })
  if (!res.ok) throw new Error('Revenue fetch failed')
  return res.json()
}

export function useLeadRevenue(leadId: number | null) {
  return useQuery({
    queryKey: ['revenue', 'lead', leadId],
    queryFn: () => fetchLeadRevenue(leadId!),
    enabled: !!leadId,
    staleTime: 5 * 60 * 1000,
    retry: false,
  })
}
