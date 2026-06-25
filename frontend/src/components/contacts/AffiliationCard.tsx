import { useState } from 'react'
import { Briefcase, Building2, MapPin, Loader2, ChevronDown, ChevronRight } from 'lucide-react'
import { usePersonAffiliations } from '@/hooks/useAffiliations'

/**
 * Employer + coverage for one contact, resolved live from contact_affiliations
 * (resolve-on-detail — only fetched when the drawer is open). Renders inside the
 * drawer's <SectionCard>. A portfolio buyer (mgmt-co VP) shows their employer +
 * the DERIVED list of every property that company manages — not a single
 * misleading hotel.
 */
export default function AffiliationCard({
  personType,
  personId,
}: {
  personType: 'contact' | 'lead_contact'
  personId: number
}) {
  const { data, isLoading, isError } = usePersonAffiliations(personType, personId)
  const [expanded, setExpanded] = useState(false)

  if (isLoading) {
    return (
      <div className="flex items-center gap-2 text-[12px] text-stone-400 py-1">
        <Loader2 className="w-3.5 h-3.5 animate-spin" /> Resolving coverage…
      </div>
    )
  }
  if (isError || !data) {
    return <div className="text-[12px] text-stone-400 py-1">Coverage unavailable.</div>
  }

  const { employer, employers, is_portfolio_buyer, coverage, coverage_count, derived_portfolio_count, former_employers } = data
  /* [patch_affiliation_card_former] */
  const hasFormer = !!(former_employers && former_employers.length)
  if (!employer && coverage_count === 0 && !hasFormer) {
    return (
      <div className="text-[12px] text-stone-400 py-1">
        No affiliation edges yet for this contact.
      </div>
    )
  }

  const shown = expanded ? coverage : coverage.slice(0, 8)
  const empList = employers && employers.length ? employers : employer ? [employer] : []

  return (
    <div className="space-y-2.5">
      {empList.length > 0 && (
        <div className="flex items-start gap-2">
          <Briefcase className="w-3.5 h-3.5 text-stone-400 mt-0.5 flex-shrink-0" />
          <div className="text-[13px] leading-snug">
            <span className="text-stone-400">{empList.length > 1 ? 'Employers ' : 'Employer '}</span>
            {empList.map((e, i) => (
              <span key={`${e.type}-${e.name}-${i}`}>
                {i > 0 && <span className="text-stone-300"> · </span>}
                <span className="font-semibold text-stone-700">{e.name || '—'}</span>
                {e.type === 'management_company' && e.scope === 'portfolio' && (
                  <span className="ml-1 inline-flex items-center px-1.5 py-0.5 rounded-md text-[10px] font-bold bg-emerald-50 text-emerald-700 ring-1 ring-emerald-200">
                    portfolio
                  </span>
                )}
              </span>
            ))}
            {empList.length > 1 && (
              <span className="ml-1.5 text-[10px] text-stone-400">({empList.length} affiliations)</span>
            )}
          </div>
        </div>
      )}

      <div className="flex items-start gap-2">
        <Building2 className="w-3.5 h-3.5 text-stone-400 mt-0.5 flex-shrink-0" />
        <div className="text-[13px] w-full leading-snug">
          <span className="text-stone-400">Covers </span>
          <span className="font-semibold text-stone-700">
            {coverage_count} {coverage_count === 1 ? 'property' : 'properties'}
          </span>
          {derived_portfolio_count > 0 && (
            <span className="text-stone-400"> · {derived_portfolio_count} via portfolio</span>
          )}

          {coverage.length > 0 && (
            <ul className="mt-1.5 space-y-1">
              {shown.map((c) => (
                <li
                  key={`${c.account_type}-${c.account_id}`}
                  className="flex items-center gap-1.5 text-[12px] text-stone-600"
                >
                  <MapPin className="w-3 h-3 text-stone-300 flex-shrink-0" />
                  <span className="truncate">{c.name || `#${c.account_id}`}</span>
                  <span
                    className={
                      'ml-auto flex-shrink-0 text-[10px] px-1 rounded ' +
                      (c.via === 'explicit'
                        ? 'text-navy-600 bg-navy-50'
                        : 'text-stone-400 bg-stone-100')
                    }
                  >
                    {c.via}
                  </span>
                </li>
              ))}
            </ul>
          )}

          {coverage.length > 8 && (
            <button
              onClick={() => setExpanded(!expanded)}
              className="mt-1.5 inline-flex items-center gap-1 text-[11px] font-medium text-navy-600 hover:text-navy-700"
            >
              {expanded ? <ChevronDown className="w-3 h-3" /> : <ChevronRight className="w-3 h-3" />}
              {expanded ? 'Show less' : `Show all ${coverage_count}`}
            </button>
          )}
        </div>
      </div>

      {former_employers && former_employers.length > 0 && (
        <div className="flex items-start gap-2 pt-0.5">
          <Briefcase className="w-3.5 h-3.5 text-stone-300 mt-0.5 flex-shrink-0" />
          <div className="text-[12px] leading-snug text-stone-400">
            Previously{' '}
            {former_employers.map((f, i) => (
              <span key={`former-${f.name}-${i}`}>
                {i > 0 && <span className="text-stone-300"> {'·'} </span>}
                <span className="font-medium text-stone-500">{f.name || '—'}</span>
                {f.email && <span className="text-stone-400"> {'·'} <span className="font-mono">{f.email}</span></span>}
              </span>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}
