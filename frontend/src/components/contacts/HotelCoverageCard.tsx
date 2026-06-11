import { useState } from 'react'
import { Layers, Briefcase, Star, Loader2, ChevronRight, Check } from 'lucide-react'
import { useNavigate } from 'react-router-dom'
import { useAccountCoverage, useSaveLeadContact } from '@/hooks/useAffiliations'
import type { AccountCoveragePerson } from '@/api/affiliations'

// matches ContactsPage: lead-gen people are offset so one ?selected= id space
// addresses both inbox contacts and lead_contacts.
const LEAD_ID_OFFSET = 10_000_000

/**
 * "Who covers this hotel" — resolved from contact_affiliations, distinct from
 * the hotel's own directly-linked contact list. Shows:
 *   - portfolio buyers (operator-corporate people whose management company runs
 *     this hotel — they cover it by derivation, via='management_company')
 *   - direct contacts/correspondents affiliated straight to this account
 * Includes lead-generator AND inbox-sourced people once edges exist. Renders
 * nothing if there's no edge-based coverage to add.
 */
function PersonRow({
  p,
  onOpen,
  onSave,
  saving,
}: {
  p: AccountCoveragePerson
  onOpen: () => void
  onSave: () => void
  saving: boolean
}) {
  // inbox contacts always resolve; lead contacts only once a human has saved
  // (verified) them. Unverified rows aren't a dead click — they offer Save.
  const verified = p.person_type === 'contact' || p.is_saved

  const Identity = (
    <div className="min-w-0 flex-1">
      <div className="flex items-center gap-1.5">
        <span
          className={
            'text-[13px] font-medium truncate ' +
            (verified ? 'text-stone-700 group-hover:text-navy-700' : 'text-stone-400')
          }
        >
          {p.name || p.email || '—'}
        </span>
        {p.is_decision_maker && <Star className="w-3 h-3 text-gold-500 flex-shrink-0" />}
        {(p.scope === 'regional' || p.scope === 'cluster') && (
          <span className="flex-shrink-0 text-[10px] px-1.5 py-0.5 rounded-md bg-amber-50 text-amber-700 ring-1 ring-amber-200">
            {p.scope}
          </span>
        )}
        {!verified && (
          <span className="flex-shrink-0 text-[10px] px-1.5 py-0.5 rounded-md bg-stone-100 text-stone-500">
            unverified
          </span>
        )}
      </div>
      {(p.title || p.organization) && (
        <div className="text-[11px] text-stone-400 truncate">
          {[p.title, p.organization].filter(Boolean).join(' · ')}
        </div>
      )}
    </div>
  )

  if (verified) {
    return (
      <button
        type="button"
        onClick={onOpen}
        title="Open contact profile"
        className="group w-full text-left flex items-center gap-2 py-1.5 px-1 -mx-1 rounded-lg hover:bg-stone-50 transition"
      >
        {Identity}
        {p.via === 'management_company' && (
          <span className="flex-shrink-0 text-[10px] px-1.5 py-0.5 rounded-md bg-emerald-50 text-emerald-700 ring-1 ring-emerald-200">
            portfolio
          </span>
        )}
        <ChevronRight className="w-3.5 h-3.5 text-stone-300 opacity-0 group-hover:opacity-100 flex-shrink-0 transition" />
      </button>
    )
  }

  // unverified lead contact — show Save so a human can vouch it into the directory
  return (
    <div className="w-full flex items-center gap-2 py-1.5 px-1 -mx-1">
      {Identity}
      {p.via === 'management_company' && (
        <span className="flex-shrink-0 text-[10px] px-1.5 py-0.5 rounded-md bg-emerald-50 text-emerald-700 ring-1 ring-emerald-200">
          portfolio
        </span>
      )}
      <button
        type="button"
        onClick={onSave}
        disabled={saving}
        title="Verify this contact — adds it to the directory"
        className="flex-shrink-0 inline-flex items-center gap-1 text-[11px] font-semibold px-2 py-1 rounded-md bg-navy-600 text-white hover:bg-navy-700 transition disabled:opacity-50"
      >
        {saving ? <Loader2 className="w-3 h-3 animate-spin" /> : <Check className="w-3 h-3" />}
        Save
      </button>
    </div>
  )
}

export default function HotelCoverageCard({
  accountType,
  accountId,
}: {
  accountType: 'existing_hotel' | 'potential_lead'
  accountId: number
}) {
  const { data, isLoading, isError } = useAccountCoverage(accountType, accountId)
  const navigate = useNavigate()
  const saveMut = useSaveLeadContact()
  const [savingId, setSavingId] = useState<number | null>(null)

  function openPerson(p: AccountCoveragePerson) {
    const selected = p.person_type === 'lead_contact' ? p.person_id + LEAD_ID_OFFSET : p.person_id
    navigate(`/contacts?selected=${selected}`)
  }

  function savePerson(p: AccountCoveragePerson) {
    setSavingId(p.person_id)
    saveMut.mutate(p.person_id, { onSettled: () => setSavingId(null) })
  }

  if (isLoading) {
    return (
      <div className="mb-4 flex items-center gap-2 text-xs text-stone-400">
        <Loader2 className="w-3.5 h-3.5 animate-spin" /> Resolving coverage…
      </div>
    )
  }
  if (isError || !data || data.people_count === 0) return null

  const { people, people_count, management_company } = data
  const portfolio = people.filter((p) => p.via === 'management_company')
  const direct = people.filter((p) => p.via !== 'management_company')

  return (
    <div className="mb-4 rounded-xl ring-1 ring-stone-200/70 bg-white p-4">
      <div className="flex items-center gap-2 mb-2">
        <Layers className="w-4 h-4 text-navy-600" />
        <span className="text-sm font-semibold text-stone-700">Coverage</span>
        <span className="text-xs text-stone-400">
          {people_count} {people_count === 1 ? 'person' : 'people'}
        </span>
      </div>

      {portfolio.length > 0 && (
        <div className="mt-1">
          <div className="flex items-center gap-1 text-2xs uppercase tracking-wide text-stone-400 mb-0.5">
            <Briefcase className="w-3 h-3" />
            Via management company{management_company ? ` · ${management_company}` : ''}
          </div>
          {portfolio.map((p) => (
            <PersonRow key={`${p.person_type}-${p.person_id}`} p={p} onOpen={() => openPerson(p)} onSave={() => savePerson(p)} saving={savingId === p.person_id} />
          ))}
        </div>
      )}

      {direct.length > 0 && (
        <div className="mt-2">
          <div className="text-2xs uppercase tracking-wide text-stone-400 mb-0.5">
            Direct
          </div>
          {direct.map((p) => (
            <PersonRow key={`${p.person_type}-${p.person_id}`} p={p} onOpen={() => openPerson(p)} onSave={() => savePerson(p)} saving={savingId === p.person_id} />
          ))}
        </div>
      )}
    </div>
  )
}
