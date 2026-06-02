/**
 * Inbox Contacts page (Phase 4)
 * ==============================
 * Displays contacts extracted from JA Uniforms Gmail mailboxes via
 * signature parsing (inbox_sync.py). Contacts land as "pending" and
 * get triaged: Approve → Push to Insightly, or Reject (hard delete).
 *
 * Layout:
 *   - Stats header bar (total, P1-P4, pending/approved, last sync)
 *   - Filter row: priority, status tabs, search, sort
 *   - Sortable table with expandable detail rows
 *   - Bulk approve + per-row Approve/Reject/Push buttons
 *   - Manual sync trigger button
 */
import { useState, useMemo } from 'react'
import { useSearchParams } from 'react-router-dom'
import { cn, formatDate, relativeDate, getTierLabel } from '@/lib/utils'
import {
  useInboxContacts,
  useInboxContactStats,
  useTriggerInboxSync,
  useDeepEnrichContact,
} from '@/hooks/useInboxContacts'
import type { InboxContact } from '@/api/inboxContacts'
import {
  Search, X, ChevronLeft, ChevronRight, ChevronDown,
  Loader2, RefreshCw, ExternalLink,
  Mail, Phone, Building2, User, Linkedin, Inbox, Shield, Users,
  Sparkles, Clock, Eye, Hash, Check,
} from 'lucide-react'

/* ════════════════════════════════════════
   MAIN PAGE
   ════════════════════════════════════════ */

export default function ContactsPage() {
  const [searchParams, setSearchParams] = useSearchParams()

  // URL-synced state
  const page = Number(searchParams.get('page') || '1')
  const perPage = Number(searchParams.get('per_page') || '50')
  const search = searchParams.get('search') || ''
  const priority = searchParams.get('priority') || ''
  const category = searchParams.get('category') || ''
  const status = searchParams.get('status') || ''
  const sortBy = searchParams.get('sort') || 'priority_score'

  // Local UI state
  const [searchInput, setSearchInput] = useState(search)
  const [expandedId, setExpandedId] = useState<number | null>(null)

  // Data
  const statsQ = useInboxContactStats()
  const listQ = useInboxContacts({
    page,
    per_page: perPage,
    search: search || undefined,
    procurement_priority: priority || undefined,
    contact_category: category || undefined,
    approval_status: status || undefined,
    order_by: sortBy,
  })

  // Mutations
  const syncMut = useTriggerInboxSync()

  const stats = statsQ.data
  const items = listQ.data?.items || []
  const total = listQ.data?.total || 0
  const pages = listQ.data?.pages || 1

  // URL update helper
  function updateParams(updates: Record<string, string | null>) {
    const next = new URLSearchParams(searchParams)
    for (const [k, v] of Object.entries(updates)) {
      if (v) next.set(k, v)
      else next.delete(k)
    }
    // Reset page when filters change (unless page itself is being set)
    if (!('page' in updates)) next.set('page', '1')
    setSearchParams(next)
  }

  function handleSearch(e: React.FormEvent) {
    e.preventDefault()
    updateParams({ search: searchInput || null })
  }

  return (
    <div className="h-full flex flex-col overflow-hidden bg-stone-50">

      {/* ── HEADER ── */}
      <div className="flex-shrink-0 bg-white border-b border-stone-200 shadow-soft px-6 pt-5 pb-3">
        <div className="flex items-center justify-between mb-3">
          <div className="flex items-center gap-3">
            <div className="w-9 h-9 rounded-xl bg-gradient-to-br from-navy-600 to-navy-800 flex items-center justify-center shadow-soft">
              <Users className="w-4.5 h-4.5 text-white" />
            </div>
            <div>
              <h1 className="text-lg font-bold text-navy-900 leading-tight">Contacts</h1>
              <p className="text-2xs text-stone-400 font-medium uppercase tracking-wide">Intelligence Directory</p>
            </div>
          </div>
          <div className="flex items-center gap-3">
            {stats?.last_sync_at && (
              <span className="text-2xs text-stone-400">
                Synced {relativeDate(stats.last_sync_at)}
              </span>
            )}
            <button
              onClick={() => syncMut.mutate()}
              disabled={syncMut.isPending}
              className={cn(
                'flex items-center gap-1.5 px-3 h-8 rounded-lg text-xs font-semibold transition-all',
                syncMut.isPending
                  ? 'bg-stone-100 text-stone-400'
                  : 'bg-navy-600 text-white hover:bg-navy-700 shadow-soft',
              )}
            >
              <RefreshCw className={cn('w-3.5 h-3.5', syncMut.isPending && 'animate-spin')} />
              {syncMut.isPending ? 'Syncing…' : 'Sync Now'}
            </button>
          </div>
        </div>

        {/* Quick facts — reference, not a workflow queue */}
        {stats && (
          <div className="flex items-center gap-3 text-xs text-stone-500">
            <span className="inline-flex items-center gap-1.5"><span className="w-1.5 h-1.5 rounded-full bg-navy-500" /><span className="font-bold text-navy-900 tabular-nums">{stats.total.toLocaleString()}</span> contacts</span>
            <span className="text-stone-300">·</span>
            <span className="inline-flex items-center gap-1.5"><span className="w-1.5 h-1.5 rounded-full bg-gold-400" /><span className="font-bold text-gold-600 tabular-nums">{stats.decision_makers.toLocaleString()}</span> decision-makers</span>
            <span className="text-stone-300">·</span>
            <span><span className="font-bold text-stone-700 tabular-nums">{stats.with_phone.toLocaleString()}</span> with phone</span>
            <span className="text-stone-300">·</span>
            <span><span className="font-bold text-coral-500 tabular-nums">{stats.new_today.toLocaleString()}</span> new today</span>
          </div>
        )}
      </div>

      {/* ── CATEGORY TABS (primary slice) ── */}
      {stats && (
        <div className="flex-shrink-0 bg-white border-b border-stone-200 px-6 py-2.5 flex items-center gap-1.5 overflow-x-auto">
          <CatTab label="All" count={stats.total} active={!category} onClick={() => updateParams({ category: null })} />
          <CatTab label="Buyers" count={stats.buyer} color="emerald" active={category === 'buyer'} onClick={() => updateParams({ category: category === 'buyer' ? null : 'buyer' })} />
          <CatTab label="Sellers" count={stats.seller} color="gold" active={category === 'seller'} onClick={() => updateParams({ category: category === 'seller' ? null : 'seller' })} />
          <CatTab label="Competitors" count={stats.competitor} color="coral" active={category === 'competitor'} onClick={() => updateParams({ category: category === 'competitor' ? null : 'competitor' })} />
          <CatTab label="Personal" count={stats.personal} color="navy" active={category === 'personal'} onClick={() => updateParams({ category: category === 'personal' ? null : 'personal' })} />
          <CatTab label="Junk" count={stats.junk} color="stone" active={category === 'junk'} onClick={() => updateParams({ category: category === 'junk' ? null : 'junk' })} />
          <div className="w-px h-5 bg-stone-200 mx-1.5" />
          <CatTab label="★ Decision-makers" count={stats.decision_makers} color="gold" active={false} onClick={() => updateParams({ sort: 'priority_score' })} />
        </div>
      )}
      <div className="flex-shrink-0 bg-white border-b border-stone-200 px-6 py-2.5 flex items-center gap-3">
        {/* Search */}
        <form onSubmit={handleSearch} className="relative flex-1 max-w-sm">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-stone-400" />
          <input
            value={searchInput}
            onChange={e => setSearchInput(e.target.value)}
            placeholder="Search name, email, org, title…"
            className="w-full h-9 pl-9 pr-8 text-sm bg-stone-50 border border-stone-200 rounded-lg outline-none focus:bg-white focus:border-navy-400 focus:ring-2 focus:ring-navy-100 transition placeholder:text-stone-400"
          />
          {searchInput && (
            <button
              type="button"
              onClick={() => { setSearchInput(''); updateParams({ search: null }) }}
              className="absolute right-2 top-1/2 -translate-y-1/2 p-0.5 text-stone-400 hover:text-stone-600"
            >
              <X className="w-3.5 h-3.5" />
            </button>
          )}
        </form>

        {/* Sort */}
        <select
          value={sortBy}
          onChange={e => updateParams({ sort: e.target.value })}
          className="text-xs border border-stone-200 rounded-lg px-2 py-1.5 text-stone-600 bg-white"
        >
          <option value="priority_score">Priority Score</option>
          <option value="last_seen">Last Seen</option>
          <option value="first_seen">First Seen</option>
          <option value="name">Name A-Z</option>
        </select>

        {/* Per page */}
        <select
          value={perPage}
          onChange={e => updateParams({ per_page: e.target.value })}
          className="text-xs border border-stone-200 rounded-lg px-2 py-1.5 text-stone-600 bg-white"
        >
          <option value="50">50 / page</option>
          <option value="100">100 / page</option>
          <option value="200">200 / page</option>
          <option value="500">500 / page</option>
        </select>

        {/* Active filters indicator */}
        {(priority || status || search || category) && (
          <button
            onClick={() => {
              setSearchInput('')
              setSearchParams(new URLSearchParams())
            }}
            className="flex items-center gap-1 text-xs text-stone-400 hover:text-coral-500 transition"
          >
            <X className="w-3 h-3" />
            Clear filters
          </button>
        )}

        <div className="flex-1" />
      </div>

      {/* ── TABLE ── */}
      <div className="flex-1 overflow-auto px-6 py-4">
        {listQ.isLoading ? (
          <div className="flex items-center justify-center h-64">
            <Loader2 className="w-6 h-6 animate-spin text-navy-400" />
          </div>
        ) : items.length === 0 ? (
          <div className="flex flex-col items-center justify-center h-64 text-stone-400 bg-white rounded-xl border border-stone-200 shadow-card">
            <Inbox className="w-10 h-10 mb-2 text-stone-300" />
            <p className="text-sm font-medium text-stone-500">No contacts found</p>
            <p className="text-xs mt-1">Try a different category or search</p>
          </div>
        ) : (
          <div className="bg-white rounded-xl border border-stone-200 shadow-card overflow-hidden">
          <table className="w-full text-sm">
            <thead className="sticky top-0 z-10 bg-stone-100/80 backdrop-blur border-b border-stone-200">
              <tr className="text-left text-2xs font-bold text-stone-400 uppercase tracking-wider">
                <th className="pl-5 px-3 py-2.5">Name</th>
                <th className="px-3 py-2.5">Email</th>
                <th className="px-3 py-2.5">Role</th>
                <th className="px-3 py-2.5">Organization</th>
                <th className="px-3 py-2.5 w-28">Category</th>
                <th className="px-3 py-2.5 w-16 text-center">Emails</th>
                <th className="px-3 py-2.5 w-24">Last Seen</th>
                <th className="px-2 py-2.5 w-10 pr-5"></th>
              </tr>
            </thead>
            <tbody className="divide-y divide-stone-100">
              {items.map(contact => (
                <ContactRow
                  key={contact.id}
                  contact={contact}
                  isExpanded={expandedId === contact.id}
                  onToggleExpand={() => setExpandedId(expandedId === contact.id ? null : contact.id)}
                />
              ))}
            </tbody>
          </table>
          </div>
        )}
      </div>

      {/* ── PAGINATION ── */}
      {pages > 1 && (
        <div className="flex-shrink-0 bg-white border-t border-stone-200 px-6 py-2.5 flex items-center justify-between">
          <span className="text-xs text-stone-400">
            Showing {((page - 1) * perPage) + 1}–{Math.min(page * perPage, total)} of {total.toLocaleString()}
          </span>
          <div className="flex items-center gap-1">
            <button
              disabled={page <= 1}
              onClick={() => updateParams({ page: String(page - 1) })}
              className="p-1.5 rounded-lg text-stone-400 hover:bg-stone-100 disabled:opacity-30"
            >
              <ChevronLeft className="w-4 h-4" />
            </button>
            <span className="text-xs font-medium text-stone-600 px-2">
              {page} / {pages}
            </span>
            <button
              disabled={page >= pages}
              onClick={() => updateParams({ page: String(page + 1) })}
              className="p-1.5 rounded-lg text-stone-400 hover:bg-stone-100 disabled:opacity-30"
            >
              <ChevronRight className="w-4 h-4" />
            </button>
          </div>
        </div>
      )}
    </div>
  )
}


const CAT_COLORS: Record<string, string> = {
  emerald: 'text-emerald-700 bg-emerald-50 ring-emerald-200',
  gold: 'text-gold-700 bg-gold-50 ring-gold-200',
  coral: 'text-coral-600 bg-coral-50 ring-coral-200',
  navy: 'text-navy-700 bg-navy-50 ring-navy-200',
  stone: 'text-stone-600 bg-stone-100 ring-stone-300',
}

function CatTab({
  label, count, color = 'navy', active, onClick,
}: {
  label: string
  count: number
  color?: string
  active: boolean
  onClick: () => void
}) {
  return (
    <button
      onClick={onClick}
      className={cn(
        'flex items-center gap-1.5 px-3 h-8 rounded-lg text-xs font-semibold whitespace-nowrap transition-all',
        active
          ? CAT_COLORS[color] + ' ring-1 shadow-soft'
          : 'text-stone-500 hover:bg-stone-100/70',
      )}
    >
      {label}
      <span className={cn('tabular-nums px-1.5 py-0.5 rounded-md text-2xs', active ? 'bg-white/60' : 'bg-stone-100 text-stone-400')}>{count.toLocaleString()}</span>
    </button>
  )
}

const CATEGORY_BADGE: Record<string, string> = {
  buyer: 'bg-emerald-50 text-emerald-700 ring-1 ring-emerald-200',
  seller: 'bg-gold-50 text-gold-700 ring-1 ring-gold-200',
  competitor: 'bg-coral-50 text-coral-600 ring-1 ring-coral-200',
  personal: 'bg-navy-50 text-navy-600 ring-1 ring-navy-200',
  junk: 'bg-stone-100 text-stone-500 ring-1 ring-stone-200',
}

function CategoryBadge({ category }: { category: string | null }) {
  if (!category) return <span className="text-stone-300 text-xs">—</span>
  return (
    <span className={cn(
      'inline-flex px-2 py-0.5 rounded-full text-2xs font-bold capitalize',
      CATEGORY_BADGE[category] || 'bg-stone-100 text-stone-500',
    )}>
      {category}
    </span>
  )
}

/* ════════════════════════════════════════
   CONTACT ROW + EXPANDED DETAIL
   ════════════════════════════════════════ */

function ContactRow({
  contact,
  isExpanded,
  onToggleExpand,
}: {
  contact: InboxContact
  isExpanded: boolean
  onToggleExpand: () => void
}) {
  const name = [contact.first_name, contact.last_name].filter(Boolean).join(' ') || contact.display_name || '—'

  return (
    <>
      <tr
        className={cn(
          'group hover:bg-stone-50/80 transition-colors cursor-pointer',
          isExpanded && 'bg-stone-50',
        )}
        onClick={onToggleExpand}
      >
        <td className="pl-5 px-3 py-3 font-semibold text-navy-900 max-w-[170px]">
          <span className="flex items-center gap-1.5 truncate">
            {contact.is_decision_maker && (
              <span title="Likely decision-maker" className="text-gold-500 flex-shrink-0">★</span>
            )}
            <span className="truncate">{name}</span>
          </span>
        </td>
        <td className="px-3 py-3 text-stone-500 max-w-[210px] truncate font-mono text-2xs">
          {contact.email}
        </td>
        <td className="px-3 py-3 text-stone-600 max-w-[180px]">
          {contact.title
            ? <span className="truncate block">{contact.title}</span>
            : contact.inferred_role
              ? <span className="truncate italic text-stone-400 block" title="AI-inferred role">{contact.inferred_role}</span>
              : <span className="text-stone-300">—</span>}
        </td>
        <td className="px-3 py-3 text-stone-700 max-w-[180px]">
          <span className="flex items-center gap-1.5 truncate">
            <span className="truncate font-medium">{contact.organization || '—'}</span>
            {contact.gpo && (
              <span
                className="inline-flex items-center gap-0.5 px-1 py-0.5 rounded text-[10px] font-bold bg-gold-50 text-gold-600 ring-1 ring-gold-200 flex-shrink-0"
                title={`${contact.gpo} GPO — informational only`}
              >
                <Shield className="w-2.5 h-2.5" /> GPO
              </span>
            )}
          </span>
        </td>
        <td className="px-3 py-3">
          <CategoryBadge category={contact.contact_category} />
        </td>
        <td className="px-3 py-3 text-center">
          <span className="inline-flex items-center gap-1 text-xs text-stone-400 tabular-nums">
            <Mail className="w-3 h-3" />
            {contact.interaction_count}
          </span>
        </td>
        <td className="px-3 py-3 text-2xs text-stone-400">
          {relativeDate(contact.last_seen)}
        </td>
        <td className="px-2 py-3 pr-5 text-right">
          <ChevronDown className={cn('w-4 h-4 inline text-stone-300 transition-transform', isExpanded && 'rotate-180 text-navy-500')} />
        </td>
      </tr>

      {/* Expanded detail row */}
      {isExpanded && (
        <tr className="bg-stone-50">
          <td colSpan={8} className="px-5 py-4 border-l-2 border-navy-500">
            <ContactDetail contact={contact} />
          </td>
        </tr>
      )}
    </>
  )
}


/* ════════════════════════════════════════
   EXPANDED DETAIL
   ════════════════════════════════════════ */

function ContactDetail({ contact }: { contact: InboxContact }) {
  const deepMut = useDeepEnrichContact()
  const [result, setResult] = useState<string | null>(null)

  async function runEnrich(findEmail: boolean) {
    setResult(null)
    try {
      const r = await deepMut.mutateAsync({ id: contact.id, findEmail })
      const bits = [
        r.role && `Role: ${r.role}`,
        r.background && r.background,
        r.found_email && `Found email: ${r.found_email}`,
        `(${r.sources_used} sources, ${Math.round((r.confidence || 0) * 100)}% confidence)`,
      ].filter(Boolean)
      setResult(bits.join(' · ') || 'No new info found.')
    } catch {
      setResult('Enrichment failed — check Serper/Wiza keys or try again.')
    }
  }

  return (
    <div className="space-y-4">
      {/* ── AI INTELLIGENCE BAND ── */}
      <div className="rounded-xl border border-navy-200 bg-gradient-to-br from-navy-50 to-white px-4 py-3.5 shadow-soft">
        <div className="flex items-center justify-between mb-3">
          <h4 className="text-xs font-bold text-navy-700 uppercase tracking-wider flex items-center gap-1.5">
            <Sparkles className="w-3.5 h-3.5 text-gold-500" /> AI Intelligence
          </h4>
          <div className="flex items-center gap-2">
            <button
              onClick={() => runEnrich(false)}
              disabled={deepMut.isPending}
              className="text-xs font-semibold px-3 h-7 rounded-lg bg-navy-600 text-white hover:bg-navy-700 disabled:opacity-50 transition shadow-soft inline-flex items-center gap-1.5"
            >
              <Sparkles className="w-3 h-3" />
              {deepMut.isPending ? 'Researching…' : 'Deep Enrich'}
            </button>
            {(!contact.email || !contact.email.includes('@')) && (
              <button
                onClick={() => runEnrich(true)}
                disabled={deepMut.isPending}
                className="text-xs font-semibold px-3 h-7 rounded-lg border border-navy-300 text-navy-700 bg-white hover:bg-navy-50 disabled:opacity-50 transition"
                title="Uses Wiza credits"
              >
                Find Email
              </button>
            )}
          </div>
        </div>
        <div className="grid grid-cols-4 gap-2.5">
          {[
            ['Category', <CategoryBadge category={contact.contact_category} />],
            ['Role', <span className="text-sm font-medium text-navy-900">{contact.inferred_role || contact.title || '—'}</span>],
            ['Seniority', <span className="text-sm font-medium text-navy-900 capitalize">{contact.seniority || '—'}</span>],
            ['Decision-maker', contact.is_decision_maker ? <span className="text-sm font-bold text-gold-600">★ Yes</span> : <span className="text-sm text-stone-400">No</span>],
          ].map(([label, val], i) => (
            <div key={i} className="bg-white rounded-lg border border-stone-200 px-3 py-2">
              <div className="text-[10px] text-stone-400 uppercase tracking-wide font-semibold mb-0.5">{label}</div>
              {val}
            </div>
          ))}
        </div>
        {contact.background && (
          <p className="mt-3 text-xs text-stone-600 leading-relaxed bg-white/70 rounded-lg px-3 py-2 border border-stone-100">{contact.background}</p>
        )}
        {result && (
          <p className="mt-2 text-xs text-navy-800 bg-gold-50 ring-1 ring-gold-200 rounded-lg px-3 py-2 leading-relaxed">{result}</p>
        )}
        {contact.enrichment_source && (
          <p className="mt-2 text-[10px] text-stone-400">
            source: {contact.enrichment_source}
            {contact.enrichment_confidence != null && ` · ${Math.round(contact.enrichment_confidence * 100)}% confidence`}
          </p>
        )}
      </div>

    <div className="grid grid-cols-3 gap-6 text-sm bg-white rounded-xl border border-stone-200 shadow-soft px-5 py-4">
      {/* Column 1: Personal */}
      <div className="space-y-2">
        <h4 className="text-xs font-bold text-stone-400 uppercase tracking-wider mb-2">Contact Info</h4>
        <DetailRow icon={User} label="Name" value={[contact.first_name, contact.last_name].filter(Boolean).join(' ') || contact.display_name} />
        <DetailRow icon={Mail} label="Email" value={contact.email} mono />
        <DetailRow icon={Phone} label="Phone" value={contact.phone} />
        <DetailRow icon={Building2} label="Address" value={contact.address} />
        {contact.linkedin_url && (
          <div className="flex items-center gap-2">
            <Linkedin className="w-3.5 h-3.5 text-stone-400 flex-shrink-0" />
            <a
              href={contact.linkedin_url}
              target="_blank"
              rel="noopener noreferrer"
              className="text-navy-600 hover:underline text-xs truncate"
            >
              LinkedIn Profile <ExternalLink className="inline w-3 h-3 ml-0.5" />
            </a>
          </div>
        )}
      </div>

      {/* Column 2: Hospitality Enrichment */}
      <div className="space-y-2">
        <h4 className="text-xs font-bold text-stone-400 uppercase tracking-wider mb-2">Hospitality Intel</h4>
        <DetailRow icon={Building2} label="Organization" value={contact.organization} />
        <DetailRow icon={Shield} label="Parent Company" value={contact.parent_company} />
        <DetailRow icon={Sparkles} label="Brand Tier" value={contact.brand_tier ? getTierLabel(contact.brand_tier) : null} />
        <DetailRow icon={Hash} label="Operating Model" value={contact.operating_model} />
        <DetailRow icon={Hash} label="Management Co." value={contact.management_company} />
        <DetailRow icon={Hash} label="GPO" value={contact.gpo} />
        <DetailRow icon={Hash} label="Opportunity" value={
          contact.opportunity_level
            ? `${contact.opportunity_level}${contact.opportunity_score != null ? ` (${contact.opportunity_score})` : ''}`
            : null
        } />
        {contact.priority_reason && (
          <div className="mt-1 text-xs text-stone-500 bg-stone-100 rounded px-2 py-1">
            {contact.priority_reason}
          </div>
        )}
      </div>

      {/* Column 3: Tracking & History */}
      <div className="space-y-2">
        <h4 className="text-xs font-bold text-stone-400 uppercase tracking-wider mb-2">Tracking</h4>
        <DetailRow icon={Mail} label="Interactions" value={String(contact.interaction_count)} />
        <DetailRow icon={Eye} label="First Seen" value={formatDate(contact.first_seen)} />
        <DetailRow icon={Clock} label="Last Seen" value={formatDate(contact.last_seen)} />
        <DetailRow icon={Inbox} label="Source Mailboxes" value={contact.source_mailboxes?.join(', ')} />
        <DetailRow icon={Hash} label="Confidence" value={contact.confidence != null ? `${(contact.confidence * 100).toFixed(0)}%` : null} />
        <DetailRow icon={Check} label="Has Signature" value={contact.has_signature ? 'Yes' : 'No'} />
        {contact.matched_lead_id && (
          <DetailRow icon={ExternalLink} label="Matched Lead" value={`#${contact.matched_lead_id}`} />
        )}
        {contact.matched_hotel_id && (
          <DetailRow icon={ExternalLink} label="Matched Hotel" value={`#${contact.matched_hotel_id}`} />
        )}

        {/* Sync history (last 5 events) */}
        {contact.sync_history && contact.sync_history.length > 0 && (
          <div className="mt-3">
            <h5 className="text-xs font-semibold text-stone-400 mb-1">Recent Sync Events</h5>
            <div className="space-y-0.5 max-h-32 overflow-y-auto">
              {contact.sync_history.slice(-5).reverse().map((evt, i) => (
                <div key={i} className="text-2xs text-stone-400 font-mono">
                  {evt.ts ? new Date(evt.ts).toLocaleString() : '—'} · {evt.action}
                  {evt.mailbox && ` · ${evt.mailbox}`}
                </div>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
    </div>
  )
}

function DetailRow({
  icon: Icon,
  label,
  value,
  mono,
}: {
  icon: React.ElementType
  label: string
  value: string | null | undefined
  mono?: boolean
}) {
  if (!value) return null
  return (
    <div className="flex items-start gap-2">
      <Icon className="w-3.5 h-3.5 text-stone-400 flex-shrink-0 mt-0.5" />
      <div className="min-w-0">
        <span className="text-2xs text-stone-400 font-medium">{label}</span>
        <p className={cn('text-xs text-stone-700 truncate', mono && 'font-mono')}>{value}</p>
      </div>
    </div>
  )
}
