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
import { cn, formatDate, relativeDate, getTierLabel, getTierColor } from '@/lib/utils'
import {
  useInboxContacts,
  useInboxContactStats,
  useApproveInboxContact,
  useBulkApproveInboxContacts,
  useDeleteInboxContact,
  useTriggerInboxSync,
} from '@/hooks/useInboxContacts'
import type { InboxContact } from '@/api/inboxContacts'
import {
  Search, X, ChevronLeft, ChevronRight, ChevronDown, ChevronUp,
  Loader2, CheckCircle2, XCircle, RefreshCw, ExternalLink,
  Mail, Phone, Building2, User, Linkedin, Inbox, Shield,
  Sparkles, Clock, Eye, Hash, Filter, Check,
} from 'lucide-react'

/* ════════════════════════════════════════
   PRIORITY BADGE
   ════════════════════════════════════════ */

const PRIORITY_COLORS: Record<string, string> = {
  P1: 'bg-coral-100 text-coral-700 ring-1 ring-coral-200',
  P2: 'bg-gold-100 text-gold-700 ring-1 ring-gold-200',
  P3: 'bg-navy-100 text-navy-600',
  P4: 'bg-stone-100 text-stone-500',
  P_unknown: 'bg-stone-50 text-stone-400',
}

function PriorityBadge({ priority }: { priority: string }) {
  const label = priority === 'P_unknown' ? '?' : priority
  return (
    <span className={cn(
      'inline-flex items-center px-2 py-0.5 rounded-full text-xs font-bold tracking-wide',
      PRIORITY_COLORS[priority] || PRIORITY_COLORS.P_unknown,
    )}>
      {label}
    </span>
  )
}

/* ════════════════════════════════════════
   STATUS BADGE
   ════════════════════════════════════════ */

const STATUS_COLORS: Record<string, string> = {
  pending: 'bg-amber-50 text-amber-600',
  approved: 'bg-emerald-50 text-emerald-600',
  pushed_to_insightly: 'bg-violet-50 text-violet-600',
}

function StatusBadge({ status }: { status: string }) {
  const labels: Record<string, string> = {
    pending: 'Pending',
    approved: 'Approved',
    pushed_to_insightly: 'Pushed',
  }
  return (
    <span className={cn(
      'inline-flex items-center px-2 py-0.5 rounded-full text-xs font-semibold',
      STATUS_COLORS[status] || 'bg-stone-100 text-stone-400',
    )}>
      {labels[status] || status}
    </span>
  )
}

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
  const status = searchParams.get('status') || ''
  const sortBy = searchParams.get('sort') || 'priority_score'

  // Local UI state
  const [searchInput, setSearchInput] = useState(search)
  const [selected, setSelected] = useState<Set<number>>(new Set())
  const [expandedId, setExpandedId] = useState<number | null>(null)

  // Data
  const statsQ = useInboxContactStats()
  const listQ = useInboxContacts({
    page,
    per_page: perPage,
    search: search || undefined,
    procurement_priority: priority || undefined,
    approval_status: status || undefined,
    order_by: sortBy,
  })

  // Mutations
  const approveMut = useApproveInboxContact()
  const bulkApproveMut = useBulkApproveInboxContacts()
  const deleteMut = useDeleteInboxContact()
  const syncMut = useTriggerInboxSync()

  const stats = statsQ.data
  const items = listQ.data?.items || []
  const total = listQ.data?.total || 0
  const pages = listQ.data?.pages || 1

  // Selection helpers
  const allSelected = items.length > 0 && items.every(c => selected.has(c.id))
  const someSelected = selected.size > 0

  function toggleSelect(id: number) {
    setSelected(prev => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })
  }

  function toggleSelectAll() {
    if (allSelected) {
      setSelected(new Set())
    } else {
      setSelected(new Set(items.map(c => c.id)))
    }
  }

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
    setSelected(new Set())
  }

  function handleSearch(e: React.FormEvent) {
    e.preventDefault()
    updateParams({ search: searchInput || null })
  }

  // Bulk approve
  function handleBulkApprove() {
    const pendingSelected = items
      .filter(c => selected.has(c.id) && c.approval_status === 'pending')
      .map(c => c.id)
    if (pendingSelected.length === 0) return
    bulkApproveMut.mutate(pendingSelected, {
      onSuccess: () => setSelected(new Set()),
    })
  }

  return (
    <div className="h-full flex flex-col overflow-hidden">

      {/* ── STATS HEADER ── */}
      <div className="flex-shrink-0 bg-white border-b border-stone-200/60 px-6 py-4">
        <div className="flex items-center justify-between mb-3">
          <div className="flex items-center gap-3">
            <Inbox className="w-5 h-5 text-navy-600" />
            <h1 className="text-lg font-bold text-navy-900">Inbox Contacts</h1>
            {stats && (
              <span className="text-sm text-stone-400 font-medium">
                {stats.total.toLocaleString()} total
              </span>
            )}
          </div>
          <div className="flex items-center gap-2">
            {stats?.last_sync_at && (
              <span className="text-xs text-stone-400">
                Last sync: {relativeDate(stats.last_sync_at)}
              </span>
            )}
            <button
              onClick={() => syncMut.mutate()}
              disabled={syncMut.isPending}
              className={cn(
                'flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-semibold transition-all',
                syncMut.isPending
                  ? 'bg-stone-100 text-stone-400'
                  : 'bg-navy-50 text-navy-700 hover:bg-navy-100',
              )}
            >
              <RefreshCw className={cn('w-3.5 h-3.5', syncMut.isPending && 'animate-spin')} />
              {syncMut.isPending ? 'Syncing…' : 'Sync Now'}
            </button>
          </div>
        </div>

        {/* Stat pills */}
        {stats && (
          <div className="flex items-center gap-2 flex-wrap">
            <StatPill label="P1" value={stats.p1} color="bg-coral-50 text-coral-600" onClick={() => updateParams({ priority: priority === 'P1' ? null : 'P1' })} active={priority === 'P1'} />
            <StatPill label="P2" value={stats.p2} color="bg-gold-50 text-gold-600" onClick={() => updateParams({ priority: priority === 'P2' ? null : 'P2' })} active={priority === 'P2'} />
            <StatPill label="P3" value={stats.p3} color="bg-navy-50 text-navy-600" onClick={() => updateParams({ priority: priority === 'P3' ? null : 'P3' })} active={priority === 'P3'} />
            <StatPill label="P4" value={stats.p4} color="bg-stone-50 text-stone-500" onClick={() => updateParams({ priority: priority === 'P4' ? null : 'P4' })} active={priority === 'P4'} />
            <div className="w-px h-5 bg-stone-200 mx-1" />
            <StatPill label="Pending" value={stats.pending} color="bg-amber-50 text-amber-600" onClick={() => updateParams({ status: status === 'pending' ? null : 'pending' })} active={status === 'pending'} />
            <StatPill label="Approved" value={stats.approved} color="bg-emerald-50 text-emerald-600" onClick={() => updateParams({ status: status === 'approved' ? null : 'approved' })} active={status === 'approved'} />
            <StatPill label="Pushed" value={stats.pushed_to_insightly} color="bg-violet-50 text-violet-600" onClick={() => updateParams({ status: status === 'pushed_to_insightly' ? null : 'pushed_to_insightly' })} active={status === 'pushed_to_insightly'} />
            <div className="w-px h-5 bg-stone-200 mx-1" />
            <StatPill label="New Today" value={stats.new_today} color="bg-sky-50 text-sky-600" />
            <StatPill label="Has Sig" value={stats.with_signature} color="bg-stone-50 text-stone-500" />
            <StatPill label="Has Phone" value={stats.with_phone} color="bg-stone-50 text-stone-500" />
          </div>
        )}
      </div>

      {/* ── FILTER ROW ── */}
      <div className="flex-shrink-0 bg-white border-b border-stone-200/40 px-6 py-2.5 flex items-center gap-3">
        {/* Search */}
        <form onSubmit={handleSearch} className="relative flex-1 max-w-sm">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-stone-400" />
          <input
            value={searchInput}
            onChange={e => setSearchInput(e.target.value)}
            placeholder="Search name, email, org, title…"
            className="w-full pl-9 pr-8 py-1.5 text-sm border border-stone-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-navy-200 focus:border-navy-400"
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
        {(priority || status || search) && (
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

        {/* Bulk approve */}
        {someSelected && (
          <button
            onClick={handleBulkApprove}
            disabled={bulkApproveMut.isPending}
            className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-semibold bg-emerald-600 text-white hover:bg-emerald-700 transition-all"
          >
            <Check className="w-3.5 h-3.5" />
            Approve {selected.size} selected
          </button>
        )}
      </div>

      {/* ── TABLE ── */}
      <div className="flex-1 overflow-auto">
        {listQ.isLoading ? (
          <div className="flex items-center justify-center h-64">
            <Loader2 className="w-6 h-6 animate-spin text-navy-400" />
          </div>
        ) : items.length === 0 ? (
          <div className="flex flex-col items-center justify-center h-64 text-stone-400">
            <Inbox className="w-10 h-10 mb-2" />
            <p className="text-sm font-medium">No contacts found</p>
            <p className="text-xs mt-1">Try adjusting your filters or run a sync</p>
          </div>
        ) : (
          <table className="w-full text-sm">
            <thead className="sticky top-0 z-10 bg-stone-50 border-b border-stone-200">
              <tr className="text-left text-xs font-semibold text-stone-500 uppercase tracking-wider">
                <th className="pl-6 pr-2 py-2.5 w-8">
                  <input
                    type="checkbox"
                    checked={allSelected}
                    onChange={toggleSelectAll}
                    className="rounded border-stone-300 text-navy-600 focus:ring-navy-400"
                  />
                </th>
                <th className="px-2 py-2.5 w-14">Priority</th>
                <th className="px-2 py-2.5">Name</th>
                <th className="px-2 py-2.5">Email</th>
                <th className="px-2 py-2.5">Title</th>
                <th className="px-2 py-2.5">Organization</th>
                <th className="px-2 py-2.5 w-20">Tier</th>
                <th className="px-2 py-2.5 w-16 text-center">Emails</th>
                <th className="px-2 py-2.5 w-20">Status</th>
                <th className="px-2 py-2.5 w-24">Last Seen</th>
                <th className="px-2 py-2.5 w-28 text-right pr-6">Actions</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-stone-100">
              {items.map(contact => (
                <ContactRow
                  key={contact.id}
                  contact={contact}
                  isSelected={selected.has(contact.id)}
                  isExpanded={expandedId === contact.id}
                  onToggleSelect={() => toggleSelect(contact.id)}
                  onToggleExpand={() => setExpandedId(expandedId === contact.id ? null : contact.id)}
                  onApprove={() => approveMut.mutate(contact.id)}
                  onDelete={() => { if (confirm(`Delete ${contact.email}? This cannot be undone.`)) deleteMut.mutate(contact.id) }}
                  approving={approveMut.isPending}
                  deleting={deleteMut.isPending}
                />
              ))}
            </tbody>
          </table>
        )}
      </div>

      {/* ── PAGINATION ── */}
      {pages > 1 && (
        <div className="flex-shrink-0 bg-white border-t border-stone-200/60 px-6 py-2.5 flex items-center justify-between">
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


/* ════════════════════════════════════════
   STAT PILL
   ════════════════════════════════════════ */

function StatPill({
  label, value, color, onClick, active,
}: {
  label: string
  value: number
  color: string
  onClick?: () => void
  active?: boolean
}) {
  const Tag = onClick ? 'button' : 'div'
  return (
    <Tag
      onClick={onClick}
      className={cn(
        'inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-semibold transition-all',
        color,
        onClick && 'cursor-pointer hover:opacity-80',
        active && 'ring-2 ring-navy-400 ring-offset-1',
      )}
    >
      {label}
      <span className="font-bold tabular-nums">{value.toLocaleString()}</span>
    </Tag>
  )
}


/* ════════════════════════════════════════
   CONTACT ROW + EXPANDED DETAIL
   ════════════════════════════════════════ */

function ContactRow({
  contact,
  isSelected,
  isExpanded,
  onToggleSelect,
  onToggleExpand,
  onApprove,
  onDelete,
  approving,
  deleting,
}: {
  contact: InboxContact
  isSelected: boolean
  isExpanded: boolean
  onToggleSelect: () => void
  onToggleExpand: () => void
  onApprove: () => void
  onDelete: () => void
  approving: boolean
  deleting: boolean
}) {
  const name = [contact.first_name, contact.last_name].filter(Boolean).join(' ') || contact.display_name || '—'
  const isPending = contact.approval_status === 'pending'

  return (
    <>
      <tr
        className={cn(
          'group hover:bg-stone-50/80 transition-colors cursor-pointer',
          isSelected && 'bg-navy-50/40',
          isExpanded && 'bg-stone-50',
        )}
        onClick={onToggleExpand}
      >
        <td className="pl-6 pr-2 py-2.5" onClick={e => e.stopPropagation()}>
          <input
            type="checkbox"
            checked={isSelected}
            onChange={onToggleSelect}
            className="rounded border-stone-300 text-navy-600 focus:ring-navy-400"
          />
        </td>
        <td className="px-2 py-2.5">
          <PriorityBadge priority={contact.procurement_priority} />
        </td>
        <td className="px-2 py-2.5 font-medium text-navy-900 max-w-[160px] truncate">
          {name}
        </td>
        <td className="px-2 py-2.5 text-stone-600 max-w-[200px] truncate font-mono text-xs">
          {contact.email}
        </td>
        <td className="px-2 py-2.5 text-stone-500 max-w-[160px] truncate">
          {contact.title || '—'}
        </td>
        <td className="px-2 py-2.5 text-stone-600 max-w-[160px] truncate">
          {contact.organization || '—'}
        </td>
        <td className="px-2 py-2.5">
          {contact.brand_tier ? (
            <span className={cn('inline-flex px-1.5 py-0.5 rounded text-2xs font-semibold', getTierColor(contact.brand_tier))}>
              {getTierLabel(contact.brand_tier)}
            </span>
          ) : (
            <span className="text-stone-300">—</span>
          )}
        </td>
        <td className="px-2 py-2.5 text-center">
          <span className="inline-flex items-center gap-1 text-xs text-stone-500 tabular-nums">
            <Mail className="w-3 h-3" />
            {contact.interaction_count}
          </span>
        </td>
        <td className="px-2 py-2.5">
          <StatusBadge status={contact.approval_status} />
        </td>
        <td className="px-2 py-2.5 text-xs text-stone-400">
          {relativeDate(contact.last_seen)}
        </td>
        <td className="px-2 py-2.5 pr-6 text-right" onClick={e => e.stopPropagation()}>
          <div className="flex items-center justify-end gap-1">
            {isPending && (
              <>
                <button
                  onClick={onApprove}
                  disabled={approving}
                  title="Approve"
                  className="p-1 rounded text-emerald-500 hover:bg-emerald-50 transition"
                >
                  <CheckCircle2 className="w-4 h-4" />
                </button>
                <button
                  onClick={onDelete}
                  disabled={deleting}
                  title="Reject (delete)"
                  className="p-1 rounded text-stone-400 hover:text-coral-500 hover:bg-coral-50 transition"
                >
                  <XCircle className="w-4 h-4" />
                </button>
              </>
            )}
            <button
              onClick={onToggleExpand}
              className="p-1 rounded text-stone-400 hover:text-navy-600 hover:bg-navy-50 transition"
            >
              {isExpanded ? <ChevronUp className="w-4 h-4" /> : <ChevronDown className="w-4 h-4" />}
            </button>
          </div>
        </td>
      </tr>

      {/* Expanded detail row */}
      {isExpanded && (
        <tr className="bg-stone-50/60">
          <td colSpan={11} className="px-6 py-4">
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
  return (
    <div className="grid grid-cols-3 gap-6 text-sm">
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
