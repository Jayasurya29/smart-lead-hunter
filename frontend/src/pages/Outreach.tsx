import { useState, useEffect } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { useSearchParams } from 'react-router-dom'
import { Plus, Sparkles, Search, Loader2, Inbox, CheckCircle2, XCircle, Send, Building2 } from 'lucide-react'
import {
  listOutreach, getOutreachStats,
  ResearchRecord, ApprovalStatus,
} from '@/api/outreach'
import OutreachDetail from '@/components/outreach/OutreachDetail'
import OutreachComposer from '@/components/outreach/OutreachComposer'

type TabKey = 'pending' | 'approved' | 'rejected' | 'sent' | 'all'

const TABS: { key: TabKey; label: string; icon: React.ElementType; statusFilter: string | null }[] = [
  { key: 'pending',  label: 'Pending Review', icon: Inbox,        statusFilter: 'pending' },
  { key: 'approved', label: 'Approved',       icon: CheckCircle2, statusFilter: 'approved' },
  { key: 'sent',     label: 'Sent',           icon: Send,         statusFilter: 'sent' },
  { key: 'rejected', label: 'Rejected',       icon: XCircle,      statusFilter: 'rejected' },
  { key: 'all',      label: 'All History',    icon: Sparkles,     statusFilter: null },
]

/**
 * Outreach tab — three-pane layout:
 *   - Top header with "+ New Outreach" button + tabs
 *   - Left: list of research records (filtered by tab)
 *   - Right: detail panel for selected record (or empty state)
 *
 * On "+ New Outreach" click → modal opens, user fills form, watches
 * SSE progress, completes → record appears at top of Pending tab,
 * detail panel opens automatically.
 */
export default function Outreach() {
  const [searchParams, setSearchParams] = useSearchParams()
  const [tab, setTab] = useState<TabKey>('pending')
  const [search, setSearch] = useState('')
  const [page, setPage] = useState(1)
  const [selectedId, setSelectedId] = useState<number | null>(null)
  const [composerOpen, setComposerOpen] = useState(false)
  const [composerPrefill, setComposerPrefill] = useState<{
    contactId?: number
    parentKind?: 'lead' | 'existing_hotel'
    parentId?: number
  }>({})
  const [toast, setToast] = useState<{ msg: string; visible: boolean }>({ msg: '', visible: false })
  const qc = useQueryClient()
  const PER_PAGE = 30

  // Auto-open composer when navigated here with ?new=1&contact_id=...
  // (used by the "Send to Outreach" button on lead/hotel contact cards).
  useEffect(() => {
    if (searchParams.get('new') === '1') {
      const cid = searchParams.get('contact_id')
      const pkind = searchParams.get('parent_kind') as 'lead' | 'existing_hotel' | null
      const pid = searchParams.get('parent_id')
      setComposerPrefill({
        contactId: cid ? Number(cid) : undefined,
        parentKind: pkind || undefined,
        parentId: pid ? Number(pid) : undefined,
      })
      setComposerOpen(true)
      // Strip the params so a refresh doesn't re-open the modal
      const next = new URLSearchParams(searchParams)
      next.delete('new')
      next.delete('contact_id')
      next.delete('parent_kind')
      next.delete('parent_id')
      setSearchParams(next, { replace: true })
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  const filterStatus = TABS.find((t) => t.key === tab)?.statusFilter

  const { data: stats } = useQuery({
    queryKey: ['outreach-stats'],
    queryFn: getOutreachStats,
    refetchInterval: 30000,
  })

  const { data, isLoading } = useQuery({
    queryKey: ['outreach', tab, page, search],
    queryFn: () =>
      listOutreach({
        page,
        per_page: PER_PAGE,
        status: filterStatus || undefined,
        search: search.trim() || undefined,
      }),
    refetchInterval: 30000,
  })

  const rows = data?.rows || []
  const selected = rows.find((r) => r.id === selectedId) || null

  return (
    <div className="h-full flex flex-col bg-stone-50">
      {/* ── Header — compact single-row layout with color treatment ─ */}
      <div className="px-6 py-3 bg-gradient-to-r from-purple-50/60 via-white to-white border-b border-stone-200 flex-shrink-0">
        <div className="flex items-center gap-4">
          {/* Title block — purple icon badge + heading */}
          <div className="flex items-center gap-2.5 flex-shrink-0">
            <div className="w-9 h-9 rounded-lg bg-gradient-to-br from-purple-500 to-purple-700 flex items-center justify-center shadow-sm">
              <Sparkles className="w-4 h-4 text-white" />
            </div>
            <div>
              <h1 className="text-base font-bold text-navy-900 leading-tight">Outreach</h1>
              <p className="text-2xs text-stone-500">AI-personalized · review · copy &amp; send</p>
            </div>
          </div>

          {/* Tabs — color-coded by status, active state has color depth */}
          <div className="flex items-center gap-1 flex-shrink-0">
            {TABS.map((t) => {
              const Icon = t.icon
              const count = t.statusFilter
                ? (stats as any)?.[t.statusFilter] ?? 0
                : stats?.total ?? 0
              const isActive = tab === t.key
              // Each tab has its own color theme so the user gets a sense
              // of where they are even mid-skim
              const themes = {
                pending:  { active: 'bg-amber-50 text-amber-800 border-amber-200',   badge: 'bg-amber-200 text-amber-900' },
                approved: { active: 'bg-blue-50 text-blue-800 border-blue-200',      badge: 'bg-blue-200 text-blue-900' },
                sent:     { active: 'bg-emerald-50 text-emerald-800 border-emerald-200', badge: 'bg-emerald-200 text-emerald-900' },
                rejected: { active: 'bg-rose-50 text-rose-800 border-rose-200',      badge: 'bg-rose-200 text-rose-900' },
                all:      { active: 'bg-purple-50 text-purple-800 border-purple-200', badge: 'bg-purple-200 text-purple-900' },
              } as const
              const theme = themes[t.key]
              return (
                <button
                  key={t.key}
                  onClick={() => {
                    setTab(t.key)
                    setPage(1)
                    setSelectedId(null)
                  }}
                  className={`flex items-center gap-1.5 px-3 py-1.5 text-xs font-semibold rounded-md transition border ${
                    isActive
                      ? theme.active
                      : 'text-stone-600 hover:bg-stone-100 border-transparent'
                  }`}
                >
                  <Icon className="w-3.5 h-3.5" />
                  <span>{t.label}</span>
                  {count > 0 && (
                    <span
                      className={`ml-0.5 px-1.5 py-0.5 text-2xs font-bold rounded ${
                        isActive ? theme.badge : 'bg-stone-200 text-stone-600'
                      }`}
                    >
                      {count}
                    </span>
                  )}
                </button>
              )
            })}
          </div>

          {/* Search */}
          <div className="relative flex-1 max-w-md ml-auto">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-stone-400" />
            <input
              type="text"
              value={search}
              onChange={(e) => {
                setSearch(e.target.value)
                setPage(1)
              }}
              placeholder="Search contact, hotel, subject..."
              className="w-full pl-9 pr-3 h-9 text-xs bg-white border border-stone-200 rounded-md focus:outline-none focus:border-purple-400 focus:ring-2 focus:ring-purple-100"
            />
          </div>

          {/* New Outreach button — far right with stronger gradient */}
          <button
            onClick={() => setComposerOpen(true)}
            className="flex items-center gap-1.5 px-4 py-2 bg-gradient-to-br from-purple-600 to-purple-700 text-white text-xs font-bold rounded-md hover:from-purple-700 hover:to-purple-800 transition shadow-sm hover:shadow-md flex-shrink-0"
          >
            <Plus className="w-3.5 h-3.5" />
            New Outreach
          </button>
        </div>
      </div>

      {/* ── Body — list + detail ───────────────────────────── */}
      <div className="flex-1 flex overflow-hidden">
        {/* List */}
        <div className="w-2/5 max-w-md border-r border-stone-200 bg-white overflow-y-auto">
          {isLoading ? (
            <div className="flex items-center justify-center h-32 text-stone-400">
              <Loader2 className="w-5 h-5 animate-spin" />
            </div>
          ) : rows.length === 0 ? (
            <EmptyState tab={tab} onNew={() => setComposerOpen(true)} />
          ) : (
            <div className="divide-y divide-stone-100">
              {rows.map((r) => (
                <OutreachRow
                  key={r.id}
                  record={r}
                  selected={r.id === selectedId}
                  onClick={() => setSelectedId(r.id)}
                />
              ))}
            </div>
          )}

          {/* Pagination */}
          {data && data.pages > 1 && (
            <div className="px-4 py-3 border-t border-stone-100 flex items-center justify-between bg-stone-50">
              <span className="text-2xs text-stone-400">
                Page {page} of {data.pages} · {data.total} total
              </span>
              <div className="flex gap-1">
                <button
                  onClick={() => setPage((p) => Math.max(1, p - 1))}
                  disabled={page <= 1}
                  className="px-2 py-1 text-2xs font-semibold rounded bg-white border border-stone-200 text-stone-500 disabled:opacity-30 hover:bg-stone-100"
                >
                  ←
                </button>
                <button
                  onClick={() => setPage((p) => Math.min(data.pages, p + 1))}
                  disabled={page >= data.pages}
                  className="px-2 py-1 text-2xs font-semibold rounded bg-white border border-stone-200 text-stone-500 disabled:opacity-30 hover:bg-stone-100"
                >
                  →
                </button>
              </div>
            </div>
          )}
        </div>

        {/* Detail */}
        <div className="flex-1 overflow-hidden">
          {selected ? (
            <OutreachDetail record={selected} onClose={() => setSelectedId(null)} />
          ) : (
            <SelectPrompt />
          )}
        </div>
      </div>

      {/* ── Composer Modal ─────────────────────────────────── */}
      {composerOpen && (
        <OutreachComposer
          onClose={() => {
            setComposerOpen(false)
            setComposerPrefill({})
          }}
          onComplete={(researchId) => {
            setComposerOpen(false)
            setComposerPrefill({})
            setTab('pending')
            // Force refetch — the new outreach should appear at the
            // top of the Pending list without the user needing to
            // refresh manually
            qc.invalidateQueries({ queryKey: ['outreach'] })
            qc.invalidateQueries({ queryKey: ['outreach-stats'] })
            setSelectedId(researchId)
            setToast({ msg: 'Outreach generated · ready to review', visible: true })
            setTimeout(() => setToast({ msg: '', visible: false }), 3500)
          }}
          initialContactId={composerPrefill.contactId}
          initialParentKind={composerPrefill.parentKind}
          initialParentId={composerPrefill.parentId}
        />
      )}

      {/* ── Toast notification ─────────────────────────────── */}
      {toast.visible && (
        <div className="fixed bottom-6 right-6 z-50 animate-fadeIn">
          <div className="flex items-center gap-3 px-4 py-3 bg-navy-900 text-white rounded-lg shadow-2xl">
            <div className="w-6 h-6 rounded-full bg-emerald-500 flex items-center justify-center flex-shrink-0">
              <CheckCircle2 className="w-4 h-4" strokeWidth={3} />
            </div>
            <span className="text-sm font-medium">{toast.msg}</span>
          </div>
        </div>
      )}
    </div>
  )
}

/* ──────────────── Subcomponents ──────────────── */

function OutreachRow({
  record, selected, onClick,
}: {
  record: ResearchRecord
  selected: boolean
  onClick: () => void
}) {
  const fitScore = record.fit_score ?? 0
  // Gradient avatars matching detail-panel header — same color logic
  const avatarStyle =
    fitScore >= 80 ? 'bg-gradient-to-br from-emerald-500 to-emerald-600 text-white'
      : fitScore >= 60 ? 'bg-gradient-to-br from-amber-500 to-amber-600 text-white'
      : fitScore >= 40 ? 'bg-gradient-to-br from-stone-500 to-stone-600 text-white'
      : 'bg-gradient-to-br from-red-400 to-red-500 text-white'

  return (
    <button
      onClick={onClick}
      className={`w-full text-left px-4 py-3.5 transition group ${
        selected
          ? 'bg-gradient-to-r from-purple-50/80 to-transparent border-l-[3px] border-purple-600'
          : 'hover:bg-stone-50/80 border-l-[3px] border-transparent'
      }`}
    >
      <div className="flex items-start gap-3">
        {/* Larger avatar with shadow + gradient — matches detail header style */}
        <div className={`flex-shrink-0 w-11 h-11 rounded-lg flex items-center justify-center text-sm font-bold shadow-sm ${avatarStyle}`}>
          {fitScore}
        </div>
        <div className="flex-1 min-w-0">
          <div className="flex items-center justify-between gap-2 mb-0.5">
            <h3 className="text-sm font-bold text-navy-900 truncate">{record.contact_name}</h3>
            <StatusBadge status={record.approval_status} />
          </div>
          <p className="text-xs font-medium text-stone-700 truncate">{record.contact_title || '—'}</p>
          <p className="text-2xs text-stone-500 truncate mt-0.5 flex items-center gap-1">
            <Building2 className="w-3 h-3 flex-shrink-0 text-stone-400" />
            <span className="truncate">{record.hotel_name}</span>
          </p>
          {record.outreach_angle && (
            <p className="text-2xs text-purple-700 mt-1.5 italic line-clamp-1 flex items-center gap-1">
              <Sparkles className="w-3 h-3 flex-shrink-0" />
              <span className="truncate">{record.outreach_angle}</span>
            </p>
          )}
        </div>
      </div>
    </button>
  )
}

function StatusBadge({ status }: { status: ApprovalStatus }) {
  const colors: Record<ApprovalStatus, string> = {
    pending:  'bg-amber-100 text-amber-800',
    approved: 'bg-blue-100 text-blue-800',
    sent:     'bg-emerald-100 text-emerald-800',
    rejected: 'bg-red-100 text-red-800',
  }
  const labels: Record<ApprovalStatus, string> = {
    pending: 'Pending',
    approved: 'Approved',
    sent: 'Sent',
    rejected: 'Rejected',
  }
  return (
    <span className={`px-1.5 py-0.5 text-2xs font-bold rounded ${colors[status]}`}>
      {labels[status]}
    </span>
  )
}

function EmptyState({ tab, onNew }: { tab: TabKey; onNew: () => void }) {
  const messages: Record<TabKey, string> = {
    pending:  'No pending outreaches. Click "New Outreach" to generate one.',
    approved: 'No approved outreaches yet.',
    sent:     'No sent outreaches yet.',
    rejected: 'No rejected outreaches.',
    all:      'No outreaches generated yet.',
  }
  return (
    <div className="px-6 py-16 text-center">
      <Sparkles className="w-10 h-10 text-stone-200 mx-auto mb-3" />
      <p className="text-sm text-stone-500 mb-4">{messages[tab]}</p>
      {(tab === 'pending' || tab === 'all') && (
        <button
          onClick={onNew}
          className="inline-flex items-center gap-1.5 px-3 py-1.5 text-xs font-semibold text-purple-700 bg-purple-50 hover:bg-purple-100 rounded-md"
        >
          <Plus className="w-3 h-3" />
          New Outreach
        </button>
      )}
    </div>
  )
}

function SelectPrompt() {
  return (
    <div className="h-full flex items-center justify-center text-center px-6">
      <div>
        <Sparkles className="w-10 h-10 text-stone-200 mx-auto mb-3" />
        <p className="text-sm text-stone-500">Select an outreach to view details</p>
        <p className="text-xs text-stone-400 mt-1">Brief, email, LinkedIn, follow-up sequence</p>
      </div>
    </div>
  )
}
