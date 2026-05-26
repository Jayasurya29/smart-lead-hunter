// src/pages/Dashboard.tsx — minimal animated replacement.
// Same hooks, same handlers, same layout — just the TAB BAR is upgraded:
// a sliding amber-tinted pill glides between Pipeline / Approved / Rejected
// instead of the active tab snapping into place, and the tab count tweens.

import { useState, useEffect, useCallback, useLayoutEffect, useRef } from 'react'
import { useSearchParams } from 'react-router-dom'
import { useLeads } from '@/hooks/useLeads'
import type { LeadTab } from '@/api/types'
import StatsCards from '@/components/stats/StatsCards'
import LeadTable from '@/components/leads/LeadTable'
import LeadDetail from '@/components/leads/LeadDetail'
import FilterBar, { DEFAULT_FILTERS, type Filters } from '@/components/leads/FilterBar'
import { cn } from '@/lib/utils'
import { Inbox, CheckCircle2, XCircle, Search, X, Download, Loader2 } from 'lucide-react'
import api from '@/api/client'
import { markLeadReviewed } from '@/api/leads'

const TABS: { key: LeadTab; label: string; icon: React.ElementType }[] = [
  { key: 'pipeline', label: 'Pipeline', icon: Inbox },
  { key: 'approved', label: 'Approved', icon: CheckCircle2 },
  { key: 'rejected', label: 'Rejected', icon: XCircle },
]

/* ─── tween-up helper (mirrors StatsCards) ─── */
function useCountUp(target: number | undefined, duration = 700): number | undefined {
  const [val, setVal] = useState<number | undefined>(target)
  const fromRef  = useRef<number>(target ?? 0)
  const startRef = useRef<number | null>(null)
  const rafRef   = useRef<number | null>(null)
  useEffect(() => {
    if (target === undefined) { setVal(undefined); return }
    fromRef.current = (val ?? 0)
    startRef.current = null
    const ease = (t: number) => 1 - Math.pow(1 - t, 3)
    const tick = (ts: number) => {
      if (startRef.current === null) startRef.current = ts
      const t = Math.min(1, (ts - startRef.current) / duration)
      const next = fromRef.current + (target - fromRef.current) * ease(t)
      setVal(t === 1 ? target : next)
      if (t < 1) rafRef.current = requestAnimationFrame(tick)
    }
    rafRef.current = requestAnimationFrame(tick)
    return () => { if (rafRef.current) cancelAnimationFrame(rafRef.current) }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [target])
  return val === undefined ? undefined : Math.round(val)
}

/* ─── animated tab bar ─── */
function AnimatedTabs({
  tab, onChange, totalForTab,
}: {
  tab: LeadTab
  onChange: (t: LeadTab) => void
  totalForTab: number | undefined
}) {
  const wrapRef = useRef<HTMLDivElement>(null)
  const btnRefs = useRef<Record<string, HTMLButtonElement | null>>({})
  const [pill, setPill] = useState({ x: 0, w: 0, ready: false })
  const tween = useCountUp(totalForTab)

  useLayoutEffect(() => {
    function measure() {
      const el = btnRefs.current[tab]; const wrap = wrapRef.current
      if (!el || !wrap) return
      const wr = wrap.getBoundingClientRect()
      const r  = el.getBoundingClientRect()
      setPill({ x: r.left - wr.left, w: r.width, ready: true })
    }
    measure()
    // Re-measure after fonts finish loading (fixes shift on first login)
    document.fonts?.ready?.then(() => requestAnimationFrame(measure))
  }, [tab, tween])

  useEffect(() => {
    function onResize() {
      const el = btnRefs.current[tab]; const wrap = wrapRef.current
      if (!el || !wrap) return
      const wr = wrap.getBoundingClientRect()
      const r  = el.getBoundingClientRect()
      setPill(p => ({ ...p, x: r.left - wr.left, w: r.width }))
    }
    window.addEventListener('resize', onResize)
    return () => window.removeEventListener('resize', onResize)
  }, [tab])

  return (
    <div ref={wrapRef}
         className="relative flex gap-px bg-stone-200/60 p-0.5 rounded-lg flex-shrink-0">
      <span className="tab-pill"
            style={{
              transform: `translateX(${pill.x - 2}px)`,
              width: pill.w,
              opacity: pill.ready ? 1 : 0,
            }} />
      {TABS.map((t) => {
        const Icon = t.icon
        const isActive = tab === t.key
        return (
          <button
            key={t.key}
            ref={(el) => { btnRefs.current[t.key] = el }}
            onClick={() => onChange(t.key)}
            className={cn(
              'relative z-10 flex items-center gap-1.5 px-3.5 py-2 text-xs font-semibold rounded-md transition-colors duration-200',
              isActive ? 'text-navy-900' : 'text-stone-500 hover:text-stone-700',
            )}
          >
            <Icon className="w-4 h-4" />
            {t.label}
            {/* Reserve count badge width on active tab to prevent layout
                shift when data arrives. Inactive tabs don't need it. */}
            {isActive && (
              <span className={cn(
                'text-2xs ml-0.5 tabular-nums',
                tween !== undefined ? 'text-navy-500' : 'invisible',
              )}>
                {tween !== undefined ? tween.toLocaleString() : '00'}
              </span>
            )}
          </button>
        )
      })}
    </div>
  )
}

export default function Dashboard() {
  const [tab, setTab] = useState<LeadTab>('pipeline')
  const [page, setPage] = useState(1)
  const [search, setSearch] = useState('')
  const [selectedLeadId, setSelectedLeadId] = useState<number | null>(null)
  const [searchParams, setSearchParams] = useSearchParams()

  const [perPage, setPerPage] = useState<number>(() => {
    try {
      const saved = window.localStorage.getItem('slh.leads.perPage')
      const n = saved ? Number(saved) : NaN
      return [25, 50, 100].includes(n) ? n : 25
    } catch { return 25 }
  })
  function handlePerPageChange(n: number) {
    setPerPage(n); setPage(1)
    try { window.localStorage.setItem('slh.leads.perPage', String(n)) } catch { /* silent */ }
  }

  useEffect(() => {
    const leadParam = searchParams.get('lead')
    if (leadParam) {
      setSelectedLeadId(Number(leadParam))
      setSearchParams({}, { replace: true })
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  const [filters, setFilters] = useState<Filters>(DEFAULT_FILTERS)
  const [exporting, setExporting] = useState(false)
  const { data, isLoading } = useLeads(tab, page, search, filters as unknown as Record<string, string>, perPage)
  const totalPages = data?.pages ?? (Math.ceil((data?.total || 0) / (data?.per_page || perPage)) || 1)

  function handleTabChange(newTab: LeadTab) {
    setTab(newTab); setPage(1); setSelectedLeadId(null); setFilters(DEFAULT_FILTERS)
  }
  function handleSearch(val: string) { setSearch(val); setPage(1) }
  function handleFilterChange(newFilters: Filters) { setFilters(newFilters); setPage(1) }
  function handleSort(sort: string) { setFilters((prev) => ({ ...prev, sort })); setPage(1) }
  function handleStatClick(action: { tab?: string; timeline?: string }) {
    if (action.tab) setTab(action.tab as LeadTab)
    if (action.timeline) setFilters({ ...DEFAULT_FILTERS, timeline: action.timeline })
    else setFilters(DEFAULT_FILTERS)
    setPage(1); setSelectedLeadId(null)
  }

  const handleSelectLead = useCallback((id: number) => {
    setSelectedLeadId(id); markLeadReviewed(id)
  }, [])

  return (
    <div className="h-full flex flex-col">
      <div className="px-4 pt-3 pb-2 flex-shrink-0">
        <StatsCards onFilter={handleStatClick} activeTab={tab} activeTimeline={filters.timeline} />
      </div>

      <div className="px-4 pb-2 flex-shrink-0 space-y-2.5">
        <div className="flex items-center gap-4">
          <AnimatedTabs tab={tab} onChange={handleTabChange} totalForTab={data?.total} />

          <div className="relative flex-1 max-w-lg search-fluid">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-stone-400" />
            <input
              type="text"
              value={search}
              onChange={(e) => handleSearch(e.target.value)}
              placeholder="Search hotel, brand, city..."
              className="w-full h-9 pl-9 pr-9 text-sm bg-white border border-stone-200 rounded-lg outline-none focus:border-navy-400 focus:ring-2 focus:ring-navy-100 transition placeholder:text-stone-400"
            />
            {search && (
              <button onClick={() => handleSearch('')}
                      className="absolute right-2.5 top-1/2 -translate-y-1/2 p-0.5 text-stone-400 hover:text-stone-600 rounded transition">
                <X className="w-3.5 h-3.5" />
              </button>
            )}
          </div>

          <button
            onClick={async () => {
              setExporting(true)
              try {
                const queryParams: Record<string, string> = {}
                const STATUS_BY_TAB: Record<string, string> = {
                  pipeline: 'new', approved: 'approved', rejected: 'rejected',
                }
                if (STATUS_BY_TAB[tab]) queryParams.status = STATUS_BY_TAB[tab]
                if (filters.timeline) queryParams.timeline = filters.timeline
                if (filters.tier)     queryParams.tier     = filters.tier
                if (filters.location) queryParams.location = filters.location
                if (search)           queryParams.search   = search
                const res = await api.get('/leads/export', {
                  params: queryParams, responseType: 'blob', headers: { Accept: '*/*' },
                })
                const blob = new Blob([res.data], {
                  type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                })
                let filename = `JA_NewHotels_${new Date().toISOString().split('T')[0]}.xlsx`
                const cd = res.headers?.['content-disposition'] || ''
                const match = /filename="?([^"]+)"?/i.exec(cd)
                if (match?.[1]) filename = match[1]
                const url = URL.createObjectURL(blob)
                const a = document.createElement('a')
                a.href = url; a.download = filename
                document.body.appendChild(a); a.click(); document.body.removeChild(a)
                URL.revokeObjectURL(url)
              } catch (e) {
                console.error('Export failed', e); alert('Export failed — check console for details.')
              } finally { setExporting(false) }
            }}
            disabled={exporting}
            className="btn-export flex items-center gap-1.5 px-3 h-9 text-xs font-semibold text-stone-600 bg-white border border-stone-200 rounded-lg hover:bg-emerald-50 hover:text-emerald-700 hover:border-emerald-300 transition disabled:opacity-50 flex-shrink-0"
            title="Export to Excel"
          >
            {exporting ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Download className="w-3.5 h-3.5" />}
            {exporting ? 'Exporting...' : 'Export'}
          </button>
        </div>

        <FilterBar filters={filters} onChange={handleFilterChange} />
      </div>

      <div className="flex-1 flex overflow-hidden px-4 pb-3 gap-3">
        <div className={cn(
          'bg-white rounded-xl border border-slate-200/80 shadow-sm overflow-hidden flex flex-col transition-all duration-300',
          selectedLeadId ? 'flex-[3]' : 'flex-1',
        )}>
          <LeadTable
            leads={data?.leads || []}
            total={data?.total || 0}
            page={page}
            totalPages={totalPages}
            tab={tab}
            selectedId={selectedLeadId}
            onSelect={handleSelectLead}
            onPageChange={setPage}
            onSort={handleSort}
            currentSort={filters.sort}
            isLoading={isLoading}
            perPage={perPage}
            onPerPageChange={handlePerPageChange}
          />
        </div>

        {selectedLeadId && (
          <div className="flex-[2] bg-white rounded-xl border border-slate-200/80 shadow-sm overflow-hidden animate-slideIn">
            <LeadDetail leadId={selectedLeadId} tab={tab} onClose={() => setSelectedLeadId(null)} />
          </div>
        )}
      </div>
    </div>
  )
}
