import { useState, useEffect } from 'react'
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

const TABS: { key: LeadTab; label: string; icon: React.ElementType }[] = [
  { key: 'pipeline', label: 'Pipeline', icon: Inbox },
  { key: 'approved', label: 'Approved', icon: CheckCircle2 },
  { key: 'rejected', label: 'Rejected', icon: XCircle },
]

export default function Dashboard() {
  const [tab, setTab] = useState<LeadTab>('pipeline')
  const [page, setPage] = useState(1)
  const [search, setSearch] = useState('')
  const [selectedLeadId, setSelectedLeadId] = useState<number | null>(null)
  const [searchParams, setSearchParams] = useSearchParams()

  // Page-size persists across sessions via localStorage so the user's
  // boss can set 50 once and have it stick. Defaults to 25 if no
  // saved preference. Falls back gracefully if localStorage is blocked.
  const [perPage, setPerPage] = useState<number>(() => {
    try {
      const saved = window.localStorage.getItem('slh.leads.perPage')
      const n = saved ? Number(saved) : NaN
      return [25, 50, 100].includes(n) ? n : 25
    } catch {
      return 25
    }
  })
  function handlePerPageChange(n: number) {
    setPerPage(n)
    setPage(1)
    try { window.localStorage.setItem('slh.leads.perPage', String(n)) } catch { /* silent */ }
  }

  // Auto-open lead from map (e.g. ?lead=123)
  useEffect(() => {
    const leadParam = searchParams.get('lead')
    if (leadParam) {
      setSelectedLeadId(Number(leadParam))
      setSearchParams({}, { replace: true }) // clean URL
    }
  }, [])
  const [filters, setFilters] = useState<Filters>(DEFAULT_FILTERS)
  const [exporting, setExporting] = useState(false)

  const { data, isLoading } = useLeads(tab, page, search, filters as unknown as Record<string, string>, perPage)

  const totalPages = data?.pages ?? (Math.ceil((data?.total || 0) / (data?.per_page || perPage)) || 1)

  function handleTabChange(newTab: LeadTab) {
    setTab(newTab)
    setPage(1)
    setSelectedLeadId(null)
    setFilters(DEFAULT_FILTERS)
  }

  function handleSearch(val: string) {
    setSearch(val)
    setPage(1)
  }

  function handleFilterChange(newFilters: Filters) {
    setFilters(newFilters)
    setPage(1)
  }

  function handleSort(sort: string) {
    setFilters((prev) => ({ ...prev, sort }))
    setPage(1)
  }

  function handleStatClick(action: { tab?: string; timeline?: string }) {
    if (action.tab) setTab(action.tab as LeadTab)
    if (action.timeline) {
      setFilters({ ...DEFAULT_FILTERS, timeline: action.timeline })
    } else {
      setFilters(DEFAULT_FILTERS)
    }
    setPage(1)
    setSelectedLeadId(null)
  }

  return (
    <div className="h-full flex flex-col">
      <div className="px-4 pt-3 pb-2 flex-shrink-0">
        <StatsCards onFilter={handleStatClick} activeTab={tab} activeTimeline={filters.timeline} />
      </div>

      <div className="px-4 pb-2 flex-shrink-0 space-y-2.5">
        <div className="flex items-center gap-4">
          <div className="flex gap-px bg-stone-200/60 p-0.5 rounded-lg flex-shrink-0">
            {TABS.map((t) => {
              const Icon = t.icon
              const count = data && t.key === tab ? data.total : undefined
              return (
                <button
                  key={t.key}
                  onClick={() => handleTabChange(t.key)}
                  className={cn(
                    'flex items-center gap-1.5 px-3.5 py-2 text-xs font-semibold rounded-md transition-all duration-150',
                    tab === t.key
                      ? 'bg-white text-navy-900 shadow-sm'
                      : 'text-stone-500 hover:text-stone-700',
                  )}
                >
                  <Icon className="w-4 h-4" />
                  {t.label}
                  {count !== undefined && (
                    <span className={cn('text-2xs ml-0.5 tabular-nums', tab === t.key ? 'text-navy-500' : 'text-stone-400')}>
                      {count}
                    </span>
                  )}
                </button>
              )
            })}
          </div>

          <div className="relative flex-1 max-w-lg">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-stone-400" />
            <input
              type="text"
              value={search}
              onChange={(e) => handleSearch(e.target.value)}
              placeholder="Search hotel, brand, city..."
              className="w-full h-9 pl-9 pr-9 text-sm bg-white border border-stone-200 rounded-lg outline-none focus:border-navy-400 focus:ring-2 focus:ring-navy-100 transition placeholder:text-stone-400"
            />
            {search && (
              <button onClick={() => handleSearch('')} className="absolute right-2.5 top-1/2 -translate-y-1/2 p-0.5 text-stone-400 hover:text-stone-600 rounded transition">
                <X className="w-3.5 h-3.5" />
              </button>
            )}
          </div>

          {/* Export Button */}
          <button
            onClick={async () => {
              setExporting(true)
              try {
                const params = new URLSearchParams()
                if (filters.timeline) params.set('timeline', filters.timeline)
                if (filters.tier) params.set('tier', filters.tier)
                const res = await api.get(`/api/leads/export?${params}`, { responseType: 'blob' })
                const url = URL.createObjectURL(new Blob([res.data]))
                const a = document.createElement('a')
                a.href = url
                a.download = `leads_export_${new Date().toISOString().split('T')[0]}.xlsx`
                a.click()
                URL.revokeObjectURL(url)
              } catch(e) { console.error('Export failed', e) }
              finally { setExporting(false) }
            }}
            disabled={exporting}
            className="flex items-center gap-1.5 px-3 h-9 text-xs font-semibold text-stone-600 bg-white border border-stone-200 rounded-lg hover:bg-emerald-50 hover:text-emerald-700 hover:border-emerald-300 transition disabled:opacity-50 flex-shrink-0"
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
            onSelect={setSelectedLeadId}
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
