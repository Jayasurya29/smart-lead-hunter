import { useState, useEffect } from 'react'
import { useQuery, useMutation, useQueryClient, keepPreviousData } from '@tanstack/react-query'
import api from '@/api/client'
import { cn } from '@/lib/utils'
import {
  Radar, Clock, AlertTriangle, CheckCircle2, XCircle, Search, X,
  ChevronLeft, ChevronRight, ExternalLink, ToggleLeft, ToggleRight,
  RefreshCw, Activity, Globe, Zap, Eye, Timer, BarChart3, Loader2,
  Mail, Sparkles, RotateCw, Newspaper, Radio, Circle, Users, Target,
} from 'lucide-react'
import { formatDistanceToNow } from 'date-fns'

/* ═══════════════════════════════════════════════════
   TYPES
   ═══════════════════════════════════════════════════ */

interface Source {
  id: number
  name: string
  base_url: string
  source_type: string
  priority: number
  is_active: boolean
  last_scraped_at: string | null
  last_success_at: string | null
  leads_found: number
  success_rate: number
  consecutive_failures: number
  health_status: string
  scrape_frequency: string
  use_playwright: boolean
  gold_urls: Record<string, unknown>
  total_scrapes: number
  notes: string | null
}

interface SourceStats {
  total: number
  active: number
  healthy: number
  blocked: number
  total_leads: number
  no_patterns: number
}

interface SourceListResponse {
  sources: Source[]
  total: number
  page: number
  per_page: number
  pages: number
}

interface ScrapeLog {
  id: number
  source_id: number
  started_at: string
  completed_at: string | null
  urls_scraped: number
  pages_crawled: number
  leads_found: number
  leads_new: number
  leads_duplicate: number
  leads_skipped: number
  status: string
  error_message: string | null
}

/* ═══════════════════════════════════════════════════
   SCHEDULE CONFIG (mirrors celery_app.py)
   ═══════════════════════════════════════════════════ */

const SCHEDULE = [
  { name: 'Recompute Timelines',      task: 'recompute_timeline_labels',  time: '9:30 AM',   icon: RotateCw, color: 'text-stone-500',   desc: 'Drain expired → existing, resurrect ghosts' },
  { name: 'Health Check',             task: 'daily_health_check',         time: '9:35 AM',   icon: Activity, color: 'text-emerald-500', desc: 'Cleanup, deactivate dead sources, rescore' },
  { name: 'Pre-opening Digest',       task: 'pre_opening_digest',         time: '9:40 AM',   icon: Mail,     color: 'text-blue-500',    desc: 'Email digest: leads entering 6-12mo window' },
  { name: 'Auto Smart Fill',          task: 'auto_smart_fill',            time: '9:50 AM',   icon: Sparkles, color: 'text-fuchsia-500', desc: 'Backfill opening/tier/rooms on top 10' },
  { name: 'Auto Full Refresh',        task: 'auto_full_refresh',          time: '10:15 AM',  icon: RefreshCw,color: 'text-cyan-500',    desc: 'Re-check 5 stalest leads (>14d old)' },
  { name: 'Smart Scrape #1',          task: 'smart_scrape',               time: '10:30 AM',  icon: Radar,    color: 'text-violet-500',  desc: 'Mon/Tue/Wed/Fri only (Thu = Discovery)' },
  { name: 'Weekly Discovery',         task: 'weekly_discovery',           time: 'Thu 11:00', icon: Globe,    color: 'text-sky-500',     desc: 'Thursday only — replaces Smart Scrape #1' },
  { name: 'Auto Enrich #1',           task: 'auto_enrich',                time: '12:00 PM',  icon: Zap,      color: 'text-amber-500',   desc: 'Enrich top 5 HOT/URGENT leads' },
  { name: 'Smart Scrape #2',          task: 'smart_scrape',               time: '1:00 PM',   icon: Radar,    color: 'text-violet-500',  desc: 'Mid-day scrape (picks up Discovery on Thu)' },
  { name: 'Auto Enrich #2',           task: 'auto_enrich',                time: '2:30 PM',   icon: Zap,      color: 'text-amber-500',   desc: 'Second contact-enrichment pass' },
  { name: 'Smart Scrape #3',          task: 'smart_scrape',               time: '3:00 PM',   icon: Radar,    color: 'text-violet-500',  desc: 'Final scrape of the day' },
  { name: 'Auto Enrich #3',           task: 'auto_enrich',                time: '4:00 PM',   icon: Zap,      color: 'text-amber-500',   desc: 'End-of-day enrich — no leads sit overnight' },
]

/* ═══════════════════════════════════════════════════
   HEALTH BADGE
   ═══════════════════════════════════════════════════ */

function HealthBadge({ status }: { status: string }) {
  const cfg: Record<string, { bg: string; text: string; icon: React.ElementType }> = {
    healthy:  { bg: 'bg-emerald-50', text: 'text-emerald-600', icon: CheckCircle2 },
    degraded: { bg: 'bg-amber-50',   text: 'text-amber-600',   icon: AlertTriangle },
    failing:  { bg: 'bg-red-50',     text: 'text-red-500',     icon: XCircle },
    dead:     { bg: 'bg-stone-100',  text: 'text-stone-400',   icon: XCircle },
    new:      { bg: 'bg-sky-50',     text: 'text-sky-500',     icon: Eye },
  }
  const c = cfg[status] || cfg.new
  const Icon = c.icon
  return (
    <span className={cn('inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-2xs font-bold', c.bg, c.text)}>
      <Icon className="w-3 h-3" /> {status}
    </span>
  )
}

/* ═══════════════════════════════════════════════════
   MAIN PAGE
   ═══════════════════════════════════════════════════ */

export default function SourcesPage() {
  const [viewMode, setViewMode] = useState<'sources' | 'queries' | 'news'>('sources')
  const [search, setSearch] = useState('')
  const [debouncedSearch, setDebouncedSearch] = useState('')
  const [filter, setFilter] = useState<string>('')
  const [page, setPage] = useState(1)
  const [perPage, setPerPage] = useState(25)
  const [selectedId, setSelectedId] = useState<number | null>(null)
  const qc = useQueryClient()

  // Debounce the search box so we don't hit the API on every keystroke.
  useEffect(() => {
    const t = setTimeout(() => setDebouncedSearch(search), 300)
    return () => clearTimeout(t)
  }, [search])

  // Any filter / search / page-size change resets back to page 1.
  useEffect(() => { setPage(1) }, [debouncedSearch, filter, perPage])

  // Stat cards — aggregate over the WHOLE table, independent of page/filter.
  const { data: stats } = useQuery<SourceStats>({
    queryKey: ['sources-stats'],
    queryFn: async () => (await api.get('/sources/stats')).data,
    refetchInterval: 60_000,
    staleTime: 30_000,
  })

  // Paginated list — only the current page is fetched from the server.
  const { data: listData, isLoading, isFetching } = useQuery<SourceListResponse>({
    queryKey: ['sources', page, perPage, debouncedSearch, filter],
    queryFn: async () => (await api.get('/sources', {
      params: {
        page,
        per_page: perPage,
        ...(debouncedSearch ? { search: debouncedSearch } : {}),
        ...(filter ? { status: filter } : {}),
      },
    })).data,
    placeholderData: keepPreviousData,
    refetchInterval: 60_000,
    staleTime: 30_000,
  })

  const sources = listData?.sources ?? []
  const totalPages = listData?.pages ?? 1
  const totalRows = listData?.total ?? 0

  const { data: logs = [] } = useQuery<ScrapeLog[]>({
    queryKey: ['scrape-logs'],
    queryFn: async () => (await api.get('/scrape/logs')).data,
    refetchInterval: 60_000,
    staleTime: 30_000,
  })

  const toggleMut = useMutation({
    mutationFn: async (id: number) => (await api.post(`/sources/${id}/toggle`)).data,
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['sources'] })
      qc.invalidateQueries({ queryKey: ['sources-stats'] })
    },
  })

  const resetMut = useMutation({
    mutationFn: async (id: number) => (await api.post(`/sources/${id}/reset-health`)).data,
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['sources'] })
      qc.invalidateQueries({ queryKey: ['sources-stats'] })
    },
  })

  // Stats from the aggregate endpoint (0 while loading).
  const total = stats?.total ?? 0
  const healthy = stats?.healthy ?? 0
  const blocked = stats?.blocked ?? 0
  const active = stats?.active ?? 0
  const totalLeads = stats?.total_leads ?? 0
  const missingPatterns = stats?.no_patterns ?? 0

  // Server already applied search + filter + ordering; render rows as-is.
  const filtered = sources

  const selected = sources.find(s => s.id === selectedId)
  const selectedLogs = logs.filter(l => l.source_id === selectedId).slice(0, 10)

  return (
    <div className="h-full flex flex-col">
      {/* Stats + Schedule Row */}
      <div className="px-4 pt-3 pb-2 flex-shrink-0">
        <div className="grid grid-cols-12 gap-3">
          {/* Stats */}
          <div className="col-span-7 grid grid-cols-6 gap-2">
            <StatCard label="Total Sources" value={total} icon={Globe} bg="bg-navy-50" text="text-navy-600" />
            <StatCard label="Active" value={active} icon={CheckCircle2} bg="bg-emerald-50" text="text-emerald-600" />
            <StatCard label="Healthy" value={healthy} icon={Activity} bg="bg-sky-50" text="text-sky-600" />
            <StatCard label="Blocked" value={blocked} icon={XCircle} bg="bg-red-50" text="text-red-500" />
            <StatCard label="Total Leads" value={totalLeads} icon={BarChart3} bg="bg-violet-50" text="text-violet-600" />
            <StatCard label="No Patterns" value={missingPatterns} icon={AlertTriangle} bg="bg-amber-50" text="text-amber-600" />
          </div>

          {/* Schedule + Run Controls */}
          <div className="col-span-5 bg-white rounded-xl border border-slate-200/80 shadow-sm p-3 overflow-hidden">
            <div className="flex items-center justify-between mb-2">
              <h3 className="text-xs font-bold text-stone-400 uppercase tracking-wider">Schedule (Mon-Fri ET)</h3>
              <TaskRunButtons />
            </div>
            <div className="grid grid-cols-2 gap-x-4 gap-y-1.5">
              {SCHEDULE.map((s, i) => {
                const Icon = s.icon
                return (
                  <div key={i} className="flex items-center gap-2">
                    <Icon className={cn('w-3.5 h-3.5 flex-shrink-0', s.color)} />
                    <span className="text-xs font-semibold text-navy-800 truncate">{s.name}</span>
                    <span className="text-2xs text-stone-400 ml-auto tabular-nums">{s.time}</span>
                  </div>
                )
              })}
            </div>
          </div>
        </div>
      </div>

      {/* Sources ↔ Queries tab toggle */}
      <div className="px-4 pb-2 flex-shrink-0">
        <div className="inline-flex rounded-lg border border-stone-200 bg-white p-0.5 shadow-sm">
          <button
            onClick={() => setViewMode('sources')}
            className={cn(
              'px-4 h-8 text-xs font-semibold rounded-md transition flex items-center gap-1.5',
              viewMode === 'sources'
                ? 'bg-navy-900 text-white'
                : 'text-stone-500 hover:text-stone-700',
            )}
          >
            <Globe className="w-3.5 h-3.5" />
            Sources
          </button>
          <button
            onClick={() => setViewMode('queries')}
            className={cn(
              'px-4 h-8 text-xs font-semibold rounded-md transition flex items-center gap-1.5',
              viewMode === 'queries'
                ? 'bg-navy-900 text-white'
                : 'text-stone-500 hover:text-stone-700',
            )}
          >
            <Search className="w-3.5 h-3.5" />
            Discovery Queries
          </button>
          <button
            onClick={() => setViewMode('news')}
            className={cn(
              'px-4 h-8 text-xs font-semibold rounded-md transition flex items-center gap-1.5',
              viewMode === 'news'
                ? 'bg-navy-900 text-white'
                : 'text-stone-500 hover:text-stone-700',
            )}
          >
            <Newspaper className="w-3.5 h-3.5" />
            News
          </button>
        </div>
      </div>

      {viewMode === 'queries' ? (
        <QueriesPanel />
      ) : viewMode === 'news' ? (
        <NewsPanel />
      ) : (
        <>
      {/* Filters */}
      <div className="px-4 pb-2 flex-shrink-0">
        <div className="flex items-center gap-3">
          <div className="relative flex-1 max-w-sm">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-stone-400" />
            <input
              type="text"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              placeholder="Search source name or URL..."
              className="w-full h-9 pl-9 pr-9 text-sm bg-white border border-stone-200 rounded-lg outline-none focus:border-navy-400 focus:ring-2 focus:ring-navy-100 transition placeholder:text-stone-400"
            />
            {search && (
              <button onClick={() => setSearch('')} className="absolute right-2.5 top-1/2 -translate-y-1/2 p-0.5 text-stone-400 hover:text-stone-600 rounded transition">
                <X className="w-3.5 h-3.5" />
              </button>
            )}
          </div>

          {['', 'healthy', 'problems', 'active', 'inactive'].map(f => (
            <button
              key={f}
              onClick={() => setFilter(f)}
              className={cn(
                'h-9 px-3 text-xs font-semibold rounded-lg border transition',
                filter === f
                  ? 'bg-navy-900 text-white border-navy-900'
                  : 'bg-white text-stone-500 border-stone-200 hover:bg-stone-50',
              )}
            >
              {f === '' ? 'All' : f === 'problems' ? 'Problems' : f.charAt(0).toUpperCase() + f.slice(1)}
            </button>
          ))}

          <span className="text-xs text-stone-400 ml-auto">{totalRows.toLocaleString()} sources</span>
        </div>
      </div>

      {/* Table + Detail */}
      <div className="flex-1 flex overflow-hidden px-4 pb-3 gap-3">
        {/* Source Table */}
        <div className={cn(
          'bg-white rounded-xl border border-slate-200/80 shadow-sm overflow-hidden flex flex-col transition-all duration-300',
          selectedId ? 'flex-[3]' : 'flex-1',
        )}>
          {isLoading ? (
            <div className="flex items-center justify-center py-20">
              <Loader2 className="w-8 h-8 animate-spin text-navy-400" />
            </div>
          ) : (
            <div className="flex-1 overflow-y-auto">
              <table className="w-full">
                <thead className="sticky top-0 z-10">
                  <tr className="bg-slate-50/90 backdrop-blur-sm border-b border-slate-100">
                    <th className="px-3 py-2.5 text-left text-[11px] font-bold text-slate-400 uppercase tracking-wider">Source</th>
                    <th className="px-3 py-2.5 text-left text-[11px] font-bold text-slate-400 uppercase tracking-wider w-20">Health</th>
                    <th className="px-3 py-2.5 text-left text-[11px] font-bold text-slate-400 uppercase tracking-wider w-16">Leads</th>
                    <th className="px-3 py-2.5 text-left text-[11px] font-bold text-slate-400 uppercase tracking-wider w-20">Priority</th>
                    <th className="px-3 py-2.5 text-left text-[11px] font-bold text-slate-400 uppercase tracking-wider w-28">Last Scraped</th>
                    <th className="px-3 py-2.5 text-left text-[11px] font-bold text-slate-400 uppercase tracking-wider w-16">Active</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-100/80">
                  {filtered.map((source) => (
                    <tr
                      key={source.id}
                      onClick={() => setSelectedId(source.id)}
                      className={cn(
                        'lead-row cursor-pointer',
                        selectedId === source.id && 'active',
                        !source.is_active && 'opacity-50',
                      )}
                    >
                      <td className="px-3 py-2.5 max-w-[260px]">
                        <div className="truncate text-[14px] font-bold text-navy-950 leading-snug">{source.name}</div>
                        <div className="truncate text-xs text-stone-400 leading-snug">{source.base_url}</div>
                      </td>
                      <td className="px-3 py-2.5">
                        <HealthBadge status={source.health_status} />
                      </td>
                      <td className="px-3 py-2.5">
                        <span className="text-sm font-bold text-navy-800 tabular-nums">{source.leads_found || 0}</span>
                      </td>
                      <td className="px-3 py-2.5">
                        <PriorityBar value={source.priority} />
                      </td>
                      <td className="px-3 py-2.5">
                        {source.last_scraped_at ? (
                          <span className="text-xs text-stone-500">
                            {formatDistanceToNow(new Date(source.last_scraped_at), { addSuffix: true })}
                          </span>
                        ) : (
                          <span className="text-xs text-stone-300">Never</span>
                        )}
                      </td>
                      <td className="px-3 py-2.5">
                        <button
                          onClick={(e) => { e.stopPropagation(); toggleMut.mutate(source.id) }}
                          className="transition hover:scale-110"
                        >
                          {source.is_active
                            ? <ToggleRight className="w-5 h-5 text-emerald-500" />
                            : <ToggleLeft className="w-5 h-5 text-stone-300" />
                          }
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}

          {/* Pager */}
          <div className="flex items-center justify-between gap-3 px-3 py-2 border-t border-slate-100 bg-slate-50/60 flex-shrink-0">
            <div className="flex items-center gap-2 text-xs text-stone-500">
              <span className="tabular-nums">{totalRows.toLocaleString()} sources</span>
              <span className="text-stone-300">·</span>
              <label className="flex items-center gap-1">
                <span>Per page</span>
                <select
                  value={perPage}
                  onChange={(e) => setPerPage(Number(e.target.value))}
                  className="h-7 px-1.5 rounded-md border border-stone-200 bg-white text-xs outline-none focus:border-navy-400"
                >
                  {[25, 50, 100].map(n => <option key={n} value={n}>{n}</option>)}
                </select>
              </label>
              {isFetching && <Loader2 className="w-3.5 h-3.5 animate-spin text-stone-400" />}
            </div>
            <div className="flex items-center gap-2">
              <button
                onClick={() => setPage(p => Math.max(1, p - 1))}
                disabled={page <= 1}
                className="h-7 px-2 inline-flex items-center gap-1 text-xs font-semibold rounded-md border border-stone-200 bg-white text-stone-600 disabled:opacity-40 disabled:cursor-not-allowed hover:bg-stone-50 transition"
              >
                <ChevronLeft className="w-3.5 h-3.5" /> Prev
              </button>
              <span className="text-xs text-stone-500 tabular-nums">Page {page} of {totalPages}</span>
              <button
                onClick={() => setPage(p => Math.min(totalPages, p + 1))}
                disabled={page >= totalPages}
                className="h-7 px-2 inline-flex items-center gap-1 text-xs font-semibold rounded-md border border-stone-200 bg-white text-stone-600 disabled:opacity-40 disabled:cursor-not-allowed hover:bg-stone-50 transition"
              >
                Next <ChevronRight className="w-3.5 h-3.5" />
              </button>
            </div>
          </div>
        </div>

        {/* Detail Panel */}
        {selected && (
          <div className="flex-[2] bg-white rounded-xl border border-slate-200/80 shadow-sm overflow-hidden flex flex-col animate-slideIn">
            <SourceDetail
              source={selected}
              logs={selectedLogs}
              onClose={() => setSelectedId(null)}
              onToggle={() => toggleMut.mutate(selected.id)}
              onReset={() => resetMut.mutate(selected.id)}
            />
          </div>
        )}
      </div>
        </>
      )}
    </div>
  )
}

/* ═══════════════════════════════════════════════════
   QUERIES PANEL — Discovery query intelligence tab
   ═══════════════════════════════════════════════════ */

interface QueryStat {
  query_text: string
  status: 'gold' | 'maybe' | 'junk' | 'paused'
  total_runs: number
  total_new_sources: number
  total_new_leads: number
  total_duplicates: number
  consecutive_zero_runs: number
  first_run_at: string | null
  last_run_at: string | null
  last_success_at: string | null
  paused_until: string | null
  last_run_detail: Record<string, unknown> | null
}

interface QuerySummary {
  total_queries: number
  gold: { count: number; sources: number; leads: number }
  maybe: { count: number; sources: number; leads: number }
  junk: { count: number; sources: number; leads: number }
  paused: { count: number; sources: number; leads: number }
  total_new_sources_ever: number
  total_new_leads_ever: number
}

function QueriesPanel() {
  const [statusFilter, setStatusFilter] = useState<string>('')
  const [search, setSearch] = useState('')

  const { data: summary } = useQuery<QuerySummary>({
    queryKey: ['discovery-query-summary'],
    queryFn: async () => (await api.get('/discovery/queries/stats')).data,
    refetchInterval: 60_000,
  })

  const { data: queries = [], isLoading } = useQuery<QueryStat[]>({
    queryKey: ['discovery-queries', statusFilter],
    queryFn: async () => {
      const params = statusFilter ? `?status_filter=${statusFilter}` : ''
      return (await api.get(`/discovery/queries${params}`)).data
    },
    refetchInterval: 60_000,
  })

  const filtered = queries.filter(q =>
    !search || q.query_text.toLowerCase().includes(search.toLowerCase()),
  )

  const statusBadge = (status: string) => {
    const map: Record<string, { bg: string; text: string; label: string }> = {
      gold: { bg: 'bg-amber-100', text: 'text-amber-700', label: '🥇 Gold' },
      maybe: { bg: 'bg-sky-100', text: 'text-sky-700', label: 'Learning' },
      junk: { bg: 'bg-red-100', text: 'text-red-600', label: '🗑️ Junk' },
      paused: { bg: 'bg-violet-100', text: 'text-violet-700', label: '⏸ Paused' },
    }
    const m = map[status] || { bg: 'bg-stone-100', text: 'text-stone-600', label: status }
    return (
      <span className={cn('px-2 py-0.5 rounded-full text-2xs font-semibold', m.bg, m.text)}>
        {m.label}
      </span>
    )
  }

  return (
    <>
      {/* Query Summary Cards */}
      <div className="px-4 pb-2 flex-shrink-0">
        <div className="grid grid-cols-6 gap-2">
          <StatCard
            label="Total Queries"
            value={summary?.total_queries || 0}
            icon={Search}
            bg="bg-navy-50"
            text="text-navy-600"
          />
          <StatCard
            label="Gold"
            value={summary?.gold.count || 0}
            icon={CheckCircle2}
            bg="bg-amber-50"
            text="text-amber-600"
          />
          <StatCard
            label="Learning"
            value={summary?.maybe.count || 0}
            icon={Activity}
            bg="bg-sky-50"
            text="text-sky-600"
          />
          <StatCard
            label="Paused"
            value={summary?.paused.count || 0}
            icon={Timer}
            bg="bg-violet-50"
            text="text-violet-600"
          />
          <StatCard
            label="Junk"
            value={summary?.junk.count || 0}
            icon={XCircle}
            bg="bg-red-50"
            text="text-red-500"
          />
          <StatCard
            label="Leads Ever"
            value={summary?.total_new_leads_ever || 0}
            icon={BarChart3}
            bg="bg-emerald-50"
            text="text-emerald-600"
          />
        </div>
      </div>

      {/* Query Filters */}
      <div className="px-4 pb-2 flex-shrink-0">
        <div className="flex items-center gap-3">
          <div className="relative flex-1 max-w-sm">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-stone-400" />
            <input
              type="text"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              placeholder="Search query text..."
              className="w-full h-9 pl-9 pr-9 text-sm bg-white border border-stone-200 rounded-lg outline-none focus:border-navy-400 focus:ring-2 focus:ring-navy-100 transition placeholder:text-stone-400"
            />
            {search && (
              <button
                onClick={() => setSearch('')}
                className="absolute right-2.5 top-1/2 -translate-y-1/2 p-0.5 text-stone-400 hover:text-stone-600 rounded transition"
              >
                <X className="w-3.5 h-3.5" />
              </button>
            )}
          </div>

          {['', 'gold', 'maybe', 'paused', 'junk'].map(f => (
            <button
              key={f}
              onClick={() => setStatusFilter(f)}
              className={cn(
                'h-9 px-3 text-xs font-semibold rounded-lg border transition',
                statusFilter === f
                  ? 'bg-navy-900 text-white border-navy-900'
                  : 'bg-white text-stone-500 border-stone-200 hover:bg-stone-50',
              )}
            >
              {f === '' ? 'All' : f.charAt(0).toUpperCase() + f.slice(1)}
            </button>
          ))}

          <span className="text-xs text-stone-400 ml-auto">{filtered.length} queries</span>
        </div>
      </div>

      {/* Query Table */}
      <div className="flex-1 overflow-hidden px-4 pb-3">
        <div className="bg-white rounded-xl border border-slate-200/80 shadow-sm overflow-hidden flex flex-col h-full">
          {isLoading ? (
            <div className="flex items-center justify-center py-20">
              <Loader2 className="w-8 h-8 animate-spin text-navy-400" />
            </div>
          ) : filtered.length === 0 ? (
            <div className="flex items-center justify-center py-20 text-stone-400 text-sm">
              No queries tracked yet. Run discovery first:{' '}
              <code className="ml-2 px-2 py-0.5 bg-stone-100 rounded text-xs">
                python -m scripts.discover_sources
              </code>
            </div>
          ) : (
            <div className="flex-1 overflow-y-auto">
              <table className="w-full">
                <thead className="sticky top-0 z-10">
                  <tr className="bg-slate-50/90 backdrop-blur-sm border-b border-slate-100">
                    <th className="px-3 py-2.5 text-left text-[11px] font-bold text-slate-400 uppercase tracking-wider">
                      Query
                    </th>
                    <th className="px-3 py-2.5 text-left text-[11px] font-bold text-slate-400 uppercase tracking-wider w-24">
                      Status
                    </th>
                    <th className="px-3 py-2.5 text-left text-[11px] font-bold text-slate-400 uppercase tracking-wider w-20">
                      Runs
                    </th>
                    <th className="px-3 py-2.5 text-left text-[11px] font-bold text-slate-400 uppercase tracking-wider w-24">
                      New Sources
                    </th>
                    <th className="px-3 py-2.5 text-left text-[11px] font-bold text-slate-400 uppercase tracking-wider w-20">
                      Leads
                    </th>
                    <th className="px-3 py-2.5 text-left text-[11px] font-bold text-slate-400 uppercase tracking-wider w-28">
                      Last Run
                    </th>
                    <th className="px-3 py-2.5 text-left text-[11px] font-bold text-slate-400 uppercase tracking-wider w-28">
                      Last Success
                    </th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-100/80">
                  {filtered.map((q) => (
                    <tr key={q.query_text} className="hover:bg-slate-50/60 transition">
                      <td className="px-3 py-2.5">
                        <div className="text-[13px] font-semibold text-navy-900 leading-snug truncate max-w-[500px]">
                          {q.query_text}
                        </div>
                        {q.consecutive_zero_runs > 0 && q.status !== 'junk' && (
                          <div className="text-2xs text-amber-600 leading-snug mt-0.5">
                            {q.consecutive_zero_runs} consecutive zero-yield runs
                          </div>
                        )}
                      </td>
                      <td className="px-3 py-2.5">{statusBadge(q.status)}</td>
                      <td className="px-3 py-2.5">
                        <span className="text-sm font-semibold text-navy-800 tabular-nums">
                          {q.total_runs}
                        </span>
                      </td>
                      <td className="px-3 py-2.5">
                        <span
                          className={cn(
                            'text-sm font-bold tabular-nums',
                            q.total_new_sources > 0 ? 'text-emerald-600' : 'text-stone-300',
                          )}
                        >
                          {q.total_new_sources}
                        </span>
                      </td>
                      <td className="px-3 py-2.5">
                        <span
                          className={cn(
                            'text-sm font-bold tabular-nums',
                            q.total_new_leads > 0 ? 'text-emerald-600' : 'text-stone-300',
                          )}
                        >
                          {q.total_new_leads}
                        </span>
                      </td>
                      <td className="px-3 py-2.5 text-xs text-stone-500">
                        {q.last_run_at
                          ? formatDistanceToNow(new Date(q.last_run_at), { addSuffix: true })
                          : 'never'}
                      </td>
                      <td className="px-3 py-2.5 text-xs text-stone-500">
                        {q.last_success_at
                          ? formatDistanceToNow(new Date(q.last_success_at), { addSuffix: true })
                          : 'never'}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </div>
    </>
  )
}

/* ═══════════════════════════════════════════════════
   NEWS PANEL — hospitality news query / source analytics
   ═══════════════════════════════════════════════════ */

interface NewsQueryStat {
  query: string
  stories: number
  fresh_30d: number
  active_days: number
  last_seen: string | null
  rel_hits: number
  pipeline_hits: number
}

interface NewsSourceStat {
  source: string
  stories: number
  fresh_30d: number
  active_days: number
  first_seen: string | null
  last_seen: string | null
  rel_hits: number
  pipeline_hits: number
}

interface NewsSourceStats {
  queries: NewsQueryStat[]
  sources: NewsSourceStat[]
}

// active_days > 1 means the query/source keeps producing on different days
// (a continuous feeder); active_days === 1 is a one-time / static hit.
function PatternBadge({ activeDays }: { activeDays: number }) {
  const cfg =
    activeDays >= 3
      ? { label: 'Continuous', cls: 'bg-emerald-50 text-emerald-600 border-emerald-200', dot: 'bg-emerald-500' }
      : activeDays === 2
      ? { label: 'Recurring', cls: 'bg-amber-50 text-amber-600 border-amber-200', dot: 'bg-amber-500' }
      : { label: 'One-time', cls: 'bg-slate-100 text-slate-500 border-slate-200', dot: 'bg-slate-400' }
  return (
    <span className={cn('inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full text-2xs font-semibold border', cfg.cls)}>
      <span className={cn('w-1.5 h-1.5 rounded-full', cfg.dot)} /> {cfg.label}
    </span>
  )
}

// Cosmetic vertical tag inferred from the query text (edu / health / industry).
function queryVertical(q: string): { label: string; cls: string } | null {
  const s = q.toLowerCase()
  if (/(universit|college|campus|residence hall|student|dining hall|academic)/.test(s))
    return { label: 'edu', cls: 'bg-blue-50 text-blue-600' }
  if (/(hospital|medical center|health system|healthcare|clinic)/.test(s))
    return { label: 'health', cls: 'bg-red-50 text-red-600' }
  if (/(industry|trend|innovation|sustainab|regulation|\blaw\b|labor|technology|guest experience)/.test(s))
    return { label: 'industry', cls: 'bg-violet-50 text-violet-600' }
  return null
}

function HitCell({ n }: { n: number }) {
  return (
    <span className={cn('text-sm tabular-nums', n > 0 ? 'font-bold text-navy-800' : 'font-medium text-stone-300')}>
      {n}
    </span>
  )
}

function VolBar({ value, max }: { value: number; max: number }) {
  const pct = max > 0 && value > 0 ? Math.max(3, Math.round((value / max) * 100)) : 0
  return (
    <div className="h-[3px] rounded-sm bg-stone-100 mt-1 overflow-hidden">
      <div className="h-full bg-emerald-400 rounded-sm" style={{ width: `${pct}%` }} />
    </div>
  )
}

function NewsPanel() {
  const { data, isLoading } = useQuery<NewsSourceStats>({
    queryKey: ['news-source-stats'],
    queryFn: async () => (await api.get('/api/news/source-stats')).data,
    refetchInterval: 60_000,
    staleTime: 30_000,
  })

  const queries = data?.queries ?? []
  const sources = data?.sources ?? []

  // Totals are derived from the per-source rows (every story has a source).
  const totalStories = sources.reduce((a, s) => a + s.stories, 0)
  const continuousSrc = sources.filter(s => s.active_days >= 3).length
  const onetimeSrc = sources.filter(s => s.active_days === 1).length
  const contactHits = sources.reduce((a, s) => a + s.rel_hits, 0)
  const pipelineHits = sources.reduce((a, s) => a + s.pipeline_hits, 0)
  const maxQ = queries.reduce((m, q) => Math.max(m, q.stories), 0)
  const maxS = sources.reduce((m, s) => Math.max(m, s.stories), 0)

  return (
    <>
      {/* Summary strip — same compact StatCard used across the app */}
      <div className="px-4 pb-2 flex-shrink-0">
        <div className="grid grid-cols-6 gap-2">
          <StatCard label="Stories" value={totalStories} icon={Newspaper} bg="bg-sky-50" text="text-sky-600" />
          <StatCard label="Queries" value={queries.length} icon={Search} bg="bg-emerald-50" text="text-emerald-600" />
          <StatCard label="Continuous" value={continuousSrc} icon={Radio} bg="bg-violet-50" text="text-violet-600" />
          <StatCard label="One-time" value={onetimeSrc} icon={Circle} bg="bg-stone-100" text="text-stone-500" />
          <StatCard label="Contact Hits" value={contactHits} icon={Users} bg="bg-amber-50" text="text-amber-600" />
          <StatCard label="Pipeline" value={pipelineHits} icon={Target} bg="bg-red-50" text="text-red-500" />
        </div>
      </div>

      {isLoading ? (
        <div className="flex items-center justify-center py-20">
          <Loader2 className="w-8 h-8 animate-spin text-navy-400" />
        </div>
      ) : queries.length === 0 && sources.length === 0 ? (
        <div className="flex items-center justify-center py-20 text-stone-400 text-sm">
          No news tracked yet. Run a scan first:{' '}
          <code className="ml-2 px-2 py-0.5 bg-stone-100 rounded text-xs">python news_scan.py --apply</code>
        </div>
      ) : (
        <div className="flex-1 overflow-y-auto px-4 pb-3 space-y-3">
          {/* QUERY PRODUCTIVITY */}
          <div className="bg-white rounded-xl border border-slate-200/80 shadow-sm overflow-hidden">
            <div className="px-4 py-3 border-b border-slate-100">
              <h3 className="text-sm font-bold text-navy-900">Query productivity</h3>
              <p className="text-xs text-stone-400 mt-0.5">Which scan queries surface stories — and which are dead weight.</p>
            </div>
            <table className="w-full">
              <thead>
                <tr className="bg-slate-50/90 border-b border-slate-100">
                  <th className="px-4 py-2 text-left text-[11px] font-bold text-slate-400 uppercase tracking-wider">Query</th>
                  <th className="px-4 py-2 text-right text-[11px] font-bold text-slate-400 uppercase tracking-wider w-28">Stories</th>
                  <th className="px-4 py-2 text-right text-[11px] font-bold text-slate-400 uppercase tracking-wider w-20">Fresh 30d</th>
                  <th className="px-4 py-2 text-center text-[11px] font-bold text-slate-400 uppercase tracking-wider w-28">Pattern</th>
                  <th className="px-4 py-2 text-right text-[11px] font-bold text-slate-400 uppercase tracking-wider w-24">Last</th>
                  <th className="px-4 py-2 text-center text-[11px] font-bold text-slate-400 uppercase tracking-wider w-14">🤝</th>
                  <th className="px-4 py-2 text-center text-[11px] font-bold text-slate-400 uppercase tracking-wider w-14">🎯</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-100/80">
                {queries.map((q) => {
                  const v = queryVertical(q.query)
                  return (
                    <tr key={q.query} className="hover:bg-slate-50/60 transition">
                      <td className="px-4 py-2.5 max-w-[460px]">
                        <div className="flex items-center gap-2">
                          <span className="text-[13px] font-semibold text-navy-900 truncate">{q.query}</span>
                          {v && (
                            <span className={cn('text-2xs font-semibold px-1.5 py-0.5 rounded uppercase tracking-wide flex-shrink-0', v.cls)}>
                              {v.label}
                            </span>
                          )}
                        </div>
                      </td>
                      <td className="px-4 py-2.5 text-right">
                        <span className="text-sm font-bold text-navy-800 tabular-nums">{q.stories}</span>
                        <VolBar value={q.stories} max={maxQ} />
                      </td>
                      <td className="px-4 py-2.5 text-right text-sm font-semibold text-stone-500 tabular-nums">{q.fresh_30d}</td>
                      <td className="px-4 py-2.5 text-center"><PatternBadge activeDays={q.active_days} /></td>
                      <td className="px-4 py-2.5 text-right text-xs text-stone-500">
                        {q.last_seen ? formatDistanceToNow(new Date(q.last_seen), { addSuffix: true }) : '—'}
                      </td>
                      <td className="px-4 py-2.5 text-center"><HitCell n={q.rel_hits} /></td>
                      <td className="px-4 py-2.5 text-center"><HitCell n={q.pipeline_hits} /></td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
            <div className="flex items-center gap-4 px-4 py-2.5 border-t border-slate-100 bg-slate-50/60 text-2xs text-stone-500">
              <span className="inline-flex items-center gap-1"><span className="w-1.5 h-1.5 rounded-full bg-emerald-500" />Continuous · 3+ days</span>
              <span className="inline-flex items-center gap-1"><span className="w-1.5 h-1.5 rounded-full bg-amber-500" />Recurring · 2 days</span>
              <span className="inline-flex items-center gap-1"><span className="w-1.5 h-1.5 rounded-full bg-slate-400" />One-time · single day, candidate to cut</span>
            </div>
          </div>

          {/* SOURCE PRODUCTIVITY */}
          <div className="bg-white rounded-xl border border-slate-200/80 shadow-sm overflow-hidden">
            <div className="px-4 py-3 border-b border-slate-100">
              <h3 className="text-sm font-bold text-navy-900">Source productivity</h3>
              <p className="text-xs text-stone-400 mt-0.5">Which outlets feed the pipeline — continuous producers vs one-off mentions.</p>
            </div>
            <table className="w-full">
              <thead>
                <tr className="bg-slate-50/90 border-b border-slate-100">
                  <th className="px-4 py-2 text-left text-[11px] font-bold text-slate-400 uppercase tracking-wider">Source</th>
                  <th className="px-4 py-2 text-right text-[11px] font-bold text-slate-400 uppercase tracking-wider w-28">Stories</th>
                  <th className="px-4 py-2 text-right text-[11px] font-bold text-slate-400 uppercase tracking-wider w-24">Active days</th>
                  <th className="px-4 py-2 text-center text-[11px] font-bold text-slate-400 uppercase tracking-wider w-28">Pattern</th>
                  <th className="px-4 py-2 text-right text-[11px] font-bold text-slate-400 uppercase tracking-wider w-24">First</th>
                  <th className="px-4 py-2 text-right text-[11px] font-bold text-slate-400 uppercase tracking-wider w-24">Last</th>
                  <th className="px-4 py-2 text-center text-[11px] font-bold text-slate-400 uppercase tracking-wider w-14">🤝</th>
                  <th className="px-4 py-2 text-center text-[11px] font-bold text-slate-400 uppercase tracking-wider w-14">🎯</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-100/80">
                {sources.map((s) => (
                  <tr key={s.source} className="hover:bg-slate-50/60 transition">
                    <td className="px-4 py-2.5 max-w-[320px]">
                      <span className="block text-[13px] font-semibold text-navy-900 truncate">{s.source}</span>
                    </td>
                    <td className="px-4 py-2.5 text-right">
                      <span className="text-sm font-bold text-navy-800 tabular-nums">{s.stories}</span>
                      <VolBar value={s.stories} max={maxS} />
                    </td>
                    <td className="px-4 py-2.5 text-right text-sm font-semibold text-stone-500 tabular-nums">{s.active_days}</td>
                    <td className="px-4 py-2.5 text-center"><PatternBadge activeDays={s.active_days} /></td>
                    <td className="px-4 py-2.5 text-right text-xs text-stone-500">
                      {s.first_seen ? new Date(s.first_seen).toLocaleDateString('en-US', { month: 'short', day: 'numeric' }) : '—'}
                    </td>
                    <td className="px-4 py-2.5 text-right text-xs text-stone-500">
                      {s.last_seen ? formatDistanceToNow(new Date(s.last_seen), { addSuffix: true }) : '—'}
                    </td>
                    <td className="px-4 py-2.5 text-center"><HitCell n={s.rel_hits} /></td>
                    <td className="px-4 py-2.5 text-center"><HitCell n={s.pipeline_hits} /></td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </>
  )
}

/* ═══════════════════════════════════════════════════
   STAT CARD
   ═══════════════════════════════════════════════════ */

function StatCard({ label, value, icon: Icon, bg, text }: {
  label: string; value: number; icon: React.ElementType; bg: string; text: string
}) {
  return (
    <div className="rounded-lg border border-stone-200 bg-white px-3 py-2.5 flex items-center gap-2.5">
      <div className={cn('w-8 h-8 rounded-lg flex items-center justify-center flex-shrink-0', bg)}>
        <Icon className={cn('w-4 h-4', text)} />
      </div>
      <div className="min-w-0">
        <div className="text-lg font-bold text-navy-900 leading-tight tabular-nums">{value}</div>
        <div className="text-2xs text-stone-400 font-semibold uppercase tracking-wider">{label}</div>
      </div>
    </div>
  )
}

/* ═══════════════════════════════════════════════════
   PRIORITY BAR
   ═══════════════════════════════════════════════════ */

function PriorityBar({ value }: { value: number }) {
  const color = value >= 8 ? 'bg-emerald-400' : value >= 5 ? 'bg-amber-400' : 'bg-stone-300'
  return (
    <div className="flex items-center gap-1.5">
      <div className="flex gap-px">
        {Array.from({ length: 10 }).map((_, i) => (
          <div key={i} className={cn('w-1.5 h-3 rounded-sm', i < value ? color : 'bg-stone-100')} />
        ))}
      </div>
      <span className="text-2xs text-stone-400 tabular-nums">{value}</span>
    </div>
  )
}

/* ═══════════════════════════════════════════════════
   SOURCE DETAIL PANEL
   ═══════════════════════════════════════════════════ */

function SourceDetail({ source, logs, onClose, onToggle, onReset }: {
  source: Source; logs: ScrapeLog[]; onClose: () => void
  onToggle: () => void; onReset: () => void
}) {
  const goldCount = source.gold_urls ? Object.keys(source.gold_urls).length : 0

  return (
    <div className="h-full flex flex-col">
      {/* Header */}
      <div className="px-5 pt-5 pb-3 flex-shrink-0 border-b border-slate-100 bg-gradient-to-b from-slate-50/50 to-white">
        <div className="flex items-start justify-between gap-3">
          <div className="min-w-0 flex-1">
            <h2 className="text-lg font-bold text-navy-900 leading-snug">{source.name}</h2>
            <a href={source.base_url} target="_blank" rel="noopener noreferrer"
               className="text-xs text-navy-500 hover:underline flex items-center gap-1 mt-0.5">
              <ExternalLink className="w-3 h-3" /> {source.base_url}
            </a>
          </div>
          <button onClick={onClose} className="p-1.5 text-stone-400 hover:text-stone-600 rounded-lg hover:bg-stone-100 transition">
            <X className="w-4 h-4" />
          </button>
        </div>

        <div className="flex items-center gap-2 mt-2.5">
          <HealthBadge status={source.health_status} />
          <span className={cn('inline-flex px-2 py-0.5 rounded-full text-2xs font-bold',
            source.is_active ? 'bg-emerald-50 text-emerald-600' : 'bg-stone-100 text-stone-400'
          )}>
            {source.is_active ? 'Active' : 'Inactive'}
          </span>
          <span className="inline-flex px-2 py-0.5 rounded-full text-2xs font-bold bg-stone-100 text-stone-500">
            {source.source_type}
          </span>
          {source.use_playwright && (
            <span className="inline-flex px-2 py-0.5 rounded-full text-2xs font-bold bg-violet-50 text-violet-500">
              Playwright
            </span>
          )}
        </div>
      </div>

      {/* Content */}
      <div className="flex-1 overflow-y-auto p-5 space-y-5">
        {/* Actions */}
        <div className="flex gap-2">
          <button onClick={onToggle}
            className={cn('flex items-center gap-1.5 px-3 py-2 rounded-lg text-xs font-semibold transition',
              source.is_active
                ? 'bg-red-50 text-red-600 hover:bg-red-100'
                : 'bg-emerald-50 text-emerald-600 hover:bg-emerald-100'
            )}>
            {source.is_active ? <ToggleLeft className="w-4 h-4" /> : <ToggleRight className="w-4 h-4" />}
            {source.is_active ? 'Deactivate' : 'Activate'}
          </button>
          {['failing', 'dead', 'degraded'].includes(source.health_status) && (
            <button onClick={onReset}
              className="flex items-center gap-1.5 px-3 py-2 rounded-lg text-xs font-semibold bg-sky-50 text-sky-600 hover:bg-sky-100 transition">
              <RefreshCw className="w-4 h-4" /> Reset Health
            </button>
          )}
        </div>

        {/* Stats Grid */}
        <section>
          <h4 className="section-label">Performance</h4>
          <div className="grid grid-cols-3 gap-3">
            <div className="bg-slate-50 rounded-lg p-3 text-center">
              <div className="text-xl font-bold text-navy-900 tabular-nums">{source.leads_found || 0}</div>
              <div className="text-2xs text-stone-400 font-semibold mt-0.5">Total Leads</div>
            </div>
            <div className="bg-slate-50 rounded-lg p-3 text-center">
              <div className="text-xl font-bold text-navy-900 tabular-nums">{source.total_scrapes || 0}</div>
              <div className="text-2xs text-stone-400 font-semibold mt-0.5">Total Scrapes</div>
            </div>
            <div className="bg-slate-50 rounded-lg p-3 text-center">
              <div className="text-xl font-bold text-navy-900 tabular-nums">{goldCount}</div>
              <div className="text-2xs text-stone-400 font-semibold mt-0.5">Gold URLs</div>
            </div>
          </div>
        </section>

        {/* Details */}
        <section>
          <h4 className="section-label">Details</h4>
          <div className="space-y-2 text-sm">
            <DetailRow label="Priority" value={String(source.priority)} />
            <DetailRow label="Frequency" value={source.scrape_frequency} />
            <DetailRow label="Success Rate" value={`${source.success_rate || 0}%`} />
            <DetailRow label="Consecutive Failures" value={String(source.consecutive_failures || 0)} />
            <DetailRow label="Last Scraped" value={source.last_scraped_at
              ? formatDistanceToNow(new Date(source.last_scraped_at), { addSuffix: true })
              : 'Never'} />
            <DetailRow label="Last Success" value={source.last_success_at
              ? formatDistanceToNow(new Date(source.last_success_at), { addSuffix: true })
              : 'Never'} />
          </div>
        </section>

        {/* Notes */}
        {source.notes && (
          <section>
            <h4 className="section-label">Notes</h4>
            <p className="text-xs text-stone-500 leading-relaxed">{source.notes}</p>
          </section>
        )}

        {/* Scrape History */}
        <section>
          <h4 className="section-label">Recent Scrape History</h4>
          {logs.length === 0 ? (
            <p className="text-xs text-stone-400">No scrape logs yet</p>
          ) : (
            <div className="space-y-1.5">
              {logs.map(log => (
                <div key={log.id} className={cn(
                  'flex items-center gap-3 px-3 py-2 rounded-lg text-xs',
                  log.status === 'success' ? 'bg-emerald-50/50' :
                  log.status === 'failed' ? 'bg-red-50/50' : 'bg-stone-50'
                )}>
                  <span className={cn('w-1.5 h-1.5 rounded-full flex-shrink-0',
                    log.status === 'success' ? 'bg-emerald-500' :
                    log.status === 'failed' ? 'bg-red-500' : 'bg-stone-400'
                  )} />
                  <span className="text-stone-500 tabular-nums w-24 flex-shrink-0">
                    {new Date(log.started_at).toLocaleDateString('en-US', { month: 'short', day: 'numeric' })}
                    {' '}
                    {new Date(log.started_at).toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit' })}
                  </span>
                  <span className="font-semibold text-navy-800">
                    {log.leads_new > 0 ? `+${log.leads_new} new` : '0 new'}
                  </span>
                  <span className="text-stone-400">
                    {log.pages_crawled} pages
                  </span>
                  {log.error_message && (
                    <span className="text-red-500 truncate max-w-[150px]" title={log.error_message}>
                      {log.error_message}
                    </span>
                  )}
                </div>
              ))}
            </div>
          )}
        </section>
      </div>
    </div>
  )
}

/* ═══════════════════════════════════════════════════
   TASK RUN BUTTONS
   ═══════════════════════════════════════════════════ */

function TaskRunButtons() {
  const qc = useQueryClient()

  const triggerMut = useMutation({
    mutationFn: async (task: string) => {
      const { data } = await api.post('/api/tasks/trigger', { task })
      return data
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['sources'] })
      qc.invalidateQueries({ queryKey: ['scrape-logs'] })
    },
  })

  const { data: activeTasks } = useQuery({
    queryKey: ['active-tasks'],
    queryFn: async () => (await api.get('/api/tasks/active')).data as { tasks: { name: string; status: string }[]; count: number },
    refetchInterval: 5_000,
  })

  const isRunning = (activeTasks?.count || 0) > 0
  const runningTask = activeTasks?.tasks?.[0]?.name || ''

  const buttons = [
    { task: 'smart_scrape', label: 'Scrape', icon: Radar, color: 'bg-violet-600 hover:bg-violet-700' },
    { task: 'auto_enrich', label: 'Enrich', icon: Zap, color: 'bg-amber-600 hover:bg-amber-700' },
    { task: 'daily_health_check', label: 'Health', icon: Activity, color: 'bg-emerald-600 hover:bg-emerald-700' },
  ]

  return (
    <div className="flex items-center gap-1.5">
      {isRunning && (
        <span className="flex items-center gap-1.5 px-2 py-1 rounded-full bg-navy-950 text-white text-2xs font-semibold mr-1">
          <span className="w-1.5 h-1.5 rounded-full bg-emerald-400 animate-pulse" />
          {runningTask.replace('_', ' ')}
        </span>
      )}
      {buttons.map(b => {
        const Icon = b.icon
        return (
          <button
            key={b.task}
            onClick={() => triggerMut.mutate(b.task)}
            disabled={isRunning || triggerMut.isPending}
            className={cn(
              'flex items-center gap-1 px-2 py-1 rounded text-2xs font-bold text-white transition',
              isRunning ? 'bg-stone-300 cursor-not-allowed' : b.color,
            )}
          >
            <Icon className="w-3 h-3" /> {b.label}
          </button>
        )
      })}
    </div>
  )
}


function DetailRow({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex justify-between">
      <span className="text-stone-400 font-medium">{label}</span>
      <span className="text-navy-700 font-semibold capitalize">{value}</span>
    </div>
  )
}
