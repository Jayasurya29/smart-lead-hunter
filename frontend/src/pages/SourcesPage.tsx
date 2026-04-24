import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import api from '@/api/client'
import { cn } from '@/lib/utils'
import {
  Radar, Clock, AlertTriangle, CheckCircle2, XCircle, Search, X,
  ChevronLeft, ChevronRight, ExternalLink, ToggleLeft, ToggleRight,
  RefreshCw, Activity, Globe, Zap, Eye, Timer, BarChart3, Loader2,
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
  { name: 'Health Check',     task: 'daily_health_check', time: '9:35 AM',  icon: Activity,  color: 'text-emerald-500', desc: 'Cleanup, rescore stale leads' },
  { name: 'Smart Scrape #1',  task: 'smart_scrape',       time: '10:00 AM', icon: Radar,     color: 'text-violet-500',  desc: 'Brain picks due sources' },
  { name: 'Auto Enrich #1',   task: 'auto_enrich',        time: '11:00 AM', icon: Zap,       color: 'text-amber-500',   desc: 'Enrich top 5 HOT/URGENT leads' },
  { name: 'Smart Scrape #2',  task: 'smart_scrape',       time: '12:30 PM', icon: Radar,     color: 'text-violet-500',  desc: 'Brain picks due sources' },
  { name: 'Auto Enrich #2',   task: 'auto_enrich',        time: '2:00 PM',  icon: Zap,       color: 'text-amber-500',   desc: 'Enrich top 5 HOT/URGENT leads' },
  { name: 'Smart Scrape #3',  task: 'smart_scrape',       time: '3:30 PM',  icon: Radar,     color: 'text-violet-500',  desc: 'Last scrape of the day' },
  { name: 'Auto Enrich #3',   task: 'auto_enrich',        time: '4:30 PM',  icon: Zap,       color: 'text-amber-500',   desc: 'Final enrichment round' },
  { name: 'Weekly Discovery',  task: 'weekly_discovery',   time: 'Sun 10 AM', icon: Globe,   color: 'text-sky-500',     desc: 'Discover new sources' },
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
  const [viewMode, setViewMode] = useState<'sources' | 'queries'>('sources')
  const [search, setSearch] = useState('')
  const [filter, setFilter] = useState<string>('')
  const [selectedId, setSelectedId] = useState<number | null>(null)
  const qc = useQueryClient()

  const { data: sources = [], isLoading } = useQuery<Source[]>({
    queryKey: ['sources'],
    queryFn: async () => (await api.get('/sources')).data,
    refetchInterval: 30_000,
  })

  const { data: logs = [] } = useQuery<ScrapeLog[]>({
    queryKey: ['scrape-logs'],
    queryFn: async () => (await api.get('/scrape/logs')).data,
    refetchInterval: 30_000,
  })

  const toggleMut = useMutation({
    mutationFn: async (id: number) => (await api.post(`/sources/${id}/toggle`)).data,
    onSuccess: () => qc.invalidateQueries({ queryKey: ['sources'] }),
  })

  const resetMut = useMutation({
    mutationFn: async (id: number) => (await api.post(`/sources/${id}/reset-health`)).data,
    onSuccess: () => qc.invalidateQueries({ queryKey: ['sources'] }),
  })

  // Stats
  const total = sources.length
  const healthy = sources.filter(s => s.health_status === 'healthy').length
  const blocked = sources.filter(s => s.health_status === 'failing' || s.health_status === 'dead').length
  const active = sources.filter(s => s.is_active).length
  const totalLeads = sources.reduce((sum, s) => sum + (s.leads_found || 0), 0)
  const missingPatterns = sources.filter(s => !s.gold_urls || Object.keys(s.gold_urls || {}).length === 0).length

  // Filter & search
  const filtered = sources.filter(s => {
    if (search && !s.name.toLowerCase().includes(search.toLowerCase()) && !s.base_url.toLowerCase().includes(search.toLowerCase())) return false
    if (filter === 'healthy' && s.health_status !== 'healthy') return false
    if (filter === 'problems' && !['failing', 'dead', 'degraded'].includes(s.health_status)) return false
    if (filter === 'active' && !s.is_active) return false
    if (filter === 'inactive' && s.is_active) return false
    return true
  }).sort((a, b) => (b.leads_found || 0) - (a.leads_found || 0))

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
        </div>
      </div>

      {viewMode === 'queries' ? (
        <QueriesPanel />
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

          <span className="text-xs text-stone-400 ml-auto">{filtered.length} sources</span>
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
