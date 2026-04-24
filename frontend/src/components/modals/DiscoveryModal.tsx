import { useState, useRef, useEffect, useCallback } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { useBackgroundTask } from '@/hooks/useBackgroundTask'
import {
  X, Radar, Loader2, CheckCircle2, AlertCircle, Square, Minimize2,
  Zap, Activity, BarChart3, ChevronDown, ChevronRight, Search,
} from 'lucide-react'
import { triggerDiscovery, cancelDiscovery } from '@/api/leads'
import api from '@/api/client'
import { cn } from '@/lib/utils'

interface Props { onClose: () => void }

type Status = 'idle' | 'running' | 'done' | 'error'
type Mode = 'all' | 'gold' | 'quick' | 'custom'

const MAX_RETRIES = 5

interface QuerySummary {
  total_queries: number
  gold: { count: number }
  maybe: { count: number }
  junk: { count: number }
  paused: { count: number }
}

interface QueryItem {
  query_text: string
  status: 'gold' | 'maybe' | 'junk' | 'paused'
  total_runs: number
  total_new_sources: number
  total_new_leads: number
  last_run_at: string | null
}

export default function DiscoveryModal({ onClose }: Props) {
  const [status, setStatus] = useState<Status>('idle')
  const [logs, setLogs] = useState<string[]>([])
  const [mode, setMode] = useState<Mode>('all')
  const [customCount, setCustomCount] = useState(20)
  const [extractLeads, setExtractLeads] = useState(true)
  const [showQueryList, setShowQueryList] = useState(false)
  const [queryFilter, setQueryFilter] = useState('')
  const logRef = useRef<HTMLDivElement>(null)
  const esRef = useRef<EventSource | null>(null)
  const qc = useQueryClient()
  const bg = useBackgroundTask()

  const statusRef = useRef<Status>('idle')
  const retryCountRef = useRef(0)
  const retryTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null)
  const closedRef = useRef(false)
  const receivedDataRef = useRef(false)
  const discoveryIdRef = useRef<string | null>(null)

  // Log batching to prevent UI freeze when hundreds of SSE messages arrive fast
  const logBufferRef = useRef<string[]>([])
  const logFlushTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  useEffect(() => { statusRef.current = status }, [status])

  const { data: querySummary } = useQuery<QuerySummary>({
    queryKey: ['discovery-query-summary'],
    queryFn: async () => (await api.get('/discovery/queries/stats')).data,
    staleTime: 30_000,
  })

  // Full query list — only fetched when user opens the preview pane, to
  // avoid pulling 100+ rows on every modal open.
  const { data: allQueries = [] } = useQuery<QueryItem[]>({
    queryKey: ['discovery-queries', 'full'],
    queryFn: async () => (await api.get('/discovery/queries')).data,
    enabled: showQueryList,
    staleTime: 30_000,
  })

  useEffect(() => {
    return () => {
      closedRef.current = true
      esRef.current?.close()
      if (retryTimerRef.current) clearTimeout(retryTimerRef.current)
      if (logFlushTimerRef.current) clearTimeout(logFlushTimerRef.current)
    }
  }, [])

  useEffect(() => {
    if (logRef.current) logRef.current.scrollTop = logRef.current.scrollHeight
  }, [logs])

  const flushLogs = useCallback(() => {
    if (logBufferRef.current.length === 0) return
    const batch = logBufferRef.current
    logBufferRef.current = []
    setLogs((prev) => {
      const next = [...prev, ...batch]
      return next.length > 500 ? next.slice(-500) : next
    })
  }, [])

  const addLog = useCallback((msg: string) => {
    logBufferRef.current.push(msg)
    if (!logFlushTimerRef.current) {
      logFlushTimerRef.current = setTimeout(() => {
        logFlushTimerRef.current = null
        flushLogs()
      }, 250)
    }
  }, [flushLogs])

  function createSSE(path: string) {
    if (esRef.current) {
      esRef.current.close()
      esRef.current = null
    }
    const es = new EventSource(path)
    esRef.current = es

    es.onopen = () => { retryCountRef.current = 0 }

    es.onmessage = (e) => {
      receivedDataRef.current = true
      try {
        const data = JSON.parse(e.data)
        if (data.type === 'ping') return
        if (data.type === 'stats') return
        if (data.message) {
          addLog(data.message)
          bg.addEvent()
        }
        if (data.type === 'complete' || data.done || data.status === 'complete') {
          flushLogs()
          setStatus('done')
          closedRef.current = true
          es.close()
          const newLeads = data.stats?.leads ?? data.stats?.sources ?? 0
          const duration = data.duration_seconds ?? 0
          bg.completeTask({
            type: 'discovery',
            message: 'Discovery complete',
            newLeads,
            duration,
          })
          qc.invalidateQueries({ queryKey: ['leads'] })
          qc.invalidateQueries({ queryKey: ['stats'] })
          qc.invalidateQueries({ queryKey: ['sources'] })
          qc.invalidateQueries({ queryKey: ['discovery-queries'] })
          qc.invalidateQueries({ queryKey: ['discovery-query-summary'] })
        }
        if (data.type === 'error') {
          addLog(`❌ ${data.message}`)
        }
      } catch {
        // Non-JSON message
      }
    }

    es.onerror = () => {
      if (closedRef.current || statusRef.current !== 'running') return
      if (retryCountRef.current < MAX_RETRIES) {
        retryCountRef.current++
        addLog(`⚠️ Connection lost. Retrying (${retryCountRef.current}/${MAX_RETRIES})...`)
        es.close()
        retryTimerRef.current = setTimeout(() => {
          if (!closedRef.current) createSSE(path)
        }, 2000 * retryCountRef.current)
      } else {
        addLog('❌ Connection lost. Discovery may still be running in background.')
      }
    }
  }

  async function handleStart() {
    setStatus('running')
    setLogs([])
    closedRef.current = false
    retryCountRef.current = 0
    receivedDataRef.current = false
    discoveryIdRef.current = null

    bg.startTask('discovery')

    try {
      const payload: {
        mode: string
        extract_leads: boolean
        max_queries?: number
        filter_gold?: boolean
      } = {
        mode: mode === 'quick' ? 'quick' : 'full',
        extract_leads: extractLeads,
      }
      if (mode === 'gold') payload.filter_gold = true
      if (mode === 'custom') payload.max_queries = customCount

      const result = await triggerDiscovery(payload.mode, payload.extract_leads, payload)
      discoveryIdRef.current = result.discovery_id

      addLog(`Discovery started (${labelForMode(mode, customCount, querySummary)})...`)
      createSSE(`/api/dashboard/discovery/stream?discovery_id=${result.discovery_id}`)
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : 'Unknown error'
      addLog(`❌ Failed: ${msg}`)
      setStatus('error')
    }
  }

  async function handleStop() {
    const id = discoveryIdRef.current
    closedRef.current = true
    esRef.current?.close()
    if (retryTimerRef.current) clearTimeout(retryTimerRef.current)

    if (!id) {
      setStatus('done')
      return
    }

    try {
      await cancelDiscovery(id)
      addLog('⛔ Discovery cancelled.')
    } catch {
      addLog('⛔ Discovery stopped.')
    }

    setStatus('done')
    bg.completeTask({
      type: 'discovery',
      message: 'Discovery cancelled by user',
      newLeads: 0,
      duration: 0,
    })
    qc.invalidateQueries({ queryKey: ['leads'] })
    qc.invalidateQueries({ queryKey: ['stats'] })
  }

  function handleBackground() {
    closedRef.current = true
    esRef.current?.close()
    if (retryTimerRef.current) clearTimeout(retryTimerRef.current)

    const id = discoveryIdRef.current
    if (id) {
      bg.startBackgroundPoll(id)
      addLog('Running in background — you\'ll be notified when done.')
    }
    onClose()
  }

  const total = querySummary?.total_queries ?? 0
  const goldCount = querySummary?.gold.count ?? 0

  return (
    <div className="fixed inset-0 z-50 modal-backdrop flex items-center justify-center animate-fadeIn">
      <div className="bg-white rounded-2xl shadow-modal w-[540px] max-h-[85vh] flex flex-col overflow-hidden animate-scaleIn">

        {/* Header */}
        <div className="px-5 py-4 flex items-center justify-between border-b border-stone-100">
          <div className="flex items-center gap-3">
            <div className="w-9 h-9 rounded-xl bg-violet-100 flex items-center justify-center flex-shrink-0">
              <Radar className="w-5 h-5 text-violet-600" />
            </div>
            <div>
              <h2 className="text-[15px] font-bold text-navy-900">Source Discovery</h2>
              <p className="text-[11px] text-stone-500 mt-0.5">
                {status === 'running' ? 'Running — may take a while' :
                 status === 'done' ? 'Discovery complete' :
                 'Find new hotels + new sources'}
              </p>
            </div>
          </div>
          <button
            onClick={status === 'running' ? handleBackground : onClose}
            className="p-1.5 text-stone-400 hover:text-stone-700 hover:bg-stone-100 rounded-lg transition"
            title={status === 'running' ? 'Minimize to background' : 'Close'}
          >
            {status === 'running' ? <Minimize2 className="w-4 h-4" /> : <X className="w-4 h-4" />}
          </button>
        </div>

        {/* Content */}
        <div className="flex-1 overflow-y-auto px-5 py-4 space-y-4">

          {status === 'idle' && (
            <>
              <div>
                <p className="text-[10px] uppercase tracking-wider text-stone-400 font-semibold mb-2">
                  What to run
                </p>
                <div className="grid grid-cols-2 gap-2">
                  <ModeCard
                    active={mode === 'all'}
                    onClick={() => setMode('all')}
                    icon={Radar}
                    label="All Queries"
                    sub={total > 0 ? `${total} queries · ~20 min` : 'thorough'}
                    color="violet"
                  />
                  <ModeCard
                    active={mode === 'gold'}
                    onClick={() => setMode('gold')}
                    icon={Zap}
                    disabled={goldCount === 0}
                    label="Gold Only"
                    sub={goldCount > 0 ? `${goldCount} proven queries` : 'need 3+ runs first'}
                    color="amber"
                  />
                  <ModeCard
                    active={mode === 'quick'}
                    onClick={() => setMode('quick')}
                    icon={Activity}
                    label="Quick Scan"
                    sub="5 queries · ~3 min"
                    color="sky"
                  />
                  <ModeCard
                    active={mode === 'custom'}
                    onClick={() => setMode('custom')}
                    icon={BarChart3}
                    label="Custom"
                    sub={`${customCount} queries`}
                    color="emerald"
                  />
                </div>
              </div>

              {mode === 'custom' && (
                <div className="bg-emerald-50 border border-emerald-200 rounded-xl p-4">
                  <div className="flex items-center justify-between mb-2">
                    <label className="text-[12px] font-semibold text-navy-900">
                      How many queries?
                    </label>
                    <span className="text-[14px] font-bold text-emerald-700 tabular-nums">
                      {customCount}
                    </span>
                  </div>
                  <input
                    type="range"
                    min="5"
                    max={Math.max(total, 100)}
                    step="5"
                    value={customCount}
                    onChange={(e) => setCustomCount(Number(e.target.value))}
                    className="w-full accent-emerald-600"
                  />
                  <div className="flex justify-between text-[10px] text-stone-500 mt-1">
                    <span>5 · fast</span>
                    <span>~{Math.round(customCount * 0.2)} min</span>
                    <span>{Math.max(total, 100)} · all</span>
                  </div>
                </div>
              )}

              {/* ── Preview pane: show actual queries that will run ── */}
              {total > 0 && (() => {
                // Filter the query list to match the selected mode
                const byMode = allQueries.filter((q) => {
                  if (mode === 'gold') return q.status === 'gold'
                  if (mode === 'quick') return true  // first 5 shown
                  return true // all / custom show everything
                })
                const bySearch = queryFilter
                  ? byMode.filter((q) =>
                      q.query_text.toLowerCase().includes(queryFilter.toLowerCase()),
                    )
                  : byMode
                const displayed =
                  mode === 'quick' ? bySearch.slice(0, 5) :
                  mode === 'custom' ? bySearch.slice(0, customCount) :
                  bySearch

                return (
                  <div className="border border-stone-200 rounded-xl overflow-hidden">
                    <button
                      type="button"
                      onClick={() => setShowQueryList(!showQueryList)}
                      className="w-full px-3 py-2.5 flex items-center gap-2 text-left hover:bg-stone-50 transition"
                    >
                      {showQueryList ? (
                        <ChevronDown className="w-3.5 h-3.5 text-stone-400" />
                      ) : (
                        <ChevronRight className="w-3.5 h-3.5 text-stone-400" />
                      )}
                      <span className="text-[12px] font-semibold text-navy-900">
                        {showQueryList ? 'Hide' : 'Show'} queries that will run
                      </span>
                      <span className="ml-auto text-[11px] text-stone-500 tabular-nums">
                        {mode === 'quick' ? '5' :
                         mode === 'custom' ? customCount :
                         mode === 'gold' ? (querySummary?.gold.count ?? 0) :
                         total}
                      </span>
                    </button>

                    {showQueryList && (
                      <div className="border-t border-stone-200">
                        <div className="p-2 border-b border-stone-100">
                          <div className="relative">
                            <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 w-3 h-3 text-stone-400" />
                            <input
                              type="text"
                              value={queryFilter}
                              onChange={(e) => setQueryFilter(e.target.value)}
                              placeholder="Filter queries..."
                              className="w-full h-7 pl-7 pr-2 text-[11px] bg-stone-50 border border-stone-200 rounded-md outline-none focus:border-navy-400"
                            />
                          </div>
                        </div>
                        <div className="max-h-[260px] overflow-y-auto">
                          {allQueries.length === 0 ? (
                            <div className="py-6 text-center text-[11px] text-stone-400">
                              <Loader2 className="w-4 h-4 animate-spin mx-auto mb-1" />
                              Loading queries...
                            </div>
                          ) : displayed.length === 0 ? (
                            <div className="py-6 text-center text-[11px] text-stone-400">
                              No queries match this mode or filter.
                            </div>
                          ) : (
                            displayed.map((q) => (
                              <div
                                key={q.query_text}
                                className="px-3 py-1.5 border-b border-stone-100 last:border-b-0 flex items-center gap-2 hover:bg-stone-50/50"
                              >
                                <StatusPill status={q.status} />
                                <span className="flex-1 text-[11px] text-navy-900 truncate">
                                  {q.query_text}
                                </span>
                                {q.total_new_leads > 0 && (
                                  <span className="text-[10px] font-semibold text-emerald-600 tabular-nums">
                                    {q.total_new_leads}L
                                  </span>
                                )}
                                {q.total_new_sources > 0 && (
                                  <span className="text-[10px] font-semibold text-sky-600 tabular-nums">
                                    {q.total_new_sources}S
                                  </span>
                                )}
                              </div>
                            ))
                          )}
                        </div>
                      </div>
                    )}
                  </div>
                )
              })()}

              <label className="flex items-start gap-2.5 p-3 rounded-xl border border-stone-200 hover:border-stone-300 cursor-pointer transition">
                <input
                  type="checkbox"
                  checked={extractLeads}
                  onChange={(e) => setExtractLeads(e.target.checked)}
                  className="w-4 h-4 mt-0.5 rounded border-stone-300 text-violet-600 focus:ring-violet-500"
                />
                <div className="flex-1">
                  <p className="text-[12px] font-semibold text-navy-900">Auto-extract leads</p>
                  <p className="text-[11px] text-stone-500 mt-0.5">
                    Run AI on discovered pages to extract new hotels. Uncheck to only add sources (faster).
                  </p>
                </div>
              </label>
            </>
          )}

          {(status === 'running' || status === 'done' || status === 'error') && (
            <div className="space-y-2">
              <div className="flex items-center gap-2">
                {status === 'running' && <Loader2 className="w-4 h-4 text-violet-500 animate-spin" />}
                {status === 'done'    && <CheckCircle2 className="w-4 h-4 text-emerald-500" />}
                {status === 'error'   && <AlertCircle className="w-4 h-4 text-red-500" />}
                <p className={cn(
                  'text-[13px] font-semibold',
                  status === 'running' && 'text-violet-700',
                  status === 'done' && 'text-emerald-700',
                  status === 'error' && 'text-red-700',
                )}>
                  {status === 'running' ? 'Discovery running...' :
                   status === 'done' ? 'Discovery finished' : 'Discovery failed'}
                </p>
              </div>

              <div
                ref={logRef}
                className="bg-navy-950 text-slate-200 rounded-lg p-3 font-mono text-[10.5px] leading-snug"
                style={{ minHeight: 200, maxHeight: 320, overflowY: 'auto' }}
              >
                {logs.length === 0 ? (
                  <p className="text-slate-500">Waiting for output...</p>
                ) : (
                  logs.map((line, i) => (
                    <div key={i} className="break-words">{line}</div>
                  ))
                )}
              </div>
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="px-5 py-3.5 flex items-center justify-end gap-2 border-t border-stone-100 bg-stone-50/50">
          {status === 'idle' && (
            <>
              <button onClick={onClose} className="px-4 h-9 text-[12px] font-semibold text-stone-600 hover:text-stone-900 transition">
                Cancel
              </button>
              <button
                onClick={handleStart}
                className="px-4 h-9 bg-violet-600 hover:bg-violet-700 text-white text-[12px] font-semibold rounded-lg transition flex items-center gap-1.5 shadow-sm"
              >
                <Radar className="w-3.5 h-3.5" />
                Start Discovery
              </button>
            </>
          )}

          {status === 'running' && (
            <>
              <button
                onClick={handleStop}
                className="px-4 h-9 bg-red-50 hover:bg-red-100 text-red-700 text-[12px] font-semibold rounded-lg transition flex items-center gap-1.5"
              >
                <Square className="w-3 h-3" fill="currentColor" />
                Stop
              </button>
              <button
                onClick={handleBackground}
                className="px-4 h-9 bg-navy-900 hover:bg-navy-800 text-white text-[12px] font-semibold rounded-lg transition flex items-center gap-1.5"
              >
                <Minimize2 className="w-3.5 h-3.5" />
                Run in background
              </button>
            </>
          )}

          {(status === 'done' || status === 'error') && (
            <button
              onClick={onClose}
              className="px-4 h-9 bg-navy-900 hover:bg-navy-800 text-white text-[12px] font-semibold rounded-lg transition"
            >
              Close
            </button>
          )}
        </div>
      </div>
    </div>
  )
}


function ModeCard({
  active, onClick, icon: Icon, label, sub, color, disabled,
}: {
  active: boolean
  onClick: () => void
  icon: React.ElementType
  label: string
  sub: string
  color: 'violet' | 'amber' | 'sky' | 'emerald'
  disabled?: boolean
}) {
  const colorMap = {
    violet:  { bg: 'bg-violet-50',  border: 'border-violet-300',  text: 'text-violet-700',  icon: 'text-violet-600' },
    amber:   { bg: 'bg-amber-50',   border: 'border-amber-300',   text: 'text-amber-700',   icon: 'text-amber-600' },
    sky:     { bg: 'bg-sky-50',     border: 'border-sky-300',     text: 'text-sky-700',     icon: 'text-sky-600' },
    emerald: { bg: 'bg-emerald-50', border: 'border-emerald-300', text: 'text-emerald-700', icon: 'text-emerald-600' },
  }[color]
  return (
    <button
      onClick={disabled ? undefined : onClick}
      disabled={disabled}
      className={cn(
        'text-left p-3 rounded-xl border-2 transition flex items-start gap-2.5',
        active && !disabled ? `${colorMap.bg} ${colorMap.border}` : 'bg-white border-stone-200 hover:border-stone-300',
        disabled && 'opacity-40 cursor-not-allowed',
      )}
    >
      <div className={cn(
        'w-8 h-8 rounded-lg flex items-center justify-center flex-shrink-0',
        active && !disabled ? colorMap.bg : 'bg-stone-100',
      )}>
        <Icon className={cn('w-4 h-4', active && !disabled ? colorMap.icon : 'text-stone-400')} />
      </div>
      <div className="flex-1 min-w-0">
        <p className={cn(
          'text-[12px] font-bold',
          active && !disabled ? colorMap.text : 'text-navy-900',
        )}>{label}</p>
        <p className="text-[10.5px] text-stone-500 mt-0.5 leading-tight">{sub}</p>
      </div>
    </button>
  )
}


function labelForMode(mode: Mode, customCount: number, summary?: QuerySummary): string {
  if (mode === 'all') return `all ${summary?.total_queries ?? '?'} queries`
  if (mode === 'gold') return `${summary?.gold.count ?? 0} gold queries`
  if (mode === 'quick') return '5 queries (quick scan)'
  return `${customCount} queries (custom)`
}

function StatusPill({ status }: { status: 'gold' | 'maybe' | 'junk' | 'paused' }) {
  const map = {
    gold:   { bg: 'bg-amber-100',  text: 'text-amber-700',  label: '🥇' },
    maybe:  { bg: 'bg-sky-100',    text: 'text-sky-700',    label: '◌' },
    junk:   { bg: 'bg-stone-200',  text: 'text-stone-500',  label: '✕' },
    paused: { bg: 'bg-violet-100', text: 'text-violet-700', label: '⏸' },
  }[status] || { bg: 'bg-stone-100', text: 'text-stone-500', label: '?' }
  return (
    <span
      className={cn(
        'w-5 h-5 rounded flex items-center justify-center text-[9px] font-bold shrink-0',
        map.bg, map.text,
      )}
      title={status}
    >
      {map.label}
    </span>
  )
}
