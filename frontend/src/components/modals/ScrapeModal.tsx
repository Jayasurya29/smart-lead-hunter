import { useState, useRef, useEffect, useMemo, useCallback } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { X, Zap, Target, Radio, Rocket, Loader2, CheckCircle2, AlertCircle, Star, Search } from 'lucide-react'
import { triggerScrape, createSSEStream } from '@/api/leads'
import { useSources } from '@/hooks/useLeads'
import type { SourceInfo, SSEEvent } from '@/api/types'
import { cn } from '@/lib/utils'

type ScrapeMode = 'smart' | 'manual' | 'full'
type Status = 'idle' | 'running' | 'done' | 'error'

interface Props { onClose: () => void }

const CATEGORIES: { key: string; label: string }[] = [
  { key: 'aggregator', label: 'Aggregators' },
  { key: 'caribbean', label: 'Caribbean' },
  { key: 'chain_newsroom', label: 'Chain Newsrooms' },
  { key: 'florida', label: 'Florida' },
  { key: 'industry', label: 'Industry' },
  { key: 'luxury_independent', label: 'Luxury' },
  { key: 'travel_pub', label: 'Travel Pubs' },
  { key: 'pr_wire', label: 'PR Wire' },
]

const MODE_CONFIG = [
  { key: 'smart' as const, label: 'Smart', desc: 'Only due sources', icon: Zap, color: 'amber' },
  { key: 'manual' as const, label: 'Manual', desc: 'Pick sources', icon: Target, color: 'emerald' },
  { key: 'full' as const, label: 'Full Sweep', desc: 'All sources', icon: Radio, color: 'blue' },
]

// FIX H-01: Tailwind JIT can't detect dynamic classes like `bg-${color}-50`.
// Use a static lookup so all class strings appear in source code.
const MODE_STYLES: Record<string, { bg: string; border: string; text: string }> = {
  amber:   { bg: 'bg-amber-50',   border: 'border-amber-400',   text: 'text-amber-600' },
  emerald: { bg: 'bg-emerald-50', border: 'border-emerald-400', text: 'text-emerald-600' },
  blue:    { bg: 'bg-blue-50',    border: 'border-blue-400',    text: 'text-blue-600' },
}

export default function ScrapeModal({ onClose }: Props) {
  const [mode, setMode] = useState<ScrapeMode>('smart')
  const [status, setStatus] = useState<Status>('idle')
  const [logs, setLogs] = useState<string[]>([])
  const [selected, setSelected] = useState<Set<number>>(new Set())
  const [activeCategories, setActiveCategories] = useState<Set<string>>(new Set())
  const [search, setSearch] = useState('')

  const logRef = useRef<HTMLDivElement>(null)
  const esRef = useRef<EventSource | null>(null)
  const qc = useQueryClient()

  const { data: sourcesData, isLoading: sourcesLoading } = useSources()

  const allSources = useMemo(() => sourcesData?.sources || [], [sourcesData])
  const dueSources = useMemo(() => sourcesData?.due_sources || [], [sourcesData])

  const categoryCounts = useMemo(() => {
    const c: Record<string, number> = {}
    allSources.forEach(s => { c[s.type || 'other'] = (c[s.type || 'other'] || 0) + 1 })
    return c
  }, [allSources])

  const filteredSources = useMemo(() => {
    let list = allSources
    if (activeCategories.size > 0) list = list.filter(s => activeCategories.has(s.type || ''))
    if (search.trim()) {
      const q = search.toLowerCase()
      list = list.filter(s => s.name.toLowerCase().includes(q))
    }
    return list
  }, [allSources, activeCategories, search])

  useEffect(() => {
    return () => { esRef.current?.close() }
  }, [])

  useEffect(() => {
    if (logRef.current) logRef.current.scrollTop = logRef.current.scrollHeight
  }, [logs])

  const toggleSource = useCallback((id: number) => {
    setSelected(prev => { const n = new Set(prev); n.has(id) ? n.delete(id) : n.add(id); return n })
  }, [])

  const toggleCategory = useCallback((key: string) => {
    setActiveCategories(prev => { const n = new Set(prev); n.has(key) ? n.delete(key) : n.add(key); return n })
  }, [])

  function switchMode(m: ScrapeMode) {
    setMode(m); setSelected(new Set()); setActiveCategories(new Set()); setSearch('')
  }

  async function handleStart() {
    setStatus('running')
    const ids = mode === 'manual' ? Array.from(selected) : []
    setLogs([`Starting ${mode} scrape...`])
    try {
      const result = await triggerScrape(mode, ids)
      const scrapeId = result.scrape_id || ''
      const es = createSSEStream(`/api/dashboard/scrape/stream?scrape_id=${scrapeId}`)
      esRef.current = es
      es.onmessage = (e) => {
        try {
          const d: SSEEvent = JSON.parse(e.data)
          if (d.message) setLogs(p => [...p, d.message])
          if (d.type === 'complete' || d.type === 'error') {
            setStatus(d.type === 'error' ? 'error' : 'done')
            es.close()
            qc.invalidateQueries({ queryKey: ['leads'] })
            qc.invalidateQueries({ queryKey: ['stats'] })
            qc.invalidateQueries({ queryKey: ['sources'] })
          }
        } catch {
          if (e.data && e.data !== 'ping') setLogs(p => [...p, e.data])
        }
      }
      es.onerror = () => {
        es.close()
        setStatus('done')
        setLogs(p => [...p, '— Stream ended —'])
        qc.invalidateQueries({ queryKey: ['leads'] })
        qc.invalidateQueries({ queryKey: ['stats'] })
      }
    } catch (err: any) {
      setStatus('error')
      setLogs(p => [...p, `Error: ${err?.response?.data?.message || err.message || 'Failed to start'}`])
    }
  }

  const actionLabel =
    mode === 'smart'  ? `Start Pipeline (${dueSources.length} due)` :
    mode === 'manual' ? `Scrape ${selected.size} Source${selected.size !== 1 ? 's' : ''}` :
    `Sweep All ${allSources.length}`

  const canStart =
    mode === 'smart'  ? dueSources.length > 0 :
    mode === 'manual' ? selected.size > 0 :
    allSources.length > 0

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center modal-backdrop animate-fadeIn" onClick={onClose}>
      <div className="bg-white rounded-xl shadow-2xl w-full max-w-[580px] mx-4 overflow-hidden animate-scaleIn flex flex-col max-h-[82vh]" onClick={e => e.stopPropagation()}>
        {/* Header */}
        <div className="flex items-center justify-between px-5 py-4 border-b border-stone-100">
          <div className="flex items-center gap-2.5">
            <div className="w-8 h-8 rounded-lg bg-gradient-to-br from-emerald-400 to-emerald-600 flex items-center justify-center shadow-sm">
              <Rocket className="w-4 h-4 text-white" />
            </div>
            <div>
              <h3 className="text-[15px] font-bold text-stone-900">Run Scrape Pipeline</h3>
              <p className="text-[11px] text-stone-400">
                {sourcesLoading ? 'Loading sources...' : `${allSources.length} sources · ${dueSources.length} due`}
              </p>
            </div>
          </div>
          <button onClick={onClose} className="p-1.5 text-stone-400 hover:text-stone-600 rounded-lg hover:bg-stone-100 transition">
            <X className="w-4 h-4" />
          </button>
        </div>

        {status === 'idle' ? (
          <>
            {/* Mode Cards */}
            <div className="grid grid-cols-3 gap-2 px-5 pt-4 pb-3">
              {MODE_CONFIG.map(m => {
                const active = mode === m.key
                const styles = MODE_STYLES[m.color]
                return (
                  <button key={m.key} onClick={() => switchMode(m.key)} className={cn(
                    'p-3 rounded-lg border-2 text-left transition-all duration-150',
                    active ? `${styles.bg} ${styles.border}` : 'border-stone-200 hover:border-stone-300',
                  )}>
                    <m.icon className={cn('w-4 h-4 mb-1.5', active ? styles.text : 'text-stone-400')} />
                    <div className={cn('text-[12px] font-bold', active ? 'text-stone-900' : 'text-stone-700')}>{m.label}</div>
                    <div className="text-[10px] text-stone-400 mt-0.5">{m.desc}</div>
                  </button>
                )
              })}
            </div>

            {/* Smart Mode Info */}
            {mode === 'smart' && (
              <div className="px-5 pb-2">
                <div className="flex items-center gap-3 p-3.5 rounded-lg bg-amber-50 border border-amber-200">
                  <Zap className="w-5 h-5 text-amber-500 flex-shrink-0" />
                  <div>
                    <div className="text-[13px] font-semibold text-stone-800">
                      {dueSources.length} source{dueSources.length !== 1 ? 's' : ''} due for scraping
                    </div>
                    <div className="text-[11px] text-stone-400 mt-0.5">Based on schedule intervals and last scrape times</div>
                    {dueSources.length > 0 && (
                      <div className="mt-2 flex flex-wrap gap-1">
                        {dueSources.slice(0, 8).map(s => (
                          <span key={s.id} className="text-[10px] bg-amber-100 text-amber-700 px-1.5 py-0.5 rounded font-medium">
                            {s.name}
                          </span>
                        ))}
                        {dueSources.length > 8 && (
                          <span className="text-[10px] text-amber-500 font-medium">+{dueSources.length - 8} more</span>
                        )}
                      </div>
                    )}
                  </div>
                </div>
              </div>
            )}

            {/* Source Picker (manual + full) */}
            {(mode === 'manual' || mode === 'full') && (
              <>
                <div className="px-5 pb-2 space-y-2">
                  {mode === 'manual' && (
                    <div className="relative">
                      <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-stone-400" />
                      <input type="text" value={search} onChange={e => setSearch(e.target.value)}
                        placeholder="Search sources..." className="w-full pl-8 pr-3 py-1.5 text-xs rounded-lg border border-stone-200 bg-stone-50 text-stone-700 placeholder:text-stone-400 outline-none focus:border-stone-400 transition" />
                    </div>
                  )}
                  <div className="flex flex-wrap gap-1.5 items-center">
                    {CATEGORIES.filter(c => categoryCounts[c.key]).map(cat => (
                      <button key={cat.key} onClick={() => toggleCategory(cat.key)}
                        className={cn('px-2 py-1 rounded-md text-[11px] font-medium border transition-all',
                          activeCategories.has(cat.key) ? 'bg-emerald-50 border-emerald-300 text-emerald-700' : 'border-stone-200 text-stone-500 hover:border-stone-300',
                        )}>
                        {cat.label} <span className="opacity-50">({categoryCounts[cat.key]})</span>
                      </button>
                    ))}
                    {mode === 'manual' && (
                      <div className="ml-auto flex items-center gap-1">
                        <button onClick={() => setSelected(new Set(filteredSources.map(s => s.id)))}
                          className="px-2 py-1 rounded-md text-[11px] font-bold border bg-emerald-50 border-emerald-300 text-emerald-700">Select All</button>
                        <button onClick={() => setSelected(new Set())}
                          className="px-2 py-1 text-[11px] font-medium text-stone-400 hover:text-stone-600 transition">Clear</button>
                      </div>
                    )}
                  </div>
                </div>
                <div className="border-t border-stone-100 overflow-y-auto max-h-[260px]">
                  {sourcesLoading ? (
                    <div className="py-10 text-center text-xs text-stone-400">Loading sources...</div>
                  ) : filteredSources.length === 0 ? (
                    <div className="py-10 text-center text-xs text-stone-400">No sources match your filters</div>
                  ) : (
                    filteredSources.map(source => {
                      const checked = mode === 'full' || selected.has(source.id)
                      return (
                        <label key={source.id} className={cn(
                          'flex items-start gap-3 px-5 py-2.5 cursor-pointer transition-colors border-b border-stone-50',
                          checked ? 'bg-stone-50/60' : 'hover:bg-stone-50/40',
                        )}>
                          <input type="checkbox" checked={checked}
                            onChange={() => mode === 'manual' && toggleSource(source.id)}
                            disabled={mode === 'full'}
                            className="mt-1 w-4 h-4 rounded border-stone-300 accent-emerald-600" />
                          <div className="flex-1 min-w-0">
                            <div className="flex items-center gap-2 flex-wrap">
                              <span className="text-[13px] font-semibold text-stone-800">{source.name}</span>
                              <HealthBadge status={source.health} />
                              {source.gold_count > 0 && (
                                <span className="inline-flex items-center gap-px">
                                  <Star className="w-3 h-3 fill-amber-400 text-amber-400" />
                                  <span className="text-[10px] font-semibold text-amber-600">{source.gold_count}</span>
                                </span>
                              )}
                              {source.leads > 0 && (
                                <span className="text-[11px] font-semibold text-stone-500 tabular-nums">{source.leads} leads</span>
                              )}
                            </div>
                            <div className="text-[11px] text-stone-400 mt-0.5">
                              {source.type?.replace(/_/g, ' ')} · P{source.priority}
                              {source.last_scraped && ` · Last: ${new Date(source.last_scraped).toLocaleDateString()}`}
                            </div>
                          </div>
                        </label>
                      )
                    })
                  )}
                </div>
              </>
            )}

            {/* Footer */}
            <div className="flex items-center justify-between px-5 py-3 border-t border-stone-100 mt-auto">
              <span className="text-[11px] text-stone-400 tabular-nums">
                {mode === 'manual' && `${selected.size} of ${filteredSources.length} sources selected`}
                {mode === 'full' && `All ${allSources.length} active sources`}
                {mode === 'smart' && `${dueSources.length} due sources`}
              </span>
              <div className="flex items-center gap-2">
                <button onClick={onClose} className="px-4 py-2 text-xs font-semibold text-stone-500 border border-stone-200 rounded-lg hover:bg-stone-50 transition">Cancel</button>
                <button onClick={handleStart} disabled={!canStart}
                  className={cn('px-5 py-2 text-xs font-bold text-white rounded-lg transition-all shadow-sm flex items-center gap-1.5 active:scale-[0.98]',
                    canStart ? 'bg-emerald-600 hover:bg-emerald-700' : 'bg-stone-200 text-stone-400 cursor-not-allowed shadow-none',
                  )}>
                  <Rocket className="w-3.5 h-3.5" />
                  {actionLabel}
                </button>
              </div>
            </div>
          </>
        ) : (
          /* Running / Done / Error */
          <>
            <div className="p-5 flex-1 min-h-0 flex flex-col">
              <div className="flex items-center gap-2 mb-3">
                {status === 'running' && <Loader2 className="w-4 h-4 animate-spin text-emerald-500" />}
                {status === 'done' && <CheckCircle2 className="w-4 h-4 text-emerald-500" />}
                {status === 'error' && <AlertCircle className="w-4 h-4 text-red-500" />}
                <span className="text-xs font-semibold text-stone-800">
                  {status === 'running' ? 'Scraping in progress...' : status === 'done' ? 'Scrape complete' : 'Operation failed'}
                </span>
                {status === 'running' && <span className="ml-auto text-[10px] text-stone-400 tabular-nums">{logs.length} events</span>}
              </div>
              <div ref={logRef} className="bg-stone-950 text-stone-400 rounded-lg p-3 flex-1 overflow-y-auto font-mono text-[11px] leading-relaxed min-h-[220px] max-h-[300px]">
                {logs.map((log, i) => (
                  <div key={i} className={cn(
                    log.includes('Error') || log.includes('error') || log.includes('❌') ? 'text-red-400' :
                    log.includes('✅') || log.includes('complete') || log.includes('Saved') ? 'text-emerald-400' :
                    log.includes('Phase') || log.includes('Starting') || log.includes('═') ? 'text-amber-400' : '',
                  )}>{log}</div>
                ))}
                {status === 'running' && (
                  <span className="inline-flex gap-1 mt-1">
                    <span className="w-1 h-1 rounded-full bg-stone-500 animate-pulse" />
                    <span className="w-1 h-1 rounded-full bg-stone-500 animate-pulse" style={{ animationDelay: '150ms' }} />
                    <span className="w-1 h-1 rounded-full bg-stone-500 animate-pulse" style={{ animationDelay: '300ms' }} />
                  </span>
                )}
              </div>
            </div>
            {(status === 'done' || status === 'error') && (
              <div className="px-5 py-3 border-t border-stone-100 flex justify-end">
                <button onClick={onClose} className="px-4 py-2 text-xs font-bold bg-stone-900 text-white rounded-lg hover:bg-stone-800 transition">Close</button>
              </div>
            )}
          </>
        )}
      </div>
    </div>
  )
}

function HealthBadge({ status }: { status: string | null }) {
  if (!status) return null
  const styles: Record<string, string> = {
    healthy: 'bg-emerald-100 text-emerald-700',
    degraded: 'bg-amber-100 text-amber-700',
    failing: 'bg-red-100 text-red-700',
    dead: 'bg-stone-200 text-stone-500',
    new: 'bg-blue-100 text-blue-700',
  }
  return (
    <span className={cn('text-[10px] font-semibold px-1.5 py-0.5 rounded', styles[status] || 'bg-stone-100 text-stone-500')}>
      {status}
    </span>
  )
}
