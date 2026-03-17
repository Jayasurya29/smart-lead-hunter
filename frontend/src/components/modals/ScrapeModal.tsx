import { useState, useRef, useEffect, useMemo, useCallback } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import {
  X, Zap, Target, Radio, Link2, Search,
  Rocket, Loader2, CheckCircle2, AlertCircle, Star,
} from 'lucide-react'
import { triggerScrape, fetchSources } from '@/api/leads'
import api from '@/api/client'
import type { Source } from '@/api/types'
import { cn } from '@/lib/utils'

// ── Types ──

type ScrapeMode = 'smart' | 'manual' | 'full' | 'url'
type Status = 'idle' | 'running' | 'done' | 'error'

interface Props {
  onClose: () => void
}

// ── Categories ──

const CATEGORIES: { key: string; label: string }[] = [
  { key: 'aggregator', label: 'Aggregators' },
  { key: 'caribbean', label: 'Caribbean' },
  { key: 'chain_newsroom', label: 'Chain Newsrooms' },
  { key: 'florida', label: 'Florida' },
  { key: 'industry', label: 'Industry' },
  { key: 'luxury_independent', label: 'Luxury & Independent' },
  { key: 'travel_pub', label: 'Travel Pubs' },
]

// ── Modes ──

const MODES: {
  key: ScrapeMode
  label: string
  desc: string
  icon: typeof Zap
  activeBg: string
  activeBorder: string
  activeText: string
}[] = [
  { key: 'smart',  label: 'Smart',        desc: 'Only due sources',  icon: Zap,    activeBg: 'bg-amber-50',   activeBorder: 'border-amber-400',   activeText: 'text-amber-600' },
  { key: 'manual', label: 'Manual',       desc: 'Pick sources',      icon: Target, activeBg: 'bg-emerald-50',  activeBorder: 'border-emerald-400', activeText: 'text-emerald-600' },
  { key: 'full',   label: 'Full Sweep',   desc: 'All sources',       icon: Radio,  activeBg: 'bg-blue-50',    activeBorder: 'border-blue-400',    activeText: 'text-blue-600' },
  { key: 'url',    label: 'URL Extract',  desc: 'Paste any URL',     icon: Link2,  activeBg: 'bg-violet-50',  activeBorder: 'border-violet-400',  activeText: 'text-violet-600' },
]

// ── Accent per mode ──

function useAccent(mode: ScrapeMode) {
  const map = {
    smart:  { btn: 'bg-amber-500 hover:bg-amber-600 shadow-amber-500/25',      tag: 'bg-amber-50 border-amber-300 text-amber-700',   check: 'accent-amber-500' },
    manual: { btn: 'bg-emerald-600 hover:bg-emerald-700 shadow-emerald-600/25', tag: 'bg-emerald-50 border-emerald-300 text-emerald-700', check: 'accent-emerald-600' },
    full:   { btn: 'bg-blue-600 hover:bg-blue-700 shadow-blue-600/25',          tag: 'bg-blue-50 border-blue-300 text-blue-700',       check: 'accent-blue-600' },
    url:    { btn: 'bg-violet-600 hover:bg-violet-700 shadow-violet-600/25',    tag: 'bg-violet-50 border-violet-300 text-violet-700', check: 'accent-violet-600' },
  }
  return map[mode]
}

// ── Health badge ──

function HealthBadge({ status }: { status: string | null }) {
  if (!status) return null
  const styles: Record<string, string> = {
    healthy: 'bg-emerald-100 text-emerald-700',
    warning: 'bg-amber-100 text-amber-700',
    error:   'bg-red-100 text-red-700',
  }
  return (
    <span className={cn('text-[10px] font-semibold px-1.5 py-0.5 rounded', styles[status] || 'bg-stone-100 text-stone-500')}>
      {status}
    </span>
  )
}

// ── Stars ──

function StarRating({ count }: { count: number }) {
  if (!count) return null
  return (
    <span className="inline-flex items-center gap-px">
      <Star className="w-3 h-3 fill-amber-400 text-amber-400" />
      <span className="text-[10px] font-semibold text-amber-600">{count}</span>
    </span>
  )
}

// ═══════════════════════════════════════════
// ── MAIN ──
// ═══════════════════════════════════════════

export default function ScrapeModal({ onClose }: Props) {
  const [mode, setMode] = useState<ScrapeMode>('smart')
  const [status, setStatus] = useState<Status>('idle')
  const [logs, setLogs] = useState<string[]>([])
  const [sources, setSources] = useState<Source[]>([])
  const [selected, setSelected] = useState<Set<number>>(new Set())
  const [activeCategories, setActiveCategories] = useState<Set<string>>(new Set())
  const [search, setSearch] = useState('')
  const [url, setUrl] = useState('')

  const logRef = useRef<HTMLDivElement>(null)
  const esRef = useRef<EventSource | null>(null)
  const qc = useQueryClient()
  const accent = useAccent(mode)

  useEffect(() => {
    fetchSources().then(data => {
      const list: Source[] = Array.isArray(data) ? data : data?.sources || []
      setSources(list)
    }).catch(() => {})
    return () => { esRef.current?.close() }
  }, [])

  useEffect(() => {
    if (logRef.current) logRef.current.scrollTop = logRef.current.scrollHeight
  }, [logs])

  const activeSources = useMemo(() => sources.filter(s => s.is_active), [sources])

  const categoryCounts = useMemo(() => {
    const c: Record<string, number> = {}
    activeSources.forEach(s => { c[s.source_type || 'other'] = (c[s.source_type || 'other'] || 0) + 1 })
    return c
  }, [activeSources])

  const filteredSources = useMemo(() => {
    let list = activeSources
    if (activeCategories.size > 0) list = list.filter(s => activeCategories.has(s.source_type || ''))
    if (search.trim()) { const q = search.toLowerCase(); list = list.filter(s => s.name.toLowerCase().includes(q)) }
    return list
  }, [activeSources, activeCategories, search])

  const dueSources = useMemo(() => activeSources.filter((_, i) => i % 5 === 0), [activeSources])

  const toggleSource = useCallback((id: number) => {
    setSelected(prev => { const n = new Set(prev); n.has(id) ? n.delete(id) : n.add(id); return n })
  }, [])

  const toggleCategory = useCallback((key: string) => {
    setActiveCategories(prev => { const n = new Set(prev); n.has(key) ? n.delete(key) : n.add(key); return n })
  }, [])

  function switchMode(m: ScrapeMode) {
    setMode(m); setSelected(new Set()); setActiveCategories(new Set()); setSearch('')
  }

  // ── Stream ──

  function connectStream(streamUrl: string) {
    const token = localStorage.getItem('slh_token')
    const sep = streamUrl.includes('?') ? '&' : '?'
    const fullUrl = token ? `${streamUrl}${sep}api_key=${token}` : streamUrl
    const es = new EventSource(fullUrl)
    esRef.current = es
    es.onmessage = (e) => {
      try {
        const d = JSON.parse(e.data)
        if (d.message) setLogs(p => [...p, d.message])
        // Backend SSE sends type='complete' (not status='complete')
        if (d.type === 'complete' || d.type === 'error') {
          setStatus(d.type === 'error' ? 'error' : 'done'); es.close()
          qc.invalidateQueries({ queryKey: ['leads'] })
          qc.invalidateQueries({ queryKey: ['stats'] })
        }
      } catch { if (e.data && e.data !== 'ping') setLogs(p => [...p, e.data]) }
    }
    es.onerror = () => {
      es.close(); setStatus('done')
      setLogs(p => [...p, '— Stream ended —'])
      qc.invalidateQueries({ queryKey: ['leads'] })
      qc.invalidateQueries({ queryKey: ['stats'] })
    }
  }

  async function handleStart() {
    setStatus('running')
    if (mode === 'url') {
      if (!url.trim()) return
      setLogs(['Submitting URL for extraction...'])
      try {
        const { data } = await api.post('/api/dashboard/extract-url', { url: url.trim() })
        const extractId = data?.extract_id || data?.id || ''
        connectStream(`/api/dashboard/extract-url/stream?extract_id=${extractId}`)
      } catch (err: any) {
        setStatus('error'); setLogs(p => [...p, `Error: ${err.message || 'Failed to extract'}`])
      }
      return
    }
    const apiMode = mode === 'smart' ? 'smart' : 'full'
    const ids = mode === 'manual' ? Array.from(selected) : []
    setLogs([`Starting ${mode} scrape...`])
    try {
      const result = await triggerScrape(apiMode, ids)
      const scrapeId = result?.scrape_id || ''
      connectStream(`/api/dashboard/scrape/stream?scrape_id=${scrapeId}`)
    } catch (err: any) {
      setStatus('error'); setLogs(p => [...p, `Error: ${err.message || 'Failed to start'}`])
    }
  }

  const actionLabel =
    mode === 'smart'  ? 'Start Pipeline' :
    mode === 'manual' ? `Scrape ${selected.size} Source${selected.size !== 1 ? 's' : ''}` :
    mode === 'full'   ? `Sweep All ${activeSources.length}` :
    'Extract Leads'

  const canStart =
    mode === 'smart'  ? true :
    mode === 'manual' ? selected.size > 0 :
    mode === 'full'   ? activeSources.length > 0 :
    url.trim().length > 0

  const showSourcePicker = mode === 'manual' || mode === 'full'

  function modeStat(key: ScrapeMode) {
    if (key === 'smart')  return `${dueSources.length} due`
    if (key === 'manual') return `${selected.size} selected`
    if (key === 'full')   return `${activeSources.length} sources`
    return 'Direct extract'
  }

  // ══════════════════════════
  // RENDER
  // ══════════════════════════

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center modal-backdrop animate-fadeIn" onClick={onClose}>
      <div
        className="bg-white dark:bg-stone-900 rounded-xl shadow-2xl w-full max-w-[580px] mx-4 overflow-hidden animate-scaleIn flex flex-col max-h-[82vh]"
        onClick={e => e.stopPropagation()}
      >
        {/* ── HEADER ── */}
        <div className="flex items-center justify-between px-5 py-4 border-b border-stone-100 dark:border-stone-800">
          <div className="flex items-center gap-2.5">
            <div className="w-8 h-8 rounded-lg bg-gradient-to-br from-emerald-400 to-emerald-600 flex items-center justify-center shadow-sm shadow-emerald-600/20">
              <Rocket className="w-4 h-4 text-white" />
            </div>
            <div>
              <h3 className="text-[15px] font-bold text-stone-900 dark:text-stone-100">Run Scrape Pipeline</h3>
              <p className="text-[11px] text-stone-400">Choose scrape mode and sources</p>
            </div>
          </div>
          <button onClick={onClose} className="p-1.5 text-stone-400 hover:text-stone-600 dark:hover:text-stone-300 rounded-lg hover:bg-stone-100 dark:hover:bg-stone-800 transition">
            <X className="w-4 h-4" />
          </button>
        </div>

        {status === 'idle' ? (
          <>
            {/* ── MODE CARDS ── */}
            <div className="grid grid-cols-4 gap-2 px-5 pt-4 pb-3">
              {MODES.map(m => {
                const active = mode === m.key
                return (
                  <button
                    key={m.key}
                    onClick={() => switchMode(m.key)}
                    className={cn(
                      'p-3 rounded-lg border-2 text-left transition-all duration-150',
                      active
                        ? cn(m.activeBg, m.activeBorder)
                        : 'border-stone-200 dark:border-stone-700 hover:border-stone-300 dark:hover:border-stone-600',
                    )}
                  >
                    <m.icon className={cn('w-4 h-4 mb-1.5', active ? m.activeText : 'text-stone-400')} />
                    <div className={cn('text-[12px] font-bold leading-tight', active ? 'text-stone-900 dark:text-stone-100' : 'text-stone-700 dark:text-stone-300')}>
                      {m.label}
                    </div>
                    <div className="text-[10px] text-stone-400 mt-0.5 leading-snug">{m.desc}</div>
                    <div className={cn('text-[10px] font-semibold mt-1 tabular-nums', active ? m.activeText : 'text-stone-400')}>
                      {modeStat(m.key)}
                    </div>
                  </button>
                )
              })}
            </div>

            {/* ── SOURCE PICKER ── */}
            {showSourcePicker && (
              <>
                <div className="px-5 pb-2 space-y-2">
                  {mode === 'manual' && (
                    <div className="relative">
                      <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-stone-400" />
                      <input
                        type="text"
                        value={search}
                        onChange={e => setSearch(e.target.value)}
                        placeholder="Search sources..."
                        className="w-full pl-8 pr-3 py-1.5 text-xs rounded-lg border border-stone-200 dark:border-stone-700 bg-stone-50 dark:bg-stone-800 text-stone-700 dark:text-stone-200 placeholder:text-stone-400 outline-none focus:border-stone-400 transition"
                      />
                    </div>
                  )}
                  <div className="flex flex-wrap gap-1.5 items-center">
                    {CATEGORIES.filter(c => categoryCounts[c.key]).map(cat => {
                      const isActive = activeCategories.has(cat.key)
                      return (
                        <button
                          key={cat.key}
                          onClick={() => toggleCategory(cat.key)}
                          className={cn(
                            'px-2 py-1 rounded-md text-[11px] font-medium border transition-all',
                            isActive ? accent.tag : 'border-stone-200 dark:border-stone-700 text-stone-500 hover:border-stone-300',
                          )}
                        >
                          {cat.label} <span className="opacity-50">({categoryCounts[cat.key]})</span>
                        </button>
                      )
                    })}
                    {mode === 'manual' && (
                      <div className="ml-auto flex items-center gap-1">
                        <button onClick={() => setSelected(new Set(filteredSources.map(s => s.id)))} className={cn('px-2 py-1 rounded-md text-[11px] font-bold border', accent.tag)}>
                          Select All
                        </button>
                        <button onClick={() => setSelected(new Set())} className="px-2 py-1 text-[11px] font-medium text-stone-400 hover:text-stone-600 transition">
                          Clear
                        </button>
                      </div>
                    )}
                  </div>
                </div>

                <div className="border-t border-stone-100 dark:border-stone-800 overflow-y-auto max-h-[260px]">
                  {filteredSources.length === 0 ? (
                    <div className="py-10 text-center text-xs text-stone-400">No sources match your filters</div>
                  ) : (
                    filteredSources.map(source => {
                      const checked = mode === 'full' || selected.has(source.id)
                      return (
                        <label
                          key={source.id}
                          className={cn(
                            'flex items-start gap-3 px-5 py-2.5 cursor-pointer transition-colors border-b border-stone-50 dark:border-stone-800/50',
                            checked ? 'bg-stone-50/60 dark:bg-stone-800/30' : 'hover:bg-stone-50/40 dark:hover:bg-stone-800/20',
                          )}
                        >
                          <input
                            type="checkbox"
                            checked={checked}
                            onChange={() => mode === 'manual' && toggleSource(source.id)}
                            disabled={mode === 'full'}
                            className={cn('mt-1 w-4 h-4 rounded border-stone-300 dark:border-stone-600', accent.check)}
                          />
                          <div className="flex-1 min-w-0">
                            <div className="flex items-center gap-2 flex-wrap">
                              <span className="text-[13px] font-semibold text-stone-800 dark:text-stone-200">{source.name}</span>
                              <HealthBadge status={source.health_status} />
                              <StarRating count={source.gold_count || 0} />
                              {source.leads_found > 0 && (
                                <span className="text-[11px] font-semibold text-stone-500 tabular-nums">{source.leads_found} leads</span>
                              )}
                            </div>
                            <div className="text-[11px] text-stone-400 mt-0.5">
                              {source.source_type?.replace(/_/g, ' ')}
                              {source.priority != null && <> · P{source.priority}</>}
                            </div>
                          </div>
                        </label>
                      )
                    })
                  )}
                </div>
              </>
            )}

            {/* ── SMART MODE ── */}
            {mode === 'smart' && (
              <div className="px-5 pb-2">
                <div className="flex items-center gap-3 p-3.5 rounded-lg bg-amber-50 dark:bg-amber-950/30 border border-amber-200 dark:border-amber-800">
                  <Zap className="w-5 h-5 text-amber-500 flex-shrink-0" />
                  <div>
                    <div className="text-[13px] font-semibold text-stone-800 dark:text-stone-200">
                      {dueSources.length} source{dueSources.length !== 1 ? 's' : ''} due for scraping
                    </div>
                    <div className="text-[11px] text-stone-400 mt-0.5">Based on schedule intervals and last scrape times</div>
                  </div>
                </div>
              </div>
            )}

            {/* ── URL MODE ── */}
            {mode === 'url' && (
              <div className="px-5 pb-2">
                <div className="p-4 rounded-lg border-2 border-dashed border-stone-200 dark:border-stone-700">
                  <label className="text-[11px] font-semibold text-stone-500 uppercase tracking-wider mb-2 block">Article URL</label>
                  <input
                    type="url"
                    value={url}
                    onChange={e => setUrl(e.target.value)}
                    placeholder="https://hoteldive.com/news/new-hotel-2026..."
                    className="w-full px-3 py-2 text-sm border border-stone-200 dark:border-stone-700 rounded-lg bg-white dark:bg-stone-800 text-stone-800 dark:text-stone-200 placeholder:text-stone-400 outline-none focus:border-violet-400 transition"
                    autoFocus
                    onKeyDown={e => e.key === 'Enter' && canStart && handleStart()}
                  />
                  <p className="text-[10px] text-stone-400 mt-1.5">Paste any hotel opening article — we'll extract and score every lead.</p>
                </div>
              </div>
            )}

            {/* ── FOOTER ── */}
            <div className="flex items-center justify-between px-5 py-3 border-t border-stone-100 dark:border-stone-800 mt-auto">
              <span className="text-[11px] text-stone-400 tabular-nums">
                {mode === 'manual' && `${selected.size} sources selected`}
                {mode === 'full'   && `All ${activeSources.length} sources`}
                {mode === 'smart'  && `${dueSources.length} due sources`}
                {mode === 'url'    && 'Direct URL extraction'}
              </span>
              <div className="flex items-center gap-2">
                <button onClick={onClose} className="px-4 py-2 text-xs font-semibold text-stone-500 border border-stone-200 dark:border-stone-700 rounded-lg hover:bg-stone-50 dark:hover:bg-stone-800 transition">
                  Cancel
                </button>
                <button
                  onClick={handleStart}
                  disabled={!canStart}
                  className={cn(
                    'px-5 py-2 text-xs font-bold text-white rounded-lg transition-all shadow-sm flex items-center gap-1.5 active:scale-[0.98]',
                    canStart ? accent.btn : 'bg-stone-200 dark:bg-stone-700 text-stone-400 cursor-not-allowed shadow-none',
                  )}
                >
                  <Rocket className="w-3.5 h-3.5" />
                  {actionLabel}
                </button>
              </div>
            </div>
          </>
        ) : (
          /* ── RUNNING / DONE / ERROR ── */
          <>
            <div className="p-5 flex-1 min-h-0 flex flex-col">
              <div className="flex items-center gap-2 mb-3">
                {status === 'running' && <Loader2 className={cn('w-4 h-4 animate-spin', MODES.find(m => m.key === mode)?.activeText || 'text-emerald-500')} />}
                {status === 'done'    && <CheckCircle2 className="w-4 h-4 text-emerald-500" />}
                {status === 'error'   && <AlertCircle className="w-4 h-4 text-red-500" />}
                <span className="text-xs font-semibold text-stone-800 dark:text-stone-200">
                  {status === 'running' ? (mode === 'url' ? 'Extracting leads...' : 'Scraping in progress...') :
                   status === 'done'    ? (mode === 'url' ? 'Extraction complete' : 'Scrape complete') :
                   'Operation failed'}
                </span>
                {status === 'running' && <span className="ml-auto text-[10px] text-stone-400 tabular-nums">{logs.length} events</span>}
              </div>
              <div ref={logRef} className="bg-stone-950 text-stone-400 rounded-lg p-3 flex-1 overflow-y-auto font-mono text-[11px] leading-relaxed min-h-[220px] max-h-[300px]">
                {logs.map((log, i) => (
                  <div key={i} className={cn(
                    log.includes('Error') || log.includes('error') ? 'text-red-400' :
                    log.includes('✓') || log.includes('complete') || log.includes('found') ? 'text-emerald-400' :
                    log.includes('Starting') || log.includes('—') ? 'text-amber-400' : '',
                  )}>{log}</div>
                ))}
                {status === 'running' && (
                  <span className="inline-flex gap-1 mt-1">
                    <span className="w-1 h-1 rounded-full bg-stone-500 animate-pulse" />
                    <span className="w-1 h-1 rounded-full bg-stone-500 animate-pulse [animation-delay:150ms]" />
                    <span className="w-1 h-1 rounded-full bg-stone-500 animate-pulse [animation-delay:300ms]" />
                  </span>
                )}
              </div>
            </div>
            {(status === 'done' || status === 'error') && (
              <div className="px-5 py-3 border-t border-stone-100 dark:border-stone-800 flex justify-end">
                <button onClick={onClose} className="px-4 py-2 text-xs font-bold bg-stone-900 dark:bg-stone-100 text-white dark:text-stone-900 rounded-lg hover:bg-stone-800 dark:hover:bg-stone-200 transition">
                  Close
                </button>
              </div>
            )}
          </>
        )}
      </div>
    </div>
  )
}