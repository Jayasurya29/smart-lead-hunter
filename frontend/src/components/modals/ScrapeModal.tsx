import { useState, useRef, useEffect } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { useBackgroundTask } from '@/hooks/useBackgroundTask'
import {
  X, Play, Loader2, CheckCircle2, AlertCircle,
  Zap, RotateCcw, Link2, Globe,
} from 'lucide-react'
import { triggerScrape, triggerExtractUrl, fetchSources } from '@/api/leads'
import { cn } from '@/lib/utils'

interface Props { onClose: () => void }

type Mode = 'smart' | 'full' | 'url'

export default function ScrapeModal({ onClose }: Props) {
  const [mode, setMode] = useState<Mode>('smart')
  const [status, setStatus] = useState<'idle' | 'running' | 'done' | 'error'>('idle')
  const [logs, setLogs] = useState<string[]>([])
  const [sources, setSources] = useState<any[]>([])
  const [selectedSources, setSelectedSources] = useState<number[]>([])
  const [extractUrl, setExtractUrl] = useState('')
  const logRef = useRef<HTMLDivElement>(null)
  const esRef = useRef<EventSource | null>(null)
  const qc = useQueryClient()
  const bg = useBackgroundTask()

  /* Load sources on mount */
  useEffect(() => {
    fetchSources()
      .then((data) => {
        const list = Array.isArray(data) ? data : data?.sources || []
        setSources(list)
      })
      .catch(() => {})
    return () => { esRef.current?.close() }
  }, [])

  /* Auto-scroll log */
  useEffect(() => {
    if (logRef.current) logRef.current.scrollTop = logRef.current.scrollHeight
  }, [logs])

  function toggleSource(id: number) {
    setSelectedSources((prev) =>
      prev.includes(id) ? prev.filter((x) => x !== id) : [...prev, id],
    )
  }

  function addLog(msg: string) {
    setLogs((prev) => [...prev, msg])
  }

  /* Connect SSE stream */
  function connectSSE(path: string, taskType: 'scrape' | 'extract') {
    const es = new EventSource(path)
    esRef.current = es
    bg.startTask(taskType)

    es.onmessage = (e) => {
      try {
        const data = JSON.parse(e.data)
        if (data.message) {
          addLog(data.message)
          bg.addEvent()
        }
        if (data.type === 'complete' || data.done || data.status === 'complete') {
          setStatus('done')
          es.close()
          const newLeads = data.stats?.leads_saved ?? data.stats?.leads_found ?? 0
          const duration = data.duration_seconds ?? 0
          bg.completeTask({
            type: taskType,
            message: taskType === 'extract' ? 'URL extraction complete' : 'Scrape complete',
            newLeads,
            duration,
          })
          qc.invalidateQueries({ queryKey: ['leads'] })
          qc.invalidateQueries({ queryKey: ['stats'] })
        }
        if (data.type === 'error') {
          addLog(`❌ ${data.message}`)
          setStatus('error')
          es.close()
          bg.failTask(data.message)
        }
      } catch {
        if (e.data && e.data !== 'ping') addLog(e.data)
      }
    }

    es.onerror = () => {
      es.close()
      if (status === 'running') {
        setStatus('done')
        addLog('Stream ended.')
        bg.completeTask({ type: taskType, message: 'Stream ended', newLeads: 0, duration: 0 })
      }
    }
  }

  /* Start scrape */
  async function handleStartScrape() {
    setStatus('running')
    setLogs([`Starting ${mode} scrape...`])
    try {
      const result = await triggerScrape(mode, selectedSources)
      const scrapeId = result?.scrape_id || result?.id
      const url = `/api/dashboard/scrape/stream${scrapeId ? `?scrape_id=${scrapeId}` : ''}`
      connectSSE(url, 'scrape')
      onClose()
    } catch (err: any) {
      addLog(`❌ Failed to start: ${err.message}`)
      setStatus('error')
    }
  }

  /* Start URL extract */
  async function handleStartExtract() {
    if (!extractUrl.trim()) return
    setStatus('running')
    setLogs([`Extracting from: ${extractUrl}`])
    try {
      const result = await triggerExtractUrl(extractUrl.trim())
      const extractId = result?.extract_id || result?.id
      const url = `/api/dashboard/extract-url/stream${extractId ? `?extract_id=${extractId}` : ''}`
      connectSSE(url, 'extract')
      onClose()
    } catch (err: any) {
      addLog(`❌ Failed: ${err.message}`)
      setStatus('error')
    }
  }

  const isRunning = status === 'running'

  return (
    <div className="fixed inset-0 z-50 modal-backdrop flex items-center justify-center animate-fadeIn">
      <div className="bg-white rounded-2xl shadow-modal w-[520px] max-h-[85vh] flex flex-col overflow-hidden animate-scaleIn">

        {/* ── Header ── */}
        <div className="px-5 py-4 flex items-center justify-between border-b border-stone-100">
          <div>
            <h2 className="text-[15px] font-bold text-navy-900">
              {mode === 'url' ? 'Extract from URL' : 'Run Scrape'}
            </h2>
            <p className="text-[11px] text-stone-400 mt-0.5">
              {mode === 'smart' ? 'Scrape sources due for refresh' :
               mode === 'full'  ? 'Full sweep of all active sources' :
               'Extract leads from a specific article URL'}
            </p>
          </div>
          <button onClick={onClose} className="p-1.5 text-stone-400 hover:text-stone-600 rounded-lg hover:bg-stone-100 transition">
            <X className="w-4 h-4" />
          </button>
        </div>

        {/* ── Mode tabs ── */}
        <div className="px-5 pt-3 flex gap-1">
          {([
            { key: 'smart', label: 'Smart', icon: Zap },
            { key: 'full',  label: 'Full Sweep', icon: RotateCcw },
            { key: 'url',   label: 'URL Extract', icon: Link2 },
          ] as { key: Mode; label: string; icon: React.ElementType }[]).map((m) => {
            const Icon = m.icon
            return (
              <button
                key={m.key}
                onClick={() => !isRunning && setMode(m.key)}
                disabled={isRunning}
                className={cn(
                  'flex items-center gap-1.5 px-3 py-1.5 text-[11px] font-semibold rounded-md transition',
                  mode === m.key
                    ? 'bg-navy-900 text-white'
                    : 'text-stone-500 hover:bg-stone-100 disabled:opacity-50',
                )}
              >
                <Icon className="w-3 h-3" /> {m.label}
              </button>
            )
          })}
        </div>

        {/* ── Body ── */}
        <div className="flex-1 overflow-y-auto px-5 py-3">

          {/* URL input (url mode) */}
          {mode === 'url' && status === 'idle' && (
            <div className="space-y-3">
              <div>
                <label className="block text-[10px] font-bold text-stone-400 uppercase tracking-wider mb-1">Article URL</label>
                <div className="relative">
                  <Globe className="absolute left-3 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-stone-400" />
                  <input
                    type="url"
                    value={extractUrl}
                    onChange={(e) => setExtractUrl(e.target.value)}
                    placeholder="https://lodgingmagazine.com/..."
                    className="w-full h-9 pl-9 pr-3 text-[12px] bg-stone-50 border border-stone-200 rounded-lg outline-none focus:border-navy-400 focus:ring-1 focus:ring-navy-200 transition"
                  />
                </div>
              </div>
              <p className="text-[10px] text-stone-400">
                Paste a hotel news article URL. The AI will extract any new hotel leads from it.
              </p>
            </div>
          )}

          {/* Source list (smart/full mode) */}
          {mode !== 'url' && status === 'idle' && (
            <div className="space-y-2" style={{ maxHeight: '320px', overflowY: 'auto' }}>
              {mode === 'full' && (
                <div className="bg-gold-50 border border-gold-200 rounded-lg p-3 mb-2">
                  <p className="text-[11px] font-semibold text-gold-700">
                    ⚠️ Full sweep will scrape all {sources.filter((s) => s.is_active !== false).length} active sources
                  </p>
                  <p className="text-[10px] text-gold-600 mt-0.5">This may take 30-90 minutes.</p>
                </div>
              )}
              {mode === 'smart' && sources.length === 0 && (
                <div className="text-center py-8 text-stone-400">
                  <p className="text-sm font-medium">Loading sources...</p>
                </div>
              )}
              {mode === 'smart' && sources.length > 0 && (
                <>
                  <p className="text-[10px] text-stone-400 font-medium">
                    {selectedSources.length > 0
                      ? `${selectedSources.length} source${selectedSources.length !== 1 ? 's' : ''} selected`
                      : 'Select specific sources or start to scrape all due sources'}
                  </p>
                  {sources.filter((s) => s.is_active !== false).map((s) => (
                    <label
                      key={s.id}
                      className={cn(
                        'flex items-center gap-3 px-3 py-2 rounded-lg border cursor-pointer transition',
                        selectedSources.includes(s.id)
                          ? 'border-navy-300 bg-navy-50'
                          : 'border-stone-100 hover:border-stone-200',
                      )}
                    >
                      <input
                        type="checkbox"
                        checked={selectedSources.includes(s.id)}
                        onChange={() => toggleSource(s.id)}
                        className="w-3.5 h-3.5 rounded border-stone-300 text-navy-600 focus:ring-navy-500"
                      />
                      <div className="flex-1 min-w-0">
                        <p className="text-[12px] font-semibold text-navy-900 truncate">{s.name}</p>
                        {s.url && <p className="text-[10px] text-stone-400 truncate">{s.url}</p>}
                      </div>
                      {s.gold_url_count > 0 && (
                        <span className="text-[10px] text-gold-600 font-medium">⭐ {s.gold_url_count}</span>
                      )}
                    </label>
                  ))}
                </>
              )}
            </div>
          )}

          {/* Running/Done log */}
          {(status === 'running' || status === 'done' || status === 'error') && (
            <div className="space-y-2">
              <div className="flex items-center gap-2">
                {status === 'running' && <Loader2 className="w-4 h-4 text-emerald-500 animate-spin" />}
                {status === 'done'    && <CheckCircle2 className="w-4 h-4 text-emerald-500" />}
                {status === 'error'   && <AlertCircle className="w-4 h-4 text-coral-500" />}
                <span className="text-[12px] font-semibold text-navy-900">
                  {status === 'running' ? 'Processing...' : status === 'done' ? 'Complete' : 'Failed'}
                </span>
              </div>
              <div
                ref={logRef}
                className="bg-navy-950 text-stone-400 rounded-lg p-3 h-60 overflow-y-auto font-mono text-[10px] leading-relaxed"
              >
                {logs.map((log, i) => (
                  <div
                    key={i}
                    className={cn(
                      log.includes('Error') || log.includes('❌') ? 'text-red-400' :
                      log.includes('✅') || log.includes('Saved') || log.includes('complete') || log.includes('found') ? 'text-emerald-400' :
                      log.includes('Phase') || log.includes('Starting') || log.includes('━') ? 'text-amber-400' :
                      '',
                    )}
                  >
                    {log}
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>

        {/* ── Footer ── */}
        <div className="px-5 py-3 border-t border-stone-100 bg-stone-50/50 flex items-center justify-end gap-2">
          {status === 'idle' && (
            <>
              <button onClick={onClose} className="px-3 py-1.5 text-[11px] font-semibold text-stone-500 hover:text-stone-700 transition">
                Cancel
              </button>
              {mode === 'url' ? (
                <button
                  onClick={handleStartExtract}
                  disabled={!extractUrl.trim()}
                  className="flex items-center gap-1.5 px-4 py-2 text-[11px] font-semibold bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition disabled:opacity-40"
                >
                  <Link2 className="w-3 h-3" /> Extract
                </button>
              ) : (
                <button
                  onClick={handleStartScrape}
                  className="flex items-center gap-1.5 px-4 py-2 text-[11px] font-semibold bg-emerald-600 text-white rounded-lg hover:bg-emerald-700 transition"
                >
                  <Play className="w-3 h-3" /> Start {mode === 'smart' ? 'Smart' : 'Full'} Scrape
                </button>
              )}
            </>
          )}
          {(status === 'done' || status === 'error') && (
            <button
              onClick={onClose}
              className="px-4 py-1.5 text-[11px] font-semibold bg-navy-900 text-white rounded-lg hover:bg-navy-800 transition"
            >
              Close
            </button>
          )}
        </div>
      </div>
    </div>
  )
}
