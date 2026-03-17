import { useState, useRef, useEffect } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { X, Radar, Loader2, CheckCircle2, AlertCircle } from 'lucide-react'
import { triggerDiscovery } from '@/api/leads'
import { cn } from '@/lib/utils'

interface Props { onClose: () => void }

export default function DiscoveryModal({ onClose }: Props) {
  const [status, setStatus] = useState<'idle' | 'running' | 'done' | 'error'>('idle')
  const [logs, setLogs] = useState<string[]>([])
  const [queries, setQueries] = useState(10)
  const logRef = useRef<HTMLDivElement>(null)
  const esRef = useRef<EventSource | null>(null)
  const qc = useQueryClient()

  useEffect(() => () => { esRef.current?.close() }, [])
  useEffect(() => { if (logRef.current) logRef.current.scrollTop = logRef.current.scrollHeight }, [logs])

  async function handleStart() {
    setStatus('running')
    setLogs(['Starting source discovery...'])
    try {
      const result = await triggerDiscovery(queries)
      const discoveryId = result?.discovery_id || result?.id
      const token = localStorage.getItem('slh_token')
      const url = `/api/dashboard/discovery/stream${discoveryId ? `?discovery_id=${discoveryId}` : ''}${token ? `${discoveryId ? '&' : '?'}api_key=${token}` : ''}`
      const es = new EventSource(url)
      esRef.current = es
      es.onmessage = (e) => {
        try {
          const d = JSON.parse(e.data)
          if (d.message) setLogs(p => [...p, d.message])
          if (d.status === 'complete' || d.done) { setStatus('done'); es.close(); qc.invalidateQueries({ queryKey: ['stats'] }) }
        } catch { if (e.data && e.data !== 'ping') setLogs(p => [...p, e.data]) }
      }
      es.onerror = () => { es.close(); setStatus('done'); setLogs(p => [...p, '— Stream ended —']); qc.invalidateQueries({ queryKey: ['stats'] }) }
    } catch (err: any) {
      setStatus('error')
      setLogs(p => [...p, `Error: ${err.message || 'Failed to start discovery'}`])
    }
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center modal-backdrop animate-fadeIn" onClick={onClose}>
      <div className="bg-white rounded-xl shadow-2xl w-full max-w-lg mx-4 overflow-hidden animate-scaleIn" onClick={e => e.stopPropagation()}>
        <div className="flex items-center justify-between px-5 py-3.5 border-b border-stone-200 bg-stone-50">
          <div className="flex items-center gap-2">
            <div className="w-7 h-7 rounded-lg bg-violet-100 flex items-center justify-center">
              <Radar className="w-3.5 h-3.5 text-violet-600" />
            </div>
            <h3 className="text-sm font-bold text-navy-900">Source Discovery</h3>
          </div>
          <button onClick={onClose} className="p-1.5 text-stone-400 hover:text-stone-600 rounded-md hover:bg-stone-100 transition">
            <X className="w-4 h-4" />
          </button>
        </div>

        <div className="p-5">
          {status === 'idle' && (
            <div className="space-y-4">
              <div>
                <label className="text-[11px] font-semibold text-stone-500 uppercase tracking-wider mb-2 block">Search Queries</label>
                <div className="flex items-center gap-3">
                  {[5, 10, 20].map(n => (
                    <button
                      key={n}
                      onClick={() => setQueries(n)}
                      className={cn(
                        'px-4 py-2 rounded-lg border-2 text-xs font-semibold transition-all',
                        queries === n ? 'border-violet-400 bg-violet-50 text-violet-700' : 'border-stone-200 text-stone-600 hover:border-stone-300'
                      )}
                    >{n} queries</button>
                  ))}
                </div>
                <p className="text-[10px] text-stone-400 mt-2">Discovers new hospitality news sources via Google News + DuckDuckGo.</p>
              </div>
              <button
                onClick={handleStart}
                className="w-full flex items-center justify-center gap-2 px-4 py-2.5 bg-violet-600 text-white text-sm font-semibold rounded-lg hover:bg-violet-700 transition-all shadow-sm shadow-violet-600/20 active:scale-[0.98]"
              >
                <Radar className="w-4 h-4" />
                Start Discovery
              </button>
            </div>
          )}

          {status !== 'idle' && (
            <div>
              <div className="flex items-center gap-2 mb-3">
                {status === 'running' && <Loader2 className="w-4 h-4 text-violet-500 animate-spin" />}
                {status === 'done' && <CheckCircle2 className="w-4 h-4 text-emerald-500" />}
                {status === 'error' && <AlertCircle className="w-4 h-4 text-coral-500" />}
                <span className="text-xs font-semibold text-navy-800">
                  {status === 'running' ? 'Discovering sources...' : status === 'done' ? 'Discovery complete' : 'Discovery failed'}
                </span>
              </div>
              <div ref={logRef} className="bg-navy-950 text-stone-400 rounded-lg p-3 h-60 overflow-y-auto font-mono text-[11px] leading-relaxed">
                {logs.map((log, i) => (
                  <div key={i} className={cn(
                    log.includes('Error') ? 'text-coral-400' : log.includes('✓') || log.includes('found') ? 'text-emerald-400' : log.includes('—') ? 'text-violet-400' : ''
                  )}>{log}</div>
                ))}
              </div>
            </div>
          )}
        </div>

        {(status === 'done' || status === 'error') && (
          <div className="px-5 py-3 border-t border-stone-200 bg-stone-50 flex justify-end">
            <button onClick={onClose} className="px-4 py-1.5 text-xs font-semibold bg-navy-900 text-white rounded-lg hover:bg-navy-800 transition">Close</button>
          </div>
        )}
      </div>
    </div>
  )
}
