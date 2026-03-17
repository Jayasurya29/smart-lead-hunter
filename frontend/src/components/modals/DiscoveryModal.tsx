import { useState, useRef, useEffect } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { X, Radar, Loader2, CheckCircle2, AlertCircle } from 'lucide-react'
import { triggerDiscovery, createSSEStream } from '@/api/leads'
import type { SSEEvent } from '@/api/types'
import { cn } from '@/lib/utils'

interface Props { onClose: () => void }

export default function DiscoveryModal({ onClose }: Props) {
  const [status, setStatus] = useState<'idle' | 'running' | 'done' | 'error'>('idle')
  const [logs, setLogs] = useState<string[]>([])
  const [mode, setMode] = useState<'quick' | 'full'>('quick')
  const logRef = useRef<HTMLDivElement>(null)
  const esRef = useRef<EventSource | null>(null)
  const qc = useQueryClient()

  useEffect(() => () => { esRef.current?.close() }, [])
  useEffect(() => { if (logRef.current) logRef.current.scrollTop = logRef.current.scrollHeight }, [logs])

  async function handleStart() {
    setStatus('running')
    setLogs(['Starting source discovery...'])
    try {
      const result = await triggerDiscovery(mode)
      const discoveryId = result.discovery_id || ''
      const es = createSSEStream(`/api/dashboard/discovery/stream?discovery_id=${discoveryId}`)
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
        qc.invalidateQueries({ queryKey: ['stats'] })
      }
    } catch (err: any) {
      setStatus('error')
      setLogs(p => [...p, `Error: ${err?.response?.data?.message || err.message || 'Failed to start'}`])
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
            <h3 className="text-sm font-bold text-stone-900">Source Discovery</h3>
          </div>
          <button onClick={onClose} className="p-1.5 text-stone-400 hover:text-stone-600 rounded-md hover:bg-stone-100 transition">
            <X className="w-4 h-4" />
          </button>
        </div>

        <div className="p-5">
          {status === 'idle' && (
            <div className="space-y-4">
              <div>
                <label className="text-[11px] font-semibold text-stone-500 uppercase tracking-wider mb-2 block">Discovery Mode</label>
                <div className="flex items-center gap-3">
                  {[
                    { key: 'quick' as const, label: 'Quick (5 queries)', desc: 'Fast scan' },
                    { key: 'full' as const, label: 'Full (all queries)', desc: 'Thorough search' },
                  ].map(m => (
                    <button key={m.key} onClick={() => setMode(m.key)}
                      className={cn('flex-1 px-4 py-3 rounded-lg border-2 text-left transition-all',
                        mode === m.key ? 'border-violet-400 bg-violet-50' : 'border-stone-200 hover:border-stone-300',
                      )}>
                      <div className={cn('text-xs font-bold', mode === m.key ? 'text-violet-700' : 'text-stone-600')}>{m.label}</div>
                      <div className="text-[10px] text-stone-400 mt-0.5">{m.desc}</div>
                    </button>
                  ))}
                </div>
                <p className="text-[10px] text-stone-400 mt-2">Discovers new hospitality news sources via Google News + DuckDuckGo, then extracts leads from found articles.</p>
              </div>
              <button onClick={handleStart}
                className="w-full flex items-center justify-center gap-2 px-4 py-2.5 bg-violet-600 text-white text-sm font-semibold rounded-lg hover:bg-violet-700 transition-all shadow-sm active:scale-[0.98]">
                <Radar className="w-4 h-4" /> Start Discovery
              </button>
            </div>
          )}

          {status !== 'idle' && (
            <div>
              <div className="flex items-center gap-2 mb-3">
                {status === 'running' && <Loader2 className="w-4 h-4 text-violet-500 animate-spin" />}
                {status === 'done' && <CheckCircle2 className="w-4 h-4 text-emerald-500" />}
                {status === 'error' && <AlertCircle className="w-4 h-4 text-red-500" />}
                <span className="text-xs font-semibold text-stone-800">
                  {status === 'running' ? 'Discovering sources...' : status === 'done' ? 'Discovery complete' : 'Discovery failed'}
                </span>
              </div>
              <div ref={logRef} className="bg-stone-950 text-stone-400 rounded-lg p-3 h-60 overflow-y-auto font-mono text-[11px] leading-relaxed">
                {logs.map((log, i) => (
                  <div key={i} className={cn(
                    log.includes('Error') || log.includes('❌') ? 'text-red-400' :
                    log.includes('✅') || log.includes('found') || log.includes('Saved') ? 'text-emerald-400' :
                    log.includes('Phase') || log.includes('—') || log.includes('═') ? 'text-violet-400' : '',
                  )}>{log}</div>
                ))}
              </div>
            </div>
          )}
        </div>

        {(status === 'done' || status === 'error') && (
          <div className="px-5 py-3 border-t border-stone-200 bg-stone-50 flex justify-end">
            <button onClick={onClose} className="px-4 py-1.5 text-xs font-semibold bg-stone-900 text-white rounded-lg hover:bg-stone-800 transition">Close</button>
          </div>
        )}
      </div>
    </div>
  )
}
