import { useState, useRef, useEffect } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { X, Link2, Loader2, CheckCircle2, AlertCircle } from 'lucide-react'
import api from '@/api/client'
import { cn } from '@/lib/utils'

interface Props { onClose: () => void }

export default function ExtractModal({ onClose }: Props) {
  const [url, setUrl] = useState('')
  const [status, setStatus] = useState<'idle' | 'running' | 'done' | 'error'>('idle')
  const [logs, setLogs] = useState<string[]>([])
  const logRef = useRef<HTMLDivElement>(null)
  const esRef = useRef<EventSource | null>(null)
  const qc = useQueryClient()

  useEffect(() => () => { esRef.current?.close() }, [])
  useEffect(() => { if (logRef.current) logRef.current.scrollTop = logRef.current.scrollHeight }, [logs])

  async function handleExtract() {
    if (!url.trim()) return
    setStatus('running')
    setLogs(['Submitting URL for extraction...'])
    try {
      const { data } = await api.post('/api/dashboard/extract-url', { url: url.trim() })
      const extractId = data?.extract_id || data?.id
      const token = localStorage.getItem('slh_token')
      const streamUrl = `/api/dashboard/extract-url/stream${extractId ? `?extract_id=${extractId}` : ''}${token ? `${extractId ? '&' : '?'}api_key=${token}` : ''}`
      const es = new EventSource(streamUrl)
      esRef.current = es
      es.onmessage = (e) => {
        try {
          const d = JSON.parse(e.data)
          if (d.message) setLogs(p => [...p, d.message])
          if (d.type === 'complete' || d.type === 'error') {
            setStatus(d.type === 'error' ? 'error' : 'done'); es.close()
            qc.invalidateQueries({ queryKey: ['leads'] })
            qc.invalidateQueries({ queryKey: ['stats'] })
          }
        } catch { if (e.data && e.data !== 'ping') setLogs(p => [...p, e.data]) }
      }
      es.onerror = () => { es.close(); setStatus('done'); setLogs(p => [...p, '— Stream ended —']); qc.invalidateQueries({ queryKey: ['leads'] }); qc.invalidateQueries({ queryKey: ['stats'] }) }
    } catch (err: any) {
      setStatus('error')
      setLogs(p => [...p, `Error: ${err.message || 'Failed to extract'}`])
    }
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center modal-backdrop animate-fadeIn" onClick={onClose}>
      <div className="bg-white rounded-xl shadow-2xl w-full max-w-lg mx-4 overflow-hidden animate-scaleIn" onClick={e => e.stopPropagation()}>
        <div className="flex items-center justify-between px-5 py-3.5 border-b border-stone-200 bg-stone-50">
          <div className="flex items-center gap-2">
            <div className="w-7 h-7 rounded-lg bg-blue-100 flex items-center justify-center">
              <Link2 className="w-3.5 h-3.5 text-blue-600" />
            </div>
            <h3 className="text-sm font-bold text-navy-900">Extract Leads from URL</h3>
          </div>
          <button onClick={onClose} className="p-1.5 text-stone-400 hover:text-stone-600 rounded-md hover:bg-stone-100 transition">
            <X className="w-4 h-4" />
          </button>
        </div>

        <div className="p-5">
          {status === 'idle' && (
            <div className="space-y-4">
              <div>
                <label className="text-[11px] font-semibold text-stone-500 uppercase tracking-wider mb-2 block">Article URL</label>
                <input
                  type="url"
                  value={url}
                  onChange={e => setUrl(e.target.value)}
                  placeholder="https://example.com/hotel-openings-2026"
                  className="w-full px-3.5 py-2.5 text-sm border-2 border-stone-200 rounded-lg focus:ring-0 focus:border-blue-400 outline-none transition-colors bg-white"
                  autoFocus
                  onKeyDown={e => e.key === 'Enter' && handleExtract()}
                />
                <p className="text-[10px] text-stone-400 mt-1.5">Paste any article with hotel opening news — we'll extract and score every lead.</p>
              </div>
              <button
                onClick={handleExtract}
                disabled={!url.trim()}
                className="w-full flex items-center justify-center gap-2 px-4 py-2.5 bg-blue-600 text-white text-sm font-semibold rounded-lg hover:bg-blue-700 disabled:bg-stone-300 disabled:text-stone-500 transition-all shadow-sm shadow-blue-600/20 active:scale-[0.98]"
              >
                <Link2 className="w-4 h-4" />
                Extract Leads
              </button>
            </div>
          )}

          {status !== 'idle' && (
            <div>
              <div className="flex items-center gap-2 mb-3">
                {status === 'running' && <Loader2 className="w-4 h-4 text-blue-500 animate-spin" />}
                {status === 'done' && <CheckCircle2 className="w-4 h-4 text-emerald-500" />}
                {status === 'error' && <AlertCircle className="w-4 h-4 text-coral-500" />}
                <span className="text-xs font-semibold text-navy-800">
                  {status === 'running' ? 'Extracting leads...' : status === 'done' ? 'Extraction complete' : 'Extraction failed'}
                </span>
              </div>
              <div ref={logRef} className="bg-navy-950 text-stone-400 rounded-lg p-3 h-60 overflow-y-auto font-mono text-[11px] leading-relaxed">
                {logs.map((log, i) => (
                  <div key={i} className={cn(log.includes('Error') ? 'text-coral-400' : log.includes('✓') ? 'text-emerald-400' : log.includes('—') ? 'text-blue-400' : '')}>{log}</div>
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
