import { useState, useRef, useEffect, useCallback } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { useBackgroundTask } from '@/hooks/useBackgroundTask'
import { X, Radar, Loader2, CheckCircle2, AlertCircle, Square, Minimize2 } from 'lucide-react'
import { triggerDiscovery, cancelDiscovery } from '@/api/leads'
import { cn } from '@/lib/utils'

interface Props { onClose: () => void }

type Status = 'idle' | 'running' | 'done' | 'error'

const MAX_RETRIES = 5

export default function DiscoveryModal({ onClose }: Props) {
  const [status, setStatus] = useState<Status>('idle')
  const [logs, setLogs] = useState<string[]>([])
  const [mode, setMode] = useState<'full' | 'quick'>('full')
  const [extractLeads, setExtractLeads] = useState(true)
  const logRef = useRef<HTMLDivElement>(null)
  const esRef = useRef<EventSource | null>(null)
  const qc = useQueryClient()
  const bg = useBackgroundTask()

  // ── Refs to avoid stale closures in EventSource callbacks ──
  const statusRef = useRef<Status>('idle')
  const retryCountRef = useRef(0)
  const retryTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null)
  const closedRef = useRef(false)
  const receivedDataRef = useRef(false)
  const discoveryIdRef = useRef<string | null>(null)

  useEffect(() => { statusRef.current = status }, [status])

  useEffect(() => {
    return () => {
      closedRef.current = true
      esRef.current?.close()
      if (retryTimerRef.current) clearTimeout(retryTimerRef.current)
    }
  }, [])

  useEffect(() => {
    if (logRef.current) logRef.current.scrollTop = logRef.current.scrollHeight
  }, [logs])

  const addLog = useCallback((msg: string) => {
    setLogs((prev) => [...prev, msg])
  }, [])

  /* ── SSE with retry ── */

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
        // Skip pings
        if (data.type === 'ping') return
        if (data.type === 'stats') return // Stats handled silently
        if (data.message) {
          addLog(data.message)
          bg.addEvent()
        }
        if (data.type === 'complete' || data.done || data.status === 'complete') {
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
        }
        if (data.type === 'error') {
          addLog(`❌ ${data.message}`)
          setStatus('error')
          closedRef.current = true
          es.close()
          bg.failTask(data.message)
        }
      } catch {
        if (e.data && e.data !== 'ping') addLog(e.data)
      }
    }

    es.onerror = () => {
      es.close()
      esRef.current = null

      if (closedRef.current || statusRef.current === 'done' || statusRef.current === 'error') {
        return
      }

      retryCountRef.current++

      if (retryCountRef.current > MAX_RETRIES) {
        if (receivedDataRef.current) {
          setStatus('done')
          addLog('Stream ended — discovery may still be running on server.')
          bg.completeTask({ type: 'discovery', message: 'Stream ended', newLeads: 0, duration: 0 })
          qc.invalidateQueries({ queryKey: ['leads'] })
          qc.invalidateQueries({ queryKey: ['stats'] })
        } else {
          setStatus('error')
          addLog('❌ Lost connection to server after multiple retries.')
          bg.failTask('Connection lost')
        }
        return
      }

      const delay = Math.min(1000 * Math.pow(2, retryCountRef.current - 1), 16000)
      addLog(`Connection interrupted — retrying in ${Math.round(delay / 1000)}s (attempt ${retryCountRef.current}/${MAX_RETRIES})...`)

      retryTimerRef.current = setTimeout(() => {
        if (!closedRef.current && statusRef.current === 'running') {
          createSSE(path)
        }
      }, delay)
    }
  }

  async function handleStart() {
    setStatus('running')
    setLogs(['Starting source discovery...'])
    try {
      const result = await triggerDiscovery(mode, extractLeads)

      if (result?.status === 'error') {
        addLog(`❌ ${result.message || 'Server returned an error'}`)
        setStatus('error')
        return
      }

      const discoveryId = result?.discovery_id || result?.id
      if (!discoveryId) {
        addLog('❌ No discovery ID returned from server')
        setStatus('error')
        return
      }

      discoveryIdRef.current = discoveryId
      const url = `/api/dashboard/discovery/stream?discovery_id=${discoveryId}`
      closedRef.current = false
      receivedDataRef.current = false
      retryCountRef.current = 0
      bg.startTask('discovery')
      createSSE(url)
    } catch (err: any) {
      const detail = err.response?.data?.detail || err.response?.data?.message || err.message
      addLog(`❌ Failed: ${detail}`)
      setStatus('error')
    }
  }

  async function handleStop() {
    const id = discoveryIdRef.current
    if (!id) return

    closedRef.current = true
    esRef.current?.close()
    if (retryTimerRef.current) clearTimeout(retryTimerRef.current)

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
    const id = discoveryIdRef.current
    if (!id) return

    // Close the EventSource — we'll poll instead
    closedRef.current = true
    esRef.current?.close()
    if (retryTimerRef.current) clearTimeout(retryTimerRef.current)

    addLog('Running in background — you\'ll be notified when done.')

    // Start background polling (survives modal unmount)
    bg.startBackgroundPoll(id)

    // Close the modal
    onClose()
  }

  return (
    <div className="fixed inset-0 z-50 modal-backdrop flex items-center justify-center animate-fadeIn">
      <div className="bg-white rounded-2xl shadow-modal w-[480px] max-h-[80vh] flex flex-col overflow-hidden animate-scaleIn">

        {/* Header */}
        <div className="px-5 py-4 flex items-center justify-between border-b border-stone-100">
          <div className="flex items-center gap-3">
            <div className="w-9 h-9 rounded-lg bg-violet-50 flex items-center justify-center">
              <Radar className="w-4.5 h-4.5 text-violet-600" />
            </div>
            <div>
              <h2 className="text-[15px] font-bold text-navy-900">Source Discovery</h2>
              <p className="text-[11px] text-stone-400">Search the web for new hospitality news sources</p>
            </div>
          </div>
          <button onClick={onClose} className="p-1.5 text-stone-400 hover:text-stone-600 rounded-lg hover:bg-stone-100 transition">
            <X className="w-4 h-4" />
          </button>
        </div>

        {/* Body */}
        <div className="flex-1 overflow-y-auto px-5 py-4">
          {status === 'idle' && (
            <div className="space-y-4">
              {/* Mode */}
              <div>
                <label className="block text-[10px] font-bold text-stone-400 uppercase tracking-wider mb-2">Mode</label>
                <div className="flex gap-2">
                  <button
                    onClick={() => setMode('full')}
                    className={cn(
                      'flex-1 px-3 py-2.5 rounded-lg border text-[12px] font-semibold transition',
                      mode === 'full'
                        ? 'border-violet-300 bg-violet-50 text-violet-700'
                        : 'border-stone-200 text-stone-500 hover:border-stone-300',
                    )}
                  >
                    <div>Full Discovery</div>
                    <div className="text-[10px] font-normal mt-0.5 opacity-70">~35 queries, thorough</div>
                  </button>
                  <button
                    onClick={() => setMode('quick')}
                    className={cn(
                      'flex-1 px-3 py-2.5 rounded-lg border text-[12px] font-semibold transition',
                      mode === 'quick'
                        ? 'border-violet-300 bg-violet-50 text-violet-700'
                        : 'border-stone-200 text-stone-500 hover:border-stone-300',
                    )}
                  >
                    <div>Quick Scan</div>
                    <div className="text-[10px] font-normal mt-0.5 opacity-70">~10 queries, faster</div>
                  </button>
                </div>
              </div>

              {/* Extract leads toggle */}
              <label className="flex items-center gap-3 px-3 py-2.5 rounded-lg border border-stone-200 cursor-pointer hover:border-stone-300 transition">
                <input
                  type="checkbox"
                  checked={extractLeads}
                  onChange={(e) => setExtractLeads(e.target.checked)}
                  className="w-4 h-4 rounded border-stone-300 text-violet-600 focus:ring-violet-500"
                />
                <div>
                  <p className="text-[12px] font-semibold text-navy-900">Auto-extract leads</p>
                  <p className="text-[10px] text-stone-400">Run AI extraction on newly discovered sources</p>
                </div>
              </label>
            </div>
          )}

          {/* Running/Done log */}
          {(status === 'running' || status === 'done' || status === 'error') && (
            <div className="space-y-2">
              <div className="flex items-center gap-2">
                {status === 'running' && <Loader2 className="w-4 h-4 text-violet-500 animate-spin" />}
                {status === 'done'    && <CheckCircle2 className="w-4 h-4 text-emerald-500" />}
                {status === 'error'   && <AlertCircle className="w-4 h-4 text-coral-500" />}
                <span className="text-[12px] font-semibold text-navy-900">
                  {status === 'running' ? 'Discovering...' : status === 'done' ? 'Complete' : 'Failed'}
                </span>
              </div>
              <div
                ref={logRef}
                className="bg-navy-950 text-stone-400 rounded-lg p-3 h-52 overflow-y-auto font-mono text-[10px] leading-relaxed"
              >
                {logs.map((log, i) => (
                  <div
                    key={i}
                    className={cn(
                      log.includes('Error') || log.includes('❌') ? 'text-red-400' :
                      log.includes('✅') || log.includes('Added') || log.includes('Found') ? 'text-emerald-400' :
                      log.includes('🔍') || log.includes('Searching') ? 'text-violet-400' :
                      log.includes('⚠') ? 'text-amber-400' :
                      log.includes('retrying') ? 'text-yellow-500' :
                      log.includes('⛔') || log.includes('cancelled') ? 'text-orange-400' :
                      log.includes('background') ? 'text-blue-400' : '',
                    )}
                  >
                    {log}
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="px-5 py-3 border-t border-stone-100 bg-stone-50/50 flex items-center justify-between">
          <div>
            {/* Left: Stop + Background buttons while running */}
            {status === 'running' && (
              <div className="flex items-center gap-2">
                <button
                  onClick={handleStop}
                  className="flex items-center gap-1.5 px-3 py-1.5 text-[11px] font-semibold text-red-600 border border-red-200 rounded-lg hover:bg-red-50 transition"
                >
                  <Square className="w-3 h-3" /> Stop
                </button>
                <button
                  onClick={handleBackground}
                  className="flex items-center gap-1.5 px-3 py-1.5 text-[11px] font-semibold text-blue-600 border border-blue-200 rounded-lg hover:bg-blue-50 transition"
                >
                  <Minimize2 className="w-3 h-3" /> Run in Background
                </button>
              </div>
            )}
          </div>
          <div className="flex items-center gap-2">
            {status === 'idle' && (
              <>
                <button onClick={onClose} className="px-3 py-1.5 text-[11px] font-semibold text-stone-500 hover:text-stone-700 transition">
                  Cancel
                </button>
                <button
                  onClick={handleStart}
                  className="flex items-center gap-1.5 px-4 py-2 text-[11px] font-semibold bg-violet-600 text-white rounded-lg hover:bg-violet-700 transition"
                >
                  <Radar className="w-3 h-3" /> Start Discovery
                </button>
              </>
            )}
            {(status === 'done' || status === 'error') && (
              <button onClick={onClose} className="px-4 py-1.5 text-[11px] font-semibold bg-navy-900 text-white rounded-lg hover:bg-navy-800 transition">
                Close
              </button>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}
