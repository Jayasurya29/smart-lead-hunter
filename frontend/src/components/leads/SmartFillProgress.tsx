/**
 * SmartFillProgress
 * ══════════════════
 * Live progress UI for Smart Fill + Full Refresh (shared component).
 *
 * Subscribes to GET /api/leads/{leadId}/smart-fill-stream?mode=smart|full
 * and renders an honest progress bar:
 *   - Percentage computed from stage / total (never lies)
 *   - Elapsed seconds ticker
 *   - 8-stage checklist
 *
 * Usage:
 *   <SmartFillProgress
 *     leadId={1225}
 *     mode="full"
 *     onComplete={(summary) => { refetch(); showToast(...) }}
 *     onCancel={() => setShowProgress(false)}
 *   />
 *
 * The 8 stages mirror the SSE endpoint in scraping.py (smart_fill_stream).
 * Update STAGE_LABELS if stages change in the backend.
 */

import { useEffect, useRef, useState } from 'react'
import { CheckCircle2, Loader2, AlertCircle, X, Zap } from 'lucide-react'
import { cn } from '@/lib/utils'

const STAGE_LABELS = [
  'Classifying project type',
  'Building targeted queries',
  'Searching web (Serper)',
  'Extracting data fields',
  'Extracting mgmt / owner / developer',
  'Extracting street address',
  'Saving changes to database',
  'Rescoring lead',
]

interface CompleteSummary {
  status: 'enriched' | 'no_data' | 'expired'
  changes?: string[]
  confidence?: string
  duration_s?: number
  message?: string
}

interface Props {
  leadId: number
  mode: 'smart' | 'full'
  onComplete: (summary: CompleteSummary) => void
  onCancel: () => void
  // Path Y → 1B (2026-04-28): URL prefix to support both potential_leads
  // and existing_hotels. Default keeps original behavior.
  // For existing hotels pass basePath="/api/existing-hotels".
  basePath?: string
}

export default function SmartFillProgress({
  leadId,
  mode,
  onComplete,
  onCancel,
  basePath = '/api/leads',
}: Props) {
  const [currentStage, setCurrentStage] = useState(0)
  const [currentLabel, setCurrentLabel] = useState<string>('Connecting...')
  const [pct, setPct] = useState(0)
  const [elapsed, setElapsed] = useState(0)
  const [status, setStatus] = useState<'running' | 'done' | 'error'>('running')
  const [errorMsg, setErrorMsg] = useState<string>('')
  const esRef = useRef<EventSource | null>(null)
  const elapsedTimerRef = useRef<ReturnType<typeof setInterval> | null>(null)
  const onCompleteRef = useRef(onComplete)

  useEffect(() => { onCompleteRef.current = onComplete }, [onComplete])

  // Anchor elapsed time to the backend's job start (via elapsed_s in
  // each event), not the component mount time. Without this, remounting
  // the component (tab switch, etc.) resets the displayed counter to 0
  // even though the actual job has been running for a while. See
  // EnrichProgress.tsx for the same fix + full reasoning.
  const jobStartedAtRef = useRef<number | null>(null)

  useEffect(() => {
    elapsedTimerRef.current = setInterval(() => {
      if (jobStartedAtRef.current !== null) {
        setElapsed(Math.round((Date.now() - jobStartedAtRef.current) / 1000))
      }
    }, 1000)
    return () => {
      if (elapsedTimerRef.current) clearInterval(elapsedTimerRef.current)
    }
  }, [])

  useEffect(() => {
    const es = new EventSource(
      `${basePath}/${leadId}/smart-fill-stream?mode=${mode}`,
      { withCredentials: true },
    )
    esRef.current = es

    es.onmessage = (e) => {
      try {
        const data = JSON.parse(e.data)
        if (data.type === 'ping' || data.type === 'started') return
        if (data.type === 'stage') {
          setCurrentStage(data.stage)
          setCurrentLabel(data.label)
          setPct(data.pct)
          // Trust backend's elapsed_s as truth — sets the anchor for
          // the local 1-second ticker.
          if (typeof data.elapsed_s === 'number') {
            jobStartedAtRef.current = Date.now() - (data.elapsed_s * 1000)
            setElapsed(Math.round(data.elapsed_s))
          }
          return
        }
        if (data.type === 'complete') {
          setPct(100)
          setCurrentStage(STAGE_LABELS.length)
          setStatus('done')
          if (typeof data.elapsed_s === 'number') {
            setElapsed(Math.round(data.elapsed_s))
          }
          es.close()
          setTimeout(() => onCompleteRef.current(data.summary), 500)
          return
        }
        if (data.type === 'error') {
          setStatus('error')
          setErrorMsg(data.message || 'Enrichment failed')
          es.close()
          return
        }
      } catch {
        // Malformed event, ignore
      }
    }

    es.onerror = () => {
      if (status === 'done') return
    }

    return () => {
      es.close()
      esRef.current = null
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [leadId, mode])

  const label = mode === 'full' ? 'Full Refresh' : 'Smart Fill'
  const accent = mode === 'full' ? 'emerald' : 'violet'

  return (
    <div className={cn(
      'rounded-lg border p-4',
      mode === 'full'
        ? 'border-emerald-200 bg-gradient-to-br from-emerald-50/80 to-white'
        : 'border-violet-200 bg-gradient-to-br from-violet-50/80 to-white',
    )}>
      {/* Header row */}
      <div className="flex items-start justify-between mb-3">
        <div className="flex-1">
          <div className="flex items-center gap-2">
            {status === 'running' && <Loader2 className={cn(
              'w-4 h-4 animate-spin',
              mode === 'full' ? 'text-emerald-600' : 'text-violet-600',
            )} />}
            {status === 'done' && <CheckCircle2 className="w-4 h-4 text-emerald-500" />}
            {status === 'error' && <AlertCircle className="w-4 h-4 text-red-500" />}
            <h4 className="text-[13px] font-bold text-navy-900 flex items-center gap-1.5">
              <Zap className="w-3.5 h-3.5" />
              {status === 'running' ? `Running ${label}...` :
               status === 'done' ? `${label} complete` :
               `${label} failed`}
            </h4>
          </div>
          <p className="text-[11px] text-stone-500 mt-0.5">
            {status === 'error' ? errorMsg : currentLabel}
          </p>
        </div>

        <div className="flex items-center gap-2 ml-3">
          <div className="text-right">
            <div className={cn(
              'text-[18px] font-bold tabular-nums leading-none',
              mode === 'full' ? 'text-emerald-700' : 'text-violet-700',
            )}>
              {pct}<span className={cn(
                'text-[11px]',
                mode === 'full' ? 'text-emerald-400' : 'text-violet-400',
              )}>%</span>
            </div>
            <div className="text-[10px] text-stone-400 tabular-nums mt-0.5">
              {elapsed}s elapsed
            </div>
          </div>
          {status !== 'running' && (
            <button
              onClick={onCancel}
              className="p-1 text-stone-400 hover:text-stone-700 hover:bg-stone-100 rounded transition"
              title="Close"
            >
              <X className="w-3.5 h-3.5" />
            </button>
          )}
        </div>
      </div>

      {/* Progress bar */}
      <div className="relative w-full h-2 bg-stone-100 rounded-full overflow-hidden mb-3">
        <div
          className={cn(
            'h-full transition-all duration-500 ease-out',
            status === 'error' ? 'bg-red-500' :
            status === 'done' ? 'bg-emerald-500' :
            mode === 'full'
              ? 'bg-gradient-to-r from-emerald-500 to-emerald-600'
              : 'bg-gradient-to-r from-violet-500 to-violet-600',
          )}
          style={{ width: `${pct}%` }}
        />
      </div>

      {/* Stage checklist */}
      <div className="grid grid-cols-1 gap-0.5 mt-2">
        {STAGE_LABELS.map((stageLabel, idx) => {
          const stageNum = idx + 1
          const state =
            status === 'error' ? 'pending' :
            stageNum < currentStage ? 'done' :
            stageNum === currentStage ? (status === 'done' ? 'done' : 'running') :
            'pending'
          return (
            <div
              key={stageLabel}
              className={cn(
                'flex items-center gap-2 px-2 py-1 rounded text-[11px] transition',
                state === 'done' && 'text-emerald-600',
                state === 'running' && 'font-semibold',
                state === 'running' && mode === 'full' && 'text-emerald-700 bg-emerald-50',
                state === 'running' && mode === 'smart' && 'text-violet-700 bg-violet-50',
                state === 'pending' && 'text-stone-400',
              )}
            >
              {state === 'done' && <CheckCircle2 className="w-3 h-3 flex-shrink-0" />}
              {state === 'running' && <Loader2 className="w-3 h-3 animate-spin flex-shrink-0" />}
              {state === 'pending' && (
                <span className="w-3 h-3 rounded-full border border-stone-300 flex-shrink-0" />
              )}
              <span className="truncate">{stageLabel}</span>
            </div>
          )
        })}
      </div>
    </div>
  )
}
