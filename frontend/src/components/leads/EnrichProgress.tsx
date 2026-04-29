/**
 * EnrichProgress
 * ═══════════════
 * Live progress UI for the 9-stage contact enrichment pipeline.
 *
 * Subscribes to GET /api/dashboard/leads/{leadId}/enrich-stream (SSE),
 * renders a honest progress bar:
 *   - Percentage computed from stage / total (never lies, never stalls)
 *   - Elapsed seconds ticker
 *   - Stage checklist showing completed ✓ / current ⟳ / pending
 *
 * Usage:
 *   <EnrichProgress
 *     leadId={1225}
 *     onComplete={(summary) => { refetchContacts(); showToast(...) }}
 *     onCancel={() => setShowProgress(false)}
 *   />
 *
 * The 9 stages mirror iterative_researcher.run_iterative_research().
 * Update STAGE_LABELS if you add/remove iterations in the backend.
 */

import { useEffect, useRef, useState } from 'react'
import { CheckCircle2, Loader2, AlertCircle, X, Square } from 'lucide-react'
import { cn } from '@/lib/utils'
import api from '@/api/client'

const STAGE_LABELS = [
  'Iter 1 · Discovery',
  'Iter 2 · GM hunt',
  'Iter 2.5 · Department heads',
  'Iter 3 · Corporate hunt',
  'Iter 4 · LinkedIn lookup',
  'Iter 5 · Verify current role',
  'Iter 5.5 · Regional fit',
  'Iter 6 · Gemini strategist',
  'Iter 6.5 · Employment verify',
  'Verifying contact scope',
  'Saving & scoring contacts',
]

interface CompleteSummary {
  contacts_saved: number
  contacts_rejected: number
  duration_s: number
  should_reject?: boolean
  rejection_reason?: string
}

interface Props {
  leadId: number
  onComplete: (summary: CompleteSummary) => void
  onCancel: () => void
  // Path Y → 1B (2026-04-28): allow rendering this component for either
  // potential_leads OR existing_hotels by parameterizing the URL prefix.
  // Default keeps the original behavior — the lead variant.
  // For existing hotels pass basePath="/api/existing-hotels".
  basePath?: string
}

export default function EnrichProgress({
  leadId,
  onComplete,
  onCancel,
  basePath = '/api/dashboard/leads',
}: Props) {
  const [currentStage, setCurrentStage] = useState(0)  // 1-indexed, 0 = not yet started
  const [currentLabel, setCurrentLabel] = useState<string>('Connecting...')
  const [pct, setPct] = useState(0)
  const [elapsed, setElapsed] = useState(0)
  const [status, setStatus] = useState<'running' | 'done' | 'error'>('running')
  const [errorMsg, setErrorMsg] = useState<string>('')
  // Collapsed by default — the 11-stage checklist takes ~280px which would
  // push tab content out of view. User can click to expand for the full
  // detailed checklist when they want to see exactly which iteration runs.
  const [collapsed, setCollapsed] = useState(true)
  const esRef = useRef<EventSource | null>(null)
  const elapsedTimerRef = useRef<ReturnType<typeof setInterval> | null>(null)
  const onCompleteRef = useRef(onComplete)

  // Keep the onComplete callback up-to-date via ref so we don't re-subscribe
  // the SSE stream every render.
  useEffect(() => { onCompleteRef.current = onComplete }, [onComplete])

  // Local elapsed counter. The backend's `elapsed_s` is the SOURCE OF
  // TRUTH (it's based on when the actual job started server-side).
  // We use a ref to hold the OFFSET between Date.now() and the backend's
  // job start, so the local 1-second ticker reads from the correct anchor.
  //
  // Without this, the elapsed time RESETS to 0 every time the user
  // navigates away and back (component remounts → fresh `startedAt` →
  // counter starts over). With this anchor, the counter reflects the
  // real job-elapsed time across remounts.
  const jobStartedAtRef = useRef<number | null>(null)

  useEffect(() => {
    elapsedTimerRef.current = setInterval(() => {
      // If the backend has told us the job's start time, use it.
      // Otherwise fall back to "0 seconds" until the first event arrives.
      if (jobStartedAtRef.current !== null) {
        setElapsed(Math.round((Date.now() - jobStartedAtRef.current) / 1000))
      }
    }, 1000)
    return () => {
      if (elapsedTimerRef.current) clearInterval(elapsedTimerRef.current)
    }
  }, [])

  useEffect(() => {
    // Subscribe to the SSE stream — basePath dictates which parent kind
    // (lead or hotel) we're enriching. Both endpoints emit the same
    // event shape, so the UI logic below is identical for both.
    const es = new EventSource(
      `${basePath}/${leadId}/enrich-stream`,
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
          // Trust the backend's elapsed_s as the truth. Compute the
          // job's wall-clock start time from it and store as our anchor
          // so the local ticker can extrapolate forward smoothly between
          // events. This keeps the displayed elapsed time consistent
          // across navigate-away-and-back cycles.
          if (typeof data.elapsed_s === 'number') {
            const backendElapsedMs = data.elapsed_s * 1000
            jobStartedAtRef.current = Date.now() - backendElapsedMs
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
          // Give the UI a tick to render 100% before closing
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
      // EventSource auto-retries on error. If we're already done, it's a
      // benign disconnect after the `complete` event; don't show an error.
      if (status === 'done') return
      // Otherwise leave it alone — the browser will reconnect and the
      // backend task keeps running.
    }

    return () => {
      es.close()
      esRef.current = null
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [leadId])

  return (
    <div className="rounded-lg border border-violet-200 bg-gradient-to-br from-violet-50/80 to-white p-4">
      {/* Header row */}
      <div className="flex items-start justify-between mb-3">
        <div className="flex-1">
          <div className="flex items-center gap-2">
            {status === 'running' && <Loader2 className="w-4 h-4 text-violet-600 animate-spin" />}
            {status === 'done' && <CheckCircle2 className="w-4 h-4 text-emerald-500" />}
            {status === 'error' && <AlertCircle className="w-4 h-4 text-red-500" />}
            <h4 className="text-[13px] font-bold text-navy-900">
              {status === 'running' ? 'Enriching contacts...' :
               status === 'done' ? 'Enrichment complete' :
               'Enrichment failed'}
            </h4>
          </div>
          <p className="text-[11px] text-stone-500 mt-0.5">
            {status === 'error' ? errorMsg : currentLabel}
          </p>
        </div>

        <div className="flex items-center gap-2 ml-3">
          <div className="text-right">
            <div className="text-[18px] font-bold text-violet-700 tabular-nums leading-none">
              {pct}<span className="text-[11px] text-violet-400">%</span>
            </div>
            <div className="text-[10px] text-stone-400 tabular-nums mt-0.5">
              {elapsed}s elapsed
            </div>
          </div>
          {status === 'running' && (
            <button
              onClick={async () => {
                try {
                  // POST against basePath so we hit the right endpoint:
                  //   /api/dashboard/leads/{id}/enrich-cancel  (lead variant)
                  //   /api/existing-hotels/{id}/enrich-cancel  (hotel variant)
                  // Backend will emit a final cancelled error event;
                  // the SSE handler will pick it up and flip status.
                  await api.post(`${basePath}/${leadId}/enrich-cancel`)
                } catch (err) {
                  console.error('Cancel enrichment failed:', err)
                }
              }}
              className="px-2 h-6 text-[10px] font-semibold text-red-600 bg-red-50 border border-red-200 rounded-md hover:bg-red-100 transition flex items-center gap-1"
              title="Stop enrichment"
            >
              <Square className="w-3 h-3" fill="currentColor" />
              Stop
            </button>
          )}
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
            'bg-gradient-to-r from-violet-500 to-violet-600',
          )}
          style={{ width: `${pct}%` }}
        />
      </div>

      {/* Stage checklist — collapsed by default, can be expanded via state if needed */}
      <div className="grid grid-cols-1 gap-0.5 mt-2">
        {STAGE_LABELS.map((label, idx) => {
          const stageNum = idx + 1
          const state =
            status === 'error' ? 'pending' :
            stageNum < currentStage ? 'done' :
            stageNum === currentStage ? (status === 'done' ? 'done' : 'running') :
            'pending'
          return (
            <div
              key={label}
              className={cn(
                'flex items-center gap-2 px-2 py-1 rounded text-[11px] transition',
                state === 'done' && 'text-emerald-600',
                state === 'running' && 'text-violet-700 font-semibold bg-violet-50',
                state === 'pending' && 'text-stone-400',
              )}
            >
              {state === 'done' && <CheckCircle2 className="w-3 h-3 flex-shrink-0" />}
              {state === 'running' && <Loader2 className="w-3 h-3 animate-spin flex-shrink-0" />}
              {state === 'pending' && (
                <span className="w-3 h-3 rounded-full border border-stone-300 flex-shrink-0" />
              )}
              <span className="truncate">{label}</span>
            </div>
          )
        })}
      </div>
    </div>
  )
}
