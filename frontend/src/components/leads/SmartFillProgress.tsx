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
  status: 'enriched' | 'no_data' | 'expired' | 'transferred' | 'merged'
  changes?: string[]
  confidence?: string
  duration_s?: number
  message?: string
  // Set when Smart Fill auto-graduated the lead → existing_hotels.
  // The parent (LeadDetail) checks this flag to skip refetching the
  // now-deleted lead and close the panel cleanly.
  auto_transferred?: boolean
  existing_hotel_id?: number
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
  const [status, setStatus] = useState<'running' | 'done' | 'error' | 'graduated'>('running')
  const [errorMsg, setErrorMsg] = useState<string>('')
  // When Smart Fill auto-transfers the lead → existing_hotels, we
  // capture the new ID so the success card can show "Open in Existing
  // Hotels" deep link.
  const [graduatedToHotelId, setGraduatedToHotelId] = useState<number | null>(null)
  // Captures the summary payload from `complete` event so the success
  // card can show what fields changed.
  const [changeSummary, setChangeSummary] = useState<any>(null)
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
    const es = new EventSource(`${basePath}/${leadId}/smart-fill-stream?mode=${mode}`)
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
          setChangeSummary(data.summary || null)
          if (typeof data.elapsed_s === 'number') {
            setElapsed(Math.round(data.elapsed_s))
          }
          es.close()
          // Hold success state visible for 1.5s so user clearly sees
          // "Done!" before the parent closes the card. Was 500ms which
          // looked like a flash. 1.5s feels intentional.
          setTimeout(() => onCompleteRef.current(data.summary), 1500)
          return
        }
        if (data.type === 'auto_transferred') {
          // Smart Fill discovered the lead's opening_date is now in
          // the past or <3 months out. The lead has been moved to
          // existing_hotels and DELETED from potential_leads. Show a
          // dedicated success state explaining what happened (rather
          // than the lead silently disappearing).
          setPct(100)
          setCurrentStage(STAGE_LABELS.length)
          setStatus('graduated')
          setGraduatedToHotelId(data.existing_hotel_id || null)
          es.close()
          // Hold the graduated state visible for 2.5s so user reads
          // the message and sees "Open in Existing Hotels" button
          setTimeout(() => onCompleteRef.current({
            auto_transferred: true,
            existing_hotel_id: data.existing_hotel_id,
            status: data.status,
          }), 2500)
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
      // EventSource emits onerror when the SSE connection breaks. The
      // backend's job-with-subscribers pattern means the underlying
      // task continues — a future re-mount of this component will
      // re-attach via the auto-attach effect in LeadDetail. So we
      // don't need to do anything here.
    }

    return () => {
      es.close()
      esRef.current = null
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [leadId, mode])

  const label = mode === 'full' ? 'Full Refresh' : 'Smart Fill'

  // ── GRADUATED state — special card explaining what happened ─────
  // Shown when Smart Fill discovers the lead's opening_date is now
  // in the past or <3 months out and auto-transfers to existing_hotels.
  // Without this dedicated card, the lead would silently disappear
  // from new-hotels and reps wouldn't know where it went.
  if (status === 'graduated') {
    return (
      <div className="rounded-lg border-2 border-emerald-300 bg-gradient-to-br from-emerald-50 to-white p-5 animate-fadeIn">
        <div className="flex flex-col items-center text-center mb-4">
          <div className="w-14 h-14 rounded-full bg-emerald-500 flex items-center justify-center mb-3 shadow-sm">
            <CheckCircle2 className="w-8 h-8 text-white" strokeWidth={2.5} />
          </div>
          <h3 className="text-base font-bold text-navy-900 mb-1">
            Lead graduated to Existing Hotels
          </h3>
          <p className="text-xs text-stone-600 max-w-sm">
            Smart Fill found this hotel's opening date is now under 3 months away,
            so it moved to the Existing Hotels pipeline automatically.
          </p>
        </div>
        {graduatedToHotelId && (
          <a
            href={`/existing-hotels?hotel=${graduatedToHotelId}`}
            className="block w-full px-3 py-2 text-sm font-semibold text-white bg-emerald-600 rounded-md hover:bg-emerald-700 transition text-center"
          >
            View in Existing Hotels →
          </a>
        )}
        <p className="text-2xs text-stone-400 text-center mt-3">
          Closing in a moment...
        </p>
      </div>
    )
  }

  // ── DONE state — clean success card with summary ─────────────────
  if (status === 'done') {
    const changes = (changeSummary?.changes || []) as string[]
    return (
      <div className={cn(
        'rounded-lg border-2 p-4 animate-fadeIn',
        mode === 'full'
          ? 'border-emerald-300 bg-gradient-to-br from-emerald-50 to-white'
          : 'border-violet-300 bg-gradient-to-br from-violet-50 to-white',
      )}>
        <div className="flex items-start gap-3">
          <div className={cn(
            'w-10 h-10 rounded-full flex items-center justify-center flex-shrink-0',
            mode === 'full' ? 'bg-emerald-500' : 'bg-violet-500',
          )}>
            <CheckCircle2 className="w-6 h-6 text-white" strokeWidth={2.5} />
          </div>
          <div className="flex-1 min-w-0">
            <h4 className="text-sm font-bold text-navy-900 mb-0.5">
              {label} complete
            </h4>
            <p className="text-xs text-stone-500">
              {elapsed}s · {changes.length > 0 ? `${changes.length} field(s) updated` : 'No changes needed'}
            </p>
            {changes.length > 0 && changes.length <= 6 && (
              <div className="flex flex-wrap gap-1 mt-2">
                {changes.map((c, i) => (
                  <span
                    key={i}
                    className={cn(
                      'inline-block px-1.5 py-0.5 text-2xs font-semibold rounded',
                      mode === 'full' ? 'bg-emerald-100 text-emerald-800' : 'bg-violet-100 text-violet-800',
                    )}
                  >
                    {c}
                  </span>
                ))}
              </div>
            )}
          </div>
        </div>
      </div>
    )
  }

  // ── RUNNING + ERROR states — original detailed card ──────────────
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
            {status === 'error' && <AlertCircle className="w-4 h-4 text-red-500" />}
            <h4 className="text-[13px] font-bold text-navy-900 flex items-center gap-1.5">
              <Zap className="w-3.5 h-3.5" />
              {status === 'running' ? `Running ${label}...` : `${label} failed`}
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
            stageNum === currentStage ? 'running' :
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
