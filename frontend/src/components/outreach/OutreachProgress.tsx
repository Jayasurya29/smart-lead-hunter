import { useEffect, useRef, useState } from 'react'
import { Check, Loader2, AlertCircle, Sparkles, Mail } from 'lucide-react'

const STAGES = [
  'Researching hotel + contact',
  'Analyzing fit + value props',
  'Drafting email + LinkedIn',
  'Quality-checking the draft',
  'Scheduling + follow-ups',
]

type Status = 'running' | 'done' | 'error'

interface CompleteResult {
  fit_score?: number
  email_subject?: string
  tone?: string
  outreach_angle?: string
}

interface Props {
  query: string
  onComplete: (researchId: number) => void
  onError?: (msg: string) => void
}

/**
 * SSE-driven progress card for outreach generation.
 *
 * Flow:
 *   running  → live progress through 5 stages
 *   done     → success card visible for ~1.8s showing fit score + subject preview
 *              (gives the user visual confirmation the run worked, instead
 *              of jumping silently to the detail panel — that previous flow
 *              looked like nothing had happened)
 *   error    → error state with retry-friendly message
 *
 * Same withCredentials=true requirement as Smart Fill — without that the
 * EventSource won't send the slh_session cookie and the auth middleware
 * 401s the request.
 */
export default function OutreachProgress({ query, onComplete, onError }: Props) {
  const [stage, setStage] = useState(0)
  const [pct, setPct] = useState(0)
  const [label, setLabel] = useState('Connecting...')
  const [status, setStatus] = useState<Status>('running')
  const [errorMsg, setErrorMsg] = useState('')
  const [elapsed, setElapsed] = useState(0)
  const [completedResult, setCompletedResult] = useState<CompleteResult | null>(null)
  const startRef = useRef(Date.now())
  const onCompleteRef = useRef(onComplete)
  const onErrorRef = useRef(onError)
  onCompleteRef.current = onComplete
  onErrorRef.current = onError

  useEffect(() => {
    if (status !== 'running') return
    const iv = setInterval(() => {
      setElapsed(Math.round((Date.now() - startRef.current) / 1000))
    }, 1000)
    return () => clearInterval(iv)
  }, [status])

  useEffect(() => {
    const url = `/api/outreach/generate-stream?${query}`
    const es = new EventSource(url, { withCredentials: true })

    es.onmessage = (e) => {
      try {
        const data = JSON.parse(e.data)
        if (data.type === 'ping' || data.type === 'started') return

        if (data.type === 'stage') {
          setStage(data.stage)
          setLabel(data.label)
          setPct(data.pct)
          if (typeof data.elapsed_s === 'number') {
            startRef.current = Date.now() - data.elapsed_s * 1000
            setElapsed(Math.round(data.elapsed_s))
          }
          return
        }

        if (data.type === 'complete') {
          setStage(STAGES.length)
          setPct(100)
          setStatus('done')
          if (typeof data.elapsed_s === 'number') {
            setElapsed(Math.round(data.elapsed_s))
          }
          setCompletedResult({
            fit_score: data.result?.fit_score,
            email_subject: data.result?.email_subject,
            tone: data.result?.tone,
            outreach_angle: data.result?.outreach_angle,
          })
          es.close()
          // Hold success view for 1.8s so user sees "it worked"
          // before the parent transitions to the detail panel
          if (data.research_id) {
            setTimeout(() => onCompleteRef.current(data.research_id), 1800)
          }
          return
        }

        if (data.type === 'error') {
          setStatus('error')
          setErrorMsg(data.message || 'Generation failed')
          es.close()
          onErrorRef.current?.(data.message || 'Generation failed')
        }
      } catch {
        /* malformed event — ignore */
      }
    }

    es.onerror = () => {
      if (status === 'done') return
    }

    return () => es.close()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [query])

  /* ── Done state ──────────────────────────────────────────── */
  if (status === 'done' && completedResult) {
    const fit = completedResult.fit_score
    const fitColor =
      fit && fit >= 80 ? 'bg-emerald-500'
        : fit && fit >= 60 ? 'bg-amber-500'
        : fit && fit >= 40 ? 'bg-stone-500'
        : 'bg-stone-400'
    return (
      <div className="bg-gradient-to-br from-emerald-50 to-purple-50 rounded-xl border-2 border-emerald-200 p-6 animate-fadeIn">
        <div className="flex flex-col items-center text-center mb-5">
          <div className="w-14 h-14 rounded-full bg-emerald-500 flex items-center justify-center mb-3">
            <Check className="w-8 h-8 text-white" strokeWidth={3} />
          </div>
          <h3 className="text-base font-bold text-navy-900 mb-1">
            Outreach generated successfully
          </h3>
          <p className="text-xs text-stone-500">
            Took {elapsed}s · 5/5 agents complete
          </p>
        </div>

        <div className="space-y-2.5">
          {fit !== undefined && fit !== null && (
            <div className="flex items-center justify-between gap-3 px-3 py-2 bg-white rounded-lg border border-stone-100">
              <div className="flex items-center gap-2">
                <Sparkles className="w-3.5 h-3.5 text-purple-600" />
                <span className="text-2xs uppercase tracking-wider font-bold text-stone-500">Fit Score</span>
              </div>
              <div className={`px-2 py-0.5 rounded text-xs font-bold text-white ${fitColor}`}>
                {fit}/100
              </div>
            </div>
          )}

          {completedResult.email_subject && (
            <div className="flex items-start gap-3 px-3 py-2 bg-white rounded-lg border border-stone-100">
              <Mail className="w-3.5 h-3.5 text-purple-600 flex-shrink-0 mt-0.5" />
              <div className="min-w-0 flex-1">
                <span className="text-2xs uppercase tracking-wider font-bold text-stone-500 block mb-0.5">
                  Subject
                </span>
                <p className="text-xs text-navy-900 font-medium truncate">
                  {completedResult.email_subject}
                </p>
              </div>
            </div>
          )}

          {completedResult.tone && (
            <div className="flex items-center justify-between gap-3 px-3 py-2 bg-white rounded-lg border border-stone-100">
              <span className="text-2xs uppercase tracking-wider font-bold text-stone-500">Tone</span>
              <span className="text-xs text-navy-900 font-medium">
                {completedResult.tone.replace(/-/g, ' ')}
              </span>
            </div>
          )}
        </div>

        <div className="mt-5 pt-4 border-t border-emerald-200/50 flex items-center justify-center gap-2 text-xs text-emerald-700">
          <Loader2 className="w-3 h-3 animate-spin" />
          Opening details...
        </div>
      </div>
    )
  }

  /* ── Error state ─────────────────────────────────────────── */
  if (status === 'error') {
    return (
      <div className="bg-red-50 rounded-xl border-2 border-red-200 p-5">
        <div className="flex items-start gap-3">
          <AlertCircle className="w-5 h-5 text-red-600 flex-shrink-0 mt-0.5" />
          <div className="flex-1">
            <h3 className="text-sm font-bold text-red-900 mb-1">Generation failed</h3>
            <p className="text-xs text-red-700">{errorMsg}</p>
            <p className="text-2xs text-red-600 mt-2 italic">
              Try again — the agents may need a retry. If it keeps failing,
              check the uvicorn logs.
            </p>
          </div>
        </div>
      </div>
    )
  }

  /* ── Running state (default) ─────────────────────────────── */
  return (
    <div className="bg-white rounded-xl border border-stone-200 p-5 shadow-sm">
      <div className="flex items-center justify-between mb-3">
        <div className="flex items-center gap-2">
          <Loader2 className="w-4 h-4 text-purple-600 animate-spin" />
          <span className="text-sm font-semibold text-navy-900">
            Generating outreach...
          </span>
        </div>
        <div className="text-2xs font-bold text-purple-600">
          {pct}% · {elapsed}s
        </div>
      </div>

      <div className="h-2 rounded-full bg-stone-100 overflow-hidden mb-4">
        <div
          className="h-full bg-gradient-to-r from-purple-500 to-purple-700 transition-all duration-500"
          style={{ width: `${pct}%` }}
        />
      </div>

      <div className="space-y-2">
        {STAGES.map((stageLabel, i) => {
          const stageNum = i + 1
          const isActive = stage === stageNum
          const isDone = stage > stageNum
          return (
            <div key={i} className="flex items-center gap-2 text-xs">
              {isDone ? (
                <Check className="w-3 h-3 text-emerald-600 flex-shrink-0" />
              ) : isActive ? (
                <Loader2 className="w-3 h-3 text-purple-600 animate-spin flex-shrink-0" />
              ) : (
                <div className="w-3 h-3 rounded-full border border-stone-300 flex-shrink-0" />
              )}
              <span
                className={
                  isDone
                    ? 'text-stone-400 line-through'
                    : isActive
                    ? 'text-navy-900 font-semibold'
                    : 'text-stone-400'
                }
              >
                {stageLabel}
              </span>
            </div>
          )
        })}
      </div>

      {label && stage > 0 && (
        <p className="mt-3 text-xs text-stone-500 italic">{label}</p>
      )}
    </div>
  )
}
