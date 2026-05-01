import { useState, useEffect } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import {
  Check, X as XIcon, Send, Copy, Edit3, Loader2, Mail,
  ExternalLink, Sparkles, Star, AlertCircle, TrendingUp, Target,
  Building2, MapPin, Linkedin,
} from 'lucide-react'
import {
  ResearchRecord,
  approveOutreach, rejectOutreach, markSent, revertToPending,
  updateOutreach, generateSequence, SequenceTouch,
} from '@/api/outreach'

interface Props {
  record: ResearchRecord
  onClose: () => void
}

/**
 * Strip the AI-generated signature from the email body so Outlook's
 * own auto-signature (image + certifications + contact info) takes over.
 *
 * The Writer agent always ends emails with a structure like:
 *
 *     Would you be open to a quick chat?
 *
 *     Best,
 *
 *     Jay
 *     J.A. Uniforms
 *
 * We detect the closing + name + company block at the end and remove it.
 * If detection fails, we leave the body untouched (better to have a
 * duplicate signature than to truncate the actual content).
 */
function stripAiSignature(body: string): string {
  if (!body) return ''
  // Common closings used by the Writer agent
  const closings = [
    'best,', 'best regards,', 'regards,', 'cheers,',
    'thanks,', 'thank you,', 'sincerely,',
  ]
  const lines = body.split('\n')
  // Walk backward — find the line containing a known closing
  for (let i = lines.length - 1; i >= 0; i--) {
    const trimmed = lines[i].trim().toLowerCase()
    if (closings.some((c) => trimmed === c || trimmed === c.replace(',', ''))) {
      // Cut from this line onward (closing + signature + company line)
      // Trim any trailing blank lines from what's left
      const kept = lines.slice(0, i).join('\n').replace(/\s+$/, '')
      return kept
    }
  }
  // Fallback — couldn't detect, return as-is
  return body
}

export default function OutreachDetail({ record, onClose }: Props) {
  const qc = useQueryClient()
  const [editing, setEditing] = useState<'subject' | 'body' | 'linkedin' | null>(null)
  // Sub-tab inside the detail panel — Brief & Email is the default,
  // Sources is the second tab. Sources used to live mid-scroll on the
  // Brief view but pushed Email Draft below the fold; promoting it to
  // its own tab keeps the Brief lean and makes verification a one-click
  // toggle when reps want to fact-check.
  const [activeTab, setActiveTab] = useState<'brief' | 'sources'>('brief')
  const [draft, setDraft] = useState({
    email_subject: record.email_subject || '',
    email_body: record.email_body || '',
    linkedin_message: record.linkedin_message || '',
  })
  const [copyMsg, setCopyMsg] = useState<string | null>(null)
  const [showRejectInput, setShowRejectInput] = useState(false)
  const [rejectFeedback, setRejectFeedback] = useState('')
  const [sequence, setSequence] = useState<SequenceTouch[] | null>(null)
  // After clicking "Open in Outlook" we show a confirmation banner —
  // "Did you actually send it?" — instead of auto-marking. Avoids the
  // bug where status flipped to Sent even if the user cancelled.
  const [showConfirmSent, setShowConfirmSent] = useState(false)

  // Re-sync draft when a different record is loaded
  useEffect(() => {
    setDraft({
      email_subject: record.email_subject || '',
      email_body: record.email_body || '',
      linkedin_message: record.linkedin_message || '',
    })
    setEditing(null)
    setShowRejectInput(false)
    setRejectFeedback('')
    setSequence(null)
    setShowConfirmSent(false)
    setActiveTab('brief')
  }, [record.id])

  const saveMut = useMutation({
    mutationFn: (patch: { email_subject?: string; email_body?: string; linkedin_message?: string }) =>
      updateOutreach(record.id, patch),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['outreach'] })
      qc.invalidateQueries({ queryKey: ['outreach', record.id] })
      setEditing(null)
    },
  })

  const approveMut = useMutation({
    mutationFn: () => approveOutreach(record.id),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['outreach'] })
      qc.invalidateQueries({ queryKey: ['outreach-stats'] })
    },
  })

  const rejectMut = useMutation({
    mutationFn: (feedback: string) => rejectOutreach(record.id, feedback),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['outreach'] })
      qc.invalidateQueries({ queryKey: ['outreach-stats'] })
      setShowRejectInput(false)
      setRejectFeedback('')
    },
  })

  const sentMut = useMutation({
    mutationFn: () => markSent(record.id),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['outreach'] })
      qc.invalidateQueries({ queryKey: ['outreach-stats'] })
      setShowConfirmSent(false)
    },
  })

  const revertMut = useMutation({
    mutationFn: () => revertToPending(record.id),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['outreach'] })
      qc.invalidateQueries({ queryKey: ['outreach-stats'] })
    },
  })

  const seqMut = useMutation({
    mutationFn: () => generateSequence(record.id),
    onSuccess: (data) => setSequence(data.touches || []),
  })

  function copy(text: string, label: string) {
    if (!text) return
    // navigator.clipboard.writeText() requires HTTPS or localhost.
    // SLH runs on http://192.168.1.151:8000 (plain HTTP), so the modern
    // API is blocked silently. Fall back to the legacy execCommand
    // approach — works on HTTP and in older browsers.
    const tryLegacyCopy = () => {
      try {
        const textarea = document.createElement('textarea')
        textarea.value = text
        textarea.style.position = 'fixed'
        textarea.style.left = '-9999px'
        textarea.style.top = '0'
        document.body.appendChild(textarea)
        textarea.focus()
        textarea.select()
        const ok = document.execCommand('copy')
        document.body.removeChild(textarea)
        return ok
      } catch {
        return false
      }
    }

    // Try the modern API first (HTTPS). If it fails or rejects, fall back.
    if (navigator.clipboard && window.isSecureContext) {
      navigator.clipboard.writeText(text)
        .then(() => {
          setCopyMsg(label)
          setTimeout(() => setCopyMsg(null), 1800)
        })
        .catch(() => {
          if (tryLegacyCopy()) {
            setCopyMsg(label)
            setTimeout(() => setCopyMsg(null), 1800)
          } else {
            alert('Copy failed — please select and copy manually')
          }
        })
    } else {
      // No secure context — go straight to legacy copy
      if (tryLegacyCopy()) {
        setCopyMsg(label)
        setTimeout(() => setCopyMsg(null), 1800)
      } else {
        alert('Copy failed — please select and copy manually')
      }
    }
  }

  const fitScore = record.fit_score ?? 0
  const fitColor =
    fitScore >= 80 ? 'bg-emerald-500 text-white'
      : fitScore >= 60 ? 'bg-amber-500 text-white'
      : fitScore >= 40 ? 'bg-stone-500 text-white'
      : 'bg-red-400 text-white'

  return (
    <div className="h-full flex flex-col bg-white">
      {/* ── Header — avatar + name + contact info chips ─────── */}
      <div className="px-6 pt-6 pb-5 border-b border-stone-100 bg-gradient-to-br from-stone-50/50 to-white flex-shrink-0">
        <div className="flex items-start justify-between gap-3 mb-3">
          <div className="flex items-start gap-3 min-w-0 flex-1">
            {/* Avatar circle with initials */}
            <div className={`flex-shrink-0 w-12 h-12 rounded-full flex items-center justify-center text-base font-bold text-white shadow-sm ${
              fitScore >= 80 ? 'bg-gradient-to-br from-emerald-500 to-emerald-600'
                : fitScore >= 60 ? 'bg-gradient-to-br from-amber-500 to-amber-600'
                : fitScore >= 40 ? 'bg-gradient-to-br from-stone-500 to-stone-600'
                : 'bg-gradient-to-br from-red-400 to-red-500'
            }`}>
              {(record.contact_name || '?').split(' ').map(n => n[0]).slice(0, 2).join('').toUpperCase()}
            </div>
            <div className="min-w-0 flex-1">
              {/* Name + fit score */}
              <div className="flex items-center gap-2 mb-1">
                <h2 className="text-xl font-bold text-navy-900 truncate">
                  {record.contact_name}
                </h2>
                <div className={`px-2 py-0.5 rounded text-2xs font-bold flex-shrink-0 ${fitColor}`}>
                  FIT {fitScore}
                </div>
              </div>
              {/* Title */}
              {record.contact_title && (
                <p className="text-sm font-medium text-stone-700 mb-2">
                  {record.contact_title}
                </p>
              )}
              {/* Hotel + location chips */}
              <div className="flex flex-wrap items-center gap-1.5">
                <span className="inline-flex items-center gap-1.5 px-2.5 py-1 bg-navy-50 text-navy-800 rounded-md text-xs font-semibold">
                  <Building2 className="w-3 h-3" />
                  {record.hotel_name}
                </span>
                {record.hotel_location && (
                  <span className="inline-flex items-center gap-1.5 px-2.5 py-1 bg-stone-100 text-stone-700 rounded-md text-xs">
                    <MapPin className="w-3 h-3" />
                    {record.hotel_location}
                  </span>
                )}
              </div>
              {/* Email + LinkedIn — clickable when present */}
              {(record.email || record.linkedin_url) && (
                <div className="flex flex-wrap items-center gap-1.5 mt-2">
                  {record.email && (
                    <a
                      href={`mailto:${record.email}`}
                      className="inline-flex items-center gap-1.5 px-2.5 py-1 bg-white border border-stone-200 hover:border-purple-300 hover:bg-purple-50 text-stone-700 hover:text-purple-700 rounded-md text-xs transition group"
                      title="Click to compose in default mail client"
                    >
                      <Mail className="w-3 h-3 text-stone-400 group-hover:text-purple-600" />
                      <span className="truncate max-w-[200px]">{record.email}</span>
                    </a>
                  )}
                  {record.linkedin_url && (
                    <a
                      href={record.linkedin_url}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="inline-flex items-center gap-1.5 px-2.5 py-1 bg-white border border-stone-200 hover:border-blue-300 hover:bg-blue-50 text-stone-700 hover:text-blue-700 rounded-md text-xs transition group"
                      title="Open LinkedIn profile in new tab"
                    >
                      <Linkedin className="w-3 h-3 text-stone-400 group-hover:text-blue-600" />
                      LinkedIn
                      <ExternalLink className="w-2.5 h-2.5 text-stone-300 group-hover:text-blue-500" />
                    </a>
                  )}
                </div>
              )}
            </div>
          </div>
          <button
            onClick={onClose}
            className="p-1.5 text-stone-400 hover:text-stone-600 rounded-lg hover:bg-stone-100 transition flex-shrink-0"
          >
            <XIcon className="w-4 h-4" />
          </button>
        </div>
        {/* Status pill row */}
        <div className="flex items-center gap-2 text-xs pt-2 border-t border-stone-100">
          <StatusPill status={record.approval_status} />
          {record.research_confidence && (
            <ConfidenceBadge confidence={record.research_confidence} />
          )}
          {record.send_time && (
            <span className="text-stone-400">· Suggested send: {record.send_time}</span>
          )}
        </div>
      </div>

      {/* ── Sub-tab strip — Brief & Email vs Sources ───────────── */}
      <div className="flex items-center gap-1 px-5 pt-3 bg-white border-b border-stone-100 flex-shrink-0">
        <button
          onClick={() => setActiveTab('brief')}
          className={`px-4 py-2 text-xs font-bold uppercase tracking-wider rounded-t-md transition border-b-2 -mb-px ${
            activeTab === 'brief'
              ? 'text-purple-700 border-purple-600 bg-purple-50/40'
              : 'text-stone-500 border-transparent hover:text-stone-700 hover:bg-stone-50'
          }`}
        >
          Brief &amp; Email
        </button>
        <button
          onClick={() => setActiveTab('sources')}
          className={`px-4 py-2 text-xs font-bold uppercase tracking-wider rounded-t-md transition border-b-2 -mb-px flex items-center gap-1.5 ${
            activeTab === 'sources'
              ? 'text-stone-800 border-stone-600 bg-stone-50/60'
              : 'text-stone-500 border-transparent hover:text-stone-700 hover:bg-stone-50'
          }`}
        >
          <ExternalLink className="w-3 h-3" />
          Sources
          {record.sources && record.sources.length > 0 && (
            <span className={`px-1.5 py-0.5 text-2xs font-bold rounded ${
              activeTab === 'sources'
                ? 'bg-stone-200 text-stone-700'
                : 'bg-stone-100 text-stone-500'
            }`}>
              {record.sources.length}
            </span>
          )}
        </button>
      </div>

      {/* ── Body (scrollable) — content swaps based on activeTab ── */}
      <div className="flex-1 overflow-y-auto p-5 space-y-5">

        {/* ════════════════ SOURCES TAB ════════════════ */}
        {activeTab === 'sources' && (
          <>
            {(!record.sources || record.sources.length === 0) ? (
              <div className="text-center py-16">
                <ExternalLink className="w-10 h-10 text-stone-200 mx-auto mb-3" />
                <p className="text-sm text-stone-500">No sources captured for this outreach</p>
                <p className="text-xs text-stone-400 mt-1">
                  This may be an older record generated before source tracking was added.
                </p>
              </div>
            ) : (
              <>
                <div className="bg-blue-50/60 border border-blue-200/60 rounded-lg px-4 py-3">
                  <p className="text-sm text-blue-900 font-semibold mb-0.5">
                    All facts in the Brief are grounded in these sources
                  </p>
                  <p className="text-xs text-blue-700">
                    Click any card to open the original article. Use this to verify any specific claim before you send the email.
                  </p>
                </div>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                  {record.sources.map((src, i) => {
                    const categoryColors: Record<string, { bg: string; text: string }> = {
                      news:        { bg: 'bg-blue-100',    text: 'text-blue-800' },
                      pre_opening: { bg: 'bg-amber-100',   text: 'text-amber-800' },
                      expansion:   { bg: 'bg-violet-100',  text: 'text-violet-800' },
                      hiring:      { bg: 'bg-emerald-100', text: 'text-emerald-800' },
                      staffing:    { bg: 'bg-emerald-100', text: 'text-emerald-800' },
                      awards:      { bg: 'bg-yellow-100',  text: 'text-yellow-800' },
                      contact:     { bg: 'bg-purple-100',  text: 'text-purple-800' },
                      contact_linkedin: { bg: 'bg-blue-100', text: 'text-blue-800' },
                      contact_brand: { bg: 'bg-purple-100', text: 'text-purple-800' },
                      press_release: { bg: 'bg-rose-100', text: 'text-rose-800' },
                      brand:       { bg: 'bg-indigo-100',  text: 'text-indigo-800' },
                      brand_city:  { bg: 'bg-indigo-100',  text: 'text-indigo-800' },
                    }
                    const c = categoryColors[src.category] || { bg: 'bg-stone-100', text: 'text-stone-700' }
                    return (
                      <a
                        key={i}
                        href={src.url}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="block p-3.5 bg-white border border-stone-200 rounded-lg hover:border-stone-400 hover:shadow-md transition group"
                        title={src.url}
                      >
                        <div className="flex items-start justify-between gap-2 mb-2">
                          <span className={`inline-flex items-center px-1.5 py-0.5 text-2xs font-bold uppercase tracking-wider rounded ${c.bg} ${c.text}`}>
                            {src.category.replace(/_/g, ' ')}
                          </span>
                          <ExternalLink className="w-3.5 h-3.5 text-stone-300 group-hover:text-stone-700 flex-shrink-0" />
                        </div>
                        <p className="text-sm font-semibold text-navy-900 line-clamp-2 group-hover:text-purple-700 transition leading-snug">
                          {src.title}
                        </p>
                        {src.snippet && (
                          <p className="text-xs text-stone-600 mt-1.5 line-clamp-3 leading-relaxed">
                            {src.snippet}
                          </p>
                        )}
                        <p className="text-2xs text-stone-400 mt-2 truncate flex items-center gap-1">
                          <span>🔗</span>
                          {(() => {
                            try {
                              return new URL(src.url).hostname.replace(/^www\./, '')
                            } catch {
                              return src.url
                            }
                          })()}
                        </p>
                      </a>
                    )
                  })}
                </div>
              </>
            )}
          </>
        )}

        {/* ════════════════ BRIEF & EMAIL TAB ════════════════ */}
        {activeTab === 'brief' && (
          <>
        {/* Personalization Brief — purple-tinted card, comfortable typography */}
        {(record.outreach_angle || record.personalization_hook) && (
          <div className="bg-gradient-to-br from-purple-50/70 to-indigo-50/40 border border-purple-200/70 rounded-xl p-5">
            <div className="flex items-center gap-2 mb-4 pb-2 border-b border-purple-200/60">
              <div className="w-7 h-7 rounded-lg bg-purple-100 flex items-center justify-center">
                <Sparkles className="w-4 h-4 text-purple-600" />
              </div>
              <h3 className="text-xs uppercase tracking-wider font-bold text-purple-900">
                Personalization Brief
              </h3>
            </div>
            <div className="space-y-4">
              {record.outreach_angle && (
                <div>
                  <span className="inline-flex items-center px-2 py-0.5 text-2xs uppercase tracking-wider font-bold text-purple-800 bg-purple-100 rounded mb-1.5">
                    Angle
                  </span>
                  <p className="text-sm text-navy-900 leading-relaxed">{record.outreach_angle}</p>
                </div>
              )}
              {record.personalization_hook && (
                <div>
                  <span className="inline-flex items-center px-2 py-0.5 text-2xs uppercase tracking-wider font-bold text-indigo-800 bg-indigo-100 rounded mb-1.5">
                    Hook
                  </span>
                  <p className="text-sm text-navy-900 leading-relaxed italic">"{record.personalization_hook}"</p>
                </div>
              )}
              {record.contact_summary && (
                <div>
                  <span className="inline-flex items-center px-2 py-0.5 text-2xs uppercase tracking-wider font-bold text-stone-700 bg-stone-100 rounded mb-1.5">
                    About {record.contact_name.split(' ')[0]}
                  </span>
                  <p className="text-sm text-stone-700 leading-relaxed">{record.contact_summary}</p>
                </div>
              )}
            </div>
          </div>
        )}

        {/* Pain points + value props — color-washed cards with comfortable typography */}
        {(record.pain_points.length > 0 || record.value_props.length > 0) && (
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {record.pain_points.length > 0 && (
              <div className="bg-gradient-to-br from-rose-50/70 to-orange-50/40 border border-rose-200/70 rounded-xl p-4">
                <div className="flex items-center gap-2 mb-3 pb-2 border-b border-rose-200/60">
                  <div className="w-7 h-7 rounded-lg bg-rose-100 flex items-center justify-center">
                    <Target className="w-4 h-4 text-rose-600" />
                  </div>
                  <h3 className="text-xs uppercase tracking-wider font-bold text-rose-900">
                    Pain Points
                  </h3>
                  <span className="ml-auto text-2xs font-bold text-rose-400">
                    {record.pain_points.length}
                  </span>
                </div>
                <ul className="space-y-2.5">
                  {record.pain_points.map((p, i) => (
                    <li key={i} className="flex gap-2.5 text-sm text-stone-800 leading-relaxed">
                      <span className="flex-shrink-0 w-5 h-5 rounded-full bg-rose-100 text-rose-700 text-2xs font-bold flex items-center justify-center mt-0.5">
                        {i + 1}
                      </span>
                      <span>{p}</span>
                    </li>
                  ))}
                </ul>
              </div>
            )}
            {record.value_props.length > 0 && (
              <div className="bg-gradient-to-br from-emerald-50/70 to-teal-50/40 border border-emerald-200/70 rounded-xl p-4">
                <div className="flex items-center gap-2 mb-3 pb-2 border-b border-emerald-200/60">
                  <div className="w-7 h-7 rounded-lg bg-emerald-100 flex items-center justify-center">
                    <TrendingUp className="w-4 h-4 text-emerald-600" />
                  </div>
                  <h3 className="text-xs uppercase tracking-wider font-bold text-emerald-900">
                    Value Props
                  </h3>
                  <span className="ml-auto text-2xs font-bold text-emerald-400">
                    {record.value_props.length}
                  </span>
                </div>
                <ul className="space-y-2.5">
                  {record.value_props.map((v, i) => (
                    <li key={i} className="flex gap-2.5 text-sm text-stone-800 leading-relaxed">
                      <span className="flex-shrink-0 w-5 h-5 rounded-full bg-emerald-100 text-emerald-700 flex items-center justify-center mt-0.5">
                        <Check className="w-3 h-3" strokeWidth={3} />
                      </span>
                      <span>{v}</span>
                    </li>
                  ))}
                </ul>
              </div>
            )}
          </div>
        )}

        {/* Email */}
        {/* Sources hint — small button that switches to Sources tab.
            Replaces the inline Sources card that used to live here, which
            was eating too much vertical space and pushing Email Draft
            below the fold. */}
        {record.sources && record.sources.length > 0 && (
          <button
            onClick={() => setActiveTab('sources')}
            className="w-full flex items-center justify-between gap-3 px-4 py-2.5 bg-white border border-stone-200 hover:border-stone-400 hover:bg-stone-50 rounded-lg transition group"
          >
            <span className="flex items-center gap-2 text-xs">
              <ExternalLink className="w-3.5 h-3.5 text-stone-400 group-hover:text-stone-700" />
              <span className="font-semibold text-stone-700">
                {record.sources.length} sources backing this brief
              </span>
              <span className="text-stone-400">— click to verify any fact</span>
            </span>
            <span className="text-xs font-semibold text-purple-600 group-hover:underline">
              View sources →
            </span>
          </button>
        )}

        {/* Email Draft — blue/sky tinted card, the deliverable */}
        <div className="bg-gradient-to-br from-sky-50/70 to-blue-50/40 border border-sky-200/70 rounded-xl p-5">
          <div className="flex items-center gap-2 mb-4 pb-2 border-b border-sky-200/60">
            <div className="w-7 h-7 rounded-lg bg-sky-100 flex items-center justify-center">
              <Mail className="w-4 h-4 text-sky-600" />
            </div>
            <h3 className="text-xs uppercase tracking-wider font-bold text-sky-900">
              Email Draft
            </h3>
            <span className="ml-auto text-2xs text-sky-400 font-semibold">
              Ready to send
            </span>
          </div>

          {/* Subject */}
          <div className="mb-4">
            <div className="flex items-center justify-between mb-1.5">
              <span className="inline-flex items-center px-2 py-0.5 text-2xs uppercase tracking-wider font-bold text-sky-800 bg-sky-100 rounded">
                Subject
              </span>
              <div className="flex gap-1">
                <IconBtn onClick={() => copy(draft.email_subject, 'subject')} title="Copy subject">
                  <Copy className="w-3 h-3" />
                </IconBtn>
                <IconBtn onClick={() => setEditing(editing === 'subject' ? null : 'subject')} title="Edit">
                  <Edit3 className="w-3 h-3" />
                </IconBtn>
              </div>
            </div>
            {editing === 'subject' ? (
              <textarea
                value={draft.email_subject}
                onChange={(e) => setDraft({ ...draft, email_subject: e.target.value })}
                className="w-full px-3 py-2 text-sm bg-white border border-stone-300 rounded-md focus:outline-none focus:border-sky-400 focus:ring-2 focus:ring-sky-100 resize-none"
                rows={1}
              />
            ) : (
              <p className="text-base text-navy-900 font-semibold leading-relaxed">{draft.email_subject || '—'}</p>
            )}
          </div>

          {/* Body */}
          <div>
            <div className="flex items-center justify-between mb-1.5">
              <span className="inline-flex items-center px-2 py-0.5 text-2xs uppercase tracking-wider font-bold text-sky-800 bg-sky-100 rounded">
                Body
              </span>
              <div className="flex gap-1">
                <IconBtn onClick={() => copy(draft.email_body, 'body')} title="Copy body">
                  <Copy className="w-3 h-3" />
                </IconBtn>
                <IconBtn onClick={() => setEditing(editing === 'body' ? null : 'body')} title="Edit">
                  <Edit3 className="w-3 h-3" />
                </IconBtn>
                {record.email && (
                  <a
                    href={`mailto:${record.email}?subject=${encodeURIComponent(draft.email_subject || '')}&body=${encodeURIComponent(stripAiSignature(draft.email_body || ''))}`}
                    title="Open in default mail client"
                    className="p-1.5 rounded hover:bg-stone-100 text-stone-500 hover:text-sky-600 transition"
                  >
                    <ExternalLink className="w-3 h-3" />
                  </a>
                )}
              </div>
            </div>
            {editing === 'body' ? (
              <textarea
                value={draft.email_body}
                onChange={(e) => setDraft({ ...draft, email_body: e.target.value })}
                className="w-full px-3 py-2 text-sm bg-white border border-stone-300 rounded-md focus:outline-none focus:border-sky-400 focus:ring-2 focus:ring-sky-100 font-mono"
                rows={8}
              />
            ) : (
              <div className="bg-white/80 border border-sky-100 rounded-lg p-4">
                <pre className="whitespace-pre-wrap text-sm text-stone-800 leading-relaxed font-sans">{draft.email_body || '—'}</pre>
              </div>
            )}
          </div>

          {(editing === 'subject' || editing === 'body') && (
            <div className="mt-3 flex justify-end gap-2">
              <button
                onClick={() => {
                  setDraft({
                    email_subject: record.email_subject || '',
                    email_body: record.email_body || '',
                    linkedin_message: record.linkedin_message || '',
                  })
                  setEditing(null)
                }}
                className="px-3 py-1.5 text-xs font-semibold text-stone-600 bg-stone-100 rounded-md hover:bg-stone-200"
              >
                Cancel
              </button>
              <button
                onClick={() => saveMut.mutate({
                  email_subject: draft.email_subject,
                  email_body: draft.email_body,
                })}
                disabled={saveMut.isPending}
                className="px-3 py-1.5 text-xs font-semibold text-white bg-sky-600 rounded-md hover:bg-sky-700 disabled:opacity-50"
              >
                {saveMut.isPending ? 'Saving...' : 'Save edits'}
              </button>
            </div>
          )}
        </div>

        {/* LinkedIn Message — blue-tinted card */}
        {(record.linkedin_message || draft.linkedin_message) && (
          <div className="bg-gradient-to-br from-blue-50/70 to-indigo-50/40 border border-blue-200/70 rounded-xl p-5">
            <div className="flex items-center gap-2 mb-3 pb-2 border-b border-blue-200/60">
              <div className="w-7 h-7 rounded-lg bg-blue-100 flex items-center justify-center">
                <Linkedin className="w-4 h-4 text-blue-600" />
              </div>
              <h3 className="text-xs uppercase tracking-wider font-bold text-blue-900">
                LinkedIn Message
              </h3>
              <span className="ml-auto text-2xs text-blue-400 font-semibold">
                {(draft.linkedin_message || '').length}/280
              </span>
              <div className="flex gap-1 ml-1">
                <IconBtn onClick={() => copy(draft.linkedin_message, 'linkedin')} title="Copy">
                  <Copy className="w-3 h-3" />
                </IconBtn>
                <IconBtn onClick={() => setEditing(editing === 'linkedin' ? null : 'linkedin')} title="Edit">
                  <Edit3 className="w-3 h-3" />
                </IconBtn>
              </div>
            </div>
            {editing === 'linkedin' ? (
              <>
                <textarea
                  value={draft.linkedin_message}
                  onChange={(e) => setDraft({ ...draft, linkedin_message: e.target.value })}
                  className="w-full px-3 py-2 text-sm bg-white border border-stone-300 rounded-md focus:outline-none focus:border-blue-400 focus:ring-2 focus:ring-blue-100"
                  rows={3}
                />
                <div className="mt-2 flex justify-end gap-2">
                  <button
                    onClick={() => {
                      setDraft({ ...draft, linkedin_message: record.linkedin_message || '' })
                      setEditing(null)
                    }}
                    className="px-3 py-1.5 text-xs font-semibold text-stone-600 bg-stone-100 rounded-md hover:bg-stone-200"
                  >
                    Cancel
                  </button>
                  <button
                    onClick={() => saveMut.mutate({ linkedin_message: draft.linkedin_message })}
                    disabled={saveMut.isPending}
                    className="px-3 py-1.5 text-xs font-semibold text-white bg-blue-600 rounded-md hover:bg-blue-700 disabled:opacity-50"
                  >
                    {saveMut.isPending ? 'Saving...' : 'Save'}
                  </button>
                </div>
              </>
            ) : (
              <div className="bg-white/80 border border-blue-100 rounded-lg p-4">
                <p className="text-sm text-stone-800 leading-relaxed">{draft.linkedin_message || '—'}</p>
              </div>
            )}
          </div>
        )}

        {/* 3-touch sequence (generated on demand) */}
        {/* Follow-up Sequence — amber/orange tinted card */}
        <div className="bg-gradient-to-br from-amber-50/70 to-orange-50/40 border border-amber-200/70 rounded-xl p-5">
          <div className="flex items-center gap-2 mb-3 pb-2 border-b border-amber-200/60">
            <div className="w-7 h-7 rounded-lg bg-amber-100 flex items-center justify-center">
              <Send className="w-4 h-4 text-amber-600" />
            </div>
            <h3 className="text-xs uppercase tracking-wider font-bold text-amber-900">
              Follow-up Sequence
            </h3>
            {sequence && (
              <span className="ml-auto text-2xs text-amber-400 font-semibold">
                {sequence.length} touches
              </span>
            )}
          </div>

          {!sequence ? (
            <button
              onClick={() => seqMut.mutate()}
              disabled={seqMut.isPending}
              className="w-full px-4 py-3 text-sm font-semibold text-amber-800 bg-white/70 border border-amber-200 hover:bg-white hover:border-amber-300 rounded-lg transition disabled:opacity-50 flex items-center justify-center gap-2"
            >
              {seqMut.isPending ? <Loader2 className="w-4 h-4 animate-spin" /> : <Sparkles className="w-4 h-4" />}
              {seqMut.isPending ? 'Generating sequence...' : 'Generate 3-touch sequence'}
            </button>
          ) : (
            <div className="space-y-3">
              {sequence.map((touch, i) => (
                <div key={i} className="bg-white/80 border border-amber-100 rounded-lg p-4">
                  <div className="flex items-center justify-between mb-2">
                    <div className="flex items-center gap-2">
                      <span className="inline-flex items-center px-2 py-0.5 text-2xs font-bold text-amber-900 bg-amber-100 rounded">
                        DAY {touch.day}
                      </span>
                      <span className="text-2xs uppercase tracking-wider font-semibold text-stone-500">
                        {touch.type}
                      </span>
                    </div>
                    <IconBtn
                      onClick={() => copy(`Subject: ${touch.subject}\n\n${touch.body}`, `touch-${i}`)}
                      title="Copy this touch"
                    >
                      <Copy className="w-3 h-3" />
                    </IconBtn>
                  </div>
                  <p className="text-sm font-semibold text-navy-900 mb-2 leading-relaxed">{touch.subject}</p>
                  <pre className="whitespace-pre-wrap text-xs text-stone-700 font-sans leading-relaxed">{touch.body}</pre>
                </div>
              ))}
            </div>
          )}
        </div>

        {record.approval_notes && (
          <div className="px-3 py-2 bg-red-50 border border-red-200 rounded-md text-xs text-red-700">
            <span className="font-bold">Rejection notes:</span> {record.approval_notes}
          </div>
        )}
          </>
        )}
      </div>

      {/* ── Footer actions ────────────────────────────────────── */}
      <div className="px-5 py-4 border-t border-stone-100 bg-stone-50 flex-shrink-0 space-y-2">
        {copyMsg && (
          <p className="text-xs text-emerald-600 text-center">✓ Copied {copyMsg}</p>
        )}

        {showRejectInput ? (
          <div className="space-y-2">
            <textarea
              value={rejectFeedback}
              onChange={(e) => setRejectFeedback(e.target.value)}
              placeholder="Why are you rejecting? (optional)"
              className="w-full px-3 py-2 text-xs border border-stone-300 rounded-md focus:outline-none focus:border-red-400"
              rows={2}
            />
            <div className="flex gap-2">
              <button
                onClick={() => setShowRejectInput(false)}
                className="flex-1 px-3 py-2 text-xs font-semibold text-stone-600 bg-white border border-stone-200 rounded-md hover:bg-stone-100"
              >
                Cancel
              </button>
              <button
                onClick={() => rejectMut.mutate(rejectFeedback)}
                disabled={rejectMut.isPending}
                className="flex-1 px-3 py-2 text-xs font-semibold text-white bg-red-600 rounded-md hover:bg-red-700 disabled:opacity-50"
              >
                Confirm Reject
              </button>
            </div>
          </div>
        ) : showConfirmSent ? (
          /* Confirmation banner — appears AFTER user clicks "Open in Outlook"
             so we never mark Sent unless the user confirms they actually
             hit Send in Outlook. Fixes the bug where status flipped to
             Sent even if the user closed Outlook without sending. */
          <div className="space-y-2">
            <div className="px-3 py-2 bg-blue-50 border border-blue-200 rounded-md text-xs text-blue-900">
              <p className="font-semibold mb-0.5">Did you send the email in Outlook?</p>
              <p className="text-blue-700 text-2xs">
                Outlook should have opened with your draft. After you hit Send there, click below.
              </p>
            </div>
            <div className="flex gap-2">
              <button
                onClick={() => sentMut.mutate()}
                disabled={sentMut.isPending}
                className="flex-1 px-3 py-2 text-sm font-semibold text-white bg-emerald-600 rounded-md hover:bg-emerald-700 disabled:opacity-50 flex items-center justify-center gap-1.5"
              >
                <Check className="w-4 h-4" />
                {sentMut.isPending ? 'Marking...' : 'Yes, mark as sent'}
              </button>
              <button
                onClick={() => setShowConfirmSent(false)}
                className="px-3 py-2 text-sm font-semibold text-stone-700 bg-white border border-stone-200 rounded-md hover:bg-stone-50"
              >
                Not yet
              </button>
            </div>
          </div>
        ) : (
          <div className="flex gap-2">
            {record.approval_status === 'pending' && (
              <>
                {/* Primary action: Open in Outlook
                    Builds a mailto: URL with To/Subject/Body pre-filled.
                    After it opens, we show the confirm banner so the user
                    explicitly confirms they sent — no more silently
                    flipping status to Sent when the user backed out. */}
                <button
                  onClick={() => {
                    if (!record.email) {
                      alert('No email address on file for this contact. Add one in the contact details first.')
                      return
                    }
                    const subject = encodeURIComponent(draft.email_subject || '')
                    const body = encodeURIComponent(stripAiSignature(draft.email_body || ''))
                    window.location.href = `mailto:${record.email}?subject=${subject}&body=${body}`
                    setTimeout(() => setShowConfirmSent(true), 400)
                  }}
                  className="flex-1 px-3 py-2 text-sm font-semibold text-white bg-emerald-600 rounded-md hover:bg-emerald-700 flex items-center justify-center gap-1.5 transition"
                  title="Opens Outlook with the email pre-filled. After you hit Send there, confirm here."
                >
                  <Mail className="w-4 h-4" />
                  Open in Outlook
                </button>
                <button
                  onClick={() => approveMut.mutate()}
                  disabled={approveMut.isPending}
                  className="px-3 py-2 text-sm font-semibold text-stone-700 bg-white border border-stone-200 rounded-md hover:bg-stone-50 disabled:opacity-50 flex items-center justify-center gap-1.5"
                  title="Approve and save for later — no email sent now"
                >
                  <Check className="w-4 h-4" />
                  Save for later
                </button>
                <button
                  onClick={() => setShowRejectInput(true)}
                  className="px-3 py-2 text-sm font-semibold text-red-700 bg-white border border-red-200 rounded-md hover:bg-red-50 flex items-center justify-center gap-1.5"
                >
                  <XIcon className="w-4 h-4" />
                  Reject
                </button>
              </>
            )}
            {record.approval_status === 'approved' && (
              <>
                <button
                  onClick={() => {
                    if (!record.email) {
                      alert('No email address on file for this contact.')
                      return
                    }
                    const subject = encodeURIComponent(draft.email_subject || '')
                    const body = encodeURIComponent(stripAiSignature(draft.email_body || ''))
                    window.location.href = `mailto:${record.email}?subject=${subject}&body=${body}`
                    setTimeout(() => setShowConfirmSent(true), 400)
                  }}
                  className="flex-1 px-3 py-2 text-sm font-semibold text-white bg-emerald-600 rounded-md hover:bg-emerald-700 flex items-center justify-center gap-1.5"
                >
                  <Mail className="w-4 h-4" />
                  Open in Outlook
                </button>
                <button
                  onClick={() => sentMut.mutate()}
                  disabled={sentMut.isPending}
                  className="px-3 py-2 text-sm font-semibold text-stone-700 bg-white border border-stone-200 rounded-md hover:bg-stone-50 disabled:opacity-50"
                  title="Mark sent without opening Outlook (e.g., already sent manually)"
                >
                  Mark Sent
                </button>
              </>
            )}
            {record.approval_status === 'sent' && (
              <>
                <div className="flex-1 px-3 py-2 text-sm font-semibold text-emerald-700 bg-emerald-50 rounded-md text-center">
                  ✓ Sent {record.sent_at ? `· ${new Date(record.sent_at).toLocaleString()}` : ''}
                </div>
                <button
                  onClick={() => revertMut.mutate()}
                  disabled={revertMut.isPending}
                  className="px-3 py-2 text-2xs font-semibold text-stone-600 bg-white border border-stone-200 rounded-md hover:bg-stone-50 disabled:opacity-50 flex items-center gap-1"
                  title="Move this back to Pending Review (e.g., you didn't actually send it)"
                >
                  ↺ Revert
                </button>
              </>
            )}
            {record.approval_status === 'rejected' && (
              <>
                <div className="flex-1 px-3 py-2 text-sm font-semibold text-red-700 bg-red-50 rounded-md text-center">
                  ✗ Rejected
                </div>
                <button
                  onClick={() => revertMut.mutate()}
                  disabled={revertMut.isPending}
                  className="px-3 py-2 text-2xs font-semibold text-stone-600 bg-white border border-stone-200 rounded-md hover:bg-stone-50 disabled:opacity-50 flex items-center gap-1"
                  title="Move this back to Pending Review for re-consideration"
                >
                  ↺ Revert
                </button>
              </>
            )}
          </div>
        )}
      </div>
    </div>
  )
}

/* ──────────────── Subcomponents ──────────────── */

function Section({
  title, icon, compact, children,
}: {
  title: string
  icon?: React.ReactNode
  compact?: boolean
  children: React.ReactNode
}) {
  return (
    <div className={compact ? '' : 'bg-white border border-stone-200 rounded-xl p-4'}>
      <div className="flex items-center gap-1.5 mb-2">
        {icon && <span className="text-purple-600">{icon}</span>}
        <h3 className="text-2xs uppercase tracking-wider font-bold text-stone-500">
          {title}
        </h3>
      </div>
      {children}
    </div>
  )
}

function IconBtn({ onClick, title, children }: { onClick: () => void; title: string; children: React.ReactNode }) {
  return (
    <button
      onClick={onClick}
      title={title}
      className="p-1.5 rounded hover:bg-stone-100 text-stone-500 hover:text-purple-600 transition"
    >
      {children}
    </button>
  )
}

function StatusPill({ status }: { status: string }) {
  const config: Record<string, { bg: string; text: string; label: string }> = {
    pending: { bg: 'bg-amber-100', text: 'text-amber-800', label: 'Pending Review' },
    approved: { bg: 'bg-blue-100', text: 'text-blue-800', label: 'Approved' },
    sent: { bg: 'bg-emerald-100', text: 'text-emerald-800', label: 'Sent' },
    rejected: { bg: 'bg-red-100', text: 'text-red-800', label: 'Rejected' },
  }
  const c = config[status] || config.pending
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded text-2xs font-bold uppercase tracking-wider ${c.bg} ${c.text}`}>
      {c.label}
    </span>
  )
}

/**
 * Confidence badge — surfaces how trustworthy the brief is at a glance.
 * Displayed next to the status pill on every outreach record.
 *
 *   high    → green ✓ (full research, brief is solid)
 *   medium  → yellow (some sparseness, glance-check the brief)
 *   low     → red ⚠ (very thin research, fact-check before sending)
 */
function ConfidenceBadge({ confidence }: { confidence: 'high' | 'medium' | 'low' }) {
  const config = {
    high:   { bg: 'bg-emerald-100', text: 'text-emerald-800', icon: '✓', label: 'Strong research' },
    medium: { bg: 'bg-amber-100',   text: 'text-amber-800',   icon: '~', label: 'Moderate research' },
    low:    { bg: 'bg-rose-100',    text: 'text-rose-800',    icon: '⚠', label: 'Thin research — verify' },
  } as const
  const c = config[confidence]
  if (!c) return null
  return (
    <span
      className={`inline-flex items-center gap-1 px-2 py-0.5 rounded text-2xs font-bold uppercase tracking-wider ${c.bg} ${c.text}`}
      title={
        confidence === 'low'
          ? 'Research came back sparse — fact-check this brief manually before sending.'
          : confidence === 'medium'
          ? 'Research had some gaps — quick scan recommended.'
          : 'Research returned strong, varied data.'
      }
    >
      <span>{c.icon}</span>
      {c.label}
    </span>
  )
}
