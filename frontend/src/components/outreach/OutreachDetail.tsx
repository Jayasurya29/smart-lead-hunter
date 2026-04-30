import { useState, useEffect } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import {
  Check, X as XIcon, Send, Copy, Edit3, Loader2, Mail,
  ExternalLink, Sparkles, Star,
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
      {/* ── Header ────────────────────────────────────────────── */}
      <div className="px-5 pt-5 pb-4 border-b border-stone-100 flex-shrink-0">
        <div className="flex items-start justify-between gap-3 mb-2">
          <div className="min-w-0">
            <div className="flex items-center gap-2 mb-1">
              <h2 className="text-lg font-bold text-navy-900 truncate">
                {record.contact_name}
              </h2>
              <div className={`px-2 py-0.5 rounded text-2xs font-bold ${fitColor}`}>
                FIT {fitScore}
              </div>
            </div>
            <p className="text-sm text-stone-500 truncate">
              {record.contact_title || '—'} · {record.hotel_name}
            </p>
            {record.hotel_location && (
              <p className="text-xs text-stone-400 mt-0.5">{record.hotel_location}</p>
            )}
          </div>
          <button
            onClick={onClose}
            className="p-1.5 text-stone-400 hover:text-stone-600 rounded-lg hover:bg-stone-100 transition flex-shrink-0"
          >
            <XIcon className="w-4 h-4" />
          </button>
        </div>
        {/* Status pill */}
        <div className="flex items-center gap-2 text-xs">
          <StatusPill status={record.approval_status} />
          {record.send_time && (
            <span className="text-stone-400">· Suggested: {record.send_time}</span>
          )}
        </div>
      </div>

      {/* ── Body (scrollable) ─────────────────────────────────── */}
      <div className="flex-1 overflow-y-auto p-5 space-y-5">

        {/* Personalization brief */}
        {(record.outreach_angle || record.personalization_hook) && (
          <Section title="Personalization Brief" icon={<Sparkles className="w-3.5 h-3.5" />}>
            {record.outreach_angle && (
              <div className="mb-2">
                <span className="text-2xs uppercase tracking-wider font-semibold text-stone-400">Angle</span>
                <p className="text-sm text-navy-900 mt-0.5">{record.outreach_angle}</p>
              </div>
            )}
            {record.personalization_hook && (
              <div className="mb-2">
                <span className="text-2xs uppercase tracking-wider font-semibold text-stone-400">Hook</span>
                <p className="text-sm text-navy-900 mt-0.5 italic">{record.personalization_hook}</p>
              </div>
            )}
            {record.contact_summary && (
              <div className="mb-2">
                <span className="text-2xs uppercase tracking-wider font-semibold text-stone-400">About {record.contact_name.split(' ')[0]}</span>
                <p className="text-sm text-stone-700 mt-0.5">{record.contact_summary}</p>
              </div>
            )}
          </Section>
        )}

        {/* Pain points + value props */}
        {(record.pain_points.length > 0 || record.value_props.length > 0) && (
          <div className="grid grid-cols-2 gap-4">
            {record.pain_points.length > 0 && (
              <Section title="Pain Points" compact>
                <ul className="space-y-1">
                  {record.pain_points.map((p, i) => (
                    <li key={i} className="text-xs text-stone-700 flex gap-1">
                      <span className="text-red-500">·</span><span>{p}</span>
                    </li>
                  ))}
                </ul>
              </Section>
            )}
            {record.value_props.length > 0 && (
              <Section title="Value Props" compact>
                <ul className="space-y-1">
                  {record.value_props.map((v, i) => (
                    <li key={i} className="text-xs text-stone-700 flex gap-1">
                      <span className="text-emerald-500">·</span><span>{v}</span>
                    </li>
                  ))}
                </ul>
              </Section>
            )}
          </div>
        )}

        {/* Email */}
        <Section title="Email Draft" icon={<Mail className="w-3.5 h-3.5" />}>
          {/* Subject */}
          <div className="mb-3">
            <div className="flex items-center justify-between mb-1">
              <span className="text-2xs uppercase tracking-wider font-semibold text-stone-400">Subject</span>
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
                className="w-full px-3 py-2 text-sm border border-stone-300 rounded-md focus:outline-none focus:border-purple-400 focus:ring-2 focus:ring-purple-100 resize-none"
                rows={1}
              />
            ) : (
              <p className="text-sm text-navy-900 font-medium">{draft.email_subject || '—'}</p>
            )}
          </div>
          {/* Body */}
          <div>
            <div className="flex items-center justify-between mb-1">
              <span className="text-2xs uppercase tracking-wider font-semibold text-stone-400">Body</span>
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
                    className="p-1.5 rounded hover:bg-stone-100 text-stone-500 hover:text-purple-600 transition"
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
                className="w-full px-3 py-2 text-sm border border-stone-300 rounded-md focus:outline-none focus:border-purple-400 focus:ring-2 focus:ring-purple-100 font-mono"
                rows={8}
              />
            ) : (
              <pre className="whitespace-pre-wrap text-sm text-stone-800 leading-relaxed font-sans">{draft.email_body || '—'}</pre>
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
                className="px-3 py-1.5 text-xs font-semibold text-white bg-purple-600 rounded-md hover:bg-purple-700 disabled:opacity-50"
              >
                {saveMut.isPending ? 'Saving...' : 'Save edits'}
              </button>
            </div>
          )}
        </Section>

        {/* LinkedIn */}
        {(record.linkedin_message || draft.linkedin_message) && (
          <Section title="LinkedIn Message" icon={<Star className="w-3.5 h-3.5" />}>
            <div className="flex items-center justify-end mb-1 gap-1">
              <IconBtn onClick={() => copy(draft.linkedin_message, 'linkedin')} title="Copy">
                <Copy className="w-3 h-3" />
              </IconBtn>
              <IconBtn onClick={() => setEditing(editing === 'linkedin' ? null : 'linkedin')} title="Edit">
                <Edit3 className="w-3 h-3" />
              </IconBtn>
            </div>
            {editing === 'linkedin' ? (
              <>
                <textarea
                  value={draft.linkedin_message}
                  onChange={(e) => setDraft({ ...draft, linkedin_message: e.target.value })}
                  className="w-full px-3 py-2 text-sm border border-stone-300 rounded-md focus:outline-none focus:border-purple-400 focus:ring-2 focus:ring-purple-100"
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
                    className="px-3 py-1.5 text-xs font-semibold text-white bg-purple-600 rounded-md hover:bg-purple-700 disabled:opacity-50"
                  >
                    {saveMut.isPending ? 'Saving...' : 'Save'}
                  </button>
                </div>
              </>
            ) : (
              <p className="text-sm text-stone-800 leading-relaxed">{draft.linkedin_message || '—'}</p>
            )}
          </Section>
        )}

        {/* 3-touch sequence (generated on demand) */}
        <Section title="Follow-up Sequence">
          {!sequence ? (
            <button
              onClick={() => seqMut.mutate()}
              disabled={seqMut.isPending}
              className="w-full px-4 py-2 text-xs font-semibold text-purple-700 bg-purple-50 hover:bg-purple-100 rounded-md transition disabled:opacity-50 flex items-center justify-center gap-2"
            >
              {seqMut.isPending ? <Loader2 className="w-3 h-3 animate-spin" /> : <Sparkles className="w-3 h-3" />}
              {seqMut.isPending ? 'Generating sequence...' : 'Generate 3-touch sequence'}
            </button>
          ) : (
            <div className="space-y-3">
              {sequence.map((touch, i) => (
                <div key={i} className="border border-stone-200 rounded-lg p-3">
                  <div className="flex items-center justify-between mb-1">
                    <div className="flex items-center gap-2">
                      <span className="text-2xs font-bold text-purple-700">DAY {touch.day}</span>
                      <span className="text-2xs uppercase tracking-wider font-semibold text-stone-400">{touch.type}</span>
                    </div>
                    <IconBtn
                      onClick={() => copy(`Subject: ${touch.subject}\n\n${touch.body}`, `touch-${i}`)}
                      title="Copy this touch"
                    >
                      <Copy className="w-3 h-3" />
                    </IconBtn>
                  </div>
                  <p className="text-sm font-medium text-navy-900 mb-1">{touch.subject}</p>
                  <pre className="whitespace-pre-wrap text-xs text-stone-700 font-sans">{touch.body}</pre>
                </div>
              ))}
            </div>
          )}
        </Section>

        {record.approval_notes && (
          <div className="px-3 py-2 bg-red-50 border border-red-200 rounded-md text-xs text-red-700">
            <span className="font-bold">Rejection notes:</span> {record.approval_notes}
          </div>
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
                    // Show the confirm banner — user has to actively
                    // confirm "yes I sent it" for status to change
                    setTimeout(() => setShowConfirmSent(true), 400)
                  }}
                  className="flex-1 px-3 py-2 text-sm font-semibold text-white bg-emerald-600 rounded-md hover:bg-emerald-700 flex items-center justify-center gap-1.5 transition"
                  title="Opens Outlook (or your default mail client) with the email pre-filled. After you hit Send there, confirm here."
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
