/**
 * ContactProfile — right panel of the split-view Contacts page.
 * Live AI profile: intelligence band (real Deep Enrich), suggested steps,
 * contact + hospitality intel, engagement timeline.
 */
import { useEffect, useRef, useState } from 'react'
import {
  Sparkles, Wand2, Check, Send, Mail, Phone, Linkedin, ExternalLink,
  MapPin, Building2, Shield, Hash, Eye, Users, Activity, Star, Loader2,
  Ban, RotateCcw,
} from 'lucide-react'
import { cn, formatDate, relativeDate, getTierLabel } from '@/lib/utils'
import type { InboxContact } from '@/api/inboxContacts'
import { useDeepEnrichContact, useFindCurrentEmployer, useApproveInboxContact, useJunkContact, useUnjunkContact, useJunkDomain } from '@/hooks/useInboxContacts'
import {
  Avatar, CategoryBadge, fullName, roleText, confidencePct, isHighOpportunity, StaleBadge,
  deriveSignals, deriveNextSteps, deriveSummary,
} from './contactsUi'

/* ── small pieces ── */

function ActionBtn({
  icon, label, primary, done, tone, onClick, disabled,
}: {
  icon?: React.ReactNode
  label: string
  primary?: boolean
  done?: boolean
  tone?: string
  onClick?: () => void
  disabled?: boolean
}) {
  const base =
    'inline-flex items-center gap-2 h-9 px-3.5 rounded-lg text-[13px] font-semibold transition-all active:scale-[.97] disabled:opacity-60'
  if (done) {
    return (
      <span className={cn(base, 'bg-emerald-50 text-emerald-700 ring-1 ring-emerald-200')}>
        <Check className="w-4 h-4" /> {label}
      </span>
    )
  }
  if (primary) {
    return (
      <button onClick={onClick} disabled={disabled} className={cn(base, 'text-white shadow-soft')} style={{ background: tone || '#2e4a6e' }}>
        {icon} {label}
      </button>
    )
  }
  return (
    <button onClick={onClick} disabled={disabled} className={cn(base, 'bg-white text-navy-700 ring-1 ring-stone-200 hover:ring-stone-300 hover:bg-stone-50')}>
      {icon} {label}
    </button>
  )
}

function InfoRow({
  icon, label, value, mono, href,
}: {
  icon: React.ReactNode
  label: string
  value: string | null | undefined
  mono?: boolean
  href?: string
}) {
  if (!value) return null
  return (
    <div className="flex items-start gap-2.5 py-1.5">
      <span className="text-stone-400 mt-0.5 flex-shrink-0">{icon}</span>
      <div className="min-w-0 flex-1">
        <div className="text-[10px] uppercase tracking-wide font-bold text-stone-400">{label}</div>
        {href ? (
          <a href={href} target="_blank" rel="noopener noreferrer" className="text-[13px] font-medium text-navy-600 hover:underline truncate block">
            {value} <ExternalLink className="inline w-3 h-3 ml-0.5 -mt-0.5" />
          </a>
        ) : (
          <div className={cn('text-[13px] font-medium text-navy-800 truncate', mono && 'font-mono text-xs')}>{value}</div>
        )}
      </div>
    </div>
  )
}

function SectionCard({
  title, icon, children, className,
}: {
  title: string
  icon: React.ReactNode
  children: React.ReactNode
  className?: string
}) {
  return (
    <div className={cn('bg-white rounded-2xl ring-1 ring-stone-200/80 shadow-card', className)}>
      <div className="flex items-center gap-2 px-5 pt-4 pb-2">
        <span className="text-stone-400">{icon}</span>
        <h3 className="text-[11px] font-bold text-stone-400 uppercase tracking-wider">{title}</h3>
      </div>
      <div className="px-5 pb-4">{children}</div>
    </div>
  )
}

/* ── AI intelligence band (real Deep Enrich) ── */

const ENRICH_STEPS = [
  'Scanning email threads…',
  'Cross-referencing LinkedIn & company data…',
  'Matching hospitality brand intel…',
  'Synthesizing profile…',
]

function AIBand({ contact }: { contact: InboxContact }) {
  const deepMut = useDeepEnrichContact()
  const [step, setStep] = useState(0)
  const [result, setResult] = useState<string | null>(null)
  /* [patch_frontend_current_employer] */
  const ceMut = useFindCurrentEmployer()
  const [ceMsg, setCeMsg] = useState<string | null>(null)
  const [ceMoved, setCeMoved] = useState<{ employer: string; title?: string } | null>(null)
  const timer = useRef<number | null>(null)

  // reset when switching contacts
  useEffect(() => {
    setResult(null)
    setStep(0)
    setCeMsg(null)
    setCeMoved(null)
  }, [contact.id])

  // advance the step animation while the mutation is pending
  useEffect(() => {
    if (deepMut.isPending) {
      setStep(0)
      timer.current = window.setInterval(() => {
        setStep((s) => Math.min(s + 1, ENRICH_STEPS.length - 1))
      }, 700)
    } else if (timer.current) {
      window.clearInterval(timer.current)
      timer.current = null
    }
    return () => {
      if (timer.current) window.clearInterval(timer.current)
    }
  }, [deepMut.isPending])

  async function runEnrich(findEmail = false) {
    setResult(null)
    try {
      const r = await deepMut.mutateAsync({ id: contact.id, findEmail })
      const bits = [
        r.role && `Role: ${r.role}`,
        r.background || undefined,
        r.found_email && `Found email: ${r.found_email}`,
        `(${r.sources_used} sources · ${Math.round((r.confidence || 0) * 100)}% confidence)`,
      ].filter(Boolean)
      setResult(bits.join(' · ') || 'No new info found.')
    } catch {
      setResult('Enrichment failed — check Serper/Wiza keys or try again.')
    }
  }

  async function runFindEmployer(apply = false, useWiza = false) {
    setCeMsg(null)
    try {
      const r = await ceMut.mutateAsync({ id: contact.id, apply, useWiza, findEmail: useWiza })
      if (apply) {
        setCeMoved(r.current_employer ? { employer: r.current_employer, title: r.current_title } : null)
        setCeMsg(r.employer_changed ? `Re-filed to ${r.current_employer}. Bio refreshed.` : 'Coverage confirmed current.')
        return
      }
      if (!r.found) { setCeMsg('Could not confirm a current employer (no clear profile match).'); return }
      if (r.moved && r.current_employer) {
        setCeMoved({ employer: r.current_employer, title: r.current_title })
        setCeMsg(`Looks like they moved to ${r.current_employer}${r.current_title ? ` (${r.current_title})` : ''}. Apply to re-file + refresh bio.`)
      } else {
        setCeMsg(`Still at ${r.current_employer || r.on_file_org} — coverage current.`)
      }
    } catch {
      setCeMsg('Lookup failed — check Serper key or try again.')
    }
  }

  const signals = deriveSignals(contact)
  const emailMissing = !contact.email || !contact.email.includes('@')

  return (
    <div
      className="relative overflow-hidden rounded-2xl text-white shadow-lift"
      style={{ background: 'linear-gradient(135deg,#0f1d32 0%,#1a2d4a 55%,#253d5e 100%)' }}
    >
      <div
        className="pointer-events-none absolute -top-16 -right-10 w-56 h-56 rounded-full"
        style={{ background: 'radial-gradient(circle,rgba(212,168,83,.28),transparent 65%)' }}
      />
      <div className="relative px-5 py-4">
        <div className="flex items-center justify-between mb-3">
          <div className="flex items-center gap-2">
            <span className="inline-flex items-center justify-center w-6 h-6 rounded-lg bg-gold-400/20 ring-1 ring-gold-300/40">
              <Sparkles className="w-3.5 h-3.5 text-gold-300" />
            </span>
            <h3 className="text-xs font-bold uppercase tracking-wider text-gold-200">AI Intelligence</h3>
            <span className="text-[10px] font-semibold text-white/40">· {confidencePct(contact)}% confidence</span>
          </div>
          <div className="flex items-center gap-2">
            {emailMissing && !deepMut.isPending && (
              <button
                onClick={() => runEnrich(true)}
                className="inline-flex items-center h-7 px-3 rounded-lg text-[11px] font-bold text-white/90 ring-1 ring-white/25 hover:bg-white/10 transition-all"
                title="Uses Wiza credits"
              >
                Find Email
              </button>
            )}
            {!deepMut.isPending && (
              <button
                onClick={() => runEnrich(false)}
                className="inline-flex items-center gap-1.5 h-7 px-3 rounded-lg text-[11px] font-bold text-navy-900 bg-gold-300 hover:bg-gold-200 transition-all active:scale-95"
              >
                <Wand2 className="w-3 h-3" />
                {result ? 'Re-run' : 'Deep Enrich'}
              </button>
            )}
            {!deepMut.isPending && !ceMut.isPending && (
              <button
                onClick={() => runFindEmployer(false)}
                className="inline-flex items-center gap-1.5 h-7 px-3 rounded-lg text-[11px] font-bold text-white/90 ring-1 ring-white/25 hover:bg-white/10 transition-all"
                title="Find where this contact works now (Serper, free)"
              >
                Where now?
              </button>
            )}
          </div>
        </div>

        {(ceMut.isPending || ceMsg) && (
          <div className="mb-2 rounded-lg bg-white/10 ring-1 ring-white/15 px-3 py-2 text-[12px] text-white/90">
            {ceMut.isPending ? 'Checking where they work now…' : (
              <div className="flex items-center justify-between gap-2">
                <span>{ceMsg}</span>
                {ceMoved && (
                  <span className="flex items-center gap-1.5 shrink-0">
                    <button onClick={() => runFindEmployer(true, false)}
                      className="h-6 px-2 rounded-md text-[11px] font-bold text-navy-900 bg-gold-300 hover:bg-gold-200">Apply</button>
                    <button onClick={() => runFindEmployer(true, true)} title="Verify + find new email (1 Wiza credit)"
                      className="h-6 px-2 rounded-md text-[11px] font-bold text-white/90 ring-1 ring-white/25 hover:bg-white/10">Confirm w/ Wiza</button>
                  </span>
                )}
              </div>
            )}
          </div>
        )}

        {deepMut.isPending ? (
          <div className="py-2 space-y-2">
            {ENRICH_STEPS.map((s, i) => (
              <div key={i} className="flex items-center gap-2.5 text-[13px] transition-all duration-300" style={{ opacity: i <= step ? 1 : 0.35 }}>
                {i < step ? (
                  <Check className="w-3.5 h-3.5 text-emerald-300" />
                ) : i === step ? (
                  <Loader2 className="w-3.5 h-3.5 text-gold-300 animate-spin" />
                ) : (
                  <span className="w-3.5 h-3.5 rounded-full ring-1 ring-white/20" />
                )}
                <span className={i <= step ? 'text-white' : 'text-white/50'}>{s}</span>
              </div>
            ))}
          </div>
        ) : (
          <p className="text-[13.5px] leading-relaxed text-white/85">{deriveSummary(contact)}</p>
        )}

        {result && !deepMut.isPending && (
          <p className="mt-2.5 text-[12.5px] leading-relaxed text-navy-900 bg-gold-200/90 rounded-lg px-3 py-2">{result}</p>
        )}

        {!deepMut.isPending && signals.length > 0 && (
          <div className="flex flex-wrap gap-1.5 mt-3.5">
            {signals.map((s, i) => (
              <span key={i} className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-lg text-[11px] font-semibold bg-white/15 text-white ring-1 ring-white/20 backdrop-blur">
                <span className="w-1.5 h-1.5 rounded-full bg-gold-300" />
                {s}
              </span>
            ))}
          </div>
        )}
      </div>
    </div>
  )
}

/* ── suggested next steps ── */

function NextSteps({ contact }: { contact: InboxContact }) {
  const [done, setDone] = useState<number[]>([])
  useEffect(() => setDone([]), [contact.id])
  const steps = deriveNextSteps(contact)
  const icons = [Send, Phone, Mail, Hash]
  if (steps.length === 0) return null
  return (
    <SectionCard title="AI suggested next steps" icon={<Wand2 className="w-3.5 h-3.5" />}>
      <div className="space-y-2">
        {steps.map((n, i) => {
          const isDone = done.includes(i)
          const Ic = icons[i % icons.length]
          return (
            <button
              key={i}
              onClick={() => setDone((d) => (d.includes(i) ? d.filter((x) => x !== i) : [...d, i]))}
              className={cn(
                'group w-full flex items-center gap-3 text-left px-3 py-2.5 rounded-xl ring-1 transition-all',
                isDone ? 'bg-emerald-50 ring-emerald-200' : 'bg-stone-50 ring-stone-200/70 hover:ring-navy-200 hover:bg-white',
              )}
            >
              <span className={cn('flex-shrink-0 w-7 h-7 rounded-lg flex items-center justify-center text-white', isDone ? 'bg-emerald-500' : 'bg-navy-600')}>
                {isDone ? <Check className="w-3.5 h-3.5" /> : <Ic className="w-3.5 h-3.5" />}
              </span>
              <span className={cn('flex-1 text-[13px] font-medium', isDone ? 'text-emerald-700 line-through' : 'text-navy-800')}>{n}</span>
              <span className={cn('text-[10px] font-bold uppercase tracking-wide', isDone ? 'text-emerald-500' : 'text-stone-300 group-hover:text-navy-400')}>
                {isDone ? 'Done' : 'Do it'}
              </span>
            </button>
          )
        })}
      </div>
    </SectionCard>
  )
}

/* ── engagement timeline ── */

function Timeline({ contact }: { contact: InboxContact }) {
  const events = [
    { t: contact.last_seen, label: 'Last email received', color: '#2e4a6e' },
    isHighOpportunity(contact) ? { t: contact.last_seen, label: 'Flagged high-opportunity by AI', color: '#c49a3c' } : null,
    { t: contact.first_seen, label: 'First seen in inbox', color: '#b0a99e' },
  ].filter(Boolean) as Array<{ t: string | null; label: string; color: string }>

  const recent = (contact.sync_history || []).slice(-3).reverse()

  return (
    <SectionCard title="Engagement" icon={<Activity className="w-3.5 h-3.5" />}>
      <div className="flex items-center gap-4 mb-3 pb-3 border-b border-stone-100">
        <div>
          <div className="text-2xl font-bold text-navy-900 tabular-nums leading-none">{contact.interaction_count}</div>
          <div className="text-[10px] uppercase tracking-wide font-bold text-stone-400 mt-1">Emails</div>
        </div>
        <div className="w-px h-9 bg-stone-200" />
        <div>
          <div className="text-2xl font-bold text-navy-900 tabular-nums leading-none">{contact.source_mailboxes?.length ?? 0}</div>
          <div className="text-[10px] uppercase tracking-wide font-bold text-stone-400 mt-1">Mailboxes</div>
        </div>
        {contact.opportunity_score != null && (
          <>
            <div className="w-px h-9 bg-stone-200" />
            <div>
              <div className="text-2xl font-bold tabular-nums leading-none" style={{ color: contact.opportunity_score >= 75 ? '#e85d4a' : '#c49a3c' }}>
                {contact.opportunity_score}
              </div>
              <div className="text-[10px] uppercase tracking-wide font-bold text-stone-400 mt-1">Opp score</div>
            </div>
          </>
        )}
      </div>

      <div className="relative pl-4 space-y-3">
        <span className="absolute left-[5px] top-1.5 bottom-1.5 w-px bg-stone-200" />
        {events.map((e, i) => (
          <div key={i} className="relative">
            <span className="absolute -left-4 top-1 w-2.5 h-2.5 rounded-full ring-2 ring-white" style={{ background: e.color }} />
            <div className="text-[13px] font-medium text-navy-800">{e.label}</div>
            <div className="text-[11px] text-stone-400">{formatDate(e.t)} · {relativeDate(e.t)}</div>
          </div>
        ))}
      </div>

      {recent.length > 0 && (
        <div className="mt-3 pt-3 border-t border-stone-100 space-y-0.5">
          {recent.map((evt, i) => (
            <div key={i} className="text-[11px] text-stone-400 font-mono">
              {evt.ts ? new Date(evt.ts).toLocaleString() : '—'} · {evt.action}
              {evt.mailbox && ` · ${evt.mailbox}`}
            </div>
          ))}
        </div>
      )}

      {contact.source_mailboxes && contact.source_mailboxes.length > 0 && (
        <div className="mt-3 pt-3 border-t border-stone-100 text-[11px] text-stone-400">
          Source: <span className="font-mono text-stone-500">{contact.source_mailboxes.join(', ')}</span>
        </div>
      )}
    </SectionCard>
  )
}

/* ── main panel ── */

export default function ContactProfile({ contact }: { contact: InboxContact | null }) {
  const approveMut = useApproveInboxContact()
  const junkMut = useJunkContact()
  const unjunkMut = useUnjunkContact()
  const junkDomainMut = useJunkDomain()
  const [showJunkMenu, setShowJunkMenu] = useState(false)

  if (!contact) {
    return (
      <div className="flex-1 flex flex-col items-center justify-center text-stone-400 bg-stone-100/40">
        <Users className="w-11 h-11 text-stone-300 mb-3" />
        <p className="text-sm font-semibold text-stone-500">Select a contact</p>
        <p className="text-xs mt-1">Their live AI profile appears here</p>
      </div>
    )
  }

  const inCrm = !!contact.insightly_contact_id || !!contact.pushed_to_insightly_at || contact.approval_status === 'approved'
  const isJunked = contact.manual_category === 'junk'
  const junkDomainName = (contact.email || '').split('@')[1] || ''

  return (
    <div key={contact.id} className="flex-1 overflow-y-auto bg-stone-100/50">
      {/* hero */}
      <div className="relative px-7 pt-7 pb-6 text-white overflow-hidden" style={{ background: 'linear-gradient(120deg,#0a1628 0%,#152844 60%,#1f3a5c 100%)' }}>
        <div className="pointer-events-none absolute inset-0 opacity-60" style={{ background: 'radial-gradient(900px 300px at 90% -20%, rgba(212,168,83,.18), transparent 60%)' }} />
        <div className="relative flex items-start gap-4">
          <Avatar contact={contact} size={64} showPip={false} />
          <div className="min-w-0 flex-1">
            <div className="flex items-center gap-2 flex-wrap">
              <h2 className="text-[22px] font-bold leading-tight">{fullName(contact)}</h2>
              {contact.is_decision_maker && (
                <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[10px] font-bold bg-gold-400 text-navy-900">
                  <Star className="w-2.5 h-2.5" fill="#0f1d32" strokeWidth={0} /> DECISION-MAKER
                </span>
              )}
            </div>
            <p className="text-[14px] text-white/80 mt-0.5">
              {roleText(contact) || <span className="italic text-white/50">role unknown</span>}
              <span className="text-white/40">{'  at  '}</span>
              <span className="font-semibold text-white">{contact.organization || '—'}</span>
            </p>
            <div className="flex items-center gap-3 mt-2 text-[12px] text-white/60">
              {contact.address && (
                <span className="inline-flex items-center gap-1"><MapPin className="w-3 h-3" />{contact.address}</span>
              )}
              <span className="inline-flex items-center gap-1"><Mail className="w-3 h-3" />In inbox {relativeDate(contact.last_seen)}</span>
              <CategoryBadge category={contact.contact_category} />
              <StaleBadge contact={contact} />
            </div>
          </div>
        </div>

        {/* actions */}
        <div className="relative flex flex-wrap items-center gap-2 mt-5">
          <ActionBtn primary tone="#c49a3c" icon={<Send className="w-4 h-4" />} label="Draft outreach" />
          {contact.email && <ActionBtn icon={<Mail className="w-4 h-4" />} label="Email" onClick={() => { window.location.href = `mailto:${contact.email}` }} />}
          {contact.phone && <ActionBtn icon={<Phone className="w-4 h-4" />} label="Call" onClick={() => { window.location.href = `tel:${contact.phone}` }} />}
          {contact.linkedin_url && <ActionBtn icon={<Linkedin className="w-4 h-4" />} label="LinkedIn" onClick={() => window.open(contact.linkedin_url!, '_blank')} />}
          <div className="flex-1" />
          {isJunked ? (
            <button
              onClick={() => unjunkMut.mutate(contact.id)}
              disabled={unjunkMut.isPending}
              className="inline-flex items-center gap-2 h-9 px-3.5 rounded-lg text-[13px] font-semibold text-white/90 ring-1 ring-white/25 hover:bg-white/10 transition-all active:scale-[.97] disabled:opacity-60"
            >
              <RotateCcw className="w-4 h-4" /> Restore
            </button>
          ) : (
            <div className="relative">
              <button
                onClick={() => setShowJunkMenu((v) => !v)}
                className="inline-flex items-center gap-2 h-9 px-3.5 rounded-lg text-[13px] font-semibold text-white/80 ring-1 ring-white/25 hover:bg-white/10 transition-all active:scale-[.97]"
              >
                <Ban className="w-4 h-4" /> Junk
              </button>
              {showJunkMenu && (
                <div className="absolute right-0 top-11 z-30 w-64 rounded-xl bg-white shadow-lift ring-1 ring-stone-200 overflow-hidden text-left">
                  <button
                    onClick={() => { junkMut.mutate(contact.id); setShowJunkMenu(false) }}
                    className="w-full text-left px-4 py-2.5 text-[13px] font-medium text-navy-800 hover:bg-stone-50"
                  >
                    Junk just this contact
                  </button>
                  {junkDomainName && (
                    <button
                      onClick={() => { junkDomainMut.mutate({ domain: junkDomainName }); setShowJunkMenu(false) }}
                      className="w-full text-left px-4 py-2.5 text-[13px] font-medium text-red-600 hover:bg-red-50 border-t border-stone-100"
                    >
                      Junk all of @{junkDomainName}
                      <span className="block text-[11px] font-normal text-stone-400 mt-0.5">
                        Auto-junks every contact from this domain — now and in future syncs
                      </span>
                    </button>
                  )}
                </div>
              )}
            </div>
          )}
          <ActionBtn
            done={inCrm}
            primary={!inCrm}
            disabled={approveMut.isPending}
            icon={<ExternalLink className="w-4 h-4" />}
            label={inCrm ? 'In CRM' : approveMut.isPending ? 'Pushing…' : 'Push to CRM'}
            onClick={() => approveMut.mutate(contact.id)}
          />
        </div>
      </div>

      {/* body */}
      <div className="p-5 space-y-4">
        <AIBand contact={contact} />
        <NextSteps contact={contact} />

        <div className="grid grid-cols-2 gap-4">
          <SectionCard title="Contact" icon={<Users className="w-3.5 h-3.5" />}>
            <InfoRow icon={<Mail className="w-3.5 h-3.5" />} label="Email" value={contact.email} mono />
            <InfoRow icon={<Phone className="w-3.5 h-3.5" />} label="Phone" value={contact.phone} />
            <InfoRow icon={<MapPin className="w-3.5 h-3.5" />} label="Address" value={contact.address} />
            <InfoRow icon={<Linkedin className="w-3.5 h-3.5" />} label="LinkedIn" value={contact.linkedin_url ? 'View profile' : null} href={contact.linkedin_url || undefined} />
            <InfoRow icon={<Hash className="w-3.5 h-3.5" />} label="Department" value={contact.department} />
            <InfoRow icon={<Eye className="w-3.5 h-3.5" />} label="Seniority" value={contact.seniority} />
          </SectionCard>

          <SectionCard title="Hospitality intel" icon={<Building2 className="w-3.5 h-3.5" />}>
            <InfoRow icon={<Building2 className="w-3.5 h-3.5" />} label="Organization" value={contact.organization} />
            <InfoRow icon={<Shield className="w-3.5 h-3.5" />} label="Parent company" value={contact.parent_company} />
            <InfoRow icon={<Sparkles className="w-3.5 h-3.5" />} label="Brand tier" value={contact.brand_tier ? getTierLabel(contact.brand_tier) : null} />
            <InfoRow icon={<Hash className="w-3.5 h-3.5" />} label="Management co." value={contact.management_company} />
            <InfoRow icon={<Shield className="w-3.5 h-3.5" />} label="GPO" value={contact.gpo} />
            <InfoRow
              icon={<Activity className="w-3.5 h-3.5" />}
              label="Opportunity"
              value={contact.opportunity_level ? `${contact.opportunity_level}${contact.opportunity_score != null ? ` · ${contact.opportunity_score}/100` : ''}` : null}
            />
          </SectionCard>
        </div>

        <Timeline contact={contact} />
      </div>
    </div>
  )
}
