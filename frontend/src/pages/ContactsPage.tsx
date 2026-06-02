/**
 * ContactsPage — AI-first, split-view contact directory (FULL, self-contained).
 * ============================================================================
 * Drop-in replacement. This single file contains everything: helpers, small
 * presentational components, the list rail, the live AI profile, and the full
 * triage workflow. The only imports are your existing hooks / api / utils.
 *
 *   Left rail  : natural-language "ask AI" search, decision-maker focus,
 *                category + status filters, sort, multi-select bulk-approve.
 *   Right panel: live AI profile — Deep Enrich (+ Find Email), suggested next
 *                steps, contact + hospitality intel, engagement timeline, and
 *                Approve / Push-to-CRM / Reject actions.
 *
 * Server-side filtering: category + approval status + ordering.
 * Client-side (over the loaded page, per_page 200): smart-search, DM focus,
 * sort. For very large inboxes, push `search` to the server (marked below).
 */
import { useEffect, useMemo, useRef, useState } from 'react'
import { useSearchParams } from 'react-router-dom'
import {
  Sparkles, Wand2, X, RefreshCw, Filter, Inbox, Star, TrendingUp,
  Mail, Phone, Linkedin, ExternalLink, MapPin, Building2, Shield, Hash,
  Eye, Users, Activity, Send, Check, Loader2, Trash2, CheckSquare, Square,
} from 'lucide-react'
import { cn, formatDate, relativeDate, getTierLabel } from '@/lib/utils'
import type { InboxContact, InboxContactStats } from '@/api/inboxContacts'
import {
  useInboxContacts,
  useInboxContactStats,
  useTriggerInboxSync,
  useDeepEnrichContact,
  useApproveInboxContact,
  useBulkApproveInboxContacts,
  useDeleteInboxContact,
} from '@/hooks/useInboxContacts'

/* ════════════════════════════════════════════════════════════════════
   HELPERS
   ════════════════════════════════════════════════════════════════════ */

type SortKey = 'confidence' | 'opportunity' | 'recent' | 'name'

function fullName(c: InboxContact): string {
  return [c.first_name, c.last_name].filter(Boolean).join(' ') || c.display_name || c.email || '—'
}
function initials(c: InboxContact): string {
  const both = ((c.first_name?.[0] || '') + (c.last_name?.[0] || '')).toUpperCase()
  return both || (c.display_name?.[0] || c.email?.[0] || '?').toUpperCase()
}
/** Backend stores confidence as 0–1; surface a 0–100 integer. */
function confidencePct(c: InboxContact): number {
  const v = c.confidence ?? c.enrichment_confidence ?? 0
  return Math.round(v <= 1 ? v * 100 : v)
}
function roleText(c: InboxContact): string | null {
  return c.title || c.inferred_role || null
}
function isHighOpportunity(c: InboxContact): boolean {
  return (c.opportunity_level || '').toLowerCase() === 'high'
}

const AVATAR_GRADIENT: Record<string, string> = {
  buyer: 'linear-gradient(135deg,#1a7a55,#0f5c3e)',
  seller: 'linear-gradient(135deg,#c49a3c,#a8832e)',
  competitor: 'linear-gradient(135deg,#e85d4a,#d14836)',
  personal: 'linear-gradient(135deg,#3e638c,#253d5e)',
  junk: 'linear-gradient(135deg,#b0a99e,#8a847b)',
}
const avatarGradient = (cat: string | null) => AVATAR_GRADIENT[cat || 'junk'] || AVATAR_GRADIENT.junk

const CATEGORY_BADGE: Record<string, string> = {
  buyer: 'bg-emerald-50 text-emerald-700 ring-1 ring-emerald-200',
  seller: 'bg-gold-50 text-gold-700 ring-1 ring-gold-200',
  competitor: 'bg-coral-50 text-coral-600 ring-1 ring-coral-200',
  personal: 'bg-navy-50 text-navy-600 ring-1 ring-navy-200',
  junk: 'bg-stone-100 text-stone-500 ring-1 ring-stone-200',
}

/** Signal chips — derived from concrete fields (heuristics, not new API data). */
function deriveSignals(c: InboxContact): string[] {
  const out: string[] = []
  if (c.is_decision_maker) out.push('Decision-maker')
  if (isHighOpportunity(c)) out.push('High opportunity')
  if (c.gpo) out.push(`${c.gpo} GPO`)
  if (c.seniority && /exec|director|chief|vp|head/i.test(c.seniority)) out.push(c.seniority)
  if (c.brand_tier && /tier1|tier2|luxury/i.test(c.brand_tier)) out.push(getTierLabel(c.brand_tier))
  if (c.phone) out.push('Direct dial on file')
  if (c.interaction_count >= 8) out.push(`${c.interaction_count} emails`)
  return out.slice(0, 4)
}

/** Suggested next steps — heuristic, based on the contact's shape. */
function deriveNextSteps(c: InboxContact): string[] {
  const cat = c.contact_category
  const steps: string[] = []
  if (cat === 'competitor') return ['Flag as competitor in CRM', 'Keep replies factual — withhold pricing']
  if (cat === 'seller' || c.gpo) {
    steps.push(c.gpo ? `Align on ${c.gpo} approved-supplier path` : 'Submit supplier-application packet', 'Share pricing & capacity sheet')
  }
  if (cat === 'buyer' || !cat) {
    steps.push(c.is_decision_maker ? 'Draft a tailored proposal' : 'Identify the decision-maker / approver')
    if (isHighOpportunity(c)) steps.push('Prioritize outreach within 7 days')
  }
  if (!c.phone) steps.push('Find a direct dial (Deep Enrich)')
  if (!c.matched_lead_id && !c.matched_hotel_id) steps.push('Match to a lead or hotel record')
  return Array.from(new Set(steps)).slice(0, 3)
}

/** AI summary text: real enriched background, else a composed fallback. */
function deriveSummary(c: InboxContact): string {
  if (c.background) return c.background
  const role = (c.seniority || roleText(c) || 'a contact').toString().toLowerCase()
  const dept = c.department ? ` in ${c.department.toLowerCase()}` : ''
  const org = c.organization ? ` at ${c.organization}` : ''
  const opp = isHighOpportunity(c) ? ' Flagged as a high-opportunity account — prioritize outreach.' : ''
  return `${fullName(c)} appears to be ${role}${dept}${org}.${opp} Run Deep Enrich to generate a full AI profile.`
}

/** Map natural-language queries to a predicate over a contact. */
function smartMatch(c: InboxContact, query: string): boolean {
  const t = query.toLowerCase().trim()
  if (!t) return true
  const hay = [
    c.first_name, c.last_name, c.display_name, c.title, c.inferred_role, c.organization,
    c.email, c.address, c.department, c.parent_company, c.gpo, getTierLabel(c.brand_tier),
    c.contact_category, c.opportunity_level,
  ].filter(Boolean).join(' ').toLowerCase()
  if (/decision|maker|\bdm\b/.test(t)) return !!c.is_decision_maker
  if (/high opp|opportun|hot lead/.test(t)) return isHighOpportunity(c)
  if (/repl|recent|engaged|active/.test(t)) return c.interaction_count >= 5
  if (/luxury|premium|upscale|high.?end/.test(t)) return /luxury|upscale/i.test(getTierLabel(c.brand_tier) || '')
  if (/buyer/.test(t)) return c.contact_category === 'buyer'
  if (/seller|gpo/.test(t)) return c.contact_category === 'seller'
  if (/competitor/.test(t)) return c.contact_category === 'competitor'
  if (/phone|call|dial/.test(t)) return !!c.phone
  return t.split(/\s+/).every((w) => hay.includes(w))
}

function inCrm(c: InboxContact): boolean {
  return !!c.insightly_contact_id || !!c.pushed_to_insightly_at || c.approval_status === 'pushed_to_insightly'
}

/* ════════════════════════════════════════════════════════════════════
   SMALL PRESENTATIONAL COMPONENTS
   ════════════════════════════════════════════════════════════════════ */

function CategoryBadge({ category }: { category: string | null }) {
  if (!category) return <span className="text-stone-300 text-xs">—</span>
  return (
    <span className={cn('inline-flex items-center px-2 py-0.5 rounded-full text-[10px] font-bold capitalize', CATEGORY_BADGE[category] || CATEGORY_BADGE.junk)}>
      {category}
    </span>
  )
}

function ConfRing({ value, size = 34 }: { value: number; size?: number }) {
  const r = (size - 5) / 2
  const c = 2 * Math.PI * r
  const off = c * (1 - value / 100)
  const col = value >= 90 ? '#1a7a55' : value >= 75 ? '#c49a3c' : '#b0a99e'
  return (
    <div className="relative flex-shrink-0" style={{ width: size, height: size }}>
      <svg width={size} height={size} className="-rotate-90">
        <circle cx={size / 2} cy={size / 2} r={r} fill="none" stroke="#edebe5" strokeWidth={3} />
        <circle cx={size / 2} cy={size / 2} r={r} fill="none" stroke={col} strokeWidth={3}
          strokeDasharray={c} strokeDashoffset={off} strokeLinecap="round"
          style={{ transition: 'stroke-dashoffset .6s cubic-bezier(.16,1,.3,1)' }} />
      </svg>
      <span className="absolute inset-0 flex items-center justify-center text-[9px] font-bold tabular-nums" style={{ color: col }}>{value}</span>
    </div>
  )
}

function Avatar({ contact, size = 40, showPip = true }: { contact: InboxContact; size?: number; showPip?: boolean }) {
  return (
    <div className="relative flex-shrink-0 rounded-full flex items-center justify-center text-white font-bold select-none"
      style={{ width: size, height: size, fontSize: size * 0.36, background: avatarGradient(contact.contact_category), boxShadow: '0 2px 6px rgba(15,29,50,.18), inset 0 1px 0 rgba(255,255,255,.25)' }}>
      {initials(contact)}
      {showPip && contact.is_decision_maker && (
        <span title="Likely decision-maker" className="absolute -bottom-0.5 -right-0.5 w-[18px] h-[18px] rounded-full bg-gold-400 ring-2 ring-white flex items-center justify-center">
          <Star className="w-2.5 h-2.5 text-white" fill="white" strokeWidth={0} />
        </span>
      )}
    </div>
  )
}

function Chip({ label, count, active, color, onClick }: { label: string; count?: number; active: boolean; color?: string; onClick: () => void }) {
  return (
    <button onClick={onClick}
      className={cn('inline-flex items-center gap-1.5 px-3 h-8 rounded-lg text-xs font-semibold whitespace-nowrap transition-all',
        active ? 'text-white shadow-soft' : 'text-stone-500 bg-stone-100/70 hover:bg-stone-200/60')}
      style={active ? { background: color || '#2e4a6e' } : undefined}>
      {label}
      {count != null && (
        <span className={cn('tabular-nums px-1.5 py-0.5 rounded-md text-[10px]', active ? 'bg-white/25' : 'bg-white text-stone-400')}>{count.toLocaleString()}</span>
      )}
    </button>
  )
}

/* ════════════════════════════════════════════════════════════════════
   LIST ROW
   ════════════════════════════════════════════════════════════════════ */

function ContactRow({
  contact, active, selectMode, checked, onOpen, onToggleCheck,
}: {
  contact: InboxContact
  active: boolean
  selectMode: boolean
  checked: boolean
  onOpen: () => void
  onToggleCheck: () => void
}) {
  const pending = contact.approval_status === 'pending'
  return (
    <div className={cn('group w-full flex items-center gap-2.5 px-2 py-3 rounded-xl transition-all duration-150 relative cursor-pointer',
      active ? 'bg-white shadow-card ring-1 ring-navy-100' : 'hover:bg-white/70')}
      onClick={onOpen}>
      {active && <span className="absolute left-0 top-1/2 -translate-y-1/2 h-7 w-1 rounded-r-full bg-navy-600" />}

      {/* checkbox — visible on hover, when selected, or in select mode */}
      <button
        onClick={(e) => { e.stopPropagation(); onToggleCheck() }}
        className={cn('flex-shrink-0 transition-opacity', checked || selectMode ? 'opacity-100' : 'opacity-0 group-hover:opacity-100')}
        title={checked ? 'Deselect' : 'Select'}>
        {checked ? <CheckSquare className="w-4 h-4 text-navy-600" /> : <Square className="w-4 h-4 text-stone-300" />}
      </button>

      <Avatar contact={contact} size={40} />
      <div className="min-w-0 flex-1">
        <div className="flex items-center gap-1.5">
          <span className="truncate font-semibold text-navy-900 text-sm">{fullName(contact)}</span>
          {contact.is_decision_maker && <span className="text-gold-500 text-[10px] flex-shrink-0" title="Decision-maker">★</span>}
          {pending && <span className="ml-auto flex-shrink-0 w-1.5 h-1.5 rounded-full bg-coral-400" title="Pending review" />}
        </div>
        <div className="truncate text-stone-500 text-xs mt-0.5">
          {roleText(contact) || <span className="italic text-stone-400">role unknown</span>}
          <span className="text-stone-300">{'  ·  '}</span>
          <span className="text-stone-400 font-medium">{contact.organization || '—'}</span>
        </div>
        <div className="flex items-center gap-2 mt-1.5">
          <CategoryBadge category={contact.contact_category} />
          {isHighOpportunity(contact) && (
            <span className="inline-flex items-center gap-1 text-[10px] font-bold text-coral-500 whitespace-nowrap flex-shrink-0">
              <TrendingUp className="w-3 h-3" /> High opp
            </span>
          )}
          <span className="text-[10px] text-stone-400 ml-auto whitespace-nowrap flex-shrink-0">{relativeDate(contact.last_seen)}</span>
        </div>
      </div>
      <ConfRing value={confidencePct(contact)} size={34} />
    </div>
  )
}

/* ════════════════════════════════════════════════════════════════════
   AI INTELLIGENCE BAND (real Deep Enrich)
   ════════════════════════════════════════════════════════════════════ */

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
  const timer = useRef<number | null>(null)

  useEffect(() => { setResult(null); setStep(0) }, [contact.id])

  useEffect(() => {
    if (deepMut.isPending) {
      setStep(0)
      timer.current = window.setInterval(() => setStep((s) => Math.min(s + 1, ENRICH_STEPS.length - 1)), 700)
    } else if (timer.current) {
      window.clearInterval(timer.current); timer.current = null
    }
    return () => { if (timer.current) window.clearInterval(timer.current) }
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

  const signals = deriveSignals(contact)
  const emailMissing = !contact.email || !contact.email.includes('@')

  return (
    <div className="relative overflow-hidden rounded-2xl text-white shadow-lift" style={{ background: 'linear-gradient(135deg,#0f1d32 0%,#1a2d4a 55%,#253d5e 100%)' }}>
      <div className="pointer-events-none absolute -top-16 -right-10 w-56 h-56 rounded-full" style={{ background: 'radial-gradient(circle,rgba(212,168,83,.28),transparent 65%)' }} />
      <div className="relative px-5 py-4">
        <div className="flex items-center justify-between mb-3">
          <div className="flex items-center gap-2">
            <span className="inline-flex items-center justify-center w-6 h-6 rounded-lg bg-gold-400/20 ring-1 ring-gold-300/40"><Sparkles className="w-3.5 h-3.5 text-gold-300" /></span>
            <h3 className="text-xs font-bold uppercase tracking-wider text-gold-200">AI Intelligence</h3>
            <span className="text-[10px] font-semibold text-white/40">· {confidencePct(contact)}% confidence</span>
          </div>
          <div className="flex items-center gap-2">
            {emailMissing && !deepMut.isPending && (
              <button onClick={() => runEnrich(true)} title="Uses Wiza credits"
                className="inline-flex items-center h-7 px-3 rounded-lg text-[11px] font-bold text-white/90 ring-1 ring-white/25 hover:bg-white/10 transition-all">Find Email</button>
            )}
            {!deepMut.isPending && (
              <button onClick={() => runEnrich(false)}
                className="inline-flex items-center gap-1.5 h-7 px-3 rounded-lg text-[11px] font-bold text-navy-900 bg-gold-300 hover:bg-gold-200 transition-all active:scale-95">
                <Wand2 className="w-3 h-3" />{result ? 'Re-run' : 'Deep Enrich'}
              </button>
            )}
          </div>
        </div>

        {deepMut.isPending ? (
          <div className="py-2 space-y-2">
            {ENRICH_STEPS.map((s, i) => (
              <div key={i} className="flex items-center gap-2.5 text-[13px] transition-all duration-300" style={{ opacity: i <= step ? 1 : 0.35 }}>
                {i < step ? <Check className="w-3.5 h-3.5 text-emerald-300" />
                  : i === step ? <Loader2 className="w-3.5 h-3.5 text-gold-300 animate-spin" />
                  : <span className="w-3.5 h-3.5 rounded-full ring-1 ring-white/20" />}
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
                <span className="w-1.5 h-1.5 rounded-full bg-gold-300" />{s}
              </span>
            ))}
          </div>
        )}
      </div>
    </div>
  )
}

/* ════════════════════════════════════════════════════════════════════
   SECTION CARD + INFO ROW + NEXT STEPS + TIMELINE
   ════════════════════════════════════════════════════════════════════ */

function SectionCard({ title, icon, children }: { title: string; icon: React.ReactNode; children: React.ReactNode }) {
  return (
    <div className="bg-white rounded-2xl ring-1 ring-stone-200/80 shadow-card">
      <div className="flex items-center gap-2 px-5 pt-4 pb-2">
        <span className="text-stone-400">{icon}</span>
        <h3 className="text-[11px] font-bold text-stone-400 uppercase tracking-wider">{title}</h3>
      </div>
      <div className="px-5 pb-4">{children}</div>
    </div>
  )
}

function InfoRow({ icon, label, value, mono, href }: { icon: React.ReactNode; label: string; value: string | null | undefined; mono?: boolean; href?: string }) {
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
            <button key={i} onClick={() => setDone((d) => (d.includes(i) ? d.filter((x) => x !== i) : [...d, i]))}
              className={cn('group w-full flex items-center gap-3 text-left px-3 py-2.5 rounded-xl ring-1 transition-all',
                isDone ? 'bg-emerald-50 ring-emerald-200' : 'bg-stone-50 ring-stone-200/70 hover:ring-navy-200 hover:bg-white')}>
              <span className={cn('flex-shrink-0 w-7 h-7 rounded-lg flex items-center justify-center text-white', isDone ? 'bg-emerald-500' : 'bg-navy-600')}>
                {isDone ? <Check className="w-3.5 h-3.5" /> : <Ic className="w-3.5 h-3.5" />}
              </span>
              <span className={cn('flex-1 text-[13px] font-medium', isDone ? 'text-emerald-700 line-through' : 'text-navy-800')}>{n}</span>
              <span className={cn('text-[10px] font-bold uppercase tracking-wide', isDone ? 'text-emerald-500' : 'text-stone-300 group-hover:text-navy-400')}>{isDone ? 'Done' : 'Do it'}</span>
            </button>
          )
        })}
      </div>
    </SectionCard>
  )
}

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
              <div className="text-2xl font-bold tabular-nums leading-none" style={{ color: contact.opportunity_score >= 75 ? '#e85d4a' : '#c49a3c' }}>{contact.opportunity_score}</div>
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
              {evt.ts ? new Date(evt.ts).toLocaleString() : '—'} · {evt.action}{evt.mailbox && ` · ${evt.mailbox}`}
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

/* ════════════════════════════════════════════════════════════════════
   PROFILE PANEL
   ════════════════════════════════════════════════════════════════════ */

function ActionBtn({ icon, label, primary, done, tone, onClick, disabled }: {
  icon?: React.ReactNode; label: string; primary?: boolean; done?: boolean; tone?: string; onClick?: () => void; disabled?: boolean
}) {
  const base = 'inline-flex items-center gap-2 h-9 px-3.5 rounded-lg text-[13px] font-semibold transition-all active:scale-[.97] disabled:opacity-60'
  if (done) return <span className={cn(base, 'bg-emerald-50 text-emerald-700 ring-1 ring-emerald-200')}><Check className="w-4 h-4" /> {label}</span>
  if (primary) return <button onClick={onClick} disabled={disabled} className={cn(base, 'text-white shadow-soft')} style={{ background: tone || '#2e4a6e' }}>{icon} {label}</button>
  return <button onClick={onClick} disabled={disabled} className={cn(base, 'bg-white text-navy-700 ring-1 ring-stone-200 hover:ring-stone-300 hover:bg-stone-50')}>{icon} {label}</button>
}

function ProfilePanel({ contact, onDeleted }: { contact: InboxContact | null; onDeleted: () => void }) {
  const approveMut = useApproveInboxContact()
  const deleteMut = useDeleteInboxContact()
  const [confirmDelete, setConfirmDelete] = useState(false)

  useEffect(() => setConfirmDelete(false), [contact?.id])

  if (!contact) {
    return (
      <div className="flex-1 flex flex-col items-center justify-center text-stone-400 bg-stone-100/40">
        <Users className="w-11 h-11 text-stone-300 mb-3" />
        <p className="text-sm font-semibold text-stone-500">Select a contact</p>
        <p className="text-xs mt-1">Their live AI profile appears here</p>
      </div>
    )
  }

  const crm = inCrm(contact)
  const approved = contact.approval_status === 'approved' || crm

  function handleDelete() {
    if (!confirmDelete) { setConfirmDelete(true); return }
    deleteMut.mutate(contact!.id, { onSuccess: onDeleted })
  }

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
              {approved && (
                <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[10px] font-bold bg-emerald-500/20 text-emerald-200 ring-1 ring-emerald-300/30">
                  {crm ? 'IN CRM' : 'APPROVED'}
                </span>
              )}
            </div>
            <p className="text-[14px] text-white/80 mt-0.5">
              {roleText(contact) || <span className="italic text-white/50">role unknown</span>}
              <span className="text-white/40">{'  at  '}</span>
              <span className="font-semibold text-white">{contact.organization || '—'}</span>
            </p>
            <div className="flex items-center gap-3 mt-2 text-[12px] text-white/60">
              {contact.address && <span className="inline-flex items-center gap-1"><MapPin className="w-3 h-3" />{contact.address}</span>}
              <span className="inline-flex items-center gap-1"><Mail className="w-3 h-3" />In inbox {relativeDate(contact.last_seen)}</span>
              <CategoryBadge category={contact.contact_category} />
            </div>
          </div>
        </div>

        {/* actions */}
        <div className="relative flex flex-wrap items-center gap-2 mt-5">
          <ActionBtn primary tone="#c49a3c" icon={<Send className="w-4 h-4" />} label="Draft outreach"
            onClick={() => contact.email && (window.location.href = `mailto:${contact.email}`)} />
          {contact.email && <ActionBtn icon={<Mail className="w-4 h-4" />} label="Email" onClick={() => (window.location.href = `mailto:${contact.email}`)} />}
          {contact.phone && <ActionBtn icon={<Phone className="w-4 h-4" />} label="Call" onClick={() => (window.location.href = `tel:${contact.phone}`)} />}
          {contact.linkedin_url && <ActionBtn icon={<Linkedin className="w-4 h-4" />} label="LinkedIn" onClick={() => window.open(contact.linkedin_url!, '_blank')} />}
          <div className="flex-1" />
          {/* Reject (delete) */}
          <button onClick={handleDelete} disabled={deleteMut.isPending}
            className={cn('inline-flex items-center gap-2 h-9 px-3.5 rounded-lg text-[13px] font-semibold transition-all active:scale-[.97] disabled:opacity-60',
              confirmDelete ? 'bg-coral-500 text-white' : 'bg-white/10 text-white/80 ring-1 ring-white/20 hover:bg-white/15')}>
            <Trash2 className="w-4 h-4" /> {confirmDelete ? 'Confirm reject?' : 'Reject'}
          </button>
          {/* Approve / Push to CRM */}
          <ActionBtn done={crm} primary={!crm} disabled={approveMut.isPending}
            icon={<ExternalLink className="w-4 h-4" />}
            label={crm ? 'In CRM' : approveMut.isPending ? 'Saving…' : approved ? 'Push to CRM' : 'Approve'}
            onClick={() => approveMut.mutate(contact.id)} />
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
            <InfoRow icon={<Activity className="w-3.5 h-3.5" />} label="Opportunity"
              value={contact.opportunity_level ? `${contact.opportunity_level}${contact.opportunity_score != null ? ` · ${contact.opportunity_score}/100` : ''}` : null} />
            <InfoRow icon={<ExternalLink className="w-3.5 h-3.5" />} label="Matched lead" value={contact.matched_lead_id ? `#${contact.matched_lead_id}` : null} />
            <InfoRow icon={<ExternalLink className="w-3.5 h-3.5" />} label="Matched hotel" value={contact.matched_hotel_id ? `#${contact.matched_hotel_id}` : null} />
          </SectionCard>
        </div>

        {contact.priority_reason && (
          <div className="text-xs text-stone-500 bg-white rounded-2xl ring-1 ring-stone-200/80 shadow-card px-5 py-3.5">
            <span className="font-bold text-stone-400 uppercase tracking-wider text-[10px]">Why this priority</span>
            <p className="mt-1 leading-relaxed">{contact.priority_reason}</p>
          </div>
        )}

        <Timeline contact={contact} />
      </div>
    </div>
  )
}

/* ════════════════════════════════════════════════════════════════════
   MAIN PAGE
   ════════════════════════════════════════════════════════════════════ */

const STATUS_OPTIONS: Array<[string, string]> = [
  ['', 'All status'],
  ['pending', 'Pending'],
  ['approved', 'Approved'],
  ['pushed_to_insightly', 'In CRM'],
]

export default function ContactsPage() {
  const [params, setParams] = useSearchParams()

  // URL-synced state
  const query = params.get('q') || ''
  const category = params.get('category') || ''
  const status = params.get('status') || ''
  const dmOnly = params.get('dm') === '1'
  const sort = (params.get('sort') as SortKey) || 'confidence'
  const selectedId = params.get('selected') ? Number(params.get('selected')) : null

  // local state
  const [selected, setSelected] = useState<Set<number>>(new Set())

  function patch(updates: Record<string, string | null>) {
    const next = new URLSearchParams(params)
    for (const [k, v] of Object.entries(updates)) {
      if (v) next.set(k, v)
      else next.delete(k)
    }
    setParams(next, { replace: true })
  }

  // data
  const statsQ = useInboxContactStats()
  const listQ = useInboxContacts({
    per_page: 200,
    contact_category: category || undefined,
    approval_status: status || undefined,
    order_by: 'priority_score',
    // For very large inboxes, also pass: search: query || undefined  (and drop the client-side text match below)
  })
  const syncMut = useTriggerInboxSync()
  const bulkApproveMut = useBulkApproveInboxContacts()

  const stats: InboxContactStats | undefined = statsQ.data
  const items = listQ.data?.items || []
  const total = listQ.data?.total || 0

  // client-side refine + sort
  const filtered = useMemo(() => {
    let list = items.filter((c) => smartMatch(c, query))
    if (dmOnly) list = list.filter((c) => c.is_decision_maker)
    const sorters: Record<SortKey, (a: InboxContact, b: InboxContact) => number> = {
      confidence: (a, b) => confidencePct(b) - confidencePct(a),
      opportunity: (a, b) => (b.opportunity_score ?? 0) - (a.opportunity_score ?? 0),
      recent: (a, b) => new Date(b.last_seen || 0).getTime() - new Date(a.last_seen || 0).getTime(),
      name: (a, b) => `${a.first_name || ''}${a.last_name || ''}`.localeCompare(`${b.first_name || ''}${b.last_name || ''}`),
    }
    return [...list].sort(sorters[sort])
  }, [items, query, dmOnly, sort])

  // keep a valid selection
  useEffect(() => {
    if (!filtered.length) return
    if (selectedId == null || !filtered.some((c) => c.id === selectedId)) {
      patch({ selected: String(filtered[0].id) })
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [filtered, selectedId])

  const activeContact = items.find((c) => c.id === selectedId) || filtered[0] || null

  const [focused, setFocused] = useState(false)
  const smartActive = query.trim().length > 0
  const selectMode = selected.size > 0

  function toggleCheck(id: number) {
    setSelected((prev) => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })
  }
  function clearSelection() { setSelected(new Set()) }
  function bulkApprove() {
    bulkApproveMut.mutate(Array.from(selected), { onSuccess: clearSelection })
  }
  function selectAllVisible() { setSelected(new Set(filtered.map((c) => c.id))) }

  const suggestions: Array<[string, () => void]> = [
    ['★ Decision-makers', () => { patch({ dm: '1', q: null, category: null }) }],
    ['High opportunity', () => patch({ q: 'high opportunity' })],
    ['Replied recently', () => patch({ q: 'replied' })],
    ['Luxury brands', () => patch({ q: 'luxury' })],
  ]

  return (
    <div className="h-full flex overflow-hidden bg-stone-50">

      {/* ════ LEFT RAIL ════ */}
      <div className="flex flex-col h-full bg-stone-50 border-r border-stone-200 flex-shrink-0 w-[412px] relative">

        {/* header */}
        <div className="flex-shrink-0 px-4 pt-4 pb-3">
          <div className="flex items-center justify-between mb-3">
            <div>
              <h1 className="text-[17px] font-bold text-navy-900 leading-none">Contacts</h1>
              <p className="text-[11px] text-stone-400 font-semibold uppercase tracking-wide mt-1">
                <span className="text-navy-700 tabular-nums">{(stats?.total ?? total).toLocaleString()}</span> people
                {' · '}
                <span className="text-gold-600 tabular-nums">{(stats?.decision_makers ?? 0).toLocaleString()}</span> decision-makers
              </p>
            </div>
            <button onClick={() => syncMut.mutate()} disabled={syncMut.isPending}
              className="flex items-center gap-1.5 px-3 h-8 rounded-lg text-xs font-semibold text-white bg-navy-600 hover:bg-navy-700 shadow-soft transition-all disabled:opacity-60">
              <RefreshCw className={cn('w-3.5 h-3.5', syncMut.isPending && 'animate-spin')} />
              {syncMut.isPending ? 'Syncing…' : 'Sync'}
            </button>
          </div>

          {/* AI smart search */}
          <div className={cn('relative rounded-xl bg-white transition-all', focused ? 'ring-2 ring-navy-500 shadow-lift' : 'ring-1 ring-stone-200')}
            style={focused ? { boxShadow: '0 0 0 4px rgba(46,74,110,.08)' } : undefined}>
            <div className="flex items-center gap-2 px-3 h-11">
              <Sparkles className="w-4 h-4 flex-shrink-0 text-navy-600" />
              <input value={query} onChange={(e) => patch({ q: e.target.value || null })}
                onFocus={() => setFocused(true)} onBlur={() => setFocused(false)}
                placeholder='Ask AI — e.g. "decision-makers at luxury hotels"'
                className="flex-1 bg-transparent outline-none text-sm text-navy-900 placeholder:text-stone-400" />
              {query ? (
                <button onClick={() => patch({ q: null })} className="text-stone-400 hover:text-stone-600"><X className="w-4 h-4" /></button>
              ) : (
                <kbd className="text-[10px] text-stone-400 font-semibold bg-stone-100 px-1.5 py-0.5 rounded">⌘K</kbd>
              )}
            </div>
            {smartActive && (
              <div className="px-3 pb-2 -mt-0.5 text-[11px] text-stone-400 flex items-center gap-1">
                <Wand2 className="w-3 h-3 text-navy-600" /> AI matched <span className="font-bold text-navy-700">{filtered.length}</span> of {total}
              </div>
            )}
          </div>

          {/* suggested queries */}
          <div className="flex flex-wrap gap-1.5 mt-2.5">
            {suggestions.map(([label, fn]) => (
              <button key={label} onClick={fn}
                className="text-[11px] font-semibold text-stone-500 bg-white ring-1 ring-stone-200 hover:ring-stone-300 hover:text-navy-700 px-2.5 py-1 rounded-full transition-all">{label}</button>
            ))}
          </div>
        </div>

        {/* category chips */}
        <div className="flex-shrink-0 px-4 pb-2.5 flex items-center gap-1.5 overflow-x-auto">
          <Chip label="All" count={stats?.total} active={category === '' && !dmOnly} onClick={() => patch({ category: null, dm: null })} />
          <Chip label="★ DMs" count={stats?.decision_makers} active={dmOnly} color="#c49a3c" onClick={() => patch({ dm: dmOnly ? null : '1' })} />
          <Chip label="Buyers" count={stats?.buyer} active={category === 'buyer'} color="#1a7a55" onClick={() => patch({ category: category === 'buyer' ? null : 'buyer', dm: null })} />
          <Chip label="Sellers" count={stats?.seller} active={category === 'seller'} color="#c49a3c" onClick={() => patch({ category: category === 'seller' ? null : 'seller', dm: null })} />
          <Chip label="Competitors" count={stats?.competitor} active={category === 'competitor'} color="#e85d4a" onClick={() => patch({ category: category === 'competitor' ? null : 'competitor', dm: null })} />
        </div>

        {/* sort + status row */}
        <div className="flex-shrink-0 px-4 pb-2 flex items-center justify-between gap-2">
          <span className="text-[11px] font-bold text-stone-400 uppercase tracking-wider whitespace-nowrap">{filtered.length.toLocaleString()} shown</span>
          <div className="flex items-center gap-2">
            <select value={status} onChange={(e) => patch({ status: e.target.value || null })}
              className="text-[11px] font-semibold text-stone-500 bg-white ring-1 ring-stone-200 rounded-lg px-2 py-1 outline-none cursor-pointer hover:text-navy-700">
              {STATUS_OPTIONS.map(([v, l]) => <option key={v} value={v}>{l}</option>)}
            </select>
            <div className="flex items-center gap-1 text-[11px] text-stone-400">
              <Filter className="w-3 h-3" />
              <select value={sort} onChange={(e) => patch({ sort: e.target.value })}
                className="bg-transparent font-semibold text-stone-500 outline-none cursor-pointer hover:text-navy-700">
                <option value="confidence">Top match</option>
                <option value="opportunity">Opportunity</option>
                <option value="recent">Last seen</option>
                <option value="name">Name A–Z</option>
              </select>
            </div>
          </div>
        </div>

        {/* list */}
        <div className="flex-1 overflow-y-auto px-2.5 pb-20 space-y-0.5">
          {listQ.isLoading ? (
            <div className="space-y-2 px-1 pt-1">
              {Array.from({ length: 8 }).map((_, i) => <div key={i} className="h-[78px] rounded-xl bg-stone-100 animate-pulse" />)}
            </div>
          ) : filtered.length === 0 ? (
            <div className="flex flex-col items-center justify-center h-48 text-stone-400 text-center px-6">
              <Inbox className="w-8 h-8 text-stone-300 mb-2" />
              <p className="text-sm font-semibold text-stone-500">No matches</p>
              <p className="text-xs mt-1">Try a different search or filter</p>
            </div>
          ) : (
            filtered.map((c) => (
              <ContactRow key={c.id} contact={c} active={c.id === activeContact?.id}
                selectMode={selectMode} checked={selected.has(c.id)}
                onOpen={() => patch({ selected: String(c.id) })} onToggleCheck={() => toggleCheck(c.id)} />
            ))
          )}
        </div>

        {/* bulk action bar */}
        {selectMode && (
          <div className="absolute bottom-0 left-0 right-0 m-2.5 px-3.5 py-2.5 rounded-xl bg-navy-900 text-white shadow-lift flex items-center gap-3">
            <span className="text-xs font-bold tabular-nums">{selected.size} selected</span>
            <button onClick={selectAllVisible} className="text-[11px] font-semibold text-white/60 hover:text-white">Select all {filtered.length}</button>
            <div className="flex-1" />
            <button onClick={clearSelection} className="text-[11px] font-semibold text-white/60 hover:text-white">Clear</button>
            <button onClick={bulkApprove} disabled={bulkApproveMut.isPending}
              className="inline-flex items-center gap-1.5 h-8 px-3 rounded-lg text-xs font-bold text-navy-900 bg-gold-300 hover:bg-gold-200 transition-all disabled:opacity-60">
              {bulkApproveMut.isPending ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Check className="w-3.5 h-3.5" />}
              Approve {selected.size}
            </button>
          </div>
        )}
      </div>

      {/* ════ RIGHT PANEL ════ */}
      <ProfilePanel contact={activeContact} onDeleted={() => patch({ selected: null })} />
    </div>
  )
}
