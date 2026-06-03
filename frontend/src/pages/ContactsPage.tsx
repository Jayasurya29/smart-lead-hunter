/**
 * ContactsPage — AI-first contact directory with slide-over profile (self-contained).
 * ============================================================================
 * Drop-in replacement. One file: helpers, presentational components, the
 * full-width hotel directory, the live AI profile drawer, and the triage workflow.
 * Only imports are your existing hooks / api / utils.
 *
 *   Main pane : full-width directory — natural-language "ask AI" search (person /
 *               hotel / position), grouped by hotel (collapsible) or flat list,
 *               category + status filters, sort, multi-select bulk-approve.
 *   Drawer    : clicking a contact slides a profile in BESIDE the list (no scrim —
 *               the directory stays usable). Live AI Intelligence + Deep Enrich,
 *               contact + hospitality intel, engagement, Approve / Push-to-CRM /
 *               Reject. ‹ › steps through contacts; Esc / ✕ closes.
 *
 * Server-side filtering: category + approval status + ordering.
 * Client-side (over the loaded page, per_page 200): smart-search, DM focus, sort.
 * For very large inboxes, push `search` to the server (marked below).
 */
import { useEffect, useMemo, useRef, useState } from 'react'
import { useSearchParams } from 'react-router-dom'
import {
  Sparkles, Wand2, X, RefreshCw, Inbox, Star, ChevronRight, ChevronDown,
  Mail, Phone, Linkedin, ExternalLink, MapPin, Building2, Shield, Hash,
  Eye, Users, Activity, Send, Check, Loader2, Trash2, CheckSquare, Square,
  Radar, Briefcase, Layers,
} from 'lucide-react'
import { cn, formatDate, relativeDate, getTierLabel } from '@/lib/utils'
import type { InboxContact, InboxContactStats } from '@/api/inboxContacts'
import {
  useAllInboxContacts,
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

type SortKey = 'confidence' | 'opportunity' | 'recent' | 'newest' | 'oldest' | 'name' | 'org'

/* ────────────────────────────────────────────────────────────────────
   UNIFIED MODEL ADAPTER
   --------------------------------------------------------------------
   This page unifies TWO contact sources into one directory:
     1. Email-scraped contacts  → useInboxContacts (already wired)
     2. Lead-generator contacts → contacts attached to discovered/existing
        hotels & management companies. Merge them into `items` below and
        set `source: 'lead_generator'` (see the MERGE POINT in the page).

   These three dimensions drive the new scope bar. Each reads a real field
   if present, else falls back to a heuristic — so it works today and gets
   sharper once the backend exposes the explicit fields:
     • source        → c.source            ('email_scrape' | 'lead_generator')
     • account_type  → c.account_type      ('hotel' | 'management_company')
     • lifecycle     → c.lifecycle_stage   ('potential' | 'existing')
   ──────────────────────────────────────────────────────────────────── */
type Source = 'email_scrape' | 'lead_generator'
type AccountType = 'hotel' | 'management_company'
type Stage = 'potential' | 'existing'

type UnifiedContact = InboxContact & {
  source?: Source
  account_type?: AccountType
  lifecycle_stage?: Stage
  management_company?: string | null
}

function sourceOf(c: UnifiedContact): Source {
  return c.source || 'email_scrape'
}
function accountTypeOf(c: UnifiedContact): AccountType {
  if (c.account_type) return c.account_type
  const mc = c.management_company
  // works AT the management company itself, or is a GPO/seller → management company
  if (mc && c.organization && mc.trim().toLowerCase() === c.organization.trim().toLowerCase()) return 'management_company'
  if (c.contact_category === 'seller') return 'management_company'
  return 'hotel'
}
function stageOf(c: UnifiedContact): Stage {
  if (c.lifecycle_stage) return c.lifecycle_stage
  if (c.matched_hotel_id) return 'existing'
  return 'potential'
}

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
function smartMatch(c: UnifiedContact, query: string): boolean {
  const t = query.toLowerCase().trim()
  if (!t) return true
  const acctLabel = accountTypeOf(c) === 'management_company' ? 'management company mgmt co operator' : 'hotel'
  const srcLabel = sourceOf(c) === 'lead_generator' ? 'lead generator scraped discovery prospect' : 'email inbox'
  const hay = [
    c.first_name, c.last_name, c.display_name, c.title, c.inferred_role, c.organization,
    c.email, c.address, c.department, c.parent_company, c.management_company, c.gpo, getTierLabel(c.brand_tier),
    c.contact_category, c.opportunity_level, acctLabel, srcLabel, stageOf(c),
  ].filter(Boolean).join(' ').toLowerCase()
  if (/decision|maker|\bdm\b/.test(t)) return !!c.is_decision_maker
  if (/high opp|opportun|hot lead/.test(t)) return isHighOpportunity(c)
  if (/management compan|mgmt|operator/.test(t)) return accountTypeOf(c) === 'management_company'
  if (/lead gen|scraped|discover|prospect/.test(t)) return sourceOf(c) === 'lead_generator'
  if (/existing|customer|current client/.test(t)) return stageOf(c) === 'existing'
  if (/potential|new lead/.test(t)) return stageOf(c) === 'potential'
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
void Chip // retained for reference; the calm design uses <Facet> instead

/* ════════════════════════════════════════════════════════════════════
   DIRECTORY ROW (full-width)
   ════════════════════════════════════════════════════════════════════ */

function StatusPill({ status }: { status: string }) {
  if (status === 'pushed_to_insightly') return <span className="inline-flex items-center px-2 py-0.5 rounded-full text-[10px] font-semibold bg-navy-50 text-navy-700">In CRM</span>
  if (status === 'approved') return <span className="inline-flex items-center px-2 py-0.5 rounded-full text-[10px] font-semibold bg-emerald-50 text-emerald-700">Approved</span>
  return <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[10px] font-semibold text-stone-500"><span className="w-1.5 h-1.5 rounded-full bg-coral-400" />Pending</span>
}
void StatusPill // status now shown only inside the profile drawer; kept for reuse

/* ── one consistent filter facet (dropdown) — the only filter idiom ── */
type FacetOption = { v: string; label: string; count?: number; dot?: string; icon?: React.ComponentType<{ className?: string }> }
function Facet({ icon: Ic, label, value, options, onChange, align }: {
  icon?: React.ComponentType<{ className?: string }>
  label: string
  value: string
  options: FacetOption[]
  onChange: (v: string) => void
  align?: 'right'
}) {
  const [open, setOpen] = useState(false)
  const cur = options.find((o) => o.v === value) || options[0]
  const isAll = value == null || value === 'all' || value === ''
  return (
    <div className="relative">
      <button onClick={() => setOpen((o) => !o)}
        className={cn('inline-flex items-center gap-1.5 h-9 px-3 rounded-lg text-[13px] font-medium transition-colors',
          isAll ? 'text-stone-500 hover:bg-stone-100' : 'text-navy-800 bg-navy-50 ring-1 ring-navy-100 font-semibold')}>
        {Ic && <Ic className={cn('w-3.5 h-3.5', isAll ? 'text-stone-400' : 'text-navy-500')} />}
        <span>{isAll ? label : cur.label}</span>
        <ChevronDown className="w-3.5 h-3.5 opacity-40" />
      </button>
      {open && (
        <>
          <div className="fixed inset-0 z-40" onClick={() => setOpen(false)} />
          <div className={cn('absolute z-50 mt-1.5 min-w-[200px] bg-white rounded-xl shadow-lift ring-1 ring-stone-200/80 p-1.5', align === 'right' ? 'right-0' : 'left-0')}>
            {options.map((o) => (
              <button key={o.v} onClick={() => { onChange(o.v); setOpen(false) }}
                className={cn('w-full flex items-center gap-2.5 px-2.5 py-2 rounded-lg text-[13px] text-left transition-colors',
                  o.v === value ? 'bg-stone-50 font-semibold text-navy-900' : 'text-stone-600 hover:bg-stone-50')}>
                {o.icon ? <o.icon className="w-4 h-4 text-stone-400 flex-shrink-0" /> : o.dot ? <span className="w-2 h-2 rounded-full flex-shrink-0" style={{ background: o.dot }} /> : null}
                <span className="flex-1">{o.label}</span>
                {o.count != null && <span className="text-[11px] tabular-nums text-stone-400">{o.count}</span>}
                {o.v === value && <Check className="w-4 h-4" style={{ color: 'var(--accent, #2e4a6e)' }} />}
              </button>
            ))}
          </div>
        </>
      )}
    </div>
  )
}

function DirRow({
  contact, selected, checked, selectMode, onOpen, onToggleCheck, hideOrg, onOpenOrg,
}: {
  contact: UnifiedContact
  selected: boolean
  checked: boolean
  selectMode: boolean
  onOpen: () => void
  onToggleCheck: () => void
  hideOrg?: boolean
  onOpenOrg?: (org: string) => void
}) {
  const isLead = sourceOf(contact) === 'lead_generator'
  return (
    <div onClick={onOpen}
      className={cn('group flex items-center gap-3 pl-3 pr-4 py-2.5 rounded-xl cursor-pointer transition-colors',
        selected ? 'bg-white shadow-card ring-1 ring-navy-100' : 'hover:bg-white')}>
      <button onClick={(e) => { e.stopPropagation(); onToggleCheck() }}
        className={cn('flex-shrink-0 transition-opacity', checked || selectMode ? 'opacity-100' : 'opacity-0 group-hover:opacity-100')}>
        {checked ? <CheckSquare className="w-4 h-4 text-navy-600" /> : <Square className="w-4 h-4 text-stone-300" />}
      </button>
      <div className="relative flex-shrink-0">
        <Avatar contact={contact} size={38} />
        <span className="absolute -bottom-0.5 -left-0.5 w-3.5 h-3.5 rounded-full ring-2 ring-stone-50 flex items-center justify-center"
          style={{ background: isLead ? '#7c3aed' : '#2e4a6e' }} title={isLead ? 'Lead Generator' : 'Email'}>
          {isLead ? <Radar className="w-2 h-2 text-white" /> : <Inbox className="w-2 h-2 text-white" />}
        </span>
      </div>
      {/* name + role */}
      <div className="min-w-0 w-[34%] flex-shrink-0">
        <div className="flex items-center gap-1.5">
          <span className="truncate font-semibold text-navy-900 text-sm">{fullName(contact)}</span>
          {contact.is_decision_maker && <span className="inline-flex items-center gap-0.5 text-[10px] font-bold text-gold-700 bg-gold-50 px-1.5 py-0.5 rounded-full flex-shrink-0" title="Decision-maker">★ DM</span>}
        </div>
        <div className="truncate text-stone-500 text-[13px] mt-0.5">{hideOrg ? (roleText(contact) || 'role unknown') : `${roleText(contact) || 'role unknown'}  ·  ${contact.organization || '—'}`}</div>
      </div>
      {/* email — the field reps need most */}
      <div className="min-w-0 flex-1 hidden md:block">
        {contact.email
          ? <a href={`mailto:${contact.email}`} onClick={(e) => e.stopPropagation()} className="truncate block text-[13px] text-navy-600 hover:text-navy-800 hover:underline font-medium">{contact.email}</a>
          : <span className="text-[12px] text-stone-400 italic">no email yet</span>}
      </div>
      {/* quick actions — appear on hover, act without opening the drawer */}
      <div className="hidden lg:flex items-center gap-1 flex-shrink-0 opacity-0 group-hover:opacity-100 transition-opacity">
        {contact.email && <a href={`mailto:${contact.email}`} onClick={(e) => e.stopPropagation()} title="Email" className="w-7 h-7 rounded-lg hover:bg-stone-100 flex items-center justify-center text-stone-500"><Mail className="w-4 h-4" /></a>}
        {contact.phone && <a href={`tel:${contact.phone}`} onClick={(e) => e.stopPropagation()} title="Call" className="w-7 h-7 rounded-lg hover:bg-stone-100 flex items-center justify-center text-stone-500"><Phone className="w-4 h-4" /></a>}
      </div>
      {/* org + meta (status pills removed — org name is more useful here). Org is a link → jumps to that company. */}
      <div className="flex items-center gap-3 flex-shrink-0">
        {isHighOpportunity(contact) && <span className="hidden sm:inline text-[11px] font-bold text-coral-500" title="High opportunity">High</span>}
        {contact.organization
          ? <button onClick={(e) => { e.stopPropagation(); onOpenOrg?.(contact.organization!) }} title={`See everyone at ${contact.organization}`}
              className="hidden sm:block w-[180px] text-right truncate text-[12px] font-semibold text-navy-700 hover:text-navy-900 hover:underline">{contact.organization}</button>
          : <span className="hidden sm:block w-[180px]" />}
        <span className="w-12 text-right text-[11px] text-stone-500 whitespace-nowrap hidden xl:block">{isLead && contact.interaction_count === 0 ? 'new' : relativeDate(contact.last_seen)}</span>
        <ChevronRight className="w-4 h-4 text-stone-300 group-hover:text-navy-400 transition-colors" />
      </div>
    </div>
  )
}

/* ════════════════════════════════════════════════════════════════════
   HOTEL GROUP (collapsible, full-width)
   ════════════════════════════════════════════════════════════════════ */

function DirGroup({
  org, members, expanded, onToggle, selectedId, checked, selectMode, onToggleCheck, onSelect, onOpenOrg,
}: {
  org: string
  members: UnifiedContact[]
  expanded: boolean
  onToggle: () => void
  selectedId: number | null
  checked: Set<number>
  selectMode: boolean
  onToggleCheck: (id: number) => void
  onSelect: (id: number) => void
  onOpenOrg?: (org: string) => void
}) {
  const dm = members.filter((m) => m.is_decision_maker).length
  const isMgmt = members.length > 0 && accountTypeOf(members[0]) === 'management_company'
  const tier = members.find((m) => m.brand_tier)?.brand_tier
  const allExisting = members.every((m) => stageOf(m) === 'existing')
  return (
    <div className="mb-2" data-org={org}>
      <button onClick={onToggle} className="w-full flex items-center gap-3 px-3 py-2.5 rounded-xl hover:bg-white transition-colors">
        <ChevronRight className="w-3.5 h-3.5 text-stone-300 flex-shrink-0" style={{ transform: expanded ? 'rotate(90deg)' : 'none', transition: 'transform .18s ease' }} />
        <span className={cn('w-9 h-9 rounded-xl flex items-center justify-center flex-shrink-0', isMgmt ? 'bg-violet-50' : 'bg-navy-50')}>
          {isMgmt ? <Briefcase className="w-4 h-4 text-violet-500" /> : <Building2 className="w-4 h-4 text-navy-500" />}
        </span>
        <div className="min-w-0 text-left flex-1">
          <div className="flex items-center gap-2">
            <span className="truncate text-[15px] font-bold text-navy-900">{org}</span>
            {isMgmt
              ? <span className="text-[10px] font-semibold text-violet-600 flex-shrink-0">Mgmt co.</span>
              : (tier && <span className="text-[10px] font-semibold text-gold-600 flex-shrink-0">{getTierLabel(tier)}</span>)}
            {allExisting && <span className="w-1.5 h-1.5 rounded-full bg-emerald-400 flex-shrink-0" title="Existing customer" />}
          </div>
          <div className="text-[12px] text-stone-500 mt-0.5">
            {members.length} contact{members.length > 1 ? 's' : ''}{dm > 0 ? `  ·  ★ ${dm}` : ''}
          </div>
        </div>
        <span className="text-[12px] text-stone-500 font-medium">{expanded ? 'Hide' : 'Show'}</span>
      </button>
      {expanded && (
        <div className="pl-5 ml-4 border-l border-stone-200/70 mt-1 space-y-0.5">
          {members.map((c) => (
            <DirRow key={c.id} contact={c} selected={c.id === selectedId} hideOrg
              selectMode={selectMode} checked={checked.has(c.id)}
              onOpen={() => onSelect(c.id)} onToggleCheck={() => onToggleCheck(c.id)} onOpenOrg={onOpenOrg} />
          ))}
        </div>
      )}
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
          <p className="text-[13.5px] leading-relaxed text-white/85 max-w-[80ch]">{deriveSummary(contact)}</p>
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

function Timeline({ contact }: { contact: UnifiedContact }) {
  const isLead = sourceOf(contact) === 'lead_generator'
  const events = [
    isLead
      ? { t: contact.first_seen, label: 'Discovered via Lead Generator', color: '#7c3aed' }
      : { t: contact.last_seen, label: 'Last email received', color: '#2e4a6e' },
    isHighOpportunity(contact) ? { t: contact.last_seen, label: 'Flagged high-opportunity by AI', color: '#c49a3c' } : null,
    isLead && contact.interaction_count > 0 ? { t: contact.last_seen, label: 'First email exchanged', color: '#2e4a6e' } : null,
    !isLead ? { t: contact.first_seen, label: 'First seen in inbox', color: '#b0a99e' } : null,
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
          <div className={cn('text-2xl font-bold tabular-nums leading-none', isLead ? 'text-violet-600' : 'text-navy-900')}>{isLead ? 'Lead Gen' : `${contact.source_mailboxes?.length ?? 0} mbx`}</div>
          <div className="text-[10px] uppercase tracking-wide font-bold text-stone-400 mt-1">Source</div>
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
      {isLead ? (
        <div className="mt-3 pt-3 border-t border-stone-100 text-[11px] text-stone-400 inline-flex items-center gap-1">
          <Radar className="w-3 h-3 text-violet-500" /> Discovered by Lead Generator — no email thread yet
        </div>
      ) : contact.source_mailboxes && contact.source_mailboxes.length > 0 ? (
        <div className="mt-3 pt-3 border-t border-stone-100 text-[11px] text-stone-400">
          Source mailboxes: <span className="font-mono text-stone-500">{contact.source_mailboxes.join(', ')}</span>
        </div>
      ) : null}
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
            <div className="flex items-center gap-2 mt-2 flex-wrap text-[12px] text-white/60">
              {contact.address && <span className="inline-flex items-center gap-1"><MapPin className="w-3 h-3" />{contact.address}</span>}
              {sourceOf(contact) === 'lead_generator'
                ? <span className="inline-flex items-center gap-1"><Radar className="w-3 h-3" />Found via Lead Generator · {relativeDate(contact.first_seen)}</span>
                : <span className="inline-flex items-center gap-1"><Mail className="w-3 h-3" />From email · {relativeDate(contact.last_seen)}</span>}
              <span className="inline-flex items-center gap-1">{accountTypeOf(contact) === 'management_company' ? <Briefcase className="w-3 h-3" /> : <Building2 className="w-3 h-3" />}{accountTypeOf(contact) === 'management_company' ? 'Management co.' : 'Hotel'}</span>
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
      <div className="px-7 py-6 space-y-4">
        <AIBand contact={contact} />

        <div className="grid gap-4 items-start" style={{ gridTemplateColumns: 'repeat(auto-fit, minmax(300px, 1fr))' }}>
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

          <Timeline contact={contact} />
        </div>

        {contact.priority_reason && (
          <div className="text-xs text-stone-500 bg-white rounded-2xl ring-1 ring-stone-200/80 shadow-card px-5 py-3.5">
            <span className="font-bold text-stone-400 uppercase tracking-wider text-[10px]">Why this priority</span>
            <p className="mt-1 leading-relaxed">{contact.priority_reason}</p>
          </div>
        )}
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
void STATUS_OPTIONS // status now lives in the <Facet> options inline

export default function ContactsPage() {
  const [params, setParams] = useSearchParams()

  // URL-synced state
  const query = params.get('q') || ''
  const category = params.get('category') || ''
  const status = params.get('status') || ''
  const source = (params.get('source') as Source | '') || ''     // '' = all
  const account = (params.get('account') as AccountType | '') || '' // '' = all
  const lifecycle = (params.get('stage') as Stage | '') || ''      // '' = all
  const dmOnly = params.get('dm') === '1'
  const sort = (params.get('sort') as SortKey) || 'confidence'
  const selectedId = params.get('selected') ? Number(params.get('selected')) : null

  // local state
  const [selected, setSelected] = useState<Set<number>>(new Set())
  const [drawerOpen, setDrawerOpen] = useState<boolean>(() => !!params.get('selected'))
  const [focused, setFocused] = useState(false)
  const [view, setView] = useState<'accounts' | 'people'>('accounts')
  const [collapsedOrgs, setCollapsedOrgs] = useState<Set<string>>(new Set())

  function patch(updates: Record<string, string | null>) {
    const next = new URLSearchParams(params)
    for (const [k, v] of Object.entries(updates)) {
      if (v) next.set(k, v)
      else next.delete(k)
    }
    setParams(next, { replace: true })
  }

  // data — load the FULL table so account grouping + scope counts are real
  const statsQ = useInboxContactStats()
  const listQ = useAllInboxContacts('priority_score')
  const syncMut = useTriggerInboxSync()
  const bulkApproveMut = useBulkApproveInboxContacts()

  const stats: InboxContactStats | undefined = statsQ.data

  // ╔══════════════════════════════════════════════════════════════════╗
  // ║ MERGE POINT — unify email-scraped + lead-generator contacts here.  ║
  // ║ `items` below is the inbox (email_scrape) list. To add lead-gen    ║
  // ║ contacts, fetch them (e.g. useLeadContacts()) and concat, mapping  ║
  // ║ each into the InboxContact shape with source: 'lead_generator'.    ║
  // ║   const items = [...inboxItems, ...leadItems.map(toUnified)]       ║
  // ╚══════════════════════════════════════════════════════════════════╝
  const items = (listQ.data?.items || []) as UnifiedContact[]
  const total = listQ.data?.total || 0

  // scope counts (computed over the loaded page; for full-inbox totals expose these from the backend)
  const scope = useMemo(() => {
    const s = { emailScrape: 0, leadGen: 0, hotels: 0, mgmtCos: 0, potential: 0, existing: 0 }
    const hotelAccts = new Set<string>(); const mgmtAccts = new Set<string>()
    for (const c of items) {
      sourceOf(c) === 'lead_generator' ? s.leadGen++ : s.emailScrape++
      if (accountTypeOf(c) === 'management_company') { s.mgmtCos++; if (c.organization) mgmtAccts.add(c.organization) }
      else { s.hotels++; if (c.organization) hotelAccts.add(c.organization) }
      stageOf(c) === 'existing' ? s.existing++ : s.potential++
    }
    return { ...s, hotelAccounts: hotelAccts.size, mgmtAccounts: mgmtAccts.size }
  }, [items])

  // client-side refine + sort
  const filtered = useMemo(() => {
    let list = items.filter((c) => smartMatch(c, query))
    if (category) list = list.filter((c) => c.contact_category === category)
    if (status) list = list.filter((c) => c.approval_status === status)
    if (dmOnly) list = list.filter((c) => c.is_decision_maker)
    if (source) list = list.filter((c) => sourceOf(c) === source)
    if (account) list = list.filter((c) => accountTypeOf(c) === account)
    if (lifecycle) list = list.filter((c) => stageOf(c) === lifecycle)
    const sorters: Record<SortKey, (a: UnifiedContact, b: UnifiedContact) => number> = {
      confidence: (a, b) => confidencePct(b) - confidencePct(a),
      opportunity: (a, b) => (b.opportunity_score ?? 0) - (a.opportunity_score ?? 0),
      recent: (a, b) => new Date(b.last_seen || 0).getTime() - new Date(a.last_seen || 0).getTime(),
      newest: (a, b) => new Date(b.first_seen || 0).getTime() - new Date(a.first_seen || 0).getTime(),
      oldest: (a, b) => new Date(a.first_seen || 0).getTime() - new Date(b.first_seen || 0).getTime(),
      name: (a, b) => `${a.first_name || ''} ${a.last_name || ''}`.trim().localeCompare(`${b.first_name || ''} ${b.last_name || ''}`.trim()),
      org: (a, b) => (a.organization || '~').localeCompare(b.organization || '~') || `${a.first_name || ''}${a.last_name || ''}`.localeCompare(`${b.first_name || ''}${b.last_name || ''}`),
    }
    return [...list].sort(sorters[sort])
  }, [items, query, category, status, dmOnly, source, account, lifecycle, sort])

  // group filtered contacts by account (hotel OR management company)
  const groups = useMemo(() => {
    const m = new Map<string, UnifiedContact[]>()
    for (const c of filtered) {
      const k = c.organization || 'No organization'
      if (!m.has(k)) m.set(k, [])
      m.get(k)!.push(c)
    }
    // keep insertion order so account groups follow the active Sort (e.g. Company A–Z, Newest added)
    return [...m.entries()]
  }, [filtered])

  const hotelGroups = groups.filter(([, m]) => accountTypeOf(m[0]) === 'hotel').length
  const mgmtGroups = groups.filter(([, m]) => accountTypeOf(m[0]) === 'management_company').length
  const anyFilter = !!source || !!account || !!lifecycle || !!category || dmOnly || !!status
  function resetAll() { patch({ source: null, account: null, stage: null, category: null, dm: null, status: null }) }

  const activeContact = items.find((c) => c.id === selectedId) || null
  const idx = filtered.findIndex((c) => c.id === selectedId)
  const smartActive = query.trim().length > 0
  const selectMode = selected.size > 0

  function openContact(id: number) { patch({ selected: String(id) }); setDrawerOpen(true) }
  function closeDrawer() { setDrawerOpen(false) }
  function go(delta: number) { const n = filtered[idx + delta]; if (n) patch({ selected: String(n.id) }) }
  function onDeleted() {
    const n = filtered[idx + 1] || filtered[idx - 1] || null
    if (n) patch({ selected: String(n.id) })
    else { setDrawerOpen(false); patch({ selected: null }) }
  }

  // keyboard nav while drawer open
  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      if (!drawerOpen) return
      if (e.key === 'Escape') closeDrawer()
      else if (e.key === 'ArrowDown') { e.preventDefault(); go(1) }
      else if (e.key === 'ArrowUp') { e.preventDefault(); go(-1) }
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [drawerOpen, idx, filtered])

  function toggleOrg(org: string) {
    setCollapsedOrgs((prev) => {
      const next = new Set(prev)
      if (next.has(org)) next.delete(org)
      else next.add(org)
      return next
    })
  }
  // org → people: jump to grouped view, expand that company, and scroll to it
  function openOrg(org: string) {
    setView('accounts')
    setCollapsedOrgs((prev) => { const next = new Set(prev); next.delete(org); return next })
    const sel = `[data-org="${window.CSS && CSS.escape ? CSS.escape(org) : org}"]`
    let tries = 0
    const tick = () => {
      const el = document.querySelector(sel) as HTMLElement | null
      const box = el && (el.closest('.overflow-y-auto') as HTMLElement | null)
      if (el && box) {
        const delta = el.getBoundingClientRect().top - box.getBoundingClientRect().top
        box.scrollTop = box.scrollTop + delta - 8
        el.style.transition = 'background .2s ease'
        el.style.background = 'rgba(212,168,83,.18)'
        setTimeout(() => { el.style.background = 'transparent' }, 700)
        return
      }
      if (tries++ < 25) setTimeout(tick, 30)
    }
    setTimeout(tick, 30)
  }
  function toggleCheck(id: number) {
    setSelected((prev) => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })
  }
  function clearSelection() { setSelected(new Set()) }
  function bulkApprove() { bulkApproveMut.mutate(Array.from(selected), { onSuccess: clearSelection }) }
  function selectAllVisible() { setSelected(new Set(filtered.map((c) => c.id))) }

  return (
    <div className="h-full overflow-hidden bg-stone-50" style={{ display: 'grid', gridTemplateColumns: drawerOpen ? '1fr 600px' : '1fr 0px', transition: 'grid-template-columns .28s cubic-bezier(.16,1,.3,1)' }}>

      {/* ════ MAIN DIRECTORY ════ */}
      <div className="min-w-0 overflow-hidden flex flex-col h-full relative">

        {/* header — calm: title + one-line context + Sync */}
        <div className="flex-shrink-0 px-8 pt-6 pb-4 flex items-end justify-between">
          <div>
            <h1 className="text-2xl font-bold text-navy-900 leading-none tracking-tight">Contacts</h1>
            <p className="text-[13px] text-stone-500 mt-2">
              <span className="text-stone-600 font-semibold tabular-nums">{(stats?.total ?? total).toLocaleString()}</span> people across{' '}
              <span className="text-stone-600 font-semibold tabular-nums">{scope.hotelAccounts} hotels</span> and{' '}
              <span className="text-stone-600 font-semibold tabular-nums">{scope.mgmtAccounts} management companies</span>
            </p>
          </div>
          <button onClick={() => syncMut.mutate()} disabled={syncMut.isPending}
            className="flex items-center gap-2 px-4 h-10 rounded-xl text-[13px] font-semibold text-white bg-navy-600 hover:bg-navy-700 shadow-soft transition-all disabled:opacity-60">
            <RefreshCw className={cn('w-4 h-4', syncMut.isPending && 'animate-spin')} />
            {syncMut.isPending ? 'Syncing…' : 'Sync inbox'}
          </button>
        </div>

        {/* search — the one hero control */}
        <div className="flex-shrink-0 px-8 pb-3">
          <div className={cn('relative rounded-2xl bg-white transition-all', focused ? 'ring-2 ring-navy-500 shadow-lift' : 'ring-1 ring-stone-200')}
            style={focused ? { boxShadow: '0 0 0 4px rgba(46,74,110,.07)' } : undefined}>
            <div className="flex items-center gap-3 px-4 h-12">
              <Sparkles className="w-[18px] h-[18px] flex-shrink-0 text-navy-600" />
              <input value={query} onChange={(e) => patch({ q: e.target.value || null })}
                onFocus={() => setFocused(true)} onBlur={() => setFocused(false)}
                placeholder='Search anyone — a name, a hotel, a company, or a role like "director of procurement"'
                className="flex-1 bg-transparent outline-none text-[15px] text-navy-900 placeholder:text-stone-400" />
              {query && <button onClick={() => patch({ q: null })} className="text-stone-400 hover:text-stone-600"><X className="w-[17px] h-[17px]" /></button>}
            </div>
          </div>
        </div>

        {/* ONE filter row — every control is the same quiet facet */}
        <div className="flex-shrink-0 px-8 pb-3 flex items-center gap-1 flex-wrap">
          <Facet icon={Layers} label="Source" value={source || 'all'} onChange={(v) => patch({ source: v === 'all' ? null : v })} options={[
            { v: 'all', label: 'All sources' },
            { v: 'email_scrape', label: 'Email scraped', icon: Inbox, count: scope.emailScrape },
            { v: 'lead_generator', label: 'Lead generator', icon: Radar, count: scope.leadGen },
          ]} />
          <Facet icon={Building2} label="Account" value={account || 'all'} onChange={(v) => patch({ account: v === 'all' ? null : v })} options={[
            { v: 'all', label: 'All accounts' },
            { v: 'hotel', label: 'Hotels', icon: Building2, count: scope.hotels },
            { v: 'management_company', label: 'Management companies', icon: Briefcase, count: scope.mgmtCos },
          ]} />
          <Facet label="Stage" value={lifecycle || 'all'} onChange={(v) => patch({ stage: v === 'all' ? null : v })} options={[
            { v: 'all', label: 'Any stage' },
            { v: 'potential', label: 'Potential', dot: '#c49a3c', count: scope.potential },
            { v: 'existing', label: 'Existing', dot: '#1a7a55', count: scope.existing },
          ]} />
          <Facet label="Category" value={category || 'all'} onChange={(v) => patch({ category: v === 'all' ? null : v, dm: null })} options={[
            { v: 'all', label: 'Any category' },
            { v: 'buyer', label: 'Buyers', dot: '#1a7a55', count: stats?.buyer },
            { v: 'seller', label: 'Sellers', dot: '#c49a3c', count: stats?.seller },
            { v: 'competitor', label: 'Competitors', dot: '#e85d4a', count: stats?.competitor },
          ]} />
          <Facet label="Status" value={status || 'all'} onChange={(v) => patch({ status: v === 'all' ? null : v })} options={[
            { v: 'all', label: 'Any status' },
            { v: 'pending', label: 'Pending' },
            { v: 'approved', label: 'Approved' },
            { v: 'pushed_to_insightly', label: 'In CRM' },
          ]} />
          <button onClick={() => patch({ dm: dmOnly ? null : '1' })}
            className={cn('inline-flex items-center gap-1.5 h-9 px-3 rounded-lg text-[13px] font-medium transition-colors',
              dmOnly ? 'text-gold-700 bg-gold-50 ring-1 ring-gold-200 font-semibold' : 'text-stone-500 hover:bg-stone-100')}>
            <span className={dmOnly ? 'text-gold-500' : 'text-stone-400'}>★</span> Decision-makers
          </button>
          {anyFilter && (
            <button onClick={resetAll} className="inline-flex items-center gap-1 h-9 px-2.5 rounded-lg text-[12px] font-medium text-stone-400 hover:text-coral-500 transition-colors">
              <X className="w-3.5 h-3.5" /> Clear
            </button>
          )}
          <div className="flex-1" />
          <Facet align="right" label="Group" value={view} onChange={(v) => setView(v as 'accounts' | 'people')} options={[
            { v: 'accounts', label: 'By account' },
            { v: 'people', label: 'All people' },
          ]} />
          <Facet align="right" label="Sort" value={sort} onChange={(v) => patch({ sort: v })} options={[
            { v: 'confidence', label: 'Top match' },
            { v: 'name', label: 'Name A–Z' },
            { v: 'org', label: 'Company A–Z' },
            { v: 'newest', label: 'Newest added' },
            { v: 'oldest', label: 'Oldest added' },
            { v: 'recent', label: 'Last activity' },
            { v: 'opportunity', label: 'Opportunity' },
          ]} />
        </div>

        {/* subtle result line */}
        <div className="flex-shrink-0 px-8 pb-2 text-[12px] text-stone-500">
          {smartActive ? (
            <span className="inline-flex items-center gap-1.5"><Wand2 className="w-3 h-3 text-navy-600" /> Showing <span className="font-semibold text-navy-700">{filtered.length}</span> matches</span>
          ) : (
            <span>{view === 'accounts' ? `${hotelGroups} hotels · ${mgmtGroups} companies · ${filtered.length} people` : `${filtered.length} people`}</span>
          )}
        </div>

        {/* body */}
        <div className="flex-1 overflow-y-auto px-6 pb-24">
          {listQ.isLoading ? (
            <div className="space-y-2 px-1 pt-1 max-w-[1320px] mx-auto">
              {Array.from({ length: 10 }).map((_, i) => <div key={i} className="h-[60px] rounded-xl bg-stone-100 animate-pulse" />)}
            </div>
          ) : filtered.length === 0 ? (
            <div className="flex flex-col items-center justify-center h-64 text-stone-400 text-center">
              <Inbox className="w-8 h-8 text-stone-300 mb-2" />
              <p className="text-sm font-semibold text-stone-500">No matches</p>
              <p className="text-xs mt-1">Try a different search or filter</p>
            </div>
          ) : view === 'people' ? (
            <div className="max-w-[1320px] mx-auto space-y-0.5">
              {filtered.map((c) => (
                <DirRow key={c.id} contact={c} selected={c.id === selectedId} selectMode={selectMode} checked={selected.has(c.id)}
                  onOpen={() => openContact(c.id)} onToggleCheck={() => toggleCheck(c.id)} onOpenOrg={openOrg} />
              ))}
            </div>
          ) : (
            <div className="max-w-[1320px] mx-auto">
              {groups.map(([org, members]) => (
                <DirGroup key={org} org={org} members={members}
                  expanded={smartActive || !collapsedOrgs.has(org)}
                  onToggle={() => toggleOrg(org)}
                  selectedId={selectedId} checked={selected} selectMode={selectMode}
                  onToggleCheck={toggleCheck} onSelect={openContact} onOpenOrg={openOrg} />
              ))}
            </div>
          )}
        </div>

        {/* bulk action bar */}
        {selectMode && (
          <div className="absolute bottom-4 left-1/2 -translate-x-1/2 px-4 py-2.5 rounded-xl bg-navy-900 text-white shadow-lift flex items-center gap-3 z-10">
            <span className="text-xs font-bold tabular-nums">{selected.size} selected</span>
            <button onClick={selectAllVisible} className="text-[11px] font-semibold text-white/60 hover:text-white">Select all {filtered.length}</button>
            <div className="w-px h-4 bg-white/20" />
            <button onClick={clearSelection} className="text-[11px] font-semibold text-white/60 hover:text-white">Clear</button>
            <button onClick={bulkApprove} disabled={bulkApproveMut.isPending}
              className="inline-flex items-center gap-1.5 h-8 px-3 rounded-lg text-xs font-bold text-navy-900 bg-gold-300 hover:bg-gold-200 transition-all disabled:opacity-60">
              {bulkApproveMut.isPending ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Check className="w-3.5 h-3.5" />}
              Approve {selected.size}
            </button>
          </div>
        )}
      </div>

      {/* ════ SLIDE-OVER DRAWER (pushes in beside list; no scrim — list stays usable) ════ */}
      <div className={cn('overflow-hidden bg-stone-100', drawerOpen && 'border-l border-stone-200')}>
        <div className="h-full flex flex-col" style={{ width: 600 }}>
          <div className="flex-shrink-0 h-12 px-2.5 bg-white border-b border-stone-200 flex items-center justify-between">
            <div className="flex items-center gap-0.5">
              <button onClick={() => go(-1)} disabled={idx <= 0} title="Previous (↑)"
                className="w-8 h-8 rounded-lg hover:bg-stone-100 flex items-center justify-center text-stone-500 disabled:opacity-30">
                <ChevronRight className="w-[18px] h-[18px]" style={{ transform: 'rotate(180deg)' }} />
              </button>
              <button onClick={() => go(1)} disabled={idx >= filtered.length - 1} title="Next (↓)"
                className="w-8 h-8 rounded-lg hover:bg-stone-100 flex items-center justify-center text-stone-500 disabled:opacity-30">
                <ChevronRight className="w-[18px] h-[18px]" />
              </button>
              {idx >= 0 && <span className="text-[11px] text-stone-400 ml-1.5 tabular-nums font-semibold">{idx + 1} of {filtered.length}</span>}
            </div>
            <button onClick={closeDrawer} className="inline-flex items-center gap-1.5 h-8 px-3 rounded-lg text-[12px] font-semibold text-stone-500 hover:bg-stone-100 transition">
              <X className="w-[15px] h-[15px]" /> Close
            </button>
          </div>
          <div className="flex-1 overflow-hidden flex flex-col">
            {drawerOpen && activeContact ? <ProfilePanel contact={activeContact} onDeleted={onDeleted} /> : null}
          </div>
        </div>
      </div>
    </div>
  )
}
