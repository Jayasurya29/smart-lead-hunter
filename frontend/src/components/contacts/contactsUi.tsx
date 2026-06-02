/**
 * Contacts UI — shared helpers + small presentational pieces for the
 * redesigned (split-view, AI-first) Contacts page.
 *
 * Pure UI / derivation only. No data fetching here.
 */
import { Star, TrendingUp } from 'lucide-react'
import { cn, getTierLabel } from '@/lib/utils'
import type { InboxContact } from '@/api/inboxContacts'

/* ──────────────────────────────────────────
   NAME / IDENTITY
   ────────────────────────────────────────── */

export function fullName(c: InboxContact): string {
  return (
    [c.first_name, c.last_name].filter(Boolean).join(' ') ||
    c.display_name ||
    c.email ||
    '—'
  )
}

export function initials(c: InboxContact): string {
  const a = c.first_name?.[0] || ''
  const b = c.last_name?.[0] || ''
  const both = (a + b).toUpperCase()
  if (both) return both
  return (c.display_name?.[0] || c.email?.[0] || '?').toUpperCase()
}

/** Confidence as a 0–100 integer. Backend stores 0–1. */
export function confidencePct(c: InboxContact): number {
  const v = c.confidence ?? c.enrichment_confidence ?? 0
  return Math.round((v <= 1 ? v * 100 : v))
}

export function roleText(c: InboxContact): string | null {
  return c.title || c.inferred_role || null
}

export function isHighOpportunity(c: InboxContact): boolean {
  return (c.opportunity_level || '').toLowerCase() === 'high'
}

/* ──────────────────────────────────────────
   COLOR SYSTEM
   ────────────────────────────────────────── */

export const AVATAR_GRADIENT: Record<string, string> = {
  buyer: 'linear-gradient(135deg,#1a7a55,#0f5c3e)',
  seller: 'linear-gradient(135deg,#c49a3c,#a8832e)',
  competitor: 'linear-gradient(135deg,#e85d4a,#d14836)',
  personal: 'linear-gradient(135deg,#3e638c,#253d5e)',
  junk: 'linear-gradient(135deg,#b0a99e,#8a847b)',
}

export function avatarGradient(category: string | null): string {
  return AVATAR_GRADIENT[category || 'junk'] || AVATAR_GRADIENT.junk
}

const CATEGORY_BADGE: Record<string, string> = {
  buyer: 'bg-emerald-50 text-emerald-700 ring-1 ring-emerald-200',
  seller: 'bg-gold-50 text-gold-700 ring-1 ring-gold-200',
  competitor: 'bg-coral-50 text-coral-600 ring-1 ring-coral-200',
  personal: 'bg-navy-50 text-navy-600 ring-1 ring-navy-200',
  junk: 'bg-stone-100 text-stone-500 ring-1 ring-stone-200',
}

/* ──────────────────────────────────────────
   DERIVED INTELLIGENCE (UI-side, from known fields)
   ────────────────────────────────────────── */

/** Signal chips shown in the AI band — derived from concrete fields. */
export function deriveSignals(c: InboxContact): string[] {
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
export function deriveNextSteps(c: InboxContact): string[] {
  const cat = c.contact_category
  const steps: string[] = []

  if (cat === 'competitor') {
    steps.push('Flag as competitor in CRM', 'Keep replies factual — withhold pricing')
    return steps
  }
  if (cat === 'seller' || c.gpo) {
    steps.push(
      c.gpo ? `Align on ${c.gpo} approved-supplier path` : 'Submit supplier-application packet',
      'Share pricing & capacity sheet',
    )
  }
  if (cat === 'buyer' || !cat) {
    steps.push(
      c.is_decision_maker ? 'Draft a tailored proposal' : 'Identify the decision-maker / approver',
    )
    if (isHighOpportunity(c)) steps.push('Prioritize outreach within 7 days')
  }
  if (!c.phone) steps.push('Find a direct dial (Deep Enrich)')
  if (!c.matched_lead_id && !c.matched_hotel_id) steps.push('Match to a lead or hotel record')

  return Array.from(new Set(steps)).slice(0, 3)
}

/** AI summary text: real enriched background, else a composed fallback. */
export function deriveSummary(c: InboxContact): string {
  if (c.background) return c.background
  const role = (c.seniority || roleText(c) || 'a contact').toString().toLowerCase()
  const dept = c.department ? ` in ${c.department.toLowerCase()}` : ''
  const org = c.organization ? ` at ${c.organization}` : ''
  const opp = isHighOpportunity(c)
    ? ' Flagged as a high-opportunity account — prioritize outreach.'
    : ''
  return `${fullName(c)} appears to be ${role}${dept}${org}.${opp} Run Deep Enrich to generate a full AI profile.`
}

/* ──────────────────────────────────────────
   PRESENTATIONAL COMPONENTS
   ────────────────────────────────────────── */

export function CategoryBadge({
  category,
  size = 'sm',
}: {
  category: string | null
  size?: 'sm' | 'md'
}) {
  if (!category) return <span className="text-stone-300 text-xs">—</span>
  const pad = size === 'sm' ? 'px-2 py-0.5 text-[10px]' : 'px-2.5 py-1 text-xs'
  return (
    <span
      className={cn(
        'inline-flex items-center rounded-full font-bold capitalize',
        pad,
        CATEGORY_BADGE[category] || CATEGORY_BADGE.junk,
      )}
    >
      {category}
    </span>
  )
}

/** Circular confidence meter. */
export function ConfRing({ value, size = 34 }: { value: number; size?: number }) {
  const r = (size - 5) / 2
  const c = 2 * Math.PI * r
  const off = c * (1 - value / 100)
  const col = value >= 90 ? '#1a7a55' : value >= 75 ? '#c49a3c' : '#b0a99e'
  return (
    <div className="relative flex-shrink-0" style={{ width: size, height: size }}>
      <svg width={size} height={size} className="-rotate-90">
        <circle cx={size / 2} cy={size / 2} r={r} fill="none" stroke="#edebe5" strokeWidth={3} />
        <circle
          cx={size / 2}
          cy={size / 2}
          r={r}
          fill="none"
          stroke={col}
          strokeWidth={3}
          strokeDasharray={c}
          strokeDashoffset={off}
          strokeLinecap="round"
          style={{ transition: 'stroke-dashoffset .6s cubic-bezier(.16,1,.3,1)' }}
        />
      </svg>
      <span
        className="absolute inset-0 flex items-center justify-center text-[9px] font-bold tabular-nums"
        style={{ color: col }}
      >
        {value}
      </span>
    </div>
  )
}

/** Avatar with category-tinted gradient + decision-maker pip. */
export function Avatar({
  contact,
  size = 40,
  showPip = true,
}: {
  contact: InboxContact
  size?: number
  showPip?: boolean
}) {
  return (
    <div
      className="relative flex-shrink-0 rounded-full flex items-center justify-center text-white font-bold select-none"
      style={{
        width: size,
        height: size,
        fontSize: size * 0.36,
        background: avatarGradient(contact.contact_category),
        boxShadow: '0 2px 6px rgba(15,29,50,.18), inset 0 1px 0 rgba(255,255,255,.25)',
      }}
    >
      {initials(contact)}
      {showPip && contact.is_decision_maker && (
        <span
          title="Likely decision-maker"
          className="absolute -bottom-0.5 -right-0.5 w-[18px] h-[18px] rounded-full bg-gold-400 ring-2 ring-white flex items-center justify-center"
        >
          <Star className="w-2.5 h-2.5 text-white" fill="white" strokeWidth={0} />
        </span>
      )}
    </div>
  )
}

export function HighOppTag() {
  return (
    <span className="inline-flex items-center gap-1 text-[10px] font-bold text-coral-500 whitespace-nowrap flex-shrink-0">
      <TrendingUp className="w-3 h-3" />
      High opp
    </span>
  )
}
