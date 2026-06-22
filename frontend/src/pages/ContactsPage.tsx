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
import { useEffect, useMemo, useRef, useState, type ReactNode } from 'react'
import { useSearchParams } from 'react-router-dom'
import Fuse from 'fuse.js'
import {
  Sparkles, Wand2, X, RefreshCw, Inbox, Star, ChevronRight, ChevronDown,
  Mail, Phone, Linkedin, ExternalLink, MapPin, Building2, Shield, Hash,
  Eye, Users, Activity, Send, Check, Loader2, Trash2, CheckSquare, Square,
  Radar, Briefcase, Layers, Flame, Target, Package, Copy, Pencil,
} from 'lucide-react'
import { cn, formatDate, relativeDate, getTierLabel } from '@/lib/utils'
import type { InboxContact, InboxContactStats } from '@/api/inboxContacts'
import AffiliationCard from '@/components/contacts/AffiliationCard'
import {
  useAllInboxContacts,
  useAllLeadContacts,
  useInboxContactStats,
  useTriggerInboxSync,
  useDeepEnrichContact,
  useFindLinkedin,
  useFindCurrentEmployer,
  useFindSuccessor,
  useApproveInboxContact,
  useBulkApproveInboxContacts,
  useDeleteInboxContact,
  useUpdateInboxContact,
  useUpdateLeadContact,
} from '@/hooks/useInboxContacts'

/* ════════════════════════════════════════════════════════════════════
   HELPERS
   ════════════════════════════════════════════════════════════════════ */

type SortKey = 'confidence' | 'opportunity' | 'recent' | 'newest' | 'oldest' | 'name' | 'org' | 'warmth' | 'gaps'

/** Lead-generator contact IDs are offset into their own range so they can
 *  never collide with inbox-contact IDs in React keys / selection / the URL.
 *  (Real DB id = displayed id − offset.) */
const LEAD_ID_OFFSET = 10_000_000

/* ── Triangulation helpers ────────────────────────────────────────────
   Warmth = how alive the email relationship is with an account:
   each known contact contributes interaction_count × a recency factor. */
function recencyFactor(iso: string | null): number {
  if (!iso) return 0.1
  const days = (Date.now() - new Date(iso).getTime()) / 86_400_000
  if (days <= 30) return 1
  if (days <= 90) return 0.6
  if (days <= 180) return 0.3
  return 0.1
}
function accountWarmth(known: UnifiedContact[]): number {
  return Math.round(known.reduce((s, c) => s + (c.interaction_count || 0) * recencyFactor(c.last_seen), 0))
}
type WarmthLevel = 'hot' | 'warm' | 'cool' | 'cold'
function warmthLevel(w: number): WarmthLevel {
  if (w >= 15) return 'hot'
  if (w >= 4) return 'warm'
  if (w > 0) return 'cool'
  return 'cold'
}
const WARMTH_COLOR: Record<WarmthLevel, string> = {
  hot: '#e85d4a', warm: '#c49a3c', cool: '#5b7a9e', cold: '#b0a99e',
}

/** Triangulation only counts BUYER-side relationships. Vendors (SanMar),
 *  junk, personal, and competitors generate huge email volume that would
 *  otherwise make supplier inboxes the "hottest accounts". Uncategorized
 *  (null) is included — those may be buyers awaiting classification. */
function isRelationshipContact(c: UnifiedContact): boolean {
  return !c.contact_category || c.contact_category === 'buyer'
}

/** Per-account triangulation: who we know (warm paths), which lead-generator
 *  decision-makers we're missing (gaps), and the suggested play. */
type AccountIntel = {
  known: UnifiedContact[]
  matches: UnifiedContact[]
  gaps: UnifiedContact[]
  otherTargets: number
  warmth: number
  level: WarmthLevel
  emails: number
  lastTouch: string | null
  oppScore: number | null
  play: string
  /** Majority of this org's contacts are sellers → it's a supplier, not a target. */
  vendor: boolean
}

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
  /** Triangulation: this inbox contact's email also exists as a lead-generator
   *  target — a verified decision-maker you already have a relationship with. */
  target_match?: boolean
  target_priority?: string | null
  target_reasoning?: string | null
  /** Entity-resolution grouping key (035) + how many directory rows resolve to
   *  this same human, so duplicate per-property rows collapse into one card. */
  person_id?: number | null
  affiliation_count?: number
  /** Directory (lead-contact) rows carry a save/verify flag used to pick the
   *  richest representative when collapsing duplicates. */
  is_saved?: boolean
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

/* ── Vertical (industry) lens (2026-06-11) ──────────────────────────────────
   JA sells beyond hotels: parking/valet operators (Towne Park, SP+,
   Metropolis), universities (UMiami/Chartwells), healthcare (Wellpath) and
   grocery (Sedano's) are all real buyers. Derived client-side from org /
   email / role keywords — mirrors EDUCATION_KW / HEALTHCARE_KW in
   app/services/contact_intelligence.py so the two stay consistent. Display +
   filter only; nothing is stored, no migration. Order matters: parking and
   healthcare run before education so "Medical College" and a Towne Park rep
   stationed at a campus land in the right bucket. */
type Vertical = 'hospitality' | 'parking_valet' | 'education' | 'healthcare' | 'grocery' | 'other'
const VERT_PARKING = /towne ?park|sp ?plus|\bsp\+|metropolis|laz parking|ace parking|impark|propark|reef parking|\bvalet\b|parking/i
const VERT_HEALTH = /hospital|medical cent|medical college|health ?care|health system|clinic|medical group|physicians|surgical center|rehabilitation|infirmary|wellpath|nursing home/i
const VERT_EDU = /university|universidad|college|institute of technology|polytechnic|school district|academy|campus|seminary|chartwells/i
const VERT_GROCERY = /sedano|supermarket|grocery|food market|\bgrocer\b/i
function verticalOf(c: UnifiedContact): Vertical {
  const hay = [c.organization, c.parent_company, c.management_company, c.title, c.inferred_role, c.department, c.email]
    .filter(Boolean).join(' ').toLowerCase()
  const domain = (c.email || '').split('@')[1]?.toLowerCase() || ''
  if (VERT_PARKING.test(hay)) return 'parking_valet'
  if (VERT_HEALTH.test(hay)) return 'healthcare'
  if (domain.endsWith('.edu') || VERT_EDU.test(hay)) return 'education'
  if (VERT_GROCERY.test(hay)) return 'grocery'
  if (c.brand_tier || c.management_company || c.matched_hotel_id || c.matched_lead_id) return 'hospitality'
  if (/hotel|resort|hospitality|\binn\b|suites|lodge|casino/i.test(hay)) return 'hospitality'
  return 'other'
}

function prettyFromEmail(email?: string | null): string | null {
  const local = (email || '').split('@')[0]
  if (!local) return null
  const words = local.split('+')[0].split(/[._\-]+/)
    .map(t => t.replace(/\d+$/, ''))
    .filter(t => t.length >= 2 && /^[a-z]+$/i.test(t))
  if (words.length >= 2 && words.length <= 3)
    return words.map(w => w[0].toUpperCase() + w.slice(1)).join(' ')
  return local.length >= 2 ? local[0].toUpperCase() + local.slice(1) : null
}

function fullName(c: InboxContact): string {
  const dn = c.display_name && c.display_name.toLowerCase() !== (c.email || '').toLowerCase() ? c.display_name : null
  return [c.first_name, c.last_name].filter(Boolean).join(' ') || dn || prettyFromEmail(c.email) || c.email || '—'
}
function initials(c: InboxContact): string {
  const both = ((c.first_name?.[0] || '') + (c.last_name?.[0] || '')).toUpperCase()
  return both || (c.display_name?.[0] || c.email?.[0] || '?').toUpperCase()
}

// ── Account tree helpers (2026-06-04) ──────────────────────────────
// Two-level account hierarchy: chains via parent_company (Marriott →
// Ritz-Carlton, St. Regis, W...) and domain families for independents
// (grandbeachhotel.com → Grand Beach Hotel / ...Surfside / ...Bay Harbor).
const FREEMAIL = new Set([
  'gmail.com', 'yahoo.com', 'outlook.com', 'hotmail.com', 'aol.com', 'icloud.com',
  'me.com', 'mac.com', 'msn.com', 'live.com', 'comcast.net', 'att.net',
  'verizon.net', 'sbcglobal.net', 'bellsouth.net', 'protonmail.com', 'proton.me',
  'ymail.com', 'gmx.com', 'mail.com',
])
function workDomainOf(email?: string | null): string | null {
  const d = (email || '').split('@')[1]?.toLowerCase() || ''
  return d && !FREEMAIL.has(d) ? d : null
}
/** Most common non-empty value (case-insensitive), returned in its most common original casing. */
function modeOf(vals: (string | null | undefined)[]): string | null {
  const counts = new Map<string, { n: number; disp: string }>()
  for (const v of vals) {
    const t = (v || '').trim()
    if (!t) continue
    const k = t.toLowerCase()
    const e = counts.get(k) || { n: 0, disp: t }
    e.n++
    counts.set(k, e)
  }
  let best: string | null = null, bn = 0
  for (const { n, disp } of counts.values()) if (n > bn) { bn = n; best = disp }
  return best
}
/** Longest common leading word sequence — names a domain family ("Grand Beach Hotel"). */
function commonTokenPrefix(names: string[]): string | null {
  if (!names.length) return null
  const tok = names.map((n) => n.split(/\s+/).filter(Boolean))
  const out: string[] = []
  for (let i = 0; ; i++) {
    const w = tok[0]?.[i]
    if (!w) break
    if (tok.every((t) => (t[i] || '').toLowerCase() === w.toLowerCase())) out.push(w)
    else break
  }
  return out.length >= 2 ? out.join(' ') : null
}
type GroupEntry = [string, UnifiedContact[]]
type TreeSection = { key: string; label: string | null; children: GroupEntry[] }

/** Find Email (Wiza) for lead-generator contacts in the directory drawer
 *  (2026-06-04). Routes to the existing lead / existing-hotel enrich-email
 *  endpoints using the contact's real (un-offset) id. */
function LeadFindEmailBtn({ contact }: { contact: UnifiedContact }) {
  const [state, setState] = useState<'idle' | 'busy' | 'done' | 'err'>('idle')
  const [found, setFound] = useState<string | null>(null)
  async function run() {
    const realId = contact.id - LEAD_ID_OFFSET
    const url = contact.matched_lead_id
      ? `/api/dashboard/leads/${contact.matched_lead_id}/contacts/${realId}/enrich-email`
      : contact.matched_hotel_id
        ? `/api/existing-hotels/${contact.matched_hotel_id}/contacts/${realId}/enrich-email`
        : null
    if (!url || state === 'busy') return
    setState('busy')
    try {
      const r = await fetch(url, {
        method: 'POST',
        headers: { 'X-Requested-With': 'XMLHttpRequest', 'Content-Type': 'application/json' },
      })
      const j = await r.json().catch(() => ({} as any))
      const em = j?.email || j?.contact?.email || null
      if (r.ok && em) { setFound(em); setState('done') } else setState('err')
    } catch { setState('err') }
  }
  if (state === 'done' && found) {
    return (
      <a href={`mailto:${found}`}
        className="inline-flex items-center gap-1.5 h-9 px-3.5 rounded-lg text-[12px] font-semibold bg-emerald-500/20 text-emerald-200 ring-1 ring-emerald-400/30">
        <Check className="w-3.5 h-3.5" />{found}
      </a>
    )
  }
  return (
    <button onClick={run} disabled={state === 'busy'}
      className="inline-flex items-center gap-1.5 h-9 px-3.5 rounded-lg text-[12px] font-semibold bg-white/10 text-white/80 ring-1 ring-white/20 hover:bg-white/15 disabled:opacity-60 transition-all active:scale-[.97]">
      {state === 'busy' ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Mail className="w-3.5 h-3.5" />}
      {state === 'err' ? 'No email found — retry?' : 'Find Email (Wiza)'}
    </button>
  )
}

/** Kick tier-1 classification over still-uncategorized contacts (2026-06-11).
 *  Shown only while the Uncategorized facet is active — closes the loop after
 *  a big backfill instead of just exposing the backlog. Fire-and-forget: the
 *  Celery task runs in the background and the stats refresh on the normal
 *  30s poll, so counts drain visibly. */
function ClassifyNowBtn({ count }: { count?: number }) {
  const [state, setState] = useState<'idle' | 'busy' | 'done' | 'err'>('idle')
  async function run() {
    if (state === 'busy' || state === 'done') return
    setState('busy')
    try {
      const r = await fetch('/api/inbox-contacts/classify', {
        method: 'POST',
        headers: { 'X-Requested-With': 'XMLHttpRequest' },
      })
      setState(r.ok ? 'done' : 'err')
    } catch { setState('err') }
  }
  return (
    <button onClick={run} disabled={state === 'busy' || state === 'done'}
      className="inline-flex items-center gap-1.5 h-7 px-2.5 rounded-lg text-[12px] font-semibold bg-navy-600 text-white hover:bg-navy-700 disabled:opacity-60 transition-all focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-navy-500">
      {state === 'busy' ? <Loader2 className="w-3 h-3 animate-spin" /> : <Sparkles className="w-3 h-3" />}
      {state === 'done' ? 'Classifying in background…' : state === 'err' ? 'Failed — retry' : `Classify ${count ? count.toLocaleString() : 'these'} now`}
    </button>
  )
}
/** Backend stores confidence as 0–1; surface a 0–100 integer. */
function confidencePct(c: InboxContact): number {
  const v = c.confidence ?? c.enrichment_confidence ?? 0
  return Math.round(v <= 1 ? v * 100 : v)
}
function roleText(c: InboxContact): string | null {
  return c.title || c.inferred_role || null
}
// A "high opportunity" contact is someone worth pitching uniforms to: a
// buyer-side person who is either a flagged decision-maker, a verified
// lead-gen target, or holds a buying-relevant role (procurement, F&B, GM,
// operations, housekeeping, ownership, C-suite). This deliberately ignores
// the brand-inherited `opportunity_level` — nearly every hotel brand is
// hard-coded "high" in the registry, so on its own it carries no signal.
const BUYING_ROLE = /procure|purchas|sourc|supply chain|f\s*&\s*b|food\s*(?:and|&)\s*beverage|general manager|\bgm\b|operations|housekeep|rooms division|\bowner\b|proprietor|principal|\bpresident\b|\bchief\b|\bc[eo]o\b|\bcpo\b/i
function isStale(c: InboxContact): boolean {
  if (!c.last_inbound_at) return false
  return Date.now() - new Date(c.last_inbound_at).getTime() > 18 * 30.4 * 24 * 3600 * 1000
}

function isHighOpportunity(c: InboxContact): boolean {
  const cat = c.contact_category
  if (cat === 'seller' || cat === 'competitor' || cat === 'personal' || cat === 'junk' || cat === 'operational') return false
  if (c.is_decision_maker || (c as UnifiedContact).target_match) return true
  const role = roleText(c)
  return !!role && BUYING_ROLE.test(role)
}

const AVATAR_GRADIENT: Record<string, string> = {
  buyer: 'linear-gradient(135deg,#1a7a55,#0f5c3e)',
  seller: 'linear-gradient(135deg,#c49a3c,#a8832e)',
  competitor: 'linear-gradient(135deg,#e85d4a,#d14836)',
  personal: 'linear-gradient(135deg,#3e638c,#253d5e)',
  junk: 'linear-gradient(135deg,#b0a99e,#8a847b)',
  operational: 'linear-gradient(135deg,#5b7a9e,#3e638c)',
}
const avatarGradient = (cat: string | null) => AVATAR_GRADIENT[cat || 'junk'] || AVATAR_GRADIENT.junk

const CATEGORY_BADGE: Record<string, string> = {
  buyer: 'bg-emerald-50 text-emerald-700 ring-1 ring-emerald-200',
  seller: 'bg-gold-50 text-gold-700 ring-1 ring-gold-200',
  competitor: 'bg-coral-50 text-coral-600 ring-1 ring-coral-200',
  personal: 'bg-navy-50 text-navy-600 ring-1 ring-navy-200',
  junk: 'bg-stone-100 text-stone-500 ring-1 ring-stone-200',
  operational: 'bg-navy-50 text-navy-600 ring-1 ring-navy-200',
}

/** Signal chips — derived from concrete fields (heuristics, not new API data). */
function deriveSignals(c: InboxContact): string[] {
  const out: string[] = []
  if (c.is_decision_maker) out.push('Decision-maker')
  // Only show the heuristic 'High opportunity' chip when there is no real
  // content-based buying signal -- otherwise the Buying Signal card wins.
  if (isHighOpportunity(c) && !c.buying_signal_label) out.push('High opportunity')
  // Avendra GPO is irrelevant to a uniform sale (it's a food/beverage
  // procurement platform) -- never surface it as a signal. Other GPOs, if any,
  // can still show.
  if (c.gpo && !/avendra/i.test(c.gpo)) out.push(`${c.gpo} GPO`)
  if (c.seniority && /exec|director|chief|vp|head/i.test(c.seniority)) out.push(c.seniority)
  if (c.brand_tier && /tier1|tier2|luxury/i.test(c.brand_tier)) out.push(getTierLabel(c.brand_tier))
  if (c.phone) out.push('Direct dial on file')
  // Raw email volume is shown in the Engagement card; it is not a buying
  // signal, so it no longer appears as an AI chip.
  return out.slice(0, 4)
}

/** AI summary text: real enriched background, else a composed fallback. */
function deriveSummary(c: InboxContact): string {
  if (c.background) return c.background
  // Prefer real content-based buying evidence over the speculative
  // role/volume fallback (avoids 'appears to be unknown ...').
  if (c.buying_signal_label === 'buyer_evidence' || c.buying_signal_label === 'active_contact') {
    const org = c.organization ? ` at ${c.organization}` : ''
    const stg = c.buying_signal_stage && c.buying_signal_stage !== 'noise' && c.buying_signal_stage !== 'internal'
      ? ` Current stage: ${c.buying_signal_stage}.` : ''
    const prod = c.buying_signal_products ? ` of ${c.buying_signal_products}` : ''
    const verb = c.buying_signal_label === 'buyer_evidence'
      ? `is an active buyer${prod}` : `is in an active buying conversation${prod ? ` about${prod.replace(' of', '')}` : ''}`
    return `${fullName(c)}${org} ${verb}.${stg} See the buying signal below for details.`
  }
  const role = (c.seniority || roleText(c) || 'a contact').toString().toLowerCase()
  const dept = c.department ? ` in ${c.department.toLowerCase()}` : ''
  const org = c.organization ? ` at ${c.organization}` : ''
  const opp = isHighOpportunity(c) ? ' Flagged as a high-opportunity account — prioritize outreach.' : ''
  const cta = sourceOf(c) === 'lead_generator'
    ? ' Sourced by the Lead Generator pipeline.'
    : ' Run Deep Enrich to generate a full AI profile.'
  return `${fullName(c)} appears to be ${role}${dept}${org}.${opp}${cta}`
}

/** Build a contact's search haystack ONCE (precomputed in `indexed` below) —
 *  previously this 20-field join+lowercase ran for every contact on every
 *  keystroke, which is exactly what made typing drag at ~7k contacts. */
function buildHaystack(c: UnifiedContact): string {
  const acctLabel = accountTypeOf(c) === 'management_company' ? 'management company mgmt co operator' : 'hotel'
  const srcLabel = sourceOf(c) === 'lead_generator' ? 'lead generator scraped discovery prospect' : 'email inbox'
  return [
    c.first_name, c.last_name, c.display_name, c.title, c.inferred_role, c.organization,
    c.email, c.address, c.department, c.parent_company, c.management_company, c.gpo, getTierLabel(c.brand_tier),
    c.contact_category, c.opportunity_level, acctLabel, srcLabel, stageOf(c),
    c.target_match ? 'verified target match' : '',
    verticalOf(c), (c.source_mailboxes || []).join(' '),
  ].filter(Boolean).join(' ').toLowerCase()
}

/** Natural-language SHORTCUTS. Returns a boolean when the query is one of the
 *  known intent phrases (decision makers / luxury / buyers / ...), or `null`
 *  when it is free text -- in which case the caller runs fuzzy search instead. */
function shortcutMatch(c: UnifiedContact, t: string): boolean | null {
  if (!t) return true
  if (/decision|maker|\bdm\b/.test(t)) return !!c.is_decision_maker
  if (/high opp|opportun|hot lead/.test(t)) return isHighOpportunity(c)
  if (/management compan|mgmt|operator/.test(t)) return accountTypeOf(c) === 'management_company'
  if (/lead gen|scraped|discover|prospect/.test(t)) return sourceOf(c) === 'lead_generator'
  if (/existing|customer|current client/.test(t)) return stageOf(c) === 'existing'
  if (/potential|new lead/.test(t)) return stageOf(c) === 'potential'
  if (/stale|moved|may have moved|outdated/.test(t)) return isStale(c)
  if (/repl|recent|engaged|active/.test(t)) return c.interaction_count >= 5
  if (/luxury|premium|upscale|high.?end/.test(t)) return /luxury|upscale/i.test(getTierLabel(c.brand_tier) || '')
  if (/buyer/.test(t)) return c.contact_category === 'buyer'
  if (/seller|gpo/.test(t)) return c.contact_category === 'seller'
  if (/competitor/.test(t)) return c.contact_category === 'competitor'
  if (/phone|call|dial/.test(t)) return !!c.phone
  return null // free text -> fuzzy search
}

/** True when the query is a known shortcut phrase (so we skip fuzzy search). */
function isShortcutQuery(t: string): boolean {
  return /decision|maker|\bdm\b|high opp|opportun|hot lead|management compan|mgmt|operator|lead gen|scraped|discover|prospect|existing|customer|current client|potential|new lead|repl|recent|engaged|active|luxury|premium|upscale|high.?end|buyer|seller|gpo|competitor|phone|call|dial/.test(t)
}

/** Legacy exact-substring matcher, kept as a fuzzy-search FALLBACK guard. */
function smartMatch(c: UnifiedContact, hay: string, t: string): boolean {
  const sc = shortcutMatch(c, t)
  if (sc !== null) return sc
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
    <div role="img" aria-label={contact.contact_category ? `${fullName(contact)} — ${contact.contact_category}` : fullName(contact)}
      className="relative flex-shrink-0 rounded-full flex items-center justify-center text-white font-bold select-none"
      style={{ width: size, height: size, fontSize: size * 0.36, background: avatarGradient(contact.contact_category), boxShadow: '0 2px 6px rgba(15,29,50,.18), inset 0 1px 0 rgba(255,255,255,.25)' }}>
      <span aria-hidden="true">{initials(contact)}</span>
      {showPip && contact.is_decision_maker && (
        <span role="img" aria-label="Likely decision-maker" title="Likely decision-maker" className="absolute -bottom-0.5 -right-0.5 w-[18px] h-[18px] rounded-full bg-gold-400 ring-2 ring-white flex items-center justify-center">
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
  // a11y (2026-06-11): Esc closes and returns focus to the trigger; Arrow
  // keys cycle the options; proper popup semantics for screen readers.
  const btnRef = useRef<HTMLButtonElement | null>(null)
  const menuRef = useRef<HTMLDivElement | null>(null)
  const cur = options.find((o) => o.v === value) || options[0]
  const isAll = value == null || value === 'all' || value === ''
  function close(refocus = true) {
    setOpen(false)
    if (refocus) btnRef.current?.focus()
  }
  return (
    <div className="relative">
      <button ref={btnRef} aria-haspopup="listbox" aria-expanded={open}
        onClick={() => setOpen((o) => !o)}
        onKeyDown={(e) => { if (e.key === 'Escape' && open) { e.stopPropagation(); close() } }}
        className={cn('inline-flex items-center gap-1.5 h-9 px-3 rounded-lg text-[13px] font-medium transition-colors',
          'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-navy-500',
          isAll ? 'text-stone-500 hover:bg-stone-100' : 'text-navy-800 bg-navy-50 ring-1 ring-navy-100 font-semibold')}>
        {Ic && <Ic className={cn('w-3.5 h-3.5', isAll ? 'text-stone-400' : 'text-navy-500')} />}
        <span>{isAll ? label : cur.label}</span>
        <ChevronDown aria-hidden="true" className="w-3.5 h-3.5 opacity-40" />
      </button>
      {open && (
        <>
          <div className="fixed inset-0 z-40" onClick={() => close(false)} />
          <div ref={menuRef} role="listbox" aria-label={label}
            onKeyDown={(e) => {
              if (e.key === 'Escape') { e.stopPropagation(); close(); return }
              if (e.key !== 'ArrowDown' && e.key !== 'ArrowUp') return
              e.preventDefault()
              const opts = Array.from(menuRef.current?.querySelectorAll<HTMLButtonElement>('button[data-opt]') || [])
              if (!opts.length) return
              const i = opts.indexOf(document.activeElement as HTMLButtonElement)
              const next = e.key === 'ArrowDown' ? opts[(i + 1) % opts.length] : opts[(i - 1 + opts.length) % opts.length]
              next?.focus()
            }}
            className={cn('absolute z-50 mt-1.5 min-w-[200px] bg-white rounded-xl shadow-lift ring-1 ring-stone-200/80 p-1.5', align === 'right' ? 'right-0' : 'left-0')}>
            {options.map((o) => (
              <button key={o.v} data-opt role="option" aria-selected={o.v === value}
                onClick={() => { onChange(o.v); close() }}
                className={cn('w-full flex items-center gap-2.5 px-2.5 py-2 rounded-lg text-[13px] text-left transition-colors',
                  'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-navy-500',
                  o.v === value ? 'bg-stone-50 font-semibold text-navy-900' : 'text-stone-600 hover:bg-stone-50')}>
                {o.icon ? <o.icon className="w-4 h-4 text-stone-400 flex-shrink-0" /> : o.dot ? <span className="w-2 h-2 rounded-full flex-shrink-0" style={{ background: o.dot }} /> : null}
                <span className="flex-1">{o.label}</span>
                {o.count != null && <span className="text-[11px] tabular-nums text-stone-500">{o.count}</span>}
                {o.v === value && <Check className="w-4 h-4" style={{ color: 'var(--accent, #2e4a6e)' }} />}
              </button>
            ))}
          </div>
        </>
      )}
    </div>
  )
}

/* Collapse rows that resolve to the same person (entity resolution, 035):
   keep the richest representative (saved > has-email > most affiliations >
   lowest id) and remember how many rows folded in. Rows without a person_id
   pass through unchanged, preserving order. */
/* Known company rebrands / aliases (display-only, 2026-06-10). Maps a dead or
   alternate operator name to its current canonical name so a person split across
   the old and new name (e.g. Jordi Pelfort, whose org was scraped as both "Blue
   Diamond Resorts" and the post-2025 "Royalton Hotels & Resorts") groups under
   ONE account header. Display-only: stored data is untouched, exports/API still
   see the original. Add a line per confirmed rebrand; match is exact (normalized),
   never substring, to avoid catching unrelated "Blue Diamond …" entities. */
const COMPANY_ALIASES: Record<string, string> = {
  'blue diamond resorts': 'Royalton Hotels & Resorts',
  // The Pyrmont Curacao -- one Marriott Autograph Collection property, scraped
  // under many names (word-order + brand-phrase variants the key can't collapse).
  'curacao marriott and the pyrmont curacao': 'The Pyrmont Curacao',
  'curacao marriott beach resort | the pyrmont curacao beach resort, an autograph collection all-inclusive': 'The Pyrmont Curacao',
  'the pyrmont curacao an autograph collection hotel by marriott': 'The Pyrmont Curacao',
  'the pyrmont curacao, an autograph collection all-inclusive resort': 'The Pyrmont Curacao',
  'the pyrmontcuracao': 'The Pyrmont Curacao',
}

// Decoration that should NOT split one real account into several groups.
// Leading article, trailing legal/suffix words, and "& Co"-style tails are
// stripped for the GROUPING KEY only -- the displayed name keeps its original
// spelling. Deliberately conservative: removes boilerplate, never reorders
// words or fuzzy-matches, so "Hotel California" and "California Hotel" stay
// distinct (different word order = different account).
const _LEADING_THE = /^the\s+/i
const _ACCOUNT_SUFFIX = new RegExp(
  '\\s+(?:&\\s*co|and\\s+co|co|company|inc|incorporated|llc|l\\.l\\.c|ltd|limited|' +
  'corp|corporation|group|holdings|management|mgmt|hospitality|properties|' +
  'enterprises|international|intl)\\.?$',
  'i',
)

/** Aggressively-normalized grouping key (never shown to the user). */
function accountKey<T extends string | null | undefined>(name: T): string {
  if (!name) return 'No organization'
  let s = String(name).trim().toLowerCase()
  s = COMPANY_ALIASES[s] ? COMPANY_ALIASES[s].toLowerCase() : s
  s = s.replace(_LEADING_THE, '')
  let prev = ''
  while (prev !== s) {
    prev = s
    s = s.replace(_ACCOUNT_SUFFIX, '').trim()
  }
  s = s.replace(/\s+(?:&|and)\s+(?:resorts?|suites?|spa|spas|villas?|residences?|hotels?|clubs?)$/i, '').trim()
  s = s.replace(/[.,'"&]+/g, ' ').replace(/\s+/g, ' ').trim()
  s = s.replace(/\b(hotels|resorts|suites|inns|villas|clubs)\b/g, (m) => m.slice(0, -1))
  // Final step: collapse ALL internal spacing so cosmetic variants group as one
  // ("Brooks Brothers"=="Brooksbrothers", "The Pyrmontcuracao"=="Pyrmont Curacao").
  // Conservative on purpose: no word reorder, no fuzzy, no interior-word strip,
  // so "PG"!="PGA Resort" and "Hotel California"!="California Hotel".
  s = s.replace(/\s+/g, '')
  return s || 'No organization'
}

/** Display name: alias-mapped original spelling (keeps "The", "Hotels", case). */
function canonCompany<T extends string | null | undefined>(name: T): T | string {
  if (!name) return name
  return COMPANY_ALIASES[name.trim().toLowerCase()] || name
}

function collapsePeople(list: UnifiedContact[]): UnifiedContact[] {
  const buckets = new Map<number, UnifiedContact[]>()
  const out: UnifiedContact[] = []
  const slot = new Map<number, number>() // person_id → index in out (preserve order)
  for (const c of list) {
    const pid = c.person_id
    if (pid == null) { out.push(c); continue }
    if (!buckets.has(pid)) { buckets.set(pid, []); slot.set(pid, out.length); out.push(c) }
    buckets.get(pid)!.push(c)
  }
  for (const [pid, rows] of buckets) {
    if (rows.length === 1) continue
    const best = [...rows].sort((a, b) =>
      (Number(!!b.is_saved) - Number(!!a.is_saved)) ||
      (Number(!!b.email) - Number(!!a.email)) ||
      ((b.affiliation_count || 0) - (a.affiliation_count || 0)) ||
      (a.id - b.id),
    )[0]
    out[slot.get(pid)!] = best
  }
  return out
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
    <div onClick={onOpen} role="button" tabIndex={0}
      aria-label={`Open ${fullName(contact)}`}
      onKeyDown={(e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); onOpen() } }}
      className={cn('group flex items-center gap-3 pl-3 pr-4 py-2.5 rounded-xl cursor-pointer transition-colors',
        'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-navy-500',
        selected ? 'bg-white shadow-card ring-1 ring-navy-100' : 'hover:bg-white')}>
      {isLead ? (
        <span className="w-4 flex-shrink-0" />
      ) : (
        <button onClick={(e) => { e.stopPropagation(); onToggleCheck() }}
          role="checkbox" aria-checked={checked} aria-label={`Select ${fullName(contact)}`}
          className={cn('flex-shrink-0 transition-opacity', checked || selectMode ? 'opacity-100' : 'opacity-0 group-hover:opacity-100',
            'focus-visible:opacity-100 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-navy-500 rounded')}>
          {checked ? <CheckSquare className="w-4 h-4 text-navy-600" /> : <Square className="w-4 h-4 text-stone-300" />}
        </button>
      )}
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
          {(contact.affiliation_count || 0) > 1 && <span className="inline-flex items-center text-[10px] font-bold text-violet-700 bg-violet-50 px-1.5 py-0.5 rounded-full flex-shrink-0" title={`Resolved across ${contact.affiliation_count} records / properties`}>{contact.affiliation_count} affiliations</span>}
          {contact.target_match && <span className="text-[10px] font-bold text-emerald-700 bg-emerald-50 px-1.5 py-0.5 rounded-full flex-shrink-0" title="Lead-generator target you already email — verified relationship">verified</span>}
        </div>
        <div className="truncate text-stone-500 text-[13px] mt-0.5">{roleText(contact) || 'role unknown'}</div>
      </div>
      {/* email — the field reps need most */}
      <div className="min-w-0 flex-1 hidden md:block">
        {contact.email
          ? <a href={`mailto:${contact.email}`} onClick={(e) => e.stopPropagation()} className="truncate block text-[13px] text-navy-600 hover:text-navy-800 hover:underline font-medium">{contact.email}</a>
          : <span className="text-[12px] text-stone-500 italic">no email yet</span>}
      </div>
      {/* quick actions — appear on hover, act without opening the drawer */}
      <div className="hidden lg:flex items-center gap-1 flex-shrink-0 opacity-0 group-hover:opacity-100 transition-opacity">
        {contact.email && <a href={`mailto:${contact.email}`} onClick={(e) => e.stopPropagation()} title="Email" className="w-7 h-7 rounded-lg hover:bg-stone-100 flex items-center justify-center text-stone-500"><Mail className="w-4 h-4" /></a>}
        {contact.phone && <a href={`tel:${contact.phone}`} onClick={(e) => e.stopPropagation()} title="Call" className="w-7 h-7 rounded-lg hover:bg-stone-100 flex items-center justify-center text-stone-500"><Phone className="w-4 h-4" /></a>}
      </div>
      {/* org + meta (status pills removed — org name is more useful here). Org is a link → jumps to that company. */}
      <div className="flex items-center gap-3 flex-shrink-0">
        {isHighOpportunity(contact) && <span className="hidden sm:inline-flex items-center gap-1 px-1.5 py-0.5 rounded-full text-[10px] font-bold bg-coral-50 text-coral-600 ring-1 ring-coral-200" title="High-opportunity account — prioritize outreach"><Flame className="w-3 h-3" />High opp</span>}
        {!hideOrg && contact.organization
          ? <button onClick={(e) => { e.stopPropagation(); onOpenOrg?.(contact.organization!) }} title={`See everyone at ${contact.organization}`}
              className="hidden sm:block w-[200px] text-right truncate text-[12px] font-semibold text-navy-700 hover:text-navy-900 hover:underline">{contact.organization}</button>
          : <span className="hidden sm:block w-[200px]" />}
        {!isLead && contact.interaction_count > 0 && (
          <span className="hidden lg:inline-flex items-center gap-1 text-[11px] tabular-nums text-stone-500 whitespace-nowrap"
            title={`${contact.interaction_count} email${contact.interaction_count > 1 ? 's' : ''} exchanged — relationship warmth`}>
            <Activity className="w-3 h-3 text-stone-400" aria-hidden="true" />{contact.interaction_count}
          </span>
        )}
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
  org, members, expanded, onToggle, selectedId, checked, selectMode, onToggleCheck, onSelect, onOpenOrg, intel, onOpenAccount,
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
  intel?: AccountIntel
  onOpenAccount?: (org: string) => void
}) {
  const dm = members.filter((m) => m.is_decision_maker).length
  const isMgmt = members.length > 0 && accountTypeOf(members[0]) === 'management_company'
  const tier = members.find((m) => m.brand_tier)?.brand_tier
  const allExisting = members.every((m) => stageOf(m) === 'existing')
  // huge buckets (e.g. "No organization") would dump hundreds of rows into
  // the DOM — preview the first 40 and expand on demand
  const MEMBER_PREVIEW = 40
  const [showAll, setShowAll] = useState(false)
  const shown = showAll ? members : members.slice(0, MEMBER_PREVIEW)
  const canIntel = !!onOpenAccount && !!intel && org !== 'No organization'
  const isVendor = !!intel?.vendor
  return (
    <div className="mb-2" data-org={org}>
      <div onClick={onToggle} role="button" tabIndex={0} aria-expanded={expanded}
        aria-label={`${expanded ? 'Collapse' : 'Expand'} ${org}`}
        onKeyDown={(e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); onToggle() } }}
        className="w-full flex items-center gap-3 px-3 py-2.5 rounded-xl hover:bg-white transition-colors cursor-pointer sticky top-0 z-10 bg-stone-50 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-navy-500">
        <ChevronRight className="w-3.5 h-3.5 text-stone-300 flex-shrink-0" style={{ transform: expanded ? 'rotate(90deg)' : 'none', transition: 'transform .18s ease' }} />
        <button
          onClick={(e) => { if (!canIntel) return; e.stopPropagation(); onOpenAccount!(org) }}
          title={canIntel ? 'Account intelligence — warm paths & gaps' : undefined}
          className={cn('w-9 h-9 rounded-xl flex items-center justify-center flex-shrink-0 transition-all',
            isVendor ? 'bg-gold-50' : isMgmt ? 'bg-violet-50' : 'bg-navy-50',
            canIntel && 'hover:ring-2 hover:ring-gold-300 hover:scale-105')}>
          {isVendor ? <Package className="w-4 h-4 text-gold-600" /> : isMgmt ? <Briefcase className="w-4 h-4 text-violet-500" /> : <Building2 className="w-4 h-4 text-navy-500" />}
        </button>
        <div className="min-w-0 text-left flex-1">
          <div className="flex items-center gap-2">
            <span className="truncate text-[15px] font-bold text-navy-900">{org}</span>
            {isVendor
              ? <span className="text-[10px] font-semibold text-gold-600 flex-shrink-0">Vendor</span>
              : isMgmt
                ? <span className="text-[10px] font-semibold text-violet-600 flex-shrink-0">Mgmt co.</span>
                : (tier && <span className="text-[10px] font-semibold text-gold-600 flex-shrink-0">{getTierLabel(tier)}</span>)}
            {allExisting && <span className="w-1.5 h-1.5 rounded-full bg-emerald-400 flex-shrink-0" title="Existing customer" />}
          </div>
          <div className="text-[12px] text-stone-500 mt-0.5 flex items-center gap-1.5 flex-wrap">
            <span>{members.length} contact{members.length > 1 ? 's' : ''}{dm > 0 ? `  ·  ★ ${dm}` : ''}</span>
            {canIntel && intel!.warmth > 0 && (
              <span className="inline-flex items-center gap-0.5 font-semibold" style={{ color: WARMTH_COLOR[intel!.level] }} title={`Warmth ${intel!.warmth} · ${intel!.emails} emails`}>
                <Flame className="w-3 h-3" />{intel!.level}
              </span>
            )}
            {canIntel && intel!.matches.length > 0 && (
              <span className="font-semibold text-emerald-600" title="Lead-generator targets you already email">· {intel!.matches.length} verified</span>
            )}
            {canIntel && intel!.gaps.length > 0 && (
              <span className="inline-flex items-center gap-0.5 font-semibold text-coral-500" title="Decision-makers with no email relationship yet">· <Target className="w-3 h-3" />{intel!.gaps.length} gap{intel!.gaps.length > 1 ? 's' : ''}</span>
            )}
          </div>
        </div>
        <span className="text-[12px] text-stone-500 font-medium">{expanded ? 'Hide' : 'Show'}</span>
      </div>
      {expanded && (
        <div className="pl-5 ml-4 border-l border-stone-200/70 mt-1 space-y-0.5">
          {shown.map((c) => (
            <DirRow key={c.id} contact={c} selected={c.id === selectedId} hideOrg
              selectMode={selectMode} checked={checked.has(c.id)}
              onOpen={() => onSelect(c.id)} onToggleCheck={() => onToggleCheck(c.id)} onOpenOrg={onOpenOrg} />
          ))}
          {!showAll && members.length > MEMBER_PREVIEW && (
            <button onClick={() => setShowAll(true)}
              className="w-full text-left pl-3 py-2 text-[12px] font-semibold text-navy-600 hover:text-navy-800 hover:underline">
              Show all {members.length.toLocaleString()} people
            </button>
          )}
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
  const liMut = useFindLinkedin()
  /* [patch_frontend_ce_contactspage] */
  const ceMut = useFindCurrentEmployer()
  const [ceMsg, setCeMsg] = useState<string | null>(null)
  const [ceMoved, setCeMoved] = useState<boolean>(false)
  /* [patch_frontend_find_successor] */
  const succMut = useFindSuccessor()
  const [succMsg, setSuccMsg] = useState<string | null>(null)
  const [succFound, setSuccFound] = useState<boolean>(false)
  const [liResult, setLiResult] = useState<string | null>(null)
  const isLead = sourceOf(contact) === 'lead_generator'
  const [step, setStep] = useState(0)
  const [result, setResult] = useState<string | null>(null)
  const [log, setLog] = useState<{ t: string; ok?: boolean }[]>([])
  const timer = useRef<number | null>(null)

  const pushLog = (t: string, ok?: boolean) => setLog((L) => [...L, { t, ok }])
  const who = (`${contact.first_name || ''} ${contact.last_name || ''}`.trim()
    || contact.display_name || contact.email || 'this contact')
  const orgLabel = contact.organization || (contact.email ? `@${contact.email.split('@')[1] || ''}` : '')

  useEffect(() => { setResult(null); setStep(0); setLiResult(null); setLog([]); setCeMsg(null); setCeMoved(false); setSuccMsg(null); setSuccFound(false) }, [contact.id])

  async function runFindLinkedin() {
    setLiResult(null)
    pushLog(`Searching LinkedIn for ${who}${orgLabel ? ` - ${orgLabel}` : ''}...`)
    try {
      const r = await liMut.mutateAsync(contact.id)
      if (r.found && r.linkedin_url) {
        pushLog(`LinkedIn matched: ${r.linkedin_url}`, true)
        setLiResult(`Found: ${r.linkedin_url}`)
      } else {
        pushLog('No LinkedIn profile matched the name + company.', false)
        setLiResult('No LinkedIn profile found.')
      }
    } catch {
      pushLog('LinkedIn lookup failed (Serper key / network).', false)
      setLiResult('LinkedIn lookup failed -- check Serper key or try again.')
    }
  }

  async function runFindSuccessor(apply = false) {
    setSuccMsg(null)
    pushLog(apply ? 'Filing the successor...' : `Finding who holds ${who}'s old seat now...`)
    try {
      const r = await succMut.mutateAsync({ id: contact.id, apply })
      if (apply) {
        setSuccFound(false)
        const made = r.action === 'created_stub' ? ' Added as a new lead.'
          : r.action === 'merged_stub' ? ' Linked to existing contact.'
          : r.action === 'linked_note_only' ? ' Noted (property not tracked).' : ''
        setSuccMsg(`Recorded ${r.successor_name || 'successor'}.${made}`)
        pushLog(`Successor filed: ${r.successor_name || ''} (${r.action})`, true)
        return
      }
      if (!r.found) { setSuccMsg('No clear successor found for that seat yet.'); pushLog('No successor found', false); return }
      setSuccFound(true)
      setSuccMsg(`${r.successor_name}${r.successor_title ? ` (${r.successor_title})` : ''} appears to hold the ${r.seat_title || 'role'} at ${r.former_org} now. File it?`)
      pushLog(`Possible successor: ${r.successor_name}`, true)
    } catch {
      setSuccMsg('Successor lookup failed - check Serper key or try again.')
      pushLog('Successor lookup failed', false)
    }
  }

  async function runFindEmployer(apply = false, useWiza = false) {
    setCeMsg(null)
    pushLog(`Checking where ${who} works now${useWiza ? ' (Wiza)' : ''}...`)
    try {
      const r = await ceMut.mutateAsync({ id: contact.id, apply, useWiza, findEmail: useWiza })
      if (apply) {
        setCeMoved(false)
        setCeMsg(r.employer_changed ? `Re-filed to ${r.current_employer}. Now finding who took their seat...` : 'Coverage confirmed current.')
        pushLog(r.employer_changed ? `Moved -> ${r.current_employer}` : 'Still current', true)
        // [patch_frontend_merge_status] */ a confirmed move vacates a seat -> auto-find the successor
        if (r.employer_changed) { await runFindSuccessor(false) }
        return
      }
      if (!r.found) { setCeMsg('Could not confirm a current employer (no clear profile match).'); pushLog('No clear match', false); return }
      if (r.moved && r.current_employer) {
        setCeMoved(true)
        setCeMsg(`Looks like they moved to ${r.current_employer}${r.current_title ? ` (${r.current_title})` : ''}. Apply to re-file + refresh bio.`)
        pushLog(`Possible move: ${r.current_employer}`, true)
      } else {
        setCeMoved(false)
        setCeMsg(`Still at ${r.current_employer || r.on_file_org || contact.organization || 'current employer'} - coverage current.`)
        pushLog('Confirmed current', true)
      }
    } catch {
      setCeMsg('Lookup failed - check Serper key or try again.')
      pushLog('Where-now lookup failed', false)
    }
  }

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
    pushLog(`Deep enrich for ${who}: searching web${findEmail ? ' + Wiza email' : ''}...`)
    try {
      const r = await deepMut.mutateAsync({ id: contact.id, findEmail })
      if (r.role) pushLog(`Role found: ${r.role}`, true)
      if (r.found_email) pushLog(`Email found: ${r.found_email}`, true)
      if (!r.role && !r.found_email) pushLog('No new fields recovered from the web.', false)
      const bits = [
        r.role && `Role: ${r.role}`,
        r.background || undefined,
        r.found_email && `Found email: ${r.found_email}`,
        `(${r.sources_used} sources · ${Math.round((r.confidence || 0) * 100)}% confidence)`,
      ].filter(Boolean)
      setResult(bits.join(' · ') || 'No new info found.')
    } catch {
      pushLog('Deep enrich failed (Serper/Wiza key or network).', false)
      setResult('Enrichment failed — check Serper/Wiza keys or try again.')
    }
  }

  // One click fills EVERYTHING missing: deep-enrich (name/role/background) and
  // the LinkedIn finder run together, so you never have to enrich the role and
  // THEN come back for LinkedIn. Each reports its own result line.
  async function runEnrichAll() {
    // PATCH deep-enrich-fix: web dossier always runs
    // The web-search dossier (runEnrich) ALWAYS runs -- previously it was
    // skipped when LinkedIn was the only gap, so a name+role contact missing
    // only LinkedIn never got real web research. The LinkedIn finder runs
    // alongside it whenever LinkedIn is missing.
    const liMissing = !contact.linkedin_url
    setResult(null)
    setLiResult(null)
    const jobs: Promise<unknown>[] = [runEnrich(false)]
    if (liMissing) jobs.push(runFindLinkedin())
    await Promise.allSettled(jobs)
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
            {!isLead && emailMissing && !deepMut.isPending && (
              <button onClick={() => runEnrich(true)} title="Uses Wiza credits"
                className="inline-flex items-center h-7 px-3 rounded-lg text-[11px] font-bold text-white/90 ring-1 ring-white/25 hover:bg-white/10 transition-all">Find Email</button>
            )}
            {!isLead && (deepMut.isPending || liMut.isPending) && (
              <span className="inline-flex items-center gap-1.5 h-7 px-3 text-[11px] font-bold text-white/70">
                <Loader2 className="w-3 h-3 animate-spin" />{liMut.isPending && !deepMut.isPending ? 'Searching LinkedIn...' : 'Enriching...'}
              </span>
            )}
            {!isLead && !deepMut.isPending && !liMut.isPending && (() => {
              const nameMissing = !contact.first_name && !contact.last_name
              const roleMissing = !contact.title && !contact.inferred_role
              const liMissing = !contact.linkedin_url
              const gaps: string[] = []
              if (nameMissing) gaps.push('name')
              if (roleMissing) gaps.push('role')
              if (liMissing) gaps.push('LinkedIn')
              const gapLabel = gaps.length === 1
                ? (gaps[0] === 'LinkedIn' ? 'Find LinkedIn' : `Find ${gaps[0]}`)
                : 'Enrich all missing'
              return (
                <>
                  {/* Gap-filler: only when something's missing. Runs the web
                      dossier AND the LinkedIn finder together. */}
                  {gaps.length > 0 && (
                    <button onClick={runEnrichAll}
                      title={`Find ${gaps.join(' + ')} (web search + LinkedIn) for this person`}
                      className="inline-flex items-center gap-1.5 h-7 px-3 rounded-lg text-[11px] font-bold text-white/90 ring-1 ring-white/25 hover:bg-white/10 transition-all">
                      <Sparkles className="w-3 h-3" />{gapLabel}
                    </button>
                  )}
                  {/* Deep Enrich: ALWAYS visible, ALWAYS runs the full
                      web-search dossier (Serper + Gemini) on this person. */}
                  <button onClick={() => runEnrich(false)}
                    title="Full AI dossier: role, seniority, decision-maker, background (web search + Gemini)"
                    className="inline-flex items-center gap-1.5 h-7 px-3 rounded-lg text-[11px] font-bold text-navy-900 bg-gold-300 hover:bg-gold-200 transition-all active:scale-95">
                    <Wand2 className="w-3 h-3" />{result ? 'Re-run Deep Enrich' : 'Deep Enrich'}
                  </button>
                  {/* unified status button [patch_frontend_merge_status] */}
                  {!ceMut.isPending && !succMut.isPending && (
                    <button onClick={() => runFindEmployer(false)}
                      title="Check if this contact moved on, and who fills their seat now (Serper)"
                      className="inline-flex items-center gap-1.5 h-7 px-3 rounded-lg text-[11px] font-bold text-white/90 ring-1 ring-white/25 hover:bg-white/10 transition-all">
                      <Radar className="w-3 h-3" />Check status
                    </button>
                  )}
                </>
              )
            })()}
          </div>
        </div>

        {(deepMut.isPending || liMut.isPending || log.length > 0) ? (
          <div className="py-1.5 space-y-1.5">
            {log.map((entry, i) => (
              <div key={i} className="flex items-start gap-2 text-[12.5px] leading-snug">
                {entry.ok === true ? <Check className="w-3.5 h-3.5 mt-0.5 text-emerald-300 shrink-0" />
                  : entry.ok === false ? <span className="w-3.5 h-3.5 mt-0.5 shrink-0 text-rose-300 font-bold text-center leading-none">!</span>
                  : <Check className="w-3.5 h-3.5 mt-0.5 text-white/40 shrink-0" />}
                <span className={entry.ok === false ? 'text-rose-200 break-all' : 'text-white/90 break-all'}>{entry.t}</span>
              </div>
            ))}
            {(deepMut.isPending || liMut.isPending) && (
              <div className="flex items-center gap-2 text-[12.5px] text-gold-200">
                <Loader2 className="w-3.5 h-3.5 animate-spin shrink-0" />
                <span>{liMut.isPending && !deepMut.isPending ? 'Searching LinkedIn...' : 'Working...'}</span>
              </div>
            )}
          </div>
        ) : (
          <p className="text-[13.5px] leading-relaxed text-white/85 max-w-[80ch]">{deriveSummary(contact)}</p>
        )}

        {result && !deepMut.isPending && (
          <p className="mt-2.5 text-[12.5px] leading-relaxed text-navy-900 bg-gold-200/90 rounded-lg px-3 py-2">{result}</p>
        )}

        {liResult && !liMut.isPending && (
          <p className="mt-2 text-[12px] leading-relaxed text-white/90 bg-white/10 rounded-lg px-3 py-2 break-all">{liResult}</p>
        )}

        {/* where-now result + apply [patch_frontend_ce_contactspage] */}
        {(ceMut.isPending || ceMsg) && (
          <div className="mt-2 rounded-lg bg-white/10 ring-1 ring-white/15 px-3 py-2 text-[12px] text-white/90">
            {ceMut.isPending ? 'Checking where they work now...' : (
              <div className="flex items-center justify-between gap-2">
                <span className="break-words">{ceMsg}</span>
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

        {/* who-is-there-now result + file [patch_frontend_find_successor] */}
        {(succMut.isPending || succMsg) && (
          <div className="mt-2 rounded-lg bg-white/10 ring-1 ring-white/15 px-3 py-2 text-[12px] text-white/90">
            {succMut.isPending ? 'Working on the successor...' : (
              <div className="flex items-center justify-between gap-2">
                <span className="break-words">{succMsg}</span>
                {succFound && (
                  <button onClick={() => runFindSuccessor(true)}
                    className="h-6 px-2 rounded-md text-[11px] font-bold text-navy-900 bg-gold-300 hover:bg-gold-200 shrink-0">File it</button>
                )}
              </div>
            )}
          </div>
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

function InfoRow({ icon, label, value, mono, href, editField, contactId, leadId, placeholder }: {
  icon: React.ReactNode; label: string; value: string | null | undefined; mono?: boolean; href?: string
  editField?: string  // contacts column to edit; '__name__' edits first+last+display together
  contactId?: number  // inbox-contact id -> edits via /api/inbox-contacts
  leadId?: number     // patch_frontend_leadcontact_edit: real lead_contact id -> edits via /api/lead-contacts
  placeholder?: string
}) {
  const updMut = useUpdateInboxContact()
  const leadMut = useUpdateLeadContact()
  const [editing, setEditing] = useState(false)
  const [draft, setDraft] = useState('')
  const [copied, setCopied] = useState(false)
  const canEdit = !!editField && (!!contactId || !!leadId)
  // editable rows render even when empty (so you can ADD a value); read-only
  // rows keep the old hide-when-empty behaviour.
  if (!value && !canEdit) return null

  const linkVal = href && value === 'View profile' ? href : (value || '')

  async function doCopy(e: React.MouseEvent) {
    e.stopPropagation()
    try {
      await navigator.clipboard.writeText(linkVal)
      setCopied(true)
      setTimeout(() => setCopied(false), 1200)
    } catch { /* clipboard unavailable */ }
  }
  function startEdit(e: React.MouseEvent) {
    e.stopPropagation()
    setDraft(linkVal)
    setEditing(true)
  }
  async function save() {
    setEditing(false)
    const newVal = draft.trim()
    if (newVal === (linkVal || '')) return
    // patch_frontend_leadcontact_edit: lead-gen rows -> lead endpoint, mapping to the lead schema
    if (leadId) {
      const lf: any = editField === '__name__' ? { name: newVal }
        : editField === 'linkedin_url' ? { linkedin: newVal }
        : { [editField!]: newVal }
      try { await leadMut.mutateAsync({ realId: leadId, fields: lf }) }
      catch { /* surfaced by interceptor; reverts on refetch */ }
      return
    }
    const fields = editField === '__name__'
      ? (() => { const p = newVal.split(/\s+/); return { first_name: p[0] || '', last_name: p.slice(1).join(' ') || '', display_name: newVal } })()
      : { [editField!]: newVal }
    try { await updMut.mutateAsync({ id: contactId!, fields: fields as any }) }
    catch { /* error surfaced by the api interceptor; field reverts on refetch */ }
  }

  return (
    <div className="group flex items-start gap-2.5 py-1.5">
      <span className="text-stone-400 mt-0.5 flex-shrink-0">{icon}</span>
      <div className="min-w-0 flex-1">
        <div className="text-[10px] uppercase tracking-wide font-bold text-stone-400">{label}</div>
        {editing ? (
          <input autoFocus value={draft} onChange={(e) => setDraft(e.target.value)}
            onBlur={save}
            onKeyDown={(e) => { if (e.key === 'Enter') save(); if (e.key === 'Escape') setEditing(false) }}
            onClick={(e) => e.stopPropagation()}
            placeholder={placeholder || label}
            className={cn('w-full text-[13px] font-medium text-navy-800 bg-amber-50 ring-1 ring-amber-300 rounded px-1.5 py-0.5 outline-none', mono && 'font-mono text-xs')} />
        ) : href && value ? (
          <a href={href} target="_blank" rel="noopener noreferrer" className="text-[13px] font-medium text-navy-600 hover:underline truncate block">
            {value} <ExternalLink className="inline w-3 h-3 ml-0.5 -mt-0.5" />
          </a>
        ) : (
          <div className={cn('text-[13px] font-medium truncate', value ? 'text-navy-800' : 'text-stone-300 italic', mono && value && 'font-mono text-xs')}>
            {value || (canEdit ? `Add ${label.toLowerCase()}...` : '')}
          </div>
        )}
      </div>
      <div className="flex items-center gap-0.5 opacity-0 group-hover:opacity-100 transition-opacity flex-shrink-0">
        {value && (
          <button onClick={doCopy} title={copied ? 'Copied!' : 'Copy'}
            className="w-6 h-6 rounded hover:bg-stone-100 flex items-center justify-center text-stone-400 hover:text-stone-600">
            {copied ? <Check className="w-3 h-3 text-emerald-500" /> : <Copy className="w-3 h-3" />}
          </button>
        )}
        {canEdit && !editing && (
          <button onClick={startEdit} title="Edit"
            className="w-6 h-6 rounded hover:bg-stone-100 flex items-center justify-center text-stone-400 hover:text-stone-600">
            <Pencil className="w-3 h-3" />
          </button>
        )}
      </div>
    </div>
  )
}

function Timeline({ contact }: { contact: UnifiedContact }) {
  const isLead = sourceOf(contact) === 'lead_generator'
  // Real communication dates (from the email thread), distinct from sync time.
  // last_inbound = the last time THEY wrote (real two-way contact); last_outbound
  // = the last time WE wrote. When outbound is newer than inbound, we reached out
  // and they went quiet -- shown as fact, the rep judges what it means.
  const lastIn = contact.last_inbound_at || null
  const lastOut = contact.last_outbound_at || null
  const firstMsg = contact.first_message_at || null
  const wentQuiet = !!(lastOut && lastIn && new Date(lastOut) > new Date(lastIn))
  const events = [
    isLead
      ? { t: contact.first_seen, label: 'Discovered via Lead Generator', color: '#7c3aed' }
      : lastIn
        ? { t: lastIn, label: 'They last replied', color: '#1a7a55' }
        : { t: contact.last_seen, label: 'Last synced', color: '#b0a99e' },
    !isLead && lastOut ? { t: lastOut, label: 'We last emailed them', color: '#2e4a6e' } : null,
    !isLead && firstMsg ? { t: firstMsg, label: 'First contact', color: '#b0a99e' } : null,
    isHighOpportunity(contact) ? { t: contact.last_seen, label: 'Flagged high-opportunity by AI', color: '#c49a3c' } : null,
  ].filter(Boolean) as Array<{ t: string | null; label: string; color: string }>
  const recent = (contact.sync_history || []).slice(-3).reverse()

  return (
    <SectionCard title="Engagement" icon={<Activity className="w-3.5 h-3.5" />}>
      {wentQuiet && (
        <div className="mb-3 px-2.5 py-1.5 rounded-lg bg-amber-50 ring-1 ring-amber-200 text-[12px] text-amber-800 leading-snug">
          We emailed them {relativeDate(lastOut)}, but their last reply was {relativeDate(lastIn)}.
        </div>
      )}
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
              <div className="text-2xl font-bold tabular-nums leading-none" style={{ color: contact.opportunity_score >= 75 ? '#e85d4a' : '#c49a3c' }}>{Math.round(contact.opportunity_score)}</div>
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

function ProfilePanel({ contact, onDeleted }: { contact: UnifiedContact | null; onDeleted: () => void }) {
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
  const isLead = sourceOf(contact) === 'lead_generator'

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
              {contact.target_match && (
                <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[10px] font-bold bg-emerald-500/20 text-emerald-200 ring-1 ring-emerald-300/30"
                  title="The lead generator independently identified this person as a target — and you already email them">
                  <Target className="w-2.5 h-2.5" /> VERIFIED TARGET{contact.target_priority ? ` · ${contact.target_priority}` : ''}
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
                : <span className="inline-flex items-center gap-1"><Mail className="w-3 h-3" />{contact.last_inbound_at ? <>Last reply · {relativeDate(contact.last_inbound_at)}</> : <>In inbox · {relativeDate(contact.last_seen)}</>}</span>}
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
          {isLead ? (
            /* Lead-gen contacts live in the lead pipeline — approve/reject
               here would hit the inbox-contacts table with a wrong ID. */
            <div className="flex items-center gap-2">
              {!contact.email && <LeadFindEmailBtn contact={contact} />}
              <span className="inline-flex items-center gap-1.5 h-9 px-3.5 rounded-lg text-[12px] font-semibold bg-white/10 text-white/70 ring-1 ring-white/15">
                <Radar className="w-3.5 h-3.5" /> Lead Generator contact
              </span>
            </div>
          ) : (
            <>
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
            </>
          )}
        </div>
      </div>

      {/* body */}
      <div className="px-7 py-6 space-y-4">
        <AIBand contact={contact} />

        <div className="grid gap-4 items-start" style={{ gridTemplateColumns: 'repeat(auto-fit, minmax(300px, 1fr))' }}>
          {contact.buying_signal_label && (
            <SectionCard title="Buying signal" icon={<Target className="w-3.5 h-3.5" />}>
              <div className="flex items-center gap-2 mb-2 flex-wrap">
                {(() => {
                  const lbl = contact.buying_signal_label
                  const map: Record<string, { t: string; cls: string }> = {
                    buyer_evidence: { t: 'Buyer', cls: 'bg-emerald-50 text-emerald-700 ring-emerald-200' },
                    active_contact: { t: 'Engaged', cls: 'bg-amber-50 text-amber-700 ring-amber-200' },
                    contact: { t: 'Contact', cls: 'bg-stone-50 text-stone-600 ring-stone-200' },
                    vendor_or_noise: { t: 'Vendor / noise', cls: 'bg-stone-100 text-stone-500 ring-stone-200' },
                    internal: { t: 'Internal', cls: 'bg-stone-100 text-stone-500 ring-stone-200' },
                  }
                  const m = map[lbl] || { t: lbl, cls: 'bg-stone-50 text-stone-600 ring-stone-200' }
                  return <span className={cn('inline-flex items-center px-2 py-0.5 rounded-full text-[11px] font-bold ring-1', m.cls)}>{m.t}</span>
                })()}
                {contact.buying_signal_stage && contact.buying_signal_stage !== 'noise' && contact.buying_signal_stage !== 'internal' && (
                  <span className="inline-flex items-center px-2 py-0.5 rounded-full text-[11px] font-medium bg-navy-50 text-navy-700 ring-1 ring-navy-100">{contact.buying_signal_stage}</span>
                )}
                {contact.buying_signal_deal && (
                  <span className="inline-flex items-center gap-1 text-[11px] text-stone-500"><Package className="w-3 h-3" />{contact.buying_signal_deal}</span>
                )}
              </div>
              {contact.buying_signal_products && (
                <div className="flex items-center gap-1.5 text-[12.5px] text-navy-800 font-medium mb-2">
                  <Package className="w-3.5 h-3.5 text-stone-400" />
                  <span className="text-stone-400 font-normal">Interested in:</span> {contact.buying_signal_products}
                </div>
              )}
              {contact.buying_signal_reason && (
                <div className="text-[12px] text-stone-500 leading-snug mb-2">{contact.buying_signal_reason.split('  |  ')[0]}</div>
              )}
              {contact.buying_signal_team && contact.buying_signal_team.length > 0 && (
                <div className="pt-2 border-t border-stone-100">
                  <div className="text-[10px] uppercase tracking-wide font-bold text-stone-400 mb-1.5">Others in the thread</div>
                  <div className="space-y-1">
                    {contact.buying_signal_team.slice(0, 6).map((p, i) => (
                      <div key={i} className="flex items-center gap-2 text-[12px]">
                        <Users className="w-3 h-3 text-stone-300" />
                        <span className="font-medium text-navy-800">{p.name || p.email}</span>
                        {p.org && <span className="text-stone-400">· {p.org}</span>}
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </SectionCard>
          )}

          <SectionCard title="Contact" icon={<Users className="w-3.5 h-3.5" />}>
            <InfoRow icon={<Users className="w-3.5 h-3.5" />} label="Name" value={fullName(contact)}
              editField="__name__" contactId={sourceOf(contact) !== 'lead_generator' ? contact.id : undefined}
              leadId={sourceOf(contact) === 'lead_generator' || contact.id >= LEAD_ID_OFFSET ? (contact.id >= LEAD_ID_OFFSET ? contact.id - LEAD_ID_OFFSET : contact.id) : undefined} />
            <InfoRow icon={<Mail className="w-3.5 h-3.5" />} label={contact.secondary_email ? 'Email (former)' : 'Email'} value={contact.email} mono
              editField="email" contactId={sourceOf(contact) !== 'lead_generator' ? contact.id : undefined}
              leadId={sourceOf(contact) === 'lead_generator' || contact.id >= LEAD_ID_OFFSET ? (contact.id >= LEAD_ID_OFFSET ? contact.id - LEAD_ID_OFFSET : contact.id) : undefined} />
            {contact.secondary_email && (
              <>
                <div className="px-1 -mt-1 mb-1 text-[11px] text-amber-700">From a former employer &mdash; may be inactive. Current address found below.</div>
                <InfoRow icon={<Mail className="w-3.5 h-3.5" />} label="Email (current, found)" value={contact.secondary_email} mono href={`mailto:${contact.secondary_email}`} />
              </>
            )}
            <InfoRow icon={<Phone className="w-3.5 h-3.5" />} label="Phone" value={contact.phone}
              editField="phone" contactId={sourceOf(contact) !== 'lead_generator' ? contact.id : undefined}
              leadId={sourceOf(contact) === 'lead_generator' || contact.id >= LEAD_ID_OFFSET ? (contact.id >= LEAD_ID_OFFSET ? contact.id - LEAD_ID_OFFSET : contact.id) : undefined} />
            <InfoRow icon={<MapPin className="w-3.5 h-3.5" />} label="Address" value={contact.address} />
            <InfoRow icon={<Linkedin className="w-3.5 h-3.5" />} label="LinkedIn" value={contact.linkedin_url ? 'View profile' : null} href={contact.linkedin_url || undefined}
              editField="linkedin_url" contactId={sourceOf(contact) !== 'lead_generator' ? contact.id : undefined}
              leadId={sourceOf(contact) === 'lead_generator' || contact.id >= LEAD_ID_OFFSET ? (contact.id >= LEAD_ID_OFFSET ? contact.id - LEAD_ID_OFFSET : contact.id) : undefined} placeholder="https://www.linkedin.com/in/..." />
            <InfoRow icon={<Hash className="w-3.5 h-3.5" />} label="Department" value={contact.department} />
            <InfoRow icon={<Eye className="w-3.5 h-3.5" />} label="Seniority" value={contact.seniority} />
          </SectionCard>

          <SectionCard title="Hospitality intel" icon={<Building2 className="w-3.5 h-3.5" />}>
            <InfoRow icon={<Building2 className="w-3.5 h-3.5" />} label="Organization" value={contact.organization}
              editField="organization" contactId={sourceOf(contact) !== 'lead_generator' ? contact.id : undefined}
              leadId={sourceOf(contact) === 'lead_generator' || contact.id >= LEAD_ID_OFFSET ? (contact.id >= LEAD_ID_OFFSET ? contact.id - LEAD_ID_OFFSET : contact.id) : undefined} />
            <InfoRow icon={<Shield className="w-3.5 h-3.5" />} label="Parent company" value={contact.parent_company} />
            <InfoRow icon={<Sparkles className="w-3.5 h-3.5" />} label="Brand tier" value={contact.brand_tier ? getTierLabel(contact.brand_tier) : null} />
            <InfoRow icon={<Hash className="w-3.5 h-3.5" />} label="Management co." value={contact.management_company} />
            <InfoRow icon={<Shield className="w-3.5 h-3.5" />} label="GPO" value={contact.gpo && !/avendra/i.test(contact.gpo) ? contact.gpo : null} />
            <InfoRow icon={<Activity className="w-3.5 h-3.5" />} label="Opportunity"
              value={contact.opportunity_level ? `${contact.opportunity_level}${contact.opportunity_score != null ? ` · ${Math.round(contact.opportunity_score)}/100` : ''}` : null} />
            <InfoRow icon={<ExternalLink className="w-3.5 h-3.5" />} label="Matched lead" value={contact.matched_lead_id ? `#${contact.matched_lead_id}` : null} />
            <InfoRow icon={<ExternalLink className="w-3.5 h-3.5" />} label="Matched hotel" value={contact.matched_hotel_id ? `#${contact.matched_hotel_id}` : null} />
          </SectionCard>

          <SectionCard title="Employer & coverage" icon={<Briefcase className="w-3.5 h-3.5" />}>
            <AffiliationCard
              personType={sourceOf(contact) === 'lead_generator' || contact.id >= LEAD_ID_OFFSET ? 'lead_contact' : 'contact'}
              personId={contact.id >= LEAD_ID_OFFSET ? contact.id - LEAD_ID_OFFSET : contact.id}
            />
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
   ACCOUNT PANEL — the triangulation view for one account
   ════════════════════════════════════════════════════════════════════ */

function AccountPanel({ org, intel, onOpenContact }: {
  org: string
  intel: AccountIntel | undefined
  onOpenContact: (id: number) => void
}) {
  if (!intel) {
    return (
      <div className="flex-1 flex flex-col items-center justify-center text-stone-400 bg-stone-100/40">
        <Building2 className="w-11 h-11 text-stone-300 mb-3" />
        <p className="text-sm font-semibold text-stone-500">No intelligence for this account yet</p>
      </div>
    )
  }
  const sample = intel.known[0] || intel.gaps[0]
  const isMgmt = sample ? accountTypeOf(sample) === 'management_company' : false
  const tier = [...intel.known, ...intel.gaps].find((m) => m.brand_tier)?.brand_tier
  const stage = sample ? stageOf(sample) : 'potential'

  return (
    <div key={org} className="flex-1 overflow-y-auto bg-stone-100/50">
      {/* hero */}
      <div className="relative px-7 pt-7 pb-6 text-white overflow-hidden" style={{ background: 'linear-gradient(120deg,#0a1628 0%,#152844 60%,#1f3a5c 100%)' }}>
        <div className="pointer-events-none absolute inset-0 opacity-60" style={{ background: 'radial-gradient(900px 300px at 90% -20%, rgba(212,168,83,.18), transparent 60%)' }} />
        <div className="relative flex items-start gap-4">
          <span className="w-14 h-14 rounded-2xl flex items-center justify-center flex-shrink-0 bg-white/10 ring-1 ring-white/20">
            {isMgmt ? <Briefcase className="w-6 h-6 text-violet-300" /> : <Building2 className="w-6 h-6 text-gold-300" />}
          </span>
          <div className="min-w-0 flex-1">
            <h2 className="text-[22px] font-bold leading-tight">{org}</h2>
            <div className="flex items-center gap-2 mt-1.5 flex-wrap text-[12px] text-white/60">
              {intel.vendor ? <span>Vendor / supplier</span> : isMgmt ? <span>Management company</span> : <span>Hotel account</span>}
              {tier && <span>· {getTierLabel(tier)}</span>}
              {!intel.vendor && <span>· {stage === 'existing' ? 'Existing customer' : 'Potential'}</span>}
              {intel.oppScore != null && !intel.vendor && <span>· Opp {Math.round(intel.oppScore)}/100</span>}
            </div>
            <div className="flex items-center gap-2 mt-3 flex-wrap">
              {!intel.vendor && (
                <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[10px] font-bold ring-1 ring-white/20" style={{ background: 'rgba(255,255,255,.10)', color: WARMTH_COLOR[intel.level] }}>
                  <Flame className="w-3 h-3" /> {intel.level.toUpperCase()} · {intel.warmth}
                </span>
              )}
              {intel.matches.length > 0 && (
                <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[10px] font-bold bg-emerald-500/20 text-emerald-200 ring-1 ring-emerald-300/30">
                  {intel.matches.length} VERIFIED DM{intel.matches.length > 1 ? 'S' : ''}
                </span>
              )}
              {intel.gaps.length > 0 && (
                <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[10px] font-bold bg-coral-500/20 text-coral-200 ring-1 ring-coral-400/30">
                  <Target className="w-3 h-3" /> {intel.gaps.length} GAP{intel.gaps.length > 1 ? 'S' : ''}
                </span>
              )}
            </div>
          </div>
        </div>
      </div>

      <div className="px-7 py-6 space-y-4">
        {/* suggested play */}
        {intel.play && (
          <div className="relative overflow-hidden rounded-2xl text-white shadow-lift" style={{ background: 'linear-gradient(135deg,#0f1d32 0%,#1a2d4a 55%,#253d5e 100%)' }}>
            <div className="pointer-events-none absolute -top-16 -right-10 w-56 h-56 rounded-full" style={{ background: 'radial-gradient(circle,rgba(212,168,83,.28),transparent 65%)' }} />
            <div className="relative px-5 py-4">
              <div className="flex items-center gap-2 mb-2">
                <span className="inline-flex items-center justify-center w-6 h-6 rounded-lg bg-gold-400/20 ring-1 ring-gold-300/40"><Sparkles className="w-3.5 h-3.5 text-gold-300" /></span>
                <h3 className="text-xs font-bold uppercase tracking-wider text-gold-200">Suggested play</h3>
              </div>
              <p className="text-[13.5px] leading-relaxed text-white/90">{intel.play}</p>
            </div>
          </div>
        )}

        {/* numbers strip */}
        <div className="bg-white rounded-2xl ring-1 ring-stone-200/80 shadow-card px-5 py-4 flex items-center gap-5 flex-wrap">
          <div><div className="text-2xl font-bold text-navy-900 tabular-nums leading-none">{intel.known.length}</div><div className="text-[10px] uppercase tracking-wide font-bold text-stone-400 mt-1">People known</div></div>
          <div className="w-px h-9 bg-stone-200" />
          <div><div className="text-2xl font-bold text-navy-900 tabular-nums leading-none">{intel.emails.toLocaleString()}</div><div className="text-[10px] uppercase tracking-wide font-bold text-stone-400 mt-1">Emails</div></div>
          <div className="w-px h-9 bg-stone-200" />
          <div><div className="text-2xl font-bold tabular-nums leading-none text-emerald-600">{intel.matches.length}</div><div className="text-[10px] uppercase tracking-wide font-bold text-stone-400 mt-1">Verified DMs</div></div>
          <div className="w-px h-9 bg-stone-200" />
          <div><div className="text-2xl font-bold tabular-nums leading-none text-coral-500">{intel.gaps.length}</div><div className="text-[10px] uppercase tracking-wide font-bold text-stone-400 mt-1">DM gaps</div></div>
          {intel.lastTouch && (<><div className="w-px h-9 bg-stone-200" /><div><div className="text-2xl font-bold text-navy-900 leading-none">{relativeDate(intel.lastTouch)}</div><div className="text-[10px] uppercase tracking-wide font-bold text-stone-400 mt-1">Last touch</div></div></>)}
        </div>

        {/* warm paths */}
        <SectionCard title={`Warm paths — who you already know (${intel.known.length})`} icon={<Flame className="w-3.5 h-3.5" />}>
          {intel.known.length === 0 ? (
            <p className="text-[13px] text-stone-400 py-1">No email relationships at this account yet — it's a cold account.</p>
          ) : (
            <div className="-mx-2">
              {intel.known.slice(0, 8).map((c) => (
                <div key={c.id} onClick={() => onOpenContact(c.id)}
                  className="flex items-center gap-3 px-2 py-2 rounded-xl cursor-pointer hover:bg-stone-50 transition-colors">
                  <Avatar contact={c} size={32} />
                  <div className="min-w-0 flex-1">
                    <div className="flex items-center gap-1.5">
                      <span className="truncate text-[13px] font-semibold text-navy-900">{fullName(c)}</span>
                      {c.target_match && <span className="text-[9px] font-bold text-emerald-700 bg-emerald-50 px-1.5 py-0.5 rounded-full flex-shrink-0" title="Lead-generator target you already email">VERIFIED{c.target_priority ? ` · ${c.target_priority}` : ''}</span>}
                    </div>
                    <div className="truncate text-[12px] text-stone-500">{roleText(c) || 'role unknown'}</div>
                  </div>
                  <div className="text-right flex-shrink-0">
                    <div className="text-[12px] font-semibold text-navy-700 tabular-nums">{c.interaction_count} emails</div>
                    <div className="text-[11px] text-stone-400">{relativeDate(c.last_seen)}</div>
                  </div>
                  <ChevronRight className="w-4 h-4 text-stone-300 flex-shrink-0" />
                </div>
              ))}
              {intel.known.length > 8 && <div className="px-2 pt-1 text-[11px] text-stone-400">+{intel.known.length - 8} more in the directory</div>}
            </div>
          )}
        </SectionCard>

        {/* gaps */}
        <SectionCard title={`Decision-maker gaps — no relationship yet (${intel.gaps.length})`} icon={<Target className="w-3.5 h-3.5" />}>
          {intel.gaps.length === 0 ? (
            <p className="text-[13px] text-stone-400 py-1">No open gaps — every decision-maker the lead generator identified here is already in your inbox.</p>
          ) : (
            <div className="-mx-2">
              {intel.gaps.map((c) => (
                <div key={c.id} onClick={() => onOpenContact(c.id)}
                  className="flex items-center gap-3 px-2 py-2 rounded-xl cursor-pointer hover:bg-stone-50 transition-colors">
                  <Avatar contact={c} size={32} />
                  <div className="min-w-0 flex-1">
                    <div className="flex items-center gap-1.5">
                      <span className="truncate text-[13px] font-semibold text-navy-900">{fullName(c)}</span>
                      {c.procurement_priority && c.procurement_priority !== 'unknown' && (
                        <span className="text-[9px] font-bold text-gold-700 bg-gold-50 px-1.5 py-0.5 rounded-full flex-shrink-0">{c.procurement_priority}</span>
                      )}
                    </div>
                    <div className="truncate text-[12px] text-stone-500">{roleText(c) || 'decision-maker'}</div>
                  </div>
                  {c.linkedin_url && (
                    <button onClick={(e) => { e.stopPropagation(); window.open(c.linkedin_url!, '_blank') }} title="LinkedIn outreach"
                      className="w-8 h-8 rounded-lg bg-navy-50 hover:bg-navy-100 flex items-center justify-center text-navy-600 flex-shrink-0">
                      <Linkedin className="w-4 h-4" />
                    </button>
                  )}
                  {c.email && (
                    <a href={`mailto:${c.email}`} onClick={(e) => e.stopPropagation()} title="Email"
                      className="w-8 h-8 rounded-lg bg-stone-100 hover:bg-stone-200 flex items-center justify-center text-stone-500 flex-shrink-0">
                      <Mail className="w-4 h-4" />
                    </a>
                  )}
                  <ChevronRight className="w-4 h-4 text-stone-300 flex-shrink-0" />
                </div>
              ))}
            </div>
          )}
          {intel.otherTargets > 0 && (
            <div className="pt-2 mt-1 border-t border-stone-100 text-[11px] text-stone-400">
              +{intel.otherTargets} more lead-generator contact{intel.otherTargets > 1 ? 's' : ''} here below P1/P2 — see the group list
            </div>
          )}
        </SectionCard>
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
  const vertical = params.get('vertical') || ''
  const priority = params.get('priority') || ''
  const sort = (params.get('sort') as SortKey) || 'confidence'
  const selectedId = params.get('selected') ? Number(params.get('selected')) : null

  // local state
  const [selected, setSelected] = useState<Set<number>>(new Set())
  const [drawerOpen, setDrawerOpen] = useState<boolean>(() => !!params.get('selected'))
  const [focused, setFocused] = useState(false)
  const [view, setView] = useState<'accounts' | 'people'>('accounts')
  const [collapsedOrgs, setCollapsedOrgs] = useState<Set<string>>(new Set())
  // Secondary filters (Source / Account / Stage / Status) live behind a
  // "More filters" toggle (2026-06-11) — keeps the primary row calm at
  // four daily lenses. Auto-opens whenever one of them is active (incl.
  // via back/forward navigation) so an applied filter is never invisible.
  const [moreOpen, setMoreOpen] = useState<boolean>(() => !!(source || account || lifecycle || status))
  useEffect(() => { if (source || account || lifecycle || status) setMoreOpen(true) }, [source, account, lifecycle, status])

  // search box is debounced — typing updates `draft` instantly, the actual
  // filter (URL `q`) follows 180ms after the user pauses. Keeps keystrokes
  // snappy: filtering+sorting ~7k contacts no longer runs on every key.
  const [draft, setDraft] = useState(query)
  const lastPushed = useRef(query)
  useEffect(() => {
    // Only adopt the URL value when it changed from OUTSIDE this component
    // (i.e. it is not the value we just pushed via the debounce) -- otherwise
    // our own echo would reset `draft` mid-keystroke and drop fast typing.
    if (query !== lastPushed.current) {
      lastPushed.current = query
      setDraft(query)
    }
  }, [query])
  useEffect(() => {
    if (draft === query) return
    const t = window.setTimeout(() => {
      lastPushed.current = draft
      patch({ q: draft || null })
    }, 180)
    return () => window.clearTimeout(t)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [draft])

  // render windows — only this many rows/groups hit the DOM; an invisible
  // sentinel at the bottom grows the window as you scroll. This is what
  // fixes the draggy feel: previously ALL ~7k contacts rendered at once.
  const PEOPLE_CHUNK = 150
  const GROUP_CHUNK = 60
  const [visiblePeople, setVisiblePeople] = useState(PEOPLE_CHUNK)
  const [visibleGroups, setVisibleGroups] = useState(GROUP_CHUNK)
  useEffect(() => {
    setVisiblePeople(PEOPLE_CHUNK)
    setVisibleGroups(GROUP_CHUNK)
  }, [query, category, status, dmOnly, source, account, lifecycle, vertical, priority, sort, view])

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
  const leadQ = useAllLeadContacts()
  const syncMut = useTriggerInboxSync()
  const bulkApproveMut = useBulkApproveInboxContacts()

  const stats: InboxContactStats | undefined = statsQ.data

  // ╔══════════════════════════════════════════════════════════════════╗
  // ║ MERGE POINT — email-scraped + lead-generator contacts, unified.    ║
  // ║ Lead-gen rows whose email already exists in the inbox don't get    ║
  // ║ dropped — they ANNOTATE the inbox contact (target_match): that     ║
  // ║ overlap is the strongest triangulation signal, a verified          ║
  // ║ decision-maker you already have an email relationship with. The    ║
  // ║ lead copy also fills empty fields (title/phone/linkedin/priority). ║
  // ║ Unmatched lead rows get IDs offset into their own range so they    ║
  // ║ can never collide with inbox IDs in keys/selection/URL.            ║
  // ╚══════════════════════════════════════════════════════════════════╝
  const items = useMemo<UnifiedContact[]>(() => {
    const inboxRaw = (listQ.data?.items || []) as UnifiedContact[]
    const leadRaw = (leadQ.data?.items || []) as UnifiedContact[]
    const leadByEmail = new Map<string, UnifiedContact>()
    for (const lc of leadRaw) {
      const e = (lc.email || '').toLowerCase()
      if (e) leadByEmail.set(e, lc)
    }
    const matchedEmails = new Set<string>()
    const inbox = inboxRaw.map((c) => {
      const e = (c.email || '').toLowerCase()
      const lc = e ? leadByEmail.get(e) : undefined
      if (!lc) return c
      matchedEmails.add(e)
      return {
        ...c,
        target_match: true,
        target_priority: lc.procurement_priority && lc.procurement_priority !== 'unknown' ? lc.procurement_priority : null,
        target_reasoning: lc.priority_reason || lc.background || null,
        is_decision_maker: c.is_decision_maker || lc.is_decision_maker,
        // fill-empty enrichment from the lead generator's research
        title: c.title || lc.title,
        phone: c.phone || lc.phone,
        linkedin_url: c.linkedin_url || lc.linkedin_url,
        background: c.background || lc.background,
        priority_reason: c.priority_reason || lc.priority_reason,
        brand_tier: c.brand_tier || lc.brand_tier,
        management_company: c.management_company || lc.management_company,
        opportunity_score: c.opportunity_score ?? lc.opportunity_score,
        opportunity_level: c.opportunity_level || lc.opportunity_level,
        matched_lead_id: c.matched_lead_id ?? lc.matched_lead_id,
        matched_hotel_id: c.matched_hotel_id ?? lc.matched_hotel_id,
      }
    })
    const leads = leadRaw
      .filter((c) => !c.email || !matchedEmails.has(c.email.toLowerCase()))
      .map((c) => ({ ...c, id: c.id + LEAD_ID_OFFSET }))
    // Canonicalize known rebrands AND spelling/suffix variants so the old/new
    // operator name and "The X" / "X Inc" / "X Hotels" vs "X Hotel" all group
    // under one account header. Pick ONE display name per normalized account
    // key (most common spelling wins), then rewrite every row's org to it.
    // Display/grouping only -- never stored data.
    const _raw = [...inbox, ...leads]
    const _pickDisplay = (field: 'organization' | 'management_company' | 'parent_company') => {
      const byKey = new Map<string, Map<string, number>>()
      for (const c of _raw) {
        const v = c[field]
        if (!v) continue
        const k = accountKey(canonCompany(v) as string)
        if (k === 'No organization') continue
        if (!byKey.has(k)) byKey.set(k, new Map())
        const cnt = byKey.get(k)!
        const disp = canonCompany(v) as string
        cnt.set(disp, (cnt.get(disp) || 0) + 1)
      }
      const chosen = new Map<string, string>()
      for (const [k, cnts] of byKey) {
        const best = [...cnts.entries()].sort((a, b) =>
          b[1] - a[1] || a[0].length - b[0].length || a[0].localeCompare(b[0]))[0][0]
        chosen.set(k, best)
      }
      return (v: string | null | undefined) => {
        if (!v) return v as string | null | undefined
        const k = accountKey(canonCompany(v) as string)
        return chosen.get(k) || (canonCompany(v) as string)
      }
    }
    const _canonOrg = _pickDisplay('organization')
    const _canonMgmt = _pickDisplay('management_company')
    const _canonParent = _pickDisplay('parent_company')
    const merged = _raw.map((c) => ({
      ...c,
      organization: _canonOrg(c.organization),
      management_company: _canonMgmt(c.management_company),
      parent_company: _canonParent(c.parent_company),
    }))
    // Junk costs ZERO everywhere (2026-06-04): excluded from header counts,
    // account groups, warmth math and search. Rows stay in the DB only for
    // reversibility; the Category facet's Junk option flips to an audit view.
    // Shared mailboxes (2026-06-08): operational inboxes (accounting@, ap@,
    // frontdesk@ ...) are account infrastructure, not people — hidden the
    // same way junk is, surfaced only under the "Shared inboxes" category.
    // Buying inboxes (purchasing@/procurement@) are category 'buyer' → stay.
    const effCat = (c: UnifiedContact) => c.manual_category || c.contact_category
    if (category === 'junk') return merged.filter((c) => effCat(c) === 'junk')
    if (category === 'operational') return merged.filter((c) => effCat(c) === 'operational')
    return merged.filter((c) => effCat(c) !== 'junk' && effCat(c) !== 'operational')
  }, [listQ.data, leadQ.data, category])
  const total = items.length
  // Distinct people after entity-resolution de-dup (same person across multiple
  // emails/employers collapses to one). This is the honest "people" headline --
  // `total` above is raw rows. Unfiltered, so the header stays stable as filters
  // change (the list subline uses the filtered dedup count).
  const totalPeople = useMemo(() => collapsePeople(items).length, [items])
  // operational inboxes are excluded from `items` (so they leave the people
  // count, account groups and warmth math) — count them from the raw inbox
  // feed for the header sub-line and the facet badge.
  const sharedInboxCount = useMemo(
    () => ((listQ.data?.items || []) as UnifiedContact[]).filter((c) => c.contact_category === 'operational').length,
    [listQ.data],
  )

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

  // vertical counts for the facet — computed once per data load, over the
  // same junk-free dataset every other count uses
  const vertCounts = useMemo(() => {
    const m: Record<Vertical, number> = { hospitality: 0, parking_valet: 0, education: 0, healthcare: 0, grocery: 0, other: 0 }
    for (const c of items) m[verticalOf(c)]++
    return m
  }, [items])

  // search index — haystacks built once per data load, reused on every keystroke
  const indexed = useMemo(() => items.map((c) => ({ c, hay: buildHaystack(c) })), [items])

  // Fuzzy search index (Fuse.js) -- typo tolerance + relevance ranking for free-
  // text queries. Built once per data load. Fields are WEIGHTED so a name hit
  // ranks above an org hit above a title/email hit. Natural-language shortcut
  // phrases (decision makers / luxury / buyers / ...) bypass this and use the
  // exact predicates in shortcutMatch.
  const fuse = useMemo(() => new Fuse(items, {
    includeScore: true,
    threshold: 0.38,        // 0 = exact, 1 = anything; 0.38 tolerates typos
    ignoreLocation: true,   // match anywhere in the field, not just the start
    minMatchCharLength: 2,
    keys: [
      { name: 'display_name', weight: 0.9 },
      { name: 'first_name', weight: 0.8 },
      { name: 'last_name', weight: 0.8 },
      { name: 'organization', weight: 0.7 },
      { name: 'title', weight: 0.4 },
      { name: 'inferred_role', weight: 0.3 },
      { name: 'email', weight: 0.5 },
      { name: 'management_company', weight: 0.3 },
      { name: 'parent_company', weight: 0.3 },
    ],
  }), [items])

  // ── TRIANGULATION — per-account intelligence over the FULL dataset ──
  // (intentionally unfiltered: an account's warmth/gaps shouldn't change
  // when you narrow the list)
  const accountIntel = useMemo(() => {
    const buckets = new Map<string, UnifiedContact[]>()
    for (const c of items) {
      const k = c.organization || 'No organization'
      if (!buckets.has(k)) buckets.set(k, [])
      buckets.get(k)!.push(c)
    }
    const m = new Map<string, AccountIntel>()
    for (const [org, members] of buckets) {
      const sellers = members.filter((c) => c.contact_category === 'seller').length
      const vendor = members.length > 0 && sellers / members.length > 0.5
      const known = members
        .filter((c) => sourceOf(c) === 'email_scrape' && isRelationshipContact(c))
        .sort((a, b) => (b.interaction_count || 0) - (a.interaction_count || 0))
      const targets = members.filter((c) => sourceOf(c) === 'lead_generator')
      const matches = known.filter((c) => c.target_match)
      const gaps = targets.filter((c) => c.is_decision_maker)
      const emails = known.reduce((s, c) => s + (c.interaction_count || 0), 0)
      const warmth = vendor ? 0 : accountWarmth(known)
      const lastTouch = known.reduce<string | null>(
        (mx, c) => (c.last_seen && (!mx || c.last_seen > mx) ? c.last_seen : mx), null)
      const oppScore = members.reduce<number | null>(
        (mx, c) => (c.opportunity_score != null && (mx == null || c.opportunity_score > mx) ? c.opportunity_score : mx), null)
      const best = matches[0] || known[0]
      const gap = gaps[0]
      let play = ''
      if (vendor) play = 'Supplier to JA — not a sales target. Excluded from warm-path triangulation.'
      else if (best && gap) play = `Warm path: ${fullName(best)} (${roleText(best) || 'your contact'}) → ask for an intro to ${fullName(gap)} (${roleText(gap) || 'decision-maker'}).`
      else if (best && matches.length) play = 'Verified decision-maker relationship — keep it warm.'
      else if (best) play = 'Relationship account — no open decision-maker gaps from the lead generator.'
      else if (gap) play = `Cold account — no email relationship yet. LinkedIn outreach to ${fullName(gap)}${gaps.length > 1 ? ` (+${gaps.length - 1} more)` : ''}.`
      m.set(org, {
        known, matches, gaps,
        otherTargets: targets.length - gaps.length,
        warmth, level: warmthLevel(warmth), emails, lastTouch, oppScore, play, vendor,
      })
    }
    return m
  }, [items])
  const intelOf = (c: UnifiedContact) => accountIntel.get(c.organization || 'No organization')

  // client-side refine + sort
  const filtered = useMemo(() => {
    const t = query.toLowerCase().trim()
    // When the query is free text (not a known shortcut phrase), use fuzzy
    // search: typo-tolerant + ranked by relevance. Shortcut phrases use the
    // exact predicates. `fuzzyRanked` preserves Fuse's relevance order so we
    // can keep it instead of the sort dropdown for text queries.
    let list: UnifiedContact[]
    let fuzzyRanked = false
    if (!t) {
      list = items
    } else if (isShortcutQuery(t)) {
      list = indexed.filter(({ c, hay }) => smartMatch(c, hay, t)).map(({ c }) => c)
    } else {
      list = fuse.search(t).map((r) => r.item)
      fuzzyRanked = true
    }
    if (category === 'uncategorized') list = list.filter((c) => !c.contact_category)
    else if (category && category !== 'junk' && category !== 'operational') list = list.filter((c) => c.contact_category === category)
    if (status) list = list.filter((c) => c.approval_status === status)
    if (dmOnly) list = list.filter((c) => c.is_decision_maker)
    if (source) list = list.filter((c) => sourceOf(c) === source)
    if (account) list = list.filter((c) => accountTypeOf(c) === account)
    if (lifecycle) list = list.filter((c) => stageOf(c) === lifecycle)
    if (vertical) list = list.filter((c) => verticalOf(c) === vertical)
    if (priority) list = list.filter((c) => (priority === 'P_unknown' ? !c.procurement_priority || c.procurement_priority === 'P_unknown' : c.procurement_priority === priority))
    // A free-text fuzzy query is already ranked best-match-first; keep that
    // order (the sort dropdown applies to non-search browsing).
    if (fuzzyRanked) return list
    const sorters: Record<SortKey, (a: UnifiedContact, b: UnifiedContact) => number> = {
      confidence: (a, b) => confidencePct(b) - confidencePct(a),
      opportunity: (a, b) => (b.opportunity_score ?? 0) - (a.opportunity_score ?? 0),
      recent: (a, b) => new Date(b.last_seen || 0).getTime() - new Date(a.last_seen || 0).getTime(),
      newest: (a, b) => new Date(b.first_seen || 0).getTime() - new Date(a.first_seen || 0).getTime(),
      oldest: (a, b) => new Date(a.first_seen || 0).getTime() - new Date(b.first_seen || 0).getTime(),
      name: (a, b) => fullName(a).localeCompare(fullName(b), undefined, { sensitivity: 'base' }),
      org: (a, b) => (a.organization || '~').localeCompare(b.organization || '~') || fullName(a).localeCompare(fullName(b), undefined, { sensitivity: 'base' }),
      warmth: (a, b) => (intelOf(b)?.warmth || 0) - (intelOf(a)?.warmth || 0),
      gaps: (a, b) => (intelOf(b)?.gaps.length || 0) - (intelOf(a)?.gaps.length || 0),
    }
    return [...list].sort(sorters[sort])
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [items, indexed, fuse, accountIntel, query, category, status, dmOnly, source, account, lifecycle, vertical, priority, sort])

  // group filtered contacts by account (hotel OR management company)
  const groups = useMemo(() => {
    const m = new Map<string, UnifiedContact[]>()
    for (const c of filtered) {
      const k = c.organization || 'No organization'
      if (!m.has(k)) m.set(k, [])
      m.get(k)!.push(c)
    }
    // Entity resolution (035): within an account, rows that resolve to the SAME
    // human (shared person_id) collapse to one card — the Account lens. A person
    // who spans two accounts still appears once under each. Rows without a
    // person_id (inbox contacts, unresolved leads) pass through untouched.
    for (const [k, list] of m) m.set(k, collapsePeople(list))
    // keep insertion order so account groups follow the active Sort (e.g. Company A–Z, Newest added)
    let entries = [...m.entries()]
    if (sort === 'warmth') entries = entries.sort((a, b) => (accountIntel.get(b[0])?.warmth || 0) - (accountIntel.get(a[0])?.warmth || 0))
    else if (sort === 'gaps') entries = entries.sort((a, b) => (accountIntel.get(b[0])?.gaps.length || 0) - (accountIntel.get(a[0])?.gaps.length || 0))
    return entries
  }, [filtered, sort, accountIntel])

  // People view (2026-06-11): the flat list collapses rows that resolve to
  // the SAME human (entity resolution, 035) — one row per person, richest
  // representative wins — matching what the account groups already do.
  const peopleList = useMemo(() => collapsePeople(filtered), [filtered])

  // ── Two-level account tree (2026-06-04) ──
  // parent_company (chains) wins over domain family (independents with
  // multiple properties on one email domain). Singletons stay flat.
  // mode with an evidence bar (2026-06-04): a parent claim must appear on
  // >=2 rows, or be a lone vote only in a tiny (<=2 member) group. Without
  // this, one Towne Park employee stationed at a Loews property dragged all
  // 108 Towne Park contacts under the Loews account tree.
  function strongModeOf(values: Array<string | null | undefined>, total: number): string | null {
    const counts = new Map<string, number>()
    for (const v of values) {
      const t = (v || '').trim()
      if (t) counts.set(t, (counts.get(t) || 0) + 1)
    }
    let best: string | null = null
    let n = 0
    for (const [v, c] of counts) if (c > n) { best = v; n = c }
    if (!best) return null
    if (n >= 2) return best
    return total <= 2 ? best : null
  }

  const groupTree = useMemo<TreeSection[]>(() => {
    const pcOf = new Map<string, string>()
    const domOf = new Map<string, string>()
    for (const [org, members] of groups) {
      if (org === 'No organization') continue
      // chains via parent_company; operator relationships via
      // management_company (the River Market GM with a Crestline email
      // nests his property under the Crestline parent)
      const pc = strongModeOf(members.map((m) => m.parent_company), members.length)
        || strongModeOf(members.map((m) => m.management_company), members.length)
      if (pc && pc.trim().toLowerCase() !== org.trim().toLowerCase()) pcOf.set(org, pc)
      const dom = modeOf(members.map((m) => workDomainOf(m.email)))
      if (dom) domOf.set(org, dom)
    }
    const famCount = new Map<string, number>()
    for (const [org] of groups) {
      const pc = pcOf.get(org)
      const k = pc ? 'pc:' + pc.toLowerCase() : domOf.has(org) ? 'dom:' + domOf.get(org) : null
      if (k) famCount.set(k, (famCount.get(k) || 0) + 1)
    }
    const sections: TreeSection[] = []
    const secIdx = new Map<string, number>()
    for (const entry of groups) {
      const [org] = entry
      let key = 'flat:' + org
      let label: string | null = null
      const pc = pcOf.get(org)
      if (pc && (famCount.get('pc:' + pc.toLowerCase()) || 0) >= 2) {
        key = 'pc:' + pc.toLowerCase()
        label = pc
      } else if (!pc && domOf.has(org) && (famCount.get('dom:' + domOf.get(org)) || 0) >= 2) {
        key = 'dom:' + domOf.get(org)
        label = domOf.get(org)!
      }
      let i = secIdx.get(key)
      if (i === undefined) {
        i = sections.length
        secIdx.set(key, i)
        sections.push({ key, label, children: [] })
      }
      sections[i].children.push(entry)
    }
    for (const s of sections) {
      if (s.children.length < 2) { s.label = null; continue }
      if (s.key.startsWith('dom:')) s.label = commonTokenPrefix(s.children.map(([o]) => o)) || s.label
    }
    // absorb an operator's OWN corporate group into its family so
    // "Crestline Hotels & Resorts" sits as the first child under the
    // Crestline parent header instead of floating beside it
    const byLabel = new Map<string, TreeSection>()
    for (const s of sections) if (s.label && s.children.length >= 2) byLabel.set(s.label.trim().toLowerCase(), s)
    const absorbed = new Set<number>()
    sections.forEach((s, i) => {
      if (s.children.length !== 1 || s.label) {
        return
      }
      const org = s.children[0][0]
      const fam = byLabel.get(org.trim().toLowerCase())
      if (fam && fam !== s) {
        fam.children.unshift(s.children[0])
        absorbed.add(i)
      }
    })
    return sections.filter((_, i) => !absorbed.has(i))
  }, [groups])

  const hotelGroups = groups.filter(([, m]) => accountTypeOf(m[0]) === 'hotel').length
  const mgmtGroups = groups.filter(([, m]) => accountTypeOf(m[0]) === 'management_company').length
  const anyFilter = !!source || !!account || !!lifecycle || !!category || dmOnly || !!status || !!vertical || !!priority
  const moreActive = [source, account, lifecycle, status].filter(Boolean).length
  function resetAll() { patch({ source: null, account: null, stage: null, category: null, dm: null, status: null, vertical: null, priority: null }) }

  const activeContact = items.find((c) => c.id === selectedId) || null
  const idx = filtered.findIndex((c) => c.id === selectedId)
  const smartActive = query.trim().length > 0
  const selectMode = selected.size > 0

  // grow the render window when the bottom sentinel scrolls into view
  const moreRef = useRef<HTMLDivElement | null>(null)
  useEffect(() => {
    const el = moreRef.current
    if (!el) return
    const io = new IntersectionObserver(
      (entries) => {
        if (entries[0]?.isIntersecting) {
          if (view === 'people') setVisiblePeople((v) => v + PEOPLE_CHUNK)
          else setVisibleGroups((v) => v + GROUP_CHUNK)
        }
      },
      { rootMargin: '600px' },
    )
    io.observe(el)
    return () => io.disconnect()
    // re-create after each grow so it re-fires if the sentinel is still in view
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [view, filtered.length, groups.length, visiblePeople, visibleGroups])

  // drawer shows EITHER a contact profile OR an account's triangulation panel
  const [accountOrg, setAccountOrg] = useState<string | null>(null)
  // a11y (2026-06-11): remember what had focus before the drawer opened,
  // move focus to its Close button, and give focus back on close — keyboard
  // users were stranded in the list when the drawer slid in.
  const lastFocusRef = useRef<HTMLElement | null>(null)
  const drawerCloseRef = useRef<HTMLButtonElement | null>(null)
  useEffect(() => {
    if (drawerOpen) {
      lastFocusRef.current = document.activeElement as HTMLElement | null
      window.setTimeout(() => drawerCloseRef.current?.focus(), 50)
    } else if (lastFocusRef.current) {
      lastFocusRef.current.focus?.()
      lastFocusRef.current = null
    }
  }, [drawerOpen])
  function openContact(id: number) { setAccountOrg(null); patch({ selected: String(id) }); setDrawerOpen(true) }
  function openAccount(org: string) { patch({ selected: null }); setAccountOrg(org); setDrawerOpen(true) }
  function closeDrawer() { setDrawerOpen(false); setAccountOrg(null) }
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
      else if (accountOrg) return // account mode — no contact stepping
      else if (e.key === 'ArrowDown') { e.preventDefault(); go(1) }
      else if (e.key === 'ArrowUp') { e.preventDefault(); go(-1) }
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [drawerOpen, accountOrg, idx, filtered])

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
    // the target group must be rendered before we can scroll to it
    const gi = groups.findIndex(([o]) => o === org)
    if (gi >= 0) setVisibleGroups((v) => Math.max(v, gi + 3))
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
  function selectAllVisible() { setSelected(new Set(filtered.filter((c) => sourceOf(c) !== 'lead_generator').map((c) => c.id))) }

  return (
    <div className="h-full overflow-hidden bg-stone-50" style={{ display: 'grid', gridTemplateColumns: drawerOpen ? '1fr 600px' : '1fr 0px', transition: 'grid-template-columns .28s cubic-bezier(.16,1,.3,1)' }}>

      {/* ════ MAIN DIRECTORY ════ */}
      <div className="min-w-0 overflow-hidden flex flex-col h-full relative">

        {/* header — calm: title + one-line context + Sync */}
        <div className="flex-shrink-0 px-8 pt-6 pb-4 flex items-end justify-between">
          <div>
            <h1 className="text-2xl font-bold text-navy-900 leading-none tracking-tight">Contacts</h1>
            <p className="text-[13px] text-stone-500 mt-2">
              <span className="text-stone-600 font-semibold tabular-nums">{(totalPeople || total || stats?.total || 0).toLocaleString()}</span> people across{' '}
              <span className="text-stone-600 font-semibold tabular-nums">{scope.hotelAccounts} hotels</span> and{' '}
              <span className="text-stone-600 font-semibold tabular-nums">{scope.mgmtAccounts} management companies</span>
              {sharedInboxCount > 0 && category !== 'operational' && (
                <>{'  ·  '}<span className="text-stone-600 font-semibold tabular-nums">{sharedInboxCount.toLocaleString()}</span> shared inboxes</>
              )}
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
              <input value={draft} onChange={(e) => setDraft(e.target.value)}
                onFocus={() => setFocused(true)} onBlur={() => setFocused(false)}
                placeholder='Search anyone — a name, a hotel, a company, or a role like "director of procurement"'
                className="flex-1 bg-transparent outline-none text-[15px] text-navy-900 placeholder:text-stone-400" />
              {draft && <button onClick={() => { setDraft(''); patch({ q: null }) }} className="text-stone-400 hover:text-stone-600"><X className="w-[17px] h-[17px]" /></button>}
            </div>
          </div>
        </div>

        {/* ONE filter row — every control is the same quiet facet */}
        <div className="flex-shrink-0 px-8 pb-3 flex items-center gap-1 flex-wrap">
          <Facet label="Category" value={category || 'all'} onChange={(v) => patch({ category: v === 'all' ? null : v, dm: null })} options={[
            { v: 'all', label: 'Any category' },
            { v: 'buyer', label: 'Buyers', dot: '#1a7a55', count: stats?.buyer },
            { v: 'seller', label: 'Sellers', dot: '#c49a3c', count: stats?.seller },
            { v: 'competitor', label: 'Competitors', dot: '#e85d4a', count: stats?.competitor },
            { v: 'personal', label: 'Personal', dot: '#3e638c', count: stats?.personal },
            { v: 'uncategorized', label: 'Uncategorized', dot: '#9ca3af', count: stats?.uncategorized },
            { v: 'operational', label: 'Shared inboxes', dot: '#5b7a9e', count: sharedInboxCount },
            { v: 'junk', label: 'Junk (hidden by default)', dot: '#9ca3af' },
          ]} />
          <Facet icon={Package} label="Vertical" value={vertical || 'all'} onChange={(v) => patch({ vertical: v === 'all' ? null : v })} options={[
            { v: 'all', label: 'All verticals' },
            { v: 'hospitality', label: 'Hospitality', icon: Building2, count: vertCounts.hospitality },
            { v: 'parking_valet', label: 'Parking & valet', count: vertCounts.parking_valet },
            { v: 'education', label: 'Universities & schools', count: vertCounts.education },
            { v: 'healthcare', label: 'Healthcare', count: vertCounts.healthcare },
            { v: 'grocery', label: 'Grocery', count: vertCounts.grocery },
            { v: 'other', label: 'Other / unknown', count: vertCounts.other },
          ]} />
          <Facet label="Priority" value={priority || 'all'} onChange={(v) => patch({ priority: v === 'all' ? null : v })} options={[
            { v: 'all', label: 'Any priority' },
            { v: 'P1', label: 'P1 — Procurement', dot: '#1a7a55', count: stats?.p1 },
            { v: 'P2', label: 'P2 — Operations', dot: '#c49a3c', count: stats?.p2 },
            { v: 'P3', label: 'P3', count: stats?.p3 },
            { v: 'P4', label: 'P4', count: stats?.p4 },
            { v: 'P_unknown', label: 'No priority yet', count: stats?.p_unknown },
          ]} />
          <button onClick={() => patch({ dm: dmOnly ? null : '1' })} aria-pressed={dmOnly}
            className={cn('inline-flex items-center gap-1.5 h-9 px-3 rounded-lg text-[13px] font-medium transition-colors',
              'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-navy-500',
              dmOnly ? 'text-gold-700 bg-gold-50 ring-1 ring-gold-200 font-semibold' : 'text-stone-500 hover:bg-stone-100')}>
            <span aria-hidden="true" className={dmOnly ? 'text-gold-500' : 'text-stone-400'}>★</span> Decision-makers
          </button>
          <button onClick={() => setMoreOpen((o) => !o)} aria-expanded={moreOpen}
            className={cn('inline-flex items-center gap-1.5 h-9 px-3 rounded-lg text-[13px] font-medium transition-colors',
              'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-navy-500',
              moreOpen || moreActive > 0 ? 'text-navy-800 bg-navy-50 ring-1 ring-navy-100 font-semibold' : 'text-stone-500 hover:bg-stone-100')}>
            <ChevronDown aria-hidden="true" className={cn('w-3.5 h-3.5 transition-transform', moreOpen && 'rotate-180')} />
            More filters{moreActive > 0 ? ` · ${moreActive}` : ''}
          </button>
          {anyFilter && (
            <button onClick={resetAll} className="inline-flex items-center gap-1 h-9 px-2.5 rounded-lg text-[12px] font-medium text-stone-500 hover:text-coral-500 transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-navy-500">
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
            { v: 'warmth', label: 'Warmest account', icon: Flame },
            { v: 'gaps', label: 'Most DM gaps', icon: Target },
            { v: 'name', label: 'Name A–Z' },
            { v: 'org', label: 'Company A–Z' },
            { v: 'newest', label: 'Newest added' },
            { v: 'oldest', label: 'Oldest added' },
            { v: 'recent', label: 'Last activity' },
            { v: 'opportunity', label: 'Opportunity' },
          ]} />
        </div>

        {/* secondary filters — Source / Account / Stage / Status behind "More filters" */}
        {moreOpen && (
          <div className="flex-shrink-0 px-8 pb-3 -mt-1 flex items-center gap-1 flex-wrap" role="group" aria-label="More filters">
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
            <Facet label="Status" value={status || 'all'} onChange={(v) => patch({ status: v === 'all' ? null : v })} options={[
              { v: 'all', label: 'Any status' },
              { v: 'pending', label: 'Pending' },
              { v: 'approved', label: 'Approved' },
              { v: 'pushed_to_insightly', label: 'In CRM' },
            ]} />
          </div>
        )}

        {/* subtle result line */}
        <div className="flex-shrink-0 px-8 pb-2 text-[12px] text-stone-500">
          {smartActive ? (
            <span className="inline-flex items-center gap-1.5"><Wand2 className="w-3 h-3 text-navy-600" /> Showing <span className="font-semibold text-navy-700">{filtered.length}</span> matches</span>
          ) : (
            <span>{view === 'accounts' ? `${hotelGroups} hotels · ${mgmtGroups} companies · ${peopleList.length} people` : `${peopleList.length} people`}</span>
          )}
          {category === 'uncategorized' && (stats?.uncategorized || 0) > 0 && (
            <span className="ml-3 inline-flex align-middle"><ClassifyNowBtn count={stats?.uncategorized} /></span>
          )}
        </div>

        {/* body */}
        <div className="flex-1 overflow-y-auto px-6 pb-24">
          {listQ.isLoading || leadQ.isLoading ? (
            <div className="space-y-2 px-1 pt-1 max-w-[1320px] mx-auto" role="status" aria-live="polite">
              <div className="flex items-center gap-2 px-1 pb-1 text-[12px] text-stone-500">
                <Loader2 className="w-3.5 h-3.5 animate-spin text-navy-500" aria-hidden="true" />
                Loading {stats?.total ? `${stats.total.toLocaleString()} contacts` : 'contacts'} — large inboxes can take a few seconds
              </div>
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
              {peopleList.slice(0, visiblePeople).map((c) => (
                <DirRow key={c.id} contact={c} selected={c.id === selectedId} selectMode={selectMode} checked={selected.has(c.id)}
                  onOpen={() => openContact(c.id)} onToggleCheck={() => toggleCheck(c.id)} onOpenOrg={openOrg} />
              ))}
              {peopleList.length > visiblePeople && (
                <div ref={moreRef} className="py-4 text-center text-[12px] text-stone-500">
                  Showing {visiblePeople.toLocaleString()} of {peopleList.length.toLocaleString()} — scroll for more
                </div>
              )}
            </div>
          ) : (
            <div className="max-w-[1320px] mx-auto">
              {(() => {
                const out: ReactNode[] = []
                let used = 0
                for (const sec of groupTree) {
                  if (used >= visibleGroups) break
                  const kids = sec.children.slice(0, Math.max(0, visibleGroups - used))
                  used += kids.length
                  const renderGroup = ([org, members]: GroupEntry) => (
                    <DirGroup key={org} org={org} members={members}
                      expanded={smartActive || !collapsedOrgs.has(org)}
                      onToggle={() => toggleOrg(org)}
                      selectedId={selectedId} checked={selected} selectMode={selectMode}
                      onToggleCheck={toggleCheck} onSelect={openContact} onOpenOrg={openOrg}
                      intel={accountIntel.get(org)} onOpenAccount={openAccount} />
                  )
                  if (sec.label && sec.children.length >= 2) {
                    const totalContacts = sec.children.reduce((n, [, m]) => n + m.length, 0)
                    const warmth = sec.children.reduce((n, [o]) => n + (accountIntel.get(o)?.warmth || 0), 0)
                    const gapTotal = sec.children.reduce((n, [o]) => n + (accountIntel.get(o)?.gaps.length || 0), 0)
                    out.push(
                      <div key={'hdr:' + sec.key} className="flex items-center gap-2 mt-5 mb-1 px-2">
                        <Layers className="w-4 h-4 text-navy-500" />
                        <span className="font-semibold text-[14px] text-navy-800">{sec.label}</span>
                        <span className="text-[12px] text-stone-500">
                          {sec.children.length} {sec.children.some(([, m]) => m.some((c) => c.matched_lead_id || c.matched_hotel_id || c.contact_category === 'buyer')) ? 'properties' : 'orgs'} · {totalContacts} contacts
                        </span>
                        {warmth > 0 && (
                          <span className="inline-flex items-center gap-0.5 text-[12px] font-semibold text-orange-600">
                            <Flame className="w-3.5 h-3.5" />{Math.round(warmth)}
                          </span>
                        )}
                        {gapTotal > 0 && (
                          <span className="inline-flex items-center gap-0.5 text-[12px] font-semibold text-rose-600">
                            <Target className="w-3.5 h-3.5" />{gapTotal} gaps
                          </span>
                        )}
                      </div>,
                    )
                    out.push(
                      <div key={'kids:' + sec.key} className="border-l-2 border-stone-200 ml-3 pl-2">
                        {kids.map(renderGroup)}
                      </div>,
                    )
                  } else {
                    for (const entry of kids) out.push(renderGroup(entry))
                  }
                }
                return out
              })()}
              {groups.length > visibleGroups && (
                <div ref={moreRef} className="py-4 text-center text-[12px] text-stone-500">
                  Showing {visibleGroups.toLocaleString()} of {groups.length.toLocaleString()} accounts — scroll for more
                </div>
              )}
            </div>
          )}
        </div>

        {/* bulk action bar */}
        {selectMode && (
          <div className="absolute bottom-4 left-1/2 -translate-x-1/2 px-4 py-2.5 rounded-xl bg-navy-900 text-white shadow-lift flex items-center gap-3 z-10">
            <span className="text-xs font-bold tabular-nums">{selected.size} selected</span>
            <button onClick={selectAllVisible} className="text-[11px] font-semibold text-white/60 hover:text-white">Select all {filtered.filter((c) => sourceOf(c) !== 'lead_generator').length}</button>
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
      <div role="complementary" aria-label="Details panel" aria-hidden={!drawerOpen}
        className={cn('overflow-hidden bg-stone-100', drawerOpen && 'border-l border-stone-200')}>
        <div className="h-full flex flex-col" style={{ width: 600 }}>
          <div className="flex-shrink-0 h-12 px-2.5 bg-white border-b border-stone-200 flex items-center justify-between">
            <div className="flex items-center gap-0.5 min-w-0">
              <button onClick={() => go(-1)} disabled={!!accountOrg || idx <= 0} title="Previous (↑)"
                className="w-8 h-8 rounded-lg hover:bg-stone-100 flex items-center justify-center text-stone-500 disabled:opacity-30">
                <ChevronRight className="w-[18px] h-[18px]" style={{ transform: 'rotate(180deg)' }} />
              </button>
              <button onClick={() => go(1)} disabled={!!accountOrg || idx >= filtered.length - 1} title="Next (↓)"
                className="w-8 h-8 rounded-lg hover:bg-stone-100 flex items-center justify-center text-stone-500 disabled:opacity-30">
                <ChevronRight className="w-[18px] h-[18px]" />
              </button>
              {accountOrg
                ? <span className="text-[11px] text-stone-400 ml-1.5 font-semibold inline-flex items-center gap-1 truncate"><Building2 className="w-3 h-3 flex-shrink-0" /> Account intelligence</span>
                : (idx >= 0 && <span className="text-[11px] text-stone-400 ml-1.5 tabular-nums font-semibold">{idx + 1} of {filtered.length}</span>)}
            </div>
            <button ref={drawerCloseRef} onClick={closeDrawer} aria-label="Close details panel"
              className="inline-flex items-center gap-1.5 h-8 px-3 rounded-lg text-[12px] font-semibold text-stone-500 hover:bg-stone-100 transition focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-navy-500">
              <X className="w-[15px] h-[15px]" /> Close
            </button>
          </div>
          <div className="flex-1 overflow-hidden flex flex-col">
            {drawerOpen && accountOrg
              ? <AccountPanel org={accountOrg} intel={accountIntel.get(accountOrg)} onOpenContact={openContact} />
              : drawerOpen && activeContact
                ? <ProfilePanel contact={activeContact} onDeleted={onDeleted} />
                : null}
          </div>
        </div>
      </div>
    </div>
  )
}
