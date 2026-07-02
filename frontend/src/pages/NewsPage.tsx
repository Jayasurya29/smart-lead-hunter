import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { cn } from '@/lib/utils'
import {
  Newspaper, Loader2, ExternalLink, Handshake, Target, Star, RefreshCw,
  Check, X, UserCheck, CheckCircle2, Search, ClipboardCheck,
} from 'lucide-react'
import { formatDistanceToNow } from 'date-fns'

interface RelationshipHit {
  person?: string
  account?: string
  strength?: string
  detail?: string
}

interface NewsItem {
  id: number
  url: string
  title: string
  snippet: string | null
  source: string | null
  published_hint: string | null
  category: string | null
  region: string | null
  vertical: string | null
  hotel_name: string | null
  brand: string | null
  person_name: string | null
  person_title: string | null
  luxury: boolean | null
  in_pipeline: boolean | null
  pipeline_ref: string | null
  relationship_hits: RelationshipHit[] | null
  created_at: string
  lead_queue_id: number | null
  lead_queue_status: string | null      // pending | approved | rejected
  lead_queue_lead_id: number | null
  person_review_id: number | null
  person_review_status: string | null   // pending | actioned | dismissed
  contact_review_id: number | null
  contact_review_status: string | null  // pending | actioned | dismissed
  contact_review_hotel: string | null
}

const CATEGORIES = [
  { key: '', label: 'All' },
  { key: 'appointment', label: 'Appointments' },
  { key: 'opening', label: 'Openings' },
  { key: 'acquisition', label: 'Acquisitions' },
  { key: 'rebrand', label: 'Rebrands' },
  { key: 'renovation', label: 'Renovations' },
  { key: 'management_change', label: 'Mgmt Change' },
  { key: 'industry', label: 'Industry' },
  { key: 'other', label: 'Other' },
]

const REGIONS = [
  { key: '', label: 'All regions' },
  { key: 'usa', label: 'USA' },
  { key: 'caribbean', label: 'Caribbean' },
]

const VERTICALS = [
  { key: '', label: 'All' },
  { key: 'hotel', label: 'Hotels' },
  { key: 'education', label: 'Education' },
  { key: 'healthcare', label: 'Healthcare' },
]

// chip bg/text + left-accent bar per category
const CAT_STYLE: Record<string, { chip: string; accent: string }> = {
  appointment:       { chip: 'bg-violet-100 text-violet-700',   accent: 'border-l-violet-400' },
  opening:           { chip: 'bg-emerald-100 text-emerald-700', accent: 'border-l-emerald-400' },
  acquisition:       { chip: 'bg-amber-100 text-amber-700',     accent: 'border-l-amber-400' },
  rebrand:           { chip: 'bg-sky-100 text-sky-700',         accent: 'border-l-sky-400' },
  renovation:        { chip: 'bg-orange-100 text-orange-700',   accent: 'border-l-orange-400' },
  management_change: { chip: 'bg-fuchsia-100 text-fuchsia-700', accent: 'border-l-fuchsia-400' },
  industry:          { chip: 'bg-slate-200 text-slate-700',     accent: 'border-l-slate-400' },
  other:             { chip: 'bg-stone-100 text-stone-600',     accent: 'border-l-stone-300' },
}

async function newsFetch(qs: string): Promise<NewsItem[]> {
  const res = await fetch(`/api/news${qs}`, { credentials: 'include' })
  if (!res.ok) throw new Error(`News API ${res.status}`)
  return res.json()
}

export default function NewsPage() {
  const [category, setCategory] = useState('')
  const [region, setRegion] = useState('')
  const [vertical, setVertical] = useState('')
  const [relOnly, setRelOnly] = useState(false)
  const [q, setQ] = useState('')
  const [reviewOnly, setReviewOnly] = useState(false)

  const params = new URLSearchParams()
  if (category) params.set('category', category)
  if (region) params.set('region', region)
  if (vertical) params.set('vertical', vertical)
  if (relOnly) params.set('only_relationships', 'true')
  const qs = params.toString() ? `?${params.toString()}` : ''

  const { data, isLoading, isError, refetch, isFetching } = useQuery({
    queryKey: ['news', category, region, vertical, relOnly],
    queryFn: () => newsFetch(qs),
    staleTime: 60_000,
  })

  const items = data ?? []
  const relCount = items.filter(
    (n) => n.relationship_hits && n.relationship_hits.length > 0,
  ).length

  const qc = useQueryClient()
  const inv = () => qc.invalidateQueries({ queryKey: ['news'] })
  const post = async (url: string) => {
    const res = await fetch(url, { method: 'POST', credentials: 'include' })
    if (!res.ok) throw new Error(`Action failed (${res.status})`)
    return res.json()
  }
  const approveLead = useMutation({
    mutationFn: (id: number) => post(`/api/news/lead-queue/${id}/approve`),
    onSuccess: (d) => {
      if (d?.status === 'not_created') alert(`Not added — ${d.reason}`)
      else if (d?.status === 'merged_contact')
        alert(
          d.attached_contact
            ? `Hotel already exists — added ${d.attached_contact} as a contact instead.`
            : 'Hotel already exists — nothing new to add.',
        )
      inv()
    },
  })
  const rejectLead = useMutation({
    mutationFn: (id: number) => post(`/api/news/lead-queue/${id}/reject`),
    onSuccess: inv,
  })
  const actionPerson = useMutation({
    mutationFn: (id: number) => post(`/api/news/person-review/${id}/action`),
    onSuccess: (d) => {
      if (d?.status === 'blocked') alert(d.reason)
      inv()
    },
  })
  const dismissPerson = useMutation({
    mutationFn: (id: number) => post(`/api/news/person-review/${id}/dismiss`),
    onSuccess: inv,
  })
  const addContact = useMutation({
    mutationFn: (id: number) => post(`/api/news/contact-review/${id}/add`),
    onSuccess: (d) => {
      if (d?.status === 'not_found') alert('That contact review is no longer pending.')
      inv()
    },
  })
  const skipContact = useMutation({
    mutationFn: (id: number) => post(`/api/news/contact-review/${id}/skip`),
    onSuccess: inv,
  })
  const m: CardMutations = {
    approveLead, rejectLead, actionPerson, dismissPerson, addContact, skipContact,
  }

  const pendingLeads = items.filter((n) => n.lead_queue_status === 'pending').length
  const pendingMoves = items.filter((n) => n.person_review_status === 'pending').length
  const pendingContacts = items.filter((n) => n.contact_review_status === 'pending').length

  // client-side search + "needs review" filter over the fetched feed
  const needle = q.trim().toLowerCase()
  const visible = items.filter((n) => {
    if (
      reviewOnly &&
      !(
        n.lead_queue_status === 'pending' ||
        n.person_review_status === 'pending' ||
        n.contact_review_status === 'pending'
      )
    )
      return false
    if (needle) {
      const hay = `${n.title} ${n.hotel_name ?? ''} ${n.person_name ?? ''} ${n.brand ?? ''} ${n.source ?? ''}`.toLowerCase()
      if (!hay.includes(needle)) return false
    }
    return true
  })

  return (
    <div className="h-full flex flex-col">
      {/* header */}
      <div className="flex-shrink-0 border-b border-slate-200/70 bg-white/60">
        <div className="max-w-6xl mx-auto w-full px-6 pt-4 pb-3 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="h-10 w-10 rounded-xl bg-navy-900 flex items-center justify-center shadow-sm">
              <Newspaper className="w-5 h-5 text-white" />
            </div>
            <div>
              <h1 className="text-xl font-bold text-navy-900 leading-tight tracking-tight">
                Hospitality News
              </h1>
              <p className="text-xs text-stone-500 mt-0.5">
                {items.length} stor{items.length === 1 ? 'y' : 'ies'}
                {relCount > 0 && (
                  <>
                    {' · '}
                    <span className="text-emerald-600 font-semibold">
                      {relCount} with a known contact
                    </span>
                  </>
                )}
                {pendingLeads > 0 && (
                  <>
                    {' · '}
                    <span className="text-amber-600 font-semibold">
                      {pendingLeads} to approve
                    </span>
                  </>
                )}
                {pendingMoves > 0 && (
                  <>
                    {' · '}
                    <span className="text-navy-600 font-semibold">
                      {pendingMoves} move{pendingMoves > 1 ? 's' : ''} to review
                    </span>
                  </>
                )}
                {pendingContacts > 0 && (
                  <>
                    {' · '}
                    <span className="text-emerald-600 font-semibold">
                      {pendingContacts} contact{pendingContacts > 1 ? 's' : ''} to add
                    </span>
                  </>
                )}
              </p>
            </div>
          </div>
          <button
            onClick={() => refetch()}
            className="h-9 px-3.5 inline-flex items-center gap-1.5 text-xs font-semibold rounded-lg border border-stone-200 bg-white text-stone-600 hover:bg-stone-50 hover:border-stone-300 transition"
          >
            <RefreshCw className={cn('w-3.5 h-3.5', isFetching && 'animate-spin')} />
            Refresh
          </button>
        </div>

        {/* filters */}
        <div className="max-w-6xl mx-auto w-full px-6 pb-3 flex flex-wrap items-center gap-2">
          <div className="relative w-full mb-1">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-stone-400" />
            <input
              value={q}
              onChange={(e) => setQ(e.target.value)}
              placeholder="Search stories — hotel, person, source…"
              className="w-full h-9 pl-9 pr-8 text-sm rounded-lg border border-stone-200 bg-white text-navy-900 placeholder:text-stone-400 focus:outline-none focus:ring-2 focus:ring-navy-500/30 focus:border-navy-300"
            />
            {q && (
              <button
                onClick={() => setQ('')}
                aria-label="Clear search"
                className="absolute right-2.5 top-1/2 -translate-y-1/2 text-stone-400 hover:text-stone-600"
              >
                <X className="w-4 h-4" />
              </button>
            )}
          </div>
          {CATEGORIES.map((c) => (
            <button
              key={c.key}
              onClick={() => setCategory(c.key)}
              className={cn(
                'h-8 px-3 text-xs font-semibold rounded-full border transition',
                category === c.key
                  ? 'bg-navy-900 text-white border-navy-900 shadow-sm'
                  : 'bg-white text-stone-500 border-stone-200 hover:border-stone-300 hover:text-stone-700',
              )}
            >
              {c.label}
            </button>
          ))}

          <div className="w-px h-6 bg-stone-200 mx-1" />

          <div className="inline-flex rounded-full border border-stone-200 bg-white p-0.5">
            {REGIONS.map((r) => (
              <button
                key={r.key}
                onClick={() => setRegion(r.key)}
                className={cn(
                  'px-3 h-7 text-xs font-semibold rounded-full transition',
                  region === r.key
                    ? 'bg-navy-900 text-white'
                    : 'text-stone-500 hover:text-stone-700',
                )}
              >
                {r.label}
              </button>
            ))}
          </div>

          <div className="inline-flex rounded-full border border-stone-200 bg-white p-0.5">
            {VERTICALS.map((v) => (
              <button
                key={v.key}
                onClick={() => setVertical(v.key)}
                className={cn(
                  'px-3 h-7 text-xs font-semibold rounded-full transition',
                  vertical === v.key
                    ? 'bg-navy-900 text-white'
                    : 'text-stone-500 hover:text-stone-700',
                )}
              >
                {v.label}
              </button>
            ))}
          </div>

          <button
            onClick={() => setRelOnly((v) => !v)}
            className={cn(
              'h-8 px-3.5 inline-flex items-center gap-1.5 text-xs font-semibold rounded-full border transition',
              relOnly
                ? 'bg-emerald-600 text-white border-emerald-600 shadow-sm'
                : 'bg-white text-emerald-700 border-emerald-200 hover:bg-emerald-50',
            )}
          >
            <Handshake className="w-3.5 h-3.5" />
            Relationships only
          </button>

          <button
            onClick={() => setReviewOnly((v) => !v)}
            className={cn(
              'h-8 px-3.5 inline-flex items-center gap-1.5 text-xs font-semibold rounded-full border transition',
              reviewOnly
                ? 'bg-amber-500 text-white border-amber-500 shadow-sm'
                : 'bg-white text-amber-700 border-amber-200 hover:bg-amber-50',
            )}
          >
            <ClipboardCheck className="w-3.5 h-3.5" />
            Needs review
            {pendingLeads + pendingMoves + pendingContacts > 0 && (
              <span
                className={cn(
                  'ml-0.5 px-1.5 rounded-full text-[10px] font-bold',
                  reviewOnly ? 'bg-white/25' : 'bg-amber-100 text-amber-700',
                )}
              >
                {pendingLeads + pendingMoves + pendingContacts}
              </span>
            )}
          </button>
        </div>
      </div>

      {/* feed */}
      <div className="flex-1 overflow-y-auto">
        <div className="max-w-6xl mx-auto w-full px-6 py-5">
          {isLoading ? (
            <div className="h-60 flex items-center justify-center text-stone-400">
              <Loader2 className="w-7 h-7 animate-spin" />
            </div>
          ) : isError ? (
            <div className="h-60 flex items-center justify-center text-sm text-red-500">
              Couldn&apos;t load news. Try Refresh.
            </div>
          ) : visible.length === 0 ? (
            <div className="h-60 flex flex-col items-center justify-center text-stone-400 gap-2">
              <Newspaper className="w-8 h-8" />
              <p className="text-sm">No stories match these filters.</p>
            </div>
          ) : (
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-3.5 items-start">
              {visible.map((n) => (
                <NewsCard key={n.id} n={n} m={m} />
              ))}
            </div>
          )}
        </div>
      </div>
    </div>
  )
}

type Mut = { mutate: (id: number) => void; isPending: boolean; variables?: number }
type CardMutations = {
  approveLead: Mut
  rejectLead: Mut
  actionPerson: Mut
  dismissPerson: Mut
  addContact: Mut
  skipContact: Mut
}

function NewsCard({ n, m }: { n: NewsItem; m: CardMutations }) {
  const cat = CAT_STYLE[n.category ?? 'other'] ?? CAT_STYLE.other
  const hits = n.relationship_hits ?? []
  const hasRel = hits.length > 0
  let when = n.published_hint || ''
  if (!when && n.created_at) {
    try {
      when = `${formatDistanceToNow(new Date(n.created_at))} ago`
    } catch {
      when = ''
    }
  }

  return (
    <div
      className={cn(
        'group bg-white rounded-xl border border-slate-200 border-l-[3px] shadow-sm p-4',
        'hover:shadow-md hover:-translate-y-0.5 transition-all duration-150',
        hasRel ? 'border-l-emerald-500 ring-1 ring-emerald-100' : cat.accent,
      )}
    >
      <div className="flex items-center gap-2 mb-2 flex-wrap">
        <span
          className={cn(
            'inline-flex items-center px-2 py-0.5 rounded-full text-2xs font-bold uppercase tracking-wide',
            cat.chip,
          )}
        >
          {(n.category ?? 'other').replace(/_/g, ' ')}
        </span>
        {n.region && (
          <span className="text-2xs font-bold text-stone-400 uppercase tracking-wide">
            {n.region}
          </span>
        )}
        {n.vertical && n.vertical !== 'hotel' && n.vertical !== 'other' && (
          <span
            className={cn(
              'text-2xs font-bold uppercase tracking-wide',
              n.vertical === 'education' ? 'text-blue-600' : 'text-red-600',
            )}
          >
            {n.vertical}
          </span>
        )}
        {n.luxury && (
          <span className="inline-flex items-center gap-0.5 text-2xs font-bold text-amber-500">
            <Star className="w-3 h-3 fill-amber-400 stroke-amber-400" />
            Luxury
          </span>
        )}
        <span className="ml-auto text-2xs text-stone-400 whitespace-nowrap">
          {n.source}
          {when && ` · ${when}`}
        </span>
      </div>

      <a
        href={n.url}
        target="_blank"
        rel="noreferrer"
        className="group/link block text-[15px] font-bold text-navy-900 hover:text-navy-600 transition leading-snug"
      >
        {n.title}
        <ExternalLink className="inline-block w-3 h-3 ml-1 mb-0.5 opacity-0 group-hover/link:opacity-60 transition" />
      </a>

      {n.snippet && (
        <p className="text-xs text-stone-500 mt-1.5 line-clamp-2 leading-relaxed">
          {n.snippet}
        </p>
      )}

      {(n.hotel_name || n.person_name) && (
        <div className="text-2xs text-stone-500 mt-2">
          {n.person_name && (
            <span className="font-semibold text-stone-700">
              {n.person_name}
              {n.person_title && (
                <span className="font-normal text-stone-500"> — {n.person_title}</span>
              )}
            </span>
          )}
          {n.person_name && n.hotel_name && <span className="text-stone-300"> · </span>}
          {n.hotel_name && (
            <span>
              {n.hotel_name}
              {n.brand && n.brand !== n.hotel_name && (
                <span className="text-stone-400"> ({n.brand})</span>
              )}
            </span>
          )}
        </div>
      )}

      {(hasRel || n.in_pipeline || n.lead_queue_status || n.person_review_status || n.contact_review_status) && (
        <div className="flex flex-wrap items-center gap-1.5 mt-3 pt-2.5 border-t border-slate-100">
          {hasRel && (
            <span className="inline-flex items-center gap-1 px-2 py-1 rounded-md text-2xs font-bold bg-emerald-50 text-emerald-700 ring-1 ring-emerald-200">
              <Handshake className="w-3 h-3" />
              Known: {hits[0].person || 'contact'}
              {hits[0].account ? ` · ${hits[0].account}` : ''}
              {hits.length > 1 && ` +${hits.length - 1}`}
            </span>
          )}
          {n.in_pipeline && (
            <span className="inline-flex items-center gap-1 px-2 py-1 rounded-md text-2xs font-bold bg-navy-50 text-navy-700 ring-1 ring-navy-100">
              <Target className="w-3 h-3" />
              {n.pipeline_ref || 'In pipeline'}
            </span>
          )}

          {/* ── new hotel: approve into the pipeline ── */}
          {n.lead_queue_status === 'approved' && (
            <span className="inline-flex items-center gap-1 px-2 py-1 rounded-md text-2xs font-bold bg-emerald-600 text-white">
              <CheckCircle2 className="w-3 h-3" />
              Added to pipeline{n.lead_queue_lead_id ? ` #${n.lead_queue_lead_id}` : ''}
            </span>
          )}
          {n.lead_queue_status === 'rejected' && (
            <span className="inline-flex items-center px-2 py-1 rounded-md text-2xs font-bold bg-stone-100 text-stone-400 line-through">
              rejected
            </span>
          )}
          {n.lead_queue_status === 'pending' && n.lead_queue_id != null && (
            <>
              <span className="inline-flex items-center gap-1 px-2 py-1 rounded-md text-2xs font-bold bg-amber-50 text-amber-700 ring-1 ring-amber-200">
                <Target className="w-3 h-3" />New hotel — queued
              </span>
              <button
                disabled={m.approveLead.isPending}
                onClick={() => m.approveLead.mutate(n.lead_queue_id!)}
                className="inline-flex items-center gap-1 px-2 py-1 rounded-md text-2xs font-bold bg-emerald-600 text-white hover:bg-emerald-700 disabled:opacity-50 transition"
              >
                {m.approveLead.isPending && m.approveLead.variables === n.lead_queue_id
                  ? <Loader2 className="w-3 h-3 animate-spin" />
                  : <Check className="w-3 h-3" />}
                Approve
              </button>
              <button
                disabled={m.rejectLead.isPending}
                onClick={() => m.rejectLead.mutate(n.lead_queue_id!)}
                className="inline-flex items-center gap-1 px-2 py-1 rounded-md text-2xs font-bold bg-white text-stone-500 ring-1 ring-stone-200 hover:bg-stone-50 disabled:opacity-50 transition"
              >
                <X className="w-3 h-3" />Reject
              </button>
            </>
          )}

          {/* ── known person moved: apply the move ── */}
          {n.person_review_status === 'actioned' && (
            <span className="inline-flex items-center gap-1 px-2 py-1 rounded-md text-2xs font-bold bg-emerald-600 text-white">
              <CheckCircle2 className="w-3 h-3" />Move applied
            </span>
          )}
          {n.person_review_status === 'pending' && n.person_review_id != null && (
            <>
              <span className="inline-flex items-center gap-1 px-2 py-1 rounded-md text-2xs font-bold bg-navy-50 text-navy-700 ring-1 ring-navy-100">
                <UserCheck className="w-3 h-3" />Known contact moved
              </span>
              <button
                disabled={m.actionPerson.isPending}
                onClick={() => m.actionPerson.mutate(n.person_review_id!)}
                className="inline-flex items-center gap-1 px-2 py-1 rounded-md text-2xs font-bold bg-navy-900 text-white hover:bg-navy-700 disabled:opacity-50 transition"
              >
                {m.actionPerson.isPending && m.actionPerson.variables === n.person_review_id
                  ? <Loader2 className="w-3 h-3 animate-spin" />
                  : <UserCheck className="w-3 h-3" />}
                Apply move
              </button>
              <button
                disabled={m.dismissPerson.isPending}
                onClick={() => m.dismissPerson.mutate(n.person_review_id!)}
                className="inline-flex items-center gap-1 px-2 py-1 rounded-md text-2xs font-bold bg-white text-stone-500 ring-1 ring-stone-200 hover:bg-stone-50 disabled:opacity-50 transition"
              >
                <X className="w-3 h-3" />Dismiss
              </button>
            </>
          )}

          {/* ── new person at a hotel we own: add as a contact ── */}
          {n.contact_review_status === 'actioned' && (
            <span className="inline-flex items-center gap-1 px-2 py-1 rounded-md text-2xs font-bold bg-emerald-600 text-white">
              <CheckCircle2 className="w-3 h-3" />Contact added
            </span>
          )}
          {n.contact_review_status === 'pending' && n.contact_review_id != null && (
            <>
              <span className="inline-flex items-center gap-1 px-2 py-1 rounded-md text-2xs font-bold bg-emerald-50 text-emerald-700 ring-1 ring-emerald-200">
                <UserCheck className="w-3 h-3" />
                New contact{n.contact_review_hotel ? ` · ${n.contact_review_hotel}` : ''}
              </span>
              <button
                disabled={m.addContact.isPending}
                onClick={() => m.addContact.mutate(n.contact_review_id!)}
                className="inline-flex items-center gap-1 px-2 py-1 rounded-md text-2xs font-bold bg-emerald-600 text-white hover:bg-emerald-700 disabled:opacity-50 transition"
              >
                {m.addContact.isPending && m.addContact.variables === n.contact_review_id
                  ? <Loader2 className="w-3 h-3 animate-spin" />
                  : <UserCheck className="w-3 h-3" />}
                Add contact
              </button>
              <button
                disabled={m.skipContact.isPending}
                onClick={() => m.skipContact.mutate(n.contact_review_id!)}
                className="inline-flex items-center gap-1 px-2 py-1 rounded-md text-2xs font-bold bg-white text-stone-500 ring-1 ring-stone-200 hover:bg-stone-50 disabled:opacity-50 transition"
              >
                <X className="w-3 h-3" />Skip
              </button>
            </>
          )}
        </div>
      )}
    </div>
  )
}
