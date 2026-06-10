import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { cn } from '@/lib/utils'
import {
  Newspaper, Loader2, ExternalLink, Handshake, Target, Star, RefreshCw,
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
          ) : items.length === 0 ? (
            <div className="h-60 flex flex-col items-center justify-center text-stone-400 gap-2">
              <Newspaper className="w-8 h-8" />
              <p className="text-sm">No stories match these filters.</p>
            </div>
          ) : (
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-3.5 items-start">
              {items.map((n) => (
                <NewsCard key={n.id} n={n} />
              ))}
            </div>
          )}
        </div>
      </div>
    </div>
  )
}

function NewsCard({ n }: { n: NewsItem }) {
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

      {(hasRel || n.in_pipeline) && (
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
        </div>
      )}
    </div>
  )
}
