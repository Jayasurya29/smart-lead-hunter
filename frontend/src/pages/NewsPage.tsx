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
  { key: 'other', label: 'Other' },
]

const REGIONS = [
  { key: '', label: 'All regions' },
  { key: 'usa', label: 'USA' },
  { key: 'caribbean', label: 'Caribbean' },
]

const CAT_STYLE: Record<string, { bg: string; text: string }> = {
  appointment: { bg: 'bg-violet-50', text: 'text-violet-700' },
  opening: { bg: 'bg-emerald-50', text: 'text-emerald-700' },
  acquisition: { bg: 'bg-amber-50', text: 'text-amber-700' },
  rebrand: { bg: 'bg-sky-50', text: 'text-sky-700' },
  renovation: { bg: 'bg-orange-50', text: 'text-orange-700' },
  management_change: { bg: 'bg-fuchsia-50', text: 'text-fuchsia-700' },
  other: { bg: 'bg-stone-100', text: 'text-stone-600' },
}

async function newsFetch(qs: string): Promise<NewsItem[]> {
  const res = await fetch(`/api/news${qs}`, { credentials: 'include' })
  if (!res.ok) throw new Error(`News API ${res.status}`)
  return res.json()
}

export default function NewsPage() {
  const [category, setCategory] = useState('')
  const [region, setRegion] = useState('')
  const [relOnly, setRelOnly] = useState(false)

  const params = new URLSearchParams()
  if (category) params.set('category', category)
  if (region) params.set('region', region)
  if (relOnly) params.set('only_relationships', 'true')
  const qs = params.toString() ? `?${params.toString()}` : ''

  const { data, isLoading, isError, refetch, isFetching } = useQuery({
    queryKey: ['news', category, region, relOnly],
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
      <div className="px-4 pt-3 pb-2 flex-shrink-0 flex items-center justify-between">
        <div className="flex items-center gap-2.5">
          <div className="h-9 w-9 rounded-lg bg-navy-50 flex items-center justify-center">
            <Newspaper className="w-5 h-5 text-navy-600" />
          </div>
          <div>
            <h1 className="text-lg font-bold text-navy-900 leading-tight">
              Hospitality News
            </h1>
            <p className="text-2xs text-stone-500">
              {items.length} stor{items.length === 1 ? 'y' : 'ies'}
              {relCount > 0 && (
                <>
                  {' '}·{' '}
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
          className="h-9 px-3 inline-flex items-center gap-1.5 text-xs font-semibold rounded-lg border border-stone-200 bg-white text-stone-600 hover:bg-stone-50 transition"
        >
          <RefreshCw className={cn('w-3.5 h-3.5', isFetching && 'animate-spin')} />
          Refresh
        </button>
      </div>

      {/* filters */}
      <div className="px-4 pb-2 flex-shrink-0 flex flex-wrap items-center gap-2">
        {CATEGORIES.map((c) => (
          <button
            key={c.key}
            onClick={() => setCategory(c.key)}
            className={cn(
              'h-8 px-3 text-xs font-semibold rounded-lg border transition',
              category === c.key
                ? 'bg-navy-600 text-white border-navy-600'
                : 'bg-white text-stone-500 border-stone-200 hover:bg-stone-50',
            )}
          >
            {c.label}
          </button>
        ))}

        <div className="w-px h-6 bg-stone-200 mx-1" />

        <div className="inline-flex rounded-lg border border-stone-200 bg-white p-0.5 shadow-sm">
          {REGIONS.map((r) => (
            <button
              key={r.key}
              onClick={() => setRegion(r.key)}
              className={cn(
                'px-3 h-7 text-xs font-semibold rounded-md transition',
                region === r.key
                  ? 'bg-navy-600 text-white'
                  : 'text-stone-500 hover:text-stone-700',
              )}
            >
              {r.label}
            </button>
          ))}
        </div>

        <button
          onClick={() => setRelOnly((v) => !v)}
          className={cn(
            'h-8 px-3 inline-flex items-center gap-1.5 text-xs font-semibold rounded-lg border transition',
            relOnly
              ? 'bg-emerald-600 text-white border-emerald-600'
              : 'bg-white text-stone-500 border-stone-200 hover:bg-stone-50',
          )}
        >
          <Handshake className="w-3.5 h-3.5" />
          Relationships only
        </button>
      </div>

      {/* feed */}
      <div className="flex-1 overflow-y-auto px-4 pb-4">
        {isLoading ? (
          <div className="h-40 flex items-center justify-center text-stone-400">
            <Loader2 className="w-6 h-6 animate-spin" />
          </div>
        ) : isError ? (
          <div className="h-40 flex items-center justify-center text-sm text-red-500">
            Couldn&apos;t load news. Try Refresh.
          </div>
        ) : items.length === 0 ? (
          <div className="h-40 flex flex-col items-center justify-center text-stone-400 gap-1">
            <Newspaper className="w-7 h-7" />
            <p className="text-sm">No stories match these filters.</p>
          </div>
        ) : (
          <div className="space-y-2.5 max-w-3xl">
            {items.map((n) => (
              <NewsCard key={n.id} n={n} />
            ))}
          </div>
        )}
      </div>
    </div>
  )
}

function NewsCard({ n }: { n: NewsItem }) {
  const cat = CAT_STYLE[n.category ?? 'other'] ?? CAT_STYLE.other
  const hits = n.relationship_hits ?? []
  let when = n.published_hint || ''
  if (!when && n.created_at) {
    try {
      when = `${formatDistanceToNow(new Date(n.created_at))} ago`
    } catch {
      when = ''
    }
  }

  return (
    <div className="bg-white rounded-xl border border-slate-200/80 shadow-sm p-3.5 hover:shadow-md transition">
      <div className="flex items-center gap-2 mb-1.5 flex-wrap">
        <span
          className={cn(
            'inline-flex items-center px-2 py-0.5 rounded-full text-2xs font-bold uppercase tracking-wide',
            cat.bg,
            cat.text,
          )}
        >
          {(n.category ?? 'other').replace(/_/g, ' ')}
        </span>
        {n.region && (
          <span className="text-2xs font-semibold text-stone-400 uppercase">
            {n.region}
          </span>
        )}
        {n.luxury && (
          <span className="inline-flex items-center gap-0.5 text-2xs font-bold text-amber-500">
            <Star className="w-3 h-3 fill-amber-400 stroke-amber-400" />
            Luxury
          </span>
        )}
        <span className="ml-auto text-2xs text-stone-400">
          {n.source}
          {when && ` · ${when}`}
        </span>
      </div>

      <a
        href={n.url}
        target="_blank"
        rel="noreferrer"
        className="group inline-flex items-start gap-1 text-sm font-semibold text-navy-900 hover:text-navy-600 transition leading-snug"
      >
        {n.title}
        <ExternalLink className="w-3 h-3 mt-0.5 opacity-0 group-hover:opacity-60 flex-shrink-0" />
      </a>

      {n.snippet && (
        <p className="text-xs text-stone-500 mt-1 line-clamp-2 leading-relaxed">
          {n.snippet}
        </p>
      )}

      {(n.hotel_name || n.person_name) && (
        <div className="text-2xs text-stone-500 mt-1.5">
          {n.person_name && (
            <span className="font-semibold text-stone-700">
              {n.person_name}
              {n.person_title && (
                <span className="font-normal text-stone-500"> — {n.person_title}</span>
              )}
            </span>
          )}
          {n.person_name && n.hotel_name && ' · '}
          {n.hotel_name && (
            <span>
              {n.hotel_name}
              {n.brand && n.brand !== n.hotel_name && ` (${n.brand})`}
            </span>
          )}
        </div>
      )}

      {(hits.length > 0 || n.in_pipeline) && (
        <div className="flex flex-wrap items-center gap-1.5 mt-2 pt-2 border-t border-slate-100">
          {hits.length > 0 && (
            <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-2xs font-bold bg-emerald-50 text-emerald-700">
              <Handshake className="w-3 h-3" />
              Known: {hits[0].person || 'contact'}
              {hits[0].account ? ` · ${hits[0].account}` : ''}
              {hits.length > 1 && ` +${hits.length - 1}`}
            </span>
          )}
          {n.in_pipeline && (
            <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-2xs font-bold bg-navy-50 text-navy-700">
              <Target className="w-3 h-3" />
              {n.pipeline_ref || 'In pipeline'}
            </span>
          )}
        </div>
      )}
    </div>
  )
}
