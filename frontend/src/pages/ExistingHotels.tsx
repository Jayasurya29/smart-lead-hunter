import { useState, useMemo, useEffect } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import api from '@/api/client'
import ConfirmDialog from '@/components/ui/ConfirmDialog'
import EnrichProgress from '@/components/leads/EnrichProgress'
import SmartFillProgress from '@/components/leads/SmartFillProgress'
import {
  cn, getTierColor, getTierLabel,
} from '@/lib/utils'
import {
  Building2, MapPin, Users, Eye, Search, X, ChevronLeft, ChevronRight,
  Phone, Globe, User, ExternalLink, Loader2, CheckCircle2, XCircle, Undo2,
  Mail, Linkedin, DollarSign,
} from 'lucide-react'

/* ═══════════════════════════════════════════════════
   TYPES
   ═══════════════════════════════════════════════════ */

interface Hotel {
  id: number
  name: string
  brand: string | null
  chain: string | null
  brand_tier: string | null
  address: string | null
  city: string | null
  state: string | null
  country: string | null
  zip_code: string | null
  latitude: number | null
  longitude: number | null
  room_count: number | null
  phone: string | null
  website: string | null
  property_type: string | null
  gm_name: string | null
  gm_title: string | null
  gm_email: string | null
  gm_phone: string | null
  gm_linkedin: string | null
  is_client: boolean
  sap_bp_code: string | null
  client_notes: string | null
  data_source: string | null
  status: string | null
  pushed_to_map: boolean
  lead_score: number | null
  revenue_opening: number | null
  revenue_annual: number | null
  insightly_id: number | null
  rejection_reason: string | null
  zone: string | null
  created_at: string | null
  updated_at: string | null
}

interface HotelStats {
  total: number
  clients: number
  prospects: number
  geocoded: number
  with_contact: number
  with_tier: number
  on_map: number
  tiers: Record<string, number>
  top_states: { state: string; count: number }[]
  zones: { zone: string; key?: string | null; state?: string | null; priority?: string | null; count: number }[]
}

interface Filters {
  search: string
  state: string
  brand_tier: string
  is_client: string
  zone: string
  sort: string
}

type PipelineTab = 'pipeline' | 'approved' | 'rejected'

// Map full region names → 2-letter codes (US states + Caribbean countries)
const STATE_NAME_TO_CODE: Record<string, string> = {
  // US states
  "Alabama":"AL","Alaska":"AK","Arizona":"AZ","Arkansas":"AR","California":"CA",
  "Colorado":"CO","Connecticut":"CT","Delaware":"DE","District of Columbia":"DC",
  "Florida":"FL","Georgia":"GA","Hawaii":"HI","Idaho":"ID","Illinois":"IL",
  "Indiana":"IN","Iowa":"IA","Kansas":"KS","Kentucky":"KY","Louisiana":"LA",
  "Maine":"ME","Maryland":"MD","Massachusetts":"MA","Michigan":"MI","Minnesota":"MN",
  "Mississippi":"MS","Missouri":"MO","Montana":"MT","Nebraska":"NE","Nevada":"NV",
  "New Hampshire":"NH","New Jersey":"NJ","New Mexico":"NM","New York":"NY",
  "North Carolina":"NC","North Dakota":"ND","Ohio":"OH","Oklahoma":"OK","Oregon":"OR",
  "Pennsylvania":"PA","Rhode Island":"RI","South Carolina":"SC","South Dakota":"SD",
  "Tennessee":"TN","Texas":"TX","Utah":"UT","Vermont":"VT","Virginia":"VA",
  "Washington":"WA","West Virginia":"WV","Wisconsin":"WI","Wyoming":"WY",
  // Caribbean countries (matching zones added to zones_registry.py)
  "Bahamas":"BS","Jamaica":"JM","Dominican Republic":"DO","Puerto Rico":"PR",
  "Cayman Islands":"CY","Turks and Caicos":"TC","Bermuda":"BM",
  "US Virgin Islands":"VI","British Virgin Islands":"VG","Barbados":"BB",
  "Aruba":"AW","Curaçao":"CW","Saint Lucia":"LC","Antigua and Barbuda":"AG",
  "Anguilla":"AI","St. Kitts and Nevis":"KN","St. Martin / Sint Maarten":"SX",
  "Grenada":"GD","Dominica":"DM","Trinidad and Tobago":"TT",
  "St. Vincent & Grenadines":"VC",
}

const DEFAULT_FILTERS: Filters = {
  search: '',
  state: '',
  brand_tier: '',
  is_client: '',
  zone: '',
  sort: 'name_az',
}

function fmtRevenue(n: number | null | undefined): string {
  if (!n) return '—'
  if (n >= 1_000_000) return `$${(n / 1_000_000).toFixed(1)}M`
  if (n >= 1_000) return `$${Math.round(n / 1_000)}K`
  return `$${n.toLocaleString()}`
}

/* ═══════════════════════════════════════════════════
   MAIN PAGE
   ═══════════════════════════════════════════════════ */

export default function ExistingHotels() {
  const [page, setPage] = useState(1)
  const [filters, setFilters] = useState<Filters>(DEFAULT_FILTERS)
  const [selectedId, setSelectedId] = useState<number | null>(null)
  const [activeTab, setActiveTab] = useState<PipelineTab>('pipeline')

  const statusMap: Record<PipelineTab, string> = {
    pipeline: 'new',
    approved: 'approved',
    rejected: 'rejected',
  }

  const { data: stats } = useQuery<HotelStats>({
    queryKey: ['existing-hotels-stats'],
    queryFn: async () => (await api.get('/api/existing-hotels/stats')).data,
  })

  const { data, isLoading } = useQuery({
    queryKey: ['existing-hotels', page, filters, activeTab],
    queryFn: async () => {
      const params: Record<string, string> = {
        page: String(page),
        per_page: '25',
        sort: filters.sort,
        status: statusMap[activeTab],
      }
      if (filters.search) params.search = filters.search
      if (filters.state) params.state = filters.state
      if (filters.brand_tier) params.brand_tier = filters.brand_tier
      if (filters.is_client) params.is_client = filters.is_client
      if (filters.zone) params.zone = filters.zone
      const { data } = await api.get('/api/existing-hotels', { params })
      return data as { hotels: Hotel[]; total: number; page: number; pages: number }
    },
  })

  function handleFilterChange(key: string, value: string) {
    if (key === 'state') {
      setFilters(prev => ({ ...prev, state: value, zone: '' }))
    } else {
      setFilters(prev => ({ ...prev, [key]: value }))
    }
    setPage(1)
  }

  function handleTabChange(tab: PipelineTab) {
    setActiveTab(tab)
    setPage(1)
    setSelectedId(null)
  }

  function handleStatClick(clientFilter: string) {
    setFilters(prev => ({ ...prev, is_client: clientFilter }))
    setPage(1)
  }

  const hotels = data?.hotels || []
  const total = data?.total || 0
  const totalPages = data?.pages || 1

  const STATS_CONFIG = [
    { key: 'total',        label: 'Total Hotels',  value: stats?.total || 0,        icon: Building2, bg: 'bg-navy-50',    text: 'text-navy-600',    accent: 'border-navy-100',    clientFilter: '' },
    { key: 'clients',      label: 'Clients',       value: stats?.clients || 0,      icon: Users,     bg: 'bg-emerald-50', text: 'text-emerald-600', accent: 'border-emerald-100', clientFilter: 'true' },
    { key: 'prospects',    label: 'Prospects',      value: stats?.prospects || 0,    icon: Eye,       bg: 'bg-coral-50',   text: 'text-coral-500',   accent: 'border-coral-100',   clientFilter: 'false' },
    { key: 'geocoded',     label: 'Geocoded',       value: stats?.geocoded || 0,     icon: MapPin,    bg: 'bg-sky-50',     text: 'text-sky-600',     accent: 'border-sky-100',     clientFilter: null },
    { key: 'with_contact', label: 'With Contact',   value: stats?.with_contact || 0, icon: User,      bg: 'bg-violet-50',  text: 'text-violet-600',  accent: 'border-violet-100',  clientFilter: null },
    { key: 'with_tier',    label: 'With Tier',      value: stats?.with_tier || 0,    icon: Building2, bg: 'bg-gold-50',    text: 'text-gold-600',    accent: 'border-gold-100',    clientFilter: null },
    { key: 'on_map',       label: 'On Map',         value: stats?.on_map || 0,       icon: MapPin,    bg: 'bg-stone-100',  text: 'text-stone-500',   accent: 'border-stone-200',   clientFilter: null },
  ]

  return (
    <div className="h-full flex flex-col">
      {/* Stats Cards — animated, clickable */}
      <div className="px-4 pt-3 pb-2 flex-shrink-0">
        {!stats ? (
          <div className="grid grid-cols-4 lg:grid-cols-7 gap-2.5">
            {Array.from({ length: 7 }).map((_, i) => (
              <div key={i} className="skeleton rounded-lg h-[58px]" style={{ animationDelay: `${i * 0.05}s` }} />
            ))}
          </div>
        ) : (
          <div className="grid grid-cols-4 lg:grid-cols-7 gap-2.5">
            {STATS_CONFIG.map((cfg, i) => {
              const Icon = cfg.icon
              const isClickable = cfg.clientFilter !== null
              const isActive = isClickable && filters.is_client === cfg.clientFilter
              return (
                <div
                  key={cfg.key}
                  onClick={() => isClickable && handleStatClick(cfg.clientFilter!)}
                  className={cn(
                    'stat-card rounded-lg border px-3 py-2.5 flex items-center gap-2.5 animate-slideUp select-none',
                    cfg.accent,
                    isClickable ? 'cursor-pointer hover:shadow-sm' : '',
                    isActive ? `${cfg.bg} ring-2 ring-navy-400 shadow-md` : 'bg-white',
                  )}
                  style={{ animationDelay: `${i * 0.04}s`, animationFillMode: 'both' }}
                >
                  <div className={cn('w-8 h-8 rounded-lg flex items-center justify-center flex-shrink-0', cfg.bg)}>
                    <Icon className={cn('w-4 h-4', cfg.text)} />
                  </div>
                  <div className="min-w-0">
                    <div className="text-lg font-bold text-navy-900 leading-tight tabular-nums">{cfg.value}</div>
                    <div className="text-2xs text-stone-400 font-semibold uppercase tracking-wider">{cfg.label}</div>
                  </div>
                </div>
              )
            })}
          </div>
        )}
      </div>

      {/* Pipeline Tabs — pill style matching Dashboard */}
      <div className="px-4 pb-2 flex-shrink-0 space-y-2.5">
        <div className="flex items-center gap-4">
          <div className="flex gap-px bg-stone-200/60 p-0.5 rounded-lg flex-shrink-0">
            {([
              { key: 'pipeline' as const, label: 'Pipeline', icon: Building2 },
              { key: 'approved' as const, label: 'Approved', icon: CheckCircle2 },
              { key: 'rejected' as const, label: 'Rejected', icon: XCircle },
            ]).map((t) => {
              const Icon = t.icon
              const count = t.key === activeTab ? total : undefined
              return (
                <button
                  key={t.key}
                  onClick={() => handleTabChange(t.key)}
                  className={cn(
                    'flex items-center gap-1.5 px-3.5 py-2 text-xs font-semibold rounded-md transition-all duration-150',
                    activeTab === t.key
                      ? 'bg-white text-navy-900 shadow-sm'
                      : 'text-stone-500 hover:text-stone-700',
                  )}
                >
                  <Icon className="w-4 h-4" />
                  {t.label}
                  {count !== undefined && (
                    <span className={cn('text-2xs ml-0.5 tabular-nums', activeTab === t.key ? 'text-navy-500' : 'text-stone-400')}>
                      {count}
                    </span>
                  )}
                </button>
              )
            })}
          </div>
          <div className="relative flex-1 max-w-lg">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-stone-400" />
            <input
              type="text"
              value={filters.search}
              onChange={(e) => handleFilterChange('search', e.target.value)}
              placeholder="Search hotel, brand, city, state..."
              className="w-full h-9 pl-9 pr-9 text-sm bg-white border border-stone-200 rounded-lg outline-none focus:border-navy-400 focus:ring-2 focus:ring-navy-100 transition placeholder:text-stone-400"
            />
            {filters.search && (
              <button onClick={() => handleFilterChange('search', '')} className="absolute right-2.5 top-1/2 -translate-y-1/2 p-0.5 text-stone-400 hover:text-stone-600 rounded transition">
                <X className="w-3.5 h-3.5" />
              </button>
            )}
          </div>
        </div>

        {/* Filter row */}
        <div className="flex items-center gap-3">
          <select value={filters.state} onChange={(e) => handleFilterChange('state', e.target.value)} className="h-9 px-3 text-sm bg-white border border-stone-200 rounded-lg outline-none focus:border-navy-400">
            <option value="">All States</option>
            {(stats?.top_states || []).map(s => (
              <option key={s.state} value={s.state}>{s.state} ({s.count})</option>
            ))}
          </select>

          <select value={filters.brand_tier} onChange={(e) => handleFilterChange('brand_tier', e.target.value)} className="h-9 px-3 text-sm bg-white border border-stone-200 rounded-lg outline-none focus:border-navy-400">
            <option value="">All Tiers</option>
            <option value="tier1_ultra_luxury">T1 — Ultra Luxury</option>
            <option value="tier2_luxury">T2 — Luxury</option>
            <option value="tier3_upper_upscale">T3 — Upper Upscale</option>
            <option value="tier4_upscale">T4 — Upscale</option>
          </select>

          <select value={filters.zone} onChange={(e) => handleFilterChange('zone', e.target.value)} className="h-9 px-3 text-sm bg-white border border-stone-200 rounded-lg outline-none focus:border-navy-400">
            <option value="">All Zones</option>
            {(stats?.zones || [])
              .filter(z => !['Out of State', 'Unknown', 'Junk'].includes(z.zone))
              .filter(z => {
                // No state selected → show every zone
                if (!filters.state) return true
                // State selected → only zones in that state
                const selectedCode = STATE_NAME_TO_CODE[filters.state] || filters.state
                return z.state === selectedCode
              })
              .map(z => (
                <option key={z.zone} value={z.zone}>{z.zone} ({z.count})</option>
              ))}
          </select>

          <select value={filters.is_client} onChange={(e) => handleFilterChange('is_client', e.target.value)} className="h-9 px-3 text-sm bg-white border border-stone-200 rounded-lg outline-none focus:border-navy-400">
            <option value="">All Hotels</option>
            <option value="true">Clients Only</option>
            <option value="false">Prospects Only</option>
          </select>

          {Object.values(filters).some(v => v) && (
            <button onClick={() => { setFilters(DEFAULT_FILTERS); setPage(1) }} className="h-9 px-3 text-xs font-semibold text-red-500 bg-red-50 border border-red-200 rounded-lg hover:bg-red-100 transition">
              <X className="w-3.5 h-3.5 inline mr-1" />Clear
            </button>
          )}
        </div>
      </div>

      {/* Table + Detail Panel */}
      <div className="flex-1 flex overflow-hidden px-4 pb-3 gap-3">
        <div className={cn(
          'bg-white rounded-xl border border-slate-200/80 shadow-sm overflow-hidden flex flex-col transition-all duration-300',
          selectedId ? 'flex-[3]' : 'flex-1',
        )}>
          <HotelTable
            hotels={hotels}
            total={total}
            page={page}
            totalPages={totalPages}
            tab={activeTab}
            selectedId={selectedId}
            onSelect={setSelectedId}
            onPageChange={setPage}
            isLoading={isLoading}
          />
        </div>

        {selectedId && (
          <div className="flex-[2] bg-white rounded-xl border border-slate-200/80 shadow-sm overflow-hidden animate-slideIn">
            <HotelDetail hotelId={selectedId} tab={activeTab} onClose={() => setSelectedId(null)} />
          </div>
        )}
      </div>
    </div>
  )
}


/* ═══════════════════════════════════════════════════
   HOTEL TABLE
   ═══════════════════════════════════════════════════ */

function HotelTable({ hotels, total, page, totalPages, tab, selectedId, onSelect, onPageChange, isLoading }: {
  hotels: Hotel[]; total: number; page: number; totalPages: number; tab: PipelineTab
  selectedId: number | null; onSelect: (id: number) => void
  onPageChange: (p: number) => void; isLoading: boolean
}) {
  const qc = useQueryClient()
  const [confirmTarget, setConfirmTarget] = useState<{ action: 'approve' | 'reject' | 'restore'; hotel: Hotel } | null>(null)

  const approveMut = useMutation({
    mutationFn: (id: number) => api.post(`/api/existing-hotels/${id}/approve`),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['existing-hotels'] }); qc.invalidateQueries({ queryKey: ['existing-hotels-stats'] }); qc.invalidateQueries({ queryKey: ['map-data'] }) },
  })
  const rejectMut = useMutation({
    mutationFn: (id: number) => api.post(`/api/existing-hotels/${id}/reject`, {}),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['existing-hotels'] }); qc.invalidateQueries({ queryKey: ['existing-hotels-stats'] }); qc.invalidateQueries({ queryKey: ['map-data'] }) },
  })
  const restoreMut = useMutation({
    mutationFn: (id: number) => api.post(`/api/existing-hotels/${id}/restore`),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['existing-hotels'] }); qc.invalidateQueries({ queryKey: ['existing-hotels-stats'] }); qc.invalidateQueries({ queryKey: ['map-data'] }) },
  })

  const isNew = tab === 'pipeline'
  const isApproved = tab === 'approved'
  const isRejected = tab === 'rejected'

  function handleConfirm() {
    if (!confirmTarget) return
    const { action, hotel } = confirmTarget
    if (action === 'approve') approveMut.mutate(hotel.id)
    if (action === 'reject') rejectMut.mutate(hotel.id)
    if (action === 'restore') restoreMut.mutate(hotel.id)
    setConfirmTarget(null)
  }

  if (isLoading) {
    return (
      <div className="space-y-px p-1">
        {Array.from({ length: 12 }).map((_, i) => (
          <div key={i} className="skeleton h-[48px] rounded" style={{ animationDelay: `${i * 0.03}s` }} />
        ))}
      </div>
    )
  }

  if (!hotels.length) {
    return (
      <div className="flex flex-col items-center justify-center py-20 text-stone-400">
        <Building2 className="w-12 h-12 text-stone-300 mb-3" />
        <p className="text-sm font-medium">No {tab === 'pipeline' ? '' : tab} hotels found</p>
      </div>
    )
  }

  const confirmHotel = confirmTarget?.hotel
  const confirmAction = confirmTarget?.action

  return (
    <div className="flex flex-col h-full">
      <div className="flex-1 overflow-y-auto">
        <table className="w-full">
          <thead className="sticky top-0 z-10">
            <tr className="bg-slate-50/90 backdrop-blur-sm border-b border-slate-100">
              <th className="px-3 py-2.5 text-left text-[11px] font-bold text-slate-400 uppercase tracking-wider w-16">Score</th>
              <th className="px-3 py-2.5 text-left text-[11px] font-bold text-slate-400 uppercase tracking-wider">Hotel</th>
              <th className="px-3 py-2.5 text-left text-[11px] font-bold text-slate-400 uppercase tracking-wider w-16">Tier</th>
              <th className="px-3 py-2.5 text-left text-[11px] font-bold text-slate-400 uppercase tracking-wider">Location</th>
              <th className="px-3 py-2.5 text-left text-[11px] font-bold text-slate-400 uppercase tracking-wider w-28">Zone</th>
              <th className="px-3 py-2.5 text-left text-[11px] font-bold text-slate-400 uppercase tracking-wider w-24">Potential</th>
              <th className="px-3 py-2.5 text-left text-[11px] font-bold text-slate-400 uppercase tracking-wider w-20">Type</th>
              <th className="px-3 py-2.5 w-24" />
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-100/80">
            {hotels.map((hotel) => {
              // Read canonical hotel_name (post-018) with legacy `name` fallback.
              // Migration 018 backfilled both, but the API now returns
              // hotel_name as the canonical field. Old rows updated by post-018
              // code only have hotel_name set — using `hotel.name` alone shows
              // a blank cell.
              const h = hotel as any
              const displayName = h.hotel_name || hotel.name
              const score = hotel.lead_score
              return (
              <tr
                key={hotel.id}
                onClick={() => onSelect(hotel.id)}
                className={cn(
                  'lead-row cursor-pointer',
                  selectedId === hotel.id && 'active',
                )}
              >
                <td className="px-3 py-2.5">
                  {/* Score badge — same color buckets as New Hotels' LeadTable.
                      Em-dash when no score yet (existing hotels not all
                      scored — Smart Fill or Run Enrichment populates over
                      time). */}
                  <div
                    className={cn(
                      'inline-flex items-center justify-center w-9 h-7 rounded-md text-xs font-bold tabular-nums',
                      score == null ? 'bg-stone-50 text-stone-300' :
                      score >= 75 ? 'bg-emerald-50 text-emerald-700' :
                      score >= 55 ? 'bg-amber-50 text-amber-700' :
                      score >= 35 ? 'bg-orange-50 text-orange-700' :
                      'bg-stone-100 text-stone-500',
                    )}
                  >
                    {score ?? '—'}
                  </div>
                </td>
                <td className="px-3 py-2.5 max-w-[280px]">
                  <div className="truncate text-[15px] font-bold text-navy-950 leading-snug">{displayName || '—'}</div>
                  {hotel.brand && <div className="truncate text-xs text-stone-400 leading-snug">{hotel.brand}</div>}
                </td>
                <td className="px-3 py-2.5">
                  {hotel.brand_tier ? (
                    <span className={cn('inline-flex px-2 py-0.5 rounded text-2xs font-bold', getTierColor(hotel.brand_tier))}>
                      {getTierLabel(hotel.brand_tier)}
                    </span>
                  ) : (
                    <span className="text-xs text-stone-300">—</span>
                  )}
                </td>
                <td className="px-3 py-2.5">
                  <span className="text-sm text-navy-800 font-medium truncate block max-w-[200px]">
                    {[hotel.city, hotel.state].filter(Boolean).join(', ') || '—'}
                  </span>
                </td>
                <td className="px-3 py-2.5">
                  {hotel.zone ? (
                    <span className="text-xs text-stone-500 font-medium">{hotel.zone}</span>
                  ) : (
                    <span className="text-xs text-stone-300">—</span>
                  )}
                </td>
                <td className="px-3 py-2.5">
                  {hotel.revenue_opening ? (
                    <span className="text-sm font-bold text-emerald-700">{fmtRevenue(hotel.revenue_annual)}</span>
                  ) : (
                    <span className="text-xs text-stone-300">—</span>
                  )}
                </td>
                <td className="px-3 py-2.5">
                  <span className={cn(
                    'inline-flex px-2 py-0.5 rounded-full text-2xs font-bold',
                    hotel.is_client ? 'bg-emerald-50 text-emerald-600' : 'bg-amber-50 text-amber-600',
                  )}>
                    {hotel.is_client ? 'Client' : 'Prospect'}
                  </span>
                </td>
                <td className="px-2 py-2.5">
                  <div className="row-actions flex items-center gap-0.5 justify-end">
                    {isNew && (
                      <>
                        <ActionBtn onClick={(e) => { e.stopPropagation(); setConfirmTarget({ action: 'approve', hotel }) }} color="emerald" title="Approve">
                          <CheckCircle2 className="w-4 h-4" />
                        </ActionBtn>
                        <ActionBtn onClick={(e) => { e.stopPropagation(); setConfirmTarget({ action: 'reject', hotel }) }} color="red" title="Reject">
                          <XCircle className="w-4 h-4" />
                        </ActionBtn>
                      </>
                    )}
                    {isApproved && (
                      <ActionBtn onClick={(e) => { e.stopPropagation(); setConfirmTarget({ action: 'restore', hotel }) }} color="amber" title="Back to Pipeline">
                        <Undo2 className="w-4 h-4" />
                      </ActionBtn>
                    )}
                    {isRejected && (
                      <ActionBtn onClick={(e) => { e.stopPropagation(); setConfirmTarget({ action: 'restore', hotel }) }} color="blue" title="Restore">
                        <Undo2 className="w-4 h-4" />
                      </ActionBtn>
                    )}
                  </div>
                </td>
              </tr>
              )
            })}
          </tbody>
        </table>
      </div>

      {/* Pagination */}
      {totalPages > 1 && (
        <div className="flex items-center justify-between px-4 py-2.5 border-t border-slate-100 bg-white/80 flex-shrink-0">
          <span className="text-xs text-stone-400">
            Page {page} of {totalPages} · {total} hotel{total !== 1 ? 's' : ''}
          </span>
          <div className="flex items-center gap-1">
            <button onClick={() => onPageChange(page - 1)} disabled={page <= 1} className="p-1.5 rounded hover:bg-stone-100 disabled:opacity-30 disabled:cursor-not-allowed transition">
              <ChevronLeft className="w-4 h-4 text-stone-500" />
            </button>
            {Array.from({ length: Math.min(totalPages, 7) }).map((_, i) => {
              let pageNum: number
              if (totalPages <= 7) pageNum = i + 1
              else if (page <= 4) pageNum = i + 1
              else if (page >= totalPages - 3) pageNum = totalPages - 6 + i
              else pageNum = page - 3 + i
              return (
                <button key={pageNum} onClick={() => onPageChange(pageNum)} className={cn('w-8 h-8 rounded text-xs font-semibold transition', page === pageNum ? 'bg-navy-900 text-white' : 'text-stone-500 hover:bg-stone-100')}>
                  {pageNum}
                </button>
              )
            })}
            <button onClick={() => onPageChange(page + 1)} disabled={page >= totalPages} className="p-1.5 rounded hover:bg-stone-100 disabled:opacity-30 disabled:cursor-not-allowed transition">
              <ChevronRight className="w-4 h-4 text-stone-500" />
            </button>
          </div>
        </div>
      )}

      {/* Confirm Dialogs */}
      <ConfirmDialog
        open={confirmAction === 'approve'}
        variant="approve"
        title="Approve Hotel"
        message={`Push "${confirmHotel?.name}" to Insightly CRM?`}
        confirmLabel="Approve & Push"
        pending={approveMut.isPending}
        onConfirm={handleConfirm}
        onCancel={() => setConfirmTarget(null)}
      />
      <ConfirmDialog
        open={confirmAction === 'reject'}
        variant="reject"
        title="Reject Hotel"
        message={`Move "${confirmHotel?.name}" to Rejected? You can restore it later.`}
        confirmLabel="Reject"
        pending={rejectMut.isPending}
        onConfirm={handleConfirm}
        onCancel={() => setConfirmTarget(null)}
      />
      <ConfirmDialog
        open={confirmAction === 'restore'}
        variant="restore"
        title={isApproved ? 'Back to Pipeline' : 'Restore Hotel'}
        message={isApproved
          ? `Move "${confirmHotel?.name}" back to pipeline? This will delete from Insightly.`
          : `Restore "${confirmHotel?.name}" back to pipeline?`}
        confirmLabel={isApproved ? 'Remove from CRM' : 'Restore'}
        pending={restoreMut.isPending}
        onConfirm={handleConfirm}
        onCancel={() => setConfirmTarget(null)}
      />
    </div>
  )
}


/* ═══════════════════════════════════════════════════
   HOTEL DETAIL PANEL
   ═══════════════════════════════════════════════════ */

function HotelDetail({ hotelId, tab, onClose }: { hotelId: number; tab: PipelineTab; onClose: () => void }) {
  const qc = useQueryClient()
  const { data: hotel, isLoading } = useQuery<Hotel>({
    queryKey: ['existing-hotel', hotelId],
    queryFn: async () => (await api.get(`/api/existing-hotels/${hotelId}`)).data,
  })

  const [confirmAction, setConfirmAction] = useState<'approve' | 'reject' | 'restore' | null>(null)

  // ── Detail tab system (1B, 2026-04-28) ──
  // Mirror the LeadDetail layout: Overview / Contacts / Edit / Sources.
  // The schema parity from migration 018 means the same data flows into
  // both pages — only the URL prefix differs ("/api/existing-hotels"
  // vs "/api/dashboard/leads"). EnrichProgress + SmartFillProgress now
  // accept a basePath prop for exactly this reason.
  type DetailTab = 'overview' | 'contacts' | 'edit' | 'sources'
  const [activeTab, setActiveTab] = useState<DetailTab>('overview')

  // Run Enrichment / Smart Fill state — scoped per-hotel so navigating
  // between hotels doesn't leak progress between them.
  const [enrichingHotelId, setEnrichingHotelId] = useState<number | null>(null)
  const [smartFillHotelId, setSmartFillHotelId] = useState<number | null>(null)
  const [smartFillMode, setSmartFillMode] = useState<'smart' | 'full'>('smart')
  const enrichingLive = enrichingHotelId === hotelId
  const smartFillLive = smartFillHotelId === hotelId ? smartFillMode : null

  // Auto-attach to running enrichment when this hotel is opened. Same
  // pattern as LeadDetail — calls cheap status endpoint on mount.
  useEffect(() => {
    let cancelled = false
    api.get(`/api/existing-hotels/${hotelId}/enrich-status`)
      .then((r) => {
        if (cancelled) return
        if (r.data?.running) setEnrichingHotelId(hotelId)
      })
      .catch(() => { /* ignore — user can still click Run Enrichment */ })
    return () => { cancelled = true }
  }, [hotelId])

  const approveMut = useMutation({
    mutationFn: () => api.post(`/api/existing-hotels/${hotelId}/approve`),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['existing-hotels'] }); qc.invalidateQueries({ queryKey: ['existing-hotels-stats'] }); qc.invalidateQueries({ queryKey: ['existing-hotel', hotelId] }) },
  })
  const rejectMut = useMutation({
    mutationFn: () => api.post(`/api/existing-hotels/${hotelId}/reject`, {}),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['existing-hotels'] }); qc.invalidateQueries({ queryKey: ['existing-hotels-stats'] }); qc.invalidateQueries({ queryKey: ['existing-hotel', hotelId] }) },
  })
  const restoreMut = useMutation({
    mutationFn: () => api.post(`/api/existing-hotels/${hotelId}/restore`),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['existing-hotels'] }); qc.invalidateQueries({ queryKey: ['existing-hotels-stats'] }); qc.invalidateQueries({ queryKey: ['existing-hotel', hotelId] }) },
  })

  const isNew = tab === 'pipeline'
  const isApproved = tab === 'approved'
  const isRejected = tab === 'rejected'

  if (isLoading || !hotel) {
    return (
      <div className="h-full flex flex-col bg-white animate-fadeIn">
        <div className="p-5 space-y-3">
          <div className="skeleton h-7 w-3/4 rounded" />
          <div className="skeleton h-5 w-1/2 rounded" />
          <div className="skeleton h-36 rounded-lg" />
        </div>
      </div>
    )
  }

  // Use canonical hotel_name (post-018) but fall back to legacy name for safety
  const displayName = (hotel as any).hotel_name || hotel.name

  return (
    <div className="h-full flex flex-col bg-white animate-slideIn">
      {/* Header */}
      <div className="px-5 pt-5 pb-3 flex-shrink-0 border-b border-slate-100 bg-gradient-to-b from-slate-50/50 to-white">
        <div className="flex items-start justify-between gap-3">
          <div className="min-w-0 flex-1">
            <h2 className="text-lg font-bold text-navy-900 leading-snug truncate">{displayName}</h2>
            {hotel.brand && <p className="text-sm text-stone-400 mt-0.5">{hotel.brand}</p>}
          </div>
          <div className="flex items-center gap-2 flex-shrink-0">
            <span className={cn(
              'inline-flex px-2.5 py-1 rounded-full text-xs font-bold',
              hotel.is_client ? 'bg-emerald-50 text-emerald-600' : 'bg-amber-50 text-amber-600',
            )}>
              {hotel.is_client ? 'Client' : 'Prospect'}
            </span>
            <button onClick={onClose} className="p-1.5 text-stone-400 hover:text-stone-600 rounded-lg hover:bg-stone-100 transition">
              <X className="w-4 h-4" />
            </button>
          </div>
        </div>

        <div className="flex items-center gap-2 mt-2 flex-wrap">
          {hotel.brand_tier && (
            <span className={cn('inline-flex px-2 py-0.5 rounded text-xs font-bold', getTierColor(hotel.brand_tier))}>
              {getTierLabel(hotel.brand_tier)}
            </span>
          )}
          {hotel.chain && (
            <span className="inline-flex px-2 py-0.5 rounded text-xs font-medium bg-stone-100 text-stone-500">
              {hotel.chain}
            </span>
          )}
        </div>
      </div>

      {/* Tabs */}
      <div className="flex border-b border-slate-100 px-5 flex-shrink-0">
        {(['overview', 'contacts', 'edit', 'sources'] as DetailTab[]).map((t) => (
          <button
            key={t}
            onClick={() => setActiveTab(t)}
            className={cn(
              'px-3.5 py-3 text-xs font-semibold capitalize transition',
              activeTab === t ? 'text-navy-900 tab-active' : 'text-stone-400 hover:text-stone-600',
            )}
          >
            {t}
          </button>
        ))}
      </div>

      {/* Tab content */}
      <div className="flex-1 overflow-y-auto p-5 space-y-5">
        {activeTab === 'overview' && (
          <HotelOverviewTab
            hotel={hotel}
            hotelId={hotelId}
            onEnrich={() => setEnrichingHotelId(hotelId)}
            enrichingLive={enrichingLive}
            onEnrichComplete={() => {
              qc.invalidateQueries({ queryKey: ['existing-hotel', hotelId] })
              qc.invalidateQueries({ queryKey: ['hotel-contacts', hotelId] })
              setEnrichingHotelId(null)
            }}
            onSmartFill={(mode) => { setSmartFillMode(mode); setSmartFillHotelId(hotelId) }}
            smartFillLive={smartFillLive}
            onSmartFillComplete={() => {
              qc.invalidateQueries({ queryKey: ['existing-hotel', hotelId] })
              setSmartFillHotelId(null)
            }}
          />
        )}
        {activeTab === 'contacts' && (
          <HotelContactsTab
            hotelId={hotelId}
            onEnrich={() => setEnrichingHotelId(hotelId)}
            enrichingLive={enrichingLive}
            onEnrichComplete={() => {
              qc.invalidateQueries({ queryKey: ['hotel-contacts', hotelId] })
              setEnrichingHotelId(null)
            }}
          />
        )}
        {activeTab === 'edit' && <HotelEditTab hotel={hotel} hotelId={hotelId} />}
        {activeTab === 'sources' && <HotelSourcesTab hotel={hotel} />}
      </div>

      {/* Action Bar */}
      <div className="px-5 py-3 border-t border-slate-100 bg-slate-50/50 flex-shrink-0">
        <div className="flex items-center gap-2">
          {isNew && (
            <>
              <button onClick={() => setConfirmAction('approve')} disabled={approveMut.isPending}
                className="flex items-center gap-1.5 px-4 py-2 text-xs font-semibold rounded-lg bg-emerald-600 text-white hover:bg-emerald-700 transition disabled:opacity-50">
                {approveMut.isPending ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <CheckCircle2 className="w-3.5 h-3.5" />}
                Approve
              </button>
              <button onClick={() => setConfirmAction('reject')} disabled={rejectMut.isPending}
                className="flex items-center gap-1.5 px-4 py-2 text-xs font-semibold rounded-lg border border-stone-200 text-stone-600 hover:bg-stone-50 transition disabled:opacity-50">
                {rejectMut.isPending ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <XCircle className="w-3.5 h-3.5" />}
                Reject
              </button>
            </>
          )}
          {isApproved && (
            <button onClick={() => setConfirmAction('restore')} disabled={restoreMut.isPending}
              className="flex items-center gap-1.5 px-4 py-2 text-xs font-semibold rounded-lg border border-amber-200 text-amber-600 hover:bg-amber-50 transition disabled:opacity-50">
              {restoreMut.isPending ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Undo2 className="w-3.5 h-3.5" />}
              Back to Pipeline
            </button>
          )}
          {isRejected && (
            <button onClick={() => setConfirmAction('restore')} disabled={restoreMut.isPending}
              className="flex items-center gap-1.5 px-4 py-2 text-xs font-semibold rounded-lg border border-blue-200 text-blue-600 hover:bg-blue-50 transition disabled:opacity-50">
              {restoreMut.isPending ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Undo2 className="w-3.5 h-3.5" />}
              Restore
            </button>
          )}
        </div>
      </div>

      {/* Confirm Dialogs */}
      <ConfirmDialog
        open={confirmAction === 'approve'}
        variant="approve"
        title="Approve Hotel"
        message={`Push "${displayName}" to Insightly CRM?`}
        confirmLabel="Approve & Push"
        pending={approveMut.isPending}
        onConfirm={() => { approveMut.mutate(); setConfirmAction(null) }}
        onCancel={() => setConfirmAction(null)}
      />
      <ConfirmDialog
        open={confirmAction === 'reject'}
        variant="reject"
        title="Reject Hotel"
        message={`Move "${displayName}" to Rejected?`}
        confirmLabel="Reject"
        pending={rejectMut.isPending}
        onConfirm={() => { rejectMut.mutate(); setConfirmAction(null) }}
        onCancel={() => setConfirmAction(null)}
      />
      <ConfirmDialog
        open={confirmAction === 'restore'}
        variant="restore"
        title={isApproved ? 'Back to Pipeline' : 'Restore Hotel'}
        message={isApproved
          ? `Move "${displayName}" back to pipeline? This will delete from Insightly.`
          : `Restore "${displayName}" back to pipeline?`}
        confirmLabel={isApproved ? 'Remove from CRM' : 'Restore'}
        pending={restoreMut.isPending}
        onConfirm={() => { restoreMut.mutate(); setConfirmAction(null) }}
        onCancel={() => setConfirmAction(null)}
      />
    </div>
  )
}


/* ═══════════════════════════════════════════════════
   HOTEL DETAIL TABS (1B, 2026-04-28)
   ═══════════════════════════════════════════════════
   Mirror of LeadDetail's tab structure. Each tab is its own component
   that takes the hotel + relevant callbacks. We reuse:
     - EnrichProgress (with basePath="/api/existing-hotels")
     - SmartFillProgress (with basePath="/api/existing-hotels")
   Everything else (contact list, edit form, sources view) is built
   inline against the hotel-specific endpoints.
*/

function HotelOverviewTab({
  hotel, hotelId,
  onEnrich, enrichingLive, onEnrichComplete,
  onSmartFill, smartFillLive, onSmartFillComplete,
}: {
  hotel: Hotel; hotelId: number;
  onEnrich: () => void; enrichingLive: boolean; onEnrichComplete: () => void;
  onSmartFill: (mode: 'smart' | 'full') => void; smartFillLive: 'smart' | 'full' | null; onSmartFillComplete: () => void;
}) {
  const h = hotel as any  // post-018 fields not in legacy interface

  return (
    <>
      {/* Smart Fill progress — replaces the action buttons while running */}
      {smartFillLive && (
        <div className="mb-3">
          <SmartFillProgress
            leadId={hotelId}
            mode={smartFillLive}
            basePath="/api/existing-hotels"
            onComplete={onSmartFillComplete}
            onCancel={onSmartFillComplete}
          />
        </div>
      )}

      <Section title="Details">
        <div className="grid grid-cols-2 gap-4">
          <Field icon={MapPin} label="Location" value={[hotel.city, hotel.state].filter(Boolean).join(', ') || '—'} />
          <Field icon={Building2} label="Rooms" value={hotel.room_count ? `${hotel.room_count} rooms` : '—'} />
          {hotel.address && <Field icon={MapPin} label="Address" value={hotel.address} />}
          {(h.hotel_type || hotel.property_type) && (
            <Field icon={Building2} label="Type" value={h.hotel_type || hotel.property_type} />
          )}
          {hotel.zone && <Field icon={MapPin} label="Zone" value={hotel.zone} />}
          {h.management_company && <Field icon={Building2} label="Mgmt Co" value={h.management_company} />}
          {h.developer && <Field icon={Building2} label="Developer" value={h.developer} />}
          {h.owner && <Field icon={Building2} label="Owner" value={h.owner} />}
          {h.opening_date && <Field icon={MapPin} label="Opening" value={h.opening_date} />}
        </div>

        {/* Smart Fill action bar (hidden while running) */}
        {!smartFillLive && (
          <div className="flex items-center gap-2 mt-3 pt-3 border-t border-stone-100">
            <button
              onClick={() => onSmartFill('smart')}
              className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-semibold bg-violet-50 text-violet-700 border border-violet-200 rounded-md hover:bg-violet-100 transition"
            >
              Smart Fill
            </button>
            <button
              onClick={() => onSmartFill('full')}
              className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium text-stone-400 hover:text-violet-600 hover:bg-violet-50 rounded-md border border-dashed border-stone-200 hover:border-violet-300 transition"
            >
              Full Refresh
            </button>
          </div>
        )}
      </Section>

      {/* Run Enrichment Button + progress card */}
      <Section title="Contact Enrichment">
        {enrichingLive ? (
          <EnrichProgress
            leadId={hotelId}
            basePath="/api/existing-hotels"
            onComplete={onEnrichComplete}
            onCancel={onEnrichComplete}
          />
        ) : (
          <button
            onClick={onEnrich}
            className="w-full px-4 py-2.5 text-xs font-semibold bg-navy-900 text-white rounded-lg hover:bg-navy-800 transition"
          >
            Run Enrichment
          </button>
        )}
      </Section>

      {/* Revenue Potential */}
      {(hotel.revenue_opening || hotel.revenue_annual) && (
        <div className="bg-white border border-stone-200 rounded-lg">
          <div className="flex items-center gap-2 px-4 py-3 border-b border-stone-100">
            <div className="w-7 h-7 rounded-md bg-emerald-50 flex items-center justify-center">
              <DollarSign className="w-4 h-4 text-emerald-600" />
            </div>
            <div>
              <h3 className="text-sm font-semibold text-navy-900">Revenue Potential</h3>
              <p className="text-[10px] text-stone-400">{getTierLabel(hotel.brand_tier)} · {hotel.room_count} rooms</p>
            </div>
          </div>
          <div className="grid grid-cols-2 divide-x divide-stone-100 p-4">
            <div>
              <div className="text-[11px] font-semibold text-stone-500 uppercase tracking-wider mb-1">Annual Recurring</div>
              <div className="text-2xl font-bold text-navy-900">{fmtRevenue(hotel.revenue_annual)}</div>
            </div>
            <div className="pl-4">
              <div className="text-[11px] font-semibold text-stone-500 uppercase tracking-wider mb-1">Opening Order</div>
              <div className="text-2xl font-bold text-navy-900">{fmtRevenue(hotel.revenue_opening)}</div>
            </div>
          </div>
        </div>
      )}

      {/* Primary Contact (denormalized snapshot) */}
      <Section title="Primary Contact">
        {(h.contact_name || hotel.gm_name) ? (
          <div className="bg-slate-50 rounded-lg p-3.5 border border-slate-200/80">
            <div className="flex items-center gap-3">
              <div className="w-10 h-10 rounded-full bg-gradient-to-br from-navy-400 to-navy-600 flex items-center justify-center flex-shrink-0">
                <span className="text-white font-bold text-sm">{(h.contact_name || hotel.gm_name)[0]}</span>
              </div>
              <div className="min-w-0 flex-1">
                <p className="text-sm font-semibold text-navy-900">{h.contact_name || hotel.gm_name}</p>
                {(h.contact_title || hotel.gm_title) && (
                  <p className="text-xs text-stone-500">{h.contact_title || hotel.gm_title}</p>
                )}
              </div>
            </div>
            <div className="mt-2.5 space-y-1.5">
              {(h.contact_email || hotel.gm_email) && (
                <a href={`mailto:${h.contact_email || hotel.gm_email}`} className="flex items-center gap-1.5 text-xs text-navy-600 hover:underline">
                  <Mail className="w-3.5 h-3.5" /> {h.contact_email || hotel.gm_email}
                </a>
              )}
              {(h.contact_phone || hotel.gm_phone) && (
                <a href={`tel:${h.contact_phone || hotel.gm_phone}`} className="flex items-center gap-1.5 text-xs text-navy-600 hover:underline">
                  <Phone className="w-3.5 h-3.5" /> {h.contact_phone || hotel.gm_phone}
                </a>
              )}
            </div>
          </div>
        ) : (
          <p className="text-xs text-stone-400">No primary contact yet — run enrichment to find one</p>
        )}
      </Section>

      {/* Website */}
      {(h.hotel_website || hotel.website) && (
        <Section title="Website">
          <a
            href={(h.hotel_website || hotel.website).startsWith('http') ? (h.hotel_website || hotel.website) : `https://${h.hotel_website || hotel.website}`}
            target="_blank" rel="noopener noreferrer"
            className="flex items-center gap-2 text-sm text-navy-600 hover:text-navy-800 hover:underline transition"
          >
            <Globe className="w-4 h-4" /> {h.hotel_website || hotel.website}
          </a>
        </Section>
      )}

      {/* SAP Info */}
      {hotel.is_client && hotel.sap_bp_code && (
        <Section title="Client Info">
          <div className="space-y-2 text-sm">
            <div className="flex justify-between">
              <span className="text-stone-400 font-medium">SAP Code</span>
              <span className="text-navy-700 font-semibold">{hotel.sap_bp_code}</span>
            </div>
          </div>
        </Section>
      )}

      {h.description && (
        <Section title="Description">
          <p className="text-sm text-stone-600 leading-relaxed whitespace-pre-line">{h.description}</p>
        </Section>
      )}
    </>
  )
}


function HotelContactsTab({
  hotelId, onEnrich, enrichingLive, onEnrichComplete,
}: {
  hotelId: number;
  onEnrich: () => void;
  enrichingLive: boolean;
  onEnrichComplete: () => void;
}) {
  const qc = useQueryClient()

  // Fetch contacts attached to this existing hotel via the dual-FK
  // endpoint added in Path Y. Same priority sorting as the lead version.
  const { data: contacts, isLoading } = useQuery<any[]>({
    queryKey: ['hotel-contacts', hotelId],
    queryFn: async () => (await api.get(`/api/existing-hotels/${hotelId}/contacts`)).data,
  })

  async function handleSave(contactId: number) {
    await api.post(`/api/existing-hotels/${hotelId}/contacts/${contactId}/save`)
    qc.invalidateQueries({ queryKey: ['hotel-contacts', hotelId] })
  }
  async function handleSetPrimary(contactId: number) {
    await api.post(`/api/existing-hotels/${hotelId}/contacts/${contactId}/set-primary`)
    qc.invalidateQueries({ queryKey: ['hotel-contacts', hotelId] })
    qc.invalidateQueries({ queryKey: ['existing-hotel', hotelId] })
  }
  async function handleDelete(contactId: number) {
    if (!confirm('Delete this contact?')) return
    await api.delete(`/api/existing-hotels/${hotelId}/contacts/${contactId}`)
    qc.invalidateQueries({ queryKey: ['hotel-contacts', hotelId] })
  }

  return (
    <>
      {/* Run Enrichment progress card — same component LeadDetail uses */}
      {enrichingLive && (
        <div className="mb-4">
          <EnrichProgress
            leadId={hotelId}
            basePath="/api/existing-hotels"
            onComplete={onEnrichComplete}
            onCancel={onEnrichComplete}
          />
        </div>
      )}

      {isLoading ? (
        <div className="space-y-2">
          {Array.from({ length: 3 }).map((_, i) => <div key={i} className="skeleton h-24 rounded-lg" />)}
        </div>
      ) : !contacts || contacts.length === 0 ? (
        !enrichingLive && (
          <div className="text-center py-12">
            <User className="w-12 h-12 text-stone-300 mx-auto mb-3" />
            <p className="text-sm font-medium text-stone-500">No contacts yet</p>
            <button
              onClick={onEnrich}
              className="mt-3 px-5 py-2.5 text-xs font-semibold bg-navy-900 text-white rounded-lg hover:bg-navy-800 transition"
            >
              Run Enrichment
            </button>
          </div>
        )
      ) : (
        <div className="space-y-2.5">
          {contacts.map((c) => (
            <div
              key={c.id}
              className={cn(
                'rounded-lg border p-4 transition',
                c.is_primary ? 'border-navy-200 bg-navy-50/30' : 'border-stone-200 bg-white',
              )}
            >
              <div className="flex items-start justify-between gap-2">
                <div className="min-w-0 flex-1">
                  <div className="flex items-center gap-2">
                    <h4 className="text-sm font-semibold text-navy-900 truncate">{c.name}</h4>
                    {c.is_primary && (
                      <span className="text-[10px] font-bold px-1.5 py-0.5 bg-navy-600 text-white rounded">PRIMARY</span>
                    )}
                    {c.priority_label && (
                      <span className="text-[10px] font-bold px-1.5 py-0.5 bg-emerald-50 text-emerald-700 rounded">{c.priority_label}</span>
                    )}
                  </div>
                  {c.title && <p className="text-xs text-stone-500 mt-0.5">{c.title}</p>}
                  {c.organization && <p className="text-xs text-stone-400 mt-0.5">{c.organization}</p>}
                </div>
                <div className="flex items-center gap-1">
                  {!c.is_primary && (
                    <ActionBtn onClick={() => handleSetPrimary(c.id)} color="amber" title="Set primary">
                      <CheckCircle2 className="w-3.5 h-3.5" />
                    </ActionBtn>
                  )}
                  {!c.is_saved && (
                    <ActionBtn onClick={() => handleSave(c.id)} color="emerald" title="Save contact">
                      <CheckCircle2 className="w-3.5 h-3.5" />
                    </ActionBtn>
                  )}
                  <ActionBtn onClick={() => handleDelete(c.id)} color="red" title="Delete">
                    <X className="w-3.5 h-3.5" />
                  </ActionBtn>
                </div>
              </div>
              <div className="mt-2 space-y-1">
                {c.email && (
                  <a href={`mailto:${c.email}`} className="flex items-center gap-1.5 text-xs text-navy-600 hover:underline">
                    <Mail className="w-3 h-3" /> {c.email}
                  </a>
                )}
                {c.phone && (
                  <a href={`tel:${c.phone}`} className="flex items-center gap-1.5 text-xs text-navy-600 hover:underline">
                    <Phone className="w-3 h-3" /> {c.phone}
                  </a>
                )}
                {c.linkedin && (
                  <a href={c.linkedin} target="_blank" rel="noopener noreferrer" className="inline-flex items-center gap-1.5 px-2 py-0.5 text-xs font-medium text-blue-700 bg-blue-50 rounded hover:bg-blue-100">
                    <Linkedin className="w-3 h-3" /> LinkedIn
                  </a>
                )}
              </div>
              {c.strategist_reasoning && (
                <p className="text-[10px] text-stone-500 mt-2 italic leading-snug">{c.strategist_reasoning}</p>
              )}
            </div>
          ))}
        </div>
      )}
    </>
  )
}


function HotelEditTab({ hotel, hotelId }: { hotel: Hotel; hotelId: number }) {
  const qc = useQueryClient()
  const h = hotel as any
  const [form, setForm] = useState<Record<string, any>>({
    hotel_name: h.hotel_name || hotel.name || '',
    brand: hotel.brand || '',
    chain: hotel.chain || '',
    brand_tier: hotel.brand_tier || '',
    address: hotel.address || '',
    city: hotel.city || '',
    state: hotel.state || '',
    country: hotel.country || 'USA',
    zip_code: hotel.zip_code || '',
    room_count: hotel.room_count ?? '',
    hotel_website: h.hotel_website || hotel.website || '',
    hotel_type: h.hotel_type || hotel.property_type || '',
    management_company: h.management_company || '',
    developer: h.developer || '',
    owner: h.owner || '',
    opening_date: h.opening_date || '',
    contact_name: h.contact_name || hotel.gm_name || '',
    contact_title: h.contact_title || hotel.gm_title || '',
    contact_email: h.contact_email || hotel.gm_email || '',
    contact_phone: h.contact_phone || hotel.gm_phone || '',
    zone: hotel.zone || '',
  })
  const [saving, setSaving] = useState(false)

  async function handleSave() {
    setSaving(true)
    try {
      const payload: any = {}
      Object.entries(form).forEach(([k, v]) => {
        if (v !== '') payload[k] = (k === 'room_count' && v !== '') ? Number(v) : v
      })
      await api.patch(`/api/existing-hotels/${hotelId}`, payload)
      qc.invalidateQueries({ queryKey: ['existing-hotel', hotelId] })
      qc.invalidateQueries({ queryKey: ['existing-hotels'] })
    } finally {
      setSaving(false)
    }
  }

  const fields: { key: string; label: string; type?: string }[] = [
    { key: 'hotel_name', label: 'Hotel Name' },
    { key: 'brand', label: 'Brand' },
    { key: 'chain', label: 'Chain' },
    { key: 'brand_tier', label: 'Brand Tier' },
    { key: 'hotel_type', label: 'Type' },
    { key: 'address', label: 'Address' },
    { key: 'city', label: 'City' },
    { key: 'state', label: 'State' },
    { key: 'country', label: 'Country' },
    { key: 'zip_code', label: 'Zip Code' },
    { key: 'room_count', label: 'Room Count', type: 'number' },
    { key: 'hotel_website', label: 'Website' },
    { key: 'management_company', label: 'Management Co' },
    { key: 'developer', label: 'Developer' },
    { key: 'owner', label: 'Owner' },
    { key: 'opening_date', label: 'Opening Date' },
    { key: 'zone', label: 'Zone' },
    { key: 'contact_name', label: 'Contact Name' },
    { key: 'contact_title', label: 'Contact Title' },
    { key: 'contact_email', label: 'Contact Email' },
    { key: 'contact_phone', label: 'Contact Phone' },
  ]

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-2 gap-3">
        {fields.map((f) => (
          <div key={f.key}>
            <label className="text-[10px] font-semibold text-stone-500 uppercase tracking-wider">{f.label}</label>
            <input
              type={f.type || 'text'}
              value={form[f.key] ?? ''}
              onChange={(e) => setForm({ ...form, [f.key]: e.target.value })}
              className="mt-1 w-full px-2.5 py-1.5 text-sm border border-stone-200 rounded-md focus:border-navy-400 focus:outline-none focus:ring-1 focus:ring-navy-400/20"
            />
          </div>
        ))}
      </div>
      <button
        onClick={handleSave}
        disabled={saving}
        className="w-full px-4 py-2.5 text-xs font-semibold bg-navy-900 text-white rounded-lg hover:bg-navy-800 transition disabled:opacity-50"
      >
        {saving ? <Loader2 className="w-3.5 h-3.5 animate-spin inline" /> : 'Save Changes'}
      </button>
    </div>
  )
}


function HotelSourcesTab({ hotel }: { hotel: Hotel }) {
  const h = hotel as any
  const sources: string[] = h.source_urls || (h.source_url ? [h.source_url] : [])

  return (
    <div className="space-y-3">
      <div className="space-y-2">
        <div className="flex justify-between text-xs">
          <span className="text-stone-400">Data source</span>
          <span className="text-navy-700 font-semibold capitalize">{h.data_source || '—'}</span>
        </div>
        <div className="flex justify-between text-xs">
          <span className="text-stone-400">Status</span>
          <span className="text-navy-700 font-semibold capitalize">{hotel.status || '—'}</span>
        </div>
        <div className="flex justify-between text-xs">
          <span className="text-stone-400">Last verified</span>
          <span className="text-navy-700 font-semibold">{h.last_verified_at ? new Date(h.last_verified_at).toLocaleDateString() : '—'}</span>
        </div>
        {hotel.insightly_id && (
          <div className="flex justify-between text-xs">
            <span className="text-stone-400">Insightly</span>
            <span className="text-navy-700 font-semibold">#{hotel.insightly_id}</span>
          </div>
        )}
      </div>

      {sources.length > 0 && (
        <Section title="Source URLs">
          <ul className="space-y-1.5">
            {sources.map((url, i) => (
              <li key={i}>
                <a href={url} target="_blank" rel="noopener noreferrer" className="flex items-center gap-1.5 text-xs text-navy-600 hover:underline truncate">
                  <ExternalLink className="w-3 h-3 flex-shrink-0" />
                  <span className="truncate">{url}</span>
                </a>
              </li>
            ))}
          </ul>
        </Section>
      )}
    </div>
  )
}


/* ═══════════════════════════════════════════════════
   SHARED COMPONENTS
   ═══════════════════════════════════════════════════ */

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <section>
      <h4 className="section-label">{title}</h4>
      {children}
    </section>
  )
}

function Field({ icon: Icon, label, value }: { icon: React.ElementType; label: string; value: string }) {
  return (
    <div className="flex items-start gap-2.5">
      <Icon className="w-4 h-4 text-stone-400 mt-0.5 flex-shrink-0" />
      <div className="min-w-0">
        <div className="field-label">{label}</div>
        <div className="text-sm text-navy-800 leading-snug font-medium">{value}</div>
      </div>
    </div>
  )
}

function ActionBtn({ onClick, color, title, children }: {
  onClick: (e: React.MouseEvent) => void; color: string; title: string; children: React.ReactNode
}) {
  const colors: Record<string, string> = {
    emerald: 'hover:bg-emerald-50 text-emerald-500 hover:text-emerald-700',
    red: 'hover:bg-red-50 text-red-400 hover:text-red-600',
    blue: 'hover:bg-blue-50 text-blue-500 hover:text-blue-700',
    amber: 'hover:bg-amber-50 text-amber-500 hover:text-amber-700',
  }
  return (
    <button onClick={onClick} title={title} className={cn('p-1.5 rounded-md transition-all duration-100', colors[color] || '')}>
      {children}
    </button>
  )
}
