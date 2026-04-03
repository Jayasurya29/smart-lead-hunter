import { useState, useMemo } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import api from '@/api/client'
import ConfirmDialog from '@/components/ui/ConfirmDialog'
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
  zones: { zone: string; count: number }[]
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

          {filters.state && filters.state.toLowerCase().includes('florida') && (
            <select value={filters.zone} onChange={(e) => handleFilterChange('zone', e.target.value)} className="h-9 px-3 text-sm bg-white border border-stone-200 rounded-lg outline-none focus:border-navy-400">
              <option value="">All Zones</option>
              {(stats?.zones || []).filter(z => !['Out of State', 'Unknown', 'Junk'].includes(z.zone)).map(z => (
                <option key={z.zone} value={z.zone}>{z.zone} ({z.count})</option>
              ))}
            </select>
          )}

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
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['existing-hotels'] }); qc.invalidateQueries({ queryKey: ['existing-hotels-stats'] }) },
  })
  const rejectMut = useMutation({
    mutationFn: (id: number) => api.post(`/api/existing-hotels/${id}/reject`, {}),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['existing-hotels'] }); qc.invalidateQueries({ queryKey: ['existing-hotels-stats'] }) },
  })
  const restoreMut = useMutation({
    mutationFn: (id: number) => api.post(`/api/existing-hotels/${id}/restore`),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['existing-hotels'] }); qc.invalidateQueries({ queryKey: ['existing-hotels-stats'] }) },
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
              <th className="px-3 py-2.5 text-left text-[11px] font-bold text-slate-400 uppercase tracking-wider">Hotel</th>
              <th className="px-3 py-2.5 text-left text-[11px] font-bold text-slate-400 uppercase tracking-wider w-16">Tier</th>
              <th className="px-3 py-2.5 text-left text-[11px] font-bold text-slate-400 uppercase tracking-wider">Location</th>
              <th className="px-3 py-2.5 text-left text-[11px] font-bold text-slate-400 uppercase tracking-wider w-28">Zone</th>
              <th className="px-3 py-2.5 text-left text-[11px] font-bold text-slate-400 uppercase tracking-wider w-16">Rooms</th>
              <th className="px-3 py-2.5 text-left text-[11px] font-bold text-slate-400 uppercase tracking-wider w-24">Potential</th>
              <th className="px-3 py-2.5 text-left text-[11px] font-bold text-slate-400 uppercase tracking-wider w-20">Type</th>
              <th className="px-3 py-2.5 text-left text-[11px] font-bold text-slate-400 uppercase tracking-wider w-20">Contact</th>
              <th className="px-3 py-2.5 w-24" />
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-100/80">
            {hotels.map((hotel) => (
              <tr
                key={hotel.id}
                onClick={() => onSelect(hotel.id)}
                className={cn(
                  'lead-row cursor-pointer',
                  selectedId === hotel.id && 'active',
                )}
              >
                <td className="px-3 py-2.5 max-w-[280px]">
                  <div className="truncate text-[15px] font-bold text-navy-950 leading-snug">{hotel.name}</div>
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
                  <span className="text-sm text-navy-800 font-medium">{hotel.room_count || '—'}</span>
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
                <td className="px-3 py-2.5">
                  {hotel.gm_name ? (
                    <span className="text-xs text-navy-600 font-medium truncate block max-w-[120px]">{hotel.gm_name}</span>
                  ) : (
                    <span className="text-xs text-stone-300">—</span>
                  )}
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
            ))}
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

  return (
    <div className="h-full flex flex-col bg-white animate-slideIn">
      {/* Header */}
      <div className="px-5 pt-5 pb-3 flex-shrink-0 border-b border-slate-100 bg-gradient-to-b from-slate-50/50 to-white">
        <div className="flex items-start justify-between gap-3">
          <div className="min-w-0 flex-1">
            <h2 className="text-lg font-bold text-navy-900 leading-snug truncate">{hotel.name}</h2>
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

      {/* Content */}
      <div className="flex-1 overflow-y-auto p-5 space-y-5">
        {/* Details */}
        <Section title="Details">
          <div className="grid grid-cols-2 gap-4">
            <Field icon={MapPin} label="Location" value={[hotel.city, hotel.state].filter(Boolean).join(', ') || '—'} />
            <Field icon={Building2} label="Rooms" value={hotel.room_count ? `${hotel.room_count} rooms` : '—'} />
            {hotel.address && <Field icon={MapPin} label="Address" value={hotel.address} />}
            {hotel.property_type && <Field icon={Building2} label="Type" value={hotel.property_type} />}
            {hotel.zone && <Field icon={MapPin} label="Zone" value={hotel.zone} />}
          </div>
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
                <p className="text-[10px] text-stone-400 mt-0.5">Yearly garment spend</p>
              </div>
              <div className="pl-4">
                <div className="text-[11px] font-semibold text-stone-500 uppercase tracking-wider mb-1">Opening Order</div>
                <div className="text-2xl font-bold text-navy-900">{fmtRevenue(hotel.revenue_opening)}</div>
                <p className="text-[10px] text-stone-400 mt-0.5">If rebrand / full refit</p>
              </div>
            </div>
          </div>
        )}

        {/* Contact */}
        <Section title="Contact">
          {hotel.gm_name ? (
            <div className="bg-slate-50 rounded-lg p-3.5 border border-slate-200/80">
              <div className="flex items-center gap-3">
                <div className="w-10 h-10 rounded-full bg-gradient-to-br from-navy-400 to-navy-600 flex items-center justify-center flex-shrink-0">
                  <span className="text-white font-bold text-sm">{hotel.gm_name[0]}</span>
                </div>
                <div className="min-w-0 flex-1">
                  <p className="text-sm font-semibold text-navy-900">{hotel.gm_name}</p>
                  {hotel.gm_title && <p className="text-xs text-stone-500">{hotel.gm_title}</p>}
                </div>
              </div>
              <div className="mt-2.5 space-y-1.5">
                {hotel.gm_email && (
                  <a href={`mailto:${hotel.gm_email}`} className="flex items-center gap-1.5 text-xs text-navy-600 hover:underline">
                    <Mail className="w-3.5 h-3.5" /> {hotel.gm_email}
                  </a>
                )}
                {hotel.gm_phone && (
                  <a href={`tel:${hotel.gm_phone}`} className="flex items-center gap-1.5 text-xs text-navy-600 hover:underline">
                    <Phone className="w-3.5 h-3.5" /> {hotel.gm_phone}
                  </a>
                )}
                {hotel.gm_linkedin && (
                  <a href={hotel.gm_linkedin} target="_blank" rel="noopener noreferrer" className="inline-flex items-center gap-1.5 px-2.5 py-1 text-xs font-medium text-blue-700 bg-blue-50 rounded-md hover:bg-blue-100 transition">
                    <Linkedin className="w-3.5 h-3.5" /> LinkedIn
                  </a>
                )}
              </div>
            </div>
          ) : (
            <p className="text-xs text-stone-400">No contact information yet</p>
          )}
        </Section>

        {/* Website */}
        {hotel.website && (
          <Section title="Website">
            <a
              href={hotel.website.startsWith('http') ? hotel.website : `https://${hotel.website}`}
              target="_blank" rel="noopener noreferrer"
              className="flex items-center gap-2 text-sm text-navy-600 hover:text-navy-800 hover:underline transition"
            >
              <Globe className="w-4 h-4" /> {hotel.website}
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

        {/* Metadata */}
        <Section title="Metadata">
          <div className="space-y-2 text-sm">
            <div className="flex justify-between">
              <span className="text-stone-400 font-medium">Hotel ID</span>
              <span className="text-navy-700 font-semibold">{hotel.id}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-stone-400 font-medium">Data Source</span>
              <span className="text-navy-700 font-semibold capitalize">{hotel.data_source || '—'}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-stone-400 font-medium">Status</span>
              <span className="text-navy-700 font-semibold capitalize">{hotel.status || '—'}</span>
            </div>
            {hotel.insightly_id && (
              <div className="flex justify-between">
                <span className="text-stone-400 font-medium">Insightly</span>
                <span className="text-navy-700 font-semibold">#{hotel.insightly_id}</span>
              </div>
            )}
          </div>
        </Section>
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
        message={`Push "${hotel.name}" to Insightly CRM?`}
        confirmLabel="Approve & Push"
        pending={approveMut.isPending}
        onConfirm={() => { approveMut.mutate(); setConfirmAction(null) }}
        onCancel={() => setConfirmAction(null)}
      />
      <ConfirmDialog
        open={confirmAction === 'reject'}
        variant="reject"
        title="Reject Hotel"
        message={`Move "${hotel.name}" to Rejected?`}
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
          ? `Move "${hotel.name}" back to pipeline? This will delete from Insightly.`
          : `Restore "${hotel.name}" back to pipeline?`}
        confirmLabel={isApproved ? 'Remove from CRM' : 'Restore'}
        pending={restoreMut.isPending}
        onConfirm={() => { restoreMut.mutate(); setConfirmAction(null) }}
        onCancel={() => setConfirmAction(null)}
      />
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
