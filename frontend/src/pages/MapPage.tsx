import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import api from '@/api/client'
import {
  cn, getTierShort, getTierColor, getTierLabel,
} from '@/lib/utils'
import {
  Building2, MapPin, Users, Eye, Search, X, ChevronLeft, ChevronRight,
  Phone, Globe, User, ExternalLink, Pencil, Check, Loader2, Download,
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
}

interface Filters {
  search: string
  state: string
  brand_tier: string
  is_client: string
  sort: string
}

const DEFAULT_FILTERS: Filters = {
  search: '',
  state: '',
  brand_tier: '',
  is_client: '',
  sort: 'name_az',
}

/* ═══════════════════════════════════════════════════
   MAIN PAGE
   ═══════════════════════════════════════════════════ */

export default function MapPage() {
  const [page, setPage] = useState(1)
  const [filters, setFilters] = useState<Filters>(DEFAULT_FILTERS)
  const [selectedId, setSelectedId] = useState<number | null>(null)

  const { data: stats } = useQuery<HotelStats>({
    queryKey: ['existing-hotels-stats'],
    queryFn: async () => (await api.get('/api/existing-hotels/stats')).data,
  })

  const { data, isLoading } = useQuery({
    queryKey: ['existing-hotels', page, filters],
    queryFn: async () => {
      const params: Record<string, string> = {
        page: String(page),
        per_page: '25',
        sort: filters.sort,
      }
      if (filters.search) params.search = filters.search
      if (filters.state) params.state = filters.state
      if (filters.brand_tier) params.brand_tier = filters.brand_tier
      if (filters.is_client) params.is_client = filters.is_client
      const { data } = await api.get('/api/existing-hotels', { params })
      return data as { hotels: Hotel[]; total: number; page: number; pages: number }
    },
  })

  function handleFilterChange(key: string, value: string) {
    setFilters(prev => ({ ...prev, [key]: value }))
    setPage(1)
  }

  function handleStatClick(clientFilter: string) {
    setFilters({ ...DEFAULT_FILTERS, is_client: clientFilter })
    setPage(1)
  }

  const hotels = data?.hotels || []
  const total = data?.total || 0
  const totalPages = data?.pages || 1

  return (
    <div className="h-full flex flex-col">
      {/* Stats Cards */}
      <div className="px-4 pt-3 pb-2 flex-shrink-0">
        <div className="grid grid-cols-4 lg:grid-cols-7 gap-2.5">
          <StatCard
            label="Total Hotels"
            value={stats?.total || 0}
            icon={Building2}
            bg="bg-navy-50"
            text="text-navy-600"
            onClick={() => handleStatClick('')}
            active={!filters.is_client}
          />
          <StatCard
            label="Clients"
            value={stats?.clients || 0}
            icon={Users}
            bg="bg-emerald-50"
            text="text-emerald-600"
            onClick={() => handleStatClick('true')}
            active={filters.is_client === 'true'}
          />
          <StatCard
            label="Prospects"
            value={stats?.prospects || 0}
            icon={Eye}
            bg="bg-coral-50"
            text="text-coral-500"
            onClick={() => handleStatClick('false')}
            active={filters.is_client === 'false'}
          />
          <StatCard
            label="Geocoded"
            value={stats?.geocoded || 0}
            icon={MapPin}
            bg="bg-sky-50"
            text="text-sky-600"
          />
          <StatCard
            label="With Contact"
            value={stats?.with_contact || 0}
            icon={User}
            bg="bg-violet-50"
            text="text-violet-600"
          />
          <StatCard
            label="With Tier"
            value={stats?.with_tier || 0}
            icon={Building2}
            bg="bg-gold-50"
            text="text-gold-600"
          />
          <StatCard
            label="On Map"
            value={stats?.on_map || 0}
            icon={MapPin}
            bg="bg-stone-100"
            text="text-stone-500"
          />
        </div>
      </div>

      {/* Filters */}
      <div className="px-4 pb-2 flex-shrink-0">
        <div className="flex items-center gap-3">
          <div className="relative flex-1 max-w-md">
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

          <select
            value={filters.state}
            onChange={(e) => handleFilterChange('state', e.target.value)}
            className="h-9 px-3 text-sm bg-white border border-stone-200 rounded-lg outline-none focus:border-navy-400"
          >
            <option value="">All States</option>
            {(stats?.top_states || []).map(s => (
              <option key={s.state} value={s.state}>{s.state} ({s.count})</option>
            ))}
          </select>

          <select
            value={filters.brand_tier}
            onChange={(e) => handleFilterChange('brand_tier', e.target.value)}
            className="h-9 px-3 text-sm bg-white border border-stone-200 rounded-lg outline-none focus:border-navy-400"
          >
            <option value="">All Tiers</option>
            <option value="tier1_ultra_luxury">T1 — Ultra Luxury</option>
            <option value="tier2_luxury">T2 — Luxury</option>
            <option value="tier3_upper_upscale">T3 — Upper Upscale</option>
            <option value="tier4_upscale">T4 — Upscale</option>
          </select>

          <select
            value={filters.is_client}
            onChange={(e) => handleFilterChange('is_client', e.target.value)}
            className="h-9 px-3 text-sm bg-white border border-stone-200 rounded-lg outline-none focus:border-navy-400"
          >
            <option value="">All Hotels</option>
            <option value="true">Clients Only</option>
            <option value="false">Prospects Only</option>
          </select>

          {Object.values(filters).some(v => v) && (
            <button
              onClick={() => { setFilters(DEFAULT_FILTERS); setPage(1) }}
              className="h-9 px-3 text-xs font-semibold text-red-500 bg-red-50 border border-red-200 rounded-lg hover:bg-red-100 transition"
            >
              <X className="w-3.5 h-3.5 inline mr-1" />
              Clear
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
            selectedId={selectedId}
            onSelect={setSelectedId}
            onPageChange={setPage}
            isLoading={isLoading}
          />
        </div>

        {selectedId && (
          <div className="flex-[2] bg-white rounded-xl border border-slate-200/80 shadow-sm overflow-hidden animate-slideIn">
            <HotelDetail hotelId={selectedId} onClose={() => setSelectedId(null)} />
          </div>
        )}
      </div>
    </div>
  )
}


/* ═══════════════════════════════════════════════════
   STAT CARD
   ═══════════════════════════════════════════════════ */

function StatCard({ label, value, icon: Icon, bg, text, onClick, active }: {
  label: string; value: number; icon: React.ElementType; bg: string; text: string
  onClick?: () => void; active?: boolean
}) {
  return (
    <div
      onClick={onClick}
      className={cn(
        'rounded-lg border px-3 py-2.5 flex items-center gap-2.5 select-none',
        onClick ? 'cursor-pointer hover:shadow-sm' : '',
        active ? `${bg} ring-2 ring-navy-400 shadow-md` : 'bg-white border-stone-200',
      )}
    >
      <div className={cn('w-8 h-8 rounded-lg flex items-center justify-center flex-shrink-0', bg)}>
        <Icon className={cn('w-4 h-4', text)} />
      </div>
      <div className="min-w-0">
        <div className="text-lg font-bold text-navy-900 leading-tight tabular-nums">{value}</div>
        <div className="text-2xs text-stone-400 font-semibold uppercase tracking-wider">{label}</div>
      </div>
    </div>
  )
}


/* ═══════════════════════════════════════════════════
   HOTEL TABLE
   ═══════════════════════════════════════════════════ */

function HotelTable({ hotels, total, page, totalPages, selectedId, onSelect, onPageChange, isLoading }: {
  hotels: Hotel[]; total: number; page: number; totalPages: number
  selectedId: number | null; onSelect: (id: number) => void
  onPageChange: (p: number) => void; isLoading: boolean
}) {
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
        <p className="text-sm font-medium">No hotels found</p>
      </div>
    )
  }

  return (
    <div className="flex flex-col h-full">
      <div className="flex-1 overflow-y-auto">
        <table className="w-full">
          <thead className="sticky top-0 z-10">
            <tr className="bg-slate-50/90 backdrop-blur-sm border-b border-slate-100">
              <th className="px-3 py-2.5 text-left text-[11px] font-bold text-slate-400 uppercase tracking-wider">Hotel</th>
              <th className="px-3 py-2.5 text-left text-[11px] font-bold text-slate-400 uppercase tracking-wider w-16">Tier</th>
              <th className="px-3 py-2.5 text-left text-[11px] font-bold text-slate-400 uppercase tracking-wider">Location</th>
              <th className="px-3 py-2.5 text-left text-[11px] font-bold text-slate-400 uppercase tracking-wider w-16">Rooms</th>
              <th className="px-3 py-2.5 text-left text-[11px] font-bold text-slate-400 uppercase tracking-wider w-20">Status</th>
              <th className="px-3 py-2.5 text-left text-[11px] font-bold text-slate-400 uppercase tracking-wider w-20">Contact</th>
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
                  <div className="truncate text-[15px] font-bold text-navy-950 leading-snug">
                    {hotel.name}
                  </div>
                  {hotel.brand && (
                    <div className="truncate text-xs text-stone-400 leading-snug">{hotel.brand}</div>
                  )}
                </td>
                <td className="px-3 py-2.5">
                  {hotel.brand_tier ? (
                    <span className={cn('inline-flex px-2 py-0.5 rounded text-2xs font-bold', getTierColor(hotel.brand_tier))}>
                      {getTierShort(hotel.brand_tier)}
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
                  <span className="text-sm text-navy-800 font-medium">
                    {hotel.room_count || '—'}
                  </span>
                </td>
                <td className="px-3 py-2.5">
                  <span className={cn(
                    'inline-flex px-2 py-0.5 rounded-full text-2xs font-bold',
                    hotel.is_client
                      ? 'bg-emerald-50 text-emerald-600'
                      : 'bg-amber-50 text-amber-600',
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
            <button
              onClick={() => onPageChange(page - 1)}
              disabled={page <= 1}
              className="p-1.5 rounded hover:bg-stone-100 disabled:opacity-30 disabled:cursor-not-allowed transition"
            >
              <ChevronLeft className="w-4 h-4 text-stone-500" />
            </button>
            {Array.from({ length: Math.min(totalPages, 7) }).map((_, i) => {
              let pageNum: number
              if (totalPages <= 7) pageNum = i + 1
              else if (page <= 4) pageNum = i + 1
              else if (page >= totalPages - 3) pageNum = totalPages - 6 + i
              else pageNum = page - 3 + i
              return (
                <button
                  key={pageNum}
                  onClick={() => onPageChange(pageNum)}
                  className={cn(
                    'w-8 h-8 rounded text-xs font-semibold transition',
                    page === pageNum ? 'bg-navy-900 text-white' : 'text-stone-500 hover:bg-stone-100',
                  )}
                >
                  {pageNum}
                </button>
              )
            })}
            <button
              onClick={() => onPageChange(page + 1)}
              disabled={page >= totalPages}
              className="p-1.5 rounded hover:bg-stone-100 disabled:opacity-30 disabled:cursor-not-allowed transition"
            >
              <ChevronRight className="w-4 h-4 text-stone-500" />
            </button>
          </div>
        </div>
      )}
    </div>
  )
}


/* ═══════════════════════════════════════════════════
   HOTEL DETAIL PANEL
   ═══════════════════════════════════════════════════ */

function HotelDetail({ hotelId, onClose }: { hotelId: number; onClose: () => void }) {
  const { data: hotel, isLoading } = useQuery<Hotel>({
    queryKey: ['existing-hotel', hotelId],
    queryFn: async () => (await api.get(`/api/existing-hotels/${hotelId}`)).data,
  })

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
              {getTierShort(hotel.brand_tier)} — {getTierLabel(hotel.brand_tier)}
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
      <div className="flex-1 overflow-y-auto p-5 space-y-6">
        {/* Location */}
        <Section title="Location">
          <div className="space-y-2">
            {hotel.address && (
              <div className="flex items-start gap-2.5">
                <MapPin className="w-4 h-4 text-stone-400 mt-0.5 flex-shrink-0" />
                <span className="text-sm text-navy-800 font-medium">{hotel.address}</span>
              </div>
            )}
            <div className="flex items-start gap-2.5">
              <MapPin className="w-4 h-4 text-stone-400 mt-0.5 flex-shrink-0" />
              <span className="text-sm text-navy-800 font-medium">
                {[hotel.city, hotel.state, hotel.zip_code].filter(Boolean).join(', ')}
              </span>
            </div>
            {hotel.latitude && hotel.longitude && (
              <div className="text-xs text-stone-400">
                {hotel.latitude.toFixed(4)}, {hotel.longitude.toFixed(4)}
              </div>
            )}
          </div>
        </Section>

        {/* Property Details */}
        <Section title="Property">
          <div className="grid grid-cols-2 gap-4">
            <Field label="Rooms" value={hotel.room_count ? String(hotel.room_count) : '—'} />
            <Field label="Type" value={hotel.property_type || '—'} />
            {hotel.phone && <Field label="Phone" value={hotel.phone} />}
            {hotel.website && (
              <div className="col-span-2">
                <div className="field-label">Website</div>
                <a
                  href={hotel.website.startsWith('http') ? hotel.website : `https://${hotel.website}`}
                  target="_blank" rel="noopener noreferrer"
                  className="text-sm text-navy-600 hover:underline flex items-center gap-1"
                >
                  <Globe className="w-3.5 h-3.5" /> {hotel.website}
                </a>
              </div>
            )}
          </div>
        </Section>

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
              <div className="mt-2 space-y-1">
                {hotel.gm_email && (
                  <a href={`mailto:${hotel.gm_email}`} className="flex items-center gap-1.5 text-xs text-navy-600 hover:underline">
                    <User className="w-3.5 h-3.5" /> {hotel.gm_email}
                  </a>
                )}
                {hotel.gm_phone && (
                  <a href={`tel:${hotel.gm_phone}`} className="flex items-center gap-1.5 text-xs text-navy-600 hover:underline">
                    <Phone className="w-3.5 h-3.5" /> {hotel.gm_phone}
                  </a>
                )}
              </div>
            </div>
          ) : (
            <p className="text-xs text-stone-400">No contact information yet</p>
          )}
        </Section>

        {/* SAP Info */}
        {hotel.is_client && (
          <Section title="Client Info">
            <div className="space-y-2 text-sm">
              {hotel.sap_bp_code && (
                <div className="flex justify-between">
                  <span className="text-stone-400 font-medium">SAP Code</span>
                  <span className="text-navy-700 font-semibold">{hotel.sap_bp_code}</span>
                </div>
              )}
              <div className="flex justify-between">
                <span className="text-stone-400 font-medium">Source</span>
                <span className="text-navy-700 font-semibold capitalize">{hotel.data_source || '—'}</span>
              </div>
            </div>
          </Section>
        )}

        {/* Metadata */}
        <Section title="Metadata">
          <div className="space-y-2 text-sm">
            <div className="flex justify-between">
              <span className="text-stone-400 font-medium">ID</span>
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
          </div>
        </Section>
      </div>
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

function Field({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <div className="field-label">{label}</div>
      <div className="text-sm text-navy-800 leading-snug font-medium">{value}</div>
    </div>
  )
}
