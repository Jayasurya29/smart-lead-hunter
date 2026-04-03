import { useState, useEffect, useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import api from '@/api/client'
import { cn, getTierColor, getTierLabel } from '@/lib/utils'
import {
  MapPin, Building2, Users, Eye, Filter, X, Layers,
  Phone, Globe, DollarSign, ChevronDown,
} from 'lucide-react'
import { MapContainer, TileLayer, CircleMarker, Popup, useMap, ZoomControl } from 'react-leaflet'
import 'leaflet/dist/leaflet.css'

/* ═══════════════════════════════════════════════════
   TYPES
   ═══════════════════════════════════════════════════ */

interface MapHotel {
  id: number
  name: string
  brand: string | null
  brand_tier: string | null
  city: string | null
  state: string | null
  lat: number
  lng: number
  is_client: boolean
  room_count: number | null
  phone: string | null
  zone?: string | null
  revenue_annual?: number | null
}

interface MapFilters {
  type: '' | 'client' | 'prospect'
  tier: string
  zone: string
}

const DEFAULT_FILTERS: MapFilters = { type: '', tier: '', zone: '' }

/* ═══════════════════════════════════════════════════
   COLORS
   ═══════════════════════════════════════════════════ */

const MARKER_COLORS = {
  client: { fill: '#059669', stroke: '#065f46', label: 'SAP Client' },       // emerald
  prospect: { fill: '#2563eb', stroke: '#1e40af', label: 'Prospect' },       // blue
  new_lead: { fill: '#f97316', stroke: '#c2410c', label: 'New Lead' },       // orange
} as const

/* ═══════════════════════════════════════════════════
   HELPERS
   ═══════════════════════════════════════════════════ */

function fmtRevenue(n: number | null | undefined): string {
  if (!n) return '—'
  if (n >= 1_000_000) return `$${(n / 1_000_000).toFixed(1)}M`
  if (n >= 1_000) return `$${Math.round(n / 1_000)}K`
  return `$${n.toLocaleString()}`
}

/* ═══════════════════════════════════════════════════
   MAP PAGE
   ═══════════════════════════════════════════════════ */

export default function MapPage() {
  const [filters, setFilters] = useState<MapFilters>(DEFAULT_FILTERS)
  const [showFilters, setShowFilters] = useState(false)
  const [selectedHotel, setSelectedHotel] = useState<MapHotel | null>(null)

  // Fetch all existing hotels with coordinates
  const { data: hotels = [], isLoading } = useQuery<MapHotel[]>({
    queryKey: ['map-data'],
    queryFn: async () => (await api.get('/api/existing-hotels/map-data')).data,
  })

  // Get unique zones for filter
  const zones = useMemo(() => {
    const zoneSet = new Set<string>()
    hotels.forEach(h => { if (h.zone) zoneSet.add(h.zone) })
    return Array.from(zoneSet).sort()
  }, [hotels])

  // Apply filters
  const filtered = useMemo(() => {
    return hotels.filter(h => {
      if (filters.type === 'client' && !h.is_client) return false
      if (filters.type === 'prospect' && h.is_client) return false
      if (filters.tier && h.brand_tier !== filters.tier) return false
      if (filters.zone && h.zone !== filters.zone) return false
      return true
    })
  }, [hotels, filters])

  // Stats
  const stats = useMemo(() => ({
    total: filtered.length,
    clients: filtered.filter(h => h.is_client).length,
    prospects: filtered.filter(h => !h.is_client).length,
    withRooms: filtered.filter(h => h.room_count && h.room_count > 0).length,
  }), [filtered])

  const hasFilters = filters.type || filters.tier || filters.zone

  return (
    <div className="h-full flex flex-col">
      {/* Top Bar */}
      <div className="px-4 pt-3 pb-2 flex-shrink-0">
        <div className="flex items-center justify-between">
          {/* Stats */}
          <div className="flex items-center gap-3">
            <StatPill icon={MapPin} value={stats.total} label="Hotels" color="text-navy-600" bg="bg-navy-50" />
            <StatPill icon={Users} value={stats.clients} label="Clients" color="text-emerald-600" bg="bg-emerald-50" />
            <StatPill icon={Eye} value={stats.prospects} label="Prospects" color="text-blue-600" bg="bg-blue-50" />
          </div>

          {/* Filter Toggle + Legend */}
          <div className="flex items-center gap-3">
            {/* Legend */}
            <div className="flex items-center gap-3 mr-2">
              {Object.entries(MARKER_COLORS).filter(([k]) => k !== 'new_lead').map(([key, val]) => (
                <div key={key} className="flex items-center gap-1.5">
                  <span className="w-3 h-3 rounded-full" style={{ backgroundColor: val.fill }} />
                  <span className="text-xs text-stone-500 font-medium">{val.label}</span>
                </div>
              ))}
            </div>

            <button
              onClick={() => setShowFilters(!showFilters)}
              className={cn(
                'flex items-center gap-1.5 px-3 py-2 text-xs font-semibold rounded-lg transition',
                hasFilters
                  ? 'bg-navy-900 text-white'
                  : 'bg-white border border-stone-200 text-stone-600 hover:bg-stone-50',
              )}
            >
              <Filter className="w-3.5 h-3.5" />
              Filters
              {hasFilters && (
                <span className="bg-white/20 px-1.5 rounded-full text-2xs">ON</span>
              )}
            </button>

            {hasFilters && (
              <button
                onClick={() => setFilters(DEFAULT_FILTERS)}
                className="flex items-center gap-1 px-2.5 py-2 text-xs font-semibold text-red-500 bg-red-50 border border-red-200 rounded-lg hover:bg-red-100 transition"
              >
                <X className="w-3 h-3" /> Clear
              </button>
            )}
          </div>
        </div>

        {/* Filter Panel */}
        {showFilters && (
          <div className="mt-2 flex items-center gap-3 animate-slideUp" style={{ animationDuration: '0.15s' }}>
            <select
              value={filters.type}
              onChange={(e) => setFilters(f => ({ ...f, type: e.target.value as MapFilters['type'] }))}
              className="h-8 px-2.5 text-xs bg-white border border-stone-200 rounded-lg outline-none focus:border-navy-400"
            >
              <option value="">All Hotels</option>
              <option value="client">Clients</option>
              <option value="prospect">Prospects</option>
            </select>

            <select
              value={filters.tier}
              onChange={(e) => setFilters(f => ({ ...f, tier: e.target.value }))}
              className="h-8 px-2.5 text-xs bg-white border border-stone-200 rounded-lg outline-none focus:border-navy-400"
            >
              <option value="">All Tiers</option>
              <option value="tier1_ultra_luxury">T1 — Ultra Luxury</option>
              <option value="tier2_luxury">T2 — Luxury</option>
              <option value="tier3_upper_upscale">T3 — Upper Upscale</option>
              <option value="tier4_upscale">T4 — Upscale</option>
            </select>

            <select
              value={filters.zone}
              onChange={(e) => setFilters(f => ({ ...f, zone: e.target.value }))}
              className="h-8 px-2.5 text-xs bg-white border border-stone-200 rounded-lg outline-none focus:border-navy-400"
            >
              <option value="">All Zones</option>
              {zones.map(z => (
                <option key={z} value={z}>{z}</option>
              ))}
            </select>
          </div>
        )}
      </div>

      {/* Map */}
      <div className="flex-1 mx-4 mb-3 rounded-xl overflow-hidden border border-slate-200/80 shadow-sm relative">
        {isLoading ? (
          <div className="h-full flex items-center justify-center bg-slate-50">
            <div className="flex flex-col items-center gap-2">
              <div className="w-8 h-8 border-3 border-navy-200 border-t-navy-600 rounded-full animate-spin" />
              <span className="text-sm text-stone-400 font-medium">Loading map data...</span>
            </div>
          </div>
        ) : (
          <MapContainer
            center={[27.5, -81.8]}
            zoom={7}
            className="h-full w-full"
            zoomControl={false}
            style={{ background: '#f1f5f9' }}
          >
            <ZoomControl position="topright" />
            <TileLayer
              attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
              url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
            />

            {filtered.map(hotel => {
              const color = hotel.is_client ? MARKER_COLORS.client : MARKER_COLORS.prospect
              return (
                <CircleMarker
                  key={hotel.id}
                  center={[hotel.lat, hotel.lng]}
                  radius={hotel.room_count ? Math.max(5, Math.min(14, hotel.room_count / 60 + 4)) : 5}
                  pathOptions={{
                    fillColor: color.fill,
                    color: color.stroke,
                    weight: 1.5,
                    opacity: 0.9,
                    fillOpacity: 0.7,
                  }}
                  eventHandlers={{
                    click: () => setSelectedHotel(hotel),
                  }}
                >
                  <Popup>
                    <HotelPopup hotel={hotel} />
                  </Popup>
                </CircleMarker>
              )
            })}

            <FitBoundsToMarkers hotels={filtered} />
          </MapContainer>
        )}

        {/* Selected Hotel Card */}
        {selectedHotel && (
          <div className="absolute bottom-4 left-4 right-4 md:left-auto md:right-4 md:w-96 bg-white rounded-xl border border-slate-200 shadow-xl z-[1000] animate-slideUp" style={{ animationDuration: '0.2s' }}>
            <div className="p-4">
              <div className="flex items-start justify-between gap-3">
                <div className="min-w-0 flex-1">
                  <h3 className="text-sm font-bold text-navy-900 truncate">{selectedHotel.name}</h3>
                  {selectedHotel.brand && <p className="text-xs text-stone-400 mt-0.5">{selectedHotel.brand}</p>}
                </div>
                <div className="flex items-center gap-2 flex-shrink-0">
                  <span className={cn(
                    'px-2 py-0.5 rounded-full text-2xs font-bold',
                    selectedHotel.is_client ? 'bg-emerald-50 text-emerald-600' : 'bg-blue-50 text-blue-600',
                  )}>
                    {selectedHotel.is_client ? 'Client' : 'Prospect'}
                  </span>
                  <button onClick={() => setSelectedHotel(null)} className="p-1 text-stone-400 hover:text-stone-600 rounded transition">
                    <X className="w-3.5 h-3.5" />
                  </button>
                </div>
              </div>

              <div className="grid grid-cols-3 gap-3 mt-3">
                <InfoCell icon={MapPin} label="Location" value={[selectedHotel.city, selectedHotel.state].filter(Boolean).join(', ') || '—'} />
                <InfoCell icon={Building2} label="Rooms" value={selectedHotel.room_count ? String(selectedHotel.room_count) : '—'} />
                <InfoCell icon={DollarSign} label="Annual" value={fmtRevenue(selectedHotel.revenue_annual)} />
              </div>

              {selectedHotel.brand_tier && (
                <div className="mt-2.5">
                  <span className={cn('inline-flex px-2 py-0.5 rounded text-2xs font-bold', getTierColor(selectedHotel.brand_tier))}>
                    {getTierLabel(selectedHotel.brand_tier)}
                  </span>
                  {selectedHotel.zone && (
                    <span className="inline-flex px-2 py-0.5 ml-1.5 rounded text-2xs font-medium bg-stone-100 text-stone-500">
                      {selectedHotel.zone}
                    </span>
                  )}
                </div>
              )}

              {selectedHotel.phone && (
                <a href={`tel:${selectedHotel.phone}`} className="flex items-center gap-1.5 mt-2.5 text-xs text-navy-600 hover:underline">
                  <Phone className="w-3 h-3" /> {selectedHotel.phone}
                </a>
              )}
            </div>
          </div>
        )}
      </div>
    </div>
  )
}


/* ═══════════════════════════════════════════════════
   FIT BOUNDS HELPER
   ═══════════════════════════════════════════════════ */

function FitBoundsToMarkers({ hotels }: { hotels: MapHotel[] }) {
  const map = useMap()

  useEffect(() => {
    if (hotels.length === 0) return
    const bounds = hotels.map(h => [h.lat, h.lng] as [number, number])
    if (bounds.length > 0) {
      map.fitBounds(bounds, { padding: [40, 40], maxZoom: 12 })
    }
  }, [hotels, map])

  return null
}


/* ═══════════════════════════════════════════════════
   POPUP COMPONENT
   ═══════════════════════════════════════════════════ */

function HotelPopup({ hotel }: { hotel: MapHotel }) {
  return (
    <div className="min-w-[200px]">
      <p className="font-bold text-sm text-navy-900 leading-snug">{hotel.name}</p>
      {hotel.brand && <p className="text-xs text-stone-400">{hotel.brand}</p>}
      <div className="mt-1.5 space-y-0.5 text-xs text-stone-600">
        <p>{[hotel.city, hotel.state].filter(Boolean).join(', ')}</p>
        {hotel.room_count && <p>{hotel.room_count} rooms</p>}
        {hotel.revenue_annual && <p>Annual: {fmtRevenue(hotel.revenue_annual)}</p>}
      </div>
    </div>
  )
}


/* ═══════════════════════════════════════════════════
   STAT PILL
   ═══════════════════════════════════════════════════ */

function StatPill({ icon: Icon, value, label, color, bg }: {
  icon: React.ElementType; value: number; label: string; color: string; bg: string
}) {
  return (
    <div className={cn('flex items-center gap-2 px-3 py-1.5 rounded-lg border border-stone-200 bg-white')}>
      <div className={cn('w-6 h-6 rounded-md flex items-center justify-center flex-shrink-0', bg)}>
        <Icon className={cn('w-3.5 h-3.5', color)} />
      </div>
      <span className="text-sm font-bold text-navy-900 tabular-nums">{value}</span>
      <span className="text-2xs text-stone-400 font-semibold uppercase">{label}</span>
    </div>
  )
}


/* ═══════════════════════════════════════════════════
   INFO CELL
   ═══════════════════════════════════════════════════ */

function InfoCell({ icon: Icon, label, value }: { icon: React.ElementType; label: string; value: string }) {
  return (
    <div>
      <div className="flex items-center gap-1 mb-0.5">
        <Icon className="w-3 h-3 text-stone-400" />
        <span className="text-2xs text-stone-400 font-semibold uppercase">{label}</span>
      </div>
      <span className="text-xs font-semibold text-navy-800">{value}</span>
    </div>
  )
}
