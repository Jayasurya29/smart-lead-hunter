import { useState, useEffect, useMemo, useCallback, useRef } from 'react'
import { useQuery } from '@tanstack/react-query'
import api from '@/api/client'
import { cn, getTierColor, getTierLabel } from '@/lib/utils'
import {
  MapPin, Building2, Users, Eye, Filter, X,
  Phone, DollarSign, Navigation, Route, Trash2,
  Layers, Zap, Clock, Milestone,
} from 'lucide-react'
import {
  MapContainer, TileLayer, Marker, Popup, Polyline,
  useMap, ZoomControl, useMapEvents,
} from 'react-leaflet'
import MarkerClusterGroup from 'react-leaflet-cluster'
import L from 'leaflet'
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

interface RouteResult {
  coords: [number, number][]
  distance: number  // km
  duration: number  // minutes
  waypoints: { name: string; lat: number; lng: number }[]
  optimizedOrder?: number[]
}

type TileStyle = 'street' | 'satellite' | 'dark'

const DEFAULT_FILTERS: MapFilters = { type: '', tier: '', zone: '' }

/* ═══════════════════════════════════════════════════
   MARKER ICONS
   ═══════════════════════════════════════════════════ */

function createIcon(color: string, size: number = 10, isRouteStop: boolean = false, stopNum?: number): L.DivIcon {
  const border = isRouteStop ? '3px solid #f97316' : `2px solid ${color === '#059669' ? '#065f46' : '#1e40af'}`
  const shadow = isRouteStop ? 'box-shadow: 0 0 0 3px rgba(249,115,22,0.3);' : ''

  if (isRouteStop && stopNum !== undefined) {
    return L.divIcon({
      className: '',
      html: `<div style="
        width: ${size + 8}px; height: ${size + 8}px; border-radius: 50%;
        background: #f97316; border: 2px solid #c2410c;
        display: flex; align-items: center; justify-content: center;
        font-size: 10px; font-weight: 800; color: white;
        box-shadow: 0 2px 8px rgba(249,115,22,0.5);
      ">${stopNum}</div>`,
      iconSize: [size + 8, size + 8],
      iconAnchor: [(size + 8) / 2, (size + 8) / 2],
    })
  }

  return L.divIcon({
    className: '',
    html: `<div style="
      width: ${size}px; height: ${size}px; border-radius: 50%;
      background: ${color}; border: ${border};
      opacity: 0.85; ${shadow}
    "></div>`,
    iconSize: [size, size],
    iconAnchor: [size / 2, size / 2],
  })
}

const MARKER_COLORS = {
  client: '#059669',
  prospect: '#2563eb',
}

/* ═══════════════════════════════════════════════════
   TILE LAYERS
   ═══════════════════════════════════════════════════ */

const TILES: Record<TileStyle, { url: string; attribution: string; label: string }> = {
  street: {
    url: 'https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',
    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>',
    label: 'Street',
  },
  satellite: {
    url: 'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
    attribution: '&copy; Esri, Maxar, Earthstar',
    label: 'Satellite',
  },
  dark: {
    url: 'https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',
    attribution: '&copy; <a href="https://carto.com/">CARTO</a>',
    label: 'Dark',
  },
}

/* ═══════════════════════════════════════════════════
   HELPERS
   ═══════════════════════════════════════════════════ */

function fmtRevenue(n: number | null | undefined): string {
  if (!n) return '—'
  if (n >= 1_000_000) return `$${(n / 1_000_000).toFixed(1)}M`
  if (n >= 1_000) return `$${Math.round(n / 1_000)}K`
  return `$${n.toLocaleString()}`
}

function markerSize(rooms: number | null): number {
  if (!rooms) return 8
  if (rooms >= 500) return 16
  if (rooms >= 300) return 14
  if (rooms >= 150) return 12
  if (rooms >= 50) return 10
  return 8
}

/* ═══════════════════════════════════════════════════
   ROUTE OPTIMIZER (OSRM — free, brute force for ≤8 stops)
   ═══════════════════════════════════════════════════ */

function permutations<T>(arr: T[]): T[][] {
  if (arr.length <= 1) return [arr]
  const result: T[][] = []
  for (let i = 0; i < arr.length; i++) {
    const rest = [...arr.slice(0, i), ...arr.slice(i + 1)]
    for (const perm of permutations(rest)) {
      result.push([arr[i], ...perm])
    }
  }
  return result
}

async function optimizeRoute(stops: MapHotel[]): Promise<RouteResult | null> {
  if (stops.length < 2) return null

  const coords = stops.map(h => `${h.lng},${h.lat}`).join(';')

  try {
    // Step 1: Get distance/duration matrix between ALL pairs
    const tableUrl = `https://router.project-osrm.org/table/v1/driving/${coords}?annotations=duration,distance`
    const tableResp = await fetch(tableUrl)
    const tableData = await tableResp.json()

    if (tableData.code !== 'Ok') {
      console.error('OSRM table failed:', tableData)
      return null
    }

    const durations: number[][] = tableData.durations // [from][to] in seconds
    const distances: number[][] = tableData.distances // [from][to] in meters

    // Step 2: Find the best order (brute force for ≤8, nearest neighbor for >8)
    const indices = stops.map((_, i) => i)
    let bestOrder: number[] = indices
    let bestDuration = Infinity

    if (stops.length <= 8) {
      // Brute force — try every permutation, pick shortest total drive time
      for (const perm of permutations(indices)) {
        let totalDuration = 0
        for (let i = 0; i < perm.length - 1; i++) {
          totalDuration += durations[perm[i]][perm[i + 1]]
        }
        if (totalDuration < bestDuration) {
          bestDuration = totalDuration
          bestOrder = perm
        }
      }
    } else {
      // Nearest neighbor heuristic for >8 stops
      const visited = new Set<number>()
      const order: number[] = [0]
      visited.add(0)
      while (order.length < stops.length) {
        const last = order[order.length - 1]
        let nearest = -1
        let nearestDist = Infinity
        for (let i = 0; i < stops.length; i++) {
          if (!visited.has(i) && durations[last][i] < nearestDist) {
            nearestDist = durations[last][i]
            nearest = i
          }
        }
        if (nearest === -1) break
        order.push(nearest)
        visited.add(nearest)
      }
      bestOrder = order
    }

    // Step 3: Get the actual driving route for the best order
    const orderedCoords = bestOrder.map(i => `${stops[i].lng},${stops[i].lat}`).join(';')
    const routeUrl = `https://router.project-osrm.org/route/v1/driving/${orderedCoords}?overview=full&geometries=geojson`
    const routeResp = await fetch(routeUrl)
    const routeData = await routeResp.json()

    if (routeData.code !== 'Ok') return null

    const route = routeData.routes[0]
    return {
      coords: route.geometry.coordinates.map((c: number[]) => [c[1], c[0]] as [number, number]),
      distance: route.distance / 1000,
      duration: route.duration / 60,
      waypoints: bestOrder.map((idx, i) => ({
        name: stops[idx].name,
        lat: stops[idx].lat,
        lng: stops[idx].lng,
        originalIndex: idx,
      })),
      optimizedOrder: bestOrder,
    } as RouteResult
  } catch (e) {
    console.error('Route optimization failed:', e)
    return null
  }
}

/* ═══════════════════════════════════════════════════
   MAP PAGE
   ═══════════════════════════════════════════════════ */

export default function MapPage() {
  const [filters, setFilters] = useState<MapFilters>(DEFAULT_FILTERS)
  const [showFilters, setShowFilters] = useState(false)
  const [selectedHotel, setSelectedHotel] = useState<MapHotel | null>(null)
  const [tileStyle, setTileStyle] = useState<TileStyle>('street')

  // Route planner state
  const [routeMode, setRouteMode] = useState(false)
  const [routeStops, setRouteStops] = useState<MapHotel[]>([])
  const [routeResult, setRouteResult] = useState<RouteResult | null>(null)
  const [isOptimizing, setIsOptimizing] = useState(false)

  // Fetch all existing hotels
  const { data: hotels = [], isLoading } = useQuery<MapHotel[]>({
    queryKey: ['map-data'],
    queryFn: async () => (await api.get('/api/existing-hotels/map-data')).data,
  })

  // Zones
  const zones = useMemo(() => {
    const s = new Set<string>()
    hotels.forEach(h => { if (h.zone) s.add(h.zone) })
    return Array.from(s).sort()
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
  }), [filtered])

  const hasFilters = filters.type || filters.tier || filters.zone

  // Route helpers
  const routeStopIds = useMemo(() => new Set(routeStops.map(h => h.id)), [routeStops])

  function handleMarkerClick(hotel: MapHotel) {
    if (routeMode) {
      // Toggle stop
      if (routeStopIds.has(hotel.id)) {
        setRouteStops(prev => prev.filter(h => h.id !== hotel.id))
      } else {
        setRouteStops(prev => [...prev, hotel])
      }
      setRouteResult(null) // Clear old route when stops change
    } else {
      setSelectedHotel(hotel)
    }
  }

  async function handleOptimize() {
    if (routeStops.length < 2) return
    setIsOptimizing(true)
    const result = await optimizeRoute(routeStops)
    if (result && (result as any).optimizedOrder) {
      // Reorder stops based on brute-force optimal order
      const order = (result as any).optimizedOrder as number[]
      setRouteStops(order.map(i => routeStops[i]))
      setRouteResult(result)
    } else if (result) {
      setRouteResult(result)
    }
    setIsOptimizing(false)
  }

  function clearRoute() {
    setRouteStops([])
    setRouteResult(null)
  }

  function exitRouteMode() {
    setRouteMode(false)
    clearRoute()
  }

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

          {/* Controls */}
          <div className="flex items-center gap-2">
            {/* Legend */}
            <div className="flex items-center gap-3 mr-2">
              <LegendDot color="#059669" label="Client" />
              <LegendDot color="#2563eb" label="Prospect" />
              {routeMode && <LegendDot color="#f97316" label="Route Stop" />}
            </div>

            {/* Tile Toggle */}
            <div className="flex bg-stone-100 rounded-lg p-0.5">
              {(Object.keys(TILES) as TileStyle[]).map(key => (
                <button
                  key={key}
                  onClick={() => setTileStyle(key)}
                  className={cn(
                    'px-2.5 py-1.5 text-2xs font-semibold rounded-md transition',
                    tileStyle === key ? 'bg-white text-navy-900 shadow-sm' : 'text-stone-500 hover:text-stone-700',
                  )}
                >
                  {TILES[key].label}
                </button>
              ))}
            </div>

            {/* Route Mode Toggle */}
            <button
              onClick={() => routeMode ? exitRouteMode() : setRouteMode(true)}
              className={cn(
                'flex items-center gap-1.5 px-3 py-2 text-xs font-semibold rounded-lg transition',
                routeMode
                  ? 'bg-orange-500 text-white hover:bg-orange-600'
                  : 'bg-white border border-stone-200 text-stone-600 hover:bg-stone-50',
              )}
            >
              <Route className="w-3.5 h-3.5" />
              {routeMode ? 'Exit Route' : 'Plan Route'}
            </button>

            {/* Filters */}
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
            </button>

            {hasFilters && (
              <button onClick={() => setFilters(DEFAULT_FILTERS)} className="flex items-center gap-1 px-2.5 py-2 text-xs font-semibold text-red-500 bg-red-50 border border-red-200 rounded-lg hover:bg-red-100 transition">
                <X className="w-3 h-3" /> Clear
              </button>
            )}
          </div>
        </div>

        {/* Filter Panel */}
        {showFilters && (
          <div className="mt-2 flex items-center gap-3 animate-slideUp" style={{ animationDuration: '0.15s' }}>
            <select value={filters.type} onChange={(e) => setFilters(f => ({ ...f, type: e.target.value as MapFilters['type'] }))} className="h-8 px-2.5 text-xs bg-white border border-stone-200 rounded-lg outline-none focus:border-navy-400">
              <option value="">All Hotels</option>
              <option value="client">Clients</option>
              <option value="prospect">Prospects</option>
            </select>
            <select value={filters.tier} onChange={(e) => setFilters(f => ({ ...f, tier: e.target.value }))} className="h-8 px-2.5 text-xs bg-white border border-stone-200 rounded-lg outline-none focus:border-navy-400">
              <option value="">All Tiers</option>
              <option value="tier1_ultra_luxury">T1 — Ultra Luxury</option>
              <option value="tier2_luxury">T2 — Luxury</option>
              <option value="tier3_upper_upscale">T3 — Upper Upscale</option>
              <option value="tier4_upscale">T4 — Upscale</option>
            </select>
            <select value={filters.zone} onChange={(e) => setFilters(f => ({ ...f, zone: e.target.value }))} className="h-8 px-2.5 text-xs bg-white border border-stone-200 rounded-lg outline-none focus:border-navy-400">
              <option value="">All Zones</option>
              {zones.map(z => <option key={z} value={z}>{z}</option>)}
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
            style={{ background: tileStyle === 'dark' ? '#1a1a2e' : '#f1f5f9' }}
          >
            <ZoomControl position="topright" />
            <TileLayer
              key={tileStyle}
              attribution={TILES[tileStyle].attribution}
              url={TILES[tileStyle].url}
            />

            {/* Clustered Markers */}
            <MarkerClusterGroup
              chunkedLoading
              maxClusterRadius={50}
              spiderfyOnMaxZoom
              showCoverageOnHover={false}
              iconCreateFunction={(cluster: any) => {
                const count = cluster.getChildCount()
                const size = count > 100 ? 44 : count > 30 ? 38 : 32
                return L.divIcon({
                  html: `<div style="
                    width: ${size}px; height: ${size}px; border-radius: 50%;
                    background: rgba(15,23,42,0.85); border: 2px solid rgba(255,255,255,0.4);
                    display: flex; align-items: center; justify-content: center;
                    font-size: 12px; font-weight: 700; color: white;
                    box-shadow: 0 2px 8px rgba(0,0,0,0.3);
                  ">${count}</div>`,
                  className: '',
                  iconSize: L.point(size, size),
                })
              }}
            >
              {filtered.filter(h => !routeStopIds.has(h.id)).map(hotel => {
                const color = hotel.is_client ? MARKER_COLORS.client : MARKER_COLORS.prospect
                const size = markerSize(hotel.room_count)
                return (
                  <Marker
                    key={hotel.id}
                    position={[hotel.lat, hotel.lng]}
                    icon={createIcon(color, size, false)}
                    eventHandlers={{ click: () => handleMarkerClick(hotel) }}
                  >
                    {!routeMode && (
                      <Popup>
                        <HotelPopup hotel={hotel} />
                      </Popup>
                    )}
                  </Marker>
                )
              })}
            </MarkerClusterGroup>

            {/* Route Stops — OUTSIDE cluster so numbers always visible */}
            {routeStops.map((hotel, i) => (
              <Marker
                key={`route-${hotel.id}`}
                position={[hotel.lat, hotel.lng]}
                icon={createIcon('#f97316', 14, true, i + 1)}
                eventHandlers={{ click: () => handleMarkerClick(hotel) }}
                zIndexOffset={1000}
              />
            ))}

            {/* Route Line */}
            {routeResult && (
              <Polyline
                positions={routeResult.coords}
                pathOptions={{
                  color: '#f97316',
                  weight: 4,
                  opacity: 0.8,
                  dashArray: '8, 6',
                }}
              />
            )}

            <FitBoundsToMarkers hotels={filtered} />
          </MapContainer>
        )}

        {/* Route Mode Banner */}
        {routeMode && !routeResult && (
          <div className="absolute top-3 left-1/2 -translate-x-1/2 bg-orange-500 text-white px-4 py-2 rounded-lg shadow-lg z-[1000] flex items-center gap-2 text-xs font-semibold">
            <Route className="w-4 h-4" />
            Click hotels to add stops ({routeStops.length} selected)
          </div>
        )}

        {/* Route Panel */}
        {routeMode && routeStops.length > 0 && (
          <div className="absolute top-3 left-3 w-72 bg-white rounded-xl border border-slate-200 shadow-xl z-[1000] max-h-[70%] flex flex-col">
            <div className="px-4 py-3 border-b border-slate-100 flex items-center justify-between">
              <div className="flex items-center gap-2">
                <Route className="w-4 h-4 text-orange-500" />
                <span className="text-sm font-bold text-navy-900">Route Planner</span>
              </div>
              <button onClick={clearRoute} className="p-1 text-stone-400 hover:text-red-500 transition" title="Clear route">
                <Trash2 className="w-3.5 h-3.5" />
              </button>
            </div>

            {/* Stops List */}
            <div className="flex-1 overflow-y-auto p-3 space-y-1.5">
              {routeStops.map((hotel, i) => (
                <div key={hotel.id} className="flex items-center gap-2 group">
                  <span className="w-5 h-5 rounded-full bg-orange-500 text-white text-2xs font-bold flex items-center justify-center flex-shrink-0">
                    {i + 1}
                  </span>
                  <div className="flex-1 min-w-0">
                    <p className="text-xs font-semibold text-navy-900 truncate">{hotel.name}</p>
                    <p className="text-2xs text-stone-400">{hotel.city}</p>
                  </div>
                  <button
                    onClick={() => { setRouteStops(prev => prev.filter(h => h.id !== hotel.id)); setRouteResult(null) }}
                    className="p-0.5 text-stone-300 hover:text-red-500 opacity-0 group-hover:opacity-100 transition"
                  >
                    <X className="w-3 h-3" />
                  </button>
                </div>
              ))}
            </div>

            {/* Route Result */}
            {routeResult && (
              <div className="px-4 py-2.5 bg-orange-50 border-t border-orange-100">
                <div className="flex items-center gap-4">
                  <div className="flex items-center gap-1.5">
                    <Milestone className="w-3.5 h-3.5 text-orange-600" />
                    <span className="text-xs font-bold text-orange-800">{(routeResult.distance * 0.621371).toFixed(1)} mi</span>
                  </div>
                  <div className="flex items-center gap-1.5">
                    <Clock className="w-3.5 h-3.5 text-orange-600" />
                    <span className="text-xs font-bold text-orange-800">
                      {routeResult.duration >= 60
                        ? `${Math.floor(routeResult.duration / 60)}h ${Math.round(routeResult.duration % 60)}m`
                        : `${Math.round(routeResult.duration)}m`}
                    </span>
                  </div>
                </div>
              </div>
            )}

            {/* Optimize Button */}
            <div className="px-3 py-2.5 border-t border-slate-100">
              <button
                onClick={handleOptimize}
                disabled={routeStops.length < 2 || isOptimizing}
                className="w-full flex items-center justify-center gap-1.5 px-3 py-2 text-xs font-semibold rounded-lg bg-orange-500 text-white hover:bg-orange-600 transition disabled:opacity-40 disabled:cursor-not-allowed"
              >
                {isOptimizing ? (
                  <>
                    <div className="w-3.5 h-3.5 border-2 border-white/30 border-t-white rounded-full animate-spin" />
                    Optimizing...
                  </>
                ) : (
                  <>
                    <Navigation className="w-3.5 h-3.5" />
                    {routeStops.length < 2 ? 'Add 2+ stops' : `Optimize ${routeStops.length} stops`}
                  </>
                )}
              </button>
            </div>
          </div>
        )}

        {/* Selected Hotel Card (non-route mode) */}
        {!routeMode && selectedHotel && (
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
   FIT BOUNDS
   ═══════════════════════════════════════════════════ */

function FitBoundsToMarkers({ hotels }: { hotels: MapHotel[] }) {
  const map = useMap()
  const prevLengthRef = useRef(0)

  useEffect(() => {
    if (hotels.length === 0) return
    // Only auto-fit when filter changes (count changes)
    if (hotels.length !== prevLengthRef.current) {
      const bounds = hotels.map(h => [h.lat, h.lng] as [number, number])
      map.fitBounds(bounds, { padding: [40, 40], maxZoom: 12 })
      prevLengthRef.current = hotels.length
    }
  }, [hotels, map])

  return null
}

/* ═══════════════════════════════════════════════════
   SUB COMPONENTS
   ═══════════════════════════════════════════════════ */

function HotelPopup({ hotel }: { hotel: MapHotel }) {
  return (
    <div className="min-w-[200px]">
      <p className="font-bold text-sm leading-snug">{hotel.name}</p>
      {hotel.brand && <p className="text-xs text-gray-500">{hotel.brand}</p>}
      <div className="mt-1.5 space-y-0.5 text-xs text-gray-600">
        <p>{[hotel.city, hotel.state].filter(Boolean).join(', ')}</p>
        {hotel.room_count && <p>{hotel.room_count} rooms</p>}
        {hotel.revenue_annual && <p>Annual: {fmtRevenue(hotel.revenue_annual)}</p>}
      </div>
    </div>
  )
}

function StatPill({ icon: Icon, value, label, color, bg }: {
  icon: React.ElementType; value: number; label: string; color: string; bg: string
}) {
  return (
    <div className="flex items-center gap-2 px-3 py-1.5 rounded-lg border border-stone-200 bg-white">
      <div className={cn('w-6 h-6 rounded-md flex items-center justify-center flex-shrink-0', bg)}>
        <Icon className={cn('w-3.5 h-3.5', color)} />
      </div>
      <span className="text-sm font-bold text-navy-900 tabular-nums">{value}</span>
      <span className="text-2xs text-stone-400 font-semibold uppercase">{label}</span>
    </div>
  )
}

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

function LegendDot({ color, label }: { color: string; label: string }) {
  return (
    <div className="flex items-center gap-1.5">
      <span className="w-3 h-3 rounded-full" style={{ backgroundColor: color }} />
      <span className="text-xs text-stone-500 font-medium">{label}</span>
    </div>
  )
}
