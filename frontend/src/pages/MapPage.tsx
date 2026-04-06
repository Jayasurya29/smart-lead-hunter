import { useState, useEffect, useMemo, useRef, memo } from 'react'
import { useQuery } from '@tanstack/react-query'
import api from '@/api/client'
import { cn, getTierColor, getTierLabel } from '@/lib/utils'
import {
  MapPin, Building2, Users, Eye, Filter, X,
  Phone, DollarSign, Navigation, Route, Trash2,
  Layers, Clock, Milestone, Search, Crosshair,
  Maximize2, Minimize2, Flame, Radar, MapPinOff,
  ExternalLink, BarChart3,
} from 'lucide-react'
import {
  MapContainer, TileLayer, Marker, Popup, Polyline,
  Circle, useMap, ZoomControl, useMapEvents,
} from 'react-leaflet'
import MarkerClusterGroup from 'react-leaflet-cluster'
import L from 'leaflet'
import 'leaflet/dist/leaflet.css'

/* ═══════════════════════════════════════════════════
   CONSTANTS
   ═══════════════════════════════════════════════════ */

const OFFICE_LAT = 25.6437
const OFFICE_LNG = -80.4082
const OFFICE_LABEL = 'JA Uniforms HQ'
const MILES_TO_METERS = 1609.34

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

interface NearbyHotel extends MapHotel { dist: number }

interface MapFilters { type: '' | 'client' | 'prospect'; tier: string; zone: string }

interface RouteResult {
  coords: [number, number][]
  distance: number
  duration: number
  waypoints: { name: string; lat: number; lng: number }[]
  optimizedOrder?: number[]
}

type TileStyle = 'street' | 'satellite' | 'dark'
type ColorMode = 'type' | 'revenue' | 'tier'
type NearbyRadius = 5 | 10 | 20

const DEFAULT_FILTERS: MapFilters = { type: '', tier: '', zone: '' }

/* ═══════════════════════════════════════════════════
   MARKER ICONS
   ═══════════════════════════════════════════════════ */

function adjustColor(hex: string, amt: number): string {
  const n = parseInt(hex.replace('#', ''), 16)
  const r = Math.min(255, Math.max(0, ((n >> 16) & 0xff) + amt))
  const g = Math.min(255, Math.max(0, ((n >> 8) & 0xff) + amt))
  const b = Math.min(255, Math.max(0, (n & 0xff) + amt))
  return `#${((r << 16) | (g << 8) | b).toString(16).padStart(6, '0')}`
}

function createIcon(color: string, size: number = 10, isRouteStop = false, stopNum?: number): L.DivIcon {
  if (isRouteStop && stopNum !== undefined) {
    return L.divIcon({
      className: '',
      html: `<div style="width:${size + 8}px;height:${size + 8}px;border-radius:50%;background:#f97316;border:2px solid #c2410c;display:flex;align-items:center;justify-content:center;font-size:10px;font-weight:800;color:#fff;box-shadow:0 2px 8px rgba(249,115,22,.5)">${stopNum}</div>`,
      iconSize: [size + 8, size + 8], iconAnchor: [(size + 8) / 2, (size + 8) / 2],
    })
  }
  const border = `2px solid ${adjustColor(color, -40)}`
  return L.divIcon({
    className: '',
    html: `<div style="width:${size}px;height:${size}px;border-radius:50%;background:${color};border:${border};opacity:.85"></div>`,
    iconSize: [size, size], iconAnchor: [size / 2, size / 2],
  })
}

const officeIcon = L.divIcon({
  className: '',
  html: `<div style="width:32px;height:32px;border-radius:8px;background:#0f1d32;border:3px solid #d4a853;display:flex;align-items:center;justify-content:center;font-size:16px;color:#d4a853;box-shadow:0 2px 12px rgba(15,29,50,.5);transform:rotate(45deg)"><span style="transform:rotate(-45deg)">★</span></div>`,
  iconSize: [32, 32], iconAnchor: [16, 16],
})

const anchorIcon = L.divIcon({
  className: '',
  html: `<div style="width:18px;height:18px;border-radius:50%;background:#7c3aed;border:3px solid #fff;box-shadow:0 0 0 3px rgba(124,58,237,.4),0 2px 8px rgba(0,0,0,.3)"></div>`,
  iconSize: [18, 18], iconAnchor: [9, 9],
})

const highlightIcon = L.divIcon({
  className: '',
  html: `<div style="position:relative;width:44px;height:44px">
    <div style="position:absolute;inset:0;border-radius:50%;border:3px solid #e85d4a;opacity:.9;animation:hlPulse 1.5s ease-out infinite"></div>
    <div style="position:absolute;inset:6px;border-radius:50%;border:3px solid #e85d4a;opacity:.6;animation:hlPulse 1.5s ease-out .3s infinite"></div>
    <div style="position:absolute;inset:14px;border-radius:50%;background:#e85d4a;border:2px solid #fff;box-shadow:0 0 8px rgba(232,93,74,.6)"></div>
    <style>@keyframes hlPulse{0%{transform:scale(1);opacity:.8}100%{transform:scale(1.8);opacity:0}}</style>
  </div>`,
  iconSize: [44, 44], iconAnchor: [22, 22],
})

/* ═══════════════════════════════════════════════════
   COLOR HELPERS
   ═══════════════════════════════════════════════════ */

const TYPE_COLORS = { client: '#059669', prospect: '#2563eb' }
const TIER_COLORS: Record<string, string> = {
  tier1_ultra_luxury: '#d4a853', tier2_luxury: '#c49a3c',
  tier3_upper_upscale: '#3e638c', tier4_upscale: '#6b665e',
}

function lerpColor(a: string, b: string, t: number): string {
  const p = (h: string) => { const n = parseInt(h.replace('#', ''), 16); return [(n >> 16) & 0xff, (n >> 8) & 0xff, n & 0xff] }
  const [r1, g1, b1] = p(a), [r2, g2, b2] = p(b)
  const r = Math.round(r1 + (r2 - r1) * t), g = Math.round(g1 + (g2 - g1) * t), bl = Math.round(b1 + (b2 - b1) * t)
  return `#${((r << 16) | (g << 8) | bl).toString(16).padStart(6, '0')}`
}

function revenueColor(rev: number | null | undefined, max: number): string {
  if (!rev || max === 0) return '#94a3b8'
  const r = Math.min(rev / max, 1)
  return r < 0.33 ? lerpColor('#3b82f6', '#d4a853', r / 0.33) : lerpColor('#d4a853', '#dc2626', (r - 0.33) / 0.67)
}

function getMarkerColor(h: MapHotel, mode: ColorMode, maxRev: number): string {
  if (mode === 'type') return h.is_client ? TYPE_COLORS.client : TYPE_COLORS.prospect
  if (mode === 'revenue') return revenueColor(h.revenue_annual, maxRev)
  return TIER_COLORS[h.brand_tier || ''] || '#94a3b8'
}

/* ═══════════════════════════════════════════════════
   TILES
   ═══════════════════════════════════════════════════ */

const TILES: Record<TileStyle, { url: string; attribution: string; label: string }> = {
  street: { url: 'https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a>', label: 'Street' },
  satellite: { url: 'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}', attribution: '&copy; Esri, Maxar', label: 'Satellite' },
  dark: { url: 'https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', attribution: '&copy; CARTO', label: 'Dark' },
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
  if (!rooms) return 8; if (rooms >= 500) return 16; if (rooms >= 300) return 14; if (rooms >= 150) return 12; if (rooms >= 50) return 10; return 8
}

function distanceMiles(lat1: number, lng1: number, lat2: number, lng2: number): number {
  const R = 3958.8, dLat = (lat2 - lat1) * Math.PI / 180, dLng = (lng2 - lng1) * Math.PI / 180
  const a = Math.sin(dLat / 2) ** 2 + Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) * Math.sin(dLng / 2) ** 2
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
}

/* ═══════════════════════════════════════════════════
   ROUTE OPTIMIZER (OSRM)
   ═══════════════════════════════════════════════════ */

function permutations<T>(arr: T[]): T[][] {
  if (arr.length <= 1) return [arr]
  const result: T[][] = []
  for (let i = 0; i < arr.length; i++) {
    const rest = [...arr.slice(0, i), ...arr.slice(i + 1)]
    for (const p of permutations(rest)) result.push([arr[i], ...p])
  }
  return result
}

async function optimizeRoute(stops: MapHotel[]): Promise<RouteResult | null> {
  if (stops.length < 2) return null
  const coords = stops.map(h => `${h.lng},${h.lat}`).join(';')
  try {
    const tResp = await fetch(`https://router.project-osrm.org/table/v1/driving/${coords}?annotations=duration,distance`)
    const tData = await tResp.json()
    if (tData.code !== 'Ok') return null
    const dur: number[][] = tData.durations
    const idx = stops.map((_, i) => i)
    let best = idx, bestDur = Infinity
    if (stops.length <= 8) {
      for (const p of permutations(idx)) {
        let t = 0; for (let i = 0; i < p.length - 1; i++) t += dur[p[i]][p[i + 1]]
        if (t < bestDur) { bestDur = t; best = p }
      }
    } else {
      const vis = new Set<number>(); const ord: number[] = [0]; vis.add(0)
      while (ord.length < stops.length) {
        const last = ord[ord.length - 1]; let nn = -1, nd = Infinity
        for (let i = 0; i < stops.length; i++) { if (!vis.has(i) && dur[last][i] < nd) { nd = dur[last][i]; nn = i } }
        if (nn === -1) break; ord.push(nn); vis.add(nn)
      }
      best = ord
    }
    const oCoords = best.map(i => `${stops[i].lng},${stops[i].lat}`).join(';')
    const rResp = await fetch(`https://router.project-osrm.org/route/v1/driving/${oCoords}?overview=full&geometries=geojson`)
    const rData = await rResp.json()
    if (rData.code !== 'Ok') return null
    const route = rData.routes[0]
    return {
      coords: route.geometry.coordinates.map((c: number[]) => [c[1], c[0]] as [number, number]),
      distance: route.distance / 1000, duration: route.duration / 60,
      waypoints: best.map(i => ({ name: stops[i].name, lat: stops[i].lat, lng: stops[i].lng })),
      optimizedOrder: best,
    }
  } catch { return null }
}

function buildGoogleMapsUrl(stops: MapHotel[]): string {
  if (stops.length < 2) return ''
  const o = `${stops[0].lat},${stops[0].lng}`, d = `${stops[stops.length - 1].lat},${stops[stops.length - 1].lng}`
  const w = stops.slice(1, -1).map(s => `${s.lat},${s.lng}`).join('|')
  let url = `https://www.google.com/maps/dir/?api=1&origin=${o}&destination=${d}&travelmode=driving`
  if (w) url += `&waypoints=${w}`
  return url
}

/* ═══════════════════════════════════════════════════
   ZONE GAP ANALYSIS
   ═══════════════════════════════════════════════════ */

interface ZoneStats { zone: string; total: number; clients: number; prospects: number; totalRevenue: number; rooms: number }

function computeZoneStats(hotels: MapHotel[]): ZoneStats[] {
  const m = new Map<string, ZoneStats>()
  for (const h of hotels) {
    const z = h.zone || 'Unknown'
    let s = m.get(z)
    if (!s) { s = { zone: z, total: 0, clients: 0, prospects: 0, totalRevenue: 0, rooms: 0 }; m.set(z, s) }
    s.total++; if (h.is_client) s.clients++; else s.prospects++
    s.totalRevenue += h.revenue_annual || 0; s.rooms += h.room_count || 0
  }
  return Array.from(m.values()).sort((a, b) => b.totalRevenue - a.totalRevenue)
}

/* ═══════════════════════════════════════════════════
   HEATMAP LAYER (Canvas — no external dep)
   ═══════════════════════════════════════════════════ */

const HeatmapLayer = memo(function HeatmapLayer({ hotels, visible }: { hotels: MapHotel[]; visible: boolean }) {
  // Nothing to render if hidden
  if (!visible) return null

  // Pre-compute values: use revenue, fall back to room_count estimate
  const values = hotels.map(h => h.revenue_annual || (h.room_count || 80) * 400)
  const maxVal = Math.max(...values, 1)

  return (
    <>
      {hotels.map((h, i) => {
        const intensity = Math.min(values[i] / maxVal, 1)
        const radius = 12 + intensity * 35
        const opacity = 0.12 + intensity * 0.3
        return (
          <Circle
            key={`heat-${h.id}`}
            center={[h.lat, h.lng]}
            radius={800 + intensity * 4000}
            pathOptions={{
              stroke: false,
              fillColor: intensity > 0.5 ? '#dc2626' : intensity > 0.2 ? '#f97316' : '#3b82f6',
              fillOpacity: opacity,
            }}
            interactive={false}
          />
        )
      })}
    </>
  )
})

/* ═══════════════════════════════════════════════════
   MAP HELPERS
   ═══════════════════════════════════════════════════ */

function FitBoundsToMarkers({ hotels }: { hotels: MapHotel[] }) {
  const map = useMap(); const prev = useRef(0)
  useEffect(() => {
    if (hotels.length && hotels.length !== prev.current) {
      map.fitBounds(hotels.map(h => [h.lat, h.lng] as [number, number]), { padding: [40, 40], maxZoom: 12 })
      prev.current = hotels.length
    }
  }, [hotels, map])
  return null
}

function FlyTo({ lat, lng, zoom, trigger }: { lat: number; lng: number; zoom: number; trigger: number }) {
  const map = useMap()
  useEffect(() => { if (trigger > 0) map.flyTo([lat, lng], zoom, { duration: 1.2 }) }, [trigger, lat, lng, zoom, map])
  return null
}

function NearbyClick({ active, onClear }: { active: boolean; onClear: () => void }) {
  useMapEvents({ click: () => { if (active) onClear() } })
  return null
}

/* ═══════════════════════════════════════════════════
   MAP PAGE
   ═══════════════════════════════════════════════════ */

export default function MapPage() {
  const [filters, setFilters] = useState<MapFilters>(DEFAULT_FILTERS)
  const [showFilters, setShowFilters] = useState(false)
  const [selectedHotel, setSelectedHotel] = useState<MapHotel | null>(null)
  const [tileStyle, setTileStyle] = useState<TileStyle>('street')
  const [colorMode, setColorMode] = useState<ColorMode>('type')

  // Search
  const [searchOpen, setSearchOpen] = useState(false)
  const [searchQuery, setSearchQuery] = useState('')
  const [flyTarget, setFlyTarget] = useState<{ lat: number; lng: number; zoom: number; t: number } | null>(null)
  const searchRef = useRef<HTMLInputElement>(null)

  // Route
  const [routeMode, setRouteMode] = useState(false)
  const [routeStops, setRouteStops] = useState<MapHotel[]>([])
  const [routeResult, setRouteResult] = useState<RouteResult | null>(null)
  const [isOptimizing, setIsOptimizing] = useState(false)

  // Heatmap / Nearby / Fullscreen / Gap
  const [heatmapOn, setHeatmapOn] = useState(false)
  const [nearbyMode, setNearbyMode] = useState(false)
  const [nearbyAnchor, setNearbyAnchor] = useState<MapHotel | null>(null)
  const [nearbyRadius, setNearbyRadius] = useState<NearbyRadius>(10)
  const [showOffice, setShowOffice] = useState(true)
  const [isFullscreen, setIsFullscreen] = useState(false)
  const [showGap, setShowGap] = useState(false)

  // Data
  const { data: hotels = [], isLoading } = useQuery<MapHotel[]>({
    queryKey: ['map-data'],
    queryFn: async () => (await api.get('/api/existing-hotels/map-data')).data,
  })

  const zones = useMemo(() => {
    const s = new Set<string>(); hotels.forEach(h => { if (h.zone) s.add(h.zone) }); return Array.from(s).sort()
  }, [hotels])

  const filtered = useMemo(() => hotels.filter(h => {
    if (filters.type === 'client' && !h.is_client) return false
    if (filters.type === 'prospect' && h.is_client) return false
    if (filters.tier && h.brand_tier !== filters.tier) return false
    if (filters.zone && h.zone !== filters.zone) return false
    return true
  }), [hotels, filters])

  const maxRevenue = useMemo(() => Math.max(...filtered.map(h => h.revenue_annual || 0), 1), [filtered])
  const stats = useMemo(() => ({ total: filtered.length, clients: filtered.filter(h => h.is_client).length, prospects: filtered.filter(h => !h.is_client).length }), [filtered])
  const zoneStats = useMemo(() => computeZoneStats(filtered), [filtered])
  const hasFilters = filters.type || filters.tier || filters.zone

  const searchResults = useMemo(() => {
    if (!searchQuery.trim()) return []
    const q = searchQuery.toLowerCase()
    return filtered.filter(h => h.name.toLowerCase().includes(q) || (h.brand || '').toLowerCase().includes(q) || (h.city || '').toLowerCase().includes(q)).slice(0, 8)
  }, [searchQuery, filtered])

  const routeStopIds = useMemo(() => new Set(routeStops.map(h => h.id)), [routeStops])

  const nearbyHotels: NearbyHotel[] = useMemo(() => {
    if (!nearbyAnchor) return []
    return filtered.filter(h => h.id !== nearbyAnchor.id)
      .map(h => ({ ...h, dist: distanceMiles(nearbyAnchor.lat, nearbyAnchor.lng, h.lat, h.lng) }))
      .filter(h => h.dist <= nearbyRadius).sort((a, b) => a.dist - b.dist)
  }, [nearbyAnchor, nearbyRadius, filtered])

  const nearbyIds = useMemo(() => new Set(nearbyHotels.map(h => h.id)), [nearbyHotels])

  // Handlers
  function handleMarkerClick(hotel: MapHotel) {
    if (routeMode) {
      if (routeStopIds.has(hotel.id)) setRouteStops(p => p.filter(h => h.id !== hotel.id))
      else setRouteStops(p => [...p, hotel])
      setRouteResult(null)
    } else if (nearbyMode) {
      setNearbyAnchor(hotel); setSelectedHotel(null)
    } else setSelectedHotel(hotel)
  }

  async function handleOptimize() {
    if (routeStops.length < 2) return; setIsOptimizing(true)
    const res = await optimizeRoute(routeStops)
    if (res?.optimizedOrder) { setRouteStops(res.optimizedOrder.map(i => routeStops[i])); setRouteResult(res) }
    else if (res) setRouteResult(res)
    setIsOptimizing(false)
  }

  function handleSearchSelect(h: MapHotel) {
    setFlyTarget({ lat: h.lat, lng: h.lng, zoom: 15, t: Date.now() }); setSelectedHotel(h); setSearchOpen(false); setSearchQuery('')
  }

  function exitRoute() { setRouteMode(false); setRouteStops([]); setRouteResult(null) }
  function exitNearby() { setNearbyMode(false); setNearbyAnchor(null) }

  function activateMode(mode: 'route' | 'nearby' | 'heat') {
    if (mode !== 'route') exitRoute(); if (mode !== 'nearby') exitNearby(); if (mode !== 'heat') setHeatmapOn(false)
    setSelectedHotel(null)
    if (mode === 'route') setRouteMode(true)
    if (mode === 'nearby') setNearbyMode(true)
    if (mode === 'heat') setHeatmapOn(true)
  }

  useEffect(() => { if (searchOpen) searchRef.current?.focus() }, [searchOpen])

  useEffect(() => {
    const fn = (e: KeyboardEvent) => {
      if (e.key === 'Escape') { if (searchOpen) setSearchOpen(false); if (isFullscreen) setIsFullscreen(false) }
      if (e.key === '/' && !searchOpen && !(e.target instanceof HTMLInputElement)) { e.preventDefault(); setSearchOpen(true) }
    }
    window.addEventListener('keydown', fn); return () => window.removeEventListener('keydown', fn)
  }, [searchOpen, isFullscreen])

  return (
    <div className={cn('flex flex-col', isFullscreen ? 'fixed inset-0 z-[9999] bg-white' : 'h-full')}>
      {/* ═══ TOP BAR ═══ */}
      <div className="px-4 pt-3 pb-2 flex-shrink-0">
        <div className="flex items-center justify-between gap-2">
          <div className="flex items-center gap-3">
            <StatPill icon={MapPin} value={stats.total} label="Hotels" color="text-navy-600" bg="bg-navy-50" />
            <StatPill icon={Users} value={stats.clients} label="Clients" color="text-emerald-600" bg="bg-emerald-50" />
            <StatPill icon={Eye} value={stats.prospects} label="Prospects" color="text-blue-600" bg="bg-blue-50" />
          </div>

          <div className="flex items-center gap-1.5">
            {/* Color Mode */}
            <div className="flex bg-stone-100 rounded-lg p-0.5 mr-0.5">
              {([['type', 'Type', Users], ['revenue', '$', DollarSign], ['tier', 'Tier', Layers]] as [ColorMode, string, any][]).map(([k, lbl, Ic]) => (
                <button key={k} onClick={() => setColorMode(k)} className={cn('flex items-center gap-1 px-2 py-1.5 text-2xs font-semibold rounded-md transition', colorMode === k ? 'bg-white text-navy-900 shadow-sm' : 'text-stone-500 hover:text-stone-700')}>
                  <Ic className="w-3 h-3" />{lbl}
                </button>
              ))}
            </div>

            {/* Tiles */}
            <div className="flex bg-stone-100 rounded-lg p-0.5">
              {(Object.keys(TILES) as TileStyle[]).map(k => (
                <button key={k} onClick={() => setTileStyle(k)} className={cn('px-2 py-1.5 text-2xs font-semibold rounded-md transition', tileStyle === k ? 'bg-white text-navy-900 shadow-sm' : 'text-stone-500 hover:text-stone-700')}>
                  {TILES[k].label}
                </button>
              ))}
            </div>

            <ToolBtn active={heatmapOn} color="red" icon={Flame} label="Heat" onClick={() => heatmapOn ? setHeatmapOn(false) : activateMode('heat')} />
            <ToolBtn active={nearbyMode} color="violet" icon={Radar} label="Nearby" onClick={() => nearbyMode ? exitNearby() : activateMode('nearby')} />
            <ToolBtn active={routeMode} color="orange" icon={Route} label="Route" onClick={() => routeMode ? exitRoute() : activateMode('route')} />
            <ToolBtn active={showGap} color="navy" icon={BarChart3} label="Zones" onClick={() => setShowGap(!showGap)} />

            <button onClick={() => setShowFilters(!showFilters)} className={cn('flex items-center gap-1 px-2.5 py-2 text-xs font-semibold rounded-lg transition', hasFilters ? 'bg-navy-900 text-white' : 'bg-white border border-stone-200 text-stone-600 hover:bg-stone-50')}>
              <Filter className="w-3.5 h-3.5" />Filters
            </button>
            {hasFilters && <button onClick={() => setFilters(DEFAULT_FILTERS)} className="flex items-center gap-1 px-2 py-2 text-xs font-semibold text-red-500 bg-red-50 border border-red-200 rounded-lg hover:bg-red-100 transition"><X className="w-3 h-3" /></button>}

            <button onClick={() => setSearchOpen(!searchOpen)} className="px-2.5 py-2 text-xs font-semibold bg-white border border-stone-200 text-stone-600 hover:bg-stone-50 rounded-lg transition" title="Search (/)"><Search className="w-3.5 h-3.5" /></button>
            <button onClick={() => setIsFullscreen(f => !f)} className="px-2.5 py-2 text-xs font-semibold bg-white border border-stone-200 text-stone-600 hover:bg-stone-50 rounded-lg transition">
              {isFullscreen ? <Minimize2 className="w-3.5 h-3.5" /> : <Maximize2 className="w-3.5 h-3.5" />}
            </button>
          </div>
        </div>

        {/* Filters */}
        {showFilters && (
          <div className="mt-2 flex items-center gap-3">
            <select value={filters.type} onChange={e => setFilters(f => ({ ...f, type: e.target.value as MapFilters['type'] }))} className="h-8 px-2.5 text-xs bg-white border border-stone-200 rounded-lg outline-none focus:border-navy-400">
              <option value="">All Hotels</option><option value="client">Clients</option><option value="prospect">Prospects</option>
            </select>
            <select value={filters.tier} onChange={e => setFilters(f => ({ ...f, tier: e.target.value }))} className="h-8 px-2.5 text-xs bg-white border border-stone-200 rounded-lg outline-none focus:border-navy-400">
              <option value="">All Tiers</option>
              <option value="tier1_ultra_luxury">T1 — Ultra Luxury</option><option value="tier2_luxury">T2 — Luxury</option>
              <option value="tier3_upper_upscale">T3 — Upper Upscale</option><option value="tier4_upscale">T4 — Upscale</option>
            </select>
            <select value={filters.zone} onChange={e => setFilters(f => ({ ...f, zone: e.target.value }))} className="h-8 px-2.5 text-xs bg-white border border-stone-200 rounded-lg outline-none focus:border-navy-400">
              <option value="">All Zones</option>
              {zones.map(z => <option key={z} value={z}>{z}</option>)}
            </select>
          </div>
        )}

        {/* Search */}
        {searchOpen && (
          <div className="mt-2 relative z-[2000]">
            <div className="relative">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-stone-400" />
              <input ref={searchRef} value={searchQuery} onChange={e => setSearchQuery(e.target.value)} placeholder="Search hotel name, brand, or city…" className="w-full h-10 pl-10 pr-10 text-sm bg-white border border-stone-200 rounded-lg outline-none focus:border-navy-400 focus:ring-2 focus:ring-navy-100 transition placeholder:text-stone-400" />
              <button onClick={() => { setSearchOpen(false); setSearchQuery('') }} className="absolute right-3 top-1/2 -translate-y-1/2 text-stone-400 hover:text-stone-600"><X className="w-4 h-4" /></button>
            </div>
            {searchResults.length > 0 && (
              <div className="absolute top-full left-0 right-0 mt-1 bg-white border border-stone-200 rounded-xl shadow-xl max-h-72 overflow-y-auto z-[2001]">
                {searchResults.map(h => (
                  <button key={h.id} onClick={() => handleSearchSelect(h)} className="w-full flex items-center gap-3 px-4 py-2.5 hover:bg-stone-50 transition text-left">
                    <div className={cn('w-2.5 h-2.5 rounded-full flex-shrink-0', h.is_client ? 'bg-emerald-500' : 'bg-blue-500')} />
                    <div className="flex-1 min-w-0">
                      <p className="text-sm font-semibold text-navy-900 truncate">{h.name}</p>
                      <p className="text-2xs text-stone-400">{[h.city, h.state].filter(Boolean).join(', ')}{h.brand ? ` · ${h.brand}` : ''}</p>
                    </div>
                    {h.room_count && <span className="text-2xs text-stone-400">{h.room_count} rooms</span>}
                  </button>
                ))}
              </div>
            )}
            {searchQuery && !searchResults.length && (
              <div className="absolute top-full left-0 right-0 mt-1 bg-white border border-stone-200 rounded-xl shadow-xl p-4 text-center">
                <MapPinOff className="w-5 h-5 text-stone-300 mx-auto mb-1" /><p className="text-sm text-stone-400">No hotels found</p>
              </div>
            )}
          </div>
        )}
      </div>

      {/* ═══ MAIN CONTENT ═══ */}
      <div className="flex-1 flex overflow-hidden px-4 pb-3 gap-3">
        {/* Map */}
        <div className="flex-1 rounded-xl overflow-hidden border border-slate-200/80 shadow-sm relative">
          {isLoading ? (
            <div className="h-full flex items-center justify-center bg-slate-50">
              <div className="w-8 h-8 border-3 border-navy-200 border-t-navy-600 rounded-full animate-spin" />
            </div>
          ) : (
            <MapContainer center={[27.5, -81.8]} zoom={7} className="h-full w-full" zoomControl={false} style={{ background: tileStyle === 'dark' ? '#1a1a2e' : '#f1f5f9' }}>
              <ZoomControl position="topright" />
              <TileLayer key={tileStyle} attribution={TILES[tileStyle].attribution} url={TILES[tileStyle].url} />

              <HeatmapLayer hotels={filtered} visible={heatmapOn} />
              {flyTarget && <FlyTo lat={flyTarget.lat} lng={flyTarget.lng} zoom={flyTarget.zoom} trigger={flyTarget.t} />}
              <NearbyClick active={nearbyMode} onClear={() => setNearbyAnchor(null)} />

              {/* Clustered markers */}
              <MarkerClusterGroup chunkedLoading maxClusterRadius={50} spiderfyOnMaxZoom showCoverageOnHover={false}
                disableClusteringAtZoom={nearbyMode && nearbyAnchor ? 1 : undefined}
                iconCreateFunction={(cluster: any) => {
                  const c = cluster.getChildCount(); const s = c > 100 ? 44 : c > 30 ? 38 : 32
                  return L.divIcon({ html: `<div style="width:${s}px;height:${s}px;border-radius:50%;background:rgba(15,23,42,.85);border:2px solid rgba(255,255,255,.4);display:flex;align-items:center;justify-content:center;font:700 12px sans-serif;color:#fff;box-shadow:0 2px 8px rgba(0,0,0,.3)">${c}</div>`, className: '', iconSize: L.point(s, s) })
                }}
              >
                {filtered.filter(h => !routeStopIds.has(h.id)).map(hotel => {
                  const color = getMarkerColor(hotel, colorMode, maxRevenue)
                  const sz = markerSize(hotel.room_count)
                  const isNH = nearbyMode && nearbyAnchor && nearbyIds.has(hotel.id)
                  const isDimmed = nearbyMode && nearbyAnchor && !nearbyIds.has(hotel.id) && hotel.id !== nearbyAnchor?.id
                  return (
                    <Marker key={hotel.id} position={[hotel.lat, hotel.lng]}
                      icon={hotel.id === nearbyAnchor?.id ? anchorIcon : createIcon(isDimmed ? '#cbd5e1' : color, isNH ? sz + 4 : sz)}
                      opacity={isDimmed ? 0.3 : 1}
                      eventHandlers={{ click: () => handleMarkerClick(hotel) }}
                    >
                      {!routeMode && !nearbyMode && <Popup><HotelPopup hotel={hotel} /></Popup>}
                    </Marker>
                  )
                })}
              </MarkerClusterGroup>

              {/* Route stops */}
              {routeStops.map((h, i) => (
                <Marker key={`r-${h.id}`} position={[h.lat, h.lng]} icon={createIcon('#f97316', 14, true, i + 1)} eventHandlers={{ click: () => handleMarkerClick(h) }} zIndexOffset={1000} />
              ))}

              {/* Route line */}
              {routeResult && <Polyline positions={routeResult.coords} pathOptions={{ color: '#f97316', weight: 4, opacity: 0.8, dashArray: '8, 6' }} />}

              {/* Nearby circle */}
              {nearbyMode && nearbyAnchor && (
                <Circle center={[nearbyAnchor.lat, nearbyAnchor.lng]} radius={nearbyRadius * MILES_TO_METERS}
                  pathOptions={{ color: '#7c3aed', fillColor: '#7c3aed', fillOpacity: 0.06, weight: 2, dashArray: '6, 4' }} />
              )}

              {/* Office */}
              {showOffice && (
                <Marker position={[OFFICE_LAT, OFFICE_LNG]} icon={officeIcon} zIndexOffset={2000}>
                  <Popup><div className="min-w-[180px]"><p className="font-bold text-sm">★ {OFFICE_LABEL}</p><p className="text-xs text-gray-500 mt-0.5">12323 SW 132nd Ct</p><p className="text-xs text-gray-500">Miami, FL 33186</p></div></Popup>
                </Marker>
              )}

              {/* Highlight selected hotel — pulsing ring */}
              {selectedHotel && !routeMode && !nearbyMode && (
                <Marker
                  position={[selectedHotel.lat, selectedHotel.lng]}
                  icon={highlightIcon}
                  zIndexOffset={3000}
                  interactive={false}
                />
              )}

              <FitBoundsToMarkers hotels={filtered} />
            </MapContainer>
          )}

          {/* My Location btn */}
          <button onClick={() => { setShowOffice(true); setFlyTarget({ lat: OFFICE_LAT, lng: OFFICE_LNG, zoom: 13, t: Date.now() }) }}
            className="absolute top-3 left-3 z-[1000] w-9 h-9 bg-white rounded-lg border border-stone-200 shadow-md flex items-center justify-center text-stone-600 hover:text-navy-600 transition" title="Go to JA Uniforms office">
            <Crosshair className="w-4 h-4" />
          </button>

          {/* Mode banners */}
          {routeMode && !routeResult && (
            <div className="absolute top-3 left-1/2 -translate-x-1/2 bg-orange-500 text-white px-4 py-2 rounded-lg shadow-lg z-[1000] flex items-center gap-2 text-xs font-semibold">
              <Route className="w-4 h-4" />Click hotels to add stops ({routeStops.length} selected)
            </div>
          )}
          {nearbyMode && !nearbyAnchor && (
            <div className="absolute top-3 left-1/2 -translate-x-1/2 bg-violet-500 text-white px-4 py-2 rounded-lg shadow-lg z-[1000] flex items-center gap-2 text-xs font-semibold">
              <Radar className="w-4 h-4" />Click a hotel to see nearby properties
            </div>
          )}

          {/* Heatmap legend */}
          {heatmapOn && (
            <div className="absolute bottom-4 left-1/2 -translate-x-1/2 z-[1000]">
              <div className="bg-white/90 backdrop-blur-sm rounded-lg px-4 py-2 shadow-lg border border-stone-200 flex items-center gap-3">
                <span className="text-2xs font-semibold text-stone-500 uppercase">Revenue Density</span>
                <div className="w-32 h-2.5 rounded-full" style={{ background: 'linear-gradient(to right,#3b82f6,#d4a853,#dc2626)' }} />
                <span className="text-2xs text-stone-400">Low → High</span>
              </div>
            </div>
          )}

          {/* Color legend */}
          <div className="absolute bottom-4 right-3 z-[1000]">
            <div className="bg-white/90 backdrop-blur-sm rounded-lg px-3 py-2 shadow border border-stone-200 space-y-1">
              {colorMode === 'type' && <><Dot c="#059669" l="Client" /><Dot c="#2563eb" l="Prospect" /></>}
              {colorMode === 'revenue' && <><Dot c="#3b82f6" l="Low Revenue" /><Dot c="#d4a853" l="Mid Revenue" /><Dot c="#dc2626" l="High Revenue" /></>}
              {colorMode === 'tier' && <><Dot c="#d4a853" l="Ultra Luxury" /><Dot c="#c49a3c" l="Luxury" /><Dot c="#3e638c" l="Upper Upscale" /><Dot c="#6b665e" l="Upscale" /></>}
              {showOffice && <Dot c="#0f1d32" l="JA Office" diamond />}
            </div>
          </div>

          {/* Route Panel */}
          {routeMode && routeStops.length > 0 && (
            <div className="absolute top-14 left-3 w-72 bg-white rounded-xl border border-slate-200 shadow-xl z-[1000] max-h-[70%] flex flex-col">
              <div className="px-4 py-3 border-b border-slate-100 flex items-center justify-between">
                <div className="flex items-center gap-2"><Route className="w-4 h-4 text-orange-500" /><span className="text-sm font-bold text-navy-900">Route Planner</span></div>
                <button onClick={() => { setRouteStops([]); setRouteResult(null) }} className="p-1 text-stone-400 hover:text-red-500 transition"><Trash2 className="w-3.5 h-3.5" /></button>
              </div>
              <div className="flex-1 overflow-y-auto p-3 space-y-1.5">
                {routeStops.map((h, i) => (
                  <div key={h.id} className="flex items-center gap-2 group">
                    <span className="w-5 h-5 rounded-full bg-orange-500 text-white text-2xs font-bold flex items-center justify-center flex-shrink-0">{i + 1}</span>
                    <div className="flex-1 min-w-0"><p className="text-xs font-semibold text-navy-900 truncate">{h.name}</p><p className="text-2xs text-stone-400">{h.city}</p></div>
                    <button onClick={() => { setRouteStops(p => p.filter(x => x.id !== h.id)); setRouteResult(null) }} className="p-0.5 text-stone-300 hover:text-red-500 opacity-0 group-hover:opacity-100 transition"><X className="w-3 h-3" /></button>
                  </div>
                ))}
              </div>
              {routeResult && (
                <div className="px-4 py-2.5 bg-orange-50 border-t border-orange-100 flex items-center gap-4">
                  <div className="flex items-center gap-1.5"><Milestone className="w-3.5 h-3.5 text-orange-600" /><span className="text-xs font-bold text-orange-800">{(routeResult.distance * 0.621371).toFixed(1)} mi</span></div>
                  <div className="flex items-center gap-1.5"><Clock className="w-3.5 h-3.5 text-orange-600" /><span className="text-xs font-bold text-orange-800">{routeResult.duration >= 60 ? `${Math.floor(routeResult.duration / 60)}h ${Math.round(routeResult.duration % 60)}m` : `${Math.round(routeResult.duration)}m`}</span></div>
                </div>
              )}
              <div className="px-3 py-2.5 border-t border-slate-100 space-y-2">
                <button onClick={handleOptimize} disabled={routeStops.length < 2 || isOptimizing} className="w-full flex items-center justify-center gap-1.5 px-3 py-2 text-xs font-semibold rounded-lg bg-orange-500 text-white hover:bg-orange-600 transition disabled:opacity-40">
                  {isOptimizing ? <><div className="w-3.5 h-3.5 border-2 border-white/30 border-t-white rounded-full animate-spin" />Optimizing...</> : <><Navigation className="w-3.5 h-3.5" />{routeStops.length < 2 ? 'Add 2+ stops' : `Optimize ${routeStops.length} stops`}</>}
                </button>
                {routeResult && routeStops.length >= 2 && (
                  <a href={buildGoogleMapsUrl(routeStops)} target="_blank" rel="noopener noreferrer" className="w-full flex items-center justify-center gap-1.5 px-3 py-2 text-xs font-semibold rounded-lg bg-white border border-stone-200 text-navy-700 hover:bg-stone-50 transition">
                    <ExternalLink className="w-3.5 h-3.5" />Open in Google Maps
                  </a>
                )}
              </div>
            </div>
          )}

          {/* Nearby Panel */}
          {nearbyMode && nearbyAnchor && (
            <div className="absolute top-14 left-3 w-72 bg-white rounded-xl border border-slate-200 shadow-xl z-[1000] max-h-[70%] flex flex-col">
              <div className="px-4 py-3 border-b border-slate-100">
                <div className="flex items-center justify-between mb-2">
                  <div className="flex items-center gap-2"><Radar className="w-4 h-4 text-violet-500" /><span className="text-sm font-bold text-navy-900">Nearby Hotels</span></div>
                  <button onClick={() => setNearbyAnchor(null)} className="p-1 text-stone-400 hover:text-red-500 transition"><X className="w-3.5 h-3.5" /></button>
                </div>
                <p className="text-2xs text-stone-400 truncate mb-2">From: <span className="font-semibold text-stone-600">{nearbyAnchor.name}</span></p>
                <div className="flex gap-1">
                  {([5, 10, 20] as NearbyRadius[]).map(r => (
                    <button key={r} onClick={() => setNearbyRadius(r)} className={cn('flex-1 py-1.5 text-2xs font-bold rounded-md transition', nearbyRadius === r ? 'bg-violet-500 text-white' : 'bg-stone-100 text-stone-500 hover:bg-stone-200')}>{r} mi</button>
                  ))}
                </div>
              </div>
              <div className="flex-1 overflow-y-auto">
                {!nearbyHotels.length ? (
                  <div className="p-4 text-center"><MapPinOff className="w-5 h-5 text-stone-300 mx-auto mb-1" /><p className="text-xs text-stone-400">No hotels within {nearbyRadius} miles</p></div>
                ) : (
                  <div className="p-2 space-y-0.5">
                    {nearbyHotels.map(h => (
                      <button key={h.id} onClick={() => setFlyTarget({ lat: h.lat, lng: h.lng, zoom: 15, t: Date.now() })} className="w-full flex items-center gap-2 px-2 py-2 rounded-lg hover:bg-stone-50 transition text-left">
                        <div className={cn('w-2 h-2 rounded-full flex-shrink-0', h.is_client ? 'bg-emerald-500' : 'bg-blue-500')} />
                        <div className="flex-1 min-w-0"><p className="text-xs font-semibold text-navy-900 truncate">{h.name}</p><p className="text-2xs text-stone-400">{h.city} · {h.room_count || '?'} rooms</p></div>
                        <span className="text-2xs font-bold text-violet-500 flex-shrink-0">{h.dist.toFixed(1)} mi</span>
                      </button>
                    ))}
                  </div>
                )}
              </div>
              <div className="px-4 py-2.5 bg-violet-50 border-t border-violet-100 text-center">
                <span className="text-xs font-bold text-violet-700">{nearbyHotels.length} hotels</span>
                <span className="text-2xs text-violet-500 ml-1">within {nearbyRadius} mi</span>
              </div>
            </div>
          )}

          {/* Hotel detail card */}
          {!routeMode && !nearbyMode && selectedHotel && (
            <div className="absolute bottom-4 left-4 right-4 md:left-auto md:right-4 md:w-96 bg-white rounded-xl border border-slate-200 shadow-xl z-[1000]">
              <div className="p-4">
                <div className="flex items-start justify-between gap-3">
                  <div className="min-w-0 flex-1">
                    <h3 className="text-sm font-bold text-navy-900 truncate">{selectedHotel.name}</h3>
                    {selectedHotel.brand && <p className="text-xs text-stone-400 mt-0.5">{selectedHotel.brand}</p>}
                  </div>
                  <div className="flex items-center gap-2 flex-shrink-0">
                    <span className={cn('px-2 py-0.5 rounded-full text-2xs font-bold', selectedHotel.is_client ? 'bg-emerald-50 text-emerald-600' : 'bg-blue-50 text-blue-600')}>
                      {selectedHotel.is_client ? 'Client' : 'Prospect'}
                    </span>
                    <button onClick={() => setSelectedHotel(null)} className="p-1 text-stone-400 hover:text-stone-600"><X className="w-3.5 h-3.5" /></button>
                  </div>
                </div>
                <div className="grid grid-cols-3 gap-3 mt-3">
                  <InfoCell icon={MapPin} label="Location" value={[selectedHotel.city, selectedHotel.state].filter(Boolean).join(', ') || '—'} />
                  <InfoCell icon={Building2} label="Rooms" value={selectedHotel.room_count ? String(selectedHotel.room_count) : '—'} />
                  <InfoCell icon={DollarSign} label="Annual" value={fmtRevenue(selectedHotel.revenue_annual)} />
                </div>
                <div className="mt-2.5 flex items-center gap-1.5 flex-wrap">
                  {selectedHotel.brand_tier && <span className={cn('inline-flex px-2 py-0.5 rounded text-2xs font-bold', getTierColor(selectedHotel.brand_tier))}>{getTierLabel(selectedHotel.brand_tier)}</span>}
                  {selectedHotel.zone && <span className="inline-flex px-2 py-0.5 rounded text-2xs font-medium bg-stone-100 text-stone-500">{selectedHotel.zone}</span>}
                  <span className="inline-flex px-2 py-0.5 rounded text-2xs font-medium bg-navy-50 text-navy-600">
                    {distanceMiles(OFFICE_LAT, OFFICE_LNG, selectedHotel.lat, selectedHotel.lng).toFixed(1)} mi from HQ
                  </span>
                </div>
                {selectedHotel.phone && <a href={`tel:${selectedHotel.phone}`} className="flex items-center gap-1.5 mt-2.5 text-xs text-navy-600 hover:underline"><Phone className="w-3 h-3" />{selectedHotel.phone}</a>}
              </div>
            </div>
          )}
        </div>

        {/* ═══ GAP ANALYSIS SIDE PANEL ═══ */}
        {showGap && (
          <div className="w-80 bg-white rounded-xl border border-slate-200/80 shadow-sm overflow-hidden flex flex-col flex-shrink-0">
            <div className="px-4 py-3 border-b border-slate-100 flex items-center justify-between">
              <div className="flex items-center gap-2"><BarChart3 className="w-4 h-4 text-navy-600" /><span className="text-sm font-bold text-navy-900">Zone Analysis</span></div>
              <button onClick={() => setShowGap(false)} className="p-1 text-stone-400 hover:text-stone-600"><X className="w-3.5 h-3.5" /></button>
            </div>
            <div className="flex-1 overflow-y-auto divide-y divide-slate-100">
              {zoneStats.map(z => {
                const pct = z.total > 0 ? Math.round((z.clients / z.total) * 100) : 0
                return (
                  <button key={z.zone} onClick={() => setFilters(f => ({ ...f, zone: f.zone === z.zone ? '' : z.zone }))} className={cn('w-full text-left px-4 py-3 hover:bg-stone-50 transition', filters.zone === z.zone && 'bg-navy-50')}>
                    <div className="flex items-center justify-between mb-1.5"><span className="text-xs font-bold text-navy-900">{z.zone}</span><span className="text-2xs font-semibold text-stone-400">{z.total} hotels</span></div>
                    <div className="flex h-1.5 rounded-full overflow-hidden bg-stone-100 mb-2"><div className="bg-emerald-500" style={{ width: `${pct}%` }} /><div className="bg-blue-400 flex-1" /></div>
                    <div className="grid grid-cols-3 gap-2 text-2xs">
                      <div><span className="text-stone-400">Clients</span><p className="font-bold text-emerald-600">{z.clients}</p></div>
                      <div><span className="text-stone-400">Prospects</span><p className="font-bold text-blue-600">{z.prospects}</p></div>
                      <div><span className="text-stone-400">Revenue</span><p className="font-bold text-navy-700">{fmtRevenue(z.totalRevenue)}</p></div>
                    </div>
                  </button>
                )
              })}
            </div>
            <div className="px-4 py-3 bg-stone-50 border-t border-slate-100 grid grid-cols-2 gap-3 text-2xs">
              <div><span className="text-stone-400">Total Revenue</span><p className="text-sm font-bold text-navy-900">{fmtRevenue(zoneStats.reduce((s, z) => s + z.totalRevenue, 0))}</p></div>
              <div><span className="text-stone-400">Total Rooms</span><p className="text-sm font-bold text-navy-900">{zoneStats.reduce((s, z) => s + z.rooms, 0).toLocaleString()}</p></div>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}

/* ═══════════════════════════════════════════════════
   SUB COMPONENTS
   ═══════════════════════════════════════════════════ */

function HotelPopup({ hotel }: { hotel: MapHotel }) {
  return (<div className="min-w-[200px]">
    <p className="font-bold text-sm leading-snug">{hotel.name}</p>
    {hotel.brand && <p className="text-xs text-gray-500">{hotel.brand}</p>}
    <div className="mt-1.5 space-y-0.5 text-xs text-gray-600">
      <p>{[hotel.city, hotel.state].filter(Boolean).join(', ')}</p>
      {hotel.room_count && <p>{hotel.room_count} rooms</p>}
      {hotel.revenue_annual && <p>Annual: {fmtRevenue(hotel.revenue_annual)}</p>}
    </div>
  </div>)
}

function StatPill({ icon: Icon, value, label, color, bg }: { icon: React.ElementType; value: number; label: string; color: string; bg: string }) {
  return (<div className="flex items-center gap-2 px-3 py-1.5 rounded-lg border border-stone-200 bg-white">
    <div className={cn('w-6 h-6 rounded-md flex items-center justify-center flex-shrink-0', bg)}><Icon className={cn('w-3.5 h-3.5', color)} /></div>
    <span className="text-sm font-bold text-navy-900 tabular-nums">{value}</span>
    <span className="text-2xs text-stone-400 font-semibold uppercase">{label}</span>
  </div>)
}

function InfoCell({ icon: Icon, label, value }: { icon: React.ElementType; label: string; value: string }) {
  return (<div><div className="flex items-center gap-1 mb-0.5"><Icon className="w-3 h-3 text-stone-400" /><span className="text-2xs text-stone-400 font-semibold uppercase">{label}</span></div><span className="text-xs font-semibold text-navy-800">{value}</span></div>)
}

function ToolBtn({ active, color, icon: Icon, label, onClick }: { active: boolean; color: string; icon: React.ElementType; label: string; onClick: () => void }) {
  const colors: Record<string, string> = { red: 'bg-red-500 hover:bg-red-600', orange: 'bg-orange-500 hover:bg-orange-600', violet: 'bg-violet-500 hover:bg-violet-600', navy: 'bg-navy-900' }
  return (<button onClick={onClick} className={cn('flex items-center gap-1 px-2.5 py-2 text-xs font-semibold rounded-lg transition', active ? `${colors[color]} text-white` : 'bg-white border border-stone-200 text-stone-600 hover:bg-stone-50')}>
    <Icon className="w-3.5 h-3.5" />{label}
  </button>)
}

function Dot({ c, l, diamond }: { c: string; l: string; diamond?: boolean }) {
  return (<div className="flex items-center gap-1.5">
    <span className={cn('w-3 h-3 flex-shrink-0', diamond ? 'rotate-45 rounded-sm' : 'rounded-full')} style={{ backgroundColor: c }} />
    <span className="text-2xs text-stone-500 font-medium">{l}</span>
  </div>)
}
