/**
 * Existing Hotels page
 * =====================
 * Mirrors the New Hotels (Dashboard) page exactly — same stat cards,
 * same Pipeline/Approved/Rejected tabs, same sortable table columns,
 * same detail panel layout. Schema parity comes from migration 018.
 *
 * Differences from New Hotels:
 *   - No Expired tab. Expired leads transfer to existing_hotels via the
 *     manual `scripts/transfer_to_existing.py` PowerShell script and
 *     land here on the Pipeline tab as status='new'.
 *   - No timeline_label column (existing hotels are operating, not
 *     pre-opening, so timeline labels are meaningless).
 *   - lead_score will be empty until the new existing-hotels scoring
 *     model is built — UI shows "—" for empty scores.
 *
 * Feature parity preserved:
 *   - Smart Fill (basePath="/api/existing-hotels")
 *   - Run Enrichment (basePath="/api/existing-hotels")
 *   - Wiza email lookup per contact (Find Email button)
 *   - Contacts P1-P4 priority badges + score-breakdown popover
 *   - Evidence panel with trust-tier styling + staleness check
 *   - Scope toggle (5 scopes: hotel_specific → chain_area →
 *     management_corporate → chain_corporate → owner)
 *   - Inline contact edit/delete, save, set primary, Add Contact form
 *   - Editable rejection reason on Rejected tab
 *
 * Created: 2026-04-28
 */
import { useState, useEffect, useMemo } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import api from '@/api/client'
import {
  fetchExistingHotels, fetchExistingHotel, fetchExistingHotelStats,
  fetchHotelContacts, approveExistingHotel, rejectExistingHotel,
  restoreExistingHotel, editExistingHotel,
  saveHotelContact, deleteHotelContact, setPrimaryHotelContact,
  updateHotelContact, addHotelContact, toggleHotelContactScope,
  enrichHotelContactEmail, getHotelEnrichmentStatus,
  type ExistingHotel, type ExistingHotelStats,
} from '@/api/existingHotels'
import type { Contact } from '@/api/types'
import ConfirmDialog from '@/components/ui/ConfirmDialog'
import EnrichProgress from '@/components/leads/EnrichProgress'
import SmartFillProgress from '@/components/leads/SmartFillProgress'
import {
  cn, getTierColor, getTierLabel, getScoreColor, getScoreRing,
  formatLocation, formatOpening, formatDate, relativeDate,
} from '@/lib/utils'
import {
  Building2, MapPin, Users, Eye, Search, X,
  ChevronLeft, ChevronRight, ChevronUp, ChevronDown, ChevronsUpDown,
  Phone, Globe, User, ExternalLink, Loader2,
  CheckCircle2, XCircle, Undo2, Mail, Linkedin, DollarSign,
  Inbox, Calendar, Layers, Star, Bookmark, BookmarkCheck,
  Pencil, Check, Zap, RefreshCw, Trash2, Save, SlidersHorizontal,
  Download, Link2,
} from 'lucide-react'

/* ═════════════════════════════════════════════════════════
   TYPES & HELPERS
   ═════════════════════════════════════════════════════════ */

type PipelineTab = 'pipeline' | 'approved' | 'rejected'

interface Filters {
  search: string
  state: string
  brand_tier: string
  is_client: string
  zone: string
  sort: string
}

const STATUS_BY_TAB: Record<PipelineTab, string> = {
  pipeline: 'new',
  approved: 'approved',
  rejected: 'rejected',
}

const DEFAULT_FILTERS: Filters = {
  search: '',
  state: '',
  brand_tier: '',
  is_client: '',
  zone: '',
  sort: 'name_az',
}

const STATE_NAME_TO_CODE: Record<string, string> = {
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
  "Bahamas":"BS","Jamaica":"JM","Dominican Republic":"DO","Puerto Rico":"PR",
  "Cayman Islands":"CY","Turks and Caicos":"TC","Bermuda":"BM",
  "US Virgin Islands":"VI","British Virgin Islands":"VG","Barbados":"BB",
  "Aruba":"AW","Curaçao":"CW","Saint Lucia":"LC","Antigua and Barbuda":"AG",
  "Anguilla":"AI","St. Kitts and Nevis":"KN","St. Martin / Sint Maarten":"SX",
  "Grenada":"GD","Dominica":"DM","Trinidad and Tobago":"TT",
  "St. Vincent & Grenadines":"VC",
}

function fmtRevenue(n: number | null | undefined): string {
  if (!n) return '—'
  if (n >= 1_000_000) return `$${(n / 1_000_000).toFixed(1)}M`
  if (n >= 1_000) return `$${Math.round(n / 1_000)}K`
  return `$${n.toLocaleString()}`
}

function safe(val: any): string {
  if (val === null || val === undefined) return '—'
  if (typeof val === 'string') return val
  if (typeof val === 'number' || typeof val === 'boolean') return String(val)
  return ''
}

interface ColDef {
  key: string
  label: string
  sortAsc?: string
  sortDesc?: string
  width?: string
}

const COLUMNS: ColDef[] = [
  { key: 'score',    label: 'Score',     sortAsc: 'score_low',    sortDesc: 'score_high',    width: 'w-16' },
  { key: 'hotel',    label: 'Hotel',     sortAsc: 'name_az',      sortDesc: 'name_za' },
  { key: 'tier',     label: 'Tier',      sortAsc: 'tier_asc',     sortDesc: 'tier_desc',     width: 'w-16' },
  { key: 'location', label: 'Location',  sortAsc: 'location_az',  sortDesc: 'location_za' },
  { key: 'zone',     label: 'Zone',                                                          width: 'w-28' },
  { key: 'opening',  label: 'Opening',   sortAsc: 'opening_soon', sortDesc: 'opening_late',  width: 'w-24' },
  { key: 'revenue',  label: 'Potential', sortAsc: 'revenue_low',  sortDesc: 'revenue_high',  width: 'w-24' },
  { key: 'type',     label: 'Type',                                                          width: 'w-20' },
  { key: 'added',    label: 'Added',     sortAsc: 'oldest',       sortDesc: 'newest',        width: 'w-20' },
]

const TIER_ORDER: Record<string, number> = {
  'tier1_ultra_luxury': 0, 'tier2_luxury': 1, 'tier3_upper_upscale': 2,
  'tier4_upscale': 3, 'tier4_low': 4, 'tier5_budget': 5,
}

function getNextSort(col: ColDef, current: string): string {
  if (!col.sortAsc || !col.sortDesc) return current
  if (current === col.sortDesc) return col.sortAsc
  if (current === col.sortAsc) return col.sortDesc
  return col.sortDesc
}

function getSortIcon(col: ColDef, current: string) {
  if (!col.sortAsc) return null
  if (current === col.sortAsc)  return <ChevronUp className="w-3 h-3 text-navy-600" />
  if (current === col.sortDesc) return <ChevronDown className="w-3 h-3 text-navy-600" />
  return <ChevronsUpDown className="w-3 h-3 text-stone-300 group-hover:text-stone-400" />
}

function sortHotels(hotels: ExistingHotel[], sort: string): ExistingHotel[] {
  const sorted = [...hotels]
  sorted.sort((a, b) => {
    const an = (a as any).hotel_name || (a as any).name || ''
    const bn = (b as any).hotel_name || (b as any).name || ''
    switch (sort) {
      case 'score_high':   return ((b.lead_score ?? 0) - (a.lead_score ?? 0))
      case 'score_low':    return ((a.lead_score ?? 0) - (b.lead_score ?? 0))
      case 'name_az':      return an.localeCompare(bn)
      case 'name_za':      return bn.localeCompare(an)
      case 'tier_asc': {
        const ta = TIER_ORDER[a.brand_tier || ''] ?? 99
        const tb = TIER_ORDER[b.brand_tier || ''] ?? 99
        return ta - tb
      }
      case 'tier_desc': {
        const ta = TIER_ORDER[a.brand_tier || ''] ?? 99
        const tb = TIER_ORDER[b.brand_tier || ''] ?? 99
        return tb - ta
      }
      case 'location_az':  return formatLocation(a).localeCompare(formatLocation(b))
      case 'location_za':  return formatLocation(b).localeCompare(formatLocation(a))
      case 'opening_soon': {
        const oa = a.opening_date || String(a.opening_year || 'zzzz')
        const ob = b.opening_date || String(b.opening_year || 'zzzz')
        return oa.localeCompare(ob)
      }
      case 'opening_late': {
        const oa = a.opening_date || String(a.opening_year || '')
        const ob = b.opening_date || String(b.opening_year || '')
        return ob.localeCompare(oa)
      }
      case 'revenue_high': return ((b.revenue_opening ?? 0) - (a.revenue_opening ?? 0))
      case 'revenue_low':  return ((a.revenue_opening ?? 0) - (b.revenue_opening ?? 0))
      case 'newest':       return new Date(b.created_at).getTime() - new Date(a.created_at).getTime()
      case 'oldest':       return new Date(a.created_at).getTime() - new Date(b.created_at).getTime()
      default:             return 0
    }
  })
  return sorted
}

function PriorityBadge({ label, reason }: { label?: string | null; reason?: string | null }) {
  if (!label) return null
  const style =
    label === 'P1' ? 'bg-emerald-100 text-emerald-700 border-emerald-300' :
    label === 'P2' ? 'bg-blue-100 text-blue-700 border-blue-300' :
    label === 'P3' ? 'bg-amber-100 text-amber-700 border-amber-300' :
                     'bg-stone-100 text-stone-500 border-stone-300'
  return (
    <span title={reason || label} className={cn('inline-flex items-center px-1.5 py-0.5 rounded text-2xs font-bold border', style)}>
      {label}
    </span>
  )
}

/* ═════════════════════════════════════════════════════════
   MAIN PAGE
   ═════════════════════════════════════════════════════════ */
export default function ExistingHotels() {
  const [page, setPage] = useState(1)
  const [filters, setFilters] = useState<Filters>(DEFAULT_FILTERS)
  const [selectedId, setSelectedId] = useState<number | null>(null)
  const [activeTab, setActiveTab] = useState<PipelineTab>('pipeline')
  const [exporting, setExporting] = useState(false)

  // Page-size persists across sessions via localStorage. Defaults to 25.
  // Separate key from the New Hotels page so each can be tuned independently.
  const [perPage, setPerPage] = useState<number>(() => {
    try {
      const saved = window.localStorage.getItem('slh.existing.perPage')
      const n = saved ? Number(saved) : NaN
      return [25, 50, 100].includes(n) ? n : 25
    } catch {
      return 25
    }
  })
  function handlePerPageChange(n: number) {
    setPerPage(n)
    setPage(1)
    try { window.localStorage.setItem('slh.existing.perPage', String(n)) } catch { /* silent */ }
  }

  const { data: stats } = useQuery<ExistingHotelStats>({
    queryKey: ['existing-hotels-stats'],
    queryFn: fetchExistingHotelStats,
    refetchInterval: 30_000,
    refetchIntervalInBackground: false,
    staleTime: 10_000,
  })

  const { data, isLoading } = useQuery({
    queryKey: ['existing-hotels', page, filters, activeTab, perPage],
    queryFn: () => fetchExistingHotels({
      page,
      per_page: perPage,
      sort: filters.sort,
      status: STATUS_BY_TAB[activeTab],
      search:     filters.search     || undefined,
      state:      filters.state      || undefined,
      brand_tier: filters.brand_tier || undefined,
      is_client:  filters.is_client  || undefined,
      zone:       filters.zone       || undefined,
    }),
    refetchInterval: 30_000,
    refetchIntervalInBackground: false,
    staleTime: 10_000,
  })

  function handleFilterChange(key: keyof Filters, value: string) {
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
    // Reset filters EXCEPT search
    setFilters({ ...DEFAULT_FILTERS, search: filters.search })
  }

  function handleSort(sort: string) {
    setFilters(prev => ({ ...prev, sort }))
    setPage(1)
  }

  function handleStatClick(clientFilter: string) {
    setFilters(prev => ({ ...prev, is_client: clientFilter }))
    setPage(1)
  }

  const hotels = data?.hotels || []
  const total = data?.total || 0
  const totalPages = data?.pages || 1
  const sortedHotels = useMemo(() => sortHotels(hotels, filters.sort), [hotels, filters.sort])

  const STATS_CONFIG = [
    { key: 'total',        label: 'Total',        value: stats?.total || 0,        icon: Building2, bg: 'bg-navy-50',    text: 'text-navy-600',    accent: 'border-navy-100',    clientFilter: '' },
    { key: 'clients',      label: 'Clients',      value: stats?.clients || 0,      icon: Users,     bg: 'bg-emerald-50', text: 'text-emerald-600', accent: 'border-emerald-100', clientFilter: 'true' },
    { key: 'prospects',    label: 'Prospects',    value: stats?.prospects || 0,    icon: Eye,       bg: 'bg-coral-50',   text: 'text-coral-500',   accent: 'border-coral-100',   clientFilter: 'false' },
    { key: 'geocoded',     label: 'Geocoded',     value: stats?.geocoded || 0,     icon: MapPin,    bg: 'bg-sky-50',     text: 'text-sky-600',     accent: 'border-sky-100',     clientFilter: null },
    { key: 'with_contact', label: 'With Contact', value: stats?.with_contact || 0, icon: User,      bg: 'bg-violet-50',  text: 'text-violet-600',  accent: 'border-violet-100',  clientFilter: null },
    { key: 'with_tier',    label: 'With Tier',    value: stats?.with_tier || 0,    icon: Building2, bg: 'bg-gold-50',    text: 'text-gold-600',    accent: 'border-gold-100',    clientFilter: null },
    { key: 'on_map',       label: 'On Map',       value: stats?.on_map || 0,       icon: MapPin,    bg: 'bg-stone-100',  text: 'text-stone-500',   accent: 'border-stone-200',   clientFilter: null },
  ]

  return (
    <div className="h-full flex flex-col">
      {/* ─── Stats Cards ─── */}
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

      {/* ─── Tabs + Search + Export ─── */}
      <div className="px-4 pb-2 flex-shrink-0 space-y-2.5">
        <div className="flex items-center gap-4 flex-wrap">
          <div className="flex gap-px bg-stone-200/60 p-0.5 rounded-lg flex-shrink-0">
            {([
              { key: 'pipeline' as const, label: 'Pipeline', icon: Inbox },
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

          <button
            onClick={async () => {
              setExporting(true)
              try {
                const res = await api.post(
                  '/api/existing-hotels/export-csv',
                  { is_client: filters.is_client || null },
                )
                const rows = res.data?.rows || []
                if (rows.length === 0) {
                  alert('No geocoded hotels match the current filter.')
                  return
                }
                const cols = Object.keys(rows[0])
                const csv = [
                  cols.join(','),
                  ...rows.map((r: any) =>
                    cols.map((c) => {
                      const v = r[c] ?? ''
                      const s = String(v).replace(/"/g, '""')
                      return /[",\n]/.test(s) ? `"${s}"` : s
                    }).join(',')
                  ),
                ].join('\n')
                const url = URL.createObjectURL(new Blob([csv], { type: 'text/csv' }))
                const a = document.createElement('a')
                a.href = url
                a.download = `existing_hotels_${new Date().toISOString().split('T')[0]}.csv`
                a.click()
                URL.revokeObjectURL(url)
              } catch (e) { console.error('Export failed', e) }
              finally { setExporting(false) }
            }}
            disabled={exporting}
            className="flex items-center gap-1.5 px-3 h-9 text-xs font-semibold text-stone-600 bg-white border border-stone-200 rounded-lg hover:bg-emerald-50 hover:text-emerald-700 hover:border-emerald-300 transition disabled:opacity-50 flex-shrink-0"
            title="Export to CSV (Atlist-compatible)"
          >
            {exporting ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Download className="w-3.5 h-3.5" />}
            {exporting ? 'Exporting...' : 'Export'}
          </button>
        </div>

        {/* Filter row */}
        <div className="flex items-center gap-2.5 flex-wrap">
          <div className="flex items-center gap-1.5 text-stone-400 mr-0.5">
            <SlidersHorizontal className="w-4 h-4" />
            <span className="text-2xs font-bold uppercase tracking-wider">Filters</span>
          </div>

          <select
            value={filters.state}
            onChange={(e) => handleFilterChange('state', e.target.value)}
            className={cn(
              'filter-select',
              filters.state
                ? 'bg-navy-900 text-white border border-navy-800 [&>option]:text-navy-900 [&>option]:bg-white'
                : 'bg-white text-stone-600 border border-stone-200 hover:border-stone-300',
            )}
          >
            <option value="">All States</option>
            {(stats?.top_states || []).map(s => (
              <option key={s.state} value={s.state}>{s.state} ({s.count})</option>
            ))}
          </select>

          <select
            value={filters.brand_tier}
            onChange={(e) => handleFilterChange('brand_tier', e.target.value)}
            className={cn(
              'filter-select',
              filters.brand_tier
                ? 'bg-navy-900 text-white border border-navy-800 [&>option]:text-navy-900 [&>option]:bg-white'
                : 'bg-white text-stone-600 border border-stone-200 hover:border-stone-300',
            )}
          >
            <option value="">All Tiers</option>
            <option value="tier1_ultra_luxury">T1 — Ultra Luxury</option>
            <option value="tier2_luxury">T2 — Luxury</option>
            <option value="tier3_upper_upscale">T3 — Upper Upscale</option>
            <option value="tier4_upscale">T4 — Upscale</option>
          </select>

          <select
            value={filters.zone}
            onChange={(e) => handleFilterChange('zone', e.target.value)}
            className={cn(
              'filter-select',
              filters.zone
                ? 'bg-navy-900 text-white border border-navy-800 [&>option]:text-navy-900 [&>option]:bg-white'
                : 'bg-white text-stone-600 border border-stone-200 hover:border-stone-300',
            )}
          >
            <option value="">All Zones</option>
            {(stats?.zones || [])
              .filter(z => !['Out of State', 'Unknown', 'Junk'].includes(z.zone))
              .filter(z => {
                if (!filters.state) return true
                const selectedCode = STATE_NAME_TO_CODE[filters.state] || filters.state
                return z.state === selectedCode
              })
              .map(z => (
                <option key={z.zone} value={z.zone}>{z.zone} ({z.count})</option>
              ))}
          </select>

          <select
            value={filters.is_client}
            onChange={(e) => handleFilterChange('is_client', e.target.value)}
            className={cn(
              'filter-select',
              filters.is_client
                ? 'bg-navy-900 text-white border border-navy-800 [&>option]:text-navy-900 [&>option]:bg-white'
                : 'bg-white text-stone-600 border border-stone-200 hover:border-stone-300',
            )}
          >
            <option value="">All Hotels</option>
            <option value="true">Clients Only</option>
            <option value="false">Prospects Only</option>
          </select>

          {Object.entries(filters).some(([k, v]) => v && k !== 'sort' && k !== 'search') && (
            <button
              onClick={() => { setFilters({ ...DEFAULT_FILTERS, search: filters.search }); setPage(1) }}
              className="flex items-center gap-1.5 px-2.5 py-1.5 text-xs font-semibold text-white bg-coral-500 hover:bg-coral-600 rounded-lg transition shadow-sm"
            >
              <X className="w-3 h-3" />
              Clear
            </button>
          )}
        </div>
      </div>

      {/* ─── Table + Detail Panel ─── */}
      <div className="flex-1 flex overflow-hidden px-4 pb-3 gap-3">
        <div className={cn(
          'bg-white rounded-xl border border-slate-200/80 shadow-sm overflow-hidden flex flex-col transition-all duration-300',
          selectedId ? 'flex-[3]' : 'flex-1',
        )}>
          <HotelTable
            hotels={sortedHotels}
            total={total}
            page={page}
            totalPages={totalPages}
            tab={activeTab}
            selectedId={selectedId}
            onSelect={setSelectedId}
            onPageChange={setPage}
            onSort={handleSort}
            currentSort={filters.sort}
            isLoading={isLoading}
            perPage={perPage}
            onPerPageChange={handlePerPageChange}
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


/* ═════════════════════════════════════════════════════════
   HOTEL TABLE
   ═════════════════════════════════════════════════════════ */
function HotelTable({
  hotels, total, page, totalPages, tab,
  selectedId, onSelect, onPageChange, onSort, currentSort, isLoading,
  perPage, onPerPageChange,
}: {
  hotels: ExistingHotel[]; total: number; page: number; totalPages: number; tab: PipelineTab
  selectedId: number | null; onSelect: (id: number) => void
  onPageChange: (p: number) => void
  onSort: (sort: string) => void; currentSort: string
  isLoading: boolean
  perPage?: number
  onPerPageChange?: (n: number) => void
}) {
  const qc = useQueryClient()
  const [confirmTarget, setConfirmTarget] = useState<{ action: 'approve' | 'reject' | 'restore'; hotel: ExistingHotel } | null>(null)
  const [rejectReason, setRejectReason] = useState('duplicate')

  const approveMut = useMutation({
    mutationFn: (id: number) => approveExistingHotel(id),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['existing-hotels'] })
      qc.invalidateQueries({ queryKey: ['existing-hotels-stats'] })
      qc.invalidateQueries({ queryKey: ['map-data'] })
    },
  })
  const rejectMut = useMutation({
    mutationFn: ({ id, reason }: { id: number; reason: string }) => rejectExistingHotel(id, reason),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['existing-hotels'] })
      qc.invalidateQueries({ queryKey: ['existing-hotels-stats'] })
      qc.invalidateQueries({ queryKey: ['map-data'] })
    },
  })
  const restoreMut = useMutation({
    mutationFn: (id: number) => restoreExistingHotel(id),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['existing-hotels'] })
      qc.invalidateQueries({ queryKey: ['existing-hotels-stats'] })
      qc.invalidateQueries({ queryKey: ['map-data'] })
    },
  })

  const isNew      = tab === 'pipeline'
  const isApproved = tab === 'approved'
  const isRejected = tab === 'rejected'

  function handleConfirm() {
    if (!confirmTarget) return
    const { action, hotel } = confirmTarget
    if (action === 'approve') approveMut.mutate(hotel.id)
    if (action === 'reject') rejectMut.mutate({ id: hotel.id, reason: rejectReason })
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
        <div className="text-4xl mb-3">
          {isNew ? '🔍' : isApproved ? '✅' : isRejected ? '🚫' : '🏨'}
        </div>
        <p className="text-sm font-medium">No hotels in {tab}</p>
        {isNew && (
          <p className="text-xs mt-1 text-stone-400">
            Run scripts/transfer_to_existing.py to move expired leads here
          </p>
        )}
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
              {COLUMNS.map((col) => (
                <th
                  key={col.key}
                  onClick={() => col.sortAsc && onSort(getNextSort(col, currentSort))}
                  className={cn(
                    'px-3 py-2.5 text-left text-[11px] font-bold text-slate-400 uppercase tracking-wider group',
                    col.width,
                    col.sortAsc && 'cursor-pointer hover:text-slate-600 select-none transition-colors',
                  )}
                >
                  <span className="flex items-center gap-1">
                    {col.label}
                    {getSortIcon(col, currentSort)}
                  </span>
                </th>
              ))}
              {isRejected && (
                <th className="px-3 py-2.5 text-left text-[11px] font-bold text-slate-400 uppercase tracking-wider w-40">
                  Reason
                </th>
              )}
              <th className="px-3 py-2.5 w-24" />
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-100/80">
            {hotels.map((hotel) => {
              const h = hotel as any
              const displayName = h.hotel_name || h.name
              const score = hotel.lead_score
              return (
                <tr
                  key={hotel.id}
                  onClick={() => onSelect(hotel.id)}
                  className={cn('lead-row cursor-pointer', selectedId === hotel.id && 'active')}
                >
                  <td className="px-3 py-2.5">
                    <span className={cn(
                      'inline-flex items-center justify-center w-9 h-7 rounded-md text-xs font-bold tabular-nums',
                      score == null ? 'bg-stone-50 text-stone-300' :
                        score >= 75 ? 'bg-emerald-50 text-emerald-700' :
                        score >= 55 ? 'bg-amber-50 text-amber-700' :
                        score >= 35 ? 'bg-orange-50 text-orange-700' :
                                       'bg-stone-100 text-stone-500',
                    )}>
                      {score ?? '—'}
                    </span>
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
                    <span className="text-sm font-medium text-navy-800">{formatOpening(hotel)}</span>
                  </td>
                  <td className="px-3 py-2.5">
                    {hotel.revenue_annual ? (
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
                    <span className="text-xs text-slate-400 font-medium">{relativeDate(hotel.created_at)}</span>
                  </td>

                  {isRejected && (
                    <td className="px-3 py-2.5">
                      <span
                        title={hotel.rejection_reason || 'No reason given'}
                        className="inline-flex items-center px-2 py-0.5 rounded text-2xs font-semibold bg-red-50 text-red-600 max-w-[160px] truncate block"
                      >
                        {hotel.rejection_reason
                          ? hotel.rejection_reason.replace(/_/g, ' ')
                          : <span className="text-stone-300 font-normal italic">—</span>
                        }
                      </span>
                    </td>
                  )}

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

      {(totalPages > 1 || onPerPageChange) && (
        <div className="flex items-center justify-between px-4 py-2.5 border-t border-slate-100 bg-white/80 flex-shrink-0">
          <div className="flex items-center gap-3">
            {onPerPageChange && perPage !== undefined && (
              <div className="flex items-center gap-1.5">
                <label className="text-2xs font-semibold text-stone-400 uppercase tracking-wider">
                  Show
                </label>
                <select
                  value={perPage}
                  onChange={(e) => onPerPageChange(Number(e.target.value))}
                  className="h-7 px-2 pr-6 text-xs font-semibold bg-white text-navy-900 border border-stone-200 rounded cursor-pointer hover:border-stone-300 focus:outline-none focus:border-navy-400 focus:ring-2 focus:ring-navy-100 transition appearance-none bg-no-repeat bg-right"
                  style={{ backgroundImage: 'url("data:image/svg+xml;charset=utf-8,%3Csvg xmlns=\'http://www.w3.org/2000/svg\' width=\'10\' height=\'6\' viewBox=\'0 0 10 6\'%3E%3Cpath d=\'M5 6L0 0h10z\' fill=\'%2378716c\'/%3E%3C/svg%3E")', backgroundPositionX: 'calc(100% - 6px)' }}
                  title="Hotels per page"
                >
                  <option value={25}>25</option>
                  <option value={50}>50</option>
                  <option value={100}>100</option>
                </select>
              </div>
            )}
            <span className="text-xs text-stone-400">
              Page {page} of {totalPages} · {total} hotel{total !== 1 ? 's' : ''}
            </span>
          </div>
          {totalPages > 1 && (
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
          )}
        </div>
      )}

      <ConfirmDialog
        open={confirmAction === 'approve'}
        variant="approve"
        title="Approve Hotel"
        message={`Push "${(confirmHotel as any)?.hotel_name || confirmHotel?.name}" to Insightly CRM?`}
        confirmLabel="Approve & Push"
        pending={approveMut.isPending}
        onConfirm={handleConfirm}
        onCancel={() => setConfirmTarget(null)}
      />
      <ConfirmDialog
        open={confirmAction === 'reject'}
        variant="reject"
        title="Reject Hotel"
        message={`Move "${(confirmHotel as any)?.hotel_name || confirmHotel?.name}" to Rejected? You can restore it later.`}
        confirmLabel="Reject"
        pending={rejectMut.isPending}
        onConfirm={handleConfirm}
        onCancel={() => setConfirmTarget(null)}
      >
        <div className="mt-3">
          <label className="block text-xs font-semibold text-stone-500 mb-1">Rejection Reason</label>
          <select
            value={rejectReason}
            onChange={e => setRejectReason(e.target.value)}
            onClick={e => e.stopPropagation()}
            className="w-full text-sm border border-stone-200 rounded-lg px-3 py-2 bg-white text-navy-900 focus:outline-none focus:border-red-400 focus:ring-2 focus:ring-red-100"
          >
            <option value="duplicate">Duplicate</option>
            <option value="bad_data">Bad data / incorrect info</option>
            <option value="not_relevant">Not relevant to JA Uniforms</option>
            <option value="closed">Closed / no longer operating</option>
            <option value="budget_brand">Budget brand — not our market</option>
            <option value="low_priority">Low priority</option>
          </select>
        </div>
      </ConfirmDialog>
      <ConfirmDialog
        open={confirmAction === 'restore'}
        variant="restore"
        title={isApproved ? 'Back to Pipeline' : 'Restore Hotel'}
        message={isApproved
          ? `Move "${(confirmHotel as any)?.hotel_name || confirmHotel?.name}" back to pipeline? This will delete from Insightly.`
          : `Restore "${(confirmHotel as any)?.hotel_name || confirmHotel?.name}" back to pipeline?`}
        confirmLabel={isApproved ? 'Remove from CRM' : 'Restore'}
        pending={restoreMut.isPending}
        onConfirm={handleConfirm}
        onCancel={() => setConfirmTarget(null)}
      />
    </div>
  )
}


/* ═════════════════════════════════════════════════════════
   HOTEL DETAIL PANEL
   ═════════════════════════════════════════════════════════ */
type DetailTab = 'overview' | 'contacts' | 'edit' | 'sources'

function HotelDetail({ hotelId, tab, onClose }: { hotelId: number; tab: PipelineTab; onClose: () => void }) {
  const qc = useQueryClient()
  const { data: hotel, isLoading } = useQuery<ExistingHotel>({
    queryKey: ['existing-hotel', hotelId],
    queryFn: () => fetchExistingHotel(hotelId),
  })
  const { data: contacts, isLoading: contactsLoading } = useQuery<Contact[]>({
    queryKey: ['hotel-contacts', hotelId],
    queryFn: () => fetchHotelContacts(hotelId),
  })

  const [activeTab, setActiveTab] = useState<DetailTab>('overview')
  const [confirmAction, setConfirmAction] = useState<'approve' | 'reject' | 'restore' | null>(null)
  const [rejectReason, setRejectReason] = useState('duplicate')
  const [editingReason, setEditingReason] = useState(false)
  const [reasonValue, setReasonValue] = useState('')
  const [savingReason, setSavingReason] = useState(false)

  const [enrichingHotelId, setEnrichingHotelId] = useState<number | null>(null)
  const [smartFillHotelId, setSmartFillHotelId] = useState<number | null>(null)
  const [smartFillMode, setSmartFillMode] = useState<'smart' | 'full'>('smart')
  const enrichingLive = enrichingHotelId === hotelId
  const smartFillLive = smartFillHotelId === hotelId ? smartFillMode : null

  useEffect(() => {
    let cancelled = false
    getHotelEnrichmentStatus(hotelId)
      .then((data) => {
        if (cancelled) return
        if (data?.running) setEnrichingHotelId(hotelId)
      })
      .catch(() => { /* silent */ })
    return () => { cancelled = true }
  }, [hotelId])

  const approveMut = useMutation({
    mutationFn: () => approveExistingHotel(hotelId),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['existing-hotels'] })
      qc.invalidateQueries({ queryKey: ['existing-hotels-stats'] })
      qc.invalidateQueries({ queryKey: ['existing-hotel', hotelId] })
    },
  })
  const rejectMut = useMutation({
    mutationFn: () => rejectExistingHotel(hotelId, rejectReason),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['existing-hotels'] })
      qc.invalidateQueries({ queryKey: ['existing-hotels-stats'] })
      qc.invalidateQueries({ queryKey: ['existing-hotel', hotelId] })
    },
  })
  const restoreMut = useMutation({
    mutationFn: () => restoreExistingHotel(hotelId),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['existing-hotels'] })
      qc.invalidateQueries({ queryKey: ['existing-hotels-stats'] })
      qc.invalidateQueries({ queryKey: ['existing-hotel', hotelId] })
    },
  })

  const isNew      = tab === 'pipeline'
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

  const h = hotel as any
  const displayName = h.hotel_name || h.name
  const contactList = Array.isArray(contacts) ? contacts : []

  return (
    <div className="h-full flex flex-col bg-white animate-slideIn">
      <div className="px-5 pt-5 pb-3 flex-shrink-0 border-b border-slate-100 bg-gradient-to-b from-slate-50/50 to-white">
        <div className="flex items-start justify-between gap-3">
          <div className="min-w-0 flex-1">
            <h2 className="text-lg font-bold text-navy-900 leading-snug truncate">{displayName}</h2>
            {hotel.brand && <p className="text-sm text-stone-400 mt-0.5">{hotel.brand}</p>}
          </div>
          <div className="flex items-center gap-2 flex-shrink-0">
            {hotel.lead_score != null && (
              <span className={cn(
                'inline-flex items-center justify-center w-10 h-8 text-sm font-bold rounded',
                getScoreColor(hotel.lead_score), getScoreRing(hotel.lead_score),
              )}>
                {hotel.lead_score}
              </span>
            )}
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
          {(h.hotel_type || h.property_type) && (() => {
            const raw = String(h.hotel_type || h.property_type || '').trim().toLowerCase()
            const typeMap: Record<string, { label: string; color: string }> = {
              resort:        { label: 'Resort',         color: 'bg-teal-100 text-teal-700' },
              all_inclusive: { label: 'All-Inclusive',  color: 'bg-cyan-100 text-cyan-700' },
              boutique:      { label: 'Boutique',       color: 'bg-violet-100 text-violet-700' },
              hotel:         { label: 'Hotel',          color: 'bg-stone-100 text-stone-600' },
              lodge:         { label: 'Lodge',          color: 'bg-emerald-100 text-emerald-700' },
              inn:           { label: 'Inn',            color: 'bg-amber-100 text-amber-700' },
            }
            // Substring match — handles freeform values like "luxury hotel",
            // "all-inclusive resort", "boutique hotel", etc.
            let key = ''
            if      (raw.includes('all-inclusive') || raw.includes('all_inclusive') || raw.includes('all inclusive')) key = 'all_inclusive'
            else if (raw.includes('resort'))   key = 'resort'
            else if (raw.includes('boutique')) key = 'boutique'
            else if (raw.includes('lodge'))    key = 'lodge'
            else if (raw.includes('inn'))      key = 'inn'
            else if (raw.includes('hotel'))    key = 'hotel'
            const t = typeMap[key]
            return t ? <span className={cn('inline-flex px-2 py-0.5 rounded text-xs font-bold', t.color)}>{t.label}</span> : null
          })()}
          {h.chain && (
            <span className="inline-flex px-2 py-0.5 rounded text-xs font-medium bg-stone-100 text-stone-500">
              {h.chain}
            </span>
          )}
          {hotel.project_type && (() => {
            const typeMap: Record<string, { label: string; color: string }> = {
              new_opening:      { label: 'New Build',  color: 'bg-emerald-100 text-emerald-700' },
              renovation:       { label: 'Renovation', color: 'bg-blue-100 text-blue-700' },
              rebrand:          { label: 'Rebrand',    color: 'bg-purple-100 text-purple-700' },
              ownership_change: { label: 'New Owner',  color: 'bg-amber-100 text-amber-700' },
              reopening:        { label: 'Reopening',  color: 'bg-blue-100 text-blue-700' },
              conversion:       { label: 'Conversion', color: 'bg-orange-100 text-orange-700' },
            }
            const t = typeMap[hotel.project_type]
            return t ? <span className={cn('inline-flex px-2 py-0.5 rounded text-xs font-bold', t.color)}>{t.label}</span> : null
          })()}
        </div>
      </div>

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
            {t === 'contacts' && contactList.length > 0 && (
              <span className="ml-1.5 text-2xs bg-navy-50 text-navy-600 px-1.5 py-0.5 rounded-full">{contactList.length}</span>
            )}
          </button>
        ))}
      </div>

      <div className="flex-1 overflow-y-auto p-5">
        {activeTab === 'overview' && (
          <HotelOverviewTab
            hotel={hotel}
            hotelId={hotelId}
            contactList={contactList}
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
            contacts={contactList}
            loading={contactsLoading}
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

      <div className="px-5 py-3 border-t border-slate-100 bg-slate-50/50 flex-shrink-0">
        <div className="flex items-center gap-2">
          {isNew && (
            <>
              <button
                onClick={() => setConfirmAction('approve')}
                disabled={approveMut.isPending}
                className="flex items-center gap-1.5 px-4 py-2 text-xs font-semibold rounded-lg bg-emerald-600 text-white hover:bg-emerald-700 transition disabled:opacity-50"
              >
                {approveMut.isPending ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <CheckCircle2 className="w-3.5 h-3.5" />}
                Approve
              </button>
              <button
                onClick={() => setConfirmAction('reject')}
                disabled={rejectMut.isPending}
                className="flex items-center gap-1.5 px-4 py-2 text-xs font-semibold rounded-lg border border-stone-200 text-stone-600 hover:bg-stone-50 transition disabled:opacity-50"
              >
                {rejectMut.isPending ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <XCircle className="w-3.5 h-3.5" />}
                Reject
              </button>
            </>
          )}
          {isApproved && (
            <button
              onClick={() => setConfirmAction('restore')}
              disabled={restoreMut.isPending}
              className="flex items-center gap-1.5 px-4 py-2 text-xs font-semibold rounded-lg border border-amber-200 text-amber-600 hover:bg-amber-50 transition disabled:opacity-50"
            >
              {restoreMut.isPending ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Undo2 className="w-3.5 h-3.5" />}
              Back to Pipeline
            </button>
          )}
          {isRejected && (
            <div className="flex items-center gap-2 flex-1">
              {editingReason ? (
                <div className="flex items-center gap-1.5 flex-1">
                  <select
                    value={reasonValue}
                    onChange={e => setReasonValue(e.target.value)}
                    autoFocus
                    className="flex-1 text-xs border border-red-300 rounded-lg px-2 py-1.5 bg-white text-navy-900 focus:outline-none focus:ring-2 focus:ring-red-100"
                  >
                    <option value="duplicate">Duplicate</option>
                    <option value="bad_data">Bad data / incorrect info</option>
                    <option value="not_relevant">Not relevant to JA Uniforms</option>
                    <option value="closed">Closed / no longer operating</option>
                    <option value="budget_brand">Budget brand — not our market</option>
                    <option value="low_priority">Low priority</option>
                  </select>
                  <button
                    disabled={savingReason}
                    onClick={async () => {
                      setSavingReason(true)
                      try {
                        await editExistingHotel(hotelId, { rejection_reason: reasonValue } as any)
                        qc.invalidateQueries({ queryKey: ['existing-hotel', hotelId] })
                        qc.invalidateQueries({ queryKey: ['existing-hotels'] })
                        setEditingReason(false)
                      } catch { /* silent */ }
                      setSavingReason(false)
                    }}
                    className="px-2.5 py-1.5 text-xs font-semibold bg-red-600 text-white rounded-lg hover:bg-red-700 transition disabled:opacity-50"
                  >
                    {savingReason ? <Loader2 className="w-3 h-3 animate-spin" /> : 'Save'}
                  </button>
                  <button onClick={() => setEditingReason(false)} className="px-2 py-1.5 text-xs text-stone-400 hover:text-stone-600 transition">
                    <X className="w-3.5 h-3.5" />
                  </button>
                </div>
              ) : (
                <button
                  onClick={() => { setReasonValue(hotel.rejection_reason || 'duplicate'); setEditingReason(true) }}
                  className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium rounded-lg border border-dashed border-red-200 text-red-500 hover:bg-red-50 transition"
                >
                  <Pencil className="w-3 h-3" />
                  {hotel.rejection_reason
                    ? hotel.rejection_reason.replace(/_/g, ' ')
                    : 'Set reason'}
                </button>
              )}

              <button
                onClick={() => setConfirmAction('restore')}
                disabled={restoreMut.isPending}
                className="flex items-center gap-1.5 px-4 py-2 text-xs font-semibold rounded-lg border border-blue-200 text-blue-600 hover:bg-blue-50 transition disabled:opacity-50"
              >
                {restoreMut.isPending ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Undo2 className="w-3.5 h-3.5" />}
                Restore
              </button>
            </div>
          )}
        </div>
      </div>

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
      >
        <div className="mt-3">
          <label className="block text-xs font-semibold text-stone-500 mb-1">Rejection Reason</label>
          <select
            value={rejectReason}
            onChange={e => setRejectReason(e.target.value)}
            onClick={e => e.stopPropagation()}
            className="w-full text-sm border border-stone-200 rounded-lg px-3 py-2 bg-white text-navy-900 focus:outline-none focus:border-red-400 focus:ring-2 focus:ring-red-100"
          >
            <option value="duplicate">Duplicate</option>
            <option value="bad_data">Bad data / incorrect info</option>
            <option value="not_relevant">Not relevant to JA Uniforms</option>
            <option value="closed">Closed / no longer operating</option>
            <option value="budget_brand">Budget brand — not our market</option>
            <option value="low_priority">Low priority</option>
          </select>
        </div>
      </ConfirmDialog>
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


/* ═════════════════════════════════════════════════════════
   OVERVIEW TAB
   ═════════════════════════════════════════════════════════ */
function HotelOverviewTab({
  hotel, hotelId, contactList,
  onEnrich, enrichingLive, onEnrichComplete,
  onSmartFill, smartFillLive, onSmartFillComplete,
}: {
  hotel: ExistingHotel; hotelId: number; contactList: Contact[]
  onEnrich: () => void; enrichingLive: boolean; onEnrichComplete: () => void
  onSmartFill: (mode: 'smart' | 'full') => void; smartFillLive: 'smart' | 'full' | null; onSmartFillComplete: () => void
}) {
  const qc = useQueryClient()
  const h = hotel as any
  const hasMissing = !hotel.brand_tier || hotel.brand_tier === 'unknown' || !hotel.opening_date || !hotel.room_count || !hotel.management_company || !hotel.owner || !hotel.developer || !hotel.address
  const [smartFilling, setSmartFilling] = useState(false)

  return (
    <div className="space-y-5 animate-fadeIn">
      <Section title="Details">
        <div className="grid grid-cols-2 gap-4">
          <Field icon={Calendar}  label="Opening"    value={formatOpening(hotel)} />
          <Field icon={MapPin}    label="Location"   value={formatLocation(hotel)} />
          <Field icon={Building2} label="Rooms"      value={hotel.room_count ? `${hotel.room_count} rooms` : '—'} />
          <Field icon={Layers}    label="Brand Tier" value={getTierLabel(hotel.brand_tier)} />
          <Field icon={MapPin}    label="Address"    value={hotel.address ? hotel.address + (hotel.zip_code ? ` ${hotel.zip_code}` : '') : '—'} />
          {(h.hotel_type || h.property_type) && (
            <Field icon={Building2} label="Type" value={h.hotel_type || h.property_type} />
          )}
          {hotel.zone && <Field icon={MapPin} label="Zone" value={hotel.zone} />}
          {hotel.management_company && <Field icon={Building2} label="Mgmt Co"   value={hotel.management_company} />}
          {hotel.developer         && <Field icon={Building2} label="Developer" value={hotel.developer} />}
          {hotel.owner             && <Field icon={User}      label="Owner"     value={hotel.owner} />}
        </div>

        {smartFillLive && (
          <div className="mt-3 pt-3 border-t border-stone-100">
            <SmartFillProgress
              leadId={hotelId}
              mode={smartFillLive}
              basePath="/api/existing-hotels"
              onComplete={() => {
                qc.invalidateQueries({ queryKey: ['existing-hotel', hotelId] })
                qc.invalidateQueries({ queryKey: ['existing-hotels'] })
                onSmartFillComplete()
              }}
              onCancel={onSmartFillComplete}
            />
          </div>
        )}

        {!smartFillLive && (
          <div className="flex items-center gap-2 mt-3 pt-3 border-t border-stone-100">
            {hasMissing && (
              <button
                onClick={() => { setSmartFilling(true); onSmartFill('smart'); setTimeout(() => setSmartFilling(false), 1000) }}
                disabled={smartFilling}
                className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-semibold rounded-md transition bg-violet-50 text-violet-700 border border-violet-200 hover:bg-violet-100 disabled:opacity-60"
              >
                <Zap className="w-3 h-3" /> Smart Fill
              </button>
            )}
            <button
              onClick={() => onSmartFill('full')}
              className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium text-stone-400 hover:text-violet-600 hover:bg-violet-50 rounded-md border border-dashed border-stone-200 hover:border-violet-300 transition"
            >
              <RefreshCw className="w-3 h-3" /> Full Refresh
            </button>
          </div>
        )}
      </Section>

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

      <Section title="Website & Location">
        <div className="flex items-center gap-2">
          {hotel.hotel_website ? (
            <a
              href={hotel.hotel_website.startsWith('http') ? hotel.hotel_website : `https://${hotel.hotel_website}`}
              target="_blank" rel="noopener noreferrer"
              className="flex items-center gap-2 text-sm text-navy-600 hover:text-navy-800 hover:underline transition flex-1 min-w-0"
            >
              <Globe className="w-4 h-4 flex-shrink-0" />
              <span className="truncate">{hotel.hotel_website}</span>
              {hotel.website_verified === 'auto' && (
                <span className="text-[10px] text-emerald-600 bg-emerald-50 px-1.5 py-0.5 rounded font-medium flex-shrink-0">auto</span>
              )}
              <ExternalLink className="w-3 h-3 flex-shrink-0" />
            </a>
          ) : (
            <span className="text-sm text-stone-400 flex-1">No website found yet</span>
          )}
          <button
            onClick={() => onSmartFill('full')}
            title="Find website + geocoordinates via Smart Fill"
            className="flex items-center gap-1.5 px-2.5 py-1.5 text-xs font-medium text-stone-500 hover:text-violet-700 hover:bg-violet-50 border border-dashed border-stone-200 hover:border-violet-300 rounded-md transition flex-shrink-0"
          >
            <Search className="w-3 h-3" /> Find
          </button>
        </div>

        {(hotel.latitude && hotel.longitude) ? (
          <div className="mt-2 flex items-center gap-2">
            <MapPin className="w-3.5 h-3.5 text-stone-400 flex-shrink-0" />
            <a
              href={`https://www.google.com/maps?q=${hotel.latitude},${hotel.longitude}`}
              target="_blank" rel="noopener noreferrer"
              className="text-xs text-stone-500 hover:text-navy-700 hover:underline transition font-mono"
            >
              {hotel.latitude.toFixed(4)}, {hotel.longitude.toFixed(4)}
            </a>
            <span className="text-[10px] text-emerald-600 bg-emerald-50 px-1.5 py-0.5 rounded font-medium">mapped</span>
          </div>
        ) : (
          <div className="mt-2 flex items-center gap-1.5">
            <MapPin className="w-3.5 h-3.5 text-stone-300" />
            <span className="text-xs text-stone-400">Not yet geocoded — click Find</span>
          </div>
        )}
      </Section>

      <Section title="Primary Contact">
        {contactList.length > 0 ? (
          <div className="bg-slate-50 rounded-lg p-3.5 border border-slate-200/80">
            <div className="flex items-center gap-3">
              <div className="w-10 h-10 rounded-full bg-gradient-to-br from-navy-400 to-navy-600 flex items-center justify-center flex-shrink-0">
                <span className="text-white font-bold text-sm">
                  {(contactList[0].name || '?')[0].toUpperCase()}
                </span>
              </div>
              <div className="min-w-0 flex-1">
                <div className="flex items-center gap-2">
                  <PriorityBadge label={(contactList[0] as any).priority_label} reason={(contactList[0] as any).priority_reason} />
                  <p className="text-sm font-semibold text-navy-900">{contactList[0].name}</p>
                </div>
                <p className="text-xs text-stone-500 truncate">{contactList[0].title || 'No title'}</p>
              </div>
              {contactList.length > 1 && (
                <span className="text-xs text-navy-500 font-medium">+{contactList.length - 1} more</span>
              )}
            </div>
          </div>
        ) : (
          <button
            onClick={onEnrich}
            disabled={enrichingLive}
            className="w-full text-left bg-gold-50 rounded-lg p-3.5 border border-gold-200 hover:border-gold-300 transition disabled:opacity-60"
          >
            <div className="flex items-center gap-2.5">
              {enrichingLive ? <Loader2 className="w-5 h-5 text-gold-600 animate-spin" /> : <Search className="w-5 h-5 text-gold-600" />}
              <div>
                <p className="text-sm font-semibold text-gold-700">
                  {enrichingLive ? 'Searching contacts...' : 'Find Contacts'}
                </p>
                <p className="text-xs text-gold-500">Search for GMs, Directors, Purchasing Managers</p>
              </div>
            </div>
          </button>
        )}
      </Section>

      {hotel.source_extractions && typeof hotel.source_extractions === 'object' && Object.keys(hotel.source_extractions).length > 0 && (
        <Section title={`Key Insights (${Object.keys(hotel.source_extractions).length} sources)`}>
          <KeyInsights extractions={hotel.source_extractions as Record<string, any>} />
        </Section>
      )}

      {hotel.is_client && hotel.sap_bp_code && (
        <Section title="Client Info">
          <div className="space-y-2 text-sm">
            <div className="flex justify-between">
              <span className="text-stone-400 font-medium">SAP Code</span>
              <span className="text-navy-700 font-semibold">{hotel.sap_bp_code}</span>
            </div>
            {h.client_notes && (
              <div className="text-xs text-stone-500 pt-2 border-t border-stone-100">{h.client_notes}</div>
            )}
          </div>
        </Section>
      )}

      {hotel.description && (
        <Section title="Description">
          <p className="text-sm text-stone-600 leading-relaxed whitespace-pre-line">{hotel.description}</p>
        </Section>
      )}

      <Section title="Metadata">
        <div className="space-y-2 text-sm">
          {[
            ['Hotel ID',  String(hotel.id)],
            ['Status',    hotel.status],
            ['Created',   formatDate(hotel.created_at)],
            hotel.updated_at ? ['Updated', formatDate(hotel.updated_at)] : null,
            h.last_verified_at ? ['Last Verified', formatDate(h.last_verified_at)] : null,
            hotel.insightly_id ? ['Insightly', `#${hotel.insightly_id}`] : null,
            (hotel.rejection_reason && typeof hotel.rejection_reason === 'string') ? ['Rejection', hotel.rejection_reason] : null,
            h.data_source ? ['Data source', h.data_source] : null,
          ].filter(Boolean).map((row: any) => (
            <div key={row[0]} className="flex justify-between">
              <span className="text-stone-400 font-medium">{safe(row[0])}</span>
              <span className="text-navy-700 font-semibold capitalize">{safe(row[1])}</span>
            </div>
          ))}
        </div>
      </Section>

      {enrichingLive && (
        <div>
          <EnrichProgress
            leadId={hotelId}
            basePath="/api/existing-hotels"
            onComplete={onEnrichComplete}
            onCancel={onEnrichComplete}
          />
        </div>
      )}
    </div>
  )
}


/* ═════════════════════════════════════════════════════════
   WIZA EMAIL BUTTON — mirrors LeadDetail's
   ═════════════════════════════════════════════════════════ */
function WizaEmailButton({
  contactId, hotelId, onEmailFound,
}: {
  contactId: number
  hotelId: number
  onEmailFound: (email: string) => void
}) {
  const [loading, setLoading] = useState(false)
  const [result, setResult] = useState<'found' | 'not_found' | null>(null)

  async function handleClick() {
    setLoading(true)
    try {
      const r = await enrichHotelContactEmail(hotelId, contactId)
      if (r.status === 'found' && r.email) {
        setResult('found')
        onEmailFound(r.email)
      } else {
        setResult('not_found')
      }
    } catch {
      setResult('not_found')
    } finally {
      setLoading(false)
    }
  }

  if (result === 'not_found') {
    return <span className="text-[10px] text-stone-400 px-2 py-1 bg-stone-50 rounded">No email found</span>
  }

  return (
    <button
      onClick={handleClick}
      disabled={loading}
      title="Find email via Wiza (costs 2 credits if found, free if not)"
      className="inline-flex items-center gap-1.5 px-2.5 py-1 text-xs font-medium text-violet-700 bg-violet-50 rounded-md hover:bg-violet-100 border border-violet-200 transition disabled:opacity-50"
    >
      {loading ? <Loader2 className="w-3 h-3 animate-spin" /> : <Mail className="w-3 h-3" />}
      {loading ? 'Searching...' : 'Find Email'}
    </button>
  )
}


/* ═════════════════════════════════════════════════════════
   CONTACTS TAB — full feature parity with LeadDetail
   ═════════════════════════════════════════════════════════ */
function HotelContactsTab({
  hotelId, contacts, loading,
  onEnrich, enrichingLive, onEnrichComplete,
}: {
  hotelId: number; contacts: Contact[]; loading: boolean
  onEnrich: () => void; enrichingLive: boolean; onEnrichComplete: () => void
}) {
  const qc = useQueryClient()
  const [editingId, setEditingId] = useState<number | null>(null)
  const [editForm, setEditForm] = useState<Record<string, string>>({})
  const [deleting, setDeleting] = useState<number | null>(null)
  const [showAdd, setShowAdd] = useState(false)
  const [addForm, setAddForm] = useState<Record<string, string>>({ scope: 'hotel_specific' })
  const [adding, setAdding] = useState(false)
  const [openBreakdownId, setOpenBreakdownId] = useState<number | null>(null)
  const [openEvidenceId, setOpenEvidenceId] = useState<number | null>(null)

  const progressCard = enrichingLive ? (
    <div className="mb-4 animate-fadeIn">
      <EnrichProgress
        leadId={hotelId}
        basePath="/api/existing-hotels"
        onComplete={() => {
          qc.invalidateQueries({ queryKey: ['hotel-contacts', hotelId] })
          qc.invalidateQueries({ queryKey: ['existing-hotel', hotelId] })
          qc.invalidateQueries({ queryKey: ['existing-hotels'] })
          onEnrichComplete()
        }}
        onCancel={onEnrichComplete}
      />
    </div>
  ) : null

  if (loading) {
    return (
      <>
        {progressCard}
        <div className="space-y-2">{Array.from({ length: 3 }).map((_, i) => <div key={i} className="skeleton h-24 rounded-lg" />)}</div>
      </>
    )
  }

  if (!contacts.length) {
    return (
      <>
        {progressCard}
        {!enrichingLive && (
          <div className="text-center py-12 animate-fadeIn">
            <User className="w-12 h-12 text-stone-300 mx-auto mb-3" />
            <p className="text-sm font-medium text-stone-500">No contacts found</p>
            <button onClick={onEnrich} className="mt-3 px-5 py-2.5 text-xs font-semibold bg-navy-900 text-white rounded-lg hover:bg-navy-800 transition">
              Run Enrichment
            </button>
          </div>
        )}
      </>
    )
  }

  async function handleSave(contactId: number) {
    await saveHotelContact(hotelId, contactId)
    qc.invalidateQueries({ queryKey: ['hotel-contacts', hotelId] })
  }
  async function handleSetPrimary(contactId: number) {
    await setPrimaryHotelContact(hotelId, contactId)
    qc.invalidateQueries({ queryKey: ['hotel-contacts', hotelId] })
    qc.invalidateQueries({ queryKey: ['existing-hotel', hotelId] })
  }
  async function handleToggleScope(contactId: number, currentScope: string) {
    const cycle = ['hotel_specific', 'chain_area', 'management_corporate', 'chain_corporate', 'owner']
    const idx = cycle.indexOf(currentScope)
    const next = cycle[(idx + 1) % cycle.length]
    try {
      await toggleHotelContactScope(hotelId, contactId, next)
      qc.invalidateQueries({ queryKey: ['hotel-contacts', hotelId] })
      qc.invalidateQueries({ queryKey: ['existing-hotel', hotelId] })
    } catch (e) { console.error('Toggle scope failed', e) }
  }
  async function handleDelete(contactId: number) {
    if (!confirm('Delete this contact?')) return
    setDeleting(contactId)
    try {
      await deleteHotelContact(hotelId, contactId)
      qc.invalidateQueries({ queryKey: ['hotel-contacts', hotelId] })
      qc.invalidateQueries({ queryKey: ['existing-hotel', hotelId] })
    } catch { /* silent */ }
    setDeleting(null)
  }
  function startEdit(c: Contact) {
    setEditingId(c.id)
    setEditForm({
      name: c.name || '', title: c.title || '', organization: c.organization || '',
      email: c.email || '', phone: c.phone || '', linkedin: c.linkedin || '',
      evidence_url: c.evidence_url || '',
    })
  }
  async function saveEdit() {
    if (!editingId) return
    try {
      await updateHotelContact(hotelId, editingId, editForm)
      qc.invalidateQueries({ queryKey: ['hotel-contacts', hotelId] })
    } catch { /* silent */ }
    setEditingId(null); setEditForm({})
  }
  function cancelEdit() { setEditingId(null); setEditForm({}) }
  function handleEditKey(e: React.KeyboardEvent) {
    if (e.key === 'Enter') saveEdit()
    if (e.key === 'Escape') cancelEdit()
  }

  const tierStyles: Record<string, { label: string; cls: string }> = {
    primary:    { label: '🟢 PRIMARY',    cls: 'bg-emerald-50 text-emerald-700 border-emerald-200' },
    official:   { label: '🔵 OFFICIAL',   cls: 'bg-blue-50 text-blue-700 border-blue-200' },
    trade:      { label: '🟡 TRADE',      cls: 'bg-amber-50 text-amber-700 border-amber-200' },
    aggregator: { label: '🟠 AGGREGATOR', cls: 'bg-orange-50 text-orange-700 border-orange-200' },
    indirect:   { label: '🔴 INDIRECT',   cls: 'bg-red-50 text-red-700 border-red-200' },
    unknown:    { label: '⚪ UNKNOWN',    cls: 'bg-stone-50 text-stone-600 border-stone-200' },
  }
  const currentYear = new Date().getFullYear()
  const isStale = (y: number | null | undefined) => typeof y === 'number' && currentYear - y >= 2

  return (
    <div className="space-y-2.5 animate-fadeIn">
      {progressCard}

      <div className="flex items-center gap-3 px-3 py-2 bg-slate-50 rounded-md border border-slate-200 text-2xs flex-wrap">
        <span className="font-semibold text-stone-500 uppercase tracking-wide">Priority:</span>
        <span className="inline-flex items-center gap-1">
          <span className="inline-block w-2 h-2 rounded-full bg-emerald-500" />
          <span className="text-stone-600"><strong>P1</strong> Call first</span>
        </span>
        <span className="inline-flex items-center gap-1">
          <span className="inline-block w-2 h-2 rounded-full bg-blue-500" />
          <span className="text-stone-600"><strong>P2</strong> Strong fit</span>
        </span>
        <span className="inline-flex items-center gap-1">
          <span className="inline-block w-2 h-2 rounded-full bg-amber-500" />
          <span className="text-stone-600"><strong>P3</strong> Useful</span>
        </span>
        <span className="inline-flex items-center gap-1">
          <span className="inline-block w-2 h-2 rounded-full bg-stone-400" />
          <span className="text-stone-600"><strong>P4</strong> Escalation only</span>
        </span>
      </div>

      {contacts.map((c) => (
        <div key={c.id} className={cn('rounded-lg border p-4 transition relative group', c.is_primary ? 'border-navy-200 bg-navy-50/30' : 'border-stone-100 hover:border-stone-200')}>
          <div className="absolute top-3 right-3 flex items-center gap-1 opacity-0 group-hover:opacity-100 transition">
            {editingId !== c.id && (
              <button onClick={() => startEdit(c)} className="p-1.5 text-stone-400 hover:text-navy-600 hover:bg-stone-100 rounded-md transition" title="Edit contact">
                <Pencil className="w-3 h-3" />
              </button>
            )}
            <button onClick={() => handleDelete(c.id)} disabled={deleting === c.id} className="p-1.5 text-stone-400 hover:text-red-600 hover:bg-red-50 rounded-md transition disabled:opacity-50" title="Delete contact">
              {deleting === c.id ? <Loader2 className="w-3 h-3 animate-spin" /> : <Trash2 className="w-3 h-3" />}
            </button>
          </div>

          <div className="flex items-start gap-3">
            <div className={cn('w-9 h-9 rounded-full flex items-center justify-center flex-shrink-0 text-sm font-bold mt-0.5', c.is_primary ? 'bg-navy-600 text-white' : 'bg-stone-200 text-stone-600')}>
              {(c.name || '?')[0].toUpperCase()}
            </div>

            <div className="flex-1 min-w-0 pr-16">
              {editingId === c.id ? (
                <div className="space-y-2" onKeyDown={handleEditKey}>
                  <div className="grid grid-cols-2 gap-2">
                    <input value={editForm.name || ''} onChange={(e) => setEditForm(f => ({ ...f, name: e.target.value }))} placeholder="Name" className="col-span-2 h-8 px-2.5 text-sm text-navy-900 bg-white border border-stone-200 rounded-md outline-none focus:border-navy-400 focus:ring-1 focus:ring-navy-200" autoFocus />
                    <input value={editForm.title || ''} onChange={(e) => setEditForm(f => ({ ...f, title: e.target.value }))} placeholder="Title / Role" className="col-span-2 h-8 px-2.5 text-sm text-navy-900 bg-white border border-stone-200 rounded-md outline-none focus:border-navy-400 focus:ring-1 focus:ring-navy-200" />
                    <input value={editForm.organization || ''} onChange={(e) => setEditForm(f => ({ ...f, organization: e.target.value }))} placeholder="Organization" className="col-span-2 h-8 px-2.5 text-sm text-navy-900 bg-white border border-stone-200 rounded-md outline-none focus:border-navy-400 focus:ring-1 focus:ring-navy-200" />
                    <input value={editForm.email || ''} onChange={(e) => setEditForm(f => ({ ...f, email: e.target.value }))} placeholder="Email" className="h-8 px-2.5 text-xs text-navy-900 bg-white border border-stone-200 rounded-md outline-none focus:border-navy-400 focus:ring-1 focus:ring-navy-200" />
                    <input value={editForm.phone || ''} onChange={(e) => setEditForm(f => ({ ...f, phone: e.target.value }))} placeholder="Phone" className="h-8 px-2.5 text-xs text-navy-900 bg-white border border-stone-200 rounded-md outline-none focus:border-navy-400 focus:ring-1 focus:ring-navy-200" />
                    <input value={editForm.linkedin || ''} onChange={(e) => setEditForm(f => ({ ...f, linkedin: e.target.value }))} placeholder="LinkedIn URL" className="col-span-2 h-8 px-2.5 text-xs text-navy-900 bg-white border border-stone-200 rounded-md outline-none focus:border-navy-400 focus:ring-1 focus:ring-navy-200" />
                    <input value={editForm.evidence_url || ''} onChange={(e) => setEditForm(f => ({ ...f, evidence_url: e.target.value }))} placeholder="Evidence URL" className="col-span-2 h-8 px-2.5 text-xs text-navy-900 bg-white border border-stone-200 rounded-md outline-none focus:border-navy-400 focus:ring-1 focus:ring-navy-200" />
                  </div>
                  <div className="flex items-center gap-2">
                    <button onClick={saveEdit} className="inline-flex items-center gap-1.5 px-3 py-1.5 text-xs font-semibold bg-navy-900 text-white rounded-md hover:bg-navy-800 transition">
                      <Check className="w-3 h-3" /> Save
                    </button>
                    <button onClick={cancelEdit} className="px-3 py-1.5 text-xs font-medium text-stone-500 hover:text-stone-700 transition">Cancel</button>
                  </div>
                </div>
              ) : (
                <>
                  <div className="flex items-center gap-2">
                    <span className="text-sm font-semibold text-navy-900">{c.name}</span>
                    {c.is_primary && <Star className="w-3.5 h-3.5 text-gold-500 fill-gold-500" />}
                    <div className="flex items-center gap-2 ml-auto">
                      <PriorityBadge label={(c as any).priority_label} reason={(c as any).priority_reason} />
                      {c.score > 0 && (
                        <div className="flex flex-col items-end relative">
                          <button onClick={() => setOpenBreakdownId(openBreakdownId === c.id ? null : c.id)} title="Click to see how this score was calculated" className="text-sm font-bold text-navy-900 hover:text-blue-700 transition cursor-pointer underline-offset-2 hover:underline">
                            {c.score}
                          </button>
                          {c.confidence && (
                            <span className={cn('text-2xs font-bold uppercase', c.confidence === 'high' ? 'text-emerald-600' : c.confidence === 'medium' ? 'text-gold-600' : 'text-stone-400')}>
                              {c.confidence}
                            </span>
                          )}
                          {openBreakdownId === c.id && (c as any).score_breakdown && (() => {
                            const b = (c as any).score_breakdown as Record<string, any>
                            return (
                              <div className="absolute top-full right-0 mt-1 z-20 w-80 bg-white border border-stone-200 rounded-lg shadow-lg p-3 text-left" onClick={(e) => e.stopPropagation()}>
                                <div className="flex items-center justify-between mb-2 pb-2 border-b border-stone-100">
                                  <span className="text-xs font-bold text-navy-900 uppercase tracking-wide">Why this score?</span>
                                  <button onClick={() => setOpenBreakdownId(null)} className="text-stone-400 hover:text-stone-700 text-xs">✕</button>
                                </div>
                                <div className="space-y-2 text-xs">
                                  <div className="flex justify-between">
                                    <span className="text-stone-500">Title tier:</span>
                                    <span className="font-semibold text-navy-900">{b.title?.tier || 'UNKNOWN'} ({b.title?.base_points ?? '?'} pts)</span>
                                  </div>
                                  <div className="flex justify-between">
                                    <span className="text-stone-500">Scope:</span>
                                    <span className="font-semibold text-navy-900">{(b.scope?.value || 'unknown').replace(/_/g, ' ')} (×{b.scope?.multiplier ?? '?'})</span>
                                  </div>
                                  <div className="flex justify-between border-t border-stone-100 pt-2">
                                    <span className="text-stone-500">Title score:</span>
                                    <span className="font-semibold text-navy-900">{b.title_score ?? '?'}</span>
                                  </div>
                                  {b.strategist?.priority && (
                                    <div className="flex justify-between">
                                      <span className="text-stone-500">Strategist {b.strategist.priority} floor:</span>
                                      <span className={cn('font-semibold', b.strategist.applied ? 'text-emerald-700' : 'text-stone-400')}>
                                        {b.strategist.floor ?? '?'} {b.strategist.applied ? '(applied)' : '(not needed)'}
                                      </span>
                                    </div>
                                  )}
                                  <div className="flex justify-between border-t border-stone-100 pt-2">
                                    <span className="text-stone-900 font-bold">Final score:</span>
                                    <span className="font-bold text-blue-700 text-sm">{b.final_score ?? c.score}</span>
                                  </div>
                                </div>
                              </div>
                            )
                          })()}
                        </div>
                      )}
                    </div>
                  </div>

                  {c.title && <p className="text-xs text-stone-500 mt-0.5">{c.title}</p>}
                  {c.organization && <p className="text-xs text-stone-400">{c.organization}</p>}

                  <div className="flex items-center gap-3 mt-2 flex-wrap">
                    {c.linkedin && (
                      <a href={c.linkedin} target="_blank" rel="noopener noreferrer" className="inline-flex items-center gap-1.5 px-2.5 py-1 text-xs font-medium text-blue-700 bg-blue-50 rounded-md hover:bg-blue-100 transition">
                        <Linkedin className="w-3.5 h-3.5" /> LinkedIn
                      </a>
                    )}
                    {c.email ? (
                      <a href={`mailto:${c.email}`} className="flex items-center gap-1.5 text-xs text-navy-600 hover:underline">
                        <Mail className="w-3.5 h-3.5" /> {c.email}
                        {c.found_via?.startsWith('wiza') && (
                          <span className="text-[10px] px-1.5 py-0.5 bg-violet-50 text-violet-600 rounded font-medium">Wiza</span>
                        )}
                      </a>
                    ) : c.linkedin ? (
                      <WizaEmailButton
                        contactId={c.id}
                        hotelId={hotelId}
                        onEmailFound={() => qc.invalidateQueries({ queryKey: ['hotel-contacts', hotelId] })}
                      />
                    ) : null}
                    {c.phone && (
                      <a href={`tel:${c.phone}`} className="flex items-center gap-1.5 text-xs text-navy-600 hover:underline">
                        <Phone className="w-3.5 h-3.5" /> {c.phone}
                      </a>
                    )}
                  </div>

                  {(c.scope || c.source_detail) && (
                    <div className="flex items-center gap-2 mt-2.5 flex-wrap">
                      {c.scope && (
                        <button
                          onClick={() => handleToggleScope(c.id, c.scope || 'chain_area')}
                          title="Click to cycle scope"
                          className={cn(
                            'text-2xs font-bold px-2 py-0.5 rounded-full uppercase flex-shrink-0 cursor-pointer transition hover:ring-2 hover:ring-offset-1',
                            c.scope === 'hotel_specific' ? 'bg-emerald-50 text-emerald-600 hover:ring-emerald-300' :
                            c.scope === 'chain_area' ? 'bg-amber-50 text-amber-600 hover:ring-amber-300' :
                            c.scope === 'management_corporate' ? 'bg-blue-50 text-blue-700 hover:ring-blue-300' :
                            c.scope === 'chain_corporate' ? 'bg-stone-100 text-stone-500 hover:ring-stone-300' :
                            c.scope === 'owner' ? 'bg-purple-50 text-purple-700 hover:ring-purple-300' :
                            'bg-stone-100 text-stone-500',
                          )}
                        >
                          {c.scope === 'hotel_specific' ? 'Hotel Specific' :
                           c.scope === 'chain_area' ? 'Chain/Area' :
                           c.scope === 'management_corporate' ? 'Management Corporate' :
                           c.scope === 'chain_corporate' ? 'Chain Corporate' :
                           c.scope === 'owner' ? 'Owner' :
                           c.scope.replace(/_/g, ' ')}
                        </button>
                      )}
                      {c.source_detail && typeof c.source_detail === 'string' && (
                        <span className="text-xs text-stone-500">{c.source_detail}</span>
                      )}
                    </div>
                  )}

                  {(() => {
                    const ev = (c as any).evidence as Array<{
                      quote: string; source_url: string; source_domain?: string;
                      trust_tier?: string; source_year?: number | null;
                    }> | null | undefined
                    if (ev && Array.isArray(ev) && ev.length > 0) {
                      const allStale = ev.every(e => isStale(e.source_year))
                      const topTier = ev[0]?.trust_tier || 'unknown'
                      const topStyle = tierStyles[topTier] || tierStyles.unknown
                      return (
                        <div className="mt-2.5">
                          <button
                            onClick={() => setOpenEvidenceId(openEvidenceId === c.id ? null : c.id)}
                            className={cn('inline-flex items-center gap-1.5 px-2.5 py-1 text-xs font-medium rounded-md border transition hover:ring-2 hover:ring-offset-1 hover:ring-stone-300', topStyle.cls)}
                          >
                            <span>Evidence ({ev.length})</span>
                            <span className="text-[10px] opacity-75">{topStyle.label}</span>
                            {allStale && <span className="text-[10px] text-red-600 font-bold">⚠ STALE</span>}
                            <span className="text-[9px] opacity-60">{openEvidenceId === c.id ? '▲' : '▼'}</span>
                          </button>
                          {openEvidenceId === c.id && (
                            <div className="mt-2 space-y-2 border-l-2 border-stone-200 pl-3">
                              {ev.map((item, i) => {
                                const style = tierStyles[item.trust_tier || 'unknown'] || tierStyles.unknown
                                const stale = isStale(item.source_year)
                                return (
                                  <div key={i} className="bg-stone-50 rounded-md p-2.5 border border-stone-100">
                                    <div className="flex items-center gap-2 mb-1.5 flex-wrap">
                                      <span className={cn('text-[10px] font-bold px-1.5 py-0.5 rounded border', style.cls)}>{style.label}</span>
                                      <span className="text-[10px] text-stone-500 font-medium">{item.source_domain || 'unknown source'}</span>
                                      {item.source_year && (
                                        <span className={cn('text-[10px]', stale ? 'text-red-600 font-bold' : 'text-stone-400')}>
                                          {item.source_year}{stale ? ' ⚠' : ''}
                                        </span>
                                      )}
                                    </div>
                                    {item.quote && <p className="text-xs text-stone-700 italic leading-relaxed mb-1.5">"{item.quote}"</p>}
                                    <a href={item.source_url} target="_blank" rel="noopener noreferrer" className="inline-flex items-center gap-1 text-[11px] text-blue-600 hover:underline">
                                      <ExternalLink className="w-2.5 h-2.5" /> Open source
                                    </a>
                                  </div>
                                )
                              })}
                            </div>
                          )}
                        </div>
                      )
                    }
                    if (c.evidence_url && typeof c.evidence_url === 'string') {
                      return (
                        <a href={c.evidence_url} target="_blank" rel="noopener noreferrer" className="flex items-center gap-1.5 mt-1.5 text-xs text-stone-500 hover:text-blue-600 hover:underline">
                          <ExternalLink className="w-3 h-3" /> View Evidence
                          <span className="text-[10px] text-stone-400">(legacy)</span>
                        </a>
                      )
                    }
                    return null
                  })()}

                  <div className="flex items-center gap-2 mt-2.5">
                    <button onClick={() => handleSave(c.id)} className={cn('inline-flex items-center gap-1.5 px-2.5 py-1 text-xs font-medium rounded-md border transition', c.is_saved ? 'border-navy-200 bg-navy-50 text-navy-700' : 'border-stone-200 text-stone-500 hover:bg-stone-50')}>
                      {c.is_saved ? <BookmarkCheck className="w-3 h-3" /> : <Bookmark className="w-3 h-3" />}
                      {c.is_saved ? 'Saved' : 'Save'}
                    </button>
                    {!c.is_primary && (
                      <button onClick={() => handleSetPrimary(c.id)} className="inline-flex items-center gap-1.5 px-2.5 py-1 text-xs font-medium rounded-md border border-stone-200 text-stone-500 hover:bg-stone-50 transition">
                        <Star className="w-3 h-3" /> Set Primary
                      </button>
                    )}
                    {c.is_primary && (
                      <span className="inline-flex items-center gap-1.5 px-2.5 py-1 text-xs font-semibold rounded-md bg-gold-50 text-gold-600 border border-gold-200">
                        <Star className="w-3 h-3 fill-gold-500" /> Primary
                      </span>
                    )}
                  </div>
                </>
              )}
            </div>
          </div>
        </div>
      ))}

      {showAdd ? (
        <div className="mt-2 p-4 rounded-lg border border-navy-200 bg-navy-50/30 space-y-2">
          <p className="text-xs font-bold text-navy-900">Add Contact Manually</p>
          <div className="grid grid-cols-2 gap-2">
            <input value={addForm.name || ''} onChange={(e) => setAddForm(f => ({ ...f, name: e.target.value }))} placeholder="Name *" className="col-span-2 h-8 px-2.5 text-sm bg-white border border-stone-200 rounded-md outline-none focus:border-navy-400" autoFocus />
            <input value={addForm.title || ''} onChange={(e) => setAddForm(f => ({ ...f, title: e.target.value }))} placeholder="Title / Role" className="col-span-2 h-8 px-2.5 text-sm bg-white border border-stone-200 rounded-md outline-none focus:border-navy-400" />
            <input value={addForm.organization || ''} onChange={(e) => setAddForm(f => ({ ...f, organization: e.target.value }))} placeholder="Organization" className="col-span-2 h-8 px-2.5 text-sm bg-white border border-stone-200 rounded-md outline-none focus:border-navy-400" />
            <input value={addForm.email || ''} onChange={(e) => setAddForm(f => ({ ...f, email: e.target.value }))} placeholder="Email" className="h-8 px-2.5 text-xs bg-white border border-stone-200 rounded-md outline-none focus:border-navy-400" />
            <input value={addForm.phone || ''} onChange={(e) => setAddForm(f => ({ ...f, phone: e.target.value }))} placeholder="Phone" className="h-8 px-2.5 text-xs bg-white border border-stone-200 rounded-md outline-none focus:border-navy-400" />
            <input value={addForm.linkedin || ''} onChange={(e) => setAddForm(f => ({ ...f, linkedin: e.target.value }))} placeholder="LinkedIn URL" className="col-span-2 h-8 px-2.5 text-xs bg-white border border-stone-200 rounded-md outline-none focus:border-navy-400" />
            <select value={addForm.scope || 'hotel_specific'} onChange={(e) => setAddForm(f => ({ ...f, scope: e.target.value }))} className="col-span-2 h-8 px-2.5 text-xs bg-white border border-stone-200 rounded-md outline-none focus:border-navy-400">
              <option value="hotel_specific">Hotel Specific</option>
              <option value="chain_area">Chain / Area</option>
              <option value="management_corporate">Management Corporate</option>
              <option value="chain_corporate">Chain Corporate</option>
              <option value="owner">Owner</option>
            </select>
          </div>
          <div className="flex items-center gap-2 pt-1">
            <button
              onClick={async () => {
                if (!addForm.name?.trim()) return
                setAdding(true)
                try {
                  await addHotelContact(hotelId, addForm)
                  qc.invalidateQueries({ queryKey: ['hotel-contacts', hotelId] })
                  qc.invalidateQueries({ queryKey: ['existing-hotel', hotelId] })
                  setAddForm({ scope: 'hotel_specific' })
                  setShowAdd(false)
                } catch { /* silent */ }
                setAdding(false)
              }}
              disabled={adding || !addForm.name?.trim()}
              className="inline-flex items-center gap-1.5 px-3 py-1.5 text-xs font-semibold bg-navy-900 text-white rounded-md hover:bg-navy-800 transition disabled:opacity-50"
            >
              {adding ? <Loader2 className="w-3 h-3 animate-spin" /> : <Check className="w-3 h-3" />} Add Contact
            </button>
            <button onClick={() => { setShowAdd(false); setAddForm({ scope: 'hotel_specific' }) }} className="px-3 py-1.5 text-xs font-medium text-stone-500 hover:text-stone-700 transition">
              Cancel
            </button>
          </div>
        </div>
      ) : (
        <div className="flex gap-2 mt-2">
          <button onClick={() => setShowAdd(true)} className="flex-1 py-2.5 text-xs font-semibold text-navy-600 hover:text-navy-800 hover:bg-navy-50 rounded-lg border border-dashed border-navy-200 transition">
            + Add Contact
          </button>
          <button onClick={onEnrich} disabled={enrichingLive} className="flex-1 py-2.5 text-xs font-semibold text-stone-500 hover:text-navy-700 hover:bg-stone-50 rounded-lg border border-dashed border-stone-200 transition disabled:opacity-50">
            {enrichingLive ? 'Searching...' : 'Re-run Enrichment'}
          </button>
        </div>
      )}
    </div>
  )
}


/* ═════════════════════════════════════════════════════════
   EDIT TAB
   ═════════════════════════════════════════════════════════ */
function HotelEditTab({ hotel, hotelId }: { hotel: ExistingHotel; hotelId: number }) {
  const qc = useQueryClient()
  const h = hotel as any
  const [saving, setSaving] = useState(false)
  const [saveMsg, setSaveMsg] = useState('')
  const [form, setForm] = useState({
    hotel_name:         h.hotel_name || h.name || '',
    brand:              hotel.brand || '',
    chain:              h.chain || '',
    brand_tier:         hotel.brand_tier || '',
    hotel_type:         h.hotel_type || h.property_type || '',
    address:            hotel.address || '',
    city:               hotel.city || '',
    state:              hotel.state || '',
    country:            hotel.country || 'USA',
    zip_code:           hotel.zip_code || '',
    zone:               h.zone || '',
    room_count:         hotel.room_count ? String(hotel.room_count) : '',
    hotel_website:      h.hotel_website || (h as any).website || '',
    opening_date:       hotel.opening_date || '',
    management_company: hotel.management_company || '',
    developer:          hotel.developer || '',
    owner:              hotel.owner || '',
    contact_name:       h.contact_name || h.gm_name || '',
    contact_title:      h.contact_title || h.gm_title || '',
    contact_email:      h.contact_email || h.gm_email || '',
    contact_phone:      h.contact_phone || h.gm_phone || '',
    sap_bp_code:        h.sap_bp_code || '',
    client_notes:       h.client_notes || '',
    is_client:          hotel.is_client || false,
  })

  useEffect(() => {
    setForm({
      hotel_name:         h.hotel_name || h.name || '',
      brand:              hotel.brand || '',
      chain:              h.chain || '',
      brand_tier:         hotel.brand_tier || '',
      hotel_type:         h.hotel_type || h.property_type || '',
      address:            hotel.address || '',
      city:               hotel.city || '',
      state:              hotel.state || '',
      country:            hotel.country || 'USA',
      zip_code:           hotel.zip_code || '',
      zone:               h.zone || '',
      room_count:         hotel.room_count ? String(hotel.room_count) : '',
      hotel_website:      h.hotel_website || (h as any).website || '',
      opening_date:       hotel.opening_date || '',
      management_company: hotel.management_company || '',
      developer:          hotel.developer || '',
      owner:              hotel.owner || '',
      contact_name:       h.contact_name || h.gm_name || '',
      contact_title:      h.contact_title || h.gm_title || '',
      contact_email:      h.contact_email || h.gm_email || '',
      contact_phone:      h.contact_phone || h.gm_phone || '',
      sap_bp_code:        h.sap_bp_code || '',
      client_notes:       h.client_notes || '',
      is_client:          hotel.is_client || false,
    })
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [hotel.id, hotel.updated_at])

  function set(key: string, val: any) { setForm((prev) => ({ ...prev, [key]: val })) }

  async function handleSave() {
    setSaving(true); setSaveMsg('')
    try {
      const payload: any = {}
      Object.entries(form).forEach(([k, v]) => {
        if (v !== '' && v !== null && v !== undefined) {
          payload[k] = (k === 'room_count' && v) ? Number(v) : v
        }
      })
      await editExistingHotel(hotelId, payload)
      setSaveMsg('Saved!')
      qc.invalidateQueries({ queryKey: ['existing-hotel', hotelId] })
      qc.invalidateQueries({ queryKey: ['existing-hotels'] })
      setTimeout(() => setSaveMsg(''), 3000)
    } catch {
      setSaveMsg('Error saving')
    }
    setSaving(false)
  }

  return (
    <div className="space-y-4 animate-fadeIn">
      <div className="grid grid-cols-2 gap-4">
        <EditField label="Hotel Name"      value={form.hotel_name}         onChange={(v) => set('hotel_name', v)} span={2} />
        <EditField label="Brand"           value={form.brand}              onChange={(v) => set('brand', v)} />
        <EditField label="Chain"           value={form.chain}              onChange={(v) => set('chain', v)} />
        <EditField label="Brand Tier"      value={form.brand_tier}         onChange={(v) => set('brand_tier', v)} />
        <EditField label="Type"            value={form.hotel_type}         onChange={(v) => set('hotel_type', v)} />
        <EditField label="Address"         value={form.address}            onChange={(v) => set('address', v)} span={2} />
        <EditField label="City"            value={form.city}               onChange={(v) => set('city', v)} />
        <EditField label="State"           value={form.state}              onChange={(v) => set('state', v)} />
        <EditField label="Country"         value={form.country}            onChange={(v) => set('country', v)} />
        <EditField label="Zip Code"        value={form.zip_code}           onChange={(v) => set('zip_code', v)} />
        <EditField label="Zone"            value={form.zone}               onChange={(v) => set('zone', v)} />
        <EditField label="Rooms"           value={form.room_count}         onChange={(v) => set('room_count', v)} />
        <EditField label="Website"         value={form.hotel_website}      onChange={(v) => set('hotel_website', v)} span={2} />
        <EditField label="Opening"         value={form.opening_date}       onChange={(v) => set('opening_date', v)} />
        <EditField label="Mgmt Co."        value={form.management_company} onChange={(v) => set('management_company', v)} span={2} />
        <EditField label="Developer"       value={form.developer}          onChange={(v) => set('developer', v)} />
        <EditField label="Owner"           value={form.owner}              onChange={(v) => set('owner', v)} />
        <EditField label="Contact Name"    value={form.contact_name}       onChange={(v) => set('contact_name', v)} />
        <EditField label="Contact Title"   value={form.contact_title}      onChange={(v) => set('contact_title', v)} />
        <EditField label="Contact Email"   value={form.contact_email}      onChange={(v) => set('contact_email', v)} />
        <EditField label="Contact Phone"   value={form.contact_phone}      onChange={(v) => set('contact_phone', v)} />
        <EditField label="SAP BP Code"     value={form.sap_bp_code}        onChange={(v) => set('sap_bp_code', v)} />
        <div className="flex items-end gap-2">
          <label className="inline-flex items-center gap-2 text-xs text-stone-600 cursor-pointer">
            <input type="checkbox" checked={!!form.is_client} onChange={(e) => set('is_client', e.target.checked)} className="rounded border-stone-300" />
            <span className="font-semibold">Mark as Client</span>
          </label>
        </div>
        <EditField label="Client Notes"    value={form.client_notes}       onChange={(v) => set('client_notes', v)} span={2} />
      </div>

      <div className="flex items-center gap-3 pt-2">
        <button onClick={handleSave} disabled={saving} className="flex items-center gap-1.5 px-4 py-2.5 text-xs font-semibold bg-navy-900 text-white rounded-lg hover:bg-navy-800 transition disabled:opacity-50">
          {saving ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Save className="w-3.5 h-3.5" />}
          Save Changes
        </button>
        {saveMsg && <span className={cn('text-xs font-semibold', saveMsg.includes('Error') ? 'text-red-500' : 'text-emerald-600')}>{saveMsg}</span>}
      </div>
    </div>
  )
}


/* ═════════════════════════════════════════════════════════
   SOURCES TAB
   ═════════════════════════════════════════════════════════ */
function HotelSourcesTab({ hotel }: { hotel: ExistingHotel }) {
  const h = hotel as any
  const sourceList: string[] = []

  if (hotel.source_url) {
    hotel.source_url.split(',').map((s) => s.trim()).filter(Boolean).forEach((u) => {
      if (!sourceList.includes(u)) sourceList.push(u)
    })
  }
  if (hotel.source_urls && Array.isArray(hotel.source_urls)) {
    hotel.source_urls.forEach((u) => {
      if (u && !sourceList.includes(u)) sourceList.push(u)
    })
  }

  return (
    <div className="space-y-5 animate-fadeIn">
      <Section title="Provenance">
        <div className="space-y-2 text-xs">
          <div className="flex justify-between">
            <span className="text-stone-400">Data source</span>
            <span className="text-navy-700 font-semibold capitalize">{h.data_source || '—'}</span>
          </div>
          <div className="flex justify-between">
            <span className="text-stone-400">Status</span>
            <span className="text-navy-700 font-semibold capitalize">{hotel.status || '—'}</span>
          </div>
          <div className="flex justify-between">
            <span className="text-stone-400">Last verified</span>
            <span className="text-navy-700 font-semibold">{h.last_verified_at ? new Date(h.last_verified_at).toLocaleDateString() : '—'}</span>
          </div>
          {hotel.insightly_id && (
            <div className="flex justify-between">
              <span className="text-stone-400">Insightly</span>
              <span className="text-navy-700 font-semibold">#{hotel.insightly_id}</span>
            </div>
          )}
        </div>
      </Section>

      <Section title={`Source Articles (${sourceList.length})`}>
        {sourceList.length > 0 ? (
          <div className="space-y-2">
            {sourceList.map((url, i) => {
              const href = url.startsWith('http') ? url : `https://${url}`
              const extraction = (hotel.source_extractions && typeof hotel.source_extractions === 'object')
                ? (hotel.source_extractions as Record<string, any>)[url] : null
              return (
                <div key={i} className="rounded-lg border border-stone-100 overflow-hidden">
                  <a href={href} target="_blank" rel="noopener noreferrer" className="flex items-center gap-2.5 px-3.5 py-2.5 bg-stone-50 hover:bg-stone-100 transition group">
                    <Link2 className="w-4 h-4 text-stone-400 group-hover:text-navy-600 flex-shrink-0" />
                    <span className="text-xs text-navy-600 truncate group-hover:underline">{url}</span>
                    <ExternalLink className="w-3.5 h-3.5 text-stone-300 group-hover:text-navy-600 ml-auto flex-shrink-0" />
                  </a>
                  {extraction && typeof extraction === 'object' && Object.keys(extraction).length > 0 && (
                    <div className="px-3.5 py-2.5 border-t border-stone-100 grid grid-cols-2 gap-x-4 gap-y-1 text-xs">
                      {Object.entries(extraction).map(([k, v]) => {
                        if (k === 'key_insights') return null
                        if (v === null || v === undefined) return null
                        if (typeof v === 'object' && !Array.isArray(v)) return null
                        const display = Array.isArray(v) ? v.join(', ') : String(v)
                        if (!display) return null
                        return (
                          <div key={k} className="flex gap-2">
                            <span className="text-stone-400 capitalize whitespace-nowrap">{k.replace(/_/g, ' ')}:</span>
                            <span className="text-navy-800 font-medium truncate">{display}</span>
                          </div>
                        )
                      })}
                    </div>
                  )}
                </div>
              )
            })}
          </div>
        ) : (
          <p className="text-xs text-stone-400">No source URLs recorded</p>
        )}
      </Section>

      {hotel.score_breakdown && typeof hotel.score_breakdown === 'object' && (
        <Section title="Score Breakdown">
          <div className="space-y-1">
            {Object.entries(hotel.score_breakdown).map(([key, val]) => {
              const obj = (val && typeof val === 'object') ? val as Record<string, any> : null
              const points = obj?.points !== undefined ? String(obj.points) : null
              const reason = (typeof obj?.tier === 'string') ? obj.tier
                           : (typeof obj?.reason === 'string') ? obj.reason
                           : (typeof obj?.label === 'string') ? obj.label
                           : (typeof obj?.detail === 'string') ? obj.detail
                           : null
              return (
                <div key={key} className="flex items-start justify-between gap-4 py-1.5 border-b border-stone-50 last:border-0">
                  <div className="min-w-0">
                    <span className="text-sm text-navy-800 font-medium capitalize">{key.replace(/_/g, ' ')}</span>
                    {reason && <p className="text-xs text-stone-400 mt-0.5">{reason}</p>}
                  </div>
                  <span className="text-sm font-bold text-navy-900 tabular-nums flex-shrink-0">
                    {points !== null ? `+${points}` : safe(val)}
                  </span>
                </div>
              )
            })}
          </div>
        </Section>
      )}
    </div>
  )
}


/* ═════════════════════════════════════════════════════════
   KEY INSIGHTS
   ═════════════════════════════════════════════════════════ */
function KeyInsights({ extractions }: { extractions: Record<string, any> }) {
  const insights: string[] = []
  for (const [_url, data] of Object.entries(extractions)) {
    if (!data || typeof data !== 'object') continue
    const ki = data.key_insights
    if (ki && typeof ki === 'string' && ki.length > 10) {
      if (ki.toLowerCase().includes('no specific details') || ki.toLowerCase().includes('no additional details')) continue
      if (!insights.includes(ki)) insights.push(ki)
    }
  }
  if (insights.length === 0) return null

  return (
    <div className="bg-slate-50 rounded-lg border border-slate-200/80 p-4">
      <ul className="space-y-2">
        {insights.map((insight, i) => (
          <li key={i} className="flex gap-2.5 text-[13px] leading-relaxed text-slate-700">
            <span className="text-slate-400 flex-shrink-0 mt-0.5">•</span>
            <span>{insight}</span>
          </li>
        ))}
      </ul>
    </div>
  )
}


/* ═════════════════════════════════════════════════════════
   SHARED COMPONENTS
   ═════════════════════════════════════════════════════════ */
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
        <div className="text-sm text-navy-800 leading-snug font-medium">{safe(value)}</div>
      </div>
    </div>
  )
}

function EditField({ label, value, onChange, span }: {
  label: string; value: string; onChange: (v: string) => void; span?: number
}) {
  return (
    <div className={span === 2 ? 'col-span-2' : ''}>
      <label className="field-label block mb-1">{label}</label>
      <input
        type="text"
        value={value}
        onChange={(e) => onChange(e.target.value)}
        className="w-full h-9 px-3 text-sm text-navy-900 bg-stone-50 border border-stone-200 rounded-lg outline-none focus:border-navy-400 focus:ring-1 focus:ring-navy-200 transition"
      />
    </div>
  )
}

function ActionBtn({ onClick, color, title, children }: {
  onClick: (e: React.MouseEvent) => void; color: string; title: string; children: React.ReactNode
}) {
  const colors: Record<string, string> = {
    emerald: 'hover:bg-emerald-50 text-emerald-500 hover:text-emerald-700',
    red:     'hover:bg-red-50 text-red-400 hover:text-red-600',
    blue:    'hover:bg-blue-50 text-blue-500 hover:text-blue-700',
    amber:   'hover:bg-amber-50 text-amber-500 hover:text-amber-700',
  }
  return (
    <button onClick={onClick} title={title} className={cn('p-1.5 rounded-md transition-all duration-100', colors[color] || '')}>
      {children}
    </button>
  )
}
