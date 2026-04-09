import { useState, useEffect } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import {
  Building2, TrendingUp, AlertTriangle,
  Search, ChevronUp, ChevronDown, ChevronLeft, ChevronRight,
  Upload, DollarSign, Activity, X, CheckCircle2, XCircle, Loader2,
} from 'lucide-react'
import { cn } from '@/lib/utils'
import { useSAPClients, useSAPFilters, useSAPSummary } from '@/hooks/useSAP'
import { importSAPCSV, type SAPClientParams } from '@/api/sap'

// ── Helpers ──

function fmtMoney(n: number | null | undefined): string {
  if (n == null) return '$0'
  if (Math.abs(n) >= 1_000_000) return `$${(n / 1_000_000).toFixed(1)}M`
  if (Math.abs(n) >= 1_000) return `$${(n / 1_000).toFixed(1)}K`
  return `$${n.toFixed(0)}`
}

function fmtRevenue(n: number | null | undefined): string {
  if (n == null) return '$0.00'
  return n.toLocaleString('en-US', { style: 'currency', currency: 'USD', minimumFractionDigits: 2 })
}

const CHURN_CONFIG: Record<string, { label: string; color: string; bg: string }> = {
  active:  { label: 'Active',  color: 'text-emerald-700', bg: 'bg-emerald-50 border-emerald-200' },
  healthy: { label: 'Healthy', color: 'text-blue-700',    bg: 'bg-blue-50 border-blue-200' },
  watch:   { label: 'Watch',   color: 'text-amber-700',   bg: 'bg-amber-50 border-amber-200' },
  at_risk: { label: 'At Risk', color: 'text-orange-700',  bg: 'bg-orange-50 border-orange-200' },
  churned: { label: 'Churned', color: 'text-red-700',     bg: 'bg-red-50 border-red-200' },
  unknown: { label: 'Unknown', color: 'text-stone-500',   bg: 'bg-stone-50 border-stone-200' },
}

const TYPE_CONFIG: Record<string, { label: string; icon: string }> = {
  hotel:      { label: 'Hotel',      icon: '🏨' },
  parking:    { label: 'Parking',    icon: '🅿️' },
  restaurant: { label: 'Restaurant', icon: '🍽️' },
  condo:      { label: 'Condo',      icon: '🏢' },
  other:      { label: 'Other',      icon: '📋' },
  unknown:    { label: 'Unknown',    icon: '❓' },
}

// ── Toast Type ──

type ToastState = {
  variant: 'success' | 'error' | 'loading'
  title: string
  message?: string
} | null

// ── Stat Card ──

function StatCard({
  label, value, sub, icon: Icon, accent,
}: {
  label: string
  value: string | number
  sub?: string
  icon: React.ElementType
  accent: string
}) {
  return (
    <div className="bg-white rounded-xl border border-stone-200 p-4 flex items-start gap-3.5 shadow-sm">
      <div className={cn('w-10 h-10 rounded-lg flex items-center justify-center flex-shrink-0', accent)}>
        <Icon className="w-5 h-5" />
      </div>
      <div className="min-w-0">
        <p className="text-[11px] font-semibold text-stone-400 uppercase tracking-wider">{label}</p>
        <p className="text-xl font-bold text-navy-900 mt-0.5 leading-tight">{value}</p>
        {sub && <p className="text-[11px] text-stone-500 mt-0.5">{sub}</p>}
      </div>
    </div>
  )
}

// ── Toast Component ──

function Toast({ toast, onClose }: { toast: ToastState; onClose: () => void }) {
  useEffect(() => {
    if (!toast || toast.variant === 'loading') return
    const t = setTimeout(onClose, 5000)
    return () => clearTimeout(t)
  }, [toast, onClose])

  if (!toast) return null

  const config = {
    success: {
      icon: CheckCircle2,
      iconColor: 'text-emerald-600',
      iconBg: 'bg-emerald-50',
      border: 'border-emerald-200',
    },
    error: {
      icon: XCircle,
      iconColor: 'text-red-600',
      iconBg: 'bg-red-50',
      border: 'border-red-200',
    },
    loading: {
      icon: Loader2,
      iconColor: 'text-navy-600',
      iconBg: 'bg-navy-50',
      border: 'border-navy-200',
    },
  }[toast.variant]

  const Icon = config.icon

  return (
    <div className="fixed bottom-5 right-5 z-50 animate-slideUp">
      <div className={cn(
        'flex items-start gap-3 bg-white rounded-xl border shadow-lg px-4 py-3.5 min-w-[320px] max-w-[420px]',
        config.border,
      )}>
        <div className={cn('w-9 h-9 rounded-lg flex items-center justify-center flex-shrink-0', config.iconBg)}>
          <Icon className={cn('w-5 h-5', config.iconColor, toast.variant === 'loading' && 'animate-spin')} />
        </div>
        <div className="flex-1 min-w-0 pt-0.5">
          <p className="text-sm font-bold text-navy-900 leading-snug">{toast.title}</p>
          {toast.message && (
            <p className="text-xs text-stone-500 mt-0.5 leading-relaxed">{toast.message}</p>
          )}
        </div>
        {toast.variant !== 'loading' && (
          <button
            onClick={onClose}
            className="p-1 -mr-1 -mt-0.5 text-stone-400 hover:text-stone-600 rounded hover:bg-stone-50 transition flex-shrink-0"
          >
            <X className="w-4 h-4" />
          </button>
        )}
      </div>
    </div>
  )
}

// ── Main Page ──

export default function ClientIntelligence() {
  const queryClient = useQueryClient()

  const [search, setSearch] = useState('')
  const [searchInput, setSearchInput] = useState('')
  const [group, setGroup] = useState('')
  const [state, setState] = useState('')
  const [salesRep, setSalesRep] = useState('')
  const [customerType, setCustomerType] = useState('')
  const [churnRisk, setChurnRisk] = useState('')
  const [isHotel, setIsHotel] = useState<string>('')
  const [sortBy, setSortBy] = useState('revenue_lifetime')
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('desc')
  const [page, setPage] = useState(1)
  const [importing, setImporting] = useState(false)
  const [toast, setToast] = useState<ToastState>(null)

  const params: SAPClientParams = {
    page,
    per_page: 50,
    sort_by: sortBy,
    sort_dir: sortDir,
    ...(search && { search }),
    ...(group && { group }),
    ...(state && { state }),
    ...(salesRep && { sales_rep: salesRep }),
    ...(customerType && { customer_type: customerType }),
    ...(churnRisk && { churn_risk: churnRisk }),
    ...(isHotel && { is_hotel: isHotel === 'true' }),
  }

  const { data, isLoading } = useSAPClients(params)
  const { data: filters } = useSAPFilters()
  const { data: summary } = useSAPSummary()

  const clients = data?.clients ?? []
  const totalPages = data?.total_pages ?? 1
  const total = data?.total ?? 0
  const churn = summary?.churn_breakdown ?? {} as Record<string, number>
  const rev = summary?.revenue ?? {} as Record<string, number>
  const filterGroups = filters?.groups ?? []
  const filterStates = filters?.states ?? []
  const filterReps = filters?.sales_reps ?? []

  const handleSearch = () => { setSearch(searchInput); setPage(1) }

  const handleSort = (field: string) => {
    if (sortBy === field) {
      setSortDir(sortDir === 'desc' ? 'asc' : 'desc')
    } else {
      setSortBy(field)
      setSortDir('desc')
    }
    setPage(1)
  }

  const clearFilters = () => {
    setSearch(''); setSearchInput(''); setGroup(''); setState('')
    setSalesRep(''); setCustomerType(''); setChurnRisk(''); setIsHotel(''); setPage(1)
  }

  const hasFilters = search || group || state || salesRep || customerType || churnRisk || isHotel

  const handleImport = async () => {
    const input = document.createElement('input')
    input.type = 'file'
    input.accept = '.csv,.xlsx,.xls'
    input.onchange = async (e) => {
      const file = (e.target as HTMLInputElement).files?.[0]
      if (!file) return
      setImporting(true)
      setToast({
        variant: 'loading',
        title: 'Importing SAP data...',
        message: file.name,
      })
      try {
        const result = await importSAPCSV(file)
        const created = result.created ?? 0
        const updated = result.updated ?? 0
        const errors = result.errors ?? 0
        const processed = result.processed ?? 0

        const parts: string[] = []
        if (created > 0) parts.push(`${created} new`)
        if (updated > 0) parts.push(`${updated} updated`)
        if (errors > 0) parts.push(`${errors} errors`)
        const message = parts.length > 0 ? parts.join(' · ') : 'All up to date'

        setToast({
          variant: errors > 0 ? 'error' : 'success',
          title: `Imported ${processed} clients`,
          message,
        })
        queryClient.invalidateQueries({ queryKey: ['sap-clients'] })
        queryClient.invalidateQueries({ queryKey: ['sap-summary'] })
        queryClient.invalidateQueries({ queryKey: ['sap-filters'] })
      } catch (err: any) {
        setToast({
          variant: 'error',
          title: 'Import failed',
          message: err.response?.data?.detail || err.message || 'Unknown error',
        })
      } finally {
        setImporting(false)
      }
    }
    input.click()
  }

  const SortIcon = ({ field }: { field: string }) => {
    if (sortBy !== field) return <ChevronDown className="w-3 h-3 text-stone-300" />
    return sortDir === 'desc'
      ? <ChevronDown className="w-3 h-3 text-navy-600" />
      : <ChevronUp className="w-3 h-3 text-navy-600" />
  }

  return (
    <div className="h-full flex flex-col overflow-hidden">
      {/* ═══ HEADER ═══ */}
      <div className="flex-shrink-0 px-5 pt-4 pb-3 bg-white border-b border-stone-200">
        <div className="flex items-center justify-between mb-4">
          <div>
            <h1 className="text-lg font-bold text-navy-900">Client Intelligence</h1>
            <p className="text-[11px] text-stone-500 mt-0.5">
              SAP Business One — {total} clients {isHotel === 'true' ? '(hotels only)' : ''}
            </p>
          </div>
          <button
            onClick={handleImport}
            disabled={importing}
            className="flex items-center gap-1.5 px-3 py-1.5 text-[11px] font-semibold rounded-lg bg-navy-800 text-white hover:bg-navy-900 transition shadow-sm disabled:opacity-50"
          >
            <Upload className="w-3 h-3" />
            {importing ? 'Importing...' : 'Import SAP CSV'}
          </button>
        </div>

        {/* ═══ STAT CARDS ═══ */}
        {summary && (
          <div className="grid grid-cols-5 gap-3 mb-4">
            <StatCard
              label="Total Clients"
              value={summary.total_clients ?? 0}
              sub={`${summary.hotel_clients ?? 0} hotels · ${summary.non_hotel_clients ?? 0} other`}
              icon={Building2}
              accent="bg-navy-100 text-navy-700"
            />
            <StatCard
              label="Lifetime Revenue"
              value={fmtMoney(rev.lifetime)}
              icon={DollarSign}
              accent="bg-emerald-100 text-emerald-700"
            />
            <StatCard
              label="2026 YTD"
              value={fmtMoney(rev.current_year)}
              sub={`vs ${fmtMoney(rev.last_year)} in 2025`}
              icon={TrendingUp}
              accent="bg-blue-100 text-blue-700"
            />
            <StatCard
              label="Active (30d)"
              value={churn.active ?? 0}
              sub={`${churn.healthy ?? 0} healthy`}
              icon={Activity}
              accent="bg-emerald-100 text-emerald-700"
            />
            <StatCard
              label="At Risk"
              value={(churn.at_risk ?? 0) + (churn.churned ?? 0)}
              sub={`${churn.watch ?? 0} on watch`}
              icon={AlertTriangle}
              accent="bg-red-100 text-red-700"
            />
          </div>
        )}

        {/* ═══ FILTERS ═══ */}
        <div className="flex items-center gap-2 flex-wrap">
          <div className="relative">
            <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-stone-400" />
            <input
              type="text"
              placeholder="Search clients..."
              value={searchInput}
              onChange={(e) => setSearchInput(e.target.value)}
              onKeyDown={(e) => e.key === 'Enter' && handleSearch()}
              className="pl-8 pr-3 py-1.5 text-[12px] border border-stone-200 rounded-lg w-52 focus:outline-none focus:ring-2 focus:ring-navy-200 focus:border-navy-400"
            />
          </div>

          <select value={group} onChange={(e) => { setGroup(e.target.value); setPage(1) }}
            className="px-2.5 py-1.5 text-[12px] border border-stone-200 rounded-lg bg-white focus:outline-none focus:ring-2 focus:ring-navy-200">
            <option value="">All Groups</option>
            {filterGroups.map((g) => <option key={g} value={g}>{g}</option>)}
          </select>

          <select value={state} onChange={(e) => { setState(e.target.value); setPage(1) }}
            className="px-2.5 py-1.5 text-[12px] border border-stone-200 rounded-lg bg-white focus:outline-none focus:ring-2 focus:ring-navy-200">
            <option value="">All States</option>
            {filterStates.map((s) => <option key={s} value={s}>{s}</option>)}
          </select>

          <select value={salesRep} onChange={(e) => { setSalesRep(e.target.value); setPage(1) }}
            className="px-2.5 py-1.5 text-[12px] border border-stone-200 rounded-lg bg-white focus:outline-none focus:ring-2 focus:ring-navy-200">
            <option value="">All Reps</option>
            {filterReps.map((r) => <option key={r} value={r}>{r}</option>)}
          </select>

          <select value={isHotel} onChange={(e) => { setIsHotel(e.target.value); setPage(1) }}
            className="px-2.5 py-1.5 text-[12px] border border-stone-200 rounded-lg bg-white focus:outline-none focus:ring-2 focus:ring-navy-200">
            <option value="">All Types</option>
            <option value="true">Hotels Only</option>
            <option value="false">Non-Hotels</option>
          </select>

          <select value={churnRisk} onChange={(e) => { setChurnRisk(e.target.value); setPage(1) }}
            className="px-2.5 py-1.5 text-[12px] border border-stone-200 rounded-lg bg-white focus:outline-none focus:ring-2 focus:ring-navy-200">
            <option value="">All Risk</option>
            <option value="active">Active</option>
            <option value="healthy">Healthy</option>
            <option value="watch">Watch</option>
            <option value="at_risk">At Risk</option>
            <option value="churned">Churned</option>
          </select>

          {hasFilters && (
            <button onClick={clearFilters}
              className="flex items-center gap-1 px-2.5 py-1.5 text-[11px] font-medium text-red-600 hover:bg-red-50 rounded-lg transition">
              <X className="w-3 h-3" /> Clear
            </button>
          )}
        </div>
      </div>

      {/* ═══ TABLE ═══ */}
      <div className="flex-1 overflow-auto">
        <table className="w-full">
          <thead className="sticky top-0 z-10">
            <tr className="bg-stone-50 border-b border-stone-200">
              {[
                { key: 'customer_name', label: 'Client', width: 'min-w-[240px]' },
                { key: 'customer_group', label: 'Group', width: 'min-w-[120px]' },
                { key: 'state', label: 'State', width: 'w-[70px]' },
                { key: 'revenue_lifetime', label: 'Lifetime Rev', width: 'w-[120px]' },
                { key: 'revenue_current_year', label: '2026 YTD', width: 'w-[100px]' },
                { key: 'revenue_last_year', label: '2025', width: 'w-[100px]' },
                { key: 'total_invoices', label: 'Invoices', width: 'w-[80px]' },
                { key: 'days_since_last_order', label: 'Last Order', width: 'w-[100px]' },
                { key: '', label: 'Risk', width: 'w-[90px]' },
              ].map((col) => (
                <th key={col.label} onClick={() => col.key && handleSort(col.key)}
                  className={cn(
                    'px-3 py-2.5 text-left text-[10px] font-semibold text-stone-500 uppercase tracking-wider',
                    col.width, col.key && 'cursor-pointer hover:text-navy-700 select-none',
                  )}>
                  <span className="flex items-center gap-1">
                    {col.label}
                    {col.key && <SortIcon field={col.key} />}
                  </span>
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-stone-100">
            {isLoading ? (
              <tr><td colSpan={9} className="px-3 py-16 text-center text-[13px] text-stone-400">Loading clients...</td></tr>
            ) : clients.length === 0 ? (
              <tr><td colSpan={9} className="px-3 py-16 text-center text-[13px] text-stone-400">
                No clients found. {!summary?.total_clients && 'Import a SAP CSV to get started.'}
              </td></tr>
            ) : (
              clients.map((c) => {
                const churnInfo = CHURN_CONFIG[c.churn_risk] || CHURN_CONFIG.unknown
                const typeInfo = TYPE_CONFIG[c.customer_type] || TYPE_CONFIG.unknown
                return (
                  <tr key={c.id} className="hover:bg-navy-50/30 transition-colors duration-100">
                    <td className="px-3 py-2.5">
                      <div className="flex items-center gap-2">
                        <span className="text-[11px]" title={typeInfo.label}>{typeInfo.icon}</span>
                        <div className="min-w-0">
                          <p className="text-[13px] font-semibold text-navy-900 truncate">{c.customer_name}</p>
                          <p className="text-[10px] text-stone-400 truncate">{c.customer_code} · {c.contact_person || 'No contact'}</p>
                        </div>
                      </div>
                    </td>
                    <td className="px-3 py-2.5">
                      <span className="text-[12px] text-stone-600 truncate block max-w-[140px]">{c.customer_group || '—'}</span>
                    </td>
                    <td className="px-3 py-2.5">
                      <span className="text-[12px] font-medium text-stone-700">{c.state || '—'}</span>
                    </td>
                    <td className="px-3 py-2.5 text-right">
                      <span className="text-[13px] font-semibold text-navy-900 tabular-nums">{fmtRevenue(c.revenue_lifetime)}</span>
                    </td>
                    <td className="px-3 py-2.5 text-right">
                      <span className="text-[12px] text-stone-700 tabular-nums">{fmtRevenue(c.revenue_current_year)}</span>
                    </td>
                    <td className="px-3 py-2.5 text-right">
                      <span className="text-[12px] text-stone-500 tabular-nums">{fmtRevenue(c.revenue_last_year)}</span>
                    </td>
                    <td className="px-3 py-2.5 text-center">
                      <span className="text-[12px] text-stone-600 tabular-nums">{c.total_invoices}</span>
                    </td>
                    <td className="px-3 py-2.5">
                      <span className="text-[11px] text-stone-500">{c.days_since_last_order != null ? `${c.days_since_last_order}d ago` : '—'}</span>
                    </td>
                    <td className="px-3 py-2.5">
                      <span className={cn('inline-flex px-2 py-0.5 rounded-full text-[10px] font-semibold border', churnInfo.bg, churnInfo.color)}>
                        {churnInfo.label}
                      </span>
                    </td>
                  </tr>
                )
              })
            )}
          </tbody>
        </table>
      </div>

      {/* ═══ PAGINATION ═══ */}
      {totalPages > 1 && (
        <div className="flex-shrink-0 px-5 py-2.5 bg-white border-t border-stone-200 flex items-center justify-between">
          <p className="text-[11px] text-stone-500">
            Showing {(page - 1) * 50 + 1}–{Math.min(page * 50, total)} of {total}
          </p>
          <div className="flex items-center gap-1">
            <button onClick={() => setPage(Math.max(1, page - 1))} disabled={page === 1}
              className="p-1.5 rounded-md hover:bg-stone-100 disabled:opacity-30 disabled:cursor-not-allowed transition">
              <ChevronLeft className="w-4 h-4" />
            </button>
            {Array.from({ length: Math.min(5, totalPages) }, (_, i) => {
              const start = Math.max(1, Math.min(page - 2, totalPages - 4))
              const p = start + i
              if (p > totalPages) return null
              return (
                <button key={p} onClick={() => setPage(p)}
                  className={cn('w-8 h-8 rounded-md text-[12px] font-medium transition',
                    p === page ? 'bg-navy-800 text-white' : 'text-stone-600 hover:bg-stone-100')}>
                  {p}
                </button>
              )
            })}
            <button onClick={() => setPage(Math.min(totalPages, page + 1))} disabled={page === totalPages}
              className="p-1.5 rounded-md hover:bg-stone-100 disabled:opacity-30 disabled:cursor-not-allowed transition">
              <ChevronRight className="w-4 h-4" />
            </button>
          </div>
        </div>
      )}

      {/* ═══ TOAST ═══ */}
      <Toast toast={toast} onClose={() => setToast(null)} />
    </div>
  )
}
