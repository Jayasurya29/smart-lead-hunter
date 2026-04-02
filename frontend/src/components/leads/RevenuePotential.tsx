import { useLeadRevenue } from '../../hooks/useRevenue'
import {
  DollarSign,
  Building2,
  Users,
  TrendingUp,
  AlertCircle,
  Loader2,
  CalendarPlus,
  RefreshCw,
  MapPin,
  Flame,
  ChevronDown,
  ChevronUp,
} from 'lucide-react'
import { useState } from 'react'

interface Props {
  leadId: number
}

function fmt(n: number | null | undefined): string {
  if (n == null) return '—'
  if (n >= 1_000_000) return `$${(n / 1_000_000).toFixed(1)}M`
  if (n >= 1_000) return `$${(n / 1_000).toFixed(0)}K`
  return `$${n.toLocaleString()}`
}

function pct(n: number | null | undefined): string {
  if (n == null) return '—'
  return `${n.toFixed(0)}%`
}

export default function RevenuePotential({ leadId }: Props) {
  const { data, isLoading, error } = useLeadRevenue(leadId)
  const [showBreakdown, setShowBreakdown] = useState(false)

  if (isLoading) {
    return (
      <div className="bg-white border border-stone-200 rounded-lg p-4">
        <div className="flex items-center gap-2 text-stone-400 text-sm">
          <Loader2 className="w-4 h-4 animate-spin" />
          Calculating revenue potential...
        </div>
      </div>
    )
  }

  if (error) {
    return (
      <div className="bg-white border border-stone-200 rounded-lg p-4">
        <div className="flex items-center gap-2 text-stone-400 text-xs">
          <AlertCircle className="w-3.5 h-3.5" />
          Could not calculate revenue potential
        </div>
      </div>
    )
  }

  if (!data) return null

  // Incomplete — missing fields
  if (data.status === 'incomplete') {
    return (
      <div className="bg-white border border-amber-200 rounded-lg p-4">
        <div className="flex items-center gap-2 mb-2">
          <DollarSign className="w-4 h-4 text-amber-500" />
          <span className="text-sm font-semibold text-navy-900">Revenue Potential</span>
          <span className="text-[10px] bg-amber-50 text-amber-600 px-1.5 py-0.5 rounded font-medium">
            Incomplete
          </span>
        </div>
        <p className="text-xs text-stone-500 mb-2">{data.message}</p>
        <div className="flex flex-wrap gap-1.5">
          {data.missing_fields?.map((field) => (
            <span
              key={field}
              className="text-[10px] bg-amber-50 text-amber-700 px-2 py-0.5 rounded-full border border-amber-200"
            >
              Missing: {field}
            </span>
          ))}
        </div>
      </div>
    )
  }

  // Success — show estimates
  const opening = data.new_opening
  const annual = data.annual_recurring

  if (!opening || !annual) return null

  return (
    <div className="bg-white border border-stone-200 rounded-lg">
      {/* Header */}
      <div className="flex items-center justify-between px-4 py-3 border-b border-stone-100">
        <div className="flex items-center gap-2">
          <div className="w-7 h-7 rounded-md bg-emerald-50 flex items-center justify-center">
            <DollarSign className="w-4 h-4 text-emerald-600" />
          </div>
          <div>
            <h3 className="text-sm font-semibold text-navy-900">Revenue Potential</h3>
            <p className="text-[10px] text-stone-400">
              {data.tier_label} • {data.property_type} • {data.location}
            </p>
          </div>
        </div>
        <span className="text-[10px] bg-stone-50 text-stone-500 px-2 py-0.5 rounded-full">
          {data.rooms} rooms
        </span>
      </div>

      {/* Two-column estimates */}
      <div className="grid grid-cols-2 divide-x divide-stone-100">
        {/* New Opening */}
        <div className="p-4">
          <div className="flex items-center gap-1.5 mb-3">
            <CalendarPlus className="w-3.5 h-3.5 text-blue-500" />
            <span className="text-[11px] font-semibold text-stone-600 uppercase tracking-wider">
              Opening Order
            </span>
          </div>
          <div className="text-2xl font-bold text-navy-900 mb-1">
            {fmt(opening.ja_addressable)}
          </div>
          <p className="text-[10px] text-stone-400">
            Year 1 initial provisioning
          </p>
          <div className="mt-3 space-y-1.5">
            <MiniStat
              icon={<Users className="w-3 h-3" />}
              label="Uniformed staff"
              value={`${opening.uniformed_staff}`}
            />
            <MiniStat
              icon={<DollarSign className="w-3 h-3" />}
              label="Kit cost / employee"
              value={`$${opening.cost_per_employee.toLocaleString()}`}
            />
          </div>
        </div>

        {/* Annual Recurring */}
        <div className="p-4">
          <div className="flex items-center gap-1.5 mb-3">
            <RefreshCw className="w-3.5 h-3.5 text-emerald-500" />
            <span className="text-[11px] font-semibold text-stone-600 uppercase tracking-wider">
              Annual Recurring
            </span>
          </div>
          <div className="text-2xl font-bold text-navy-900 mb-1">
            {fmt(annual.ja_addressable)}
          </div>
          <p className="text-[10px] text-stone-400">
            Yearly garment spend (Year 2+)
          </p>
          <div className="mt-3 space-y-1.5">
            <MiniStat
              icon={<Users className="w-3 h-3" />}
              label="Staff (incl. seasonal)"
              value={`${annual.total_staff}`}
            />
            <MiniStat
              icon={<DollarSign className="w-3 h-3" />}
              label="Annual / employee"
              value={`$${annual.cost_per_employee.toLocaleString()}`}
            />
          </div>
        </div>
      </div>

      {/* Factor badges */}
      <div className="px-4 py-2.5 bg-stone-50/50 border-t border-stone-100 flex flex-wrap gap-1.5">
        <FactorBadge
          icon={<MapPin className="w-2.5 h-2.5" />}
          label={`Climate ${annual.climate_factor}×`}
          color="blue"
        />
        <FactorBadge
          icon={<Building2 className="w-2.5 h-2.5" />}
          label={`${pct(annual.uniformed_pct * 100)} uniformed`}
          color="stone"
        />
        <FactorBadge
          icon={<Users className="w-2.5 h-2.5" />}
          label={`${annual.staff_per_room} staff/room`}
          color="stone"
        />
        {annual.seasonal_staff > 0 && (
          <FactorBadge
            icon={<Flame className="w-2.5 h-2.5" />}
            label={`+${annual.seasonal_staff} seasonal (${annual.peak_months}mo peak)`}
            color="amber"
          />
        )}
        {annual.fb_multiplier > 1.0 && (
          <FactorBadge
            icon={<TrendingUp className="w-2.5 h-2.5" />}
            label={`F&B ${annual.fb_multiplier.toFixed(2)}×`}
            color="emerald"
          />
        )}
      </div>

      {/* Expandable breakdown */}
      <button
        onClick={() => setShowBreakdown(!showBreakdown)}
        className="w-full flex items-center justify-center gap-1 px-4 py-2 text-[10px] text-stone-400 hover:text-stone-600 hover:bg-stone-50 transition border-t border-stone-100"
      >
        {showBreakdown ? <ChevronUp className="w-3 h-3" /> : <ChevronDown className="w-3 h-3" />}
        {showBreakdown ? 'Hide' : 'Show'} calculation breakdown
      </button>

      {showBreakdown && (
        <div className="px-4 pb-4 border-t border-stone-100 pt-3">
          <div className="text-[10px] text-stone-500 space-y-1 font-mono">
            <p className="font-semibold text-stone-700 mb-2 font-sans text-[11px]">Opening Formula:</p>
            <p>{opening.rooms} rooms × {opening.staff_per_room} staff/room = {opening.base_staff} employees</p>
            <p>{opening.base_staff} × {pct(opening.uniformed_pct * 100)} uniformed = {opening.uniformed_staff} uniformed</p>
            <p>{opening.uniformed_staff} × ${opening.cost_per_employee.toLocaleString()} kit cost = ${(opening.uniformed_staff * opening.cost_per_employee).toLocaleString()}</p>
            <p>× {opening.climate_factor} climate × {opening.fb_multiplier.toFixed(2)} F&B = ${opening.total_budget.toLocaleString()} total</p>
            <p>× 90% garment portion = <span className="font-bold text-navy-900">${opening.ja_addressable.toLocaleString()}</span> JA addressable</p>

            <p className="font-semibold text-stone-700 mt-3 mb-2 font-sans text-[11px]">Annual Recurring Formula:</p>
            <p>{annual.rooms} rooms × {annual.staff_per_room} staff/room = {annual.base_staff} base staff</p>
            <p>+ {annual.seasonal_staff} seasonal ({annual.peak_months}mo peak × {pct(annual.seasonal_surge_pct * 100)} surge) = {annual.total_staff} total</p>
            <p>{annual.total_staff} × {pct(annual.uniformed_pct * 100)} uniformed = {annual.uniformed_staff} uniformed</p>
            <p>{annual.uniformed_staff} × ${annual.cost_per_employee.toLocaleString()}/yr × {annual.climate_factor} climate × {annual.fb_multiplier.toFixed(2)} F&B</p>
            <p>= ${annual.total_budget.toLocaleString()} total budget</p>
            <p>× {pct(annual.garment_pct * 100)} garment portion = <span className="font-bold text-navy-900">${annual.ja_addressable.toLocaleString()}</span> JA addressable</p>
          </div>
        </div>
      )}
    </div>
  )
}

// ── Sub-components ──

function MiniStat({
  icon,
  label,
  value,
}: {
  icon: React.ReactNode
  label: string
  value: string
}) {
  return (
    <div className="flex items-center justify-between">
      <div className="flex items-center gap-1.5 text-stone-400">
        {icon}
        <span className="text-[10px]">{label}</span>
      </div>
      <span className="text-[11px] font-semibold text-stone-700">{value}</span>
    </div>
  )
}

function FactorBadge({
  icon,
  label,
  color,
}: {
  icon: React.ReactNode
  label: string
  color: 'blue' | 'amber' | 'emerald' | 'stone'
}) {
  const colors = {
    blue: 'bg-blue-50 text-blue-600 border-blue-100',
    amber: 'bg-amber-50 text-amber-600 border-amber-100',
    emerald: 'bg-emerald-50 text-emerald-600 border-emerald-100',
    stone: 'bg-stone-50 text-stone-500 border-stone-200',
  }

  return (
    <span
      className={`inline-flex items-center gap-1 text-[9px] font-medium px-1.5 py-0.5 rounded-full border ${colors[color]}`}
    >
      {icon}
      {label}
    </span>
  )
}
