import { cn } from '@/lib/utils'
import { SlidersHorizontal, X } from 'lucide-react'

export interface Filters {
  timeline: string
  location: string
  tier: string
  year: string
  added: string
  sort: string
}

export const DEFAULT_FILTERS: Filters = {
  timeline: '',
  location: '',
  tier: '',
  year: '',
  added: '',
  sort: 'newest',
}

interface Props {
  filters: Filters
  onChange: (filters: Filters) => void
}

function FilterSelect({ value, onChange, placeholder, children, isActive }: {
  value: string
  onChange: (v: string) => void
  placeholder: string
  children: React.ReactNode
  isActive: boolean
}) {
  return (
    <select
      value={value}
      onChange={(e) => onChange(e.target.value)}
      className={cn(
        'filter-select',
        isActive
          ? 'bg-navy-900 text-white border border-navy-800 [&>option]:text-navy-900 [&>option]:bg-white [&>optgroup]:text-stone-500 [&>optgroup]:bg-stone-50'
          : 'bg-white text-stone-600 border border-stone-200 hover:border-stone-300',
      )}
    >
      <option value="">{placeholder}</option>
      {children}
    </select>
  )
}

function SimpleFilterSelect({ value, onChange, placeholder, options, isActive }: {
  value: string
  onChange: (v: string) => void
  placeholder: string
  options: { value: string; label: string }[]
  isActive: boolean
}) {
  return (
    <FilterSelect value={value} onChange={onChange} placeholder={placeholder} isActive={isActive}>
      {options.map((o) => (
        <option key={o.value} value={o.value}>{o.label}</option>
      ))}
    </FilterSelect>
  )
}

export default function FilterBar({ filters, onChange }: Props) {
  const activeCount = Object.entries(filters).filter(([k, v]) => v && k !== 'sort').length

  function set(key: keyof Filters, val: string) {
    onChange({ ...filters, [key]: val })
  }

  function clearAll() {
    onChange({ ...DEFAULT_FILTERS })
  }

  return (
    <div className="flex items-center gap-2.5 flex-wrap">
      <div className="flex items-center gap-1.5 text-stone-400 mr-0.5">
        <SlidersHorizontal className="w-4 h-4" />
        <span className="text-2xs font-bold uppercase tracking-wider">Filters</span>
      </div>

      <SimpleFilterSelect
        value={filters.timeline} onChange={(v) => set('timeline', v)} placeholder="Timeline"
        isActive={!!filters.timeline}
        options={[
          { value: 'urgent', label: 'Urgent (3-6mo)' },
          { value: 'hot',    label: 'Hot (6-12mo)' },
          { value: 'warm',   label: 'Warm (12-18mo)' },
          { value: 'cool',   label: 'Cool (18mo+)' },
          { value: 'tbd',    label: 'TBD' },
        ]}
      />

      {/* Location — grouped by region */}
      <FilterSelect
        value={filters.location} onChange={(v) => set('location', v)} placeholder="Location"
        isActive={!!filters.location}
      >
        <optgroup label="Florida & Caribbean">
          <option value="south_florida">South Florida</option>
          <option value="rest_florida">Rest of Florida</option>
          <option value="caribbean">Caribbean</option>
        </optgroup>
        <optgroup label="East Coast">
          <option value="new_york">New York</option>
          <option value="northeast">Northeast</option>
          <option value="dc">Washington DC</option>
          <option value="southeast">Southeast</option>
        </optgroup>
        <optgroup label="Central & West">
          <option value="texas">Texas</option>
          <option value="midwest">Midwest</option>
          <option value="california">California</option>
          <option value="mountain">Mountain West</option>
          <option value="pacific_nw">Pacific Northwest</option>
        </optgroup>
        <optgroup label="Key Markets">
          <option value="las_vegas">Las Vegas</option>
          <option value="new_orleans">New Orleans</option>
          <option value="hawaii">Hawaii</option>
        </optgroup>
      </FilterSelect>

      <SimpleFilterSelect
        value={filters.tier} onChange={(v) => set('tier', v)} placeholder="Tier"
        isActive={!!filters.tier}
        options={[
          { value: 'tier1_ultra_luxury',  label: 'Ultra Luxury' },
          { value: 'tier2_luxury',        label: 'Luxury' },
          { value: 'tier3_upper_upscale', label: 'Upper Upscale' },
          { value: 'tier4_upscale',       label: 'Upscale' },
        ]}
      />

      <SimpleFilterSelect
        value={filters.year} onChange={(v) => set('year', v)} placeholder="Year"
        isActive={!!filters.year}
        options={[
          { value: '2026', label: '2026' },
          { value: '2027', label: '2027' },
          { value: '2028', label: '2028+' },
        ]}
      />

      <SimpleFilterSelect
        value={filters.added} onChange={(v) => set('added', v)} placeholder="Added"
        isActive={!!filters.added}
        options={[
          { value: 'today',     label: 'Today' },
          { value: 'this_week', label: 'This Week' },
          { value: 'last_7',    label: 'Last 7 Days' },
          { value: 'last_30',   label: 'Last 30 Days' },
        ]}
      />

      {activeCount > 0 && (
        <button
          onClick={clearAll}
          className="flex items-center gap-1.5 px-2.5 py-1.5 text-xs font-semibold text-white bg-coral-500 hover:bg-coral-600 rounded-lg transition shadow-sm"
        >
          <X className="w-3 h-3" />
          Clear {activeCount}
        </button>
      )}
    </div>
  )
}
