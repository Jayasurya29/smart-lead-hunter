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
  sort: 'score_desc',
}

interface Props {
  filters: Filters
  onChange: (filters: Filters) => void
}

function FilterSelect({ value, onChange, placeholder, options, isActive }: {
  value: string; onChange: (v: string) => void; placeholder: string
  options: { value: string; label: string }[]; isActive: boolean
}) {
  return (
    <select
      value={value}
      onChange={e => onChange(e.target.value)}
      className={cn(
        'filter-select h-7 px-2.5 pr-7 text-[11px] font-semibold rounded-md outline-none cursor-pointer transition-all duration-150 appearance-none',
        isActive
          ? 'bg-navy-900 text-white border border-navy-800'
          : 'bg-white text-stone-600 border border-stone-200 hover:border-stone-300',
      )}
    >
      <option value="">{placeholder}</option>
      {options.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
    </select>
  )
}

export default function FilterBar({ filters, onChange }: Props) {
  const activeCount = Object.entries(filters).filter(([k, v]) => v && k !== 'sort').length

  function set(key: keyof Filters, val: string) {
    onChange({ ...filters, [key]: val })
  }

  return (
    <div className="flex items-center gap-2 flex-wrap">
      <div className="flex items-center gap-1.5 text-stone-400 mr-1">
        <SlidersHorizontal className="w-3.5 h-3.5" />
        <span className="text-[10px] font-bold uppercase tracking-wider">Filters</span>
      </div>

      <FilterSelect
        value={filters.timeline} onChange={v => set('timeline', v)} placeholder="Timeline"
        isActive={!!filters.timeline}
        options={[
          { value: 'hot', label: 'Hot (6-12mo)' },
          { value: 'urgent', label: 'Urgent (3-6mo)' },
          { value: 'warm', label: 'Warm (12-18mo)' },
          { value: 'cool', label: 'Cool (18mo+)' },
          { value: 'late', label: 'Late (0-3mo)' },
          { value: 'tbd', label: 'TBD' },
        ]}
      />

      <FilterSelect
        value={filters.location} onChange={v => set('location', v)} placeholder="Location"
        isActive={!!filters.location}
        options={[
          { value: 'south_florida', label: 'South Florida' },
          { value: 'rest_florida', label: 'Rest of Florida' },
          { value: 'caribbean', label: 'Caribbean' },
          { value: 'california', label: 'California' },
          { value: 'new_york', label: 'New York' },
          { value: 'texas', label: 'Texas' },
          { value: 'southeast', label: 'Southeast' },
          { value: 'mountain', label: 'Mountain West' },
        ]}
      />

      <FilterSelect
        value={filters.tier} onChange={v => set('tier', v)} placeholder="Tier"
        isActive={!!filters.tier}
        options={[
          { value: 'tier1_ultra_luxury', label: 'Ultra Luxury' },
          { value: 'tier2_luxury', label: 'Luxury' },
          { value: 'tier3_upper_upscale', label: 'Upper Upscale' },
          { value: 'tier4_upscale', label: 'Upscale' },
        ]}
      />

      <FilterSelect
        value={filters.year} onChange={v => set('year', v)} placeholder="Year"
        isActive={!!filters.year}
        options={[
          { value: '2025', label: '2025' },
          { value: '2026', label: '2026' },
          { value: '2027', label: '2027' },
          { value: '2028', label: '2028+' },
        ]}
      />

      <FilterSelect
        value={filters.added} onChange={v => set('added', v)} placeholder="Added"
        isActive={!!filters.added}
        options={[
          { value: 'today', label: 'Today' },
          { value: 'this_week', label: 'This Week' },
          { value: 'last_7', label: 'Last 7 Days' },
          { value: 'last_30', label: 'Last 30 Days' },
        ]}
      />

      <div className="flex-1" />

      <FilterSelect
        value={filters.sort} onChange={v => set('sort', v)} placeholder="Sort"
        isActive={false}
        options={[
          { value: 'newest', label: 'Newest First' },
          { value: 'oldest', label: 'Oldest First' },
          { value: 'score_desc', label: 'Highest Score' },
          { value: 'score_asc', label: 'Lowest Score' },
          { value: 'opening', label: 'Opening Soon' },
          { value: 'name_asc', label: 'A → Z' },
        ]}
      />

      {activeCount > 0 && (
        <button
          onClick={() => onChange({ ...DEFAULT_FILTERS })}
          className="flex items-center gap-1 text-[10px] font-bold uppercase tracking-wider text-red-500 hover:text-red-600 transition ml-1"
        >
          <X className="w-3 h-3" />
          Clear ({activeCount})
        </button>
      )}
    </div>
  )
}
