import { useState } from 'react'
import { useLeads, type LeadFilterState } from '@/hooks/useLeads'
import type { LeadTab } from '@/api/types'
import StatsCards from '@/components/stats/StatsCards'
import LeadTable from '@/components/leads/LeadTable'
import LeadDetail from '@/components/leads/LeadDetail'
import FilterBar, { DEFAULT_FILTERS } from '@/components/leads/FilterBar'
import { cn } from '@/lib/utils'
import { Inbox, CheckCircle2, XCircle, Trash2, Search, X } from 'lucide-react'

const TABS: { key: LeadTab; label: string; icon: React.ElementType }[] = [
  { key: 'pipeline', label: 'Pipeline', icon: Inbox },
  { key: 'approved', label: 'Approved', icon: CheckCircle2 },
  { key: 'rejected', label: 'Rejected', icon: XCircle },
  { key: 'deleted', label: 'Deleted', icon: Trash2 },
]

export default function Dashboard() {
  const [tab, setTab] = useState<LeadTab>('pipeline')
  const [page, setPage] = useState(1)
  const [search, setSearch] = useState('')
  const [selectedLeadId, setSelectedLeadId] = useState<number | null>(null)
  const [filters, setFilters] = useState<LeadFilterState>(DEFAULT_FILTERS)

  // Pass filters to API
  const { data, isLoading } = useLeads(tab, page, search, filters)

  function handleTabChange(newTab: LeadTab) {
    setTab(newTab)
    setPage(1)
    setSelectedLeadId(null)
  }

  function handleSearch(val: string) {
    setSearch(val)
    setPage(1)
  }

  function handleFilterChange(newFilters: LeadFilterState) {
    setFilters(newFilters)
    setPage(1)
  }

  // When a stat card is clicked (e.g. "Hot"), set the timeline filter
  function handleTimelineClick(timeline: string) {
    setFilters(prev => ({ ...prev, timeline }))
    setPage(1)
    // Also switch to pipeline tab since timeline only applies to new leads
    if (tab !== 'pipeline') setTab('pipeline')
  }

  return (
    <div className="h-full flex flex-col">
      {/* Stats — clickable to filter */}
      <div className="px-4 pt-3 pb-2 flex-shrink-0">
        <StatsCards
          activeTimeline={filters.timeline}
          onTimelineClick={handleTimelineClick}
        />
      </div>

      {/* Toolbar */}
      <div className="px-4 pb-2 flex-shrink-0 space-y-2">
        {/* Tabs + Search */}
        <div className="flex items-center justify-between gap-4">
          <div className="flex gap-px bg-stone-200/60 p-0.5 rounded-lg">
            {TABS.map((t) => {
              const Icon = t.icon
              const count = data && t.key === tab ? data.total : undefined
              return (
                <button
                  key={t.key}
                  onClick={() => handleTabChange(t.key)}
                  className={cn(
                    'flex items-center gap-1.5 px-3 py-1.5 text-[11px] font-semibold rounded-md transition-all duration-150',
                    tab === t.key
                      ? 'bg-white text-navy-900 shadow-sm'
                      : 'text-stone-500 hover:text-stone-700',
                  )}
                >
                  <Icon className="w-3.5 h-3.5" />
                  {t.label}
                  {count !== undefined && (
                    <span className={cn('text-[10px] ml-0.5 tabular-nums', tab === t.key ? 'text-navy-500' : 'text-stone-400')}>
                      ({count})
                    </span>
                  )}
                </button>
              )
            })}
          </div>

          {/* Search */}
          <div className="relative w-72">
            <Search className="w-4 h-4 text-stone-400 absolute left-3 top-1/2 -translate-y-1/2" />
            <input
              type="text"
              placeholder="Search hotels, brands, cities..."
              value={search}
              onChange={(e) => handleSearch(e.target.value)}
              className="w-full pl-10 pr-9 py-2 text-sm border-2 border-stone-200 rounded-lg focus:border-navy-400 focus:ring-0 outline-none transition-colors bg-white"
            />
            {search && (
              <button onClick={() => handleSearch('')} className="absolute right-3 top-1/2 -translate-y-1/2 text-stone-400 hover:text-stone-600 transition">
                <X className="w-4 h-4" />
              </button>
            )}
          </div>
        </div>

        {/* Filters */}
        <FilterBar filters={filters} onChange={handleFilterChange} />
      </div>

      {/* Main: Table + Detail */}
      <div className="flex-1 overflow-hidden flex gap-3 px-4 pb-3">
        <div className={cn(
          'overflow-auto transition-all duration-250',
          selectedLeadId ? 'w-[58%]' : 'w-full',
        )}>
          <LeadTable
            leads={data?.leads || []}
            total={data?.total || 0}
            page={page}
            pages={data?.pages || 1}
            selectedId={selectedLeadId}
            onSelect={setSelectedLeadId}
            onPageChange={setPage}
            isLoading={isLoading}
            currentTab={tab}
          />
        </div>

        {selectedLeadId && (
          <div className="w-[42%] overflow-hidden">
            <LeadDetail
              leadId={selectedLeadId}
              onClose={() => setSelectedLeadId(null)}
            />
          </div>
        )}
      </div>
    </div>
  )
}
