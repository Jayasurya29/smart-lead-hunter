import { useMemo } from 'react'
import type { Lead, LeadTab } from '@/api/types'
import {
  cn, getScoreColor, getScoreRing, getTimelineLabel, getTimelineColor,
  getTierShort, getTierColor, formatLocation, formatOpening, relativeDate,
} from '@/lib/utils'
import { useApproveLead, useRejectLead, useRestoreLead } from '@/hooks/useLeads'
import {
  CheckCircle2, XCircle, Undo2,
  ChevronLeft, ChevronRight, ChevronUp, ChevronDown, ChevronsUpDown,
} from 'lucide-react'

interface Props {
  leads: Lead[]
  total: number
  page: number
  totalPages: number
  tab: LeadTab
  selectedId: number | null
  onSelect: (id: number) => void
  onPageChange: (page: number) => void
  onSort?: (sort: string) => void
  currentSort?: string
  isLoading?: boolean
}

/* ── Sort config ── */

interface ColDef {
  key: string
  label: string
  sortAsc?: string
  sortDesc?: string
  width?: string
}

const COLUMNS: ColDef[] = [
  { key: 'score',    label: 'Score',    sortAsc: 'score_low',     sortDesc: 'score_high',    width: 'w-16' },
  { key: 'hotel',    label: 'Hotel',    sortAsc: 'hotel_az',      sortDesc: 'hotel_za' },
  { key: 'tier',     label: 'Tier',     sortAsc: 'tier_asc',      sortDesc: 'tier_desc',     width: 'w-16' },
  { key: 'time',     label: 'Time',     sortAsc: 'time_asc',      sortDesc: 'time_desc',     width: 'w-16' },
  { key: 'location', label: 'Location', sortAsc: 'location_az',   sortDesc: 'location_za' },
  { key: 'opening',  label: 'Opening',  sortAsc: 'opening_soon',  sortDesc: 'opening_late',  width: 'w-28' },
  { key: 'added',    label: 'Added',    sortAsc: 'oldest',        sortDesc: 'newest',        width: 'w-20' },
]

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

/* ── Timeline ordering for sort ── */
const TIMELINE_ORDER: Record<string, number> = {
  'Late': 0, 'Urgent': 1, 'Hot': 2, 'Warm': 3, 'Cool': 4, 'TBD': 5,
}
const TIER_ORDER: Record<string, number> = {
  'tier1_ultra_luxury': 0, 'tier2_luxury': 1, 'tier3_upper_upscale': 2,
  'tier4_upscale': 3, 'tier4_low': 4, 'tier5_budget': 5,
}

/* ── Client-side sort (works immediately, no backend needed) ── */

function sortLeads(leads: Lead[], sort: string): Lead[] {
  const sorted = [...leads]

  sorted.sort((a, b) => {
    switch (sort) {
      case 'score_high':
        return (b.lead_score ?? 0) - (a.lead_score ?? 0)
      case 'score_low':
        return (a.lead_score ?? 0) - (b.lead_score ?? 0)

      case 'hotel_az':
        return (a.hotel_name || a.name || '').localeCompare(b.hotel_name || b.name || '')
      case 'hotel_za':
        return (b.hotel_name || b.name || '').localeCompare(a.hotel_name || a.name || '')

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

      case 'time_asc': {
        const ta = TIMELINE_ORDER[getTimelineLabel(a)] ?? 99
        const tb = TIMELINE_ORDER[getTimelineLabel(b)] ?? 99
        return ta - tb
      }
      case 'time_desc': {
        const ta = TIMELINE_ORDER[getTimelineLabel(a)] ?? 99
        const tb = TIMELINE_ORDER[getTimelineLabel(b)] ?? 99
        return tb - ta
      }

      case 'location_az':
        return formatLocation(a).localeCompare(formatLocation(b))
      case 'location_za':
        return formatLocation(b).localeCompare(formatLocation(a))

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

      case 'newest':
        return new Date(b.created_at).getTime() - new Date(a.created_at).getTime()
      case 'oldest':
        return new Date(a.created_at).getTime() - new Date(b.created_at).getTime()

      default:
        return 0
    }
  })

  return sorted
}


export default function LeadTable({
  leads, total, page, totalPages, tab,
  selectedId, onSelect, onPageChange, onSort, currentSort = 'newest', isLoading,
}: Props) {
  const approveMut = useApproveLead()
  const rejectMut  = useRejectLead()
  const restoreMut = useRestoreLead()

  const isNew      = tab === 'pipeline'
  const isApproved = tab === 'approved'
  const isRejected = tab === 'rejected'

  // Client-side sort
  const sortedLeads = useMemo(() => sortLeads(leads, currentSort), [leads, currentSort])

  if (isLoading) {
    return (
      <div className="space-y-px p-1">
        {Array.from({ length: 12 }).map((_, i) => (
          <div key={i} className="skeleton h-[48px] rounded" style={{ animationDelay: `${i * 0.03}s` }} />
        ))}
      </div>
    )
  }

  if (!sortedLeads.length) {
    return (
      <div className="flex flex-col items-center justify-center py-20 text-stone-400">
        <div className="text-4xl mb-3">
          {isNew ? '📭' : isApproved ? '✅' : isRejected ? '🚫' : '⏱️'}
        </div>
        <p className="text-sm font-medium">No leads in {tab}</p>
        <p className="text-xs mt-1">
          {isNew ? 'Run a scrape to find new leads' : `No ${tab} leads yet`}
        </p>
      </div>
    )
  }

  return (
    <div className="flex flex-col h-full">
      <div className="flex-1 overflow-y-auto">
        <table className="w-full">
          <thead className="sticky top-0 z-10">
            <tr className="bg-slate-50/90 backdrop-blur-sm border-b border-slate-100">
              {COLUMNS.map((col) => (
                <th
                  key={col.key}
                  onClick={() => col.sortAsc && onSort?.(getNextSort(col, currentSort))}
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
              <th className="px-3 py-2.5 w-24" />
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-100/80">
            {sortedLeads.map((lead) => {
              const timeline = getTimelineLabel(lead)
              return (
                <tr
                  key={lead.id}
                  onClick={() => onSelect(lead.id)}
                  className={cn(
                    'lead-row cursor-pointer',
                    selectedId === lead.id && 'active',
                  )}
                >
                  <td className="px-3 py-2.5">
                    <span className={cn(
                      'inline-flex items-center justify-center w-9 h-7 text-xs font-bold rounded',
                      getScoreColor(lead.lead_score),
                      getScoreRing(lead.lead_score),
                    )}>
                      {lead.lead_score ?? '—'}
                    </span>
                  </td>

                  <td className="px-3 py-2.5 max-w-[280px]">
                    <div className="truncate text-[15px] font-bold text-navy-950 leading-snug">
                      {lead.hotel_name || lead.name || '—'}
                    </div>
                    {(lead.brand || lead.brand_name) && (
                      <div className="truncate text-xs text-stone-400 leading-snug">{lead.brand || lead.brand_name}</div>
                    )}
                  </td>

                  <td className="px-3 py-2.5">
                    {lead.brand_tier ? (
                      <span className={cn('inline-flex px-2 py-0.5 rounded text-2xs font-bold', getTierColor(lead.brand_tier))}>
                        {getTierShort(lead.brand_tier)}
                      </span>
                    ) : (
                      <span className="text-xs text-stone-300">—</span>
                    )}
                  </td>

                  <td className="px-3 py-2.5">
                    <span className={cn('inline-flex px-2 py-0.5 rounded text-2xs font-bold', getTimelineColor(timeline))}>
                      {timeline}
                    </span>
                  </td>

                  <td className="px-3 py-2.5">
                    <span className="text-sm text-navy-800 font-medium truncate block max-w-[200px]">
                      {formatLocation(lead)}
                    </span>
                  </td>

                  <td className="px-3 py-2.5">
                    <span className="text-sm font-medium text-navy-800">{formatOpening(lead)}</span>
                  </td>

                  <td className="px-3 py-2.5">
                    <span className="text-xs text-slate-400 font-medium">{relativeDate(lead.created_at)}</span>
                  </td>

                  <td className="px-2 py-2.5">
                    <div className="row-actions flex items-center gap-0.5 justify-end">
                      {isNew && (
                        <>
                          <ActionBtn onClick={(e) => { e.stopPropagation(); if (window.confirm(`Approve "${lead.hotel_name || lead.name}" and push to Insightly CRM?`)) approveMut.mutate(lead.id) }} color="emerald" title="Approve" pending={approveMut.isPending}>
                            <CheckCircle2 className="w-4 h-4" />
                          </ActionBtn>
                          <ActionBtn onClick={(e) => { e.stopPropagation(); if (window.confirm(`Reject "${lead.hotel_name || lead.name}"?`)) rejectMut.mutate({ id: lead.id }) }} color="red" title="Reject" pending={rejectMut.isPending}>
                            <XCircle className="w-4 h-4" />
                          </ActionBtn>
                        </>
                      )}
                      {isApproved && (
                        <ActionBtn onClick={(e) => { e.stopPropagation(); if (window.confirm(`Move "${lead.hotel_name || lead.name}" back to pipeline? This will delete from Insightly.`)) restoreMut.mutate(lead.id) }} color="amber" title="Back to Pipeline" pending={restoreMut.isPending}>
                          <Undo2 className="w-4 h-4" />
                        </ActionBtn>
                      )}
                      {isRejected && (
                        <ActionBtn onClick={(e) => { e.stopPropagation(); if (window.confirm(`Restore "${lead.hotel_name || lead.name}" back to pipeline?`)) restoreMut.mutate(lead.id) }} color="blue" title="Restore" pending={restoreMut.isPending}>
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

      {/* ── Page Numbers ── */}
      {totalPages > 1 && (
        <div className="flex items-center justify-between px-4 py-2.5 border-t border-slate-100 bg-white/80 flex-shrink-0">
          <span className="text-xs text-stone-400">
            Page {page} of {totalPages} · {total} lead{total !== 1 ? 's' : ''}
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
              if (totalPages <= 7) {
                pageNum = i + 1
              } else if (page <= 4) {
                pageNum = i + 1
              } else if (page >= totalPages - 3) {
                pageNum = totalPages - 6 + i
              } else {
                pageNum = page - 3 + i
              }
              return (
                <button
                  key={pageNum}
                  onClick={() => onPageChange(pageNum)}
                  className={cn(
                    'w-8 h-8 rounded text-xs font-semibold transition',
                    page === pageNum
                      ? 'bg-navy-900 text-white'
                      : 'text-stone-500 hover:bg-stone-100',
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

function ActionBtn({ onClick, color, title, children, pending }: {
  onClick: (e: React.MouseEvent) => void
  color: string
  title: string
  children: React.ReactNode
  pending?: boolean
}) {
  const colors: Record<string, string> = {
    emerald: 'hover:bg-emerald-50 text-emerald-500 hover:text-emerald-700',
    red:     'hover:bg-red-50 text-red-400 hover:text-red-600',
    blue:    'hover:bg-blue-50 text-blue-500 hover:text-blue-700',
    amber:   'hover:bg-amber-50 text-amber-500 hover:text-amber-700',
    gray:    'hover:bg-stone-100 text-stone-400 hover:text-stone-600',
  }
  return (
    <button
      onClick={onClick}
      title={title}
      disabled={pending}
      className={cn(
        'p-1.5 rounded-md transition-all duration-100 disabled:opacity-40',
        colors[color] || colors.gray,
      )}
    >
      {children}
    </button>
  )
}
