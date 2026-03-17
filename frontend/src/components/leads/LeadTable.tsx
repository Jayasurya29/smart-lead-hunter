import React from 'react'
import type { Lead } from '@/api/types'
import { cn, getScoreBg, getTierLabel, getTierColor, formatLocation, relativeDate, getTimelineLabel, getTimelineColor } from '@/lib/utils'
import { ChevronLeft, ChevronRight, Hotel, CheckCircle2, XCircle, Undo2, Trash2, Loader2 } from 'lucide-react'
import { useApproveLead, useRejectLead, useRestoreLead, useDeleteLead } from '@/hooks/useLeads'
import type { UseMutationResult } from '@tanstack/react-query'

interface Props {
  leads: Lead[]
  total: number
  page: number
  pages: number
  selectedId: number | null
  onSelect: (id: number) => void
  onPageChange: (page: number) => void
  isLoading: boolean
  currentTab: string
}

export default function LeadTable({ leads, total, page, pages, selectedId, onSelect, onPageChange, isLoading, currentTab }: Props) {
  const approveMut = useApproveLead()
  const rejectMut = useRejectLead()
  const restoreMut = useRestoreLead()
  const deleteMut = useDeleteLead()

  if (isLoading) {
    return (
      <div className="bg-white rounded-lg border border-stone-200 overflow-hidden shadow-sm">
        <div className="h-12 bg-stone-50 border-b border-stone-200" />
        {Array.from({ length: 12 }).map((_, i) => (
          <div key={i} className="h-[60px] border-b border-stone-100 px-5">
            <div className="skeleton h-4 rounded mt-5 w-full" style={{ animationDelay: `${i * 0.03}s` }} />
          </div>
        ))}
      </div>
    )
  }

  if (leads.length === 0) {
    return (
      <div className="bg-white rounded-lg border border-stone-200 shadow-sm">
        <div className="text-center py-24">
          <Hotel className="w-12 h-12 mx-auto mb-3 text-stone-300" />
          <p className="text-base font-semibold text-stone-500">No leads found</p>
          <p className="text-sm text-stone-400 mt-1">Try adjusting your filters or run a new scrape.</p>
        </div>
      </div>
    )
  }

  return (
    <div className="bg-white rounded-lg border border-stone-200 overflow-hidden shadow-sm">
      <div className="overflow-x-auto">
        <table className="w-full">
          <thead>
            <tr className="bg-stone-50/80 border-b border-stone-200">
              <th className="px-4 py-3 text-[11px] font-bold text-stone-400 uppercase tracking-[0.08em] text-center w-18">Score</th>
              <th className="px-4 py-3 text-[11px] font-bold text-stone-400 uppercase tracking-[0.08em] text-left">Hotel</th>
              <th className="px-4 py-3 text-[11px] font-bold text-stone-400 uppercase tracking-[0.08em] text-left w-28">Tier</th>
              <th className="px-4 py-3 text-[11px] font-bold text-stone-400 uppercase tracking-[0.08em] text-left w-44">Location</th>
              <th className="px-4 py-3 text-[11px] font-bold text-stone-400 uppercase tracking-[0.08em] text-left w-40">Opening</th>
              <th className="px-4 py-3 text-[11px] font-bold text-stone-400 uppercase tracking-[0.08em] text-left w-24">Added</th>
              <th className="px-4 py-3 w-28"></th>
            </tr>
          </thead>
          <tbody>
            {leads.map((lead) => (
              <LeadRow
                key={lead.id}
                lead={lead}
                isSelected={selectedId === lead.id}
                onSelect={() => onSelect(lead.id)}
                approveMut={approveMut}
                rejectMut={rejectMut}
                restoreMut={restoreMut}
                deleteMut={deleteMut}
              />
            ))}
          </tbody>
        </table>
      </div>

      {pages > 1 && (
        <div className="flex items-center justify-between px-5 py-3 bg-stone-50/50 border-t border-stone-200">
          <span className="text-xs font-semibold text-stone-400 uppercase tracking-wider">
            {total} leads · Page {page} of {pages}
          </span>
          <div className="flex items-center gap-1">
            <PgBtn onClick={() => onPageChange(page - 1)} disabled={page <= 1}><ChevronLeft className="w-4 h-4" /></PgBtn>
            {getPageNumbers(page, pages).map((pn, i) =>
              pn === '...' ? (
                <span key={`dot-${i}`} className="w-8 h-8 flex items-center justify-center text-xs text-stone-400">…</span>
              ) : (
                <button
                  key={pn}
                  onClick={() => onPageChange(pn as number)}
                  className={cn(
                    'w-8 h-8 flex items-center justify-center rounded-lg text-xs font-semibold transition',
                    page === pn ? 'bg-navy-900 text-white border border-navy-800 shadow-sm' : 'border border-stone-200 text-stone-600 hover:bg-white hover:text-navy-700'
                  )}
                >{pn}</button>
              )
            )}
            <PgBtn onClick={() => onPageChange(page + 1)} disabled={page >= pages}><ChevronRight className="w-4 h-4" /></PgBtn>
          </div>
        </div>
      )}
    </div>
  )
}

function PgBtn({ onClick, disabled, children }: { onClick: () => void; disabled: boolean; children: React.ReactNode }) {
  return (
    <button onClick={onClick} disabled={disabled} className="w-8 h-8 flex items-center justify-center rounded-lg border border-stone-200 text-stone-500 hover:bg-white hover:text-navy-700 disabled:opacity-25 disabled:cursor-not-allowed transition">
      {children}
    </button>
  )
}

function getPageNumbers(current: number, total: number): (number | string)[] {
  if (total <= 7) return Array.from({ length: total }, (_, i) => i + 1)
  if (current <= 3) return [1, 2, 3, 4, '...', total]
  if (current >= total - 2) return [1, '...', total - 3, total - 2, total - 1, total]
  return [1, '...', current - 1, current, current + 1, '...', total]
}

// ── Row ──

interface LeadRowProps {
  lead: Lead
  isSelected: boolean
  onSelect: () => void
  approveMut: ReturnType<typeof useApproveLead>
  rejectMut: ReturnType<typeof useRejectLead>
  restoreMut: ReturnType<typeof useRestoreLead>
  deleteMut: ReturnType<typeof useDeleteLead>
}

function LeadRow({ lead, isSelected, onSelect, approveMut, rejectMut, restoreMut, deleteMut }: LeadRowProps) {
  const timeline = getTimelineLabel(lead.opening_date)
  const isNew = lead.status === 'new'
  const isApproved = lead.status === 'approved'
  const isRejected = lead.status === 'rejected' || lead.status === 'deleted'

  // FIX L-06: Track if THIS lead has a pending mutation (prevents double-clicks + CRM double-push)
  const isApproving = approveMut.isPending && approveMut.variables === lead.id
  const isRejecting = rejectMut.isPending && (rejectMut.variables as any)?.id === lead.id
  const isRestoring = restoreMut.isPending && restoreMut.variables === lead.id
  const isDeleting = deleteMut.isPending && deleteMut.variables === lead.id
  const isBusy = isApproving || isRejecting || isRestoring || isDeleting

  return (
    <tr
      onClick={onSelect}
      className={cn('lead-row cursor-pointer border-b border-stone-100 last:border-b-0', isSelected && 'active')}
    >
      <td className="px-4 py-3.5 text-center">
        <div className={cn(
          'inline-flex items-center justify-center w-11 h-11 rounded-xl border-2 text-sm font-bold tabular-nums',
          getScoreBg(lead.lead_score),
          lead.lead_score && lead.lead_score >= 70 && 'score-hot',
          lead.lead_score && lead.lead_score >= 50 && lead.lead_score < 70 && 'score-warm',
        )}>
          {lead.lead_score ?? '—'}
        </div>
      </td>

      <td className="px-4 py-3.5">
        <div className="font-semibold text-navy-900 text-sm leading-tight truncate max-w-[320px]">
          {lead.hotel_name}
        </div>
        {lead.brand && (
          <div className="text-xs text-stone-400 mt-0.5 truncate font-medium">{lead.brand}</div>
        )}
      </td>

      <td className="px-4 py-3.5">
        {lead.brand_tier && lead.brand_tier !== 'tier5_skip' && (
          <span className={cn(
            'inline-block text-[10px] font-bold px-2 py-1 rounded-md tracking-wide',
            getTierColor(lead.brand_tier),
          )}>
            {getTierLabel(lead.brand_tier)}
          </span>
        )}
      </td>

      <td className="px-4 py-3.5">
        <span className="text-[13px] text-stone-600 truncate block max-w-[180px]">
          {formatLocation(lead.city, lead.state, lead.country)}
        </span>
      </td>

      <td className="px-4 py-3.5">
        <div className="flex items-center gap-2">
          <span className={cn(
            'text-[10px] font-bold px-2 py-1 rounded-md border uppercase tracking-wide',
            getTimelineColor(timeline),
          )}>
            {timeline}
          </span>
          {lead.opening_date && timeline !== 'TBD' && (
            <span className="text-xs text-stone-400 truncate max-w-[100px]">{lead.opening_date}</span>
          )}
        </div>
      </td>

      <td className="px-4 py-3.5">
        <span className="text-xs text-stone-400 font-medium">{relativeDate(lead.created_at)}</span>
      </td>

      <td className="px-4 py-3.5 text-right">
        <div className="row-actions flex items-center justify-end gap-0.5">
          {isNew && (
            <>
              <ActionBtn onClick={(e) => { e.stopPropagation(); approveMut.mutate(lead.id) }} color="emerald" title="Approve" disabled={isBusy} loading={isApproving}>
                {isApproving ? <Loader2 className="w-4 h-4 animate-spin" /> : <CheckCircle2 className="w-4 h-4" />}
              </ActionBtn>
              <ActionBtn onClick={(e) => { e.stopPropagation(); rejectMut.mutate({ id: lead.id }) }} color="red" title="Reject" disabled={isBusy} loading={isRejecting}>
                {isRejecting ? <Loader2 className="w-4 h-4 animate-spin" /> : <XCircle className="w-4 h-4" />}
              </ActionBtn>
              <ActionBtn onClick={(e) => { e.stopPropagation(); deleteMut.mutate(lead.id) }} color="gray" title="Delete" disabled={isBusy} loading={isDeleting}>
                {isDeleting ? <Loader2 className="w-4 h-4 animate-spin" /> : <Trash2 className="w-4 h-4" />}
              </ActionBtn>
            </>
          )}
          {isApproved && (
            <ActionBtn onClick={(e) => { e.stopPropagation(); rejectMut.mutate({ id: lead.id }) }} color="red" title="Reject" disabled={isBusy} loading={isRejecting}>
              {isRejecting ? <Loader2 className="w-4 h-4 animate-spin" /> : <XCircle className="w-4 h-4" />}
            </ActionBtn>
          )}
          {isRejected && (
            <ActionBtn onClick={(e) => { e.stopPropagation(); restoreMut.mutate(lead.id) }} color="blue" title="Restore" disabled={isBusy} loading={isRestoring}>
              {isRestoring ? <Loader2 className="w-4 h-4 animate-spin" /> : <Undo2 className="w-4 h-4" />}
            </ActionBtn>
          )}
        </div>
      </td>
    </tr>
  )
}

interface ActionBtnProps {
  onClick: (e: React.MouseEvent<HTMLButtonElement>) => void
  color: string
  title: string
  children: React.ReactNode
  disabled?: boolean
  loading?: boolean
}

function ActionBtn({ onClick, color, title, children, disabled, loading }: ActionBtnProps) {
  const colors: Record<string, string> = {
    emerald: 'hover:bg-emerald-50 text-emerald-500 hover:text-emerald-700',
    red: 'hover:bg-red-50 text-red-400 hover:text-red-600',
    blue: 'hover:bg-blue-50 text-blue-500 hover:text-blue-700',
    gray: 'hover:bg-stone-100 text-stone-400 hover:text-stone-600',
  }
  return (
    <button
      onClick={onClick}
      title={title}
      disabled={disabled}
      className={cn(
        'p-2 rounded-lg transition-all duration-100',
        disabled ? 'opacity-40 cursor-not-allowed' : colors[color] || colors.gray,
        loading && 'opacity-70',
      )}
    >
      {children}
    </button>
  )
}
