import React from 'react'
import { useStats } from '@/hooks/useLeads'
import { Flame, Zap, ThermometerSun, Snowflake, Building2, Globe, CalendarPlus } from 'lucide-react'
import { cn } from '@/lib/utils'
import type { DashboardStats } from '@/api/types'

interface StatConfig {
  key: keyof DashboardStats
  label: string
  filterKey: string
  icon: React.ElementType
  bg: string
  text: string
  accent: string
  activeBg: string
  activeRing: string
}

const STATS_CONFIG: StatConfig[] = [
  { key: 'urgent_leads', label: 'Urgent', filterKey: 'urgent', icon: Zap, bg: 'bg-orange-50', text: 'text-orange-600', accent: 'border-orange-200', activeBg: 'bg-orange-100', activeRing: 'ring-orange-400' },
  { key: 'hot_leads', label: 'Hot', filterKey: 'hot', icon: Flame, bg: 'bg-coral-50', text: 'text-coral-600', accent: 'border-coral-200', activeBg: 'bg-coral-100', activeRing: 'ring-coral-400' },
  { key: 'warm_leads', label: 'Warm', filterKey: 'warm', icon: ThermometerSun, bg: 'bg-amber-50', text: 'text-amber-600', accent: 'border-amber-200', activeBg: 'bg-amber-100', activeRing: 'ring-amber-400' },
  { key: 'cool_leads', label: 'Cool', filterKey: 'cool', icon: Snowflake, bg: 'bg-cyan-50', text: 'text-cyan-600', accent: 'border-cyan-200', activeBg: 'bg-cyan-100', activeRing: 'ring-cyan-400' },
  { key: 'new_leads', label: 'Pipeline', filterKey: '', icon: Building2, bg: 'bg-navy-50', text: 'text-navy-600', accent: 'border-navy-200', activeBg: 'bg-navy-100', activeRing: 'ring-navy-400' },
  { key: 'total_leads', label: 'Total', filterKey: '', icon: Globe, bg: 'bg-stone-100', text: 'text-stone-600', accent: 'border-stone-200', activeBg: 'bg-stone-200', activeRing: 'ring-stone-400' },
  { key: 'approved_leads', label: 'Approved', filterKey: '', icon: Globe, bg: 'bg-emerald-50', text: 'text-emerald-600', accent: 'border-emerald-200', activeBg: 'bg-emerald-100', activeRing: 'ring-emerald-400' },
  { key: 'leads_this_week', label: 'This Week', filterKey: '', icon: CalendarPlus, bg: 'bg-violet-50', text: 'text-violet-600', accent: 'border-violet-200', activeBg: 'bg-violet-100', activeRing: 'ring-violet-400' },
]

interface Props {
  activeTimeline?: string
  onTimelineClick?: (timeline: string) => void
}

export default function StatsCards({ activeTimeline, onTimelineClick }: Props) {
  const { data: stats, isLoading } = useStats()

  if (isLoading || !stats) {
    return (
      <div className="grid grid-cols-4 lg:grid-cols-8 gap-2.5">
        {Array.from({ length: 8 }).map((_, i) => (
          <div key={i} className="skeleton rounded-lg h-[72px]" style={{ animationDelay: `${i * 0.05}s` }} />
        ))}
      </div>
    )
  }

  return (
    <div className="grid grid-cols-4 lg:grid-cols-8 gap-2.5">
      {STATS_CONFIG.map((cfg, i) => {
        const Icon = cfg.icon
        const value: number = stats[cfg.key] || 0
        const isClickable = !!cfg.filterKey
        const isActive = activeTimeline === cfg.filterKey && cfg.filterKey !== ''

        return (
          <button
            key={cfg.key}
            onClick={() => {
              if (isClickable && onTimelineClick) {
                onTimelineClick(isActive ? '' : cfg.filterKey)
              }
            }}
            disabled={!isClickable}
            className={cn(
              'stat-card rounded-lg border text-left px-4 py-3 flex items-center gap-3 animate-slideUp transition-all duration-150',
              isActive
                ? cn('ring-2 shadow-md', cfg.activeBg, cfg.activeRing, 'border-transparent')
                : cn('bg-white', cfg.accent),
              isClickable
                ? 'cursor-pointer hover:shadow-md hover:-translate-y-0.5 active:scale-[0.98]'
                : 'cursor-default opacity-90',
            )}
            style={{ animationDelay: `${i * 0.04}s`, animationFillMode: 'both' }}
          >
            <div className={cn('w-10 h-10 rounded-lg flex items-center justify-center flex-shrink-0', cfg.bg)}>
              <Icon className={cn('w-5 h-5', cfg.text)} />
            </div>
            <div className="min-w-0">
              <div className="text-2xl font-bold text-navy-900 leading-tight tabular-nums">{value}</div>
              <div className="text-[10px] text-stone-400 font-semibold uppercase tracking-wider">{cfg.label}</div>
            </div>
          </button>
        )
      })}
    </div>
  )
}
