import { useStats } from '@/hooks/useLeads'
import { cn } from '@/lib/utils'
import {
  Building2, Flame, Zap, ThermometerSun, Snowflake,
  Globe, CheckCircle2, CalendarPlus,
} from 'lucide-react'

interface Props {
  onFilter?: (action: { tab?: string; timeline?: string }) => void
  activeTab?: string
  activeTimeline?: string
}

const STATS_CONFIG = [
  { key: 'new_leads',       label: 'Pipeline',  icon: Building2,      bg: 'bg-navy-50',    text: 'text-navy-600',    accent: 'border-navy-100',    action: { tab: 'pipeline' } },
  { key: 'urgent_leads',    label: 'Urgent',     icon: Zap,            bg: 'bg-gold-50',    text: 'text-gold-600',    accent: 'border-gold-100',    action: { tab: 'pipeline', timeline: 'urgent' } },
  { key: 'hot_leads',       label: 'Hot',        icon: Flame,          bg: 'bg-coral-50',   text: 'text-coral-500',   accent: 'border-coral-100',   action: { tab: 'pipeline', timeline: 'hot' } },
  { key: 'warm_leads',      label: 'Warm',       icon: ThermometerSun, bg: 'bg-gold-50',    text: 'text-gold-500',    accent: 'border-gold-100',    action: { tab: 'pipeline', timeline: 'warm' } },
  { key: 'cool_leads',      label: 'Cool',       icon: Snowflake,      bg: 'bg-sky-50',     text: 'text-sky-600',     accent: 'border-sky-100',     action: { tab: 'pipeline', timeline: 'cool' } },
  { key: 'total_leads',     label: 'Total',      icon: Globe,          bg: 'bg-stone-100',  text: 'text-stone-500',   accent: 'border-stone-200',   action: { tab: 'pipeline' } },
  { key: 'approved_leads',  label: 'Approved',   icon: CheckCircle2,   bg: 'bg-emerald-50', text: 'text-emerald-600', accent: 'border-emerald-100', action: { tab: 'approved' } },
  { key: 'leads_this_week', label: 'This Week',  icon: CalendarPlus,   bg: 'bg-violet-50',  text: 'text-violet-600',  accent: 'border-violet-100',  action: { tab: 'pipeline' } },
] as const

export default function StatsCards({ onFilter, activeTab, activeTimeline }: Props) {
  const { data: stats, isLoading } = useStats()

  if (isLoading || !stats) {
    return (
      <div className="grid grid-cols-4 lg:grid-cols-8 gap-2.5">
        {Array.from({ length: 8 }).map((_, i) => (
          <div key={i} className="skeleton rounded-lg h-[58px]" style={{ animationDelay: `${i * 0.05}s` }} />
        ))}
      </div>
    )
  }

  return (
    <div className="grid grid-cols-4 lg:grid-cols-8 gap-2.5">
      {STATS_CONFIG.map((cfg, i) => {
        const Icon = cfg.icon
        const value = (stats as any)[cfg.key] ?? 0
        return (
          <div
            key={cfg.key}
            onClick={() => onFilter?.(cfg.action)}
            className={cn(
              'stat-card rounded-lg border px-3 py-2.5 flex items-center gap-2.5 cursor-pointer animate-slideUp select-none',
              cfg.accent,
              (activeTab === cfg.action.tab && activeTimeline === ((cfg.action as any).timeline || ''))
                ? `${cfg.bg} ring-2 ring-navy-400 shadow-md`
                : 'bg-white hover:shadow-sm',
            )}
            style={{ animationDelay: `${i * 0.04}s`, animationFillMode: 'both' }}
          >
            <div className={cn('w-8 h-8 rounded-lg flex items-center justify-center flex-shrink-0', cfg.bg)}>
              <Icon className={cn('w-4 h-4', cfg.text)} />
            </div>
            <div className="min-w-0">
              <div className="text-lg font-bold text-navy-900 leading-tight tabular-nums">{value}</div>
              <div className="text-2xs text-stone-400 font-semibold uppercase tracking-wider">{cfg.label}</div>
            </div>
          </div>
        )
      })}
    </div>
  )
}
