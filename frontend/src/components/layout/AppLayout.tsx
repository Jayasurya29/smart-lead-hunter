import React, { ReactNode, useState, useEffect } from 'react'
import { Link, useLocation } from 'react-router-dom'
import { useAuth } from '@/hooks/useAuth'
import { BackgroundTaskProvider, useBackgroundTask } from '@/hooks/useBackgroundTask'
import {
  LayoutDashboard, Map, Mail, LogOut, Play, Link2, Radar,
  Loader2, CheckCircle2, AlertCircle, X, ChevronDown, ChevronUp,
} from 'lucide-react'
import { cn } from '@/lib/utils'
import ScrapeModal from '@/components/modals/ScrapeModal'
import ExtractModal from '@/components/modals/ExtractModal'
import DiscoveryModal from '@/components/modals/DiscoveryModal'

interface NavItem {
  to: string
  label: string
  icon: React.ElementType
  disabled?: boolean
}

const NAV_ITEMS: NavItem[] = [
  { to: '/dashboard', label: 'Dashboard', icon: LayoutDashboard },
  { to: '/map', label: 'Map', icon: Map, disabled: true },
  { to: '/outreach', label: 'Outreach', icon: Mail, disabled: true },
]

export default function AppLayout({ children }: { children: ReactNode }) {
  return (
    <BackgroundTaskProvider>
      <AppLayoutInner>{children}</AppLayoutInner>
    </BackgroundTaskProvider>
  )
}

function AppLayoutInner({ children }: { children: ReactNode }) {
  const { logout } = useAuth()
  const location = useLocation()
  const [showScrape, setShowScrape] = useState(false)
  const [showExtract, setShowExtract] = useState(false)
  const [showDiscovery, setShowDiscovery] = useState(false)

  const bg = useBackgroundTask()

  return (
    <div className="h-full flex flex-col">
      <header className="bg-white border-b border-stone-200 shadow-sm h-16 flex-shrink-0 relative z-30">
        <div className="h-full px-6 flex items-center justify-between">
          <div className="flex items-center gap-6">
            <Link to="/dashboard" className="flex items-center gap-3 group h-full py-1.5">
              <img
                src="/static/img/logo.svg"
                alt="Smart Lead Hunter"
                className="h-full w-auto object-contain"
                style={{ minWidth: '80px' }}
              />
              <span className="text-base font-bold text-navy-900 tracking-tight">
                Smart Lead Hunter
              </span>
            </Link>

            <div className="h-6 w-px bg-stone-200" />

            <nav className="flex items-center gap-1">
              {NAV_ITEMS.map((item) => {
                const Icon = item.icon
                const isActive = location.pathname.startsWith(item.to)
                return (
                  <Link
                    key={item.to}
                    to={item.disabled ? '#' : item.to}
                    className={cn(
                      'flex items-center gap-2 px-3.5 py-2 rounded-lg text-sm font-medium transition-all duration-150',
                      isActive
                        ? 'bg-navy-50 text-navy-800'
                        : item.disabled
                          ? 'text-stone-300 cursor-not-allowed'
                          : 'text-stone-500 hover:bg-stone-50 hover:text-navy-800',
                    )}
                    onClick={item.disabled ? (e: React.MouseEvent) => e.preventDefault() : undefined}
                  >
                    <Icon className="w-4 h-4" />
                    {item.label}
                    {item.disabled && (
                      <span className="text-[9px] bg-stone-100 text-stone-400 px-1.5 py-0.5 rounded-full font-semibold uppercase tracking-wider">Soon</span>
                    )}
                  </Link>
                )
              })}
            </nav>
          </div>

          <div className="flex items-center gap-2.5">
            {/* ── Background task status pill ── */}
            {bg.isRunning && <TaskPill />}

            <button
              onClick={() => setShowScrape(true)}
              disabled={bg.isRunning}
              className={cn(
                'flex items-center gap-2 px-4 py-2 text-xs font-semibold rounded-lg transition-all shadow-sm active:scale-[0.97]',
                bg.isRunning
                  ? 'bg-stone-200 text-stone-400 cursor-not-allowed shadow-none'
                  : 'bg-emerald-600 text-white hover:bg-emerald-700',
              )}
            >
              <Play className="w-3.5 h-3.5" /> Run Scrape
            </button>
            <button
              onClick={() => setShowExtract(true)}
              disabled={bg.isRunning}
              className={cn(
                'flex items-center gap-2 px-4 py-2 text-xs font-semibold rounded-lg transition-all shadow-sm active:scale-[0.97]',
                bg.isRunning
                  ? 'bg-stone-200 text-stone-400 cursor-not-allowed shadow-none'
                  : 'bg-blue-600 text-white hover:bg-blue-700',
              )}
            >
              <Link2 className="w-3.5 h-3.5" /> Extract URL
            </button>
            <button
              onClick={() => setShowDiscovery(true)}
              disabled={bg.isRunning}
              className={cn(
                'flex items-center gap-2 px-4 py-2 text-xs font-semibold rounded-lg transition-all shadow-sm active:scale-[0.97]',
                bg.isRunning
                  ? 'bg-stone-200 text-stone-400 cursor-not-allowed shadow-none'
                  : 'bg-violet-600 text-white hover:bg-violet-700',
              )}
            >
              <Radar className="w-3.5 h-3.5" /> Discovery
            </button>
            <div className="h-6 w-px bg-stone-200 mx-1" />
            <button onClick={logout} className="flex items-center gap-2 px-3 py-2 text-xs font-medium text-stone-500 hover:text-navy-800 hover:bg-stone-50 rounded-lg transition-all">
              <LogOut className="w-4 h-4" /> <span className="hidden sm:inline">Logout</span>
            </button>
          </div>
        </div>
      </header>

      <main className="flex-1 overflow-hidden bg-stone-50">{children}</main>

      {/* ── Result toast ── */}
      {bg.summary && <ResultToast />}

      {/* ── Modals (config only — they close on start) ── */}
      {showScrape && <ScrapeModal onClose={() => setShowScrape(false)} />}
      {showExtract && <ExtractModal onClose={() => setShowExtract(false)} />}
      {showDiscovery && <DiscoveryModal onClose={() => setShowDiscovery(false)} />}
    </div>
  )
}

// ═══════════════════════════════════════════════════════════════
// STATUS PILL — pulsing indicator in header while task runs
// ═══════════════════════════════════════════════════════════════

function TaskPill() {
  const { taskType, eventCount } = useBackgroundTask()

  const labels: Record<string, string> = {
    scrape: 'Scraping',
    extract: 'Extracting',
    discovery: 'Discovering',
  }
  const colors: Record<string, string> = {
    scrape: 'bg-emerald-500',
    extract: 'bg-blue-500',
    discovery: 'bg-violet-500',
  }

  return (
    <div className="flex items-center gap-2 px-3 py-1.5 rounded-full bg-stone-900 text-white text-[11px] font-semibold animate-fadeIn">
      <span className={cn('w-2 h-2 rounded-full animate-pulse', colors[taskType || 'scrape'])} />
      {labels[taskType || 'scrape']}...
      <span className="text-stone-400 tabular-nums">{eventCount}</span>
    </div>
  )
}

// ═══════════════════════════════════════════════════════════════
// RESULT TOAST — slides in when task completes
// ═══════════════════════════════════════════════════════════════

function ResultToast() {
  const { summary, status, logs, dismissToast } = useBackgroundTask()
  const [showLogs, setShowLogs] = useState(false)

  // Auto-dismiss after 15s (unless logs are open)
  useEffect(() => {
    if (showLogs) return
    const t = setTimeout(dismissToast, 15_000)
    return () => clearTimeout(t)
  }, [dismissToast, showLogs])

  if (!summary) return null

  const isError = status === 'error'
  const colors = {
    scrape: { border: 'border-emerald-200', bg: 'bg-emerald-50', icon: 'text-emerald-600' },
    extract: { border: 'border-blue-200', bg: 'bg-blue-50', icon: 'text-blue-600' },
    discovery: { border: 'border-violet-200', bg: 'bg-violet-50', icon: 'text-violet-600' },
  }
  const c = isError
    ? { border: 'border-red-200', bg: 'bg-red-50', icon: 'text-red-600' }
    : colors[summary.type] || colors.scrape

  return (
    <div className={cn(
      'fixed bottom-6 right-6 z-50 w-[380px] rounded-xl border shadow-lg overflow-hidden animate-slideUp',
      c.border, 'bg-white',
    )}>
      {/* Header */}
      <div className={cn('px-4 py-3 flex items-center gap-3', c.bg)}>
        {isError
          ? <AlertCircle className={cn('w-5 h-5 flex-shrink-0', c.icon)} />
          : <CheckCircle2 className={cn('w-5 h-5 flex-shrink-0', c.icon)} />
        }
        <div className="flex-1 min-w-0">
          <div className="text-sm font-bold text-stone-900">{summary.message}</div>
          {!isError && (
            <div className="text-[11px] text-stone-500 mt-0.5">
              {summary.newLeads > 0
                ? `${summary.newLeads} new lead${summary.newLeads !== 1 ? 's' : ''} found`
                : 'No new leads this run'
              }
              {summary.duration > 0 && ` · ${Math.round(summary.duration)}s`}
            </div>
          )}
        </div>
        <button onClick={dismissToast} className="p-1 text-stone-400 hover:text-stone-600 rounded-md hover:bg-white/50 transition">
          <X className="w-4 h-4" />
        </button>
      </div>

      {/* Log toggle */}
      <div className="px-4 py-2 border-t border-stone-100 flex items-center justify-between">
        <button
          onClick={() => setShowLogs(p => !p)}
          className="flex items-center gap-1.5 text-[11px] font-medium text-stone-400 hover:text-stone-600 transition"
        >
          {showLogs ? <ChevronDown className="w-3 h-3" /> : <ChevronUp className="w-3 h-3" />}
          {showLogs ? 'Hide' : 'Show'} log ({logs.length} events)
        </button>
        <button onClick={dismissToast} className="text-[11px] font-semibold text-stone-400 hover:text-stone-600 transition">
          Dismiss
        </button>
      </div>

      {/* Expandable log */}
      {showLogs && (
        <div className="bg-stone-950 text-stone-400 px-3 py-2 max-h-[200px] overflow-y-auto font-mono text-[10px] leading-relaxed">
          {logs.map((log, i) => (
            <div key={i} className={cn(
              log.includes('Error') || log.includes('❌') ? 'text-red-400' :
              log.includes('✅') || log.includes('Saved') || log.includes('complete') ? 'text-emerald-400' :
              log.includes('Phase') || log.includes('Starting') || log.includes('═') ? 'text-amber-400' : '',
            )}>{log}</div>
          ))}
        </div>
      )}
    </div>
  )
}
