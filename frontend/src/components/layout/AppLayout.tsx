import { ReactNode, useState, useEffect } from 'react'
import { Link, useLocation } from 'react-router-dom'
import { useAuth } from '@/hooks/useAuth'
import { BackgroundTaskProvider, useBackgroundTask } from '@/hooks/useBackgroundTask'
import {
  LayoutDashboard, Map, Mail, Users, LogOut, Play, Radar, Building2, Hotel,
  CheckCircle2, AlertCircle, X, ChevronDown, ChevronUp, Shield,
} from 'lucide-react'
import { cn } from '@/lib/utils'
import ScrapeModal from '@/components/modals/ScrapeModal'
import DiscoveryModal from '@/components/modals/DiscoveryModal'

interface NavItem {
  to: string
  label: string
  icon: React.ElementType
  disabled?: boolean
}

const NAV_ITEMS: NavItem[] = [
  { to: '/dashboard',        label: 'Dashboard',        icon: LayoutDashboard },
  { to: '/existing-hotels',  label: 'Existing Hotels',  icon: Building2 },
  { to: '/clients',          label: 'Clients',          icon: Users },
  { to: '/map',              label: 'Map',              icon: Map },
  { to: '/sources',          label: 'Sources',          icon: Radar },
  { to: '/users',            label: 'Team',             icon: Shield },
  { to: '/outreach',         label: 'Outreach',         icon: Mail, disabled: true },
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
  const [showDiscovery, setShowDiscovery] = useState(false)
  const bg = useBackgroundTask()

  return (
    <div className="h-full flex flex-col">

      <header className="bg-white/95 backdrop-blur-md border-b border-slate-200/60 h-[72px] flex-shrink-0 relative z-30 shadow-[0_1px_3px_rgba(0,0,0,0.04)]">
        <div className="h-full px-6 flex items-center justify-between">

          <div className="flex items-center gap-6">
            <Link to="/dashboard" className="flex items-center gap-3 group">
              <img
                src="/static/img/logo.svg"
                alt="Smart Lead Hunter"
                className="h-12 w-auto object-contain"
              />
              <span className="text-lg font-bold text-navy-900 tracking-tight whitespace-nowrap">
                Smart Lead Hunter
              </span>
            </Link>

            <div className="h-7 w-px bg-stone-200" />

            <nav className="flex items-center gap-1">
              {NAV_ITEMS.map((item) => {
                const Icon = item.icon
                const isActive = location.pathname.startsWith(item.to)
                return (
                  <Link
                    key={item.to}
                    to={item.disabled ? '#' : item.to}
                    className={cn(
                      'flex items-center gap-2 px-4 py-2.5 rounded-lg text-sm font-semibold transition-all duration-150',
                      isActive
                        ? 'bg-navy-50 text-navy-800'
                        : item.disabled
                          ? 'text-stone-300 cursor-not-allowed'
                          : 'text-stone-500 hover:bg-stone-100/70 hover:text-navy-800',
                    )}
                    onClick={item.disabled ? (e: React.MouseEvent) => e.preventDefault() : undefined}
                  >
                    <Icon className="w-[18px] h-[18px]" />
                    {item.label}
                    {item.disabled && (
                      <span className="text-2xs bg-stone-100 text-stone-400 px-1.5 py-0.5 rounded-full font-semibold uppercase tracking-wider">
                        Soon
                      </span>
                    )}
                  </Link>
                )
              })}
            </nav>
          </div>

          <div className="flex items-center gap-3">
            {bg.isRunning && <TaskPill />}

            <button
              onClick={() => setShowScrape(true)}
              disabled={bg.isRunning}
              className={cn(
                'header-action',
                bg.isRunning ? '' : 'bg-emerald-600 text-white hover:bg-emerald-700',
              )}
            >
              <Play className="w-4 h-4" /> Run Scrape
            </button>

            <button
              onClick={() => setShowDiscovery(true)}
              disabled={bg.isRunning}
              className={cn(
                'header-action',
                bg.isRunning ? '' : 'bg-violet-600 text-white hover:bg-violet-700',
              )}
            >
              <Radar className="w-4 h-4" /> Discovery
            </button>

            <div className="h-7 w-px bg-stone-200 mx-0.5" />

            <button
              onClick={logout}
              className="flex items-center gap-2 px-3 py-2 text-sm font-medium text-stone-400 hover:text-navy-800 hover:bg-stone-100/70 rounded-lg transition-all"
            >
              <LogOut className="w-[18px] h-[18px]" />
              <span className="hidden sm:inline">Logout</span>
            </button>
          </div>
        </div>
      </header>

      <main className="flex-1 overflow-hidden bg-[#f4f4f5]">{children}</main>

      {bg.summary && <ResultToast />}

      {showScrape    && <ScrapeModal    onClose={() => setShowScrape(false)} />}
      {showDiscovery && <DiscoveryModal onClose={() => setShowDiscovery(false)} />}
    </div>
  )
}


function TaskPill() {
  const { taskType, eventCount } = useBackgroundTask()

  const cfg: Record<string, { label: string; dot: string }> = {
    scrape:    { label: 'Scraping',    dot: 'bg-emerald-500' },
    extract:   { label: 'Extracting',  dot: 'bg-blue-500' },
    discovery: { label: 'Discovering', dot: 'bg-violet-500' },
  }
  const { label, dot } = cfg[taskType || 'scrape'] ?? cfg.scrape

  return (
    <div className="flex items-center gap-2 px-3.5 py-2 rounded-full bg-navy-950 text-white text-xs font-semibold animate-fadeIn">
      <span className={cn('w-2 h-2 rounded-full animate-pulse', dot)} />
      {label}…
      <span className="text-stone-400 tabular-nums">{eventCount}</span>
    </div>
  )
}


function ResultToast() {
  const { summary, status, logs, dismissToast } = useBackgroundTask()
  const [showLogs, setShowLogs] = useState(false)

  useEffect(() => {
    if (showLogs) return
    const t = setTimeout(dismissToast, 15_000)
    return () => clearTimeout(t)
  }, [dismissToast, showLogs])

  if (!summary) return null

  const isError = status === 'error'

  const palette: Record<string, { border: string; bg: string; icon: string }> = {
    scrape:    { border: 'border-emerald-200', bg: 'bg-emerald-50', icon: 'text-emerald-600' },
    extract:   { border: 'border-blue-200',    bg: 'bg-blue-50',    icon: 'text-blue-600' },
    discovery: { border: 'border-violet-200',  bg: 'bg-violet-50',  icon: 'text-violet-600' },
  }
  const c = isError
    ? { border: 'border-red-200', bg: 'bg-red-50', icon: 'text-red-600' }
    : palette[summary.type] ?? palette.scrape

  return (
    <div className={cn(
      'fixed bottom-5 right-5 z-50 w-[380px] rounded-xl border shadow-lift overflow-hidden animate-slideUp',
      c.border, 'bg-white',
    )}>
      <div className={cn('px-4 py-3 flex items-center gap-3', c.bg)}>
        {isError
          ? <AlertCircle  className={cn('w-5 h-5 flex-shrink-0', c.icon)} />
          : <CheckCircle2 className={cn('w-5 h-5 flex-shrink-0', c.icon)} />
        }
        <div className="flex-1 min-w-0">
          <p className="text-sm font-bold text-stone-900 leading-snug">{summary.message}</p>
          {!isError && (
            <p className="text-xs text-stone-500 mt-0.5">
              {summary.newLeads > 0
                ? `${summary.newLeads} new lead${summary.newLeads !== 1 ? 's' : ''} found`
                : 'No new leads this run'}
              {summary.duration > 0 && ` · ${Math.round(summary.duration)}s`}
            </p>
          )}
        </div>
        <button onClick={dismissToast} className="p-1 text-stone-400 hover:text-stone-600 rounded hover:bg-white/60 transition">
          <X className="w-4 h-4" />
        </button>
      </div>

      <div className="px-4 py-2.5 border-t border-stone-100 flex items-center justify-between">
        <button onClick={() => setShowLogs((p) => !p)} className="flex items-center gap-1.5 text-xs font-medium text-stone-400 hover:text-stone-600 transition">
          {showLogs ? <ChevronDown className="w-3.5 h-3.5" /> : <ChevronUp className="w-3.5 h-3.5" />}
          {showLogs ? 'Hide' : 'Show'} log ({logs.length})
        </button>
        <button onClick={dismissToast} className="text-xs font-semibold text-stone-400 hover:text-stone-600 transition">
          Dismiss
        </button>
      </div>

      {showLogs && (
        <div className="bg-navy-950 text-stone-400 px-3 py-2 max-h-[200px] overflow-y-auto font-mono text-2xs leading-relaxed">
          {logs.map((line, i) => (
            <div key={i} className={cn(
              line.includes('Error') || line.includes('❌') ? 'text-red-400' :
              line.includes('✅') || line.includes('Saved') || line.includes('complete') ? 'text-emerald-400' :
              line.includes('Phase') || line.includes('Starting') || line.includes('═') ? 'text-amber-400' : '',
            )}>{line}</div>
          ))}
        </div>
      )}
    </div>
  )
}
