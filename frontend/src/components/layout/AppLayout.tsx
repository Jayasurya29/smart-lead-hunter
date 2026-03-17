import React, { ReactNode, useState } from 'react'
import { Link, useLocation } from 'react-router-dom'
import { useAuth } from '@/hooks/useAuth'
import { LayoutDashboard, Map, Mail, LogOut, Play, Link2, Radar } from 'lucide-react'
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
  const { logout } = useAuth()
  const location = useLocation()
  const [showScrape, setShowScrape] = useState(false)
  const [showExtract, setShowExtract] = useState(false)
  const [showDiscovery, setShowDiscovery] = useState(false)

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
            <button onClick={() => setShowScrape(true)} className="flex items-center gap-2 px-4 py-2 text-xs font-semibold rounded-lg bg-emerald-600 text-white hover:bg-emerald-700 transition-all shadow-sm active:scale-[0.97]">
              <Play className="w-3.5 h-3.5" /> Run Scrape
            </button>
            <button onClick={() => setShowExtract(true)} className="flex items-center gap-2 px-4 py-2 text-xs font-semibold rounded-lg bg-blue-600 text-white hover:bg-blue-700 transition-all shadow-sm active:scale-[0.97]">
              <Link2 className="w-3.5 h-3.5" /> Extract URL
            </button>
            <button onClick={() => setShowDiscovery(true)} className="flex items-center gap-2 px-4 py-2 text-xs font-semibold rounded-lg bg-violet-600 text-white hover:bg-violet-700 transition-all shadow-sm active:scale-[0.97]">
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

      {showScrape && <ScrapeModal onClose={() => setShowScrape(false)} />}
      {showExtract && <ExtractModal onClose={() => setShowExtract(false)} />}
      {showDiscovery && <DiscoveryModal onClose={() => setShowDiscovery(false)} />}
    </div>
  )
}
