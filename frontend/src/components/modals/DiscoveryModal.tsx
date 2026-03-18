import { useState } from 'react'
import { X, Radar, Loader2 } from 'lucide-react'
import { triggerDiscovery } from '@/api/leads'
import { useBackgroundTask } from '@/hooks/useBackgroundTask'
import { cn } from '@/lib/utils'

interface Props { onClose: () => void }

export default function DiscoveryModal({ onClose }: Props) {
  const [mode, setMode] = useState<'quick' | 'full'>('quick')
  const [starting, setStarting] = useState(false)
  const [error, setError] = useState('')

  const { startTask } = useBackgroundTask()

  async function handleStart() {
    setStarting(true)
    setError('')
    try {
      const result = await triggerDiscovery(mode)
      const discoveryId = result.discovery_id || ''
      startTask('discovery', `/api/dashboard/discovery/stream?discovery_id=${discoveryId}`)
      onClose()
    } catch (err: any) {
      setStarting(false)
      setError(err?.response?.data?.message || err.message || 'Failed to start discovery')
    }
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center modal-backdrop animate-fadeIn" onClick={onClose}>
      <div className="bg-white rounded-xl shadow-2xl w-full max-w-lg mx-4 overflow-hidden animate-scaleIn" onClick={e => e.stopPropagation()}>
        <div className="flex items-center justify-between px-5 py-3.5 border-b border-stone-200 bg-stone-50">
          <div className="flex items-center gap-2">
            <div className="w-7 h-7 rounded-lg bg-violet-100 flex items-center justify-center">
              <Radar className="w-3.5 h-3.5 text-violet-600" />
            </div>
            <h3 className="text-sm font-bold text-stone-900">Source Discovery</h3>
          </div>
          <button onClick={onClose} className="p-1.5 text-stone-400 hover:text-stone-600 rounded-md hover:bg-stone-100 transition">
            <X className="w-4 h-4" />
          </button>
        </div>

        <div className="p-5 space-y-4">
          <div>
            <label className="text-[11px] font-semibold text-stone-500 uppercase tracking-wider mb-2 block">Discovery Mode</label>
            <div className="flex items-center gap-3">
              {[
                { key: 'quick' as const, label: 'Quick (5 queries)', desc: 'Fast scan' },
                { key: 'full' as const, label: 'Full (all queries)', desc: 'Thorough search' },
              ].map(m => (
                <button key={m.key} onClick={() => setMode(m.key)} disabled={starting}
                  className={cn('flex-1 px-4 py-3 rounded-lg border-2 text-left transition-all',
                    mode === m.key ? 'border-violet-400 bg-violet-50' : 'border-stone-200 hover:border-stone-300',
                    starting && 'opacity-50 pointer-events-none',
                  )}>
                  <div className={cn('text-xs font-bold', mode === m.key ? 'text-violet-700' : 'text-stone-600')}>{m.label}</div>
                  <div className="text-[10px] text-stone-400 mt-0.5">{m.desc}</div>
                </button>
              ))}
            </div>
            <p className="text-[10px] text-stone-400 mt-2">Discovers new hospitality news sources via Google News + DuckDuckGo, then extracts leads from found articles.</p>
          </div>

          {error && (
            <div className="text-xs text-red-600 bg-red-50 border border-red-200 rounded-lg px-3 py-2">{error}</div>
          )}

          <button onClick={handleStart} disabled={starting}
            className={cn(
              'w-full flex items-center justify-center gap-2 px-4 py-2.5 text-sm font-semibold rounded-lg transition-all shadow-sm active:scale-[0.98]',
              starting
                ? 'bg-stone-300 text-stone-500 cursor-not-allowed shadow-none'
                : 'bg-violet-600 text-white hover:bg-violet-700',
            )}>
            {starting ? <Loader2 className="w-4 h-4 animate-spin" /> : <Radar className="w-4 h-4" />}
            {starting ? 'Starting...' : 'Start Discovery'}
          </button>
        </div>
      </div>
    </div>
  )
}
