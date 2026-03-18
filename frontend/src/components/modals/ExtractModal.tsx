import { useState } from 'react'
import { X, Link2, Loader2 } from 'lucide-react'
import { triggerExtractUrl } from '@/api/leads'
import { useBackgroundTask } from '@/hooks/useBackgroundTask'
import { cn } from '@/lib/utils'

interface Props { onClose: () => void }

export default function ExtractModal({ onClose }: Props) {
  const [url, setUrl] = useState('')
  const [starting, setStarting] = useState(false)
  const [error, setError] = useState('')

  const { startTask } = useBackgroundTask()

  async function handleExtract() {
    if (!url.trim()) return
    setStarting(true)
    setError('')
    try {
      const result = await triggerExtractUrl(url.trim())
      const extractId = result.extract_id || ''
      startTask('extract', `/api/dashboard/extract-url/stream?extract_id=${extractId}`)
      onClose()
    } catch (err: any) {
      setStarting(false)
      setError(err?.response?.data?.message || err.message || 'Failed to start extraction')
    }
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center modal-backdrop animate-fadeIn" onClick={onClose}>
      <div className="bg-white rounded-xl shadow-2xl w-full max-w-lg mx-4 overflow-hidden animate-scaleIn" onClick={e => e.stopPropagation()}>
        <div className="flex items-center justify-between px-5 py-3.5 border-b border-stone-200 bg-stone-50">
          <div className="flex items-center gap-2">
            <div className="w-7 h-7 rounded-lg bg-blue-100 flex items-center justify-center">
              <Link2 className="w-3.5 h-3.5 text-blue-600" />
            </div>
            <h3 className="text-sm font-bold text-navy-900">Extract Leads from URL</h3>
          </div>
          <button onClick={onClose} className="p-1.5 text-stone-400 hover:text-stone-600 rounded-md hover:bg-stone-100 transition">
            <X className="w-4 h-4" />
          </button>
        </div>

        <div className="p-5 space-y-4">
          <div>
            <label className="text-[11px] font-semibold text-stone-500 uppercase tracking-wider mb-2 block">Article URL</label>
            <input type="url" value={url} onChange={e => setUrl(e.target.value)}
              placeholder="https://example.com/hotel-openings-2026"
              className="w-full px-3.5 py-2.5 text-sm border-2 border-stone-200 rounded-lg focus:ring-0 focus:border-blue-400 outline-none transition-colors bg-white"
              autoFocus
              disabled={starting}
              onKeyDown={e => e.key === 'Enter' && url.trim() && !starting && handleExtract()} />
            <p className="text-[10px] text-stone-400 mt-1.5">Paste any article with hotel opening news — we'll extract and score every lead.</p>
          </div>

          {error && (
            <div className="text-xs text-red-600 bg-red-50 border border-red-200 rounded-lg px-3 py-2">{error}</div>
          )}

          <button onClick={handleExtract} disabled={!url.trim() || starting}
            className={cn(
              'w-full flex items-center justify-center gap-2 px-4 py-2.5 text-sm font-semibold rounded-lg transition-all shadow-sm active:scale-[0.98]',
              !url.trim() || starting
                ? 'bg-stone-300 text-stone-500 cursor-not-allowed shadow-none'
                : 'bg-blue-600 text-white hover:bg-blue-700',
            )}>
            {starting ? <Loader2 className="w-4 h-4 animate-spin" /> : <Link2 className="w-4 h-4" />}
            {starting ? 'Starting...' : 'Extract Leads'}
          </button>
        </div>
      </div>
    </div>
  )
}
