import { useEffect, useRef } from 'react'
import { cn } from '@/lib/utils'
import { AlertTriangle, CheckCircle2, XCircle, Undo2, Loader2 } from 'lucide-react'

type Variant = 'approve' | 'reject' | 'restore' | 'danger'

interface Props {
  open: boolean
  title: string
  message: string
  variant?: Variant
  confirmLabel?: string
  cancelLabel?: string
  pending?: boolean
  onConfirm: () => void
  onCancel: () => void
  children?: React.ReactNode
}

const VARIANT_CONFIG: Record<Variant, {
  icon: React.ElementType
  iconBg: string
  iconColor: string
  btnClass: string
}> = {
  approve: {
    icon: CheckCircle2,
    iconBg: 'bg-emerald-50',
    iconColor: 'text-emerald-600',
    btnClass: 'bg-emerald-600 hover:bg-emerald-700 text-white',
  },
  reject: {
    icon: XCircle,
    iconBg: 'bg-red-50',
    iconColor: 'text-red-500',
    btnClass: 'bg-red-600 hover:bg-red-700 text-white',
  },
  restore: {
    icon: Undo2,
    iconBg: 'bg-amber-50',
    iconColor: 'text-amber-600',
    btnClass: 'bg-amber-600 hover:bg-amber-700 text-white',
  },
  danger: {
    icon: AlertTriangle,
    iconBg: 'bg-red-50',
    iconColor: 'text-red-500',
    btnClass: 'bg-red-600 hover:bg-red-700 text-white',
  },
}

export default function ConfirmDialog({ open, title, message, variant = 'danger', confirmLabel = 'Confirm', cancelLabel = 'Cancel', pending, onConfirm, onCancel, children }: Props) {
  const cancelRef = useRef<HTMLButtonElement>(null)

  // Focus cancel button on open, trap Escape
  useEffect(() => {
    if (!open) return
    cancelRef.current?.focus()
    const handler = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onCancel()
    }
    document.addEventListener('keydown', handler)
    return () => document.removeEventListener('keydown', handler)
  }, [open, onCancel])

  if (!open) return null

  const config = VARIANT_CONFIG[variant]
  const Icon = config.icon

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      {/* Backdrop */}
      <div
        className="absolute inset-0 bg-navy-950/40 backdrop-blur-[2px] animate-fadeIn"
        onClick={onCancel}
      />

      {/* Dialog */}
      <div className="relative bg-white rounded-xl shadow-2xl shadow-navy-950/10 border border-stone-200/60 w-full max-w-md mx-4 animate-slideUp">
        <div className="p-6">
          {/* Icon + Title */}
          <div className="flex items-start gap-4">
            <div className={cn('w-10 h-10 rounded-lg flex items-center justify-center flex-shrink-0', config.iconBg)}>
              <Icon className={cn('w-5 h-5', config.iconColor)} />
            </div>
            <div className="flex-1 min-w-0">
              <h3 className="text-base font-bold text-navy-900 leading-snug">{title}</h3>
              <p className="text-sm text-stone-500 mt-1 leading-relaxed">{message}</p>
            </div>
          </div>
          {children && <div className="mt-4">{children}</div>}
        </div>

        {/* Actions */}
        <div className="flex items-center justify-end gap-2.5 px-6 py-4 border-t border-stone-100 bg-stone-50/50 rounded-b-xl">
          <button
            ref={cancelRef}
            onClick={onCancel}
            disabled={pending}
            className="px-4 py-2 text-sm font-semibold text-stone-600 hover:text-stone-800 hover:bg-stone-100 rounded-lg transition disabled:opacity-50"
          >
            {cancelLabel}
          </button>
          <button
            onClick={onConfirm}
            disabled={pending}
            className={cn(
              'flex items-center gap-2 px-4 py-2 text-sm font-semibold rounded-lg transition disabled:opacity-60',
              config.btnClass,
            )}
          >
            {pending && <Loader2 className="w-3.5 h-3.5 animate-spin" />}
            {confirmLabel}
          </button>
        </div>
      </div>
    </div>
  )
}
