import { clsx, type ClassValue } from 'clsx'
import { twMerge } from 'tailwind-merge'

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

/* ── Dates ── */

export function formatDate(dateStr: string | null | undefined): string {
  if (!dateStr) return '—'
  const d = new Date(dateStr)
  return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })
}

export function relativeDate(dateStr: string | null | undefined): string {
  if (!dateStr) return '—'
  const d = new Date(dateStr)
  const now = new Date()
  const diffMs = now.getTime() - d.getTime()
  const diffMin = Math.floor(diffMs / 60_000)
  if (diffMin < 1) return 'just now'
  if (diffMin < 60) return `${diffMin}m ago`
  const diffHr = Math.floor(diffMin / 60)
  if (diffHr < 24) return `${diffHr}h ago`
  const diffDays = Math.floor(diffHr / 24)
  if (diffDays < 7) return `${diffDays}d ago`
  return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })
}

/* ── Score ── */

export function getScoreColor(score: number | null | undefined): string {
  const s = score ?? 0
  if (s >= 80) return 'bg-coral-500 text-white'
  if (s >= 60) return 'bg-gold-400 text-white'
  if (s >= 40) return 'bg-navy-300 text-white'
  return 'bg-stone-300 text-white'
}

export const getScoreBg = getScoreColor

export function getScoreRing(score: number | null | undefined): string {
  const s = score ?? 0
  if (s >= 80) return 'score-hot'
  if (s >= 60) return 'score-warm'
  return ''
}

/* ── Timeline ── */

export function getTimelineLabel(lead: {
  opening_date?: string | null
  opening_year?: number | string | null
}): string {
  const now = new Date()
  const currentYear = now.getFullYear()
  const currentMonth = now.getMonth() + 1

  // Try to extract year (and optional month) from opening_date string
  const raw = lead.opening_date || ''
  const match = raw.match(/(\d{4})[-/]?(\d{1,2})?/)

  let parsedYear: number | null = null
  let parsedMonth: number | null = null

  if (match) {
    parsedYear = Number(match[1])
    parsedMonth = match[2] ? Number(match[2]) : null
  }

  // Also check opening_year field as fallback
  if (!parsedYear && lead.opening_year) {
    parsedYear = Number(lead.opening_year)
  }

  // If we have month-level precision, calculate months away
  if (parsedYear && parsedMonth) {
    const monthsAway = (parsedYear - currentYear) * 12 + (parsedMonth - currentMonth)
    if (monthsAway <= 3) return 'Late'
    if (monthsAway <= 6) return 'Urgent'
    if (monthsAway <= 12) return 'Hot'
    if (monthsAway <= 18) return 'Warm'
    return 'Cool'
  }

  // Year-only: use whatever year we found
  if (parsedYear) {
    if (parsedYear < currentYear) return 'Late'
    if (parsedYear === currentYear) return 'Hot'
    if (parsedYear === currentYear + 1) return 'Warm'
    return 'Cool'
  }

  return 'TBD'
}

export function getTimelineColor(label: string): string {
  switch (label) {
    case 'Late':   return 'bg-stone-200 text-stone-600'
    case 'Urgent': return 'bg-coral-100 text-coral-600'
    case 'Hot':    return 'bg-coral-50 text-coral-500'
    case 'Warm':   return 'bg-gold-100 text-gold-600'
    case 'Cool':   return 'bg-navy-50 text-navy-500'
    default:       return 'bg-stone-100 text-stone-400'
  }
}

/* ── Tier ── */

export function getTierShort(tier: string | null | undefined): string {
  if (!tier) return '—'
  const match = tier.match(/tier(\d)/i)
  return match ? `T${match[1]}` : tier
}

export function getTierLabel(tier: string | null | undefined): string {
  if (!tier) return '—'
  const map: Record<string, string> = {
    tier1_ultra_luxury: 'Ultra Luxury',
    tier2_luxury: 'Luxury',
    tier3_upper_upscale: 'Upper Upscale',
    tier4_upscale: 'Upscale',
    tier4_low: 'Select Service',
    tier5_budget: 'Budget',
  }
  return map[tier] || tier
}

export function getTierColor(tier: string | null | undefined): string {
  if (!tier) return 'bg-stone-100 text-stone-400'
  if (tier.includes('1')) return 'bg-gold-100 text-gold-600 ring-1 ring-gold-200'
  if (tier.includes('2')) return 'bg-gold-50 text-gold-500'
  if (tier.includes('3')) return 'bg-navy-50 text-navy-600'
  if (tier.includes('4')) return 'bg-stone-100 text-stone-600'
  return 'bg-stone-100 text-stone-400'
}

/* ── Location ── */

export function formatLocation(lead: {
  city?: string | null
  state?: string | null
  country?: string | null
}): string {
  const parts: string[] = []
  if (lead.city) parts.push(lead.city)
  if (lead.state) parts.push(lead.state)
  if (lead.country && lead.country !== 'USA' && lead.country !== 'US') {
    parts.push(lead.country)
  }
  return parts.join(', ') || '—'
}

/* ── Opening date display ── */

export function formatOpening(lead: {
  opening_date?: string | null
  opening_year?: number | string | null
}): string {
  if (lead.opening_date) return lead.opening_date
  if (lead.opening_year) return String(lead.opening_year)
  return '—'
}
