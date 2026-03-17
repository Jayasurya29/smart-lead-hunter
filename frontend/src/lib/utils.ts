import { clsx, type ClassValue } from 'clsx'
import { twMerge } from 'tailwind-merge'

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

export function formatDate(dateStr: string | null): string {
  if (!dateStr) return '—'
  const d = new Date(dateStr)
  return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })
}

export function relativeDate(dateStr: string | null): string {
  if (!dateStr) return '—'
  const d = new Date(dateStr)
  const now = new Date()
  const diff = now.getTime() - d.getTime()
  const hours = Math.floor(diff / 3600000)
  if (hours < 1) return 'Just now'
  if (hours < 24) return `${hours}h ago`
  const days = Math.floor(hours / 24)
  if (days === 1) return 'Yesterday'
  if (days < 7) return `${days}d ago`
  if (days < 30) return `${Math.floor(days / 7)}w ago`
  return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })
}

export function formatScore(score: number | null): string {
  return score !== null ? String(score) : '—'
}

// ── Score styling (matches HTMX score-hot / score-warm / score-cold) ──

export function getScoreColor(score: number | null): string {
  if (score === null) return 'text-gray-400'
  if (score >= 70) return 'text-red-600'
  if (score >= 50) return 'text-orange-600'
  if (score >= 30) return 'text-blue-600'
  return 'text-gray-500'
}

export function getScoreBg(score: number | null): string {
  if (score === null) return 'bg-gray-50 border-gray-200 text-gray-400'
  if (score >= 70) return 'bg-gradient-to-br from-red-50 to-red-100 border-red-300 text-red-700'
  if (score >= 50) return 'bg-gradient-to-br from-orange-50 to-orange-100 border-orange-300 text-orange-700'
  if (score >= 30) return 'bg-gradient-to-br from-blue-50 to-blue-100 border-blue-300 text-blue-700'
  return 'bg-gray-50 border-gray-200 text-gray-500'
}

// ── Status styling ──

export function getStatusColor(status: string): string {
  switch (status) {
    case 'new': return 'bg-blue-50 text-blue-700 ring-blue-600/20'
    case 'approved': return 'bg-emerald-50 text-emerald-700 ring-emerald-600/20'
    case 'rejected': return 'bg-red-50 text-red-700 ring-red-600/20'
    case 'deleted': return 'bg-gray-50 text-gray-600 ring-gray-500/20'
    default: return 'bg-gray-50 text-gray-600 ring-gray-500/20'
  }
}

// ── Tier styling (matches HTMX tier-1..4 badges) ──

export function getTierLabel(tier: string | null): string {
  const map: Record<string, string> = {
    tier1_ultra_luxury: 'Ultra Luxury',
    tier2_luxury: 'Luxury',
    tier3_upper_upscale: 'Upper Upscale',
    tier4_upscale: 'Upscale',
    tier5_skip: 'Budget',
  }
  return tier ? map[tier] || tier : '—'
}

export function getTierShort(tier: string | null): string {
  const map: Record<string, string> = {
    tier1_ultra_luxury: 'T1',
    tier2_luxury: 'T2',
    tier3_upper_upscale: 'T3',
    tier4_upscale: 'T4',
    tier5_skip: 'T5',
  }
  return tier ? map[tier] || '—' : '—'
}

export function getTierColor(tier: string | null): string {
  switch (tier) {
    case 'tier1_ultra_luxury': return 'bg-amber-50 text-amber-800 border border-amber-300'
    case 'tier2_luxury': return 'bg-violet-50 text-violet-800 border border-violet-300'
    case 'tier3_upper_upscale': return 'bg-blue-50 text-blue-800 border border-blue-300'
    case 'tier4_upscale': return 'bg-slate-50 text-slate-700 border border-slate-300'
    default: return 'bg-gray-50 text-gray-600 border border-gray-200'
  }
}

// ── Timeline styling ──

export function getTimelineLabel(openingDate: string | null): string {
  if (!openingDate) return 'TBD'
  // Parse various date formats
  const now = new Date()
  const parsed = tryParseDate(openingDate)
  if (!parsed) return 'TBD'
  const months = (parsed.getFullYear() - now.getFullYear()) * 12 + (parsed.getMonth() - now.getMonth())
  if (months < 0) return 'Expired'
  if (months <= 3) return 'Late'
  if (months <= 6) return 'Urgent'
  if (months <= 12) return 'Hot'
  if (months <= 18) return 'Warm'
  return 'Cool'
}

export function getTimelineColor(label: string): string {
  switch (label) {
    case 'Hot': return 'bg-red-100 text-red-700 border-red-300'
    case 'Urgent': return 'bg-orange-100 text-orange-700 border-orange-300'
    case 'Warm': return 'bg-amber-100 text-amber-700 border-amber-300'
    case 'Cool': return 'bg-cyan-100 text-cyan-700 border-cyan-300'
    case 'Late': return 'bg-gray-200 text-gray-600 border-gray-400'
    case 'Expired': return 'bg-gray-100 text-gray-500 border-gray-300'
    default: return 'bg-gray-100 text-gray-500 border-gray-300'
  }
}

function tryParseDate(str: string): Date | null {
  // "March 2026", "Spring 2026", "Q2 2026", "2026", "Early 2026"
  const monthNames = ['january','february','march','april','may','june','july','august','september','october','november','december']
  const lower = str.toLowerCase().trim()
  // "March 2026" or "march 15, 2026"
  for (let i = 0; i < monthNames.length; i++) {
    if (lower.startsWith(monthNames[i])) {
      const yearMatch = lower.match(/(\d{4})/)
      if (yearMatch) return new Date(parseInt(yearMatch[1]), i, 15)
    }
  }
  // Season/quarter
  const yearMatch = lower.match(/(\d{4})/)
  if (yearMatch) {
    const year = parseInt(yearMatch[1])
    if (lower.includes('q1') || lower.includes('winter') || lower.includes('early')) return new Date(year, 1, 15)
    if (lower.includes('q2') || lower.includes('spring')) return new Date(year, 4, 15)
    if (lower.includes('q3') || lower.includes('summer') || lower.includes('mid')) return new Date(year, 7, 15)
    if (lower.includes('q4') || lower.includes('fall') || lower.includes('autumn') || lower.includes('late')) return new Date(year, 10, 15)
    // Bare year
    return new Date(year, 6, 1) // mid-year estimate
  }
  return null
}

// ── Location formatting ──

export function formatLocation(city: string | null, state: string | null, country: string | null): string {
  const parts = []
  if (city) parts.push(city)
  if (state) parts.push(state)
  if (country && country !== 'USA' && country !== 'US') parts.push(country)
  return parts.join(', ') || '—'
}
