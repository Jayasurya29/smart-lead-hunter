/**
 * ContactList — left rail of the split-view Contacts page.
 * AI smart-search, decision-maker focus, category chips, scrollable list.
 */
import { useState } from 'react'
import {
  Sparkles, X, RefreshCw, Filter, Wand2, Inbox,
} from 'lucide-react'
import { cn, relativeDate } from '@/lib/utils'
import type { InboxContact, InboxContactStats } from '@/api/inboxContacts'
import {
  Avatar, CategoryBadge, ConfRing, HighOppTag, fullName, roleText,
  confidencePct, isHighOpportunity, StaleBadge,
} from './contactsUi'

export type SortKey = 'confidence' | 'opportunity' | 'recent' | 'name'

interface ListProps {
  items: InboxContact[]
  total: number
  selectedId: number | null
  onSelect: (id: number) => void
  query: string
  setQuery: (v: string) => void
  category: string
  setCategory: (v: string) => void
  dmOnly: boolean
  setDmOnly: (v: boolean) => void
  sort: SortKey
  setSort: (v: SortKey) => void
  stats?: InboxContactStats
  syncing: boolean
  onSync: () => void
  loading: boolean
}

function Chip({
  label, count, active, color, onClick,
}: {
  label: string
  count?: number
  active: boolean
  color?: string
  onClick: () => void
}) {
  return (
    <button
      onClick={onClick}
      className={cn(
        'inline-flex items-center gap-1.5 px-3 h-8 rounded-lg text-xs font-semibold whitespace-nowrap transition-all',
        active ? 'text-white shadow-soft' : 'text-stone-500 bg-stone-100/70 hover:bg-stone-200/60',
      )}
      style={active ? { background: color || '#2e4a6e' } : undefined}
    >
      {label}
      {count != null && (
        <span
          className={cn(
            'tabular-nums px-1.5 py-0.5 rounded-md text-[10px]',
            active ? 'bg-white/25' : 'bg-white text-stone-400',
          )}
        >
          {count.toLocaleString()}
        </span>
      )}
    </button>
  )
}

function Row({
  contact, active, onClick,
}: {
  contact: InboxContact
  active: boolean
  onClick: () => void
}) {
  return (
    <button
      onClick={onClick}
      className={cn(
        'group w-full text-left flex items-center gap-3 px-3.5 py-3 rounded-xl transition-all duration-150 relative',
        active ? 'bg-white shadow-card ring-1 ring-navy-100' : 'hover:bg-white/70',
      )}
    >
      {active && (
        <span className="absolute left-0 top-1/2 -translate-y-1/2 h-7 w-1 rounded-r-full bg-navy-600" />
      )}
      <Avatar contact={contact} size={40} />
      <div className="min-w-0 flex-1">
        <div className="flex items-center gap-1.5">
          <span className="truncate font-semibold text-navy-900 text-sm">{fullName(contact)}</span>
          {contact.is_decision_maker && (
            <span className="text-gold-500 text-[10px] flex-shrink-0" title="Decision-maker">★</span>
          )}
        </div>
        <div className="truncate text-stone-500 text-xs mt-0.5">
          {roleText(contact) || <span className="italic text-stone-400">role unknown</span>}
          <span className="text-stone-300">{'  ·  '}</span>
          <span className="text-stone-400 font-medium">{contact.organization || '—'}</span>
        </div>
        <div className="flex items-center gap-2 mt-1.5">
          <CategoryBadge category={contact.contact_category} />
          <StaleBadge contact={contact} />
          {isHighOpportunity(contact) && <HighOppTag />}
          <span className="text-[10px] text-stone-400 ml-auto whitespace-nowrap flex-shrink-0">
            {relativeDate(contact.last_seen)}
          </span>
        </div>
      </div>
      <ConfRing value={confidencePct(contact)} size={34} />
    </button>
  )
}

export default function ContactList({
  items, total, selectedId, onSelect, query, setQuery,
  category, setCategory, dmOnly, setDmOnly, sort, setSort,
  stats, syncing, onSync, loading,
}: ListProps) {
  const [focused, setFocused] = useState(false)
  const smart = query.trim().length > 0

  const suggestions: Array<[string, () => void]> = [
    ['★ Decision-makers', () => { setDmOnly(true); setQuery(''); setCategory('') }],
    ['High opportunity', () => setQuery('high opportunity')],
    ['Replied recently', () => setQuery('replied')],
    ['Luxury brands', () => setQuery('luxury')],
    ['May have moved', () => setQuery('may have moved')],
  ]

  return (
    <div className="flex flex-col h-full bg-stone-50 border-r border-stone-200 flex-shrink-0 w-[412px]">

      {/* header */}
      <div className="flex-shrink-0 px-4 pt-4 pb-3">
        <div className="flex items-center justify-between mb-3">
          <div>
            <h1 className="text-[17px] font-bold text-navy-900 leading-none">Contacts</h1>
            <p className="text-[11px] text-stone-400 font-semibold uppercase tracking-wide mt-1">
              <span className="text-navy-700 tabular-nums">{(stats?.total ?? total).toLocaleString()}</span> people
              {' · '}
              <span className="text-gold-600 tabular-nums">{(stats?.decision_makers ?? 0).toLocaleString()}</span> decision-makers
            </p>
          </div>
          <button
            onClick={onSync}
            disabled={syncing}
            className="flex items-center gap-1.5 px-3 h-8 rounded-lg text-xs font-semibold text-white bg-navy-600 hover:bg-navy-700 shadow-soft transition-all disabled:opacity-60"
          >
            <RefreshCw className={cn('w-3.5 h-3.5', syncing && 'animate-spin')} />
            {syncing ? 'Syncing…' : 'Sync'}
          </button>
        </div>

        {/* AI smart search */}
        <div
          className={cn(
            'relative rounded-xl bg-white transition-all',
            focused ? 'ring-2 ring-navy-500 shadow-lift' : 'ring-1 ring-stone-200',
          )}
          style={focused ? { boxShadow: '0 0 0 4px rgba(46,74,110,.08)' } : undefined}
        >
          <div className="flex items-center gap-2 px-3 h-11">
            <Sparkles className="w-4 h-4 flex-shrink-0 text-navy-600" />
            <input
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              onFocus={() => setFocused(true)}
              onBlur={() => setFocused(false)}
              placeholder='Ask AI — e.g. "decision-makers at luxury hotels"'
              className="flex-1 bg-transparent outline-none text-sm text-navy-900 placeholder:text-stone-400"
            />
            {query ? (
              <button onClick={() => setQuery('')} className="text-stone-400 hover:text-stone-600">
                <X className="w-4 h-4" />
              </button>
            ) : (
              <kbd className="text-[10px] text-stone-400 font-semibold bg-stone-100 px-1.5 py-0.5 rounded">⌘K</kbd>
            )}
          </div>
          {smart && (
            <div className="px-3 pb-2 -mt-0.5 text-[11px] text-stone-400 flex items-center gap-1">
              <Wand2 className="w-3 h-3 text-navy-600" />
              AI matched <span className="font-bold text-navy-700">{items.length}</span> of {total}
            </div>
          )}
        </div>

        {/* suggested queries */}
        <div className="flex flex-wrap gap-1.5 mt-2.5">
          {suggestions.map(([label, fn]) => (
            <button
              key={label}
              onClick={fn}
              className="text-[11px] font-semibold text-stone-500 bg-white ring-1 ring-stone-200 hover:ring-stone-300 hover:text-navy-700 px-2.5 py-1 rounded-full transition-all"
            >
              {label}
            </button>
          ))}
        </div>
      </div>

      {/* category chips */}
      <div className="flex-shrink-0 px-4 pb-2.5 flex items-center gap-1.5 overflow-x-auto">
        <Chip label="All" count={stats?.total} active={category === '' && !dmOnly} onClick={() => { setCategory(''); setDmOnly(false) }} />
        <Chip label="★ DMs" count={stats?.decision_makers} active={dmOnly} color="#c49a3c" onClick={() => setDmOnly(!dmOnly)} />
        <Chip label="Buyers" count={stats?.buyer} active={category === 'buyer'} color="#1a7a55" onClick={() => { setCategory(category === 'buyer' ? '' : 'buyer'); setDmOnly(false) }} />
        <Chip label="Sellers" count={stats?.seller} active={category === 'seller'} color="#c49a3c" onClick={() => { setCategory(category === 'seller' ? '' : 'seller'); setDmOnly(false) }} />
        <Chip label="Competitors" count={stats?.competitor} active={category === 'competitor'} color="#e85d4a" onClick={() => { setCategory(category === 'competitor' ? '' : 'competitor'); setDmOnly(false) }} />
        <Chip label="Junk" count={stats?.junk} active={category === 'junk'} color="#8a847b" onClick={() => { setCategory(category === 'junk' ? '' : 'junk'); setDmOnly(false) }} />
      </div>

      {/* sort row */}
      <div className="flex-shrink-0 px-4 pb-2 flex items-center justify-between">
        <span className="text-[11px] font-bold text-stone-400 uppercase tracking-wider">
          {items.length.toLocaleString()} shown
        </span>
        <div className="flex items-center gap-1.5 text-[11px] text-stone-400">
          <Filter className="w-3 h-3" />
          <select
            value={sort}
            onChange={(e) => setSort(e.target.value as SortKey)}
            className="bg-transparent font-semibold text-stone-500 outline-none cursor-pointer hover:text-navy-700"
          >
            <option value="confidence">Top match</option>
            <option value="opportunity">Opportunity</option>
            <option value="recent">Last seen</option>
            <option value="name">Name A–Z</option>
          </select>
        </div>
      </div>

      {/* list */}
      <div className="flex-1 overflow-y-auto px-2.5 pb-4 space-y-0.5">
        {loading ? (
          <div className="space-y-2 px-1 pt-1">
            {Array.from({ length: 8 }).map((_, i) => (
              <div key={i} className="h-[76px] rounded-xl bg-stone-100 animate-pulse" />
            ))}
          </div>
        ) : items.length === 0 ? (
          <div className="flex flex-col items-center justify-center h-48 text-stone-400 text-center px-6">
            <Inbox className="w-8 h-8 text-stone-300 mb-2" />
            <p className="text-sm font-semibold text-stone-500">No matches</p>
            <p className="text-xs mt-1">Try a different search or filter</p>
          </div>
        ) : (
          items.map((c) => (
            <Row key={c.id} contact={c} active={c.id === selectedId} onClick={() => onSelect(c.id)} />
          ))
        )}
      </div>
    </div>
  )
}
