import { useState } from 'react'
import { useLead, useContacts, useApproveLead, useRejectLead, useRestoreLead, useDeleteLead, useEnrichLead } from '@/hooks/useLeads'
import { editLead, saveContact, setPrimaryContact, deleteContact, updateContact, toggleContactScope } from '@/api/leads'
import { useQueryClient } from '@tanstack/react-query'
import type { Lead, Contact } from '@/api/types'
import {
  cn, formatDate, getScoreColor, getScoreRing, getTimelineLabel, getTimelineColor,
  getTierLabel, getTierColor, getTierShort, formatLocation, formatOpening,
} from '@/lib/utils'
import {
  X, MapPin, Calendar, Building2, Layers, Globe, ExternalLink,
  User, Mail, Phone, Linkedin, Star, Bookmark, BookmarkCheck,
  Loader2, CheckCircle2, XCircle, Undo2, Trash2, Search, Save,
  Link2, Pencil, Check,
} from 'lucide-react'

/** Safely render any value — prevents empty object {} crashing React */
function safe(val: any): string {
  if (val === null || val === undefined) return '—'
  if (typeof val === 'string') return val
  if (typeof val === 'number' || typeof val === 'boolean') return String(val)
  return ''
}

interface Props {
  leadId: number
  tab: 'pipeline' | 'approved' | 'rejected' | 'expired'
  onClose: () => void
}

type DetailTab = 'overview' | 'contacts' | 'edit' | 'sources'

export default function LeadDetail({ leadId, tab, onClose }: Props) {
  const { data: lead, isLoading } = useLead(leadId)
  const { data: contacts, isLoading: contactsLoading } = useContacts(leadId)
  const [activeTab, setActiveTab] = useState<DetailTab>('overview')

  const approveMut = useApproveLead()
  const rejectMut  = useRejectLead()
  const restoreMut = useRestoreLead()
  const deleteMut  = useDeleteLead()
  const enrichMut  = useEnrichLead()

  const isNew      = tab === 'pipeline'
  const isApproved = tab === 'approved'
  const isRejected = tab === 'rejected'

  if (isLoading || !lead) {
    return (
      <div className="h-full flex flex-col bg-white animate-fadeIn">
        <div className="p-5 space-y-3">
          <div className="skeleton h-7 w-3/4 rounded" />
          <div className="skeleton h-5 w-1/2 rounded" />
          <div className="skeleton h-36 rounded-lg" />
        </div>
      </div>
    )
  }

  const timeline = getTimelineLabel(lead)
  const contactList = Array.isArray(contacts) ? contacts : []

  return (
    <div className="h-full flex flex-col bg-white animate-slideIn">
      {/* ═══ HEADER — name, score, badges ═══ */}
      <div className="px-5 pt-5 pb-3 flex-shrink-0 border-b border-slate-100 bg-gradient-to-b from-slate-50/50 to-white">
        <div className="flex items-start justify-between gap-3">
          <div className="min-w-0 flex-1">
            <h2 className="text-lg font-bold text-navy-900 leading-snug truncate">
              {lead.hotel_name || lead.name}
            </h2>
            {(lead.brand || lead.brand_name) && (
              <p className="text-sm text-stone-400 mt-0.5">{lead.brand || lead.brand_name}</p>
            )}
          </div>
          <div className="flex items-center gap-2 flex-shrink-0">
            <span className={cn(
              'inline-flex items-center justify-center w-10 h-8 text-sm font-bold rounded',
              getScoreColor(lead.lead_score), getScoreRing(lead.lead_score),
            )}>
              {lead.lead_score ?? '—'}
            </span>
            <button onClick={onClose} className="p-1.5 text-stone-400 hover:text-stone-600 rounded-lg hover:bg-stone-100 transition">
              <X className="w-4 h-4" />
            </button>
          </div>
        </div>

        <div className="flex items-center gap-2 mt-2 flex-wrap">
          {lead.brand_tier && (
            <span className={cn('inline-flex px-2 py-0.5 rounded text-xs font-bold', getTierColor(lead.brand_tier))}>
              {getTierShort(lead.brand_tier)} — {getTierLabel(lead.brand_tier)}
            </span>
          )}
          <span className={cn('inline-flex px-2 py-0.5 rounded text-xs font-bold', getTimelineColor(timeline))}>
            {timeline}
          </span>
        </div>
      </div>

      {/* ═══ TABS ═══ */}
      <div className="flex border-b border-slate-100 px-5 flex-shrink-0">
        {(['overview', 'contacts', 'edit', 'sources'] as DetailTab[]).map((t) => (
          <button
            key={t}
            onClick={() => setActiveTab(t)}
            className={cn(
              'px-3.5 py-3 text-xs font-semibold capitalize transition',
              activeTab === t ? 'text-navy-900 tab-active' : 'text-stone-400 hover:text-stone-600',
            )}
          >
            {t}
            {t === 'contacts' && contactList.length > 0 && (
              <span className="ml-1.5 text-2xs bg-navy-50 text-navy-600 px-1.5 py-0.5 rounded-full">{contactList.length}</span>
            )}
          </button>
        ))}
      </div>

      {/* ═══ TAB CONTENT — scrollable ═══ */}
      <div className="flex-1 overflow-y-auto p-5">
        {activeTab === 'overview'  && <OverviewTab lead={lead} contactList={contactList} onEnrich={() => enrichMut.mutate(leadId)} enriching={enrichMut.isPending} />}
        {activeTab === 'contacts'  && <ContactsTab contacts={contactList} loading={contactsLoading} leadId={leadId} onEnrich={() => enrichMut.mutate(leadId)} enriching={enrichMut.isPending} />}
        {activeTab === 'edit'      && <EditTab lead={lead} leadId={leadId} />}
        {activeTab === 'sources'   && <SourcesTab lead={lead} />}
      </div>

      {/* ═══ STICKY ACTION BAR — always visible at bottom ═══ */}
      <div className="px-5 py-3 border-t border-slate-100 bg-slate-50/50 flex-shrink-0">
        <div className="flex items-center gap-2">
          {isNew && (
            <>
              <button
                onClick={() => approveMut.mutate(leadId)}
                disabled={approveMut.isPending}
                className="flex items-center gap-1.5 px-4 py-2 text-xs font-semibold rounded-lg bg-emerald-600 text-white hover:bg-emerald-700 transition disabled:opacity-50"
              >
                {approveMut.isPending ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <CheckCircle2 className="w-3.5 h-3.5" />}
                Approve
              </button>
              <button
                onClick={() => rejectMut.mutate({ id: leadId })}
                disabled={rejectMut.isPending}
                className="flex items-center gap-1.5 px-4 py-2 text-xs font-semibold rounded-lg border border-stone-200 text-stone-600 hover:bg-stone-50 transition disabled:opacity-50"
              >
                {rejectMut.isPending ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <XCircle className="w-3.5 h-3.5" />}
                Reject
              </button>
              <button
                onClick={() => { deleteMut.mutate(leadId); onClose() }}
                disabled={deleteMut.isPending}
                className="flex items-center gap-1.5 px-3 py-2 text-xs font-medium rounded-lg border border-stone-200 text-stone-400 hover:bg-stone-50 transition disabled:opacity-50 ml-auto"
              >
                <Trash2 className="w-3.5 h-3.5" />
              </button>
            </>
          )}
          {isApproved && (
            <button
              onClick={() => rejectMut.mutate({ id: leadId })}
              disabled={rejectMut.isPending}
              className="flex items-center gap-1.5 px-4 py-2 text-xs font-semibold rounded-lg border border-red-200 text-red-600 hover:bg-red-50 transition disabled:opacity-50"
            >
              {rejectMut.isPending ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <XCircle className="w-3.5 h-3.5" />}
              Reject
            </button>
          )}
          {isRejected && (
            <button
              onClick={() => restoreMut.mutate(leadId)}
              disabled={restoreMut.isPending}
              className="flex items-center gap-1.5 px-4 py-2 text-xs font-semibold rounded-lg border border-blue-200 text-blue-600 hover:bg-blue-50 transition disabled:opacity-50"
            >
              {restoreMut.isPending ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Undo2 className="w-3.5 h-3.5" />}
              Restore
            </button>
          )}
        </div>
      </div>
    </div>
  )
}


/* ═══════════════════════════════════════════════════
   OVERVIEW TAB
   ═══════════════════════════════════════════════════ */

function OverviewTab({ lead, contactList, onEnrich, enriching }: {
  lead: Lead; contactList: Contact[]; onEnrich: () => void; enriching: boolean
}) {
  return (
    <div className="space-y-6 animate-fadeIn">
      {/* Details */}
      <Section title="Details">
        <div className="grid grid-cols-2 gap-4">
          <Field icon={Calendar}  label="Opening"    value={formatOpening(lead)} />
          <Field icon={MapPin}    label="Location"   value={formatLocation(lead)} />
          <Field icon={Building2} label="Rooms"      value={lead.room_count ? `${lead.room_count} rooms` : '—'} />
          <Field icon={Layers}    label="Brand Tier"  value={getTierLabel(lead.brand_tier)} />
          {lead.management_company && <Field icon={Building2} label="Mgmt Co."   value={lead.management_company} />}
          {lead.developer         && <Field icon={Building2} label="Developer"   value={lead.developer} />}
          {lead.owner             && <Field icon={User}      label="Owner"       value={lead.owner} />}
        </div>
      </Section>

      {/* Website */}
      {lead.hotel_website && typeof lead.hotel_website === 'string' && (
        <Section title="Website">
          <a
            href={lead.hotel_website.startsWith('http') ? lead.hotel_website : `https://${lead.hotel_website}`}
            target="_blank"
            rel="noopener noreferrer"
            className="flex items-center gap-2 text-sm text-navy-600 hover:text-navy-800 hover:underline transition"
          >
            <Globe className="w-4 h-4" />
            {lead.hotel_website}
          </a>
        </Section>
      )}

      {/* Primary Contact */}
      <Section title="Primary Contact">
        {contactList.length > 0 ? (
          <div className="bg-slate-50 rounded-lg p-3.5 border border-slate-200/80">
            <div className="flex items-center gap-3">
              <div className="w-10 h-10 rounded-full bg-gradient-to-br from-navy-400 to-navy-600 flex items-center justify-center flex-shrink-0">
                <span className="text-white font-bold text-sm">
                  {(contactList[0].name || '?')[0].toUpperCase()}
                </span>
              </div>
              <div className="min-w-0 flex-1">
                <p className="text-sm font-semibold text-navy-900">{contactList[0].name}</p>
                <p className="text-xs text-stone-500 truncate">{contactList[0].title || 'No title'}</p>
              </div>
              {contactList.length > 1 && (
                <span className="text-xs text-navy-500 font-medium">+{contactList.length - 1} more</span>
              )}
            </div>
          </div>
        ) : (
          <button
            onClick={onEnrich}
            disabled={enriching}
            className="w-full text-left bg-gold-50 rounded-lg p-3.5 border border-gold-200 hover:border-gold-300 transition disabled:opacity-60"
          >
            <div className="flex items-center gap-2.5">
              {enriching ? <Loader2 className="w-5 h-5 text-gold-600 animate-spin" /> : <Search className="w-5 h-5 text-gold-600" />}
              <div>
                <p className="text-sm font-semibold text-gold-700">
                  {enriching ? 'Searching contacts...' : 'Find Contacts'}
                </p>
                <p className="text-xs text-gold-500">Search for GMs, Directors, Purchasing Managers</p>
              </div>
            </div>
          </button>
        )}
      </Section>

      {/* Key Insights — AI bullet points */}
      {lead.source_extractions && typeof lead.source_extractions === 'object' && Object.keys(lead.source_extractions).length > 0 && (
        <Section title={`Key Insights (${Object.keys(lead.source_extractions).length} sources)`}>
          <KeyInsights extractions={lead.source_extractions as Record<string, any>} />
        </Section>
      )}

      {/* Metadata */}
      <Section title="Metadata">
        <div className="space-y-2 text-sm">
          {[
            ['Lead ID',    String(lead.id)],
            ['Status',     lead.status],
            ['Created',    formatDate(lead.created_at)],
            lead.updated_at ? ['Updated', formatDate(lead.updated_at)] : null,
            lead.insightly_id ? ['Insightly', `#${lead.insightly_id}`] : null,
            (lead.rejection_reason && typeof lead.rejection_reason === 'string') ? ['Rejection', lead.rejection_reason] : null,
          ].filter(Boolean).map((row: any) => (
            <div key={row[0]} className="flex justify-between">
              <span className="text-stone-400 font-medium">{safe(row[0])}</span>
              <span className="text-navy-700 font-semibold capitalize">{safe(row[1])}</span>
            </div>
          ))}
        </div>
      </Section>
    </div>
  )
}


/* ═══════════════════════════════════════════════════
   CONTACTS TAB — with edit, delete, clean layout
   ═══════════════════════════════════════════════════ */

function ContactsTab({ contacts, loading, leadId, onEnrich, enriching }: {
  contacts: Contact[]; loading: boolean; leadId: number; onEnrich: () => void; enriching: boolean
}) {
  const qc = useQueryClient()
  const [editingId, setEditingId] = useState<number | null>(null)
  const [editForm, setEditForm] = useState<Record<string, string>>({})
  const [deleting, setDeleting] = useState<number | null>(null)

  if (loading) {
    return <div className="space-y-2">{Array.from({ length: 3 }).map((_, i) => <div key={i} className="skeleton h-24 rounded-lg" />)}</div>
  }

  if (!contacts.length) {
    return (
      <div className="text-center py-12 animate-fadeIn">
        <User className="w-12 h-12 text-stone-300 mx-auto mb-3" />
        <p className="text-sm font-medium text-stone-500">No contacts found</p>
        <button
          onClick={onEnrich}
          disabled={enriching}
          className="mt-3 px-5 py-2.5 text-xs font-semibold bg-navy-900 text-white rounded-lg hover:bg-navy-800 transition disabled:opacity-50"
        >
          {enriching ? 'Searching...' : 'Run Enrichment'}
        </button>
      </div>
    )
  }

  async function handleSave(contactId: number) {
    await saveContact(leadId, contactId)
    qc.invalidateQueries({ queryKey: ['contacts', leadId] })
  }

  async function handleSetPrimary(contactId: number) {
    await setPrimaryContact(leadId, contactId)
    qc.invalidateQueries({ queryKey: ['contacts', leadId] })
  }

  async function handleToggleScope(contactId: number, currentScope: string) {
    const cycle = ['hotel_specific', 'chain_area', 'chain_corporate']
    const idx = cycle.indexOf(currentScope)
    const next = cycle[(idx + 1) % cycle.length]
    await toggleContactScope(leadId, contactId, next)
    qc.invalidateQueries({ queryKey: ['contacts', leadId] })
    qc.invalidateQueries({ queryKey: ['lead', leadId] })
  }

  async function handleDelete(contactId: number) {
    setDeleting(contactId)
    try {
      await deleteContact(leadId, contactId)
      qc.invalidateQueries({ queryKey: ['contacts', leadId] })
      qc.invalidateQueries({ queryKey: ['lead', leadId] })
    } catch { /* ignore */ }
    setDeleting(null)
  }

  function startEdit(c: Contact) {
    setEditingId(c.id)
    setEditForm({
      name: c.name || '',
      title: c.title || '',
      organization: c.organization || '',
      email: c.email || '',
      phone: c.phone || '',
      linkedin: c.linkedin || '',
    })
  }

  async function saveEdit() {
    if (!editingId) return
    try {
      await updateContact(leadId, editingId, editForm)
      qc.invalidateQueries({ queryKey: ['contacts', leadId] })
    } catch { /* ignore */ }
    setEditingId(null)
    setEditForm({})
  }

  function cancelEdit() {
    setEditingId(null)
    setEditForm({})
  }

  function handleEditKey(e: React.KeyboardEvent) {
    if (e.key === 'Enter') saveEdit()
    if (e.key === 'Escape') cancelEdit()
  }

  return (
    <div className="space-y-2.5 animate-fadeIn">
      {contacts.map((c) => (
        <div
          key={c.id}
          className={cn(
            'rounded-lg border p-4 transition relative group',
            c.is_primary
              ? 'border-navy-200 bg-navy-50/30'
              : 'border-stone-100 hover:border-stone-200',
          )}
        >
          {/* Top-right action icons — visible on hover */}
          <div className="absolute top-3 right-3 flex items-center gap-1 opacity-0 group-hover:opacity-100 transition">
            {editingId !== c.id && (
              <button
                onClick={() => startEdit(c)}
                className="p-1.5 text-stone-400 hover:text-navy-600 hover:bg-stone-100 rounded-md transition"
                title="Edit contact"
              >
                <Pencil className="w-3 h-3" />
              </button>
            )}
            <button
              onClick={() => handleDelete(c.id)}
              disabled={deleting === c.id}
              className="p-1.5 text-stone-400 hover:text-red-600 hover:bg-red-50 rounded-md transition disabled:opacity-50"
              title="Delete contact"
            >
              {deleting === c.id ? <Loader2 className="w-3 h-3 animate-spin" /> : <Trash2 className="w-3 h-3" />}
            </button>
          </div>

          <div className="flex items-start gap-3">
            {/* Avatar */}
            <div className={cn(
              'w-9 h-9 rounded-full flex items-center justify-center flex-shrink-0 text-sm font-bold mt-0.5',
              c.is_primary ? 'bg-navy-600 text-white' : 'bg-stone-200 text-stone-600',
            )}>
              {(c.name || '?')[0].toUpperCase()}
            </div>

            <div className="flex-1 min-w-0 pr-16">
              {editingId === c.id ? (
                /* ── EDIT MODE ── */
                <div className="space-y-2" onKeyDown={handleEditKey}>
                  <div className="grid grid-cols-2 gap-2">
                    <input
                      value={editForm.name || ''}
                      onChange={(e) => setEditForm(f => ({ ...f, name: e.target.value }))}
                      placeholder="Name"
                      className="col-span-2 h-8 px-2.5 text-sm text-navy-900 bg-white border border-stone-200 rounded-md outline-none focus:border-navy-400 focus:ring-1 focus:ring-navy-200"
                      autoFocus
                    />
                    <input
                      value={editForm.title || ''}
                      onChange={(e) => setEditForm(f => ({ ...f, title: e.target.value }))}
                      placeholder="Title / Role"
                      className="col-span-2 h-8 px-2.5 text-sm text-navy-900 bg-white border border-stone-200 rounded-md outline-none focus:border-navy-400 focus:ring-1 focus:ring-navy-200"
                    />
                    <input
                      value={editForm.organization || ''}
                      onChange={(e) => setEditForm(f => ({ ...f, organization: e.target.value }))}
                      placeholder="Organization"
                      className="col-span-2 h-8 px-2.5 text-sm text-navy-900 bg-white border border-stone-200 rounded-md outline-none focus:border-navy-400 focus:ring-1 focus:ring-navy-200"
                    />
                    <input
                      value={editForm.email || ''}
                      onChange={(e) => setEditForm(f => ({ ...f, email: e.target.value }))}
                      placeholder="Email"
                      className="h-8 px-2.5 text-xs text-navy-900 bg-white border border-stone-200 rounded-md outline-none focus:border-navy-400 focus:ring-1 focus:ring-navy-200"
                    />
                    <input
                      value={editForm.phone || ''}
                      onChange={(e) => setEditForm(f => ({ ...f, phone: e.target.value }))}
                      placeholder="Phone"
                      className="h-8 px-2.5 text-xs text-navy-900 bg-white border border-stone-200 rounded-md outline-none focus:border-navy-400 focus:ring-1 focus:ring-navy-200"
                    />
                    <input
                      value={editForm.linkedin || ''}
                      onChange={(e) => setEditForm(f => ({ ...f, linkedin: e.target.value }))}
                      placeholder="LinkedIn URL"
                      className="col-span-2 h-8 px-2.5 text-xs text-navy-900 bg-white border border-stone-200 rounded-md outline-none focus:border-navy-400 focus:ring-1 focus:ring-navy-200"
                    />
                  </div>
                  <div className="flex items-center gap-2">
                    <button onClick={saveEdit} className="inline-flex items-center gap-1.5 px-3 py-1.5 text-xs font-semibold bg-navy-900 text-white rounded-md hover:bg-navy-800 transition">
                      <Check className="w-3 h-3" /> Save
                    </button>
                    <button onClick={cancelEdit} className="px-3 py-1.5 text-xs font-medium text-stone-500 hover:text-stone-700 transition">
                      Cancel
                    </button>
                  </div>
                </div>
              ) : (
                /* ── VIEW MODE ── */
                <>
                  {/* Row 1: Name + Primary star + Score */}
                  <div className="flex items-center gap-2">
                    <span className="text-sm font-semibold text-navy-900">{c.name}</span>
                    {c.is_primary && <Star className="w-3.5 h-3.5 text-gold-500 fill-gold-500" />}
                    {c.score > 0 && (
                      <div className="flex flex-col items-end ml-auto">
                        <span className="text-sm font-bold text-navy-900">{c.score}</span>
                        {c.confidence && (
                          <span className={cn(
                            'text-2xs font-bold uppercase',
                            c.confidence === 'high' ? 'text-emerald-600' :
                            c.confidence === 'medium' ? 'text-gold-600' : 'text-stone-400',
                          )}>
                            {c.confidence}
                          </span>
                        )}
                      </div>
                    )}
                  </div>

                  {/* Row 2: Title */}
                  {c.title && <p className="text-xs text-stone-500 mt-0.5">{c.title}</p>}

                  {/* Row 3: Organization */}
                  {c.organization && <p className="text-xs text-stone-400">{c.organization}</p>}

                  {/* Row 4: Contact links */}
                  <div className="flex items-center gap-3 mt-2 flex-wrap">
                    {c.linkedin && (
                      <a href={c.linkedin} target="_blank" rel="noopener noreferrer" className="inline-flex items-center gap-1.5 px-2.5 py-1 text-xs font-medium text-blue-700 bg-blue-50 rounded-md hover:bg-blue-100 transition">
                        <Linkedin className="w-3.5 h-3.5" /> LinkedIn
                      </a>
                    )}
                    {c.email && (
                      <a href={`mailto:${c.email}`} className="flex items-center gap-1.5 text-xs text-navy-600 hover:underline">
                        <Mail className="w-3.5 h-3.5" /> {c.email}
                      </a>
                    )}
                    {c.phone && (
                      <a href={`tel:${c.phone}`} className="flex items-center gap-1.5 text-xs text-navy-600 hover:underline">
                        <Phone className="w-3.5 h-3.5" /> {c.phone}
                      </a>
                    )}
                  </div>

                  {/* Row 5: Scope badge + evidence */}
                  {(c.scope || c.source_detail) && (
                    <div className="flex items-center gap-2 mt-2.5 flex-wrap">
                      {c.scope && (
                        <button
                          onClick={() => handleToggleScope(c.id, c.scope || 'chain_area')}
                          title="Click to cycle scope"
                          className={cn(
                            'text-2xs font-bold px-2 py-0.5 rounded-full uppercase flex-shrink-0 cursor-pointer transition hover:ring-2 hover:ring-offset-1',
                            c.scope === 'hotel_specific' ? 'bg-emerald-50 text-emerald-600 hover:ring-emerald-300' :
                            c.scope === 'chain_area' ? 'bg-amber-50 text-amber-600 hover:ring-amber-300' :
                            'bg-stone-100 text-stone-500 hover:ring-stone-300',
                          )}
                        >
                          {c.scope === 'hotel_specific' ? 'Hotel Specific' : c.scope === 'chain_area' ? 'Chain/Area' : c.scope.replace(/_/g, ' ')}
                        </button>
                      )}
                      {c.source_detail && typeof c.source_detail === 'string' && (
                        <span className="text-xs text-stone-500">{c.source_detail}</span>
                      )}
                    </div>
                  )}

                  {/* Row 6: Evidence link */}
                  {c.evidence_url && typeof c.evidence_url === 'string' && (
                    <a
                      href={c.evidence_url}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="flex items-center gap-1.5 mt-1.5 text-xs text-blue-600 hover:underline"
                    >
                      <ExternalLink className="w-3 h-3" /> View Evidence
                    </a>
                  )}

                  {/* Row 7: Action buttons */}
                  <div className="flex items-center gap-2 mt-2.5">
                    <button
                      onClick={() => handleSave(c.id)}
                      className={cn(
                        'inline-flex items-center gap-1.5 px-2.5 py-1 text-xs font-medium rounded-md border transition',
                        c.is_saved
                          ? 'border-navy-200 bg-navy-50 text-navy-700'
                          : 'border-stone-200 text-stone-500 hover:bg-stone-50',
                      )}
                    >
                      {c.is_saved ? <BookmarkCheck className="w-3 h-3" /> : <Bookmark className="w-3 h-3" />}
                      {c.is_saved ? 'Saved' : 'Save'}
                    </button>
                    {!c.is_primary && (
                      <button
                        onClick={() => handleSetPrimary(c.id)}
                        className="inline-flex items-center gap-1.5 px-2.5 py-1 text-xs font-medium rounded-md border border-stone-200 text-stone-500 hover:bg-stone-50 transition"
                      >
                        <Star className="w-3 h-3" /> Set Primary
                      </button>
                    )}
                    {c.is_primary && (
                      <span className="inline-flex items-center gap-1.5 px-2.5 py-1 text-xs font-semibold rounded-md bg-gold-50 text-gold-600 border border-gold-200">
                        <Star className="w-3 h-3 fill-gold-500" /> Primary
                      </span>
                    )}
                  </div>
                </>
              )}
            </div>
          </div>
        </div>
      ))}

      <button
        onClick={onEnrich}
        disabled={enriching}
        className="w-full mt-2 py-2.5 text-xs font-semibold text-stone-500 hover:text-navy-700 hover:bg-stone-50 rounded-lg border border-dashed border-stone-200 transition disabled:opacity-50"
      >
        {enriching ? 'Searching...' : 'Re-run Enrichment'}
      </button>
    </div>
  )
}


/* ═══════════════════════════════════════════════════
   EDIT TAB
   ═══════════════════════════════════════════════════ */

function EditTab({ lead, leadId }: { lead: Lead; leadId: number }) {
  const qc = useQueryClient()
  const [saving, setSaving] = useState(false)
  const [saveMsg, setSaveMsg] = useState('')
  const [form, setForm] = useState({
    hotel_name:         lead.hotel_name || '',
    brand:              lead.brand || lead.brand_name || '',
    brand_tier:         lead.brand_tier || '',
    city:               lead.city || '',
    state:              lead.state || '',
    country:            lead.country || '',
    opening_date:       lead.opening_date || '',
    room_count:         lead.room_count ? String(lead.room_count) : '',
    management_company: lead.management_company || '',
    developer:          lead.developer || '',
    owner:              lead.owner || '',
  })

  function handleChange(key: string, val: string) {
    setForm((prev) => ({ ...prev, [key]: val }))
  }

  async function handleSave() {
    setSaving(true)
    setSaveMsg('')
    try {
      await editLead(leadId, {
        ...form,
        room_count: form.room_count ? Number(form.room_count) : undefined,
      } as any)
      setSaveMsg('Saved!')
      qc.invalidateQueries({ queryKey: ['lead', leadId] })
      qc.invalidateQueries({ queryKey: ['leads'] })
      setTimeout(() => setSaveMsg(''), 3000)
    } catch {
      setSaveMsg('Error saving')
    }
    setSaving(false)
  }

  return (
    <div className="space-y-4 animate-fadeIn">
      <div className="grid grid-cols-2 gap-4">
        <EditField label="Hotel Name" value={form.hotel_name}    onChange={(v) => handleChange('hotel_name', v)} span={2} />
        <EditField label="Brand"      value={form.brand}         onChange={(v) => handleChange('brand', v)} />
        <EditField label="Brand Tier" value={form.brand_tier}    onChange={(v) => handleChange('brand_tier', v)} />
        <EditField label="City"       value={form.city}          onChange={(v) => handleChange('city', v)} />
        <EditField label="State"      value={form.state}         onChange={(v) => handleChange('state', v)} />
        <EditField label="Country"    value={form.country}       onChange={(v) => handleChange('country', v)} />
        <EditField label="Opening"    value={form.opening_date}  onChange={(v) => handleChange('opening_date', v)} />
        <EditField label="Rooms"      value={form.room_count}    onChange={(v) => handleChange('room_count', v)} />
        <EditField label="Mgmt Co."   value={form.management_company} onChange={(v) => handleChange('management_company', v)} span={2} />
        <EditField label="Developer"  value={form.developer}     onChange={(v) => handleChange('developer', v)} />
        <EditField label="Owner"      value={form.owner}         onChange={(v) => handleChange('owner', v)} />
      </div>

      <div className="flex items-center gap-3 pt-2">
        <button
          onClick={handleSave}
          disabled={saving}
          className="flex items-center gap-1.5 px-4 py-2.5 text-xs font-semibold bg-navy-900 text-white rounded-lg hover:bg-navy-800 transition disabled:opacity-50"
        >
          {saving ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Save className="w-3.5 h-3.5" />}
          Save Changes
        </button>
        {saveMsg && (
          <span className={cn('text-xs font-semibold', saveMsg.includes('Error') ? 'text-red-500' : 'text-emerald-600')}>
            {saveMsg}
          </span>
        )}
      </div>
    </div>
  )
}

function EditField({ label, value, onChange, span }: {
  label: string; value: string; onChange: (v: string) => void; span?: number
}) {
  return (
    <div className={span === 2 ? 'col-span-2' : ''}>
      <label className="field-label block mb-1">{label}</label>
      <input
        type="text"
        value={value}
        onChange={(e) => onChange(e.target.value)}
        className="w-full h-9 px-3 text-sm text-navy-900 bg-stone-50 border border-stone-200 rounded-lg outline-none focus:border-navy-400 focus:ring-1 focus:ring-navy-200 transition"
      />
    </div>
  )
}


/* ═══════════════════════════════════════════════════
   SOURCES TAB
   ═══════════════════════════════════════════════════ */

function SourcesTab({ lead }: { lead: Lead }) {
  const sourceList: string[] = []

  if (lead.source_url) {
    lead.source_url.split(',').map((s) => s.trim()).filter(Boolean).forEach((u) => {
      if (!sourceList.includes(u)) sourceList.push(u)
    })
  }
  if (lead.source_urls && Array.isArray(lead.source_urls)) {
    lead.source_urls.forEach((u) => {
      if (u && !sourceList.includes(u)) sourceList.push(u)
    })
  }
  if (lead.sources) {
    const arr = Array.isArray(lead.sources)
      ? lead.sources
      : typeof lead.sources === 'string'
        ? lead.sources.split(',').map((s) => s.trim()).filter(Boolean)
        : []
    arr.forEach((u: any) => {
      if (u && !sourceList.includes(u)) sourceList.push(u)
    })
  }

  return (
    <div className="space-y-5 animate-fadeIn">
      {lead.source_detail && typeof lead.source_detail === 'string' && (
        <Section title="Summary">
          <p className="text-sm text-stone-600 leading-relaxed">{lead.source_detail}</p>
        </Section>
      )}

      <Section title={`Source Articles (${sourceList.length})`}>
        {sourceList.length > 0 ? (
          <div className="space-y-2">
            {sourceList.map((url, i) => {
              const href = url.startsWith('http') ? url : `https://${url}`
              const extraction = (lead.source_extractions && typeof lead.source_extractions === 'object')
                ? (lead.source_extractions as Record<string, any>)[url] : null
              return (
                <div key={i} className="rounded-lg border border-stone-100 overflow-hidden">
                  <a
                    href={href}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="flex items-center gap-2.5 px-3.5 py-2.5 bg-stone-50 hover:bg-stone-100 transition group"
                  >
                    <Link2 className="w-4 h-4 text-stone-400 group-hover:text-navy-600 flex-shrink-0" />
                    <span className="text-xs text-navy-600 truncate group-hover:underline">{url}</span>
                    <ExternalLink className="w-3.5 h-3.5 text-stone-300 group-hover:text-navy-600 ml-auto flex-shrink-0" />
                  </a>
                  {extraction && typeof extraction === 'object' && Object.keys(extraction).length > 0 && (
                    <div className="px-3.5 py-2.5 border-t border-stone-100 grid grid-cols-2 gap-x-4 gap-y-1 text-xs">
                      {Object.entries(extraction).map(([k, v]) => {
                        if (k === 'key_insights') return null
                        if (v === null || v === undefined) return null
                        if (typeof v === 'object' && !Array.isArray(v)) return null
                        const display = Array.isArray(v) ? v.join(', ') : String(v)
                        if (!display) return null
                        return (
                          <div key={k} className="flex gap-2">
                            <span className="text-stone-400 capitalize whitespace-nowrap">{k.replace(/_/g, ' ')}:</span>
                            <span className="text-navy-800 font-medium truncate">{display}</span>
                          </div>
                        )
                      })}
                    </div>
                  )}
                </div>
              )
            })}
          </div>
        ) : (
          <p className="text-xs text-stone-400">No source URLs recorded</p>
        )}
      </Section>

      {lead.score_breakdown && typeof lead.score_breakdown === 'object' && (
        <Section title="Score Breakdown">
          <div className="space-y-1">
            {Object.entries(lead.score_breakdown).map(([key, val]) => {
              const obj = (val && typeof val === 'object') ? val as Record<string, any> : null
              const points = obj?.points !== undefined ? String(obj.points) : null
              const reason = (typeof obj?.reason === 'string') ? obj.reason
                           : (typeof obj?.label === 'string') ? obj.label
                           : (typeof obj?.detail === 'string') ? obj.detail
                           : null
              return (
                <div key={key} className="flex items-start justify-between gap-4 py-1.5 border-b border-stone-50 last:border-0">
                  <div className="min-w-0">
                    <span className="text-sm text-navy-800 font-medium capitalize">{key.replace(/_/g, ' ')}</span>
                    {reason && (
                      <p className="text-xs text-stone-400 mt-0.5">{reason}</p>
                    )}
                  </div>
                  <span className="text-sm font-bold text-navy-900 tabular-nums flex-shrink-0">
                    {points !== null ? `+${points}` : safe(val)}
                  </span>
                </div>
              )
            })}
          </div>
        </Section>
      )}
    </div>
  )
}


/* ═══════════════════════════════════════════════════
   KEY INSIGHTS — AI-generated bullet points
   ═══════════════════════════════════════════════════ */

function KeyInsights({ extractions }: { extractions: Record<string, any> }) {
  const insights: string[] = []
  for (const [_url, data] of Object.entries(extractions)) {
    if (!data || typeof data !== 'object') continue
    const ki = data.key_insights
    if (ki && typeof ki === 'string' && ki.length > 10) {
      if (ki.toLowerCase().includes('no specific details') || ki.toLowerCase().includes('no additional details')) continue
      if (!insights.includes(ki)) insights.push(ki)
    }
  }

  if (insights.length === 0) return null

  return (
    <div className="bg-slate-50 rounded-lg border border-slate-200/80 p-4">
      <ul className="space-y-2">
        {insights.map((insight, i) => (
          <li key={i} className="flex gap-2.5 text-[13px] leading-relaxed text-slate-700">
            <span className="text-slate-400 flex-shrink-0 mt-0.5">•</span>
            <span>{insight}</span>
          </li>
        ))}
      </ul>
    </div>
  )
}


/* ═══════════════════════════════════════════════════
   SHARED
   ═══════════════════════════════════════════════════ */

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <section>
      <h4 className="section-label">{title}</h4>
      {children}
    </section>
  )
}

function Field({ icon: Icon, label, value }: { icon: React.ElementType; label: string; value: string }) {
  return (
    <div className="flex items-start gap-2.5">
      <Icon className="w-4 h-4 text-stone-400 mt-0.5 flex-shrink-0" />
      <div className="min-w-0">
        <div className="field-label">{label}</div>
        <div className="text-sm text-navy-800 leading-snug font-medium">{safe(value)}</div>
      </div>
    </div>
  )
}
