import { useState } from 'react'
import { useLead, useContacts, useApproveLead, useRejectLead, useRestoreLead, useDeleteLead, useEnrichLead } from '@/hooks/useLeads'
import { editLead, saveContact, setPrimaryContact } from '@/api/leads'
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
  Link2,
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
  tab: 'pipeline' | 'approved' | 'rejected' | 'deleted'
  onClose: () => void
}

type DetailTab = 'overview' | 'contacts' | 'edit' | 'sources'

export default function LeadDetail({ leadId, tab, onClose }: Props) {
  const { data: lead, isLoading } = useLead(leadId)
  const { data: contacts, isLoading: contactsLoading } = useContacts(leadId)
  const [activeTab, setActiveTab] = useState<DetailTab>('overview')
  const qc = useQueryClient()

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
      {/* Header */}
      <div className="px-5 pt-5 pb-4 flex-shrink-0 border-b border-stone-100">
        <div className="flex items-start justify-between gap-3">
          <div className="min-w-0 flex-1">
            <h2 className="text-lg font-bold text-navy-900 leading-snug truncate">
              {lead.hotel_name || lead.name}
            </h2>
            {lead.brand_name && (
              <p className="text-sm text-stone-400 mt-0.5">{lead.brand_name}</p>
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

        {/* Badges */}
        <div className="flex items-center gap-2 mt-2.5 flex-wrap">
          {lead.brand_tier && (
            <span className={cn('inline-flex px-2 py-0.5 rounded text-xs font-bold', getTierColor(lead.brand_tier))}>
              {getTierShort(lead.brand_tier)} — {getTierLabel(lead.brand_tier)}
            </span>
          )}
          <span className={cn('inline-flex px-2 py-0.5 rounded text-xs font-bold', getTimelineColor(timeline))}>
            {timeline}
          </span>
        </div>

        {/* Action buttons */}
        <div className="flex items-center gap-2 mt-3">
          {isNew && (
            <>
              <button
                onClick={() => approveMut.mutate(leadId)}
                disabled={approveMut.isPending}
                className="flex items-center gap-1.5 px-3.5 py-2 text-xs font-semibold rounded-lg bg-emerald-600 text-white hover:bg-emerald-700 transition disabled:opacity-50"
              >
                {approveMut.isPending ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <CheckCircle2 className="w-3.5 h-3.5" />}
                Approve
              </button>
              <button
                onClick={() => rejectMut.mutate({ id: leadId })}
                disabled={rejectMut.isPending}
                className="flex items-center gap-1.5 px-3.5 py-2 text-xs font-semibold rounded-lg border border-red-200 text-red-600 hover:bg-red-50 transition disabled:opacity-50"
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
              className="flex items-center gap-1.5 px-3.5 py-2 text-xs font-semibold rounded-lg border border-red-200 text-red-600 hover:bg-red-50 transition disabled:opacity-50"
            >
              {rejectMut.isPending ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <XCircle className="w-3.5 h-3.5" />}
              Reject
            </button>
          )}
          {isRejected && (
            <button
              onClick={() => restoreMut.mutate(leadId)}
              disabled={restoreMut.isPending}
              className="flex items-center gap-1.5 px-3.5 py-2 text-xs font-semibold rounded-lg border border-blue-200 text-blue-600 hover:bg-blue-50 transition disabled:opacity-50"
            >
              {restoreMut.isPending ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Undo2 className="w-3.5 h-3.5" />}
              Restore
            </button>
          )}
        </div>
      </div>

      {/* Tabs */}
      <div className="flex border-b border-stone-100 px-5 flex-shrink-0">
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

      {/* Tab content */}
      <div className="flex-1 overflow-y-auto p-5">
        {activeTab === 'overview'  && <OverviewTab lead={lead} contactList={contactList} onEnrich={() => enrichMut.mutate(leadId)} enriching={enrichMut.isPending} />}
        {activeTab === 'contacts'  && <ContactsTab contacts={contactList} loading={contactsLoading} leadId={leadId} onEnrich={() => enrichMut.mutate(leadId)} enriching={enrichMut.isPending} />}
        {activeTab === 'edit'      && <EditTab lead={lead} leadId={leadId} />}
        {activeTab === 'sources'   && <SourcesTab lead={lead} />}
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
      <Section title="Details">
        <div className="grid grid-cols-2 gap-4">
          <Field icon={Calendar}  label="Opening"    value={formatOpening(lead)} />
          <Field icon={MapPin}    label="Location"   value={formatLocation(lead)} />
          <Field icon={Building2} label="Rooms"      value={lead.rooms ? `${lead.rooms} rooms` : '—'} />
          <Field icon={Layers}    label="Brand Tier"  value={getTierLabel(lead.brand_tier)} />
          {lead.management_company && <Field icon={Building2} label="Mgmt Co."   value={lead.management_company} />}
          {lead.developer         && <Field icon={Building2} label="Developer"   value={lead.developer} />}
          {lead.owner             && <Field icon={User}      label="Owner"       value={lead.owner} />}
        </div>
      </Section>

      {lead.hotel_website && (
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

      {lead.source_extractions && typeof lead.source_extractions === 'object' && Object.keys(lead.source_extractions).length > 0 && (
        <Section title={`Key Insights (${Object.keys(lead.source_extractions).length} sources)`}>
          <KeyInsights extractions={lead.source_extractions as Record<string, any>} />
        </Section>
      )}

      <Section title="Primary Contact">
        {contactList.length > 0 ? (
          <div className="bg-navy-50/40 rounded-lg p-3.5 border border-navy-100/50">
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
   CONTACTS TAB
   ═══════════════════════════════════════════════════ */

function ContactsTab({ contacts, loading, leadId, onEnrich, enriching }: {
  contacts: Contact[]; loading: boolean; leadId: number; onEnrich: () => void; enriching: boolean
}) {
  const qc = useQueryClient()

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

  return (
    <div className="space-y-2.5 animate-fadeIn">
      {contacts.map((c) => (
        <div
          key={c.id}
          className={cn(
            'rounded-lg border p-4 transition',
            c.is_primary
              ? 'border-navy-200 bg-navy-50/30'
              : 'border-stone-100 hover:border-stone-200',
          )}
        >
          <div className="flex items-start gap-3">
            <div className={cn(
              'w-9 h-9 rounded-full flex items-center justify-center flex-shrink-0 text-sm font-bold',
              c.is_primary ? 'bg-navy-600 text-white' : 'bg-stone-200 text-stone-600',
            )}>
              {(c.name || '?')[0].toUpperCase()}
            </div>

            <div className="flex-1 min-w-0">
              {/* Name + Score */}
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
              {/* Title */}
              {c.title && <p className="text-xs text-stone-500 mt-0.5">{c.title}</p>}
              {/* Organization */}
              {c.organization && <p className="text-xs text-stone-400 mt-0.5">{c.organization}</p>}

              {/* Contact links */}
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

              {/* Scope badge + Source detail */}
              {(c.scope || c.source_detail) && (
                <div className="flex items-start gap-2 mt-2.5 flex-wrap">
                  {c.scope && (
                    <span className={cn(
                      'text-2xs font-bold px-2 py-0.5 rounded-full uppercase flex-shrink-0',
                      c.scope === 'hotel_specific' ? 'bg-emerald-50 text-emerald-600' : 'bg-stone-100 text-stone-500',
                    )}>
                      {c.scope === 'hotel_specific' ? 'Hotel Specific' : c.scope === 'chain_area' ? 'Chain/Area' : c.scope.replace(/_/g, ' ')}
                    </span>
                  )}
                  {c.source_detail && typeof c.source_detail === 'string' && (
                    <span className="text-xs text-stone-500">{c.source_detail}</span>
                  )}
                </div>
              )}

              {/* Evidence URL */}
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

              {/* Actions row */}
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
    brand_name:         lead.brand_name || '',
    brand_tier:         lead.brand_tier || '',
    city:               lead.city || '',
    state:              lead.state || '',
    country:            lead.country || '',
    opening_date:       lead.opening_date || '',
    rooms:              lead.rooms ? String(lead.rooms) : '',
    management_company: lead.management_company || '',
    developer:          lead.developer || '',
    owner:              lead.owner || '',
    hotel_website:      lead.hotel_website || '',
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
        rooms: form.rooms ? Number(form.rooms) : undefined,
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
        <EditField label="Brand"      value={form.brand_name}    onChange={(v) => handleChange('brand_name', v)} />
        <EditField label="Brand Tier" value={form.brand_tier}    onChange={(v) => handleChange('brand_tier', v)} />
        <EditField label="City"       value={form.city}          onChange={(v) => handleChange('city', v)} />
        <EditField label="State"      value={form.state}         onChange={(v) => handleChange('state', v)} />
        <EditField label="Country"    value={form.country}       onChange={(v) => handleChange('country', v)} />
        <EditField label="Opening"    value={form.opening_date}  onChange={(v) => handleChange('opening_date', v)} />
        <EditField label="Rooms"      value={form.rooms}         onChange={(v) => handleChange('rooms', v)} />
        <EditField label="Mgmt Co."   value={form.management_company} onChange={(v) => handleChange('management_company', v)} />
        <EditField label="Developer"  value={form.developer}     onChange={(v) => handleChange('developer', v)} />
        <EditField label="Owner"      value={form.owner}         onChange={(v) => handleChange('owner', v)} />
        <EditField label="Website"    value={form.hotel_website}  onChange={(v) => handleChange('hotel_website', v)} span={2} />
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
  // Collect all source URLs from every possible field
  const sourceList: string[] = []

  // source_url — single string, may contain commas
  if (lead.source_url) {
    lead.source_url.split(',').map((s) => s.trim()).filter(Boolean).forEach((u) => {
      if (!sourceList.includes(u)) sourceList.push(u)
    })
  }
  // source_urls — array
  if (lead.source_urls && Array.isArray(lead.source_urls)) {
    lead.source_urls.forEach((u) => {
      if (u && !sourceList.includes(u)) sourceList.push(u)
    })
  }
  // sources — array or comma string
  if (lead.sources) {
    const arr = Array.isArray(lead.sources)
      ? lead.sources
      : typeof lead.sources === 'string'
        ? lead.sources.split(',').map((s) => s.trim()).filter(Boolean)
        : []
    arr.forEach((u) => {
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
   KEY INSIGHTS — merges best data from all source extractions
   ═══════════════════════════════════════════════════ */

function KeyInsights({ extractions }: { extractions: Record<string, any> }) {
  const merged: Record<string, string> = {}
  const skipFields = new Set(['key_insights', 'confidence', 'relevance_score', 'extraction_date'])

  for (const [_url, data] of Object.entries(extractions)) {
    if (!data || typeof data !== 'object') continue
    for (const [key, val] of Object.entries(data)) {
      if (skipFields.has(key)) continue
      if (val === null || val === undefined) continue
      if (typeof val === 'object' && !Array.isArray(val)) continue
      const display = Array.isArray(val) ? val.join(', ') : String(val)
      if (!display) continue
      if (!merged[key] || display.length > merged[key].length) {
        merged[key] = display
      }
    }
  }

  const entries = Object.entries(merged).filter(([_, v]) => v)
  if (entries.length === 0) return null

  return (
    <div className="bg-navy-50/30 rounded-lg border border-navy-100/50 p-4">
      <div className="grid grid-cols-2 gap-x-6 gap-y-2.5">
        {entries.map(([key, value]) => (
          <div key={key}>
            <span className="field-label block">{key.replace(/_/g, ' ')}</span>
            <span className="text-sm text-navy-800 font-medium">{value}</span>
          </div>
        ))}
      </div>
      <p className="text-2xs text-stone-400 mt-3">
        Combined from {Object.keys(extractions).length} source{Object.keys(extractions).length !== 1 ? 's' : ''} — best available data per field
      </p>
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
