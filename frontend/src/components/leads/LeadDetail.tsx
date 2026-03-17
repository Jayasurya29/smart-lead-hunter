import { useState } from 'react'
import { useLead, useContacts, useApproveLead, useRejectLead, useRestoreLead, useDeleteLead, useEnrichLead } from '@/hooks/useLeads'
import { editLead } from '@/api/leads'
import { cn, getTierLabel, getTierColor, getScoreBg, getTimelineLabel, getTimelineColor, formatLocation, formatDate, relativeDate } from '@/lib/utils'
import { X, CheckCircle2, XCircle, Undo2, Trash2, Sparkles, Loader2, MapPin, Calendar, Building2, User, Mail, Phone, Linkedin, ExternalLink, Save, Globe, Crown, Star, Hash, Clock } from 'lucide-react'
import { useQueryClient } from '@tanstack/react-query'

type DetailTab = 'overview' | 'contacts' | 'edit' | 'sources'

interface Props {
  leadId: number
  onClose: () => void
}

export default function LeadDetail({ leadId, onClose }: Props) {
  const [activeTab, setActiveTab] = useState<DetailTab>('overview')
  const { data: lead, isLoading } = useLead(leadId)
  const { data: contacts, isLoading: contactsLoading } = useContacts(leadId)
  const approveMut = useApproveLead()
  const rejectMut = useRejectLead()
  const restoreMut = useRestoreLead()
  const deleteMut = useDeleteLead()
  const enrichMut = useEnrichLead()

  if (isLoading || !lead) {
    return (
      <div className="bg-white rounded-lg border border-stone-200 shadow-sm overflow-hidden h-full">
        <div className="h-[120px] skeleton" />
        <div className="p-4 space-y-3">
          <div className="skeleton h-5 rounded w-3/4" />
          <div className="skeleton h-4 rounded w-1/2" />
          <div className="skeleton h-4 rounded w-2/3" />
        </div>
      </div>
    )
  }

  const isNew = lead.status === 'new'
  const isApproved = lead.status === 'approved'
  const isRejected = lead.status === 'rejected' || lead.status === 'deleted'
  const timeline = getTimelineLabel(lead.opening_date)
  const safeContacts = Array.isArray(contacts) ? contacts : []

  const TABS: { key: DetailTab; label: string; count?: number }[] = [
    { key: 'overview', label: 'Overview' },
    { key: 'contacts', label: 'Contacts', count: safeContacts.length },
    { key: 'edit', label: 'Edit' },
    { key: 'sources', label: 'Sources' },
  ]

  return (
    <div className="bg-white rounded-lg border border-stone-200 shadow-sm overflow-hidden h-full flex flex-col animate-slideIn">
      {/* ═══ HEADER ═══ */}
      <div className="px-4 py-3 bg-gradient-to-b from-white to-stone-50/50 border-b border-stone-200 flex-shrink-0">
        <div className="flex items-start justify-between mb-2.5">
          <div className="flex-1 min-w-0 pr-3">
            <h2 className="font-bold text-navy-900 text-[15px] leading-snug">{lead.hotel_name}</h2>
            <div className="flex items-center gap-1.5 mt-1.5 flex-wrap">
              {lead.brand_tier && (
                <span className={cn('text-[9px] px-1.5 py-[2px] rounded font-bold uppercase tracking-wide', getTierColor(lead.brand_tier))}>
                  {getTierLabel(lead.brand_tier)}
                </span>
              )}
              <span className={cn('text-[9px] px-1.5 py-[2px] rounded font-bold border uppercase tracking-wide', getTimelineColor(timeline))}>
                {timeline}
              </span>
              <span className={cn('inline-flex items-center justify-center px-2 py-[2px] rounded-md border-2 text-[11px] font-bold tabular-nums', getScoreBg(lead.lead_score))}>
                {lead.lead_score ?? '—'} pts
              </span>
              {lead.brand && <span className="text-[10px] text-stone-400 font-medium ml-1">{lead.brand}</span>}
            </div>
          </div>
          <button onClick={onClose} className="p-1 text-stone-400 hover:text-stone-600 rounded-md hover:bg-stone-100 transition">
            <X className="w-4 h-4" />
          </button>
        </div>

        {/* Actions */}
        <div className="flex gap-1.5 flex-wrap">
          <DetailBtn onClick={() => enrichMut.mutate(leadId)} loading={enrichMut.isPending} icon={Sparkles} label="Enrich" color="violet" />
          {isNew && (
            <>
              <DetailBtn onClick={() => approveMut.mutate(leadId)} loading={approveMut.isPending} icon={CheckCircle2} label="Approve" color="emerald" />
              <DetailBtn onClick={() => rejectMut.mutate({ id: leadId })} loading={rejectMut.isPending} icon={XCircle} label="Reject" color="red" />
            </>
          )}
          {isApproved && <DetailBtn onClick={() => rejectMut.mutate({ id: leadId })} loading={rejectMut.isPending} icon={XCircle} label="Reject" color="red" />}
          {isRejected && <DetailBtn onClick={() => restoreMut.mutate(leadId)} loading={restoreMut.isPending} icon={Undo2} label="Restore" color="blue" />}
        </div>
      </div>

      {/* ═══ TABS ═══ */}
      <div className="flex border-b border-stone-200 flex-shrink-0 bg-white">
        {TABS.map(t => (
          <button
            key={t.key}
            onClick={() => setActiveTab(t.key)}
            className={cn(
              'flex-1 px-3 py-2.5 text-[10px] font-bold uppercase tracking-[0.08em] transition-all duration-150 relative',
              activeTab === t.key ? 'text-navy-800 tab-active' : 'text-stone-400 hover:text-stone-600'
            )}
          >
            {t.label}
            {t.count !== undefined && t.count > 0 && (
              <span className={cn(
                'ml-1 text-[9px] px-1.5 py-px rounded-full',
                activeTab === t.key ? 'bg-navy-100 text-navy-700' : 'bg-stone-100 text-stone-500'
              )}>{t.count}</span>
            )}
          </button>
        ))}
      </div>

      {/* ═══ CONTENT ═══ */}
      <div className="flex-1 overflow-y-auto">
        {activeTab === 'overview' && <OverviewTab lead={lead} />}
        {activeTab === 'contacts' && <ContactsTab contacts={safeContacts} isLoading={contactsLoading} />}
        {activeTab === 'edit' && <EditTab lead={lead} />}
        {activeTab === 'sources' && <SourcesTab lead={lead} />}
      </div>
    </div>
  )
}

function DetailBtn({ onClick, loading, icon: Icon, label, color }: { onClick: () => void; loading: boolean; icon: any; label: string; color: string }) {
  const styles: Record<string, string> = {
    violet: 'border-violet-200 text-violet-700 bg-violet-50/50 hover:bg-violet-100',
    emerald: 'border-emerald-200 text-emerald-700 bg-emerald-50/50 hover:bg-emerald-100',
    red: 'border-red-200 text-red-600 bg-red-50/50 hover:bg-red-100',
    blue: 'border-blue-200 text-blue-700 bg-blue-50/50 hover:bg-blue-100',
  }
  return (
    <button
      onClick={onClick}
      disabled={loading}
      className={cn('flex items-center gap-1 px-2 py-1 text-[10px] font-bold rounded-md border transition-all duration-100 disabled:opacity-50 active:scale-[0.97]', styles[color])}
    >
      {loading ? <Loader2 className="w-3 h-3 animate-spin" /> : <Icon className="w-3 h-3" />}
      {label}
    </button>
  )
}

// ════════════════ OVERVIEW ════════════════

function OverviewTab({ lead }: { lead: any }) {
  return (
    <div className="p-4 space-y-4 animate-fadeIn">
      <Section title="Property Details">
        <div className="grid grid-cols-2 gap-2.5">
          <Field icon={MapPin} label="Location" value={formatLocation(lead.city, lead.state, lead.country)} />
          <Field icon={Calendar} label="Opening" value={lead.opening_date || '—'} />
          <Field icon={Building2} label="Rooms" value={lead.room_count ? `${lead.room_count} rooms` : '—'} />
          <Field icon={Building2} label="Type" value={lead.hotel_type || '—'} />
          {lead.brand && <Field icon={Globe} label="Brand" value={lead.brand} />}
          {lead.management_company && <Field icon={Building2} label="Management" value={lead.management_company} />}
          {lead.developer && <Field icon={Building2} label="Developer" value={lead.developer} />}
          {lead.owner && <Field icon={Building2} label="Owner" value={lead.owner} />}
        </div>
      </Section>

      {lead.key_insights && (
        <Section title="Key Insights">
          <div className="bg-gold-50/50 border border-gold-200/50 rounded-md p-3">
            <p className="text-[12px] text-navy-800 leading-relaxed whitespace-pre-line">{lead.key_insights}</p>
          </div>
        </Section>
      )}

      {lead.score_breakdown && typeof lead.score_breakdown === 'object' && Object.keys(lead.score_breakdown).length > 0 && (
        <Section title="Score Breakdown">
          <div className="space-y-1.5">
            {Object.entries(lead.score_breakdown).map(([key, val]) => (
              <div key={key} className="flex items-center justify-between">
                <span className="text-[11px] text-stone-500 capitalize">{key.replace(/_/g, ' ')}</span>
                <span className="text-[11px] font-bold text-navy-800 tabular-nums">{String(val)}</span>
              </div>
            ))}
          </div>
        </Section>
      )}

      <div className="text-[10px] text-stone-400 space-y-0.5 pt-1 border-t border-stone-100">
        <div className="flex items-center gap-1"><Clock className="w-3 h-3" /> Added {formatDate(lead.created_at)}</div>
        {lead.source_site && <div className="flex items-center gap-1"><Globe className="w-3 h-3" /> {lead.source_site}</div>}
        {lead.insightly_id && <div className="flex items-center gap-1"><Hash className="w-3 h-3" /> Insightly #{lead.insightly_id}</div>}
      </div>
    </div>
  )
}

// ════════════════ CONTACTS ════════════════

function ContactsTab({ contacts, isLoading }: { contacts: any[]; isLoading: boolean }) {
  if (isLoading) return <div className="p-4 space-y-2">{[1,2,3].map(i => <div key={i} className="skeleton h-20 rounded-lg" />)}</div>

  if (contacts.length === 0) {
    return (
      <div className="p-4 text-center py-16 animate-fadeIn">
        <User className="w-8 h-8 mx-auto text-stone-300 mb-2" />
        <p className="text-sm font-semibold text-stone-500">No contacts yet</p>
        <p className="text-[11px] text-stone-400 mt-1">Click "Enrich" to discover contacts for this hotel.</p>
      </div>
    )
  }

  return (
    <div className="p-3 space-y-2 animate-fadeIn">
      {contacts.map((c, i) => (
        <div
          key={c.id}
          className={cn(
            'rounded-lg border p-3 transition-all duration-150 animate-slideUp',
            c.is_primary ? 'border-navy-200 bg-navy-50/30' : 'border-stone-100 bg-white hover:border-stone-200 hover:shadow-sm',
          )}
          style={{ animationDelay: `${i * 0.05}s`, animationFillMode: 'both' }}
        >
          <div className="flex items-start gap-3">
            <div className={cn(
              'w-9 h-9 rounded-full flex items-center justify-center text-[11px] font-bold flex-shrink-0',
              c.is_primary ? 'bg-navy-100 text-navy-700' : 'bg-stone-100 text-stone-600',
            )}>
              {c.name.split(' ').map((n: string) => n[0]).join('').slice(0, 2).toUpperCase()}
            </div>
            <div className="flex-1 min-w-0">
              <div className="flex items-center gap-1.5 flex-wrap">
                <span className="font-bold text-[12px] text-navy-900 truncate">{c.name}</span>
                {c.is_primary && (
                  <span className="flex items-center gap-0.5 text-[8px] bg-navy-100 text-navy-700 px-1.5 py-px rounded-full font-bold uppercase tracking-wider">
                    <Crown className="w-2.5 h-2.5" />Primary
                  </span>
                )}
                {c.confidence === 'high' && (
                  <span className="text-[8px] bg-emerald-100 text-emerald-700 px-1.5 py-px rounded-full font-bold">HIGH</span>
                )}
                {c.scope && (
                  <span className={cn(
                    'text-[8px] px-1.5 py-px rounded-full font-bold',
                    c.scope === 'hotel_specific' ? 'bg-emerald-50 text-emerald-700' : 'bg-stone-100 text-stone-500',
                  )}>
                    {c.scope === 'hotel_specific' ? 'PROPERTY' : 'CHAIN'}
                  </span>
                )}
              </div>
              <div className="text-[11px] text-stone-500 truncate mt-0.5 font-medium">{c.title || 'No title'}</div>
              {c.organization && <div className="text-[10px] text-stone-400 truncate">{c.organization}</div>}
              <div className="flex items-center gap-3 mt-2">
                {c.email && (
                  <a href={`mailto:${c.email}`} className="flex items-center gap-1 text-[10px] text-emerald-600 hover:underline font-medium" onClick={e => e.stopPropagation()}>
                    <Mail className="w-3 h-3" />{c.email}
                  </a>
                )}
                {c.phone && (
                  <a href={`tel:${c.phone}`} className="flex items-center gap-1 text-[10px] text-blue-600 hover:underline font-medium" onClick={e => e.stopPropagation()}>
                    <Phone className="w-3 h-3" />{c.phone}
                  </a>
                )}
                {c.linkedin && (
                  <a href={c.linkedin} target="_blank" rel="noopener noreferrer" className="flex items-center gap-1 text-[10px] text-blue-700 hover:underline font-medium" onClick={e => e.stopPropagation()}>
                    <Linkedin className="w-3 h-3" />LinkedIn
                  </a>
                )}
              </div>
            </div>
          </div>
        </div>
      ))}
    </div>
  )
}

// ════════════════ EDIT ════════════════

function EditTab({ lead }: { lead: any }) {
  const qc = useQueryClient()
  const [saving, setSaving] = useState(false)
  const [saved, setSaved] = useState(false)
  const [form, setForm] = useState({
    hotel_name: lead.hotel_name || '', brand: lead.brand || '',
    city: lead.city || '', state: lead.state || '', country: lead.country || '',
    opening_date: lead.opening_date || '', room_count: lead.room_count || '',
    hotel_type: lead.hotel_type || '', management_company: lead.management_company || '',
    developer: lead.developer || '', owner: lead.owner || '', notes: lead.notes || '',
  })

  function set(k: string, v: string) { setForm(p => ({ ...p, [k]: v })); setSaved(false) }

  async function handleSave() {
    setSaving(true)
    try {
      await editLead(lead.id, { ...form, room_count: form.room_count ? Number(form.room_count) : null })
      qc.invalidateQueries({ queryKey: ['lead', lead.id] })
      qc.invalidateQueries({ queryKey: ['leads'] })
      setSaved(true)
    } catch (e) { console.error('Save failed', e) }
    setSaving(false)
  }

  const fields: { key: string; label: string; full?: boolean; type?: string }[] = [
    { key: 'hotel_name', label: 'Hotel Name', full: true },
    { key: 'brand', label: 'Brand', full: true },
    { key: 'city', label: 'City' }, { key: 'state', label: 'State' },
    { key: 'country', label: 'Country' }, { key: 'opening_date', label: 'Opening Date' },
    { key: 'room_count', label: 'Room Count', type: 'number' }, { key: 'hotel_type', label: 'Hotel Type' },
    { key: 'management_company', label: 'Management Co.', full: true },
    { key: 'developer', label: 'Developer', full: true },
    { key: 'owner', label: 'Owner', full: true },
  ]

  return (
    <div className="p-4 animate-fadeIn">
      <div className="grid grid-cols-2 gap-2.5">
        {fields.map(f => (
          <div key={f.key} className={f.full ? 'col-span-2' : ''}>
            <label className="text-[9px] text-stone-400 font-bold uppercase tracking-[0.1em] block mb-1">{f.label}</label>
            <input
              type={f.type || 'text'}
              value={(form as any)[f.key]}
              onChange={e => set(f.key, e.target.value)}
              className="w-full px-2.5 py-1.5 text-[12px] border-2 border-stone-200 rounded-md focus:border-navy-400 focus:ring-0 outline-none transition-colors bg-white text-navy-900"
            />
          </div>
        ))}
        <div className="col-span-2">
          <label className="text-[9px] text-stone-400 font-bold uppercase tracking-[0.1em] block mb-1">Notes</label>
          <textarea
            value={form.notes}
            onChange={e => set('notes', e.target.value)}
            rows={3}
            className="w-full px-2.5 py-1.5 text-[12px] border-2 border-stone-200 rounded-md focus:border-navy-400 focus:ring-0 outline-none transition-colors bg-white text-navy-900 resize-none"
          />
        </div>
      </div>
      <div className="flex items-center gap-2 mt-3">
        <button
          onClick={handleSave}
          disabled={saving}
          className="flex items-center gap-1.5 px-4 py-1.5 text-[11px] font-bold rounded-md bg-navy-900 text-white hover:bg-navy-800 transition-all disabled:opacity-50 active:scale-[0.97]"
        >
          {saving ? <Loader2 className="w-3 h-3 animate-spin" /> : <Save className="w-3 h-3" />}
          Save Changes
        </button>
        {saved && <span className="text-[11px] text-emerald-600 font-bold animate-fadeIn">Saved!</span>}
      </div>
    </div>
  )
}

// ════════════════ SOURCES ════════════════

function SourcesTab({ lead }: { lead: any }) {
  return (
    <div className="p-4 space-y-4 animate-fadeIn">
      {lead.source_url && (
        <Section title="Source Article">
          <a
            href={lead.source_url}
            target="_blank"
            rel="noopener noreferrer"
            className="flex items-center gap-3 p-3 bg-stone-50 rounded-lg border border-stone-200 hover:border-navy-300 hover:bg-navy-50/20 transition-all duration-150 group"
          >
            <ExternalLink className="w-4 h-4 text-navy-400 flex-shrink-0 group-hover:text-navy-600" />
            <div className="flex-1 min-w-0">
              <div className="text-[12px] font-semibold text-navy-700 truncate group-hover:underline">{lead.source_url}</div>
              {lead.source_site && <div className="text-[10px] text-stone-400 mt-0.5">{lead.source_site}</div>}
            </div>
          </a>
        </Section>
      )}

      {lead.hotel_website && (
        <Section title="Hotel Website">
          <a href={lead.hotel_website} target="_blank" rel="noopener noreferrer" className="flex items-center gap-2 text-[12px] text-navy-600 hover:underline font-medium">
            <Globe className="w-3.5 h-3.5" />{lead.hotel_website}
          </a>
        </Section>
      )}

      <Section title="Metadata">
        <div className="space-y-1.5 text-[11px]">
          {[
            ['Lead ID', lead.id],
            ['Status', lead.status],
            ['Created', formatDate(lead.created_at)],
            lead.updated_at && ['Updated', formatDate(lead.updated_at)],
            lead.insightly_id && ['Insightly', `#${lead.insightly_id}`],
            lead.rejection_reason && ['Rejection', lead.rejection_reason],
          ].filter(Boolean).map((row: any) => (
            <div key={row[0]} className="flex justify-between">
              <span className="text-stone-400 font-medium">{row[0]}</span>
              <span className="text-navy-700 font-semibold capitalize">{row[1]}</span>
            </div>
          ))}
        </div>
      </Section>
    </div>
  )
}

// ════════════════ SHARED ════════════════

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <section>
      <h4 className="text-[9px] font-bold text-stone-400 uppercase tracking-[0.1em] mb-2">{title}</h4>
      {children}
    </section>
  )
}

function Field({ icon: Icon, label, value }: { icon: React.ElementType; label: string; value: string }) {
  return (
    <div className="flex items-start gap-2">
      <Icon className="w-3.5 h-3.5 text-stone-400 mt-0.5 flex-shrink-0" />
      <div className="min-w-0">
        <div className="text-[9px] text-stone-400 uppercase font-bold tracking-wider">{label}</div>
        <div className="text-[12px] text-navy-800 leading-snug font-medium">{value}</div>
      </div>
    </div>
  )
}
