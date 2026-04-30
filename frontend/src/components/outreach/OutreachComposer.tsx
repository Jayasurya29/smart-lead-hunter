import { useState, useEffect, useRef } from 'react'
import {
  Sparkles, X as XIcon, Search, MapPin, Loader2, ArrowLeft,
  Check, User, Mail, Linkedin, Star,
} from 'lucide-react'
import {
  searchHotelsForOutreach, getHotelContactsForOutreach,
  HotelSearchHit, HotelSummary, HotelContact,
} from '@/api/outreach'
import OutreachProgress from './OutreachProgress'

interface Props {
  onClose: () => void
  onComplete: (researchId: number) => void
  initialContactId?: number
  initialParentKind?: 'lead' | 'existing_hotel'
  initialParentId?: number
}

type Step = 'pick-hotel' | 'pick-contact' | 'review' | 'generating'

/**
 * Smart 3-step composer:
 *   pick-hotel    → autocomplete across BOTH leads + existing hotels
 *   pick-contact  → contacts from that hotel, sorted (primary, score)
 *   review        → all fields auto-populated, editable, click Generate
 *   generating    → SSE progress for the 5 LangGraph agents
 *
 * Pre-fill bypass: if launched with initialContactId (Sparkles button on
 * a contact card), skip directly to generating.
 */
export default function OutreachComposer({
  onClose,
  onComplete,
  initialContactId,
  initialParentKind,
  initialParentId,
}: Props) {
  const isPrefilled = !!initialContactId

  const [step, setStep] = useState<Step>(isPrefilled ? 'generating' : 'pick-hotel')
  const [search, setSearch] = useState('')
  const [hits, setHits] = useState<HotelSearchHit[]>([])
  const [searching, setSearching] = useState(false)
  const [selectedHotel, setSelectedHotel] = useState<HotelSummary | null>(null)
  const [contacts, setContacts] = useState<HotelContact[]>([])
  const [loadingContacts, setLoadingContacts] = useState(false)
  const [selectedContact, setSelectedContact] = useState<HotelContact | null>(null)
  const [form, setForm] = useState({
    contact_name: '',
    contact_title: '',
    hotel_name: '',
    hotel_location: '',
    email: '',
    linkedin_url: '',
  })
  const [errorMsg, setErrorMsg] = useState('')

  // Debounced hotel search
  const searchTimer = useRef<ReturnType<typeof setTimeout> | null>(null)
  useEffect(() => {
    if (step !== 'pick-hotel') return
    if (searchTimer.current) clearTimeout(searchTimer.current)
    if (!search || search.trim().length < 2) {
      setHits([])
      return
    }
    setSearching(true)
    searchTimer.current = setTimeout(async () => {
      try {
        const results = await searchHotelsForOutreach(search)
        setHits(results)
      } catch (e) {
        console.error('Hotel search failed', e)
      } finally {
        setSearching(false)
      }
    }, 250)
    return () => {
      if (searchTimer.current) clearTimeout(searchTimer.current)
    }
  }, [search, step])

  async function handlePickHotel(hit: HotelSearchHit) {
    setLoadingContacts(true)
    setStep('pick-contact')
    try {
      const data = await getHotelContactsForOutreach(hit.kind, hit.id)
      setSelectedHotel(data.hotel)
      setContacts(data.contacts)
    } catch (e) {
      console.error('Failed to load contacts', e)
      setErrorMsg('Failed to load contacts')
    } finally {
      setLoadingContacts(false)
    }
  }

  function handlePickContact(c: HotelContact) {
    if (!selectedHotel) return
    const loc = [selectedHotel.city, selectedHotel.state].filter(Boolean).join(', ')
    setSelectedContact(c)
    setForm({
      contact_name: c.name || '',
      contact_title: c.title || '',
      hotel_name: selectedHotel.hotel_name || '',
      hotel_location: loc,
      email: c.email || '',
      linkedin_url: c.linkedin || '',
    })
    setStep('review')
  }

  function startQuery(): string {
    const params = new URLSearchParams()
    if (isPrefilled && initialContactId) {
      params.set('contact_id', String(initialContactId))
      if (initialParentKind) params.set('parent_kind', initialParentKind)
      if (initialParentId) params.set('parent_id', String(initialParentId))
      return params.toString()
    }
    if (selectedContact) {
      params.set('contact_id', String(selectedContact.id))
      if (selectedHotel) {
        params.set('parent_kind', selectedHotel.kind)
        params.set('parent_id', String(selectedHotel.id))
      }
      return params.toString()
    }
    if (form.contact_name) params.set('contact_name', form.contact_name)
    if (form.contact_title) params.set('contact_title', form.contact_title)
    if (form.hotel_name) params.set('hotel_name', form.hotel_name)
    if (form.hotel_location) params.set('hotel_location', form.hotel_location)
    if (form.email) params.set('email', form.email)
    if (form.linkedin_url) params.set('linkedin_url', form.linkedin_url)
    return params.toString()
  }

  const canGenerate = isPrefilled || (form.contact_name.trim() && form.hotel_name.trim())

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-stone-900/50 backdrop-blur-sm"
      onClick={onClose}
    >
      <div
        className="bg-white rounded-2xl shadow-2xl w-full max-w-xl max-h-[90vh] overflow-hidden flex flex-col"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="px-6 pt-6 pb-4 border-b border-stone-100 flex items-center justify-between flex-shrink-0">
          <div className="flex items-center gap-2">
            <div className="w-9 h-9 rounded-lg bg-gradient-to-br from-purple-100 to-purple-200 flex items-center justify-center">
              <Sparkles className="w-4 h-4 text-purple-700" />
            </div>
            <div>
              <h2 className="text-base font-bold text-navy-900">New Outreach</h2>
              <p className="text-xs text-stone-500">
                {step === 'pick-hotel' && 'Search for a hotel to start'}
                {step === 'pick-contact' && 'Pick the contact you want to reach'}
                {step === 'review' && 'Review & generate'}
                {step === 'generating' && 'AI is working...'}
              </p>
            </div>
          </div>
          <button
            onClick={onClose}
            className="p-1.5 text-stone-400 hover:text-stone-600 rounded-lg hover:bg-stone-100"
          >
            <XIcon className="w-4 h-4" />
          </button>
        </div>

        {!isPrefilled && (
          <div className="px-6 py-3 border-b border-stone-100 flex items-center gap-2 text-xs">
            <StepDot active={step === 'pick-hotel'} done={step !== 'pick-hotel'} label="Hotel" />
            <div className="flex-1 h-0.5 bg-stone-200">
              <div className={`h-full bg-purple-600 transition-all ${
                step !== 'pick-hotel' ? 'w-full' : 'w-0'
              }`} />
            </div>
            <StepDot
              active={step === 'pick-contact'}
              done={step === 'review' || step === 'generating'}
              label="Contact"
            />
            <div className="flex-1 h-0.5 bg-stone-200">
              <div className={`h-full bg-purple-600 transition-all ${
                step === 'review' || step === 'generating' ? 'w-full' : 'w-0'
              }`} />
            </div>
            <StepDot
              active={step === 'review' || step === 'generating'}
              done={false}
              label="Generate"
            />
          </div>
        )}

        <div className="flex-1 overflow-y-auto p-6 min-h-[280px]">
          {step === 'pick-hotel' && (
            <PickHotelStep
              search={search}
              setSearch={setSearch}
              hits={hits}
              searching={searching}
              onPick={handlePickHotel}
            />
          )}

          {step === 'pick-contact' && (
            <PickContactStep
              hotel={selectedHotel}
              contacts={contacts}
              loading={loadingContacts}
              onPick={handlePickContact}
              onBack={() => {
                setStep('pick-hotel')
                setSelectedHotel(null)
                setContacts([])
              }}
            />
          )}

          {step === 'review' && (
            <ReviewStep
              form={form}
              setForm={setForm}
              selectedContact={selectedContact}
              selectedHotel={selectedHotel}
              onBackToContacts={() => setStep('pick-contact')}
            />
          )}

          {step === 'generating' && (
            <OutreachProgress
              query={startQuery()}
              onComplete={(id) => onComplete(id)}
              onError={(msg) => {
                setStep(isPrefilled ? 'review' : 'review')
                setErrorMsg(msg)
              }}
            />
          )}

          {errorMsg && step !== 'generating' && (
            <p className="mt-3 text-xs text-red-600">{errorMsg}</p>
          )}
        </div>

        {step === 'review' && (
          <div className="px-6 py-4 border-t border-stone-100 bg-stone-50 flex justify-between gap-2 flex-shrink-0">
            {!isPrefilled ? (
              <button
                onClick={() => setStep('pick-contact')}
                className="px-4 py-2 text-sm font-semibold text-stone-600 bg-white border border-stone-200 rounded-md hover:bg-stone-100 flex items-center gap-1.5"
              >
                <ArrowLeft className="w-3.5 h-3.5" />
                Back
              </button>
            ) : (
              <button
                onClick={onClose}
                className="px-4 py-2 text-sm font-semibold text-stone-600 bg-white border border-stone-200 rounded-md hover:bg-stone-100"
              >
                Cancel
              </button>
            )}
            <button
              onClick={() => {
                setErrorMsg('')
                setStep('generating')
              }}
              disabled={!canGenerate}
              className="px-4 py-2 text-sm font-semibold text-white bg-gradient-to-r from-purple-600 to-purple-700 rounded-md hover:from-purple-700 hover:to-purple-800 disabled:opacity-40 disabled:cursor-not-allowed flex items-center gap-1.5"
            >
              <Sparkles className="w-4 h-4" />
              Generate Outreach
            </button>
          </div>
        )}
      </div>
    </div>
  )
}

/* ─── Step 1 ─── */
function PickHotelStep({
  search, setSearch, hits, searching, onPick,
}: {
  search: string
  setSearch: (v: string) => void
  hits: HotelSearchHit[]
  searching: boolean
  onPick: (hit: HotelSearchHit) => void
}) {
  return (
    <div>
      <div className="relative mb-3">
        <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-stone-400" />
        <input
          type="text"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          autoFocus
          placeholder="Type hotel name or brand..."
          className="w-full pl-9 pr-3 py-2.5 text-sm bg-white border border-stone-300 rounded-lg focus:outline-none focus:border-purple-400 focus:ring-2 focus:ring-purple-100"
        />
      </div>

      {search.trim().length > 0 && search.trim().length < 2 && (
        <p className="text-xs text-stone-400 italic">Type at least 2 characters...</p>
      )}

      {searching && (
        <div className="flex items-center gap-2 text-xs text-stone-400 italic mb-2">
          <Loader2 className="w-3 h-3 animate-spin" />
          Searching...
        </div>
      )}

      {!searching && search.trim().length >= 2 && hits.length === 0 && (
        <div className="text-center py-8">
          <Search className="w-8 h-8 text-stone-200 mx-auto mb-2" />
          <p className="text-xs text-stone-500">No hotels match "{search}"</p>
        </div>
      )}

      <div className="space-y-1">
        {hits.map((hit) => (
          <button
            key={`${hit.kind}-${hit.id}`}
            onClick={() => onPick(hit)}
            className="w-full text-left px-3 py-2.5 rounded-lg border border-stone-200 hover:border-purple-300 hover:bg-purple-50/30 transition flex items-center gap-3"
          >
            <ScoreChip score={hit.score} />
            <div className="flex-1 min-w-0">
              <div className="flex items-center gap-2">
                <span className="text-sm font-semibold text-navy-900 truncate">{hit.hotel_name}</span>
                <KindPill kind={hit.kind} />
              </div>
              <div className="flex items-center gap-2 text-xs text-stone-500 mt-0.5">
                {hit.brand && <span>{hit.brand}</span>}
                {hit.brand && hit.location && <span>·</span>}
                {hit.location && (
                  <span className="flex items-center gap-1">
                    <MapPin className="w-3 h-3" /> {hit.location}
                  </span>
                )}
              </div>
            </div>
          </button>
        ))}
      </div>
    </div>
  )
}

/* ─── Step 2 ─── */
function PickContactStep({
  hotel, contacts, loading, onPick, onBack,
}: {
  hotel: HotelSummary | null
  contacts: HotelContact[]
  loading: boolean
  onPick: (c: HotelContact) => void
  onBack: () => void
}) {
  return (
    <div>
      <button
        onClick={onBack}
        className="text-xs text-stone-500 hover:text-purple-700 flex items-center gap-1 mb-3"
      >
        <ArrowLeft className="w-3 h-3" />
        Pick a different hotel
      </button>

      {hotel && (
        <div className="mb-4 p-3 bg-purple-50/50 border border-purple-100 rounded-lg">
          <div className="flex items-center gap-2 mb-1">
            <ScoreChip score={hotel.score} />
            <span className="text-sm font-bold text-navy-900">{hotel.hotel_name}</span>
          </div>
          <div className="text-xs text-stone-500">
            {[hotel.brand, hotel.city, hotel.state].filter(Boolean).join(' · ')}
          </div>
        </div>
      )}

      {loading && (
        <div className="flex items-center justify-center py-8 text-stone-400">
          <Loader2 className="w-5 h-5 animate-spin" />
        </div>
      )}

      {!loading && contacts.length === 0 && (
        <div className="text-center py-8">
          <User className="w-8 h-8 text-stone-200 mx-auto mb-2" />
          <p className="text-xs text-stone-500">
            No contacts saved for this hotel yet.
          </p>
          <p className="text-2xs text-stone-400 mt-1">
            Run contact enrichment to find decision-makers first.
          </p>
        </div>
      )}

      <div className="space-y-1.5">
        {contacts.map((c) => (
          <button
            key={c.id}
            onClick={() => onPick(c)}
            className="w-full text-left px-3 py-2.5 rounded-lg border border-stone-200 hover:border-purple-300 hover:bg-purple-50/30 transition"
          >
            <div className="flex items-start gap-3">
              <div className={`w-8 h-8 rounded-full flex items-center justify-center text-xs font-bold flex-shrink-0 ${
                c.is_primary ? 'bg-navy-700 text-white' : 'bg-stone-200 text-stone-600'
              }`}>
                {(c.name || '?')[0].toUpperCase()}
              </div>
              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-1.5">
                  <span className="text-sm font-semibold text-navy-900">{c.name}</span>
                  {c.is_primary && <Star className="w-3 h-3 text-gold-500 fill-gold-500" />}
                  <span className="ml-auto text-2xs font-bold text-stone-500">{c.score}</span>
                </div>
                <p className="text-xs text-stone-500 truncate">{c.title || '—'}</p>
                <div className="flex items-center gap-2 mt-1 text-2xs text-stone-400">
                  {c.email && (
                    <span className="flex items-center gap-1">
                      <Mail className="w-2.5 h-2.5" /> email
                    </span>
                  )}
                  {c.linkedin && (
                    <span className="flex items-center gap-1">
                      <Linkedin className="w-2.5 h-2.5" /> LinkedIn
                    </span>
                  )}
                </div>
              </div>
            </div>
          </button>
        ))}
      </div>
    </div>
  )
}

/* ─── Step 3 ─── */
function ReviewStep({
  form, setForm, selectedContact, selectedHotel, onBackToContacts,
}: {
  form: any
  setForm: (f: any) => void
  selectedContact: HotelContact | null
  selectedHotel: HotelSummary | null
  onBackToContacts: () => void
}) {
  return (
    <div className="space-y-3">
      {selectedContact && selectedHotel && (
        <div className="mb-2 p-3 bg-emerald-50/50 border border-emerald-100 rounded-lg flex items-center gap-2">
          <Check className="w-4 h-4 text-emerald-600 flex-shrink-0" />
          <div className="flex-1 min-w-0">
            <p className="text-xs font-semibold text-emerald-900">
              Auto-filled from {selectedContact.name}
            </p>
            <p className="text-2xs text-emerald-700">
              All fields below pulled from your saved contact data — edit if needed.
            </p>
          </div>
          <button
            onClick={onBackToContacts}
            className="text-xs text-purple-700 hover:underline flex-shrink-0"
          >
            Change
          </button>
        </div>
      )}

      <Field label="Contact Name *" value={form.contact_name} onChange={(v) => setForm({ ...form, contact_name: v })} />
      <Field label="Contact Title" value={form.contact_title} onChange={(v) => setForm({ ...form, contact_title: v })} />
      <Field label="Hotel Name *" value={form.hotel_name} onChange={(v) => setForm({ ...form, hotel_name: v })} />
      <Field label="Location" value={form.hotel_location} onChange={(v) => setForm({ ...form, hotel_location: v })} />
      <Field label="Contact Email" value={form.email} onChange={(v) => setForm({ ...form, email: v })} />
      <Field label="LinkedIn URL" value={form.linkedin_url} onChange={(v) => setForm({ ...form, linkedin_url: v })} />
    </div>
  )
}

/* ─── tiny shared ─── */

function StepDot({ active, done, label }: { active: boolean; done: boolean; label: string }) {
  return (
    <div className="flex items-center gap-1.5 flex-shrink-0">
      <div
        className={`w-5 h-5 rounded-full flex items-center justify-center text-2xs font-bold ${
          done
            ? 'bg-purple-600 text-white'
            : active
            ? 'bg-purple-100 text-purple-700 border-2 border-purple-600'
            : 'bg-stone-200 text-stone-500'
        }`}
      >
        {done ? <Check className="w-2.5 h-2.5" /> : ''}
      </div>
      <span className={`text-2xs font-semibold ${
        active ? 'text-purple-700' : done ? 'text-stone-600' : 'text-stone-400'
      }`}>
        {label}
      </span>
    </div>
  )
}

function ScoreChip({ score }: { score: number }) {
  const color =
    score >= 80 ? 'bg-emerald-500 text-white'
      : score >= 60 ? 'bg-amber-500 text-white'
      : score >= 40 ? 'bg-stone-500 text-white'
      : 'bg-red-400 text-white'
  return (
    <div className={`flex-shrink-0 w-9 h-9 rounded-md flex items-center justify-center text-xs font-bold ${color}`}>
      {score}
    </div>
  )
}

function KindPill({ kind }: { kind: 'lead' | 'existing_hotel' }) {
  return kind === 'lead' ? (
    <span className="inline-flex items-center px-1.5 py-0.5 text-2xs font-bold uppercase tracking-wider rounded bg-blue-100 text-blue-700">
      New
    </span>
  ) : (
    <span className="inline-flex items-center px-1.5 py-0.5 text-2xs font-bold uppercase tracking-wider rounded bg-emerald-100 text-emerald-700">
      Existing
    </span>
  )
}

function Field({
  label, value, onChange,
}: {
  label: string
  value: string
  onChange: (v: string) => void
}) {
  return (
    <div>
      <label className="block text-2xs font-bold uppercase tracking-wider text-stone-500 mb-1">
        {label}
      </label>
      <input
        type="text"
        value={value}
        onChange={(e) => onChange(e.target.value)}
        className="w-full px-3 py-2 text-sm border border-stone-300 rounded-md focus:outline-none focus:border-purple-400 focus:ring-2 focus:ring-purple-100"
      />
    </div>
  )
}
