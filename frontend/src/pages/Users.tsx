import { useState, useEffect } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import api from '@/api/client'
import {
  Users as UsersIcon, Shield, ShieldCheck, Eye,
  Plus, Loader2, Check, X, Pencil, UserMinus, UserPlus,
  Mail, Lock, ChevronDown,
} from 'lucide-react'
import { cn } from '@/lib/utils'

interface UserRow {
  id: number
  email: string
  first_name: string
  last_name: string
  role: string
  is_active: boolean
  last_login: string | null
  created_at: string
}

const ROLE_CONFIG: Record<string, { label: string; color: string; icon: React.ElementType; desc: string }> = {
  admin:   { label: 'Admin',   color: 'bg-gold-50 text-gold-600 border-gold-200',     icon: Shield,      desc: 'Full access — manage users, approve leads, run scrapes' },
  sales:   { label: 'Sales',   color: 'bg-navy-50 text-navy-600 border-navy-200',     icon: ShieldCheck, desc: 'View leads, approve/reject, push to CRM' },
}

function formatDate(iso: string | null): string {
  if (!iso) return 'Never'
  const d = new Date(iso)
  return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })
}

function relativeTime(iso: string | null): string {
  if (!iso) return 'Never'
  const d = new Date(iso)
  const now = new Date()
  const diffMs = now.getTime() - d.getTime()
  const mins = Math.floor(diffMs / 60000)
  if (mins < 1) return 'Just now'
  if (mins < 60) return `${mins}m ago`
  const hrs = Math.floor(mins / 60)
  if (hrs < 24) return `${hrs}h ago`
  const days = Math.floor(hrs / 24)
  if (days < 7) return `${days}d ago`
  return formatDate(iso)
}

export default function UsersPage() {
  const qc = useQueryClient()
  const [showInvite, setShowInvite] = useState(false)
  const [editingId, setEditingId] = useState<number | null>(null)
  const [saving, setSaving] = useState(false)
  const [message, setMessage] = useState<{ text: string; type: 'success' | 'error' } | null>(null)

  const { data: users = [], isLoading } = useQuery<UserRow[]>({
    queryKey: ['users'],
    queryFn: async () => {
      const { data } = await api.get('/auth/users')
      return data
    },
  })

  // Clear message after 4s
  useEffect(() => {
    if (message) {
      const t = setTimeout(() => setMessage(null), 4000)
      return () => clearTimeout(t)
    }
  }, [message])

  async function handleRoleChange(userId: number, newRole: string) {
    setSaving(true)
    try {
      await api.patch(`/auth/users/${userId}`, { role: newRole })
      qc.invalidateQueries({ queryKey: ['users'] })
      setMessage({ text: 'Role updated', type: 'success' })
      setEditingId(null)
    } catch (e: any) {
      setMessage({ text: e?.response?.data?.detail || 'Failed to update role', type: 'error' })
    }
    setSaving(false)
  }

  async function handleToggleActive(userId: number, currentActive: boolean) {
    const action = currentActive ? 'deactivate' : 'reactivate'
    if (!window.confirm(`Are you sure you want to ${action} this user?`)) return
    setSaving(true)
    try {
      await api.patch(`/auth/users/${userId}`, { is_active: !currentActive })
      qc.invalidateQueries({ queryKey: ['users'] })
      setMessage({ text: `User ${action}d`, type: 'success' })
    } catch (e: any) {
      setMessage({ text: e?.response?.data?.detail || `Failed to ${action}`, type: 'error' })
    }
    setSaving(false)
  }

  const activeUsers = users.filter(u => u.is_active)
  const inactiveUsers = users.filter(u => !u.is_active)

  return (
    <div className="h-full overflow-y-auto">
      <div className="max-w-4xl mx-auto px-6 py-8">
        {/* Header */}
        <div className="flex items-center justify-between mb-8">
          <div>
            <h1 className="text-2xl font-bold text-navy-900">Team</h1>
            <p className="text-sm text-stone-400 mt-1">
              {activeUsers.length} active user{activeUsers.length !== 1 ? 's' : ''}
            </p>
          </div>
          <div />
        </div>

        {/* Status message */}
        {message && (
          <div className={cn(
            'mb-4 px-4 py-2.5 rounded-lg text-sm font-medium animate-fadeIn',
            message.type === 'success' ? 'bg-emerald-50 text-emerald-700 border border-emerald-200' : 'bg-red-50 text-red-700 border border-red-200',
          )}>
            {message.text}
          </div>
        )}


        {/* Role legend */}
        <div className="flex items-center gap-6 mb-6">
          {Object.entries(ROLE_CONFIG).map(([key, cfg]) => (
            <div key={key} className="flex items-center gap-2">
              <cfg.icon className="w-3.5 h-3.5 text-stone-400" />
              <span className="text-xs font-medium text-stone-500">{cfg.label} — {cfg.desc}</span>
            </div>
          ))}
        </div>

        {/* User list */}
        {isLoading ? (
          <div className="space-y-2">
            {Array.from({ length: 4 }).map((_, i) => (
              <div key={i} className="skeleton h-16 rounded-lg" />
            ))}
          </div>
        ) : (
          <div className="bg-white rounded-xl border border-stone-200 shadow-soft overflow-hidden">
            <table className="w-full">
              <thead>
                <tr className="bg-stone-50 border-b border-stone-100">
                  <th className="px-5 py-3 text-left text-xs font-bold text-stone-400 uppercase tracking-wider">User</th>
                  <th className="px-5 py-3 text-left text-xs font-bold text-stone-400 uppercase tracking-wider">Role</th>
                  <th className="px-5 py-3 text-left text-xs font-bold text-stone-400 uppercase tracking-wider">Last Login</th>
                  <th className="px-5 py-3 text-left text-xs font-bold text-stone-400 uppercase tracking-wider">Joined</th>
                  <th className="px-5 py-3 w-24" />
                </tr>
              </thead>
              <tbody className="divide-y divide-stone-100">
                {activeUsers.map((user) => (
                  <tr key={user.id} className="hover:bg-stone-50/50 transition">
                    <td className="px-5 py-4">
                      <div className="flex items-center gap-3">
                        <div className={cn(
                          'w-9 h-9 rounded-full flex items-center justify-center flex-shrink-0',
                          user.role === 'admin' ? 'bg-navy-900' : 'bg-navy-100',
                        )}>
                          <span className={cn(
                            'font-bold text-sm',
                            user.role === 'admin' ? 'text-white' : 'text-navy-600',
                          )}>
                            {user.first_name[0]}{user.last_name[0]}
                          </span>
                        </div>
                        <div>
                          <p className="text-sm font-semibold text-navy-900">{user.first_name} {user.last_name}</p>
                          <p className="text-xs text-stone-400">{user.email}</p>
                        </div>
                      </div>
                    </td>
                    <td className="px-5 py-4">
                      {editingId === user.id ? (
                        <div className="flex items-center gap-2">
                          {Object.entries(ROLE_CONFIG).map(([key, cfg]) => (
                            <button
                              key={key}
                              onClick={() => handleRoleChange(user.id, key)}
                              disabled={saving}
                              className={cn(
                                'px-2.5 py-1 rounded-full text-xs font-semibold border transition',
                                user.role === key ? cfg.color : 'border-stone-200 text-stone-400 hover:border-stone-300',
                              )}
                            >
                              {cfg.label}
                            </button>
                          ))}
                          <button onClick={() => setEditingId(null)} className="p-1 text-stone-400 hover:text-stone-600">
                            <X className="w-3.5 h-3.5" />
                          </button>
                        </div>
                      ) : (
                        <span className={cn(
                          'inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-semibold border',
                          ROLE_CONFIG[user.role]?.color || 'bg-stone-100 text-stone-500 border-stone-200',
                        )}>
                          {(() => { const Icon = ROLE_CONFIG[user.role]?.icon || Eye; return <Icon className="w-3 h-3" /> })()}
                          {ROLE_CONFIG[user.role]?.label || user.role}
                        </span>
                      )}
                    </td>
                    <td className="px-5 py-4">
                      <span className="text-xs text-stone-400">{relativeTime(user.last_login)}</span>
                    </td>
                    <td className="px-5 py-4">
                      <span className="text-xs text-stone-400">{formatDate(user.created_at)}</span>
                    </td>
                    <td className="px-5 py-4">
                      <div className="flex items-center gap-1 justify-end">
                        <button
                          onClick={() => setEditingId(editingId === user.id ? null : user.id)}
                          className="p-1.5 text-stone-400 hover:text-navy-600 hover:bg-stone-100 rounded-md transition"
                          title="Edit role"
                        >
                          <Pencil className="w-3.5 h-3.5" />
                        </button>
                        <button
                          onClick={() => handleToggleActive(user.id, true)}
                          className="p-1.5 text-stone-400 hover:text-red-600 hover:bg-red-50 rounded-md transition"
                          title="Deactivate user"
                        >
                          <UserMinus className="w-3.5 h-3.5" />
                        </button>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}

        {/* Inactive users */}
        {inactiveUsers.length > 0 && (
          <div className="mt-8">
            <h3 className="text-sm font-semibold text-stone-400 uppercase tracking-wider mb-3">Deactivated</h3>
            <div className="bg-white rounded-xl border border-stone-200 shadow-soft overflow-hidden opacity-60">
              <table className="w-full">
                <tbody className="divide-y divide-stone-100">
                  {inactiveUsers.map((user) => (
                    <tr key={user.id} className="hover:bg-stone-50/50 transition">
                      <td className="px-5 py-3">
                        <div className="flex items-center gap-3">
                          <div className="w-9 h-9 rounded-full bg-stone-100 flex items-center justify-center flex-shrink-0">
                            <span className="font-bold text-sm text-stone-400">{user.first_name[0]}{user.last_name[0]}</span>
                          </div>
                          <div>
                            <p className="text-sm font-medium text-stone-500">{user.first_name} {user.last_name}</p>
                            <p className="text-xs text-stone-400">{user.email}</p>
                          </div>
                        </div>
                      </td>
                      <td className="px-5 py-3">
                        <span className="text-xs text-stone-400">Deactivated</span>
                      </td>
                      <td className="px-5 py-3">
                        <button
                          onClick={() => handleToggleActive(user.id, false)}
                          className="inline-flex items-center gap-1.5 px-2.5 py-1 text-xs font-medium text-blue-600 hover:bg-blue-50 rounded-md transition"
                        >
                          <UserPlus className="w-3.5 h-3.5" /> Reactivate
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}


/* ═══════════════════════════════════════════════════
   INVITE FORM — triggers registration flow
   ═══════════════════════════════════════════════════ */

function InviteForm({ onClose, onSuccess }: { onClose: () => void; onSuccess: () => void }) {
  const [form, setForm] = useState({
    first_name: '',
    last_name: '',
    email: '',
    role: 'sales',
    password: '',
  })
  const [sending, setSending] = useState(false)
  const [error, setError] = useState('')

  async function handleSubmit() {
    if (!form.first_name.trim() || !form.last_name.trim() || !form.email.trim() || !form.password.trim()) {
      setError('All fields are required')
      return
    }
    if (!form.email.endsWith('@jauniforms.com')) {
      setError('Email must be @jauniforms.com')
      return
    }
    if (form.password.length < 8) {
      setError('Password must be at least 8 characters')
      return
    }

    setSending(true)
    setError('')
    try {
      const { data } = await api.post('/auth/register', form)
      if (data.success) {
        onSuccess()
      } else {
        setError(data.error || 'Registration failed')
      }
    } catch (e: any) {
      setError(e?.response?.data?.error || e?.response?.data?.detail || 'Failed to invite user')
    }
    setSending(false)
  }

  return (
    <div className="mb-6 p-5 bg-white rounded-xl border border-navy-200 shadow-md animate-fadeIn">
      <div className="flex items-center justify-between mb-4">
        <h3 className="text-sm font-bold text-navy-900">Invite New User</h3>
        <button onClick={onClose} className="p-1 text-stone-400 hover:text-stone-600 rounded transition">
          <X className="w-4 h-4" />
        </button>
      </div>

      {error && (
        <div className="mb-3 px-3 py-2 rounded-lg bg-red-50 text-red-600 text-xs font-medium border border-red-200">
          {error}
        </div>
      )}

      <div className="grid grid-cols-2 gap-3">
        <div>
          <label className="field-label block mb-1">First Name</label>
          <input
            value={form.first_name}
            onChange={(e) => setForm(f => ({ ...f, first_name: e.target.value }))}
            placeholder="Nico"
            className="w-full h-9 px-3 text-sm bg-stone-50 border border-stone-200 rounded-lg outline-none focus:border-navy-400 focus:ring-1 focus:ring-navy-200"
            autoFocus
          />
        </div>
        <div>
          <label className="field-label block mb-1">Last Name</label>
          <input
            value={form.last_name}
            onChange={(e) => setForm(f => ({ ...f, last_name: e.target.value }))}
            placeholder="Leal"
            className="w-full h-9 px-3 text-sm bg-stone-50 border border-stone-200 rounded-lg outline-none focus:border-navy-400 focus:ring-1 focus:ring-navy-200"
          />
        </div>
        <div className="col-span-2">
          <label className="field-label block mb-1">Email</label>
          <div className="relative">
            <Mail className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-stone-400" />
            <input
              value={form.email}
              onChange={(e) => setForm(f => ({ ...f, email: e.target.value }))}
              placeholder="nico@jauniforms.com"
              className="w-full h-9 pl-9 pr-3 text-sm bg-stone-50 border border-stone-200 rounded-lg outline-none focus:border-navy-400 focus:ring-1 focus:ring-navy-200"
            />
          </div>
        </div>
        <div>
          <label className="field-label block mb-1">Role</label>
          <select
            value={form.role}
            onChange={(e) => setForm(f => ({ ...f, role: e.target.value }))}
            className="w-full h-9 px-3 text-sm bg-stone-50 border border-stone-200 rounded-lg outline-none focus:border-navy-400"
          >
            <option value="sales">Sales</option>
            <option value="admin">Admin</option>
          </select>
        </div>
        <div>
          <label className="field-label block mb-1">Temporary Password</label>
          <div className="relative">
            <Lock className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-stone-400" />
            <input
              value={form.password}
              onChange={(e) => setForm(f => ({ ...f, password: e.target.value }))}
              placeholder="Min 8 chars"
              type="text"
              className="w-full h-9 pl-9 pr-3 text-sm bg-stone-50 border border-stone-200 rounded-lg outline-none focus:border-navy-400 focus:ring-1 focus:ring-navy-200"
            />
          </div>
        </div>
      </div>

      <div className="flex items-center gap-3 mt-4">
        <button
          onClick={handleSubmit}
          disabled={sending}
          className="flex items-center gap-2 px-4 py-2.5 text-sm font-semibold bg-navy-900 text-white rounded-lg hover:bg-navy-800 transition disabled:opacity-50"
        >
          {sending ? <Loader2 className="w-4 h-4 animate-spin" /> : <Plus className="w-4 h-4" />}
          Send Invite
        </button>
        <button onClick={onClose} className="px-4 py-2.5 text-sm font-medium text-stone-500 hover:text-stone-700 transition">
          Cancel
        </button>
      </div>

      <p className="text-xs text-stone-400 mt-3">
        User will receive a 6-digit verification code at their email. They'll need to verify before they can log in.
      </p>
    </div>
  )
}
