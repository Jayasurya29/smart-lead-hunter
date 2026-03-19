import { useState } from 'react'
import { Users as UsersIcon, Shield, Mail, Plus, Loader2 } from 'lucide-react'
import { cn } from '@/lib/utils'

interface UserRow {
  id: number
  email: string
  full_name: string
  role: 'admin' | 'manager' | 'viewer'
  created_at: string
}

// TODO: Wire to backend API when user management endpoints exist
const MOCK_USERS: UserRow[] = []

export default function UsersPage() {
  const [showInvite, setShowInvite] = useState(false)

  return (
    <div className="h-full overflow-y-auto">
      <div className="max-w-4xl mx-auto px-6 py-8">
        {/* Header */}
        <div className="flex items-center justify-between mb-8">
          <div>
            <h1 className="text-2xl font-bold text-navy-900">Users</h1>
            <p className="text-sm text-stone-400 mt-1">Manage team access to Smart Lead Hunter</p>
          </div>
          <button
            onClick={() => setShowInvite(true)}
            className="flex items-center gap-2 px-4 py-2.5 text-sm font-semibold bg-navy-900 text-white rounded-lg hover:bg-navy-800 transition"
          >
            <Plus className="w-4 h-4" /> Invite User
          </button>
        </div>

        {/* Role legend */}
        <div className="flex items-center gap-6 mb-6">
          <div className="flex items-center gap-2">
            <span className="w-2.5 h-2.5 rounded-full bg-gold-400" />
            <span className="text-xs font-medium text-stone-500">Admin — Full access</span>
          </div>
          <div className="flex items-center gap-2">
            <span className="w-2.5 h-2.5 rounded-full bg-navy-400" />
            <span className="text-xs font-medium text-stone-500">Manager — Approve/Reject leads</span>
          </div>
          <div className="flex items-center gap-2">
            <span className="w-2.5 h-2.5 rounded-full bg-stone-300" />
            <span className="text-xs font-medium text-stone-500">Viewer — Read only</span>
          </div>
        </div>

        {/* User list */}
        <div className="bg-white rounded-xl border border-stone-200 shadow-soft overflow-hidden">
          <table className="w-full">
            <thead>
              <tr className="bg-stone-50 border-b border-stone-100">
                <th className="px-5 py-3 text-left text-xs font-bold text-stone-400 uppercase tracking-wider">User</th>
                <th className="px-5 py-3 text-left text-xs font-bold text-stone-400 uppercase tracking-wider">Role</th>
                <th className="px-5 py-3 text-left text-xs font-bold text-stone-400 uppercase tracking-wider">Added</th>
                <th className="px-5 py-3 w-20" />
              </tr>
            </thead>
            <tbody className="divide-y divide-stone-100">
              {/* Current user (always show) */}
              <tr className="hover:bg-stone-50/50 transition">
                <td className="px-5 py-4">
                  <div className="flex items-center gap-3">
                    <div className="w-9 h-9 rounded-full bg-navy-900 flex items-center justify-center flex-shrink-0">
                      <span className="text-white font-bold text-sm">A</span>
                    </div>
                    <div>
                      <p className="text-sm font-semibold text-navy-900">Admin</p>
                      <p className="text-xs text-stone-400">it@jauniforms.com</p>
                    </div>
                  </div>
                </td>
                <td className="px-5 py-4">
                  <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-semibold bg-gold-50 text-gold-600">
                    <Shield className="w-3 h-3" /> Admin
                  </span>
                </td>
                <td className="px-5 py-4 text-xs text-stone-400">Original</td>
                <td className="px-5 py-4" />
              </tr>

              {/* Sales team */}
              {[
                { name: 'Nico Leal',        email: 'nico@jauniforms.com',      role: 'manager' },
                { name: 'Alexandra Pagan',   email: 'alexandra@jauniforms.com', role: 'manager' },
                { name: 'Ulysses',           email: 'ulysses@jauniforms.com',   role: 'viewer' },
              ].map((user) => (
                <tr key={user.email} className="hover:bg-stone-50/50 transition">
                  <td className="px-5 py-4">
                    <div className="flex items-center gap-3">
                      <div className={cn(
                        'w-9 h-9 rounded-full flex items-center justify-center flex-shrink-0',
                        user.role === 'manager' ? 'bg-navy-100' : 'bg-stone-100',
                      )}>
                        <span className={cn(
                          'font-bold text-sm',
                          user.role === 'manager' ? 'text-navy-600' : 'text-stone-500',
                        )}>
                          {user.name[0]}
                        </span>
                      </div>
                      <div>
                        <p className="text-sm font-semibold text-navy-900">{user.name}</p>
                        <p className="text-xs text-stone-400">{user.email}</p>
                      </div>
                    </div>
                  </td>
                  <td className="px-5 py-4">
                    <span className={cn(
                      'inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-semibold',
                      user.role === 'manager' ? 'bg-navy-50 text-navy-600' : 'bg-stone-100 text-stone-500',
                    )}>
                      {user.role === 'manager' ? 'Manager' : 'Viewer'}
                    </span>
                  </td>
                  <td className="px-5 py-4 text-xs text-stone-400">Team member</td>
                  <td className="px-5 py-4">
                    <button className="text-xs text-stone-400 hover:text-stone-600 transition">Edit</button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {/* Info */}
        <div className="mt-6 bg-navy-50/50 rounded-lg border border-navy-100 p-4">
          <div className="flex items-start gap-3">
            <UsersIcon className="w-5 h-5 text-navy-400 flex-shrink-0 mt-0.5" />
            <div>
              <p className="text-sm font-semibold text-navy-800">User management coming soon</p>
              <p className="text-xs text-navy-500 mt-0.5">
                Invite, edit roles, and manage team access directly from this page.
                For now, users are managed through the backend.
              </p>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}
