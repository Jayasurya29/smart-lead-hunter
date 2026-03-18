import { createContext, useContext, useState, useEffect, ReactNode } from 'react'
import api from '@/api/client'

interface User {
  id: number
  first_name: string
  last_name: string
  email: string
  role: 'sales' | 'admin'
}

interface AuthContextType {
  user: User | null
  isAuthenticated: boolean
  isLoading: boolean
  login: (email: string, password: string, remember?: boolean) => Promise<{ success: boolean; error?: string }>
  logout: () => Promise<void>
}

const AuthContext = createContext<AuthContextType | null>(null)

export function AuthProvider({ children }: { children: ReactNode }) {
  const [user, setUser] = useState<User | null>(null)
  const [isLoading, setIsLoading] = useState(true)

  useEffect(() => {
    // On mount, check if we have a valid session (cookie-based, browser handles it)
    checkAuth()
  }, [])

  async function checkAuth() {
    try {
      const resp = await api.get('/auth/me')
      setUser(resp.data)
    } catch {
      setUser(null)
    } finally {
      setIsLoading(false)
    }
  }

  async function login(email: string, password: string, remember = false): Promise<{ success: boolean; error?: string }> {
    try {
      const resp = await api.post('/auth/login', { email, password, remember })
      setUser(resp.data.user)
      return { success: true }
    } catch (err: any) {
      const detail = err.response?.data?.detail || 'Invalid email or password'
      return { success: false, error: detail }
    }
  }

  async function logout() {
    try {
      await api.post('/auth/logout')
    } catch {
      // ignore
    }
    setUser(null)
  }

  return (
    <AuthContext.Provider value={{
      user,
      isAuthenticated: !!user,
      isLoading,
      login,
      logout,
    }}>
      {children}
    </AuthContext.Provider>
  )
}

export function useAuth() {
  const ctx = useContext(AuthContext)
  if (!ctx) throw new Error('useAuth must be used within AuthProvider')
  return ctx
}
