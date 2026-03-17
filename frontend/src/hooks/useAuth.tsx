import { createContext, useContext, useState, useEffect, ReactNode } from 'react'
import api from '@/api/client'

interface AuthContextType {
  isAuthenticated: boolean
  isLoading: boolean
  login: (apiKey: string) => Promise<boolean>
  logout: () => void
}

const AuthContext = createContext<AuthContextType | null>(null)

export function AuthProvider({ children }: { children: ReactNode }) {
  const [isAuthenticated, setIsAuthenticated] = useState(false)
  const [isLoading, setIsLoading] = useState(true)

  useEffect(() => {
    checkAuth()
  }, [])

  async function checkAuth() {
    const token = localStorage.getItem('slh_token')
    if (!token) {
      setIsLoading(false)
      return
    }
    try {
      // Use the dedicated auth verification endpoint
      await api.get('/api/auth/verify', { headers: { 'X-API-Key': token } })
      api.defaults.headers.common['X-API-Key'] = token
      setIsAuthenticated(true)
    } catch {
      localStorage.removeItem('slh_token')
      setIsAuthenticated(false)
    }
    setIsLoading(false)
  }
  
  async function login(apiKey: string): Promise<boolean> {
    try {
      const resp = await api.get('/api/auth/verify', {
        headers: { 'X-API-Key': apiKey },
      })
      if (resp.status === 200) {
        localStorage.setItem('slh_token', apiKey)
        api.defaults.headers.common['X-API-Key'] = apiKey
        setIsAuthenticated(true)
        return true
      }
    } catch {
      // Login failed
    }
    return false
  }

  function logout() {
    localStorage.removeItem('slh_token')
    delete api.defaults.headers.common['X-API-Key']
    setIsAuthenticated(false)
  }

  return (
    <AuthContext.Provider value={{ isAuthenticated, isLoading, login, logout }}>
      {children}
    </AuthContext.Provider>
  )
}

export function useAuth() {
  const ctx = useContext(AuthContext)
  if (!ctx) throw new Error('useAuth must be used within AuthProvider')
  return ctx
}