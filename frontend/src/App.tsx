import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom'
import { useAuth } from '@/hooks/useAuth'
import AppLayout from '@/components/layout/AppLayout'
import ErrorBoundary from '@/components/ErrorBoundary'
import LoginPage from '@/pages/Login'
import RegisterPage from '@/pages/Register'
import Dashboard from '@/pages/Dashboard'
import { Loader2 } from 'lucide-react'
import UsersPage from '@/pages/Users'
import ExistingHotels from '@/pages/ExistingHotels'
import Outreach from '@/pages/Outreach'
import SourcesPage from '@/pages/SourcesPage'
// HIDDEN 2026-04-24 — re-enable by uncommenting
// import ClientIntelligence from '@/pages/ClientIntelligence'
import MapPage from '@/pages/MapPage'

function ProtectedRoute({ children }: { children: React.ReactNode }) {
  const { isAuthenticated, isLoading } = useAuth()

  if (isLoading) {
    return (
      <div className="h-full flex items-center justify-center">
        <Loader2 className="w-8 h-8 animate-spin text-amber-500" />
      </div>
    )
  }

  if (!isAuthenticated) {
    return <Navigate to="/login" replace />
  }

  return <AppLayout>{children}</AppLayout>
}

export default function AppRouter() {
  const { isAuthenticated } = useAuth()

  return (
    <ErrorBoundary>
      <BrowserRouter>
        <Routes>
          {/* Auth routes */}
          <Route
            path="/login"
            element={isAuthenticated ? <Navigate to="/new-hotels" replace /> : <LoginPage />}
          />
          <Route
            path="/register"
            element={isAuthenticated ? <Navigate to="/new-hotels" replace /> : <RegisterPage />}
          />

          {/* Protected routes */}
          <Route
            path="/new-hotels"
            element={
              <ProtectedRoute>
                <Dashboard />
              </ProtectedRoute>
            }
          />
          <Route
            path="/users"
            element={
              <ProtectedRoute>
                <UsersPage />
              </ProtectedRoute>
            }
          />
          <Route
            path="/existing-hotels"
            element={
              <ProtectedRoute>
                <ExistingHotels />
              </ProtectedRoute>
            }
          />
          <Route
            path="/map"
            element={
              <ProtectedRoute>
                <MapPage />
              </ProtectedRoute>
            }
          />
          <Route
            path="/sources"
            element={
              <ProtectedRoute>
                <SourcesPage />
              </ProtectedRoute>
            }
          />
          {/* HIDDEN 2026-04-24 — re-enable by uncommenting
          <Route
            path="/clients"
            element={
              <ProtectedRoute>
                <ClientIntelligence />
              </ProtectedRoute>
            }
          />
          */}
          <Route
            path="/outreach"
            element={
              <ProtectedRoute>
                <Outreach />
              </ProtectedRoute>
            }
          />

          <Route path="*" element={<Navigate to="/new-hotels" replace />} />
        </Routes>
      </BrowserRouter>
    </ErrorBoundary>
  )
}
