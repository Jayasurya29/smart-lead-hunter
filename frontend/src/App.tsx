import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom'
import { useAuth } from '@/hooks/useAuth'
import AppLayout from '@/components/layout/AppLayout'
import ErrorBoundary from '@/components/ErrorBoundary'
import LoginPage from '@/pages/Login'
import Dashboard from '@/pages/Dashboard'
import { Loader2 } from 'lucide-react'

function ProtectedRoute({ children }: { children: React.ReactNode }) {
  const { isAuthenticated, isLoading } = useAuth()

  if (isLoading) {
    return (
      <div className="h-full flex items-center justify-center">
        <Loader2 className="w-8 h-8 animate-spin text-blue-500" />
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
          <Route
            path="/login"
            element={isAuthenticated ? <Navigate to="/dashboard" replace /> : <LoginPage />}
          />
          <Route
            path="/dashboard"
            element={
              <ProtectedRoute>
                <Dashboard />
              </ProtectedRoute>
            }
          />
          {/* Future routes */}
          <Route
            path="/map"
            element={
              <ProtectedRoute>
                <div className="p-8 text-center text-gray-400">
                  <p className="text-lg font-medium mb-2">Map View</p>
                  <p className="text-sm">Coming soon — hotel locations with MapLibre GL</p>
                </div>
              </ProtectedRoute>
            }
          />
          <Route
            path="/outreach"
            element={
              <ProtectedRoute>
                <div className="p-8 text-center text-gray-400">
                  <p className="text-lg font-medium mb-2">AI Email Outreach</p>
                  <p className="text-sm">Coming soon — automated email campaigns with Gemini</p>
                </div>
              </ProtectedRoute>
            }
          />
          {/* Default redirect */}
          <Route path="*" element={<Navigate to="/dashboard" replace />} />
        </Routes>
      </BrowserRouter>
    </ErrorBoundary>
  )
}
