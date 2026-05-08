import { useState } from 'react'
import { useNavigate, Link } from 'react-router-dom'
import { Mail, Lock, ArrowRight, ArrowLeft, Loader2, Eye, EyeOff, CheckCircle2, KeyRound } from 'lucide-react'
import api from '@/api/client'

/**
 * Self-serve password reset flow.
 *
 * Three states:
 *   'request'  — user enters email; backend sends OTP code via email
 *   'verify'   — user enters the 6-digit code + new password; backend resets
 *   'success'  — confirmation, link back to login
 *
 * Backend always returns success on /forgot-password regardless of whether
 * the email exists, so we don't leak account enumeration info — but we still
 * progress to the verify step so the UX is consistent.
 */
export default function ResetPassword() {
  const navigate = useNavigate()
  const [step, setStep] = useState<'request' | 'verify' | 'success'>('request')
  const [email, setEmail] = useState('')
  const [code, setCode] = useState('')
  const [newPassword, setNewPassword] = useState('')
  const [showPwd, setShowPwd] = useState(false)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  async function handleRequestCode(e: React.FormEvent) {
    e.preventDefault()
    setError('')
    setLoading(true)
    try {
      await api.post('/auth/forgot-password', { email: email.trim().toLowerCase() })
      setStep('verify')
    } catch (e: any) {
      // Generic message — don't reveal whether the email exists
      setError(e?.response?.data?.detail || 'Could not process request. Try again.')
    }
    setLoading(false)
  }

  async function handleResetSubmit(e: React.FormEvent) {
    e.preventDefault()
    setError('')
    setLoading(true)
    try {
      const { data } = await api.post('/auth/reset-password', {
        email: email.trim().toLowerCase(),
        code: code.trim(),
        new_password: newPassword,
      })
      if (data.success) {
        setStep('success')
      } else {
        setError(data.error || 'Reset failed')
      }
    } catch (e: any) {
      setError(e?.response?.data?.detail || 'Reset failed')
    }
    setLoading(false)
  }

  return (
    <div className="min-h-screen flex items-center justify-center px-4" style={{ background: '#f8fafc' }}>
      <div className="w-full max-w-md">
        <div className="bg-white rounded-2xl shadow-xl border border-stone-100 p-8">
          {/* Header */}
          <div className="mb-6">
            <Link
              to="/login"
              className="inline-flex items-center gap-1.5 text-sm font-medium text-slate-500 hover:text-navy-900 mb-4 transition"
            >
              <ArrowLeft className="w-4 h-4" /> Back to sign in
            </Link>
            <div className="flex items-center gap-3 mb-2">
              <div className="w-10 h-10 rounded-xl flex items-center justify-center" style={{ background: 'linear-gradient(135deg, #fef3c7 0%, #fde68a 100%)' }}>
                <KeyRound className="w-5 h-5 text-amber-700" />
              </div>
              <div>
                <h1 className="text-xl font-bold text-navy-900">
                  {step === 'request' && 'Reset password'}
                  {step === 'verify' && 'Enter code'}
                  {step === 'success' && 'Password reset'}
                </h1>
              </div>
            </div>
            <p className="text-sm text-slate-500">
              {step === 'request' && 'Enter your email and we\'ll send you a verification code.'}
              {step === 'verify' && `We sent a 6-digit code to ${email}. Enter it below along with your new password.`}
              {step === 'success' && 'Your password has been reset successfully.'}
            </p>
          </div>

          {/* Step 1: request code */}
          {step === 'request' && (
            <form onSubmit={handleRequestCode} className="flex flex-col gap-5">
              <div>
                <label className="block text-[13px] font-semibold text-gray-700 mb-1.5">Email</label>
                <div className="relative">
                  <Mail className="absolute left-4 top-1/2 -translate-y-1/2 w-[18px] h-[18px] text-slate-400 pointer-events-none" />
                  <input
                    type="email"
                    value={email}
                    onChange={e => setEmail(e.target.value)}
                    placeholder="you@jauniforms.com"
                    autoComplete="email"
                    required
                    className="w-full pl-11 pr-4 py-[13px] text-sm rounded-xl text-gray-900 placeholder-slate-400 transition-all outline-none"
                    style={{ background: '#fafafa', border: '1.5px solid #e2e8f0' }}
                    onFocus={e => e.currentTarget.style.borderColor = '#f59e0b'}
                    onBlur={e => e.currentTarget.style.borderColor = '#e2e8f0'}
                  />
                </div>
              </div>

              {error && (
                <div className="text-sm text-red-600 bg-red-50 border border-red-200 rounded-lg px-3 py-2">
                  {error}
                </div>
              )}

              <button
                type="submit"
                disabled={loading || !email}
                className="w-full flex items-center justify-center gap-2 py-[13px] rounded-xl text-[15px] font-semibold text-white transition-all disabled:opacity-60 disabled:cursor-not-allowed"
                style={{ background: 'linear-gradient(135deg, #0a1628 0%, #1a3158 100%)' }}
              >
                {loading ? <Loader2 className="w-5 h-5 animate-spin" /> : (
                  <>
                    Send code <ArrowRight className="w-[18px] h-[18px]" />
                  </>
                )}
              </button>
            </form>
          )}

          {/* Step 2: verify code + set new password */}
          {step === 'verify' && (
            <form onSubmit={handleResetSubmit} className="flex flex-col gap-5">
              <div>
                <label className="block text-[13px] font-semibold text-gray-700 mb-1.5">6-digit code</label>
                <input
                  type="text"
                  value={code}
                  onChange={e => setCode(e.target.value.replace(/\D/g, '').slice(0, 6))}
                  placeholder="000000"
                  inputMode="numeric"
                  autoComplete="one-time-code"
                  required
                  className="w-full px-4 py-[13px] text-center text-2xl font-mono tracking-[8px] rounded-xl text-gray-900 placeholder-slate-300 transition-all outline-none"
                  style={{ background: '#fafafa', border: '1.5px solid #e2e8f0' }}
                  onFocus={e => e.currentTarget.style.borderColor = '#f59e0b'}
                  onBlur={e => e.currentTarget.style.borderColor = '#e2e8f0'}
                />
              </div>

              <div>
                <label className="block text-[13px] font-semibold text-gray-700 mb-1.5">New password</label>
                <div className="relative">
                  <Lock className="absolute left-4 top-1/2 -translate-y-1/2 w-[18px] h-[18px] text-slate-400 pointer-events-none" />
                  <input
                    type={showPwd ? 'text' : 'password'}
                    value={newPassword}
                    onChange={e => setNewPassword(e.target.value)}
                    placeholder="At least 8 characters"
                    autoComplete="new-password"
                    required
                    className="w-full pl-11 pr-12 py-[13px] text-sm rounded-xl text-gray-900 placeholder-slate-400 transition-all outline-none"
                    style={{ background: '#fafafa', border: '1.5px solid #e2e8f0' }}
                    onFocus={e => e.currentTarget.style.borderColor = '#f59e0b'}
                    onBlur={e => e.currentTarget.style.borderColor = '#e2e8f0'}
                  />
                  <button
                    type="button"
                    onClick={() => setShowPwd(!showPwd)}
                    className="absolute right-3 top-1/2 -translate-y-1/2 p-1 text-slate-400 hover:text-navy-900 transition-colors"
                  >
                    {showPwd ? <EyeOff className="w-[18px] h-[18px]" /> : <Eye className="w-[18px] h-[18px]" />}
                  </button>
                </div>
              </div>

              {error && (
                <div className="text-sm text-red-600 bg-red-50 border border-red-200 rounded-lg px-3 py-2">
                  {error}
                </div>
              )}

              <button
                type="submit"
                disabled={loading || code.length !== 6 || !newPassword}
                className="w-full flex items-center justify-center gap-2 py-[13px] rounded-xl text-[15px] font-semibold text-white transition-all disabled:opacity-60 disabled:cursor-not-allowed"
                style={{ background: 'linear-gradient(135deg, #0a1628 0%, #1a3158 100%)' }}
              >
                {loading ? <Loader2 className="w-5 h-5 animate-spin" /> : 'Reset password'}
              </button>

              <button
                type="button"
                onClick={() => { setStep('request'); setCode(''); setNewPassword(''); setError('') }}
                className="text-sm text-slate-500 hover:text-navy-900 transition"
              >
                Didn't receive a code? Try again
              </button>
            </form>
          )}

          {/* Step 3: success */}
          {step === 'success' && (
            <div className="flex flex-col gap-5 items-center text-center">
              <div className="w-16 h-16 rounded-full flex items-center justify-center" style={{ background: 'linear-gradient(135deg, #d1fae5 0%, #a7f3d0 100%)' }}>
                <CheckCircle2 className="w-8 h-8 text-emerald-600" />
              </div>
              <p className="text-sm text-slate-600">
                You can now sign in with your new password.
              </p>
              <button
                onClick={() => navigate('/login')}
                className="w-full flex items-center justify-center gap-2 py-[13px] rounded-xl text-[15px] font-semibold text-white transition-all"
                style={{ background: 'linear-gradient(135deg, #0a1628 0%, #1a3158 100%)' }}
              >
                Go to sign in <ArrowRight className="w-[18px] h-[18px]" />
              </button>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
