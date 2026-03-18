import { useState, useRef, useCallback } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import {
  Mail, Lock, Eye, EyeOff, User, Briefcase,
  ArrowRight, Loader2, CheckCircle, XCircle, AlertCircle
} from 'lucide-react'
import api from '@/api/client'
import { useAuth } from '@/hooks/useAuth'

// ── Password strength ──────────────────────────────────────────────────────
function getStrength(pw: string) {
  let score = 0
  if (pw.length >= 8) score++
  if (/[A-Z]/.test(pw)) score++
  if (/[a-z]/.test(pw)) score++
  if (/\d/.test(pw)) score++
  return score
}

const STRENGTH_LABEL = ['', 'Weak', 'Fair', 'Good', 'Strong']
const STRENGTH_COLOR = ['#e2e8f0', '#ef4444', '#f59e0b', '#3b82f6', '#10b981']

function ReqItem({ ok, label }: { ok: boolean; label: string }) {
  return (
    <span className="flex items-center gap-1.5 text-[12px] transition-colors"
          style={{ color: ok ? '#10b981' : '#94a3b8' }}>
      {ok
        ? <CheckCircle className="w-3.5 h-3.5 flex-shrink-0" />
        : <XCircle    className="w-3.5 h-3.5 flex-shrink-0" />}
      {label}
    </span>
  )
}

// ── OTP Input component ────────────────────────────────────────────────────
function OTPInput({ onComplete }: { onComplete: (code: string) => void }) {
  const [values, setValues] = useState(['', '', '', '', '', ''])
  const [shake, setShake] = useState(false)
  const inputs = useRef<(HTMLInputElement | null)[]>([])

  const triggerShake = useCallback(() => {
    setShake(true)
    setTimeout(() => setShake(false), 400)
  }, [])

  // Expose shake to parent
  ;(OTPInput as any)._shake = triggerShake

  function handleChange(i: number, val: string) {
    if (!/^\d*$/.test(val)) return
    const newVals = [...values]
    newVals[i] = val.slice(-1)
    setValues(newVals)
    if (val && i < 5) inputs.current[i + 1]?.focus()
    const code = newVals.join('')
    if (code.length === 6) onComplete(code)
  }

  function handleKeyDown(i: number, e: React.KeyboardEvent) {
    if (e.key === 'Backspace' && !values[i] && i > 0) {
      inputs.current[i - 1]?.focus()
    }
  }

  function handlePaste(e: React.ClipboardEvent) {
    e.preventDefault()
    const text = e.clipboardData.getData('text').replace(/\D/g, '').slice(0, 6)
    if (!text) return
    const newVals = [...values]
    text.split('').forEach((c, j) => { if (j < 6) newVals[j] = c })
    setValues(newVals)
    const last = Math.min(text.length, 5)
    inputs.current[last]?.focus()
    if (text.length === 6) onComplete(text)
  }

  function clear() {
    setValues(['', '', '', '', '', ''])
    inputs.current[0]?.focus()
  }

  ;(OTPInput as any)._clear = clear
  ;(OTPInput as any)._focus = () => inputs.current[0]?.focus()

  return (
    <div className={`flex gap-2.5 justify-center my-7 ${shake ? 'animate-otp-shake' : ''}`}>
      {values.map((v, i) => (
        <input
          key={i}
          ref={el => { inputs.current[i] = el }}
          type="text"
          inputMode="numeric"
          maxLength={1}
          value={v}
          onChange={e => handleChange(i, e.target.value)}
          onKeyDown={e => handleKeyDown(i, e)}
          onPaste={handlePaste}
          className="text-center font-bold text-[22px] rounded-xl transition-all outline-none"
          style={{
            width: 52, height: 58,
            border: `2px solid ${v ? '#f59e0b' : '#e2e8f0'}`,
            background: v ? 'rgba(245,158,11,0.05)' : '#fafafa',
            color: '#0a1628',
            fontFamily: 'monospace',
          }}
          onFocus={e => { e.currentTarget.style.borderColor = '#f59e0b'; e.currentTarget.style.boxShadow = '0 0 0 3px rgba(245,158,11,0.12)' }}
          onBlur={e => { e.currentTarget.style.borderColor = e.currentTarget.value ? '#f59e0b' : '#e2e8f0'; e.currentTarget.style.boxShadow = '' }}
        />
      ))}
    </div>
  )
}

// ── Main Register component ────────────────────────────────────────────────
export default function RegisterPage() {
  const navigate = useNavigate()
  const { login } = useAuth()

  // Form state
  const [firstName, setFirstName] = useState('')
  const [lastName,  setLastName]  = useState('')
  const [email,     setEmail]     = useState('')
  const [role,      setRole]      = useState('sales')
  const [password,  setPassword]  = useState('')
  const [confirmPw, setConfirmPw] = useState('')
  const [showPwd,   setShowPwd]   = useState(false)
  const [showCpw,   setShowCpw]   = useState(false)
  const [formError, setFormError] = useState('')
  const [loading,   setLoading]   = useState(false)

  // OTP modal state
  const [modalOpen,    setModalOpen]    = useState(false)
  const [verified,     setVerified]     = useState(false)
  const [verifyLoading,setVerifyLoading]= useState(false)
  const [verifyError,  setVerifyError]  = useState('')
  const [resendTimer,  setResendTimer]  = useState(0)
  const [pendingCode,  setPendingCode]  = useState('')
  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null)
  const otpRef = useRef<any>(null)

  // Password analysis
  const strength    = getStrength(password)
  const pwMatch     = confirmPw !== '' && password === confirmPw
  const pwMismatch  = confirmPw !== '' && password !== confirmPw

  function startResendTimer() {
    setResendTimer(60)
    if (timerRef.current) clearInterval(timerRef.current)
    timerRef.current = setInterval(() => {
      setResendTimer(t => {
        if (t <= 1) { clearInterval(timerRef.current!); return 0 }
        return t - 1
      })
    }, 1000)
  }

  // Step 1: submit form → send OTP
  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    setFormError('')
    if (password !== confirmPw)  { setFormError('Passwords do not match'); return }
    if (password.length < 8)     { setFormError('Password must be at least 8 characters'); return }
    if (strength < 2)            { setFormError('Please use a stronger password'); return }

    setLoading(true)
    try {
      const resp = await api.post('/auth/register', {
        first_name: firstName.trim(),
        last_name:  lastName.trim(),
        email:      email.trim(),
        role,
        password,
      })
      if (resp.data.success) {
        setModalOpen(true)
        startResendTimer()
        setTimeout(() => (OTPInput as any)._focus?.(), 100)
      } else {
        setFormError(resp.data.error || 'Registration failed')
      }
    } catch (err: any) {
      setFormError(err.response?.data?.detail || err.response?.data?.error || 'Registration failed')
    } finally {
      setLoading(false)
    }
  }

  // Step 2: verify OTP
  async function handleVerify(code: string) {
    if (code.length !== 6) return
    setVerifyError('')
    setVerifyLoading(true)
    try {
      const resp = await api.post('/auth/verify-code', { email: email.trim(), code })
      if (resp.data.success) {
        setVerified(true)
        setTimeout(() => navigate('/dashboard'), 1200)
      } else {
        setVerifyError(resp.data.error || 'Invalid code')
        ;(OTPInput as any)._shake?.()
        ;(OTPInput as any)._clear?.()
      }
    } catch (err: any) {
      setVerifyError(err.response?.data?.detail || 'Verification failed')
      ;(OTPInput as any)._shake?.()
      ;(OTPInput as any)._clear?.()
    } finally {
      setVerifyLoading(false)
    }
  }

  async function handleResend() {
    if (resendTimer > 0) return
    try {
      await api.post('/auth/resend-code', { email: email.trim() })
      startResendTimer()
      ;(OTPInput as any)._clear?.()
    } catch { /* ignore */ }
  }

  // Shared input style
  function inputStyle(focused = false) {
    return {
      background: '#fafafa',
      border: `1.5px solid ${focused ? '#f59e0b' : '#e2e8f0'}`,
    }
  }

  const onFocus = (e: React.FocusEvent<HTMLInputElement | HTMLSelectElement>) =>
    (e.currentTarget.style.borderColor = '#f59e0b')
  const onBlur  = (e: React.FocusEvent<HTMLInputElement | HTMLSelectElement>) =>
    (e.currentTarget.style.borderColor = '#e2e8f0')

  return (
    <div className="min-h-screen flex items-center justify-center p-5 relative overflow-hidden animate-gradient-shift"
         style={{ background: 'linear-gradient(135deg, #040d1a 0%, #0a1628 50%, #0f2040 100%)' }}>

      {/* Dot grid */}
      <div className="absolute pointer-events-none animate-dot-grid"
           style={{
             backgroundImage: 'radial-gradient(circle, rgba(245,158,11,0.06) 1px, transparent 1px)',
             backgroundSize: '48px 48px',
             top: '-50%', left: '-50%', width: '200%', height: '200%',
           }} />

      {/* Blobs */}
      <div className="absolute rounded-full pointer-events-none animate-blob-1"
           style={{ width: 320, height: 320, background: 'rgba(245,158,11,0.10)', filter: 'blur(70px)', top: '8%', left: '8%' }} />
      <div className="absolute rounded-full pointer-events-none animate-blob-2"
           style={{ width: 420, height: 420, background: 'rgba(15,32,64,0.6)', filter: 'blur(70px)', bottom: '8%', right: '8%' }} />
      <div className="absolute rounded-full pointer-events-none animate-blob-3"
           style={{ width: 280, height: 280, background: 'rgba(251,191,36,0.07)', filter: 'blur(70px)', top: '45%', right: '18%' }} />

      {/* Card */}
      <div className="relative z-10 w-full max-w-[920px] flex rounded-3xl overflow-hidden animate-card-slide-up"
           style={{ background: 'rgba(255,255,255,0.97)', boxShadow: '0 24px 64px rgba(0,0,0,0.4)', minHeight: 580 }}>

        {/* ── LEFT PANEL ── */}
        <div className="hidden lg:flex lg:w-[42%] flex-col items-center justify-center px-10 py-14 relative overflow-hidden"
             style={{ background: 'linear-gradient(135deg, #0a1628 0%, #0f2040 100%)' }}>
          <div className="absolute top-0 left-0 right-0 h-[3px]"
               style={{ background: 'linear-gradient(90deg, #f59e0b, #fbbf24, #f59e0b)' }} />
          <div className="absolute inset-0 pointer-events-none animate-pattern-move"
               style={{
                 backgroundImage: 'radial-gradient(circle, rgba(245,158,11,0.07) 1px, transparent 1px)',
                 backgroundSize: '28px 28px',
                 top: '-50%', left: '-50%', width: '200%', height: '200%',
               }} />
          <div className="animate-logo-float relative z-10 flex items-center justify-center rounded-[28px] bg-white mb-7"
               style={{ width: 172, height: 172, padding: 24, boxShadow: '0 20px 60px rgba(0,0,0,0.35)' }}>
            <img src="/static/img/logo.svg" alt="JA Uniforms"
                 className="w-full h-full object-contain"
                 onError={e => { e.currentTarget.style.display = 'none' }} />
          </div>
          <div className="text-center relative z-10">
            <div className="text-white text-[22px] font-bold tracking-tight mb-1">Smart Lead Hunter</div>
            <div className="text-white/60 text-[13px] tracking-wide">J.A. Uniforms · Hotel Intelligence</div>
            <div className="inline-flex items-center gap-2 mt-4 px-3 py-[5px] rounded-full text-[11px] tracking-widest uppercase"
                 style={{ background: 'rgba(245,158,11,0.14)', border: '1px solid rgba(245,158,11,0.28)', color: '#fcd34d' }}>
              <span className="w-1.5 h-1.5 rounded-full bg-amber-400 animate-pulse-dot" />
              USA &amp; Caribbean · 4-Star+
            </div>
          </div>
        </div>

        {/* ── RIGHT PANEL ── */}
        <div className="flex-1 flex flex-col justify-center px-10 lg:px-14 py-12 overflow-y-auto">
          <div className="mb-7">
            <h1 className="text-[26px] font-bold tracking-tight mb-1" style={{ color: '#0a1628' }}>Create Account</h1>
            <p className="text-sm text-slate-500">Join the Smart Lead Hunter team</p>
          </div>

          {formError && (
            <div className="flex items-center gap-2 mb-5 px-4 py-3 rounded-xl text-sm font-medium"
                 style={{ background: '#fef2f2', border: '1px solid #fecaca', color: '#991b1b' }}>
              <AlertCircle className="w-4 h-4 flex-shrink-0" />
              {formError}
            </div>
          )}

          <form onSubmit={handleSubmit} className="flex flex-col gap-4">

            {/* First + Last name row */}
            <div className="grid grid-cols-2 gap-3">
              {[
                { id: 'fn', label: 'First Name', val: firstName, set: setFirstName, ph: 'Nico' },
                { id: 'ln', label: 'Last Name',  val: lastName,  set: setLastName,  ph: 'Leal' },
              ].map(f => (
                <div key={f.id}>
                  <label className="block text-[13px] font-semibold text-gray-700 mb-1.5">{f.label}</label>
                  <div className="relative">
                    <User className="absolute left-3.5 top-1/2 -translate-y-1/2 w-[17px] h-[17px] text-slate-400 pointer-events-none" />
                    <input
                      type="text" value={f.val} onChange={e => f.set(e.target.value)}
                      placeholder={f.ph} required
                      className="w-full pl-10 pr-3 py-[12px] text-sm rounded-xl text-gray-900 placeholder-slate-400 outline-none transition-all"
                      style={inputStyle()} onFocus={onFocus} onBlur={onBlur}
                    />
                  </div>
                </div>
              ))}
            </div>

            {/* Email */}
            <div>
              <label className="block text-[13px] font-semibold text-gray-700 mb-1.5">Email Address</label>
              <div className="relative">
                <Mail className="absolute left-4 top-1/2 -translate-y-1/2 w-[18px] h-[18px] text-slate-400 pointer-events-none" />
                <input type="email" value={email} onChange={e => setEmail(e.target.value)}
                       placeholder="you@jauniforms.com" required autoComplete="email"
                       className="w-full pl-11 pr-4 py-[13px] text-sm rounded-xl text-gray-900 placeholder-slate-400 outline-none transition-all"
                       style={inputStyle()} onFocus={onFocus} onBlur={onBlur} />
              </div>
              <p className="text-[12px] text-slate-400 mt-1.5 flex items-center gap-1">
                <AlertCircle className="w-3 h-3" /> A verification code will be sent to this email
              </p>
            </div>

            {/* Role */}
            <div>
              <label className="block text-[13px] font-semibold text-gray-700 mb-1.5">Role</label>
              <div className="relative">
                <Briefcase className="absolute left-4 top-1/2 -translate-y-1/2 w-[18px] h-[18px] text-slate-400 pointer-events-none z-10" />
                <select value={role} onChange={e => setRole(e.target.value)}
                        className="w-full pl-11 pr-8 py-[13px] text-sm rounded-xl text-gray-900 outline-none transition-all appearance-none cursor-pointer filter-select"
                        style={inputStyle()} onFocus={onFocus} onBlur={onBlur}>
                  <option value="sales">Sales Team</option>
                  <option value="admin">Admin</option>
                </select>
              </div>
            </div>

            {/* Password */}
            <div>
              <label className="block text-[13px] font-semibold text-gray-700 mb-1.5">Password</label>
              <div className="relative">
                <Lock className="absolute left-4 top-1/2 -translate-y-1/2 w-[18px] h-[18px] text-slate-400 pointer-events-none" />
                <input type={showPwd ? 'text' : 'password'} value={password}
                       onChange={e => setPassword(e.target.value)}
                       placeholder="Create a strong password" required
                       className="w-full pl-11 pr-12 py-[13px] text-sm rounded-xl text-gray-900 placeholder-slate-400 outline-none transition-all"
                       style={inputStyle()} onFocus={onFocus} onBlur={onBlur} />
                <button type="button" onClick={() => setShowPwd(!showPwd)}
                        className="absolute right-3 top-1/2 -translate-y-1/2 p-1 text-slate-400 hover:text-navy-900 transition-colors">
                  {showPwd ? <EyeOff className="w-[18px] h-[18px]" /> : <Eye className="w-[18px] h-[18px]" />}
                </button>
              </div>
              {/* Strength bar */}
              {password.length > 0 && (
                <div className="mt-2">
                  <div className="h-[3px] rounded-full bg-gray-200 overflow-hidden">
                    <div className="h-full rounded-full transition-all duration-300"
                         style={{ width: `${strength * 25}%`, background: STRENGTH_COLOR[strength] }} />
                  </div>
                  <p className="text-[11px] mt-1 font-medium" style={{ color: STRENGTH_COLOR[strength] }}>
                    {STRENGTH_LABEL[strength]}
                  </p>
                </div>
              )}
              {/* Requirements */}
              {password.length > 0 && (
                <div className="mt-2.5 p-2.5 rounded-lg grid grid-cols-2 gap-1.5"
                     style={{ background: '#f8fafc', border: '1px solid #e2e8f0' }}>
                  <ReqItem ok={password.length >= 8}   label="8+ characters" />
                  <ReqItem ok={/[A-Z]/.test(password)} label="Uppercase (A-Z)" />
                  <ReqItem ok={/[a-z]/.test(password)} label="Lowercase (a-z)" />
                  <ReqItem ok={/\d/.test(password)}    label="Number (0-9)" />
                </div>
              )}
            </div>

            {/* Confirm Password */}
            <div>
              <label className="block text-[13px] font-semibold text-gray-700 mb-1.5">Confirm Password</label>
              <div className="relative">
                <Lock className="absolute left-4 top-1/2 -translate-y-1/2 w-[18px] h-[18px] text-slate-400 pointer-events-none" />
                <input type={showCpw ? 'text' : 'password'} value={confirmPw}
                       onChange={e => setConfirmPw(e.target.value)}
                       placeholder="Re-enter your password" required
                       className="w-full pl-11 pr-12 py-[13px] text-sm rounded-xl text-gray-900 placeholder-slate-400 outline-none transition-all"
                       style={{
                         background: '#fafafa',
                         border: `1.5px solid ${pwMatch ? '#10b981' : pwMismatch ? '#ef4444' : '#e2e8f0'}`,
                       }}
                       onFocus={e => { if (!pwMatch && !pwMismatch) e.currentTarget.style.borderColor = '#f59e0b' }}
                       onBlur={e => { if (!pwMatch && !pwMismatch) e.currentTarget.style.borderColor = '#e2e8f0' }} />
                <button type="button" onClick={() => setShowCpw(!showCpw)}
                        className="absolute right-3 top-1/2 -translate-y-1/2 p-1 text-slate-400 hover:text-navy-900 transition-colors">
                  {showCpw ? <EyeOff className="w-[18px] h-[18px]" /> : <Eye className="w-[18px] h-[18px]" />}
                </button>
              </div>
              {pwMatch    && <p className="text-[12px] mt-1 text-green-600 flex items-center gap-1"><CheckCircle className="w-3 h-3" /> Passwords match</p>}
              {pwMismatch && <p className="text-[12px] mt-1 text-red-500 flex items-center gap-1"><XCircle    className="w-3 h-3" /> Passwords do not match</p>}
            </div>

            {/* Submit */}
            <button type="submit" disabled={loading}
                    className="relative w-full flex items-center justify-center gap-2.5 py-[14px] rounded-xl text-[15px] font-semibold text-white transition-all mt-1 overflow-hidden group disabled:opacity-60 disabled:cursor-not-allowed"
                    style={{ background: 'linear-gradient(135deg, #0a1628 0%, #1a3158 100%)' }}>
              <div className="absolute inset-0 opacity-0 group-hover:opacity-100 transition-opacity"
                   style={{ background: 'linear-gradient(90deg, transparent, rgba(245,158,11,0.18), transparent)' }} />
              {loading
                ? <Loader2 className="w-5 h-5 animate-spin" />
                : <><span>Create Account</span><ArrowRight className="w-[18px] h-[18px]" /></>}
            </button>
          </form>

          <div className="text-center mt-5 pt-5 border-t border-gray-100">
            <p className="text-[13px] text-slate-500">
              Already have an account?{' '}
              <Link to="/login" className="font-semibold" style={{ color: '#f59e0b' }}>Sign In</Link>
            </p>
          </div>
        </div>
      </div>

      {/* ── OTP MODAL ── */}
      {modalOpen && (
        <div className="fixed inset-0 z-50 flex items-center justify-center"
             style={{ background: 'rgba(4,13,26,0.75)', backdropFilter: 'blur(6px)' }}
             onKeyDown={e => { if (e.key === 'Escape' && !verified) setModalOpen(false) }}>
          <div className="bg-white rounded-3xl p-10 w-[92%] max-w-[440px] animate-modal-in"
               style={{ boxShadow: '0 24px 64px rgba(0,0,0,0.4)' }}>

            {/* Icon */}
            <div className="flex justify-center mb-5">
              <div className={`w-[76px] h-[76px] rounded-full flex items-center justify-center ${verified ? '' : 'animate-icon-pulse'}`}
                   style={{ background: verified ? 'linear-gradient(135deg, #10b981, #059669)' : 'linear-gradient(135deg, #0a1628, #0f2040)' }}>
                {verified
                  ? <CheckCircle className="w-8 h-8 text-white" />
                  : <Mail className="w-8 h-8 text-white" />}
              </div>
            </div>

            <div className="text-center mb-2">
              <h2 className="text-[22px] font-bold text-navy-950">
                {verified ? 'Email Verified!' : 'Verify Your Email'}
              </h2>
              {!verified && (
                <p className="text-sm text-slate-500 mt-2 leading-relaxed">
                  We sent a 6-digit code to<br />
                  <span className="font-semibold" style={{ color: '#f59e0b' }}>{email}</span>
                </p>
              )}
              {verified && (
                <p className="text-sm text-slate-500 mt-2">Account created! Redirecting to dashboard...</p>
              )}
            </div>

            {!verified && (
              <>
                {verifyError && (
                  <div className="flex items-center gap-2 px-4 py-2.5 rounded-xl text-sm mb-2"
                       style={{ background: '#fef2f2', border: '1px solid #fecaca', color: '#991b1b' }}>
                    <AlertCircle className="w-4 h-4 flex-shrink-0" />
                    {verifyError}
                  </div>
                )}

                <OTPInput onComplete={handleVerify} />

                <div className="flex flex-col gap-2.5">
                  <button
                    onClick={() => { if (pendingCode.length === 6) handleVerify(pendingCode) }}
                    disabled={verifyLoading}
                    className="w-full py-[13px] rounded-xl text-[14px] font-semibold text-white flex items-center justify-center gap-2 transition-all disabled:opacity-60"
                    style={{ background: 'linear-gradient(135deg, #0a1628 0%, #1a3158 100%)' }}>
                    {verifyLoading ? <Loader2 className="w-5 h-5 animate-spin" /> : 'Verify Email'}
                  </button>
                  <button onClick={() => setModalOpen(false)}
                          className="w-full py-[13px] rounded-xl text-[14px] font-semibold text-slate-600 transition-all"
                          style={{ background: '#f1f5f9' }}>
                    Change Email
                  </button>
                </div>

                <p className="text-center text-[13px] text-slate-500 mt-4">
                  Didn't receive it?{' '}
                  <button
                    onClick={handleResend}
                    disabled={resendTimer > 0}
                    className="font-semibold disabled:text-slate-300 transition-colors"
                    style={{ color: resendTimer > 0 ? undefined : '#f59e0b' }}>
                    Resend Code {resendTimer > 0 && `(${resendTimer}s)`}
                  </button>
                </p>
              </>
            )}
          </div>
        </div>
      )}
    </div>
  )
}
