import { useCallback, useRef, useState } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import {
  Mail, Lock, Eye, EyeOff, User, Briefcase,
  ArrowRight, Loader2, Check, AlertCircle,
} from 'lucide-react'
import api from '@/api/client'
import JaLogo from '@/components/auth/JaLogo'
import RadarMap from '@/components/auth/RadarMap'
import '@/styles/auth.css'

// ─── Password strength helpers ────────────────────────────────────────────
function getStrength(pw: string) {
  let score = 0
  if (pw.length >= 8) score++
  if (/[A-Z]/.test(pw)) score++
  if (/[a-z]/.test(pw)) score++
  if (/\d/.test(pw))    score++
  return score
}
const STRENGTH_LABEL = ['', 'Weak', 'Fair', 'Good', 'Strong']

// ─── OTP Input ────────────────────────────────────────────────────────────
interface OTPHandle {
  shake:  () => void
  clear:  () => void
  focus:  () => void
}

function OTPInput({ onComplete, handleRef }: {
  onComplete: (code: string) => void
  handleRef: React.MutableRefObject<OTPHandle | null>
}) {
  const [values, setValues] = useState(['', '', '', '', '', ''])
  const [shake,  setShake]  = useState(false)
  const inputs = useRef<(HTMLInputElement | null)[]>([])

  handleRef.current = {
    shake: () => { setShake(true); setTimeout(() => setShake(false), 400) },
    clear: () => { setValues(['', '', '', '', '', '']); inputs.current[0]?.focus() },
    focus: () => { inputs.current[0]?.focus() },
  }

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

  return (
    <div
      className={`flex gap-2 justify-center ${shake ? 'otp-shake' : ''}`}
      style={{ marginTop: 24, marginBottom: 24 }}
    >
      {values.map((v, i) => (
        <input
          key={i}
          ref={(el) => { inputs.current[i] = el }}
          type="text"
          inputMode="numeric"
          maxLength={1}
          value={v}
          onChange={(e) => handleChange(i, e.target.value)}
          onKeyDown={(e) => handleKeyDown(i, e)}
          onPaste={handlePaste}
          style={{
            width: 48, height: 56,
            textAlign: 'center',
            fontFamily: "'JetBrains Mono', monospace",
            fontSize: 22, fontWeight: 500,
            color: 'var(--auth-ink-900)',
            background: v ? 'rgba(196,148,79,0.08)' : '#fff',
            border: `1.5px solid ${v ? 'var(--auth-brass-500)' : 'var(--auth-cream-300)'}`,
            borderRadius: 4,
            outline: 'none',
          }}
        />
      ))}
    </div>
  )
}

// ─── Page ─────────────────────────────────────────────────────────────────
export default function RegisterPage() {
  const navigate = useNavigate()

  // Form state
  const [firstName, setFirstName] = useState('')
  const [lastName,  setLastName]  = useState('')
  const [email,     setEmail]     = useState('')
  const [role,      setRole]      = useState<'sales' | 'admin'>('sales')
  const [password,  setPassword]  = useState('')
  const [confirmPw, setConfirmPw] = useState('')
  const [showPwd,   setShowPwd]   = useState(false)
  const [focus,     setFocus]     = useState<string | null>(null)
  const [formError, setFormError] = useState('')
  const [loading,   setLoading]   = useState(false)

  // OTP state
  const [modalOpen,     setModalOpen]     = useState(false)
  const [verified,      setVerified]      = useState(false)
  const [verifyLoading, setVerifyLoading] = useState(false)
  const [verifyError,   setVerifyError]   = useState('')
  const [resendTimer,   setResendTimer]   = useState(0)
  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null)
  const otpHandleRef = useRef<OTPHandle | null>(null)

  const strength    = getStrength(password)
  const pwMatch     = confirmPw !== '' && password === confirmPw
  const pwMismatch  = confirmPw !== '' && password !== confirmPw
  const emailValid  = /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)
  const nameValid   = firstName.trim().length >= 2 && lastName.trim().length >= 2

  const canSubmit =
    nameValid && emailValid && pwMatch && password.length >= 8 && strength >= 2

  function startResendTimer() {
    setResendTimer(60)
    if (timerRef.current) clearInterval(timerRef.current)
    timerRef.current = setInterval(() => {
      setResendTimer((t) => {
        if (t <= 1) { clearInterval(timerRef.current!); return 0 }
        return t - 1
      })
    }, 1000)
  }

  // Step 1: Register → trigger OTP
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
        setTimeout(() => otpHandleRef.current?.focus(), 100)
      } else {
        setFormError(resp.data.error || 'Registration failed')
      }
    } catch (err: any) {
      setFormError(err.response?.data?.detail || err.response?.data?.error || 'Registration failed')
    } finally {
      setLoading(false)
    }
  }

  // Step 2: Verify OTP
  async function handleVerify(code: string) {
    if (code.length !== 6) return
    setVerifyError('')
    setVerifyLoading(true)
    try {
      const resp = await api.post('/auth/verify-code', {
        email: email.trim(),
        code,
      })
      if (resp.data.success) {
        setVerified(true)
        setTimeout(() => navigate('/dashboard'), 1200)
      } else {
        setVerifyError(resp.data.error || 'Invalid code')
        otpHandleRef.current?.shake()
        otpHandleRef.current?.clear()
      }
    } catch (err: any) {
      setVerifyError(err.response?.data?.detail || 'Verification failed')
      otpHandleRef.current?.shake()
      otpHandleRef.current?.clear()
    } finally {
      setVerifyLoading(false)
    }
  }

  async function handleResend() {
    if (resendTimer > 0) return
    try {
      await api.post('/auth/resend-code', { email: email.trim() })
      startResendTimer()
      otpHandleRef.current?.clear()
    } catch { /* swallow */ }
  }

  return (
    <div className="auth-scene">
      <div className="auth-split">
        <RadarMap />

        <div className="form-pane" style={{ overflowY: 'auto' }}>

          <div className="mobile-brand px-8 pt-8 items-center gap-3">
            <JaLogo size="md" />
            <div className="font-mono-detail" style={{ fontSize: 9, letterSpacing: '0.20em', textTransform: 'uppercase', color: 'var(--auth-ink-500)' }}>
              AI-Powered · Hotel Intelligence
            </div>
          </div>

          <div className="desktop-top-right justify-end px-12 pt-9">
            <div className="font-mono-detail" style={{ fontSize: 10.5, letterSpacing: '0.16em', textTransform: 'uppercase', color: 'var(--auth-ink-500)' }}>
              Already approved? <Link to="/login" className="link-brass ml-1">Sign in</Link>
            </div>
          </div>

          <div className="flex-1 flex items-center px-8 lg:px-16 relative z-10" style={{ paddingTop: 32, paddingBottom: 32 }}>
            <form onSubmit={handleSubmit} className="w-full max-w-[460px] mx-auto">

              <div style={{ marginBottom: 32 }}>
                <div className="font-mono-detail reveal-word"
                     style={{ fontSize: 10.5, letterSpacing: '0.22em', textTransform: 'uppercase', color: 'var(--auth-brass-600)', marginBottom: 12, animationDelay: '0.05s' }}>
                  ⎯ Request access
                </div>
                <h1 className="font-serif-display" style={{ fontSize: 40, lineHeight: 1.05, letterSpacing: '-0.01em', color: 'var(--auth-ink-900)' }}>
                  {['Join', 'the', 'hunt.'].map((w, i) => (
                    <span
                      key={i}
                      className="reveal-word"
                      style={{
                        animationDelay: `${0.15 + i * 0.10}s`,
                        marginRight: 8,
                        fontStyle:  i === 2 ? 'italic' : 'normal',
                        fontWeight: i === 2 ? 400 : 500,
                      }}
                    >
                      {w}
                    </span>
                  ))}
                </h1>
                <p className="reveal-word" style={{ fontSize: 13.5, color: 'var(--auth-ink-500)', marginTop: 12, animationDelay: '0.55s' }}>
                  Access is invite-reviewed. Tell us about your team — we&rsquo;ll be in touch within 24 hours.
                </p>
              </div>

              {formError && (
                <div className="err-box" style={{ marginBottom: 24 }}>
                  <AlertCircle size={14} />
                  {formError}
                </div>
              )}

              {/* First + Last name */}
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 20, marginBottom: 20 }}>
                <div>
                  <label className="auth-field-label" style={{ display: 'block', marginBottom: 8 }}>First name</label>
                  <div className={`field ${focus === 'fn' ? 'is-focused' : ''}`}>
                    <span className="field-icon"><User size={18} strokeWidth={1.6} /></span>
                    <input type="text" autoComplete="given-name" value={firstName}
                           onChange={(e) => setFirstName(e.target.value)}
                           onFocus={() => setFocus('fn')} onBlur={() => setFocus(null)}
                           placeholder="Nico" required autoFocus />
                  </div>
                </div>
                <div>
                  <label className="auth-field-label" style={{ display: 'block', marginBottom: 8 }}>Last name</label>
                  <div className={`field ${focus === 'ln' ? 'is-focused' : ''}`}>
                    <span className="field-icon"><User size={18} strokeWidth={1.6} /></span>
                    <input type="text" autoComplete="family-name" value={lastName}
                           onChange={(e) => setLastName(e.target.value)}
                           onFocus={() => setFocus('ln')} onBlur={() => setFocus(null)}
                           placeholder="Leal" required />
                  </div>
                </div>
              </div>

              {/* Email */}
              <div style={{ marginBottom: 20 }}>
                <label className="auth-field-label" style={{ display: 'block', marginBottom: 8 }}>Work email</label>
                <div className={`field ${focus === 'email' ? 'is-focused' : ''} ${emailValid ? 'is-valid' : ''}`}>
                  <span className="field-icon"><Mail size={18} strokeWidth={1.6} /></span>
                  <input type="email" autoComplete="email" value={email}
                         onChange={(e) => setEmail(e.target.value)}
                         onFocus={() => setFocus('email')} onBlur={() => setFocus(null)}
                         placeholder="you@jauniforms.com" required />
                  <span className="field-trail">
                    <span className="check"><Check size={18} strokeWidth={2.2} /></span>
                  </span>
                </div>
                <p className="font-mono-detail" style={{ fontSize: 10, letterSpacing: '0.14em', textTransform: 'uppercase', color: 'var(--auth-ink-400)', marginTop: 6 }}>
                  A verification code will be sent here
                </p>
              </div>

              {/* Role */}
              <div style={{ marginBottom: 20 }}>
                <label className="auth-field-label" style={{ display: 'block', marginBottom: 8 }}>Role</label>
                <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8 }}>
                  {(['sales', 'admin'] as const).map((opt) => (
                    <button
                      key={opt}
                      type="button"
                      onClick={() => setRole(opt)}
                      style={{
                        display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 8,
                        padding: '12px 0',
                        fontSize: 13,
                        fontWeight: role === opt ? 600 : 500,
                        color: role === opt ? 'var(--auth-ink-900)' : 'var(--auth-ink-500)',
                        background: role === opt ? 'rgba(196,148,79,0.08)' : 'transparent',
                        border: `1px solid ${role === opt ? 'var(--auth-brass-500)' : 'var(--auth-cream-300)'}`,
                        borderRadius: 4,
                        cursor: 'pointer',
                        transition: 'all 0.2s ease',
                        textTransform: 'uppercase',
                        letterSpacing: '0.10em',
                      }}
                    >
                      <Briefcase size={14} color={role === opt ? 'var(--auth-brass-600)' : 'var(--auth-ink-400)'} />
                      {opt === 'sales' ? 'Sales' : 'Admin'}
                    </button>
                  ))}
                </div>
              </div>

              {/* Password */}
              <div style={{ marginBottom: 20 }}>
                <div className="flex items-baseline justify-between" style={{ marginBottom: 8 }}>
                  <label className="auth-field-label">Password</label>
                  {password.length > 0 && (
                    <span className="font-mono-detail" style={{
                      fontSize: 10, letterSpacing: '0.14em', textTransform: 'uppercase',
                      color: strength >= 3 ? 'var(--auth-green)' : strength >= 1 ? 'var(--auth-brass-600)' : 'var(--auth-ink-400)',
                    }}>
                      {STRENGTH_LABEL[strength] || '8+ chars'}
                    </span>
                  )}
                </div>
                <div className={`field ${focus === 'pwd' ? 'is-focused' : ''}`}>
                  <span className="field-icon"><Lock size={18} strokeWidth={1.6} /></span>
                  <input type={showPwd ? 'text' : 'password'} autoComplete="new-password"
                         value={password} onChange={(e) => setPassword(e.target.value)}
                         onFocus={() => setFocus('pwd')} onBlur={() => setFocus(null)}
                         placeholder="Create a password" required minLength={8} />
                  <span className="field-trail">
                    <button type="button" tabIndex={-1} onClick={() => setShowPwd((s) => !s)}>
                      {showPwd ? <EyeOff size={18} strokeWidth={1.6} /> : <Eye size={18} strokeWidth={1.6} />}
                    </button>
                  </span>
                </div>
                {password.length > 0 && (
                  <div style={{ marginTop: 8, display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 4 }}>
                    {[0, 1, 2, 3].map((i) => (
                      <div
                        key={i}
                        style={{
                          height: 3,
                          borderRadius: 2,
                          background:
                            i < strength
                              ? strength >= 3 ? 'var(--auth-green)' : 'var(--auth-brass-500)'
                              : 'var(--auth-cream-200)',
                        }}
                      />
                    ))}
                  </div>
                )}
              </div>

              {/* Confirm password */}
              <div style={{ marginBottom: 28 }}>
                <label className="auth-field-label" style={{ display: 'block', marginBottom: 8 }}>Confirm password</label>
                <div className={`field ${focus === 'cpwd' ? 'is-focused' : ''} ${pwMatch ? 'is-valid' : ''}`}>
                  <span className="field-icon"><Lock size={18} strokeWidth={1.6} /></span>
                  <input type={showPwd ? 'text' : 'password'} autoComplete="new-password"
                         value={confirmPw} onChange={(e) => setConfirmPw(e.target.value)}
                         onFocus={() => setFocus('cpwd')} onBlur={() => setFocus(null)}
                         placeholder="Re-enter password" required />
                  <span className="field-trail">
                    <span className="check"><Check size={18} strokeWidth={2.2} /></span>
                  </span>
                </div>
                {pwMismatch && (
                  <div style={{ marginTop: 8, fontSize: 12, color: '#d14836' }}>Passwords don&rsquo;t match.</div>
                )}
              </div>

              {/* CTA */}
              <button
                type="submit"
                disabled={!canSubmit || loading}
                className={`btn-cta ${loading ? 'loading' : ''}`}
              >
                {loading ? (
                  <>
                    <Loader2 size={18} className="animate-spin" />
                    <span>Submitting</span>
                  </>
                ) : (
                  <>
                    <span>Request access</span>
                    <span className="arrow"><ArrowRight size={16} strokeWidth={1.8} /></span>
                  </>
                )}
                <span className="brass-edge" />
              </button>

              {/* Footer */}
              <div style={{ marginTop: 32, paddingTop: 24, borderTop: '1px solid var(--auth-cream-200)', display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                <div className="ja-credit">
                  <JaLogo size="xs" />
                  <span>A J.A. Uniforms product</span>
                </div>
                <span className="font-mono-detail" style={{ fontSize: 10, letterSpacing: '0.12em', color: 'var(--auth-ink-400)' }}>v1.0 · 26.05</span>
              </div>
            </form>
          </div>
        </div>
      </div>

      {/* ─── OTP Verification Modal ─── */}
      {modalOpen && (
        <div
          role="dialog"
          aria-modal="true"
          onKeyDown={(e) => { if (e.key === 'Escape' && !verified) setModalOpen(false) }}
          style={{
            position: 'fixed', inset: 0, zIndex: 50,
            display: 'flex', alignItems: 'center', justifyContent: 'center',
            background: 'rgba(4, 13, 26, 0.75)',
            backdropFilter: 'blur(6px)',
          }}
        >
          <div
            className="form-pane"
            style={{
              position: 'relative',
              width: '92%', maxWidth: 460,
              padding: '40px 40px 36px',
              background: 'var(--auth-cream-50)',
              boxShadow: '0 24px 64px rgba(0, 0, 0, 0.4)',
              animation: 'auth-revealUp 0.4s cubic-bezier(0.16, 1, 0.3, 1) both',
            }}
          >
            <div style={{ textAlign: 'center', marginBottom: 8 }}>
              <div style={{
                width: 64, height: 64, margin: '0 auto 16px',
                borderRadius: '50%',
                display: 'flex', alignItems: 'center', justifyContent: 'center',
                background: verified ? 'var(--auth-green)' : 'var(--auth-navy-900)',
                color: '#fff',
              }}>
                {verified ? <Check size={28} strokeWidth={2.2} /> : <Mail size={26} strokeWidth={1.6} />}
              </div>
              <h2 className="font-serif-display" style={{ fontSize: 28, color: 'var(--auth-ink-900)' }}>
                {verified ? 'Email verified.' : 'Verify your email'}
              </h2>
              <p style={{ fontSize: 13, color: 'var(--auth-ink-500)', marginTop: 8, lineHeight: 1.5 }}>
                {verified ? (
                  <>Account created. Redirecting to your dashboard&hellip;</>
                ) : (
                  <>
                    We sent a 6-digit code to<br />
                    <span style={{ color: 'var(--auth-brass-600)', fontWeight: 500 }}>{email}</span>
                  </>
                )}
              </p>
            </div>

            {!verified && (
              <>
                {verifyError && (
                  <div className="err-box" style={{ marginTop: 16 }}>
                    <AlertCircle size={14} />
                    {verifyError}
                  </div>
                )}

                <OTPInput onComplete={handleVerify} handleRef={otpHandleRef} />

                <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
                  <button
                    type="button"
                    onClick={() => { /* OTPInput auto-submits at 6 chars */ }}
                    disabled={verifyLoading}
                    className={`btn-cta ${verifyLoading ? 'loading' : ''}`}
                  >
                    {verifyLoading ? (
                      <>
                        <Loader2 size={18} className="animate-spin" />
                        <span>Verifying</span>
                      </>
                    ) : (
                      <span>Verify email</span>
                    )}
                    <span className="brass-edge" />
                  </button>
                  <button
                    type="button"
                    onClick={() => setModalOpen(false)}
                    style={{
                      background: 'transparent',
                      border: '1px solid var(--auth-cream-300)',
                      color: 'var(--auth-ink-700)',
                      fontFamily: "'DM Sans', sans-serif",
                      fontSize: 13.5, fontWeight: 500,
                      letterSpacing: '0.18em', textTransform: 'uppercase',
                      height: 48,
                      cursor: 'pointer',
                    }}
                  >
                    Change email
                  </button>
                </div>

                <p style={{ textAlign: 'center', fontSize: 12.5, color: 'var(--auth-ink-500)', marginTop: 16 }}>
                  Didn&rsquo;t receive it?{' '}
                  <button
                    type="button"
                    onClick={handleResend}
                    disabled={resendTimer > 0}
                    className="link-brass"
                    style={{
                      background: 'none', border: 0,
                      cursor: resendTimer > 0 ? 'default' : 'pointer',
                      color: resendTimer > 0 ? 'var(--auth-ink-400)' : 'var(--auth-brass-600)',
                      fontFamily: 'inherit', fontSize: 'inherit', fontWeight: 600,
                    }}
                  >
                    Resend code{resendTimer > 0 ? ` (${resendTimer}s)` : ''}
                  </button>
                </p>
              </>
            )}
          </div>
        </div>
      )}

      <style>{`
        .otp-shake { animation: otp-shake 0.4s ease; }
        @keyframes otp-shake {
          0%, 100% { transform: translateX(0); }
          25%      { transform: translateX(-8px); }
          75%      { transform: translateX(8px); }
        }
      `}</style>
    </div>
  )
}
