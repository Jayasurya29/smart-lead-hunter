import { useState } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import {
  Mail, Lock, Eye, EyeOff, Check, ArrowRight, Loader2,
} from 'lucide-react'
import { useAuth } from '@/hooks/useAuth'
import JaLogo from '@/components/auth/JaLogo'
import RadarMap from '@/components/auth/RadarMap'
import '@/styles/auth.css'

type Status = 'idle' | 'loading' | 'success' | 'failure'

export default function LoginPage() {
  const [email,    setEmail]    = useState('')
  const [pwd,      setPwd]      = useState('')
  const [remember, setRemember] = useState(true)
  const [showPwd,  setShowPwd]  = useState(false)
  const [focus,    setFocus]    = useState<'email' | 'pwd' | null>(null)
  const [error,    setError]    = useState('')
  const [status,   setStatus]   = useState<Status>('idle')

  const { login } = useAuth()
  const navigate  = useNavigate()

  const emailValid = /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)
  const canSubmit  = emailValid && pwd.length >= 1 && status === 'idle'

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    if (!canSubmit) return
    setError('')
    setStatus('loading')
    const result = await login(email.trim(), pwd, remember)
    if (result.success) {
      setStatus('success')
      setTimeout(() => navigate('/dashboard'), 600)
    } else {
      setStatus('failure')
      setError(result.error || 'Invalid email or password')
      setTimeout(() => setStatus('idle'), 600)
    }
  }

  return (
    <div className="auth-scene">
      <div className="auth-split">
        <RadarMap />

        <div className="form-pane">

          {/* Mobile brand row */}
          <div className="mobile-brand px-8 pt-8 items-center gap-3">
            <JaLogo size="md" />
            <div className="font-mono-detail" style={{ fontSize: 9, letterSpacing: '0.20em', textTransform: 'uppercase', color: 'var(--auth-ink-500)' }}>
              AI-Powered · Hotel Intelligence
            </div>
          </div>

          {/* Top-right: link to register */}
          <div className="desktop-top-right justify-end px-12 pt-9">
            <div className="font-mono-detail" style={{ fontSize: 10.5, letterSpacing: '0.16em', textTransform: 'uppercase', color: 'var(--auth-ink-500)' }}>
              New here? <Link to="/register" className="link-brass ml-1">Request access</Link>
            </div>
          </div>

          <div className="flex-1 flex items-center px-8 lg:px-16 relative z-10">
            <form onSubmit={handleSubmit} className="w-full max-w-[420px] mx-auto">

              <div className="mb-10">
                <div className="font-mono-detail reveal-word"
                     style={{ fontSize: 10.5, letterSpacing: '0.22em', textTransform: 'uppercase', color: 'var(--auth-brass-600)', marginBottom: 12, animationDelay: '0.05s' }}>
                  ⎯ Sign in
                </div>
                <h1 className="font-serif-display" style={{ fontSize: 44, lineHeight: 1.05, letterSpacing: '-0.01em', color: 'var(--auth-ink-900)' }}>
                  {['Welcome', 'back.'].map((w, i) => (
                    <span
                      key={i}
                      className="reveal-word"
                      style={{
                        animationDelay: `${0.15 + i * 0.12}s`,
                        marginRight: 12,
                        fontStyle:  i === 1 ? 'italic' : 'normal',
                        fontWeight: i === 1 ? 400 : 500,
                      }}
                    >
                      {w}
                    </span>
                  ))}
                </h1>
                <p className="reveal-word" style={{ fontSize: 14, color: 'var(--auth-ink-500)', marginTop: 12, animationDelay: '0.55s' }}>
                  Continue tracking pre-opening hotels across USA &amp; Caribbean.
                </p>
              </div>

              {error && (
                <div className="err-box" style={{ marginBottom: 24 }}>
                  <span style={{ width: 6, height: 6, borderRadius: '50%', background: '#d14836', display: 'inline-block' }} />
                  {error}
                </div>
              )}

              {/* Email */}
              <div style={{ marginBottom: 28 }}>
                <label className="auth-field-label" style={{ display: 'block', marginBottom: 8 }}>Work email</label>
                <div className={`field ${focus === 'email' ? 'is-focused' : ''} ${emailValid ? 'is-valid' : ''}`}>
                  <span className="field-icon"><Mail size={18} strokeWidth={1.6} /></span>
                  <input
                    type="email"
                    autoComplete="email"
                    value={email}
                    onChange={(e) => setEmail(e.target.value)}
                    onFocus={() => setFocus('email')}
                    onBlur={() => setFocus(null)}
                    placeholder="you@company.com"
                    autoFocus
                    required
                  />
                  <span className="field-trail">
                    <span className="check"><Check size={18} strokeWidth={2.2} /></span>
                  </span>
                </div>
              </div>

              {/* Password */}
              <div style={{ marginBottom: 20 }}>
                <div className="flex items-baseline justify-between" style={{ marginBottom: 8 }}>
                  <label className="auth-field-label">Password</label>
                  <Link to="/reset-password" className="font-mono-detail link-brass"
                        style={{ fontSize: 10, letterSpacing: '0.14em', textTransform: 'uppercase' }}>
                    Forgot?
                  </Link>
                </div>
                <div className={`field ${focus === 'pwd' ? 'is-focused' : ''}`}>
                  <span className="field-icon"><Lock size={18} strokeWidth={1.6} /></span>
                  <input
                    type={showPwd ? 'text' : 'password'}
                    autoComplete="current-password"
                    value={pwd}
                    onChange={(e) => setPwd(e.target.value)}
                    onFocus={() => setFocus('pwd')}
                    onBlur={() => setFocus(null)}
                    placeholder="Enter your password"
                    required
                  />
                  <span className="field-trail">
                    <button type="button" tabIndex={-1} onClick={() => setShowPwd((s) => !s)}>
                      {showPwd ? <EyeOff size={18} strokeWidth={1.6} /> : <Eye size={18} strokeWidth={1.6} />}
                    </button>
                  </span>
                </div>
              </div>

              {/* Remember me */}
              <label className="flex items-center gap-3 cursor-pointer select-none" style={{ marginBottom: 36, marginTop: 16 }}>
                <span
                  className={`cbox ${remember ? 'on' : ''}`}
                  onClick={(e) => { e.preventDefault(); setRemember((r) => !r) }}
                >
                  <svg viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="white" strokeWidth="3" strokeLinecap="round" strokeLinejoin="round">
                    <polyline points="5 12 10 17 19 7"
                      style={{ strokeDasharray: 22, strokeDashoffset: remember ? 0 : 22, transition: 'stroke-dashoffset 0.3s ease' }} />
                  </svg>
                </span>
                <input type="checkbox" className="sr-only" checked={remember} onChange={(e) => setRemember(e.target.checked)} />
                <span style={{ fontSize: 13, color: 'var(--auth-ink-700)' }}>Keep me signed in for 30 days</span>
              </label>

              {/* CTA */}
              <button
                type="submit"
                disabled={!canSubmit}
                className={`btn-cta ${status === 'loading' ? 'loading' : ''} ${status === 'success' ? 'success' : ''}`}
              >
                {status === 'idle' && (
                  <>
                    <span>Sign in</span>
                    <span className="arrow"><ArrowRight size={16} strokeWidth={1.8} /></span>
                  </>
                )}
                {status === 'loading' && (
                  <>
                    <Loader2 size={18} className="animate-spin" />
                    <span>Authenticating</span>
                  </>
                )}
                {status === 'success' && (
                  <>
                    <Check size={18} strokeWidth={2.2} />
                    <span>Welcome back</span>
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
    </div>
  )
}
