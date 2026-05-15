// src/pages/Login.tsx
// Drop-in replacement for the current Login page.
// Uses your existing useAuth() hook, react-router Link/useNavigate,
// and lucide-react icons that already ship with the project.
//
// CSS dependency: paste the block in `login.css.snippet` into the bottom
// of src/index.css (it adds the keyframes + .field / .btn-cta / .radar / .city / etc).

import { useEffect, useMemo, useRef, useState } from 'react'
import { useNavigate, Link } from 'react-router-dom'
import {
  Mail, Lock, Unlock, Eye, EyeOff, ArrowRight, Check, Loader2,
} from 'lucide-react'
import { useAuth } from '@/hooks/useAuth'

type Theme = 'radar' | 'aurora' | 'city'

const HOTEL_NAMES = [
  'HILTON · SF',        'MARRIOTT · CHI',    'HYATT · MIA',
  'FOUR SEASONS · NYC', 'ROSEWOOD · LA',     'ACCOR · ATL',
  'IHG · DEN',          'OMNI · DAL',        'KIMPTON · BOS',
]

export default function LoginPage() {
  // ── auth state ───────────────────────────────────────
  const [email, setEmail]       = useState('')
  const [pwd, setPwd]           = useState('')
  const [remember, setRemember] = useState(true)
  const [showPwd, setShowPwd]   = useState(false)
  const [focus, setFocus]       = useState<'email' | 'pwd' | null>(null)
  const [error, setError]       = useState('')
  const [status, setStatus]     = useState<'idle'|'loading'|'success'|'failure'>('idle')

  const { login } = useAuth()
  const navigate  = useNavigate()
  const cardRef   = useRef<HTMLDivElement>(null)

  // ── theme (persisted) ────────────────────────────────
  const [theme, setTheme] = useState<Theme>(() => {
    return (localStorage.getItem('slh.login.theme') as Theme) || 'radar'
  })
  useEffect(() => { localStorage.setItem('slh.login.theme', theme) }, [theme])

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
      // Router auto-redirects when isAuthenticated flips — no navigate needed
    } else {
      setStatus('failure')
      setError(result.error || 'Invalid email or password')
      setTimeout(() => setStatus('idle'), 600)
    }
  }

  // cursor-following glow
  function onMove(e: React.MouseEvent) {
    const el = cardRef.current; if (!el) return
    const r = el.getBoundingClientRect()
    el.style.setProperty('--mx', `${((e.clientX - r.left) / r.width)  * 100}%`)
    el.style.setProperty('--my', `${((e.clientY - r.top)  / r.height) * 100}%`)
  }

  const chars = Array.from('Welcome Back')

  return (
    <div className="login-scene relative w-screen h-screen flex items-center justify-center p-6">
      <div className="scene-bg" />
      <div className="grid-dots" />

      {/* tiny theme switcher (optional — delete if you don't want it) */}
      <div className="absolute top-4 right-4 z-30 flex gap-1 rounded-full bg-black/30 backdrop-blur p-1 text-[11px] font-mono tracking-widest uppercase">
        {(['radar','aurora','city'] as Theme[]).map(o => (
          <button key={o} type="button" onClick={() => setTheme(o)}
                  className={`px-3 py-1 rounded-full transition ${theme === o ? 'bg-amber-500 text-navy-950' : 'text-white/60 hover:text-white'}`}>
            {o}
          </button>
        ))}
      </div>

      <div className="card-wrap w-full max-w-[1020px]" style={{ perspective: 1400 }}>
        <div ref={cardRef} className="card flex" onMouseMove={onMove} style={{ minHeight: 600 }}>
          <div className="card-bezel" />

          {/* ── LEFT PANE ── */}
          <div className="left-panel hidden lg:flex flex-col items-center justify-center relative overflow-hidden"
               style={{ width: '46%' }}>
            <div className="top-strip" />
            {theme === 'radar'  && <RadarPanel />}
            {theme === 'aurora' && <AuroraPanel />}
            {theme === 'city'   && <CityPanel />}

            <div className="relative z-20 flex flex-col items-center px-10 -mt-6">
              <div className="logo-tile rounded-3xl bg-white flex items-center justify-center mb-6"
                   style={{ width: 160, height: 160, padding: '30px 22px' }}>
                <img src="/static/img/logo.svg" alt="JA Uniforms"
                     className="w-full h-full object-contain" />
              </div>
              <div className="text-center">
                <div className="text-white text-[22px] font-bold tracking-tight">Smart Lead Hunter</div>
                <div className="text-white/60 text-[12px] tracking-widest uppercase mt-1 font-mono">
                  J.A. Uniforms · Hotel Intelligence
                </div>
              </div>
            </div>
          </div>

          {/* ── RIGHT PANE ── */}
          <div className="flex-1 flex flex-col justify-center px-8 lg:px-14 py-12 relative">
            <div className="absolute top-5 right-6 flex items-center gap-2 text-[10.5px] font-mono tracking-widest text-slate-400 uppercase">
              <span className="w-1.5 h-1.5 rounded-full bg-emerald-500 animate-pulse" />
              <span>All systems green</span>
            </div>

            {/* mobile brand row */}
            <div className="flex items-center gap-3 mb-7 lg:hidden">
              <div className="bg-navy-950 rounded-xl p-2">
                <img src="/static/img/logo.svg" alt="" className="h-6 w-auto invert" />
              </div>
              <span className="font-bold text-navy-950">Smart Lead Hunter</span>
            </div>

            <form onSubmit={handleSubmit} className="flex flex-col gap-5 w-full max-w-[400px] mx-auto">
              <div className="mb-2">
                <h1 className="text-[28px] font-bold text-navy-950 tracking-tight leading-tight">
                  {chars.map((c, i) => (
                    <span key={i} className="reveal-char"
                          style={{ animationDelay: `${0.35 + i * 0.035}s` }}>
                      {c === ' ' ? '\u00A0' : c}
                    </span>
                  ))}
                </h1>
                <p className="text-[13.5px] text-slate-500 mt-1 reveal-char"
                   style={{ animationDelay: '0.95s' }}>
                  Sign in to keep hunting hotel leads.
                </p>
              </div>

              {error && (
                <div className="px-4 py-3 rounded-xl text-sm font-medium bg-red-50 border border-red-200 text-red-800 animate-slideIn">
                  {error}
                </div>
              )}

              {/* EMAIL */}
              <div>
                <label className="field-label block mb-1.5">Email address</label>
                <div className={`field ${focus==='email' ? 'is-focused' : ''}`}>
                  <Mail className="icon-lead w-[18px] h-[18px]" />
                  <input
                    type="email"
                    autoComplete="email"
                    value={email}
                    onChange={e => setEmail(e.target.value)}
                    onFocus={() => setFocus('email')}
                    onBlur={()  => setFocus(null)}
                    placeholder="you@jauniforms.com"
                    autoFocus
                    required
                  />
                  <span className={`icon-trail ${emailValid ? 'check-on text-emerald-500' : 'text-slate-300'}`}>
                    <Check className="w-[18px] h-[18px] check-stroke-wrap" />
                  </span>
                  <div className="field-border" />
                  <div className="field-underline" />
                </div>
              </div>

              {/* PASSWORD */}
              <div>
                <div className="flex items-center justify-between mb-1.5">
                  <label className="field-label">Password</label>
                  <Link to="/reset-password" className="text-[12px] font-semibold text-amber-600 hover:text-amber-700 transition">
                    Forgot password?
                  </Link>
                </div>
                <div className={`field ${focus==='pwd' ? 'is-focused' : ''}`}>
                  {pwd.length > 0
                    ? <Unlock className="icon-lead w-[18px] h-[18px]" />
                    : <Lock   className="icon-lead w-[18px] h-[18px]" />}
                  <input
                    type={showPwd ? 'text' : 'password'}
                    autoComplete="current-password"
                    value={pwd}
                    onChange={e => setPwd(e.target.value)}
                    onFocus={() => setFocus('pwd')}
                    onBlur={()  => setFocus(null)}
                    placeholder="Enter your password"
                    required
                  />
                  <button type="button" tabIndex={-1}
                          onClick={() => setShowPwd(s => !s)}
                          className="absolute right-3 top-1/2 -translate-y-1/2 p-1 text-slate-400 hover:text-navy-900 transition">
                    {showPwd ? <EyeOff className="w-[18px] h-[18px]" /> : <Eye className="w-[18px] h-[18px]" />}
                  </button>
                  <div className="field-border" />
                  <div className="field-underline" />
                </div>

                <div className="mt-2 h-[3px] rounded-full bg-slate-100 overflow-hidden">
                  <div
                    className="h-full rounded-full transition-all duration-500"
                    style={{
                      width: `${Math.min(pwd.length, 12) / 12 * 100}%`,
                      background: pwd.length < 6
                        ? 'linear-gradient(90deg,#f59e0b,#fbbf24)'
                        : 'linear-gradient(90deg,#10b981,#22c55e)',
                    }}
                  />
                </div>
              </div>

              {/* REMEMBER */}
              <label className="flex items-center gap-2.5 cursor-pointer select-none -mt-1">
                <span className={`cbox ${remember ? 'on' : ''}`}
                      onClick={(e) => { e.preventDefault(); setRemember(r => !r) }}>
                  <svg viewBox="0 0 24 24" width={18} height={18} fill="none"
                       stroke="white" strokeWidth={3} strokeLinecap="round" strokeLinejoin="round">
                    <polyline points="5 12 10 17 19 7"
                              style={{ strokeDasharray: 20, strokeDashoffset: remember ? 0 : 20,
                                       transition: 'stroke-dashoffset .35s ease' }} />
                  </svg>
                </span>
                <input type="checkbox" className="sr-only" checked={remember}
                       onChange={e => setRemember(e.target.checked)} />
                <span className="text-[13px] text-slate-600">Keep me signed in for 30 days</span>
              </label>

              {/* CTA */}
              <button
                type="submit"
                disabled={!canSubmit && status === 'idle'}
                className={`btn-cta ${status==='loading' ? 'loading' : ''} ${status==='success' ? 'success' : ''} ${status==='failure' ? 'failure' : ''}`}
              >
                {status === 'idle' && (
                  <span className="btn-label flex items-center gap-2">
                    Sign In <ArrowRight className="btn-arrow w-[18px] h-[18px]" />
                  </span>
                )}
                {status === 'loading' && <Loader2 className="w-5 h-5 animate-spin" />}
                {status === 'success' && (
                  <span className="btn-label flex items-center gap-2">
                    <Check className="w-[18px] h-[18px]" /> Welcome back
                  </span>
                )}
              </button>

              <div className="text-center pt-4 mt-1 border-t border-slate-100">
                <p className="text-[13px] text-slate-500">
                  Don't have an account?{' '}
                  <Link to="/register" className="font-semibold text-amber-600 hover:text-amber-700 transition">
                    Request access
                  </Link>
                </p>
              </div>
            </form>
          </div>
        </div>
      </div>
    </div>
  )
}

/* ─────────── Panel components ─────────── */

function RadarPanel() {
  const pins = useMemo(() => {
    return Array.from({ length: 7 }, (_, i) => {
      const r = 18 + Math.random() * 30
      const a = Math.random() * Math.PI * 2
      return {
        x: 50 + Math.cos(a) * r,
        y: 50 + Math.sin(a) * r,
        delay: (i * 4.0 / 7).toFixed(2),
        name: HOTEL_NAMES[i % HOTEL_NAMES.length],
      }
    })
  }, [])

  const [count, setCount] = useState(1247)
  useEffect(() => {
    const id = setInterval(() => setCount(c => c + Math.floor(Math.random() * 4) + 1), 1200)
    return () => clearInterval(id)
  }, [])

  return (
    <>
      <div className="scan-grid" />
      <div className="radar">
        <div className="radar-cross" style={{ position: 'absolute', inset: 0 }} />
        <div className="radar-ring" />
        <div className="radar-ring r2" />
        <div className="radar-ring r3" />
        <div className="radar-ring r4" />
        <div className="radar-sweep" />
        {pins.map((p, i) => (
          <div key={i}>
            <div className="radar-pin"
                 style={{ left: `${p.x}%`, top: `${p.y}%`, animationDelay: `-${p.delay}s` }}>
              <div className="pin-ring" />
            </div>
            <div className="radar-label"
                 style={{ left: `${p.x}%`, top: `${p.y}%`, animationDelay: `-${p.delay}s` }}>
              {p.name}
            </div>
          </div>
        ))}
      </div>

      <div className="absolute top-5 left-5 right-5 flex items-start justify-between text-[10px] font-mono tracking-widest text-amber-300/80 uppercase z-20">
        <div className="flex items-center gap-2">
          <span className="pulse-dot" /><span>Live scan</span>
        </div>
        <div className="text-right">
          <div className="text-amber-300/60">Pipeline ▴</div>
          <div className="text-white text-[13px] font-semibold tabular-nums">
            {count.toLocaleString()}<span className="text-amber-300/70 text-[10px] ml-1">leads</span>
          </div>
        </div>
      </div>
    </>
  )
}

function AuroraPanel() {
  const dust = useMemo(() => Array.from({ length: 28 }, () => ({
    left: Math.random() * 100, top: Math.random() * 100,
    d: (2 + Math.random() * 3).toFixed(2),
    delay: (Math.random() * -4).toFixed(2),
    size: 1 + Math.random() * 1.5,
  })), [])
  return (
    <>
      <div className="aurora">
        <div className="blob" style={{ width:320, height:320, top:'-10%', left:'-10%',
          background:'radial-gradient(circle, rgba(245,158,11,0.55), transparent 70%)' }} />
        <div className="blob" style={{ width:380, height:380, bottom:'-12%', right:'-8%', animationDelay:'-5s',
          background:'radial-gradient(circle, rgba(62,99,140,0.65), transparent 70%)' }} />
        <div className="blob" style={{ width:260, height:260, top:'30%', right:'10%', animationDelay:'-9s',
          background:'radial-gradient(circle, rgba(251,191,36,0.30), transparent 70%)' }} />
        <div className="aurora-rings" />
        {dust.map((d, i) => (
          <div key={i} className="dust"
               style={{ left:`${d.left}%`, top:`${d.top}%`, width:`${d.size}px`, height:`${d.size}px`,
                        animation:`twinkle ${d.d}s ease-in-out infinite`,
                        animationDelay:`${d.delay}s` }} />
        ))}
      </div>
    </>
  )
}

function CityPanel() {
  const buildings = useMemo(() => {
    const N = 11
    return Array.from({ length: N }, (_, i) => {
      const t = Math.abs(i - (N - 1) / 2) / ((N - 1) / 2)
      const h = Math.max(28, Math.min(82, 70 - t * 30 + (Math.random() * 20 - 10)))
      return {
        h,
        windows: Array.from({ length: Math.floor(h / 8) * 3 }, (_, k) => ({
          x: 10 + (k % 3) * 35,
          y: 12 + Math.floor(k / 3) * 16,
          delay: (Math.random() * -6).toFixed(2),
        })),
      }
    })
  }, [])
  const stars = useMemo(() => Array.from({ length: 26 }, () => ({
    left: Math.random() * 100, top: Math.random() * 50,
    delay: (Math.random() * -3).toFixed(2),
  })), [])
  return (
    <div className="city">
      {stars.map((s, i) => (
        <div key={i} className="city-star"
             style={{ left:`${s.left}%`, top:`${s.top}%`, animationDelay:`${s.delay}s` }} />
      ))}
      <div className="city-row" style={{ height:'60%' }}>
        {buildings.map((b, i) => (
          <div key={i} className="city-bldg" style={{ height:`${b.h}%` }}>
            {b.windows.map((w, j) => (
              <div key={j} className="city-window"
                   style={{ left:`${w.x}%`, top:`${w.y}%`, animationDelay:`${w.delay}s` }} />
            ))}
          </div>
        ))}
      </div>
      <div className="city-haze" />
    </div>
  )
}
