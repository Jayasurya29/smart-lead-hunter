import { useState } from 'react'
import { useNavigate, Link } from 'react-router-dom'
import { useAuth } from '@/hooks/useAuth'
import { Mail, Lock, Eye, EyeOff, ArrowRight, Loader2 } from 'lucide-react'

export default function LoginPage() {
  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [remember, setRemember] = useState(false)
  const [showPwd, setShowPwd] = useState(false)
  const [error, setError] = useState('')
  const [loading, setLoading] = useState(false)
  const { login } = useAuth()
  const navigate = useNavigate()

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    setError('')
    setLoading(true)
    const result = await login(email.trim(), password, remember)
    if (result.success) {
      navigate('/dashboard')
    } else {
      setError(result.error || 'Invalid email or password')
      setLoading(false)
    }
  }

  return (
    <div className="min-h-screen flex items-center justify-center p-5 relative overflow-hidden animate-gradient-shift"
         style={{ background: 'linear-gradient(135deg, #040d1a 0%, #0a1628 50%, #0f2040 100%)' }}>

      {/* Dot grid overlay */}
      <div className="absolute inset-0 pointer-events-none animate-dot-grid"
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
           style={{ background: 'rgba(255,255,255,0.97)', boxShadow: '0 24px 64px rgba(0,0,0,0.4)', minHeight: 560 }}>

        {/* ── LEFT PANEL ── */}
        <div className="hidden lg:flex lg:w-[42%] flex-col items-center justify-center px-10 py-14 relative overflow-hidden"
             style={{ background: 'linear-gradient(135deg, #0a1628 0%, #0f2040 100%)' }}>

          {/* Amber top bar */}
          <div className="absolute top-0 left-0 right-0 h-[3px]"
               style={{ background: 'linear-gradient(90deg, #f59e0b, #fbbf24, #f59e0b)' }} />

          {/* Dot pattern */}
          <div className="absolute inset-0 pointer-events-none animate-pattern-move"
               style={{
                 backgroundImage: 'radial-gradient(circle, rgba(245,158,11,0.07) 1px, transparent 1px)',
                 backgroundSize: '28px 28px',
                 top: '-50%', left: '-50%', width: '200%', height: '200%',
               }} />

          {/* Floating logo */}
          <div className="animate-logo-float relative z-10 flex items-center justify-center rounded-[28px] bg-white mb-7"
               style={{ width: 172, height: 172, padding: 24, boxShadow: '0 20px 60px rgba(0,0,0,0.35)' }}>
            <img
              src="/static/img/logo.svg"
              alt="JA Uniforms"
              className="w-full h-full object-contain"
              onError={(e) => {
                const t = e.currentTarget
                t.style.display = 'none'
                const fallback = t.nextElementSibling as HTMLElement
                if (fallback) fallback.style.display = 'flex'
              }}
            />
            <div style={{ display: 'none' }}
                 className="w-full h-full items-center justify-center text-4xl font-black text-navy-950">
              JA
            </div>
          </div>

          {/* Brand */}
          <div className="text-center relative z-10">
            <div className="text-white text-[22px] font-bold tracking-tight mb-1">Smart Lead Hunter</div>
            <div className="text-white/60 text-[13px] tracking-wide">J.A. Uniforms · Hotel Intelligence</div>

            {/* Live badge */}
            <div className="inline-flex items-center gap-2 mt-4 px-3 py-[5px] rounded-full text-[11px] tracking-widest uppercase"
                 style={{ background: 'rgba(245,158,11,0.14)', border: '1px solid rgba(245,158,11,0.28)', color: '#fcd34d' }}>
              <span className="w-1.5 h-1.5 rounded-full bg-amber-400 animate-pulse-dot" />
              Pipeline Active
            </div>
          </div>
        </div>

        {/* ── RIGHT PANEL ── */}
        <div className="flex-1 flex flex-col justify-center px-10 lg:px-14 py-14">

          {/* Mobile logo */}
          <div className="flex items-center gap-3 mb-8 lg:hidden">
            <img src="/static/img/ja-logo.jpg" alt="JA Uniforms" className="h-9 w-auto rounded" />
            <span className="font-bold text-gray-900">Smart Lead Hunter</span>
          </div>

          <div className="mb-8">
            <h1 className="text-[26px] font-bold text-navy-950 tracking-tight mb-1">Welcome Back!</h1>
            <p className="text-sm text-slate-500">Sign in to your account</p>
          </div>

          {/* Error alert */}
          {error && (
            <div className="flex items-center gap-2 mb-5 px-4 py-3 rounded-xl text-sm font-medium animate-slideDown"
                 style={{ background: '#fef2f2', border: '1px solid #fecaca', color: '#991b1b' }}>
              <span>{error}</span>
            </div>
          )}

          <form onSubmit={handleSubmit} className="flex flex-col gap-5">
            {/* Email */}
            <div>
              <label className="block text-[13px] font-semibold text-gray-700 mb-1.5">Email Address</label>
              <div className="relative">
                <Mail className="absolute left-4 top-1/2 -translate-y-1/2 w-[18px] h-[18px] text-slate-400 pointer-events-none" />
                <input
                  type="email"
                  value={email}
                  onChange={e => setEmail(e.target.value)}
                  placeholder="you@jauniforms.com"
                  autoComplete="email"
                  autoFocus
                  required
                  className="w-full pl-11 pr-4 py-[13px] text-sm rounded-xl text-gray-900 placeholder-slate-400 transition-all outline-none"
                  style={{ background: '#fafafa', border: '1.5px solid #e2e8f0' }}
                  onFocus={e => e.currentTarget.style.borderColor = '#f59e0b'}
                  onBlur={e => e.currentTarget.style.borderColor = '#e2e8f0'}
                />
              </div>
            </div>

            {/* Password */}
            <div>
              <label className="block text-[13px] font-semibold text-gray-700 mb-1.5">Password</label>
              <div className="relative">
                <Lock className="absolute left-4 top-1/2 -translate-y-1/2 w-[18px] h-[18px] text-slate-400 pointer-events-none" />
                <input
                  type={showPwd ? 'text' : 'password'}
                  value={password}
                  onChange={e => setPassword(e.target.value)}
                  placeholder="Enter your password"
                  autoComplete="current-password"
                  required
                  className="w-full pl-11 pr-12 py-[13px] text-sm rounded-xl text-gray-900 placeholder-slate-400 transition-all outline-none"
                  style={{ background: '#fafafa', border: '1.5px solid #e2e8f0' }}
                  onFocus={e => e.currentTarget.style.borderColor = '#f59e0b'}
                  onBlur={e => e.currentTarget.style.borderColor = '#e2e8f0'}
                />
                <button type="button" onClick={() => setShowPwd(!showPwd)}
                        className="absolute right-3 top-1/2 -translate-y-1/2 p-1 text-slate-400 hover:text-navy-900 transition-colors">
                  {showPwd ? <EyeOff className="w-[18px] h-[18px]" /> : <Eye className="w-[18px] h-[18px]" />}
                </button>
              </div>
            </div>

            {/* Remember me */}
            <label className="flex items-center gap-2 cursor-pointer select-none">
              <input type="checkbox" checked={remember} onChange={e => setRemember(e.target.checked)}
                     className="w-4 h-4 rounded accent-amber-500 cursor-pointer" />
              <span className="text-[13px] text-slate-500">Remember me for 30 days</span>
            </label>

            {/* Submit */}
            <button
              type="submit"
              disabled={loading || !email || !password}
              className="relative w-full flex items-center justify-center gap-2.5 py-[14px] rounded-xl text-[15px] font-semibold text-white transition-all mt-1 overflow-hidden group disabled:opacity-60 disabled:cursor-not-allowed"
              style={{ background: 'linear-gradient(135deg, #0a1628 0%, #1a3158 100%)' }}
            >
              <div className="absolute inset-0 opacity-0 group-hover:opacity-100 transition-opacity"
                   style={{ background: 'linear-gradient(90deg, transparent, rgba(245,158,11,0.18), transparent)', animation: 'none' }} />
              {loading ? (
                <Loader2 className="w-5 h-5 animate-spin" />
              ) : (
                <>
                  <span>Sign In</span>
                  <ArrowRight className="w-[18px] h-[18px]" />
                </>
              )}
            </button>
          </form>

          <div className="text-center mt-6 pt-5 border-t border-gray-100">
            <p className="text-[13px] text-slate-500">
              Don't have an account?{' '}
              <Link to="/register" className="font-semibold transition-colors" style={{ color: '#f59e0b' }}
                    onMouseEnter={e => (e.currentTarget.style.color = '#d97706')}
                    onMouseLeave={e => (e.currentTarget.style.color = '#f59e0b')}>
                Create account
              </Link>
            </p>
          </div>
        </div>
      </div>
    </div>
  )
}
