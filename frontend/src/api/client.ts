import axios from 'axios'

const api = axios.create({
  baseURL: '',
  withCredentials: true,          // send httpOnly JWT cookie on every request
  headers: {
    'Content-Type': 'application/json',
    'Accept': 'application/json',
    'X-Requested-With': 'XMLHttpRequest',
  },
})

// Track whether user has been authenticated this session.
// Only show "session expired" if they WERE logged in and got kicked out —
// not on first visit when /auth/me naturally returns 401.
let _wasAuthenticated = false
let _sessionExpiredShown = false

api.interceptors.response.use(
  (response) => {
    // If /auth/me succeeds, user is authenticated
    if (response.config.url?.includes('/auth/me') && response.status === 200) {
      _wasAuthenticated = true
    }
    return response
  },
  (error) => {
    if (error.response?.status === 401) {
      // Only show toast + redirect if user WAS logged in (session expired mid-use)
      if (_wasAuthenticated && window.location.pathname !== '/login' && !_sessionExpiredShown) {
        _sessionExpiredShown = true
        const toast = document.createElement('div')
        toast.textContent = 'Session expired — redirecting to login...'
        toast.style.cssText = 'position:fixed;top:16px;left:50%;transform:translateX(-50%);z-index:99999;background:#1e293b;color:#fbbf24;padding:12px 24px;border-radius:8px;font-size:14px;box-shadow:0 4px 12px rgba(0,0,0,.3)'
        document.body.appendChild(toast)
        setTimeout(() => {
          window.location.href = '/login?expired=1'
        }, 1500)
      }
      // If never authenticated, just silently redirect (normal first-visit flow)
      else if (!_wasAuthenticated && window.location.pathname !== '/login') {
        window.location.href = '/login'
      }
    }
    return Promise.reject(error)
  }
)

export default api
