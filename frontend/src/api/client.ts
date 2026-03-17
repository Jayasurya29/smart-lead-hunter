import axios from 'axios'

const api = axios.create({
  baseURL: '',
  headers: {
    'Content-Type': 'application/json',
    'Accept': 'application/json',
  },
  withCredentials: true,
})

api.interceptors.request.use((config) => {
  const token = localStorage.getItem('slh_token')
  if (token) {
    config.headers['X-API-Key'] = token
  }
  // CSRF protection — backend checks for this header on mutation endpoints
  config.headers['X-Requested-With'] = 'XMLHttpRequest'
  return config
})

api.interceptors.response.use(
  (response) => {
    // Some backend endpoints return HTML (HTMX legacy) — try to handle gracefully
    const ct = response.headers['content-type'] || ''
    if (ct.includes('text/html') && typeof response.data === 'string') {
      // If we got HTML but expected JSON, return a success wrapper
      return { ...response, data: { status: 'ok', html: response.data } }
    }
    return response
  },
  (error) => {
    if (error.response?.status === 401) {
      localStorage.removeItem('slh_token')
      if (window.location.pathname !== '/login') {
        window.location.href = '/login'
      }
    }
    return Promise.reject(error)
  }
)

export default api