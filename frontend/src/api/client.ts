import axios from 'axios'

const api = axios.create({
  baseURL: '',
  headers: {
    'Content-Type': 'application/json',
    'Accept': 'application/json',
  },
})

// Attach API key and CSRF header to every request
api.interceptors.request.use((config) => {
  const token = localStorage.getItem('slh_token')
  if (token) {
    config.headers['X-API-Key'] = token
  }
  config.headers['X-Requested-With'] = 'XMLHttpRequest'
  return config
})

// Handle auth failures globally
api.interceptors.response.use(
  (response) => response,
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
