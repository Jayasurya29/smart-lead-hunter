import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import path from 'path'

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
  server: {
    port: 3000,
    proxy: {
      // SSE streaming endpoints — need special handling to prevent
      // the proxy from buffering/dropping the connection
      '/api/dashboard/scrape/stream': {
        target: 'http://localhost:8000',
        changeOrigin: true,
        // Disable proxy timeout for long-running SSE streams
        timeout: 0,
        // Required: prevent proxy from buffering SSE chunks
        configure: (proxy) => {
          proxy.on('proxyReq', (proxyReq) => {
            proxyReq.setHeader('Accept', 'text/event-stream')
          })
        },
      },
      '/api/dashboard/extract-url/stream': {
        target: 'http://localhost:8000',
        changeOrigin: true,
        timeout: 0,
        configure: (proxy) => {
          proxy.on('proxyReq', (proxyReq) => {
            proxyReq.setHeader('Accept', 'text/event-stream')
          })
        },
      },
      '/api/dashboard/discovery/stream': {
        target: 'http://localhost:8000',
        changeOrigin: true,
        timeout: 0,
        configure: (proxy) => {
          proxy.on('proxyReq', (proxyReq) => {
            proxyReq.setHeader('Accept', 'text/event-stream')
          })
        },
      },
      // Regular API routes
      '/api': {
        target: 'http://localhost:8000',
        changeOrigin: true,
      },
      '/auth': {
        target: 'http://localhost:8000',
        changeOrigin: true,
      },
      '/leads': {
        target: 'http://localhost:8000',
        changeOrigin: true,
      },
      '/sources': {
        target: 'http://localhost:8000',
        changeOrigin: true,
      },
      '/stats': {
        target: 'http://localhost:8000',
        changeOrigin: true,
      },
      '/health': {
        target: 'http://localhost:8000',
        changeOrigin: true,
      },
      '/scrape': {
        target: 'http://localhost:8000',
        changeOrigin: true,
      },
      '/static': {
        target: 'http://localhost:8000',
        changeOrigin: true,
      },
      '/revenue': {
        target: 'http://localhost:8000',
        changeOrigin: true,
      },
    },
  },
})
