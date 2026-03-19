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
      '/api': {
        target: 'http://192.168.1.151:8000',
        changeOrigin: true,
      },
      '/auth': {
        target: 'http://192.168.1.151:8000',
        changeOrigin: true,
      },
      '/leads': {
        target: 'http://192.168.1.151:8000',
        changeOrigin: true,
      },
      '/sources': {
        target: 'http://192.168.1.151:8000',
        changeOrigin: true,
      },
      '/stats': {
        target: 'http://192.168.1.151:8000',
        changeOrigin: true,
      },
      '/health': {
        target: 'http://192.168.1.151:8000',
        changeOrigin: true,
      },
      '/scrape': {
        target: 'http://192.168.1.151:8000',
        changeOrigin: true,
      },
      '/static': {
        target: 'http://192.168.1.151:8000',
        changeOrigin: true,
      },
    },
  },
})
