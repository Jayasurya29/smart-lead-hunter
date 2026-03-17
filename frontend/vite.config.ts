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
      '/api': 'http://localhost:8000',
      '/leads': 'http://localhost:8000',
      '/stats': 'http://localhost:8000',
      '/dashboard/leads': 'http://localhost:8000',
      '/dashboard/scrape': 'http://localhost:8000',
      '/dashboard/extract': 'http://localhost:8000',
      '/dashboard/discovery': 'http://localhost:8000',
      '/dashboard/sources': 'http://localhost:8000',
      '/static': 'http://localhost:8000',
    },
  },
})