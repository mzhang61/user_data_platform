import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// Goal: no CORS pain.
// - UI calls /query/... and /collector/... (same-origin)
// - Vite proxies those to your local backends.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/query': {
        target: process.env.VITE_QUERY_API_BASE || 'http://localhost:18080',
        changeOrigin: true,
        rewrite: (p) => p.replace(/^\/query/, ''),
      },
      '/collector': {
        target: process.env.VITE_COLLECTOR_API_BASE || 'http://localhost:18081',
        changeOrigin: true,
        rewrite: (p) => p.replace(/^\/collector/, ''),
      },
    },
  },
})
