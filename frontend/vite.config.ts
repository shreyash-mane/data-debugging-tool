import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      // Dataset Finder routes → Node.js backend (port 3001)
      '/auth': {
        target: 'http://localhost:3001',
        changeOrigin: true,
      },
      '/api/search': {
        target: 'http://localhost:3001',
        changeOrigin: true,
      },
      '/api/rate-limit': {
        target: 'http://localhost:3001',
        changeOrigin: true,
      },
      '/api/collections': {
        target: 'http://localhost:3001',
        changeOrigin: true,
      },
      '/api/saved': {
        target: 'http://localhost:3001',
        changeOrigin: true,
      },
      // Data Debugger routes → Python FastAPI backend (port 8000)
      '/api': {
        target: 'http://localhost:8000',
        changeOrigin: true,
      },
    },
  },
})
