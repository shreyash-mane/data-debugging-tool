import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/api': {
        target: 'http://localhost:8000',
        changeOrigin: true,
        // When backend is down, return a JSON error instead of HTML
        // so our fetch client can handle it gracefully
        configure: (proxy) => {
          proxy.on('error', (err, _req, res) => {
            // Cast to http.ServerResponse to access writeHead/end
            const serverRes = res as import('http').ServerResponse;
            serverRes.writeHead(503, { 'Content-Type': 'application/json' });
            serverRes.end(JSON.stringify({
              detail: 'Backend server is not running. Start it with: uvicorn main:app --reload --port 8000'
            }));
          });
        },
      },
    },
  },
})
