import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// During development the dev server proxies /sightings, /healthz, /readyz
// and /metrics to the Go server running on localhost:8080.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/sightings': { target: 'http://localhost:8080', changeOrigin: true },
      '/healthz': 'http://localhost:8080',
      '/readyz': 'http://localhost:8080',
    },
  },
  build: {
    outDir: 'dist',
    emptyOutDir: true,
  },
})
