import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  build: {
    chunkSizeWarningLimit: 1000,
    // Vite 7's default targets. Vite 8 raised them (Safari 16.4, Chrome 111),
    // but the windows run in the system webview: WKWebView on macOS is as old
    // as the Safari of the machine, WebKitGTK on Linux as old as the distro.
    target: ['chrome107', 'edge107', 'firefox104', 'safari16'],
  },
})
