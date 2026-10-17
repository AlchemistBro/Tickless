/// <reference types="vitest" />
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// In development the UI is served by Vite and talks to the gateway through the
// proxy below, so the browser sees a single origin (no CORS, no WS origin issues).
const gateway = process.env.TICKLESS_GATEWAY ?? "http://localhost:8080";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/v1": { target: gateway, changeOrigin: false },
      "/ws": { target: gateway.replace(/^http/, "ws"), ws: true, changeOrigin: false },
    },
  },
  test: { environment: "jsdom", globals: false, include: ["src/**/*.test.{ts,tsx}"] },
});
