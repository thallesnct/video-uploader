import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// The browser only ever talks to two absolute hosts: this dev server (proxied
// below) and MinIO directly, via the presigned URL the API hands back
// (ADR-0006) — that PUT is same-origin-free by design and relies on MinIO's
// own CORS config, not this proxy.
//
// Proxying /api and /auth instead of calling the API/devauth origins directly
// means the browser never needs CORS configured for devauth at all, and lets
// this same config work unchanged whether Vite is run on the host (targets
// resolve via localhost) or as a container in docker-compose during `make
// e2e` (targets resolve via the compose service names, injected as env vars).
const apiTarget = process.env.VITE_PROXY_API_TARGET ?? "http://localhost:8000";
const authTarget = process.env.VITE_PROXY_AUTH_TARGET ?? "http://localhost:8080";

const proxy = {
  "/api": { target: apiTarget, changeOrigin: true, rewrite: (p: string) => p.replace(/^\/api/, "") },
  "/auth": { target: authTarget, changeOrigin: true, rewrite: (p: string) => p.replace(/^\/auth/, "") },
};

export default defineConfig({
  plugins: [react()],
  // Pinned rather than left to Vite's default (5173 for dev, 4173 for
  // preview): MinIO's CORS allow-list (docker-compose.yml,
  // MINIO_API_CORS_ALLOW_ORIGIN) is one fixed origin, and the presigned-PUT
  // upload must work identically under `npm run dev` and the `build`+
  // `preview` pair `make e2e` runs against.
  server: { port: 5173, strictPort: true, proxy },
  // allowedHosts: vite's Host-header check (DNS-rebinding protection) rejects
  // anything not on this list by default, and the e2e Playwright container
  // reaches this one over the compose network by service name, not localhost.
  preview: { port: 5173, strictPort: true, proxy, allowedHosts: ["frontend"] },
});
