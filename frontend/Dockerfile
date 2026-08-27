# e2e-only image: `vite preview` serves the built assets AND keeps the same
# dev-server proxy config (vite.config.ts's server/preview.proxy) that routes
# /api and /auth to the backend — so this container behaves identically to
# `npm run dev` on a host, just pointed at the compose network's service names
# instead of localhost (VITE_PROXY_API_TARGET/VITE_PROXY_AUTH_TARGET below).
FROM node:22-slim AS builder
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
RUN npm run build

FROM node:22-slim AS runtime
RUN useradd --system --uid 10006 --create-home appuser
WORKDIR /app
COPY --from=builder --chown=appuser:appuser /app /app
USER appuser
EXPOSE 5173
CMD ["npx", "vite", "preview", "--host", "0.0.0.0"]
