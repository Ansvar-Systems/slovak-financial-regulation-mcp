FROM node:20-slim AS builder
WORKDIR /app
COPY package.json package-lock.json* ./
# Full install (with scripts) so better-sqlite3 builds its native binding
RUN npm ci
COPY tsconfig.json ./
COPY src/ src/
RUN npm run build

# Prune dev deps for the production stage; keep the prebuilt better-sqlite3 binding
RUN npm prune --omit=dev

FROM node:20-slim AS production
WORKDIR /app
ENV NODE_ENV=production
ENV NBS_DB_PATH=/app/data/nbs.db
COPY package.json package-lock.json* ./
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/dist/ dist/
COPY data/database.db data/nbs.db
RUN addgroup --system --gid 1001 mcp && \
    adduser --system --uid 1001 --ingroup mcp mcp && \
    chown -R mcp:mcp /app
USER mcp
HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=3 \
  CMD node -e "require('http').get('http://localhost:3000/health',r=>{process.exit(r.statusCode===200?0:1)}).on('error',()=>process.exit(1))"
CMD ["node", "dist/src/http-server.js"]
