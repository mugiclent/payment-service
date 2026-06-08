FROM node:22-bookworm-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 make g++ \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY package*.json ./
COPY prisma ./prisma/
COPY prisma.config.ts ./

RUN npm ci && npx prisma generate

COPY tsconfig.json ./
COPY src ./src/

RUN npm run build && npm prune --omit=dev


FROM node:22-bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends openssl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/node_modules   ./node_modules
COPY --from=builder /app/dist           ./dist
COPY --from=builder /app/package.json   ./
COPY --from=builder /app/prisma         ./prisma
COPY --from=builder /app/prisma.config.ts ./

ENV NODE_ENV=production
ENV PAYMENT_SERVICE_PORT=8092

EXPOSE 8092

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD node -e "require('http').get('http://localhost:8092/health', r => process.exit(r.statusCode === 200 ? 0 : 1)).on('error', () => process.exit(1))"

CMD ["node", "dist/server.js"]
