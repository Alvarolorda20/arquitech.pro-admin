# ─────────────────────────────────────────────────────────────────────────────
# Budget Comparator — Frontend Dockerfile  (Next.js standalone)
# ─────────────────────────────────────────────────────────────────────────────
# Uses Next.js standalone output for minimal container size (~120 MB).
#
# IMPORTANT: Requires `output: 'standalone'` in next.config.ts.
#
#   docker build -t budget-frontend .
#   docker run -p 3000:3000 --env-file .env.local budget-frontend
# ─────────────────────────────────────────────────────────────────────────────

# ─── Stage 1: Install dependencies ──────────────────────────────────────────
FROM node:22-alpine AS deps
WORKDIR /app
COPY package.json package-lock.json* ./
RUN npm ci --ignore-scripts

# ─── Stage 2: Build ─────────────────────────────────────────────────────────
FROM node:22-alpine AS builder
WORKDIR /app
COPY --from=deps /app/node_modules ./node_modules
COPY . .

# Build-time env vars (public keys are embedded at build time in Next.js)
ARG NEXT_PUBLIC_SUPABASE_URL
ARG NEXT_PUBLIC_SUPABASE_ANON_KEY
ARG NEXT_PUBLIC_API_URL
ARG NEXT_PUBLIC_BACKEND_URL
ARG NEXT_PUBLIC_WORKSPACE_APP_URL
ARG NEXT_PUBLIC_ADMIN_APP_URL

ENV NEXT_PUBLIC_SUPABASE_URL=$NEXT_PUBLIC_SUPABASE_URL \
    NEXT_PUBLIC_SUPABASE_ANON_KEY=$NEXT_PUBLIC_SUPABASE_ANON_KEY \
    NEXT_PUBLIC_API_URL=$NEXT_PUBLIC_API_URL \
    NEXT_PUBLIC_BACKEND_URL=$NEXT_PUBLIC_BACKEND_URL \
    NEXT_PUBLIC_WORKSPACE_APP_URL=$NEXT_PUBLIC_WORKSPACE_APP_URL \
    NEXT_PUBLIC_ADMIN_APP_URL=$NEXT_PUBLIC_ADMIN_APP_URL

RUN npm run build

# ─── Stage 3: Production runner ─────────────────────────────────────────────
FROM node:22-alpine AS runner
WORKDIR /app
ENV NODE_ENV=production

# Create non-root user
RUN addgroup --system --gid 1001 nextjs \
 && adduser  --system --uid 1001 nextjs

# Copy standalone output + static/public assets
COPY --from=builder /app/.next/standalone ./
COPY --from=builder /app/.next/static ./.next/static
COPY --from=builder /app/public ./public
# Ensure SWC runtime helpers exist in final image (standalone can miss nested helper files).
COPY --from=builder /app/node_modules/@swc/helpers ./node_modules/@swc/helpers
COPY --from=builder /app/node_modules/tslib ./node_modules/tslib

USER nextjs

EXPOSE 3000
ENV PORT=3000 \
    HOSTNAME="0.0.0.0"

CMD ["node", "server.js"]
