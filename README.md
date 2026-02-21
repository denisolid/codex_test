# CS2 Skin Portfolio Analyzer (MVP)

Backend MVP implemented with:
- Node.js + Express
- Supabase (PostgreSQL + Auth)
- REST API
- Clean modular architecture
- Frontend: Vite + Vanilla JS

## Quick start

1. Copy `.env.example` to `.env` and fill Supabase values.
   - for local frontend, keep `FRONTEND_ORIGIN=http://localhost:5173`
   - set `FRONTEND_ORIGINS` for production allowlist (comma-separated)
   - set `ADMIN_API_TOKEN` if you want to use admin maintenance endpoints
2. Run SQL from `supabase/schema.sql` in Supabase SQL editor.
3. Frontend env:
   - copy `frontend/.env.example` to `frontend/.env`
4. Install root dependencies:
   - `npm install`
5. Install frontend dependencies:
   - `npm --prefix frontend install`

## Base URL

- `http://localhost:4000/api`

## Frontend pages

- Public Home (available without auth): `http://localhost:5173/`
- Login: `http://localhost:5173/login.html`
- Register: `http://localhost:5173/register.html`
- OAuth callback: `http://localhost:5173/auth-callback.html`

After login, the same `/` page renders the authenticated app view.

## Google Auth setup (Supabase)

1. Supabase Dashboard -> `Authentication` -> `Providers` -> enable `Google`.
2. Add Google OAuth credentials (Client ID / Client Secret) in Supabase provider settings.
3. Supabase Dashboard -> `Authentication` -> `URL Configuration`:
   - Site URL: `http://localhost:5173`
   - Redirect URL allow list: `http://localhost:5173/auth-callback.html`
4. In `frontend/.env` set:
   - `VITE_SUPABASE_URL=...`
   - `VITE_SUPABASE_ANON_KEY=...`

## Endpoints

- `POST /auth/register`
- `POST /auth/login`
- `POST /auth/session`
- `POST /auth/logout`
- `GET /auth/me`
- `POST /admin/prices/cleanup-mock` (requires `x-admin-token` header)
- `PATCH /users/me/steam`
- `POST /inventory/sync`
- `GET /portfolio`
- `GET /portfolio/history`
- `GET /skins/:id`
- `GET /skins/by-steam-item/:steamItemId`
- `GET /transactions`
- `POST /transactions`
- `GET /transactions/:id`
- `PATCH /transactions/:id`
- `DELETE /transactions/:id`

All non-auth routes require authentication via secure `HttpOnly` cookie set by `/auth/login` or `/auth/session`.

## Console commands

- Run backend only:
  - `npm run backend:dev`
- Run frontend only:
  - `npm run frontend:dev`
- Run backend + frontend together:
  - `npm run dev`
- Run scheduled price updater worker:
  - `npm run worker:prices`
- Run tests:
  - `npm test`
- Build frontend:
  - `npm run build`
- Preview frontend build:
  - `npm run frontend:preview`
- Start backend (non-watch):
  - `npm start`

## Steam inventory sync mode

- Configure in root `.env`:
  - `STEAM_INVENTORY_SOURCE=auto` (`auto` | `real` | `mock`)
  - `STEAM_INVENTORY_TIMEOUT_MS=12000`
- Behavior:
  - `real`: fetch from Steam Community inventory endpoint only (requires public inventory).
  - `mock`: always use local mock data.
  - `auto`: try real Steam first, fallback to mock if Steam fetch fails.

## Market price source

- Configure in root `.env`:
  - `FRONTEND_ORIGIN=http://localhost:5173`
  - `FRONTEND_ORIGINS=http://localhost:5173`
  - `AUTH_RATE_LIMIT_WINDOW_MS=60000`
  - `AUTH_RATE_LIMIT_MAX=20`
  - `SYNC_RATE_LIMIT_WINDOW_MS=60000`
  - `SYNC_RATE_LIMIT_MAX=6`
- Behavior:
  - Cookie auth is `HttpOnly` and uses CORS credentials.
  - Requests from origins not in `FRONTEND_ORIGINS` are blocked.
  - Auth and inventory sync endpoints are rate-limited.

## Admin maintenance

- Configure `ADMIN_API_TOKEN` in backend `.env`.
- Cleanup old mock rows from `price_history`:
  - `POST /api/admin/prices/cleanup-mock`
  - Header: `x-admin-token: <ADMIN_API_TOKEN>`

## Price quality and alerts

- Holdings and skin details now expose:
  - status: `real`, `cached`, `stale`, `unpriced` (or `mock` in mixed mode)
  - confidence label/score
- Portfolio endpoint now includes alerts for:
  - large 24h/7d movement
  - stale prices
  - unpriced items
  - high concentration risk / weak market breadth
- Portfolio endpoint analytics now includes:
  - concentration (`top1`, `top3`, `HHI`, `effective holdings`)
  - breadth (`advancers`, `decliners`, `advancer ratio`)
  - leaders (`top gainer`, `top loser`)
  - weighted 7d move estimate

## Steam price reliability

- Configure in root `.env`:
  - `MARKET_PRICE_SOURCE=auto` (`auto` | `steam` | `mock`)
  - `MARKET_PRICE_FALLBACK_TO_MOCK=true` (`true` | `false`)
  - `MARKET_PRICE_RATE_LIMIT_PER_SECOND=2`
  - `MARKET_PRICE_STALE_HOURS=24`
  - `MARKET_PRICE_CACHE_TTL_MINUTES=60`
  - `STEAM_MARKET_CURRENCY=1` (`1` = USD in Steam priceoverview)
  - `STEAM_MARKET_TIMEOUT_MS=10000`
  - `STEAM_MARKET_MAX_RETRIES=3`
  - `STEAM_MARKET_RETRY_BASE_MS=350`
- Behavior:
  - `steam`: use Steam Market priceoverview only.
  - `mock`: always generate deterministic mock prices.
  - `auto`: try Steam Market first, fallback to mock if unavailable/rate-limited.
  - Set `MARKET_PRICE_FALLBACK_TO_MOCK=false` for strict real pricing (sync/update fails instead of using fake fallback).
  - In strict real mode, historical rows with `source` containing `mock` are ignored in portfolio and skin history reads.
  - Steam requests are queued and retried with backoff+jitter on `429/5xx/timeout`.
  - Cache: if latest `price_history` row is newer than `MARKET_PRICE_CACHE_TTL_MINUTES`, sync/update reuses cached price instead of hitting Steam.

## Price updater

- Controlled by root `.env`:
  - `PRICE_UPDATER_INTERVAL_MINUTES=60`
  - `PRICE_UPDATER_RATE_LIMIT_PER_SECOND=5`
- Worker writes hourly/daily snapshot rows into `price_history` with source `scheduled-mock`.
