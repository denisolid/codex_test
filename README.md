# CS2 Item Portfolio Analyzer (MVP)

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
   - set `AUTH_EMAIL_REDIRECT_TO` so confirmation emails return to your login page
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
- `POST /auth/resend-confirmation`
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
- `GET /market/inventory/value`
- `GET /market/items/:skinId/sell-suggestion`
- `GET /market/items/:skinId/liquidity`
- `POST /trade/calculate`
- `GET /alerts`
- `POST /alerts`
- `PATCH /alerts/:id`
- `DELETE /alerts/:id`
- `GET /alerts/events`
- `POST /alerts/check` (requires `x-admin-token` header)
- `GET /extension/keys`
- `POST /extension/keys`
- `DELETE /extension/keys/:id`
- `GET /extension/inventory/value` (requires `x-extension-api-key`)
- `GET /extension/items/:skinId/sell-suggestion` (requires `x-extension-api-key`)
- `POST /extension/trade/calculate` (requires `x-extension-api-key`)

All non-auth routes require authentication via secure `HttpOnly` cookie set by `/auth/login` or `/auth/session`.

Monetary endpoints support optional `?currency=<CODE>` query parameter.

Examples:
- `GET /api/portfolio?currency=EUR`
- `GET /api/portfolio/history?currency=GBP`
- `GET /api/skins/by-steam-item/:steamItemId?currency=UAH`
- `GET /api/market/inventory/value?currency=PLN`

## Console commands

- Run backend only:
  - `npm run backend:dev`
- Run frontend only:
  - `npm run frontend:dev`
- Run backend + frontend together:
  - `npm run dev`
- Run scheduled price updater worker:
  - `npm run worker:prices`
- Run scheduled alert checker worker:
  - `npm run worker:alerts`
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
  - `AUTH_EMAIL_REDIRECT_TO=http://localhost:5173/login.html?confirmed=1`
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

- Holdings and item details now expose:
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
  - In strict real mode, historical rows with `source` containing `mock` are ignored in portfolio and item history reads.
  - Steam requests are queued and retried with backoff+jitter on `429/5xx/timeout`.
  - Cache: if latest `price_history` row is newer than `MARKET_PRICE_CACHE_TTL_MINUTES`, sync/update reuses cached price instead of hitting Steam.

## Price updater

- Controlled by root `.env`:
  - `PRICE_UPDATER_INTERVAL_MINUTES=60`
  - `PRICE_UPDATER_RATE_LIMIT_PER_SECOND=5`
- Worker writes hourly/daily snapshot rows into `price_history` with source `scheduled-mock`.

## Trading assistant features

- Real-time inventory valuation:
  - `GET /api/market/inventory/value`
  - returns per-item value and net value after default `13%` market commission
- Quick sell suggestions:
  - `GET /api/market/items/:skinId/sell-suggestion`
  - returns `fast_sell`, `balanced`, `max_profit` tiers using lowest listing, 7d average, and liquidity
- Liquidity score:
  - `GET /api/market/items/:skinId/liquidity`
  - score normalized to `0-100` using volume, volatility, and spread
- Trade profit calculator:
  - `POST /api/trade/calculate`
  - returns net profit, ROI, break-even
- Alerts:
  - create/list/update/delete via `/api/alerts`
  - worker executes cron-style checks and writes trigger history to `alert_events`
- Multi-currency display:
  - frontend has a currency selector (`USD`, `EUR`, `GBP`, `UAH`, `PLN`, `CZK`)
  - backend converts USD-based outputs when `?currency=` is provided

## Extension API key flow

- Create/manage keys as authenticated user:
  - `POST /api/extension/keys`
  - `GET /api/extension/keys`
  - `DELETE /api/extension/keys/:id`
- Use key from extension:
  - header `x-extension-api-key: <key>`
  - call `/api/extension/*` endpoints without cookie auth

## New env for trading assistant

- `MARKET_COMMISSION_PERCENT=13`
- `MARKET_SNAPSHOT_TTL_MINUTES=30`
- `ALERT_CHECK_INTERVAL_MINUTES=5`
- `ALERT_CHECK_BATCH_SIZE=250`
- `DEFAULT_DISPLAY_CURRENCY=USD`
- `FX_RATES_USD_JSON={"EUR":0.92,"GBP":0.79,"UAH":41.2,"PLN":4.02,"CZK":23.5}`

## Deployment structure

- Backend: repository root (`src/*`, root `package.json`)
- Frontend: `frontend/*` (`frontend/package.json`)

## Deploy backend on Render (from GitHub)

Use the included `render.yaml` blueprint, or set values manually.

Manual service settings:
- Runtime: `Node`
- Root Directory: repository root
- Build Command: `npm ci`
- Start Command: `npm start`
- Health Check Path: `/health`

Render environment variables:
- Required:
  - `SUPABASE_URL`
  - `SUPABASE_ANON_KEY`
  - `SUPABASE_SERVICE_ROLE_KEY`
  - `FRONTEND_URL` (comma-separated allowed origins; include your frontend production URL)
  - `AUTH_EMAIL_REDIRECT_TO` (for example `https://your-app.vercel.app/login.html?confirmed=1`)
- Optional:
  - `ADMIN_API_TOKEN`
  - `MARKET_PRICE_SOURCE` (default `auto`)
  - `STEAM_INVENTORY_SOURCE` (default `auto`)

Where to set on Render:
- `Dashboard -> Web Service -> Environment`

## Deploy frontend on Vercel (from GitHub)

Project settings:
- Framework preset: `Vite`
- Root Directory: `frontend`
- Build Command: `npm run build`
- Output Directory: `dist`

Frontend environment variables on Vercel:
- `VITE_API_URL=https://<your-render-service>.onrender.com/api`
- `VITE_SUPABASE_URL=https://<your-project>.supabase.co`
- `VITE_SUPABASE_ANON_KEY=<your-anon-key>`

Where to set on Vercel:
- `Dashboard -> Project -> Settings -> Environment Variables`

## Deploy frontend on Netlify (alternative)

`netlify.toml` is included for this repository.

Netlify environment variables:
- `VITE_API_URL=https://<your-render-service>.onrender.com/api`
- `VITE_SUPABASE_URL=https://<your-project>.supabase.co`
- `VITE_SUPABASE_ANON_KEY=<your-anon-key>`

Where to set on Netlify:
- `Dashboard -> Site configuration -> Environment variables`

## Connect frontend and backend

1. Deploy backend to Render and copy the public backend URL.
2. Set frontend `VITE_API_URL` to `${RENDER_URL}/api`.
3. Set backend `FRONTEND_URL` to your deployed frontend origin(s), for example:
   - `https://your-app.vercel.app`
   - `https://your-app.netlify.app`
   - multiple origins: `https://app.vercel.app,https://staging.vercel.app`
4. Redeploy both services after env changes.

## Verify deployment

Backend:
- Open `https://<render-service>.onrender.com/health`
- Expect `200` with JSON, e.g. `{ "ok": true }`

Frontend:
- Open deployed frontend URL
- Login/register and confirm requests are sent to Render API URL
- Confirm authenticated API calls succeed without CORS errors

## Deployment checklist

1. Supabase migrations applied in production database.
2. Render env vars are set (especially `SUPABASE_*` and `FRONTEND_URL`).
3. Vercel/Netlify env vars are set (especially `VITE_API_URL`).
4. Backend `/health` responds with `200`.
5. Frontend can call `/api/auth/*` and `/api/portfolio` successfully.
