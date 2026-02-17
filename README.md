# CS2 Skin Portfolio Analyzer (MVP)

Backend MVP implemented with:
- Node.js + Express
- Supabase (PostgreSQL + Auth)
- REST API
- Clean modular architecture
- Frontend: Vite + Vanilla JS

## Quick start

1. Copy `.env.example` to `.env` and fill Supabase values.
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
- `PATCH /users/me/steam`
- `POST /inventory/sync`
- `GET /portfolio`
- `GET /portfolio/history`
- `GET /skins/:id`
- `GET /transactions`
- `POST /transactions`
- `GET /transactions/:id`
- `PATCH /transactions/:id`
- `DELETE /transactions/:id`

All non-auth routes require:
- `Authorization: Bearer <access_token>`

## Console commands

- Run backend only:
  - `npm run backend:dev`
- Run frontend only:
  - `npm run frontend:dev`
- Run backend + frontend together:
  - `npm run dev`
- Run scheduled price updater worker:
  - `npm run worker:prices`
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
  - `MARKET_PRICE_SOURCE=auto` (`auto` | `steam` | `mock`)
  - `STEAM_MARKET_CURRENCY=1` (`1` = USD in Steam priceoverview)
  - `STEAM_MARKET_TIMEOUT_MS=10000`
- Behavior:
  - `steam`: use Steam Market priceoverview only.
  - `mock`: always generate deterministic mock prices.
  - `auto`: try Steam Market first, fallback to mock if unavailable/rate-limited.

## Price updater

- Controlled by root `.env`:
  - `PRICE_UPDATER_INTERVAL_MINUTES=60`
  - `PRICE_UPDATER_RATE_LIMIT_PER_SECOND=5`
- Worker writes hourly/daily snapshot rows into `price_history` with source `scheduled-mock`.
