# Supabase Heartbeat (Free Plan Keep-Alive)

This project includes a GitHub Actions workflow at `.github/workflows/supabase-heartbeat.yml` that sends a real Supabase REST request to:

`GET {SUPABASE_URL}/rest/v1/heartbeat?select=id&limit=1`

## 1) Add GitHub Actions secrets

In your GitHub repository:

`Settings -> Secrets and variables -> Actions -> New repository secret`

Add:
- `SUPABASE_URL`
- `SUPABASE_ANON_KEY`

## 2) Where to find values in Supabase

In Supabase dashboard:

`Project Settings -> API`

Use:
- `Project URL` -> `SUPABASE_URL`
- `anon public` key -> `SUPABASE_ANON_KEY`

## 3) Verify locally with curl

```bash
curl -sS \
  -H "apikey: $SUPABASE_ANON_KEY" \
  -H "Authorization: Bearer $SUPABASE_ANON_KEY" \
  "$SUPABASE_URL/rest/v1/heartbeat?select=id&limit=1"
```

Expected result is a non-empty JSON array, for example:

```json
[{"id":1}]
```

## 4) Security note

Never use the `service_role` key in GitHub Actions or any public CI workflow. Use only the `anon` key for this heartbeat.
