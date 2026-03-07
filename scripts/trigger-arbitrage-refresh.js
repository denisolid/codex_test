#!/usr/bin/env node

function normalizeBoolean(value, fallback = false) {
  if (typeof value === "boolean") return value
  const raw = String(value || "")
    .trim()
    .toLowerCase()
  if (!raw) return fallback
  return raw === "1" || raw === "true" || raw === "yes" || raw === "on"
}

async function main() {
  const apiPublicUrl = String(process.env.API_PUBLIC_URL || process.env.BACKEND_URL || "").trim()
  const adminApiToken = String(process.env.ADMIN_API_TOKEN || "").trim()
  const forceRefresh = normalizeBoolean(process.env.ARBITRAGE_CRON_FORCE_REFRESH, false)
  const trigger = String(process.env.ARBITRAGE_CRON_TRIGGER || "scheduled_cron")
    .trim()
    .toLowerCase()

  if (!apiPublicUrl) {
    throw new Error("Missing API_PUBLIC_URL (or BACKEND_URL) for scanner cron trigger")
  }
  if (!adminApiToken) {
    throw new Error("Missing ADMIN_API_TOKEN for scanner cron trigger")
  }

  const endpoint = `${apiPublicUrl.replace(/\/+$/, "")}/api/opportunities/refresh/admin`
  const response = await fetch(endpoint, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-admin-token": adminApiToken
    },
    body: JSON.stringify({
      trigger: trigger || "scheduled_cron",
      forceRefresh
    })
  })

  const payload = await response.json().catch(() => ({}))
  if (!response.ok) {
    const message = String(payload?.message || payload?.error || response.statusText || "request_failed")
    throw new Error(`Scanner refresh trigger failed: ${response.status} ${message}`)
  }

  console.log(
    `[scanner-cron] Triggered run ${String(payload?.scanRunId || "unknown")} (alreadyRunning=${Boolean(
      payload?.alreadyRunning
    )})`
  )
}

main().catch((err) => {
  console.error("[scanner-cron] Failed:", err?.message || err)
  process.exit(1)
})
