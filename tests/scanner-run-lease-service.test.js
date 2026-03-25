const test = require("node:test")
const assert = require("node:assert/strict")

process.env.SUPABASE_URL = process.env.SUPABASE_URL || "https://example.supabase.co"
process.env.SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "anon"
process.env.SUPABASE_SERVICE_ROLE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || "service-role"

const scannerRunRepo = require("../src/repositories/scannerRunRepository")
const scannerRunLeaseService = require("../src/services/scannerRunLeaseService")

test("scanner run lease service forwards heartbeat updates to the repository", async () => {
  const originalTouchHeartbeat = scannerRunRepo.touchHeartbeat
  let heartbeatPayload = null

  scannerRunRepo.touchHeartbeat = async (runId, payload = {}) => {
    heartbeatPayload = {
      runId,
      payload
    }
    return {
      id: runId,
      heartbeat_at: payload.heartbeatAt
    }
  }

  try {
    const result = await scannerRunLeaseService.heartbeat({
      leaseId: "lease-123",
      heartbeatAt: "2026-03-25T10:15:00.000Z",
      diagnostics: {
        trigger: "scheduled_feed_revalidation"
      }
    })

    assert.equal(heartbeatPayload.runId, "lease-123")
    assert.equal(heartbeatPayload.payload.heartbeatAt, "2026-03-25T10:15:00.000Z")
    assert.equal(heartbeatPayload.payload.diagnosticsSummary.trigger, "scheduled_feed_revalidation")
    assert.equal(result.id, "lease-123")
  } finally {
    scannerRunRepo.touchHeartbeat = originalTouchHeartbeat
  }
})
