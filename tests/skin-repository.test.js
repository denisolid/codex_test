const test = require("node:test")
const assert = require("node:assert/strict")

process.env.SUPABASE_URL = process.env.SUPABASE_URL || "https://example.supabase.co"
process.env.SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "anon"
process.env.SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || "service-role"

const {
  __testables: { chunkArray, normalizeMarketHashNames, formatSupabaseError }
} = require("../src/repositories/skinRepository")

test("chunkArray splits large arrays into predictable chunk sizes", () => {
  const values = Array.from({ length: 125 }, (_, index) => index + 1)
  const chunks = chunkArray(values, 60)
  assert.equal(chunks.length, 3)
  assert.deepEqual(chunks.map((chunk) => chunk.length), [60, 60, 5])
})

test("normalizeMarketHashNames trims values and removes duplicates", () => {
  const values = normalizeMarketHashNames([
    " AK-47 | Redline (Field-Tested) ",
    "AK-47 | Redline (Field-Tested)",
    "",
    null,
    "Chroma 3 Case"
  ])

  assert.deepEqual(values, ["AK-47 | Redline (Field-Tested)", "Chroma 3 Case"])
})

test("formatSupabaseError keeps details, hint, and code for diagnostics", () => {
  const message = formatSupabaseError({
    message: "Bad Request",
    details: "URL too long",
    hint: "Reduce IN filter size",
    code: "PGRST000"
  })
  assert.equal(message.includes("Bad Request"), true)
  assert.equal(message.includes("details: URL too long"), true)
  assert.equal(message.includes("hint: Reduce IN filter size"), true)
  assert.equal(message.includes("code: PGRST000"), true)
})
