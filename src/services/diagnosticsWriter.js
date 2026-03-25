function toJsonObject(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return {}
  }
  return value
}

async function writePublishBatch(input = {}) {
  return toJsonObject(input)
}

async function writePublishDecisions(input = {}) {
  return toJsonObject(input)
}

async function writeRevalidationBatch(input = {}) {
  return toJsonObject(input)
}

async function writeRunSummary(input = {}) {
  return toJsonObject(input)
}

module.exports = {
  writePublishBatch,
  writePublishDecisions,
  writeRevalidationBatch,
  writeRunSummary
}
