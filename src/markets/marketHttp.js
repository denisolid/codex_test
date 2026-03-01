const AppError = require("../utils/AppError");
const {
  marketCompareTimeoutMs,
  marketCompareMaxRetries,
  marketCompareRetryBaseMs,
  marketCompareConcurrency
} = require("../config/env");

function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, Math.max(Number(ms) || 0, 0));
  });
}

function jitter(ms) {
  return Math.floor(Math.random() * Math.max(Number(ms) || 0, 1));
}

function isRetryableStatus(status) {
  return [429, 500, 502, 503, 504].includes(Number(status));
}

function toHeadersObject(input = {}) {
  if (!input || typeof input !== "object") return {};
  return Object.fromEntries(
    Object.entries(input).filter(
      ([key, value]) => String(key || "").trim() && value != null
    )
  );
}

async function fetchJsonWithRetry(url, options = {}) {
  const timeoutMs = Math.max(
    Number(options.timeoutMs || marketCompareTimeoutMs || 9000),
    500
  );
  const maxRetries = Math.max(
    Number(options.maxRetries || marketCompareMaxRetries || 3),
    1
  );
  const retryBaseMs = Math.max(
    Number(options.retryBaseMs || marketCompareRetryBaseMs || 350),
    50
  );
  const requestOptions = {
    method: options.method || "GET",
    headers: toHeadersObject(options.headers || {})
  };

  let lastError = null;

  for (let attempt = 1; attempt <= maxRetries; attempt += 1) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);

    try {
      const response = await fetch(url, {
        ...requestOptions,
        signal: controller.signal
      });

      if (!response.ok) {
        const status = Number(response.status || 500);
        const retryAfterSeconds = Number(response.headers.get("retry-after") || 0);
        const error = new AppError(
          `Market request failed with status ${status}`,
          isRetryableStatus(status) ? status : 502
        );
        if (retryAfterSeconds > 0) {
          error.retryAfterMs = retryAfterSeconds * 1000;
        }
        throw error;
      }

      const payload = await response.json();
      return payload;
    } catch (err) {
      if (err?.name === "AbortError") {
        lastError = new AppError("Market request timed out", 504);
      } else {
        lastError = err;
      }

      const status = Number(lastError?.statusCode || 0);
      if (attempt < maxRetries && isRetryableStatus(status)) {
        const retryAfterMs = Number(lastError?.retryAfterMs || 0);
        const backoffMs = Math.max(
          retryAfterMs,
          retryBaseMs * 2 ** (attempt - 1) + jitter(retryBaseMs)
        );
        await sleep(backoffMs);
        continue;
      }

      throw lastError;
    } finally {
      clearTimeout(timer);
    }
  }

  throw lastError || new AppError("Market request failed", 502);
}

async function mapWithConcurrency(items, mapper, limitInput = null) {
  const source = Array.isArray(items) ? items : [];
  if (!source.length) return [];

  const concurrencyLimit = Math.max(
    Number(limitInput || marketCompareConcurrency || 4),
    1
  );

  const results = new Array(source.length);
  let index = 0;

  async function worker() {
    while (true) {
      const current = index;
      index += 1;
      if (current >= source.length) {
        return;
      }

      results[current] = await mapper(source[current], current);
    }
  }

  const workers = Array.from(
    { length: Math.min(concurrencyLimit, source.length) },
    () => worker()
  );
  await Promise.all(workers);
  return results;
}

module.exports = {
  fetchJsonWithRetry,
  mapWithConcurrency,
  sleep
};
