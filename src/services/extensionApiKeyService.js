const crypto = require("node:crypto");
const AppError = require("../utils/AppError");
const keyRepo = require("../repositories/extensionApiKeyRepository");

function hashApiKey(rawKey) {
  return crypto.createHash("sha256").update(String(rawKey)).digest("hex");
}

function generateApiKey() {
  return `cs2ext_${crypto.randomBytes(24).toString("hex")}`;
}

function parseExpiresAt(value) {
  if (value == null || value === "") {
    return null;
  }

  const ts = new Date(value).getTime();
  if (Number.isNaN(ts)) {
    throw new AppError("expiresAt must be a valid ISO date", 400);
  }
  return new Date(ts).toISOString();
}

exports.createKey = async (userId, payload = {}) => {
  const label = String(payload.label || "default").trim() || "default";
  if (label.length > 60) {
    throw new AppError("label must be <= 60 characters", 400);
  }

  const rawKey = generateApiKey();
  const created = await keyRepo.create(userId, {
    keyHash: hashApiKey(rawKey),
    keyPrefix: rawKey.slice(0, 14),
    label,
    expiresAt: parseExpiresAt(payload.expiresAt)
  });

  return {
    apiKey: rawKey,
    key: {
      id: created.id,
      label: created.label,
      keyPrefix: created.key_prefix,
      createdAt: created.created_at,
      expiresAt: created.expires_at
    }
  };
};

exports.listKeys = async (userId) => {
  const rows = await keyRepo.listByUser(userId);
  return rows.map((row) => ({
    id: row.id,
    label: row.label,
    keyPrefix: row.key_prefix,
    lastUsedAt: row.last_used_at,
    expiresAt: row.expires_at,
    revokedAt: row.revoked_at,
    createdAt: row.created_at
  }));
};

exports.revokeKey = async (userId, id) => {
  const keyId = Number(id);
  if (!Number.isInteger(keyId) || keyId <= 0) {
    throw new AppError("Invalid API key id", 400);
  }

  const ok = await keyRepo.revoke(userId, keyId);
  if (!ok) {
    throw new AppError("API key not found or already revoked", 404);
  }
};

exports.authenticate = async (rawApiKey) => {
  if (!rawApiKey || String(rawApiKey).length < 16) {
    throw new AppError("Invalid extension API key", 401);
  }

  const keyRow = await keyRepo.getActiveByHash(hashApiKey(rawApiKey));
  if (!keyRow) {
    throw new AppError("Invalid extension API key", 401);
  }

  await keyRepo.touchLastUsed(keyRow.id);
  return keyRow;
};

exports.__testables = {
  hashApiKey
};
