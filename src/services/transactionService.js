const AppError = require("../utils/AppError");
const txRepo = require("../repositories/transactionRepository");
const skinRepo = require("../repositories/skinRepository");

function validatePayload(payload, partial = false) {
  const has = (k) => payload[k] !== undefined;

  if (!partial || has("skinId")) {
    if (!Number.isInteger(payload.skinId) || payload.skinId <= 0) {
      throw new AppError("skinId must be a positive integer", 400);
    }
  }

  if (!partial || has("type")) {
    if (!["buy", "sell"].includes(payload.type)) {
      throw new AppError('type must be "buy" or "sell"', 400);
    }
  }

  if (!partial || has("quantity")) {
    if (!Number.isInteger(payload.quantity) || payload.quantity <= 0) {
      throw new AppError("quantity must be a positive integer", 400);
    }
  }

  if (!partial || has("unitPrice")) {
    if (typeof payload.unitPrice !== "number" || payload.unitPrice < 0) {
      throw new AppError("unitPrice must be a number >= 0", 400);
    }
  }

  if (has("currency")) {
    if (typeof payload.currency !== "string" || payload.currency.length < 3) {
      throw new AppError("currency must be a valid code", 400);
    }
  }

  if (has("executedAt")) {
    const d = new Date(payload.executedAt);
    if (Number.isNaN(d.getTime())) {
      throw new AppError("executedAt must be a valid ISO date", 400);
    }
  }
}

async function ensureSkinExists(skinId) {
  const skin = await skinRepo.getById(skinId);
  if (!skin) {
    throw new AppError("Skin not found", 404);
  }
}

async function validateSellAvailability(userId, payload) {
  if (payload.type !== "sell") return;

  const positions = await txRepo.getPositionCostBasisBySkin(userId);
  const pos = positions[payload.skinId];
  const available = Number(pos?.quantity || 0);
  if (payload.quantity > available) {
    throw new AppError(
      `Insufficient quantity to sell. Available: ${available}, requested: ${payload.quantity}`,
      400
    );
  }
}

exports.create = async (userId, payload) => {
  validatePayload(payload, false);
  await ensureSkinExists(payload.skinId);
  await validateSellAvailability(userId, payload);
  return txRepo.create(userId, payload);
};

exports.list = async (userId) => {
  return txRepo.listByUser(userId);
};

exports.getById = async (userId, id) => {
  const row = await txRepo.getById(userId, id);
  if (!row) {
    throw new AppError("Transaction not found", 404);
  }
  return row;
};

exports.update = async (userId, id, payload) => {
  validatePayload(payload, true);
  if (payload.skinId != null) {
    await ensureSkinExists(payload.skinId);
  }
  if (payload.type === "sell" && payload.skinId != null && payload.quantity != null) {
    await validateSellAvailability(userId, payload);
  }

  const row = await txRepo.update(userId, id, payload);
  if (!row) {
    throw new AppError("Transaction not found", 404);
  }
  return row;
};

exports.remove = async (userId, id) => {
  const ok = await txRepo.remove(userId, id);
  if (!ok) {
    throw new AppError("Transaction not found", 404);
  }
};
