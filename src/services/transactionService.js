const AppError = require("../utils/AppError");
const txRepo = require("../repositories/transactionRepository");
const skinRepo = require("../repositories/skinRepository");
const tradeCalculatorService = require("./tradeCalculatorService");

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

  if (has("commissionPercent")) {
    if (
      typeof payload.commissionPercent !== "number" ||
      payload.commissionPercent < 0 ||
      payload.commissionPercent >= 100
    ) {
      throw new AppError("commissionPercent must be a number in [0, 100)", 400);
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

function buildFinancials(payload) {
  const commissionPercent =
    payload.commissionPercent == null ? 13 : Number(payload.commissionPercent);
  const quantity = Number(payload.quantity);
  const unitPrice = Number(payload.unitPrice);

  if (payload.type === "sell") {
    const calc = tradeCalculatorService.calculateTrade({
      buyPrice: 0,
      sellPrice: unitPrice,
      quantity,
      commissionPercent
    });

    return {
      commissionPercent: calc.commissionPercent,
      grossTotal: calc.grossSell,
      netTotal: calc.netSell
    };
  }

  const grossTotal = Number((quantity * unitPrice).toFixed(2));
  return {
    commissionPercent: Number(commissionPercent.toFixed(2)),
    grossTotal,
    netTotal: grossTotal
  };
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

async function validateSellAvailabilityForUpdate(userId, existing, merged) {
  if (merged.type !== "sell") return;

  const positions = await txRepo.getPositionCostBasisBySkin(userId);
  let available = Number(positions[merged.skinId]?.quantity || 0);

  if (Number(existing.skin_id) === Number(merged.skinId)) {
    const existingQty = Number(existing.quantity || 0);
    if (existing.type === "sell") {
      available += existingQty;
    } else if (existing.type === "buy") {
      available -= existingQty;
    }
  }

  if (merged.quantity > available) {
    throw new AppError(
      `Insufficient quantity to sell. Available: ${available}, requested: ${merged.quantity}`,
      400
    );
  }
}

exports.create = async (userId, payload) => {
  validatePayload(payload, false);
  await ensureSkinExists(payload.skinId);
  await validateSellAvailability(userId, payload);

  const financials = buildFinancials(payload);
  return txRepo.create(userId, { ...payload, ...financials });
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
  const existing = await txRepo.getById(userId, id);
  if (!existing) {
    throw new AppError("Transaction not found", 404);
  }

  const merged = {
    skinId: payload.skinId ?? existing.skin_id,
    type: payload.type ?? existing.type,
    quantity: payload.quantity ?? existing.quantity,
    unitPrice: payload.unitPrice ?? Number(existing.unit_price),
    commissionPercent:
      payload.commissionPercent ??
      Number(existing.commission_percent == null ? 13 : existing.commission_percent),
    currency: payload.currency ?? existing.currency,
    executedAt: payload.executedAt ?? existing.executed_at
  };

  if (merged.skinId != null) {
    await ensureSkinExists(merged.skinId);
  }

  await validateSellAvailabilityForUpdate(userId, existing, merged);

  const financials = buildFinancials(merged);

  const row = await txRepo.update(userId, id, {
    ...payload,
    commissionPercent: financials.commissionPercent,
    grossTotal: financials.grossTotal,
    netTotal: financials.netTotal
  });
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
