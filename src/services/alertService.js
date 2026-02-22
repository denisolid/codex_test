const AppError = require("../utils/AppError");
const alertRepo = require("../repositories/alertRepository");
const skinRepo = require("../repositories/skinRepository");
const priceRepo = require("../repositories/priceHistoryRepository");
const { alertCheckBatchSize } = require("../config/env");

function round2(n) {
  return Number((Number(n || 0)).toFixed(2));
}

function normalizeDirection(direction) {
  if (!direction) return "both";
  const normalized = String(direction).toLowerCase();
  if (["up", "down", "both"].includes(normalized)) {
    return normalized;
  }
  throw new AppError('direction must be "up", "down", or "both"', 400);
}

function validateAlertPayload(payload, partial = false) {
  const has = (k) => payload[k] !== undefined;

  if (!partial || has("skinId")) {
    const skinId = Number(payload.skinId);
    if (!Number.isInteger(skinId) || skinId <= 0) {
      throw new AppError("skinId must be a positive integer", 400);
    }
  }

  if (has("targetPrice")) {
    const target = payload.targetPrice == null ? null : Number(payload.targetPrice);
    if (target != null && (!Number.isFinite(target) || target < 0)) {
      throw new AppError("targetPrice must be a number >= 0", 400);
    }
  }

  if (has("percentChangeThreshold")) {
    const threshold =
      payload.percentChangeThreshold == null
        ? null
        : Number(payload.percentChangeThreshold);
    if (threshold != null && (!Number.isFinite(threshold) || threshold < 0)) {
      throw new AppError("percentChangeThreshold must be a number >= 0", 400);
    }
  }

  if (has("direction")) {
    normalizeDirection(payload.direction);
  }

  if (has("enabled") && typeof payload.enabled !== "boolean") {
    throw new AppError("enabled must be boolean", 400);
  }

  if (has("cooldownMinutes")) {
    const cooldown = Number(payload.cooldownMinutes);
    if (!Number.isInteger(cooldown) || cooldown < 0) {
      throw new AppError("cooldownMinutes must be an integer >= 0", 400);
    }
  }
}

async function ensureSkinExists(skinId) {
  const skin = await skinRepo.getById(skinId);
  if (!skin) {
    throw new AppError("Item not found", 404);
  }
}

function evaluateTargetTrigger(direction, targetPrice, currentPrice, previousPrice) {
  if (targetPrice == null || currentPrice == null) {
    return false;
  }

  if (direction === "up") {
    return currentPrice >= targetPrice;
  }

  if (direction === "down") {
    return currentPrice <= targetPrice;
  }

  if (previousPrice == null) {
    return Math.abs(currentPrice - targetPrice) <= 0.01;
  }

  return (
    (previousPrice < targetPrice && currentPrice >= targetPrice) ||
    (previousPrice > targetPrice && currentPrice <= targetPrice)
  );
}

function evaluatePercentChangeTrigger(direction, threshold, changePercent) {
  if (threshold == null || changePercent == null) {
    return false;
  }

  if (direction === "up") {
    return changePercent >= threshold;
  }

  if (direction === "down") {
    return changePercent <= -threshold;
  }

  return Math.abs(changePercent) >= threshold;
}

function isOnCooldown(lastTriggeredAt, cooldownMinutes, nowMs) {
  if (!lastTriggeredAt || !cooldownMinutes) {
    return false;
  }

  const last = new Date(lastTriggeredAt).getTime();
  if (Number.isNaN(last)) {
    return false;
  }

  return nowMs - last < cooldownMinutes * 60 * 1000;
}

exports.createAlert = async (userId, payload) => {
  validateAlertPayload(payload, false);
  await ensureSkinExists(payload.skinId);

  const targetPrice =
    payload.targetPrice == null ? null : Number(payload.targetPrice);
  const percentChangeThreshold =
    payload.percentChangeThreshold == null
      ? null
      : Number(payload.percentChangeThreshold);

  if (targetPrice == null && percentChangeThreshold == null) {
    throw new AppError(
      "At least one condition is required: targetPrice or percentChangeThreshold",
      400
    );
  }

  return alertRepo.create(userId, {
    ...payload,
    targetPrice,
    percentChangeThreshold,
    direction: normalizeDirection(payload.direction)
  });
};

exports.listAlerts = async (userId) => {
  const rows = await alertRepo.listByUser(userId);
  return rows.map((row) => ({
    id: row.id,
    skinId: row.skin_id,
    marketHashName: row.skins.market_hash_name,
    targetPrice: row.target_price,
    percentChangeThreshold: row.percent_change_threshold,
    direction: row.direction,
    enabled: row.enabled,
    cooldownMinutes: row.cooldown_minutes,
    lastTriggeredAt: row.last_triggered_at,
    createdAt: row.created_at,
    updatedAt: row.updated_at
  }));
};

exports.updateAlert = async (userId, id, payload) => {
  validateAlertPayload(payload, true);
  const alertId = Number(id);
  if (!Number.isInteger(alertId) || alertId <= 0) {
    throw new AppError("Invalid alert id", 400);
  }

  const existing = await alertRepo.getById(userId, alertId);
  if (!existing) {
    throw new AppError("Alert not found", 404);
  }

  const nextSkinId =
    payload.skinId != null ? Number(payload.skinId) : existing.skin_id;
  if (payload.skinId != null) {
    await ensureSkinExists(nextSkinId);
  }

  const targetPrice =
    payload.targetPrice !== undefined
      ? payload.targetPrice == null
        ? null
        : Number(payload.targetPrice)
      : existing.target_price;
  const percentChangeThreshold =
    payload.percentChangeThreshold !== undefined
      ? payload.percentChangeThreshold == null
        ? null
        : Number(payload.percentChangeThreshold)
      : existing.percent_change_threshold;

  if (targetPrice == null && percentChangeThreshold == null) {
    throw new AppError(
      "At least one condition is required: targetPrice or percentChangeThreshold",
      400
    );
  }

  const updated = await alertRepo.update(userId, alertId, {
    ...payload,
    skinId: nextSkinId,
    targetPrice,
    percentChangeThreshold,
    direction: normalizeDirection(payload.direction || existing.direction)
  });

  if (!updated) {
    throw new AppError("Alert not found", 404);
  }

  return updated;
};

exports.removeAlert = async (userId, id) => {
  const alertId = Number(id);
  if (!Number.isInteger(alertId) || alertId <= 0) {
    throw new AppError("Invalid alert id", 400);
  }

  const ok = await alertRepo.remove(userId, alertId);
  if (!ok) {
    throw new AppError("Alert not found", 404);
  }
};

exports.listAlertEvents = async (userId, limit = 100) => {
  const normalizedLimit = Math.min(Math.max(Number(limit) || 100, 1), 500);
  const rows = await alertRepo.listEventsByUser(userId, normalizedLimit);
  return rows.map((row) => ({
    id: row.id,
    alertId: row.alert_id,
    skinId: row.skin_id,
    marketHashName: row.skins.market_hash_name,
    triggerType: row.trigger_type,
    triggerValue: row.trigger_value,
    marketPrice: row.market_price,
    previousPrice: row.previous_price,
    changePercent: row.change_percent,
    triggeredAt: row.triggered_at
  }));
};

exports.checkAlertsNow = async (options = {}) => {
  const limit = Math.min(
    Math.max(Number(options.limit) || alertCheckBatchSize, 1),
    2000
  );
  const alerts = await alertRepo.listEnabled(limit);
  if (!alerts.length) {
    return {
      checkedAlerts: 0,
      triggeredAlerts: 0,
      skippedCooldown: 0,
      skippedNoPrice: 0
    };
  }

  const skinIds = [...new Set(alerts.map((alert) => alert.skin_id))];
  const latestBySkin = await priceRepo.getLatestPriceRowsBySkinIds(skinIds);
  const previousBySkin = await priceRepo.getLatestPricesBeforeDate(
    skinIds,
    new Date(Date.now() - 24 * 60 * 60 * 1000)
  );

  const now = new Date();
  const nowIso = now.toISOString();
  const nowMs = now.getTime();
  let triggeredAlerts = 0;
  let skippedCooldown = 0;
  let skippedNoPrice = 0;

  for (const alert of alerts) {
    const direction = normalizeDirection(alert.direction);
    const currentPrice = latestBySkin[alert.skin_id]
      ? Number(latestBySkin[alert.skin_id].price)
      : null;

    if (currentPrice == null || !Number.isFinite(currentPrice)) {
      skippedNoPrice += 1;
      continue;
    }

    if (isOnCooldown(alert.last_triggered_at, alert.cooldown_minutes, nowMs)) {
      skippedCooldown += 1;
      continue;
    }

    const previousPrice =
      previousBySkin[alert.skin_id] != null
        ? Number(previousBySkin[alert.skin_id])
        : null;
    const changePercent =
      previousPrice != null && previousPrice > 0
        ? round2(((currentPrice - previousPrice) / previousPrice) * 100)
        : null;

    const targetPrice =
      alert.target_price == null ? null : Number(alert.target_price);
    const percentThreshold =
      alert.percent_change_threshold == null
        ? null
        : Number(alert.percent_change_threshold);

    let triggerType = null;
    let triggerValue = null;

    if (
      evaluateTargetTrigger(direction, targetPrice, currentPrice, previousPrice)
    ) {
      triggerType = "target_price";
      triggerValue = targetPrice;
    } else if (
      evaluatePercentChangeTrigger(direction, percentThreshold, changePercent)
    ) {
      triggerType = "percent_change";
      triggerValue = percentThreshold;
    }

    if (!triggerType) {
      continue;
    }

    await alertRepo.createEvent({
      alertId: alert.id,
      userId: alert.user_id,
      skinId: alert.skin_id,
      triggerType,
      triggerValue,
      marketPrice: round2(currentPrice),
      previousPrice: previousPrice == null ? null : round2(previousPrice),
      changePercent,
      triggeredAt: nowIso
    });

    await alertRepo.markTriggered(alert.id, nowIso);
    triggeredAlerts += 1;
  }

  return {
    checkedAlerts: alerts.length,
    triggeredAlerts,
    skippedCooldown,
    skippedNoPrice
  };
};

exports.__testables = {
  validateAlertPayload,
  evaluateTargetTrigger,
  evaluatePercentChangeTrigger,
  isOnCooldown,
  normalizeDirection
};
