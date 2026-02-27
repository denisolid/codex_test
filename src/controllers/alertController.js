const asyncHandler = require("../utils/asyncHandler");
const alertService = require("../services/alertService");

exports.create = asyncHandler(async (req, res) => {
  const created = await alertService.createAlert(req.userId, req.body || {});
  res.status(201).json(created);
});

exports.list = asyncHandler(async (req, res) => {
  const alerts = await alertService.listAlerts(req.userId);
  res.json({ items: alerts });
});

exports.update = asyncHandler(async (req, res) => {
  const updated = await alertService.updateAlert(
    req.userId,
    Number(req.params.id),
    req.body || {}
  );
  res.json(updated);
});

exports.remove = asyncHandler(async (req, res) => {
  await alertService.removeAlert(req.userId, Number(req.params.id));
  res.status(204).send();
});

exports.listEvents = asyncHandler(async (req, res) => {
  const events = await alertService.listAlertEvents(req.userId, req.query.limit);
  res.json({ items: events });
});

exports.listOwnershipEvents = asyncHandler(async (req, res) => {
  const events = await alertService.listOwnershipEvents(req.userId, req.query.limit);
  res.json({ items: events });
});

exports.updateOwnershipSettings = asyncHandler(async (req, res) => {
  const result = await alertService.updateOwnershipAlertSettings(req.userId, req.body || {});
  res.json(result);
});

exports.checkNow = asyncHandler(async (req, res) => {
  const result = await alertService.checkAlertsNow({ limit: req.body?.limit });
  res.json(result);
});
