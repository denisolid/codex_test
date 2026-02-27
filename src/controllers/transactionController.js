const asyncHandler = require("../utils/asyncHandler");
const txService = require("../services/transactionService");
const planService = require("../services/planService");
const { toCsv } = require("../utils/csv");

exports.create = asyncHandler(async (req, res) => {
  const created = await txService.create(req.userId, req.body);
  res.status(201).json(created);
});

exports.list = asyncHandler(async (req, res) => {
  const rows = await txService.list(req.userId);
  res.json({ items: rows });
});

exports.getById = asyncHandler(async (req, res) => {
  const row = await txService.getById(req.userId, Number(req.params.id));
  res.json(row);
});

exports.update = asyncHandler(async (req, res) => {
  const row = await txService.update(req.userId, Number(req.params.id), req.body);
  res.json(row);
});

exports.remove = asyncHandler(async (req, res) => {
  await txService.remove(req.userId, Number(req.params.id));
  res.status(204).send();
});

exports.exportCsv = asyncHandler(async (req, res) => {
  const { entitlements } = await planService.requireFeature(req.userId, "csvExport", {
    message: "CSV export is available on Pro plan and above."
  });

  const rows = await txService.list(req.userId);
  const headers = [
    "id",
    "skin_id",
    "type",
    "quantity",
    "unit_price",
    "commission_percent",
    "gross_total",
    "net_total",
    "currency",
    "executed_at",
    "created_at"
  ];

  const csv = toCsv(headers, (rows || []).slice(0, entitlements.maxCsvRows));
  res.setHeader("Content-Type", "text/csv; charset=utf-8");
  res.setHeader("Content-Disposition", "attachment; filename=\"transactions-export.csv\"");
  res.status(200).send(csv);
});
