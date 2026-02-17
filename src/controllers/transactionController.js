const asyncHandler = require("../utils/asyncHandler");
const txService = require("../services/transactionService");

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
