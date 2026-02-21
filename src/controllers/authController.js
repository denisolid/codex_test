const asyncHandler = require("../utils/asyncHandler");
const authService = require("../services/authService");
const authMiddleware = require("../middleware/authMiddleware");
const userRepo = require("../repositories/userRepository");
const { setAuthCookie, clearAuthCookie } = require("../utils/authCookie");

exports.register = asyncHandler(async (req, res) => {
  const { email, password } = req.body;
  const data = await authService.register(email, password);
  res.status(201).json(data);
});

exports.login = asyncHandler(async (req, res) => {
  const { email, password } = req.body;
  const data = await authService.login(email, password);
  setAuthCookie(res, data.accessToken);
  res.json({ user: data.user });
});

exports.createSession = asyncHandler(async (req, res) => {
  const { accessToken } = req.body;
  const user = await authService.getUserByAccessToken(accessToken);
  await userRepo.ensureExists(user.id, user.email);
  setAuthCookie(res, accessToken);
  res.json({ user });
});

exports.logout = asyncHandler(async (_req, res) => {
  clearAuthCookie(res);
  res.status(204).send();
});

exports.me = [
  authMiddleware,
  asyncHandler(async (req, res) => {
    res.json({ user: req.authUser });
  })
];
