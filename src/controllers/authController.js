const asyncHandler = require("../utils/asyncHandler");
const authService = require("../services/authService");
const authMiddleware = require("../middleware/authMiddleware");
const userRepo = require("../repositories/userRepository");
const { setAuthCookie, clearAuthCookie } = require("../utils/authCookie");

exports.register = asyncHandler(async (req, res) => {
  const { email, password } = req.body;
  const data = await authService.register(email, password);

  if (data?.user?.id && data?.user?.email) {
    await userRepo.ensureExists(data.user.id, data.user.email);
  }

  res.status(201).json(data);
});

exports.login = asyncHandler(async (req, res) => {
  const { email, password } = req.body;
  const data = await authService.login(email, password);
  setAuthCookie(res, data.accessToken);
  res.json({ user: data.user, accessToken: data.accessToken });
});

exports.createSession = asyncHandler(async (req, res) => {
  const { accessToken } = req.body;
  const user = await authService.getUserByAccessToken(accessToken);
  await userRepo.ensureExists(user.id, user.email);
  setAuthCookie(res, accessToken);
  res.json({ user, accessToken });
});

exports.resendConfirmation = asyncHandler(async (req, res) => {
  const { email } = req.body;
  const result = await authService.resendConfirmation(email);
  res.json(result);
});

exports.logout = asyncHandler(async (_req, res) => {
  clearAuthCookie(res);
  res.status(204).send();
});

exports.me = [
  authMiddleware,
  asyncHandler(async (req, res) => {
    const emailConfirmed = Boolean(
      req.authUser?.email_confirmed_at || req.authUser?.confirmed_at
    );

    res.json({ user: req.authUser, emailConfirmed });
  })
];
