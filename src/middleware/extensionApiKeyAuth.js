const AppError = require("../utils/AppError");
const extensionApiKeyService = require("../services/extensionApiKeyService");

function readApiKey(req) {
  const direct = req.headers["x-extension-api-key"];
  if (direct) {
    return String(direct).trim();
  }

  const authHeader = String(req.headers.authorization || "");
  if (authHeader.toLowerCase().startsWith("apikey ")) {
    return authHeader.slice(7).trim();
  }

  return "";
}

module.exports = async (req, _res, next) => {
  try {
    const apiKey = readApiKey(req);
    if (!apiKey) {
      throw new AppError("Missing extension API key", 401);
    }

    const keyRow = await extensionApiKeyService.authenticate(apiKey);
    req.userId = keyRow.user_id;
    req.extensionApiKeyId = keyRow.id;
    next();
  } catch (err) {
    next(err);
  }
};
