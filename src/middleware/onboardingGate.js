const AppError = require("../utils/AppError");
const userRepo = require("../repositories/userRepository");
const emailOnboardingService = require("../services/emailOnboardingService");

module.exports = async (req, _res, next) => {
  try {
    const userId = String(req.userId || "").trim();
    if (!userId) {
      throw new AppError("Unauthorized", 401, "UNAUTHORIZED");
    }

    const userProfile = req.userProfile || (await userRepo.getById(userId));
    if (!userProfile) {
      throw new AppError("Unauthorized", 401, "USER_NOT_FOUND");
    }

    req.userProfile = userProfile;
    const onboarding = emailOnboardingService.resolveOnboardingState({
      userProfile,
      authUser: req.authUser
    });
    req.onboardingState = onboarding;

    if (onboarding.onboardingRequired) {
      throw new AppError(
        "Verify your email to activate your free plan",
        403,
        "EMAIL_VERIFICATION_REQUIRED"
      );
    }

    next();
  } catch (err) {
    next(err);
  }
};
