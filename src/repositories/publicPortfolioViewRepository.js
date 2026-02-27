const { supabaseAdmin } = require("../config/supabase");
const AppError = require("../utils/AppError");

exports.recordView = async (ownerUserId, referrer = null) => {
  const row = {
    owner_user_id: ownerUserId,
    referrer: String(referrer || "").trim() || null
  };

  const { error } = await supabaseAdmin.from("public_portfolio_views").insert(row);
  if (error) {
    throw new AppError(error.message, 500);
  }
};

exports.countByOwnersSince = async (ownerUserIds, sinceIso) => {
  if (!Array.isArray(ownerUserIds) || !ownerUserIds.length) {
    return {};
  }

  const { data, error } = await supabaseAdmin
    .from("public_portfolio_views")
    .select("owner_user_id, referrer")
    .in("owner_user_id", ownerUserIds)
    .gte("viewed_at", sinceIso);

  if (error) {
    throw new AppError(error.message, 500);
  }

  return (data || []).reduce((acc, row) => {
    const key = String(row.owner_user_id || "");
    if (!key) return acc;
    if (!acc[key]) {
      acc[key] = { views: 0, referrals: 0 };
    }

    acc[key].views += 1;
    if (String(row.referrer || "").trim()) {
      acc[key].referrals += 1;
    }
    return acc;
  }, {});
};
