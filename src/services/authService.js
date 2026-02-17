const { supabaseAuthClient } = require("../config/supabase");
const AppError = require("../utils/AppError");

exports.register = async (email, password) => {
  const { data, error } = await supabaseAuthClient.auth.signUp({
    email,
    password
  });
  if (error) {
    throw new AppError(error.message, 400);
  }

  return {
    user: data.user,
    session: data.session
  };
};

exports.login = async (email, password) => {
  const { data, error } = await supabaseAuthClient.auth.signInWithPassword({
    email,
    password
  });
  if (error) {
    throw new AppError(error.message, 401);
  }

  return {
    accessToken: data.session.access_token,
    refreshToken: data.session.refresh_token,
    user: data.user
  };
};
