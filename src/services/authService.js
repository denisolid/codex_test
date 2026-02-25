const { supabaseAdmin, supabaseAuthClient } = require("../config/supabase");
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
    user: data.user
  };
};

exports.getUserByAccessToken = async (token) => {
  if (!token) {
    throw new AppError("Missing access token", 401);
  }

  const { data, error } = await supabaseAdmin.auth.getUser(token);
  if (error || !data || !data.user) {
    throw new AppError("Invalid access token", 401);
  }

  return data.user;
};
