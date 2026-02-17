const { createClient } = require("@supabase/supabase-js");
const { supabaseUrl, supabaseAnonKey, supabaseServiceRoleKey } = require("./env");

const supabaseAuthClient = createClient(supabaseUrl, supabaseAnonKey, {
  auth: { persistSession: false, autoRefreshToken: false }
});

const supabaseAdmin = createClient(supabaseUrl, supabaseServiceRoleKey, {
  auth: { persistSession: false, autoRefreshToken: false }
});

module.exports = { supabaseAuthClient, supabaseAdmin };
