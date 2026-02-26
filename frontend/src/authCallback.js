import "./style.css";
import { hasSupabaseConfig, supabase } from "./supabaseClient";
import { API_URL } from "./config";
import { setAuthToken, withAuthHeaders } from "./authToken";

const app = document.querySelector("#app");

function render(message, isError = false) {
  app.innerHTML = `
    <main class="layout auth-layout">
      <article class="panel auth-panel">
        <h1>${isError ? "Auth failed" : "Completing sign-in"}</h1>
        <p class="${isError ? "error" : "info"}">${message}</p>
        <p class="muted"><a href="/login.html">Back to login</a></p>
      </article>
    </main>
  `;
}

async function finalize() {
  try {
    const hashParams = new URLSearchParams(window.location.hash.replace(/^#/, ""));
    const urlParams = new URLSearchParams(window.location.search);
    const hashToken = String(hashParams.get("accessToken") || "").trim();
    const steamError = String(urlParams.get("error") || "").trim();

    if (hashToken) {
      setAuthToken(hashToken);
      window.history.replaceState({}, "", "/auth-callback.html");
      window.location.href = "/";
      return;
    }

    if (steamError) {
      const messageByCode = {
        steam_login_cancelled: "Steam login cancelled. Please try again.",
        steam_verify_failed: "Steam login verification failed. Please retry.",
        steam_auth_failed: "Steam login failed. Please retry.",
        steam_account_create_failed: "Could not create Steam account. Try again later."
      };
      throw new Error(messageByCode[steamError] || "Steam login failed.");
    }

    if (!hasSupabaseConfig || !supabase) {
      throw new Error(
        "Google auth not configured. Missing VITE_SUPABASE_URL or VITE_SUPABASE_ANON_KEY"
      );
    }

    render("Processing Google authentication...");

    const { data, error } = await supabase.auth.getSession();
    if (error) {
      throw error;
    }

    const token = data?.session?.access_token;
    if (!token) {
      throw new Error("No access token returned from Google auth");
    }

    const sessionRes = await fetch(`${API_URL}/auth/session`, {
      method: "POST",
      headers: withAuthHeaders({ "Content-Type": "application/json" }),
      credentials: "include",
      body: JSON.stringify({ accessToken: token })
    });
    const payload = await sessionRes.json().catch(() => ({}));

    if (!sessionRes.ok) {
      throw new Error(payload.error || "Failed to create authenticated session");
    }

    setAuthToken(payload?.accessToken || token);
    window.location.href = "/";
  } catch (err) {
    render(err.message || "Google authentication failed", true);
  }
}

finalize();
