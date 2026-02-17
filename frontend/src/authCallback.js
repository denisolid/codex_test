import "./style.css";
import { hasSupabaseConfig, supabase } from "./supabaseClient";

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

    localStorage.setItem("accessToken", token);
    window.location.href = "/";
  } catch (err) {
    render(err.message || "Google authentication failed", true);
  }
}

finalize();
