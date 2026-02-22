import "./style.css";
import { hasSupabaseConfig, supabase } from "./supabaseClient";

const API_BASE =
  import.meta.env.VITE_API_BASE_URL || "http://localhost:4000/api";
const app = document.querySelector("#app");
const page = window.location.pathname.endsWith("register.html")
  ? "register"
  : "login";

checkExistingSession();

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function render(error = "", info = "") {
  const isLogin = page === "login";

  app.innerHTML = `
    <main class="layout auth-layout">
      <article class="panel auth-panel">
        <p class="eyebrow">CS2 Portfolio Analyzer</p>
        <h1>${isLogin ? "Welcome back" : "Create your account"}</h1>
        <p class="muted">${
          isLogin
            ? "Login to access your trading dashboard."
            : "Start tracking your CS2 items as a real portfolio."
        }</p>

        ${error ? `<div class="error">${escapeHtml(error)}</div>` : ""}
        ${info ? `<div class="info">${escapeHtml(info)}</div>` : ""}

        <form id="auth-form" class="form">
          <label>Email
            <input id="email" type="email" placeholder="you@example.com" required />
          </label>
          <label>Password
            <input id="password" type="password" placeholder="At least 6 characters" required />
          </label>
          <button type="submit">${isLogin ? "Login" : "Create account"}</button>
        </form>

        <button id="google-auth-btn" class="google-btn" type="button">Continue with Google</button>

        <div class="auth-links muted">
          <a href="/">Back to home</a>
          <span>|</span>
          ${
            isLogin
              ? '<a href="/register.html">Need an account? Register</a>'
              : '<a href="/login.html">Already have an account? Login</a>'
          }
        </div>
      </article>
    </main>
  `;

  document.querySelector("#auth-form").addEventListener("submit", onSubmit);
  document
    .querySelector("#google-auth-btn")
    .addEventListener("click", onGoogleAuth);
}

async function onSubmit(e) {
  e.preventDefault();
  const email = document.querySelector("#email").value.trim();
  const password = document.querySelector("#password").value;

  try {
    const path = page === "login" ? "/auth/login" : "/auth/register";
    const res = await fetch(`${API_BASE}${path}`, {
      method: "POST",
      credentials: "include",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ email, password })
    });
    const payload = await res.json().catch(() => ({}));

    if (!res.ok) {
      render(payload.error || "Request failed");
      return;
    }

    if (page === "register") {
      render("", "Registration successful. Continue with login.");
      return;
    }

    window.location.href = "/";
  } catch (_err) {
    render("Network error. Check backend and frontend URLs.");
  }
}

async function onGoogleAuth() {
  try {
    if (!hasSupabaseConfig || !supabase) {
      render(
        "Google auth not configured. Set VITE_SUPABASE_URL and VITE_SUPABASE_ANON_KEY in frontend/.env"
      );
      return;
    }

    const redirectTo = `${window.location.origin}/auth-callback.html`;
    const { data, error } = await supabase.auth.signInWithOAuth({
      provider: "google",
      options: { redirectTo }
    });

    if (error) {
      render(error.message);
      return;
    }

    if (data?.url) {
      window.location.href = data.url;
    }
  } catch (_err) {
    render("Google auth failed. Check Supabase Google provider settings.");
  }
}

render();

async function checkExistingSession() {
  try {
    const res = await fetch(`${API_BASE}/auth/me`, {
      credentials: "include"
    });
    if (res.ok) {
      window.location.href = "/";
    }
  } catch (_err) {
    // Ignore network errors on initial session check.
  }
}
