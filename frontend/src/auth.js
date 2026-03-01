import "./style.css";
import { hasSupabaseConfig, supabase } from "./supabaseClient";
import { API_URL } from "./config";
import {
  clearAuthToken,
  setAuthToken,
  withAuthHeaders
} from "./authToken";

const app = document.querySelector("#app");
const page = window.location.pathname.endsWith("register.html")
  ? "register"
  : "login";
const RESEND_COOLDOWN_SECONDS = 30;

let pendingConfirmationEmail = "";
let showResendConfirmation = false;
let authSubmitInFlight = false;
let resendInFlight = false;
let resendCooldownActive = false;
let resendCooldownTimer = null;

const viewState = {
  error: "",
  info: "",
  email: ""
};

checkExistingSession();
hydrateFromUrlState();

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function getErrorCode(payload) {
  return String(payload?.code || "")
    .trim()
    .toUpperCase();
}

function isEmailNotConfirmedError(payload) {
  return (
    getErrorCode(payload) === "EMAIL_NOT_CONFIRMED" ||
    /email\s+not\s+confirmed/i.test(String(payload?.error || ""))
  );
}

function isRateLimited(payload) {
  return getErrorCode(payload) === "RATE_LIMITED";
}

function getResendButtonLabel() {
  if (resendInFlight) return "Sending...";
  if (resendCooldownActive) {
    return "Please wait before resending";
  }
  return "Resend confirmation email";
}

function clearResendCooldownTimer() {
  if (!resendCooldownTimer) return;
  clearInterval(resendCooldownTimer);
  resendCooldownTimer = null;
}

function startResendCooldown(seconds = RESEND_COOLDOWN_SECONDS) {
  const durationMs = Math.max(Number(seconds) || 0, 0) * 1000;
  clearResendCooldownTimer();

  if (!durationMs) {
    resendCooldownActive = false;
    return;
  }

  resendCooldownActive = true;
  resendCooldownTimer = setTimeout(() => {
    resendCooldownActive = false;
    resendCooldownTimer = null;
    render();
  }, durationMs);

  render();
}

function render(
  error = viewState.error,
  info = viewState.info,
  emailValue = viewState.email
) {
  viewState.error = error;
  viewState.info = info;
  viewState.email = emailValue;

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

        ${
          error
            ? `<div class="error" role="alert" aria-live="assertive">${escapeHtml(
                error
              )}</div>`
            : ""
        }
        ${
          info
            ? `<div class="info" role="status" aria-live="polite">${escapeHtml(
                info
              )}</div>`
            : ""
        }

        <form id="auth-form" class="form">
          <label>Email
            <input id="email" type="email" placeholder="you@example.com" value="${escapeHtml(emailValue)}" required />
          </label>
          <label>Password
            <input id="password" type="password" placeholder="At least 6 characters" required />
          </label>
          <button type="submit" ${authSubmitInFlight ? "disabled" : ""}>${
            authSubmitInFlight ? "Please wait..." : isLogin ? "Login" : "Create account"
          }</button>
        </form>
        ${
          isLogin && showResendConfirmation
            ? `
              <p class="helper-text">Need a new verification link for <strong>${escapeHtml(
                pendingConfirmationEmail
              )}</strong>? Supabase sends a confirmation link, not a 6-digit code.</p>
              <button id="resend-confirm-btn" class="google-btn" type="button" ${
                resendInFlight || resendCooldownActive ? "disabled" : ""
              }>${escapeHtml(getResendButtonLabel())}</button>
            `
            : ""
        }

        <button id="google-auth-btn" class="google-btn" type="button" ${
          authSubmitInFlight ? "disabled" : ""
        }>Continue with Google</button>
        <button id="steam-auth-btn" class="google-btn" type="button" ${
          authSubmitInFlight ? "disabled" : ""
        }>Continue with Steam</button>

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
  document
    .querySelector("#steam-auth-btn")
    .addEventListener("click", onSteamAuth);

  if (isLogin && showResendConfirmation) {
    document
      .querySelector("#resend-confirm-btn")
      ?.addEventListener("click", onResendConfirmation);
  }
}

async function onSubmit(e) {
  e.preventDefault();
  if (authSubmitInFlight) return;

  const email = document.querySelector("#email").value.trim();
  const password = document.querySelector("#password").value;

  authSubmitInFlight = true;
  render("", "", email);

  try {
    const path = page === "login" ? "/auth/login" : "/auth/register";
    const res = await fetch(`${API_URL}${path}`, {
      method: "POST",
      credentials: "include",
      headers: withAuthHeaders({ "Content-Type": "application/json" }),
      body: JSON.stringify({ email, password })
    });
    const payload = await res.json().catch(() => ({}));

    if (!res.ok) {
      const message = payload.error || "Request failed";
      if (page === "login" && isEmailNotConfirmedError(payload)) {
        pendingConfirmationEmail = email;
        showResendConfirmation = true;
        render(
          "Email not confirmed. Open your inbox and click the confirmation link before login.",
          "If you did not receive the email, resend confirmation below.",
          email
        );
        return;
      }

      if (isRateLimited(payload)) {
        render("Too many requests. Please wait and try again.", "", email);
        return;
      }

      showResendConfirmation = false;
      pendingConfirmationEmail = "";
      render(message, "", email);
      return;
    }

    if (page === "register") {
      const requiresConfirmation = payload.requiresEmailConfirmation !== false;
      const params = new URLSearchParams({
        registered: "1",
        email
      });
      if (requiresConfirmation) {
        params.set("confirm", "1");
      }
      window.location.href = `/login.html?${params.toString()}`;
      return;
    }

    if (payload?.accessToken) {
      setAuthToken(payload.accessToken);
    }

    window.location.href = "/";
  } catch (_err) {
    render("Network error. Check backend and frontend URLs.", "", email);
  } finally {
    authSubmitInFlight = false;
    render();
  }
}

async function onGoogleAuth() {
  if (authSubmitInFlight) return;

  try {
    authSubmitInFlight = true;
    showResendConfirmation = false;
    pendingConfirmationEmail = "";
    render("", "", viewState.email);

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
  } finally {
    authSubmitInFlight = false;
    render();
  }
}

function onSteamAuth() {
  if (authSubmitInFlight) return;

  const next = `${window.location.origin}/auth-callback.html`;
  const url = `${API_URL}/auth/steam/start?next=${encodeURIComponent(next)}`;
  window.location.href = url;
}

async function onResendConfirmation() {
  if (
    !pendingConfirmationEmail ||
    resendInFlight ||
    resendCooldownActive
  ) {
    return;
  }

  resendInFlight = true;
  render("", "Sending confirmation email...", pendingConfirmationEmail);

  try {
    const res = await fetch(`${API_URL}/auth/resend-confirmation`, {
      method: "POST",
      credentials: "include",
      headers: withAuthHeaders({ "Content-Type": "application/json" }),
      body: JSON.stringify({ email: pendingConfirmationEmail })
    });
    const payload = await res.json().catch(() => ({}));

    if (!res.ok) {
      if (isRateLimited(payload)) {
        render("Too many resend attempts. Please wait and try again.", "", pendingConfirmationEmail);
        startResendCooldown();
        return;
      }

      render(
        payload.error || "Could not resend confirmation email. Try again later.",
        "",
        pendingConfirmationEmail
      );
      return;
    }

    render(
      "",
      payload.message ||
        "Confirmation email sent. Check your inbox and spam folder.",
      pendingConfirmationEmail
    );
    startResendCooldown();
  } catch (_err) {
    render(
      "Network error while resending confirmation email.",
      "",
      pendingConfirmationEmail
    );
  } finally {
    resendInFlight = false;
    render();
  }
}

function hydrateFromUrlState() {
  if (page !== "login") return;

  const params = new URLSearchParams(window.location.search);
  const confirmed = params.get("confirmed") === "1";
  const registered = params.get("registered") === "1";
  const requiresConfirmation = params.get("confirm") === "1";
  const email = String(params.get("email") || "").trim();

  if (email) {
    viewState.email = email;
    pendingConfirmationEmail = email;
  }

  if (confirmed) {
    viewState.info = "Email confirmed. You can now log in.";
    showResendConfirmation = false;
  } else if (registered && requiresConfirmation) {
    viewState.info =
      "Account created. Confirm your email from inbox before logging in.";
    showResendConfirmation = Boolean(email);
  } else if (registered) {
    viewState.info = "Account created. You can now log in.";
  }

  if (confirmed || registered || email) {
    window.history.replaceState({}, "", "/login.html");
  }
}

window.addEventListener("beforeunload", clearResendCooldownTimer);

render();

async function checkExistingSession() {
  try {
    const res = await fetch(`${API_URL}/auth/me`, {
      credentials: "include",
      headers: withAuthHeaders()
    });
    if (res.ok) {
      window.location.href = "/";
      return;
    }

    if (res.status === 401) {
      clearAuthToken();
    }
  } catch (_err) {
    // Ignore network errors on initial session check.
  }
}
