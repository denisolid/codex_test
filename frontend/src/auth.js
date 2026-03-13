import "./style.css";
import { hasSupabaseConfig, supabase } from "./supabaseClient";
import { API_URL } from "./config";
import {
  clearAuthToken,
  setAuthToken,
  withAuthHeaders
} from "./authToken";
import { initObservability } from "./observability";

const app = document.querySelector("#app");
initObservability();

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

function getVerificationErrorMessage(code) {
  const safeCode = String(code || "").trim().toLowerCase();
  const messageByCode = {
    email_verification_token_missing: "Verification link is missing required token data.",
    email_verification_invalid: "Verification link is invalid. Request a new verification email.",
    email_verification_used: "Verification link was already used. Try logging in or request a new link.",
    email_verification_expired: "Verification link expired. Request a new verification email.",
    email_verification_failed: "Verification failed. Request a new verification email.",
    email_in_use: "This email is already in use. Try logging in instead."
  };
  return (
    messageByCode[safeCode] ||
    "Verification failed. Request a new verification email."
  );
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
        <p class="eyebrow">Skin Alpha</p>
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
            ? `<p class="helper-text">Need a new verification link for <strong>${escapeHtml(
                pendingConfirmationEmail
              )}</strong>? We will send another confirmation link to this inbox.</p>`
            : ""
        }

        <div class="auth-action-stack">
          ${
            isLogin && showResendConfirmation
              ? `<button id="resend-confirm-btn" class="google-btn auth-action-btn" type="button" ${
                  resendInFlight || resendCooldownActive ? "disabled" : ""
                }>${escapeHtml(getResendButtonLabel())}</button>`
              : ""
          }
          <button id="google-auth-btn" class="google-btn auth-action-btn auth-social-btn" type="button" ${
            authSubmitInFlight ? "disabled" : ""
          }>
            <span class="auth-social-icon" aria-hidden="true">
              <svg viewBox="0 0 24 24" focusable="false">
                <path fill="#EA4335" d="M12 10.2v3.9h5.5c-.2 1.2-.9 2.2-1.9 2.9l3.1 2.4c1.8-1.7 2.8-4.2 2.8-7.2 0-.7-.1-1.4-.2-2H12z" />
                <path fill="#34A853" d="M12 22c2.7 0 5-0.9 6.6-2.6l-3.1-2.4c-.9.6-2 .9-3.5.9-2.7 0-4.9-1.8-5.7-4.2l-3.2 2.5C4.7 19.7 8 22 12 22z" />
                <path fill="#4A90E2" d="M6.3 13.7c-.2-.6-.3-1.2-.3-1.7s.1-1.2.3-1.7l-3.2-2.5C2.4 9 2 10.4 2 12s.4 3 1.1 4.2l3.2-2.5z" />
                <path fill="#FBBC05" d="M12 6.1c1.5 0 2.9.5 4 1.6l3-3C17 2.8 14.7 2 12 2 8 2 4.7 4.3 3.1 7.8l3.2 2.5C7.1 7.9 9.3 6.1 12 6.1z" />
              </svg>
            </span>
            <span class="auth-social-label">Continue with Google</span>
          </button>
          <button id="steam-auth-btn" class="google-btn auth-action-btn auth-social-btn" type="button" ${
            authSubmitInFlight ? "disabled" : ""
          }>
            <span class="auth-social-icon" aria-hidden="true">
              <svg viewBox="0 0 24 24" focusable="false">
                <circle cx="17.5" cy="6.5" r="3.4" fill="currentColor" />
                <circle cx="17.5" cy="6.5" r="1.5" fill="rgba(12,18,30,0.9)" />
                <circle cx="9" cy="14.5" r="4.5" fill="none" stroke="currentColor" stroke-width="2" />
                <circle cx="9" cy="14.5" r="1.7" fill="currentColor" />
                <path d="M12.3 12.7l2.9-1.8" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" />
              </svg>
            </span>
            <span class="auth-social-label">Continue with Steam</span>
          </button>
        </div>

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
    const requestBody =
      page === "register"
        ? {
            email,
            password,
            next: `${window.location.origin}/login.html`
          }
        : { email, password };
    const res = await fetch(`${API_URL}${path}`, {
      method: "POST",
      credentials: "include",
      headers: withAuthHeaders({ "Content-Type": "application/json" }),
      body: JSON.stringify(requestBody)
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
      if (payload?.verificationEmailSent === false) {
        params.set("verifyEmailSent", "0");
      }
      window.location.href = `/login.html?${params.toString()}`;
      return;
    }

    if (payload?.accessToken) {
      setAuthToken(payload.accessToken);
    }

    window.location.href = "/?syncOnLogin=1";
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
      body: JSON.stringify({
        email: pendingConfirmationEmail,
        next: `${window.location.origin}/login.html`
      })
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
  const verificationEmailSent = params.get("verifyEmailSent") !== "0";
  const verificationFlow = params.get("verification") === "1";
  const verificationErrorCode = String(params.get("error") || "")
    .trim()
    .toLowerCase();
  const email = String(params.get("email") || "").trim();

  if (email) {
    viewState.email = email;
    pendingConfirmationEmail = email;
  }

  if (confirmed) {
    viewState.info = "Email confirmed. You can now log in.";
    showResendConfirmation = false;
  } else if (verificationFlow && verificationErrorCode) {
    viewState.error = getVerificationErrorMessage(verificationErrorCode);
    showResendConfirmation = Boolean(email);
  } else if (registered && requiresConfirmation) {
    viewState.info = verificationEmailSent
      ? "Account created. Confirm your email from inbox before logging in."
      : "Account created, but we could not send verification email. Use resend below.";
    showResendConfirmation = Boolean(email);
  } else if (registered) {
    viewState.info = "Account created. You can now log in.";
  }

  if (confirmed || registered || verificationFlow || verificationErrorCode || email) {
    window.history.replaceState({}, "", "/login.html");
  }
}

window.addEventListener("beforeunload", clearResendCooldownTimer);

render();

async function checkExistingSession() {
  try {
    const res = await fetch(`${API_URL}/auth/bootstrap`, {
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
