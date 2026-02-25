import "./style.css";
import { hasSupabaseConfig, supabase } from "./supabaseClient";
import { API_URL } from "./config";

const app = document.querySelector("#app");
const page = window.location.pathname.endsWith("register.html")
  ? "register"
  : "login";
let pendingConfirmationEmail = "";
let showResendConfirmation = false;
let resendInFlight = false;
const viewState = {
  error: "",
  info: "",
  email: ""
};

checkExistingSession();

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function isEmailNotConfirmedError(message) {
  return /email\s+not\s+confirmed/i.test(String(message || ""));
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

        ${error ? `<div class="error">${escapeHtml(error)}</div>` : ""}
        ${info ? `<div class="info">${escapeHtml(info)}</div>` : ""}

        <form id="auth-form" class="form">
          <label>Email
            <input id="email" type="email" placeholder="you@example.com" value="${escapeHtml(emailValue)}" required />
          </label>
          <label>Password
            <input id="password" type="password" placeholder="At least 6 characters" required />
          </label>
          <button type="submit">${isLogin ? "Login" : "Create account"}</button>
        </form>
        ${
          isLogin && showResendConfirmation
            ? `
              <p class="helper-text">Need a new verification link for <strong>${escapeHtml(
                pendingConfirmationEmail
              )}</strong>?</p>
              <button id="resend-confirm-btn" class="google-btn" type="button" ${
                resendInFlight ? "disabled" : ""
              }>${resendInFlight ? "Sending..." : "Resend confirmation email"}</button>
            `
            : ""
        }

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

  if (isLogin && showResendConfirmation) {
    document
      .querySelector("#resend-confirm-btn")
      ?.addEventListener("click", onResendConfirmation);
  }
}

async function onSubmit(e) {
  e.preventDefault();
  const email = document.querySelector("#email").value.trim();
  const password = document.querySelector("#password").value;

  try {
    const path = page === "login" ? "/auth/login" : "/auth/register";
    const res = await fetch(`${API_URL}${path}`, {
      method: "POST",
      credentials: "include",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ email, password })
    });
    const payload = await res.json().catch(() => ({}));

    if (!res.ok) {
      const message = payload.error || "Request failed";
      if (page === "login" && isEmailNotConfirmedError(message)) {
        pendingConfirmationEmail = email;
        showResendConfirmation = true;
        render(
          "Email not confirmed. Open your inbox and confirm your email before login.",
          "If you did not receive it, resend confirmation below.",
          email
        );
        return;
      }

      showResendConfirmation = false;
      pendingConfirmationEmail = "";
      render(message, "", email);
      return;
    }

    if (page === "register") {
      render(
        "",
        "Registration successful. If email confirmation is enabled, check inbox before login.",
        email
      );
      return;
    }

    window.location.href = "/";
  } catch (_err) {
    render("Network error. Check backend and frontend URLs.", "", email);
  }
}

async function onGoogleAuth() {
  try {
    showResendConfirmation = false;
    pendingConfirmationEmail = "";

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

async function onResendConfirmation() {
  if (!pendingConfirmationEmail || resendInFlight) {
    return;
  }

  resendInFlight = true;
  render("", "Sending confirmation email...", pendingConfirmationEmail);

  try {
    const res = await fetch(`${API_URL}/auth/resend-confirmation`, {
      method: "POST",
      credentials: "include",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ email: pendingConfirmationEmail })
    });
    const payload = await res.json().catch(() => ({}));

    if (!res.ok) {
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

render();

async function checkExistingSession() {
  try {
    const res = await fetch(`${API_URL}/auth/me`, {
      credentials: "include"
    });
    if (res.ok) {
      window.location.href = "/";
    }
  } catch (_err) {
    // Ignore network errors on initial session check.
  }
}
