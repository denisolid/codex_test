const AppError = require("../utils/AppError");
const {
  nodeEnv,
  emailProvider,
  resendApiKey,
  emailFrom,
  emailReplyTo
} = require("../config/env");

function normalizeEmail(value) {
  return String(value || "").trim().toLowerCase();
}

async function sendWithResend({ to, subject, html, text }) {
  if (!resendApiKey) {
    throw new AppError(
      "Missing RESEND_API_KEY for outbound email",
      503,
      "EMAIL_PROVIDER_NOT_CONFIGURED"
    );
  }
  if (!emailFrom) {
    throw new AppError(
      "Missing EMAIL_FROM for outbound email",
      503,
      "EMAIL_PROVIDER_NOT_CONFIGURED"
    );
  }

  const response = await fetch("https://api.resend.com/emails", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${resendApiKey}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      from: emailFrom,
      to: [to],
      reply_to: emailReplyTo || undefined,
      subject,
      html,
      text
    })
  });

  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    const message = String(payload?.message || payload?.error || "Email send failed");
    throw new AppError(message, 502, "EMAIL_SEND_FAILED");
  }

  return {
    provider: "resend",
    id: payload?.id || null
  };
}

async function sendWithConsole({ to, subject, text }) {
  const safeTo = normalizeEmail(to);
  console.log(
    `[email:console] to=${safeTo} subject="${String(subject || "").trim()}"\n${String(text || "").trim()}`
  );
  return {
    provider: "console",
    id: null
  };
}

exports.sendEmail = async ({ to, subject, html, text }) => {
  const safeTo = normalizeEmail(to);
  if (!safeTo) {
    throw new AppError("Recipient email is required", 400, "INVALID_EMAIL");
  }

  const provider = String(emailProvider || "console").trim().toLowerCase();
  if (provider === "resend") {
    return sendWithResend({ to: safeTo, subject, html, text });
  }

  if (provider === "console") {
    if (String(nodeEnv || "").trim().toLowerCase() === "production") {
      throw new AppError(
        "Email delivery is not configured on the server.",
        503,
        "EMAIL_PROVIDER_NOT_CONFIGURED"
      );
    }
    return sendWithConsole({ to: safeTo, subject, text: text || "" });
  }

  throw new AppError(
    "Unsupported email provider configuration",
    500,
    "EMAIL_PROVIDER_NOT_CONFIGURED"
  );
};

exports.sendSteamOnboardingVerificationEmail = async ({
  to,
  verifyUrl,
  displayName = ""
}) => {
  const safeTo = normalizeEmail(to);
  const safeVerifyUrl = String(verifyUrl || "").trim();
  if (!safeTo) {
    throw new AppError("Recipient email is required", 400, "INVALID_EMAIL");
  }
  if (!safeVerifyUrl) {
    throw new AppError("Verification URL is required", 500, "EMAIL_VERIFICATION_URL_MISSING");
  }

  const greeting = String(displayName || "").trim() || "there";
  const subject = "Verify your email to activate your free plan";
  const text = [
    `Hi ${greeting},`,
    "",
    "Finish onboarding by verifying your email address.",
    `Verify now: ${safeVerifyUrl}`,
    "",
    "If you didn't request this, you can ignore this email."
  ].join("\n");

  const html = `
    <div style="background:#090f1f;padding:24px;font-family:Inter,Arial,sans-serif;color:#d7def5;">
      <div style="max-width:560px;margin:0 auto;background:linear-gradient(140deg,#0e1730,#101a39);border:1px solid #1f2a4d;border-radius:14px;padding:24px;">
        <p style="margin:0 0 10px;color:#96a5d9;font-size:12px;letter-spacing:0.08em;text-transform:uppercase;">Skin Alpha</p>
        <h1 style="margin:0 0 14px;font-size:24px;line-height:1.2;color:#f2f5ff;">Verify your email</h1>
        <p style="margin:0 0 18px;color:#c8d0ef;">Hi ${greeting}, finish onboarding to activate your free plan.</p>
        <p style="margin:0 0 22px;">
          <a href="${safeVerifyUrl}" style="display:inline-block;background:#3b82f6;color:#ffffff;text-decoration:none;padding:11px 16px;border-radius:10px;font-weight:600;">
            Verify Email
          </a>
        </p>
        <p style="margin:0;color:#9aa7d6;font-size:13px;line-height:1.5;">If the button does not work, open this link:</p>
        <p style="margin:6px 0 0;word-break:break-all;color:#9fc2ff;font-size:13px;">${safeVerifyUrl}</p>
      </div>
    </div>
  `;

  return exports.sendEmail({
    to: safeTo,
    subject,
    html,
    text
  });
};

exports.sendAccountVerificationEmail = async ({
  to,
  verifyUrl,
  displayName = ""
}) => {
  const safeTo = normalizeEmail(to);
  const safeVerifyUrl = String(verifyUrl || "").trim();
  if (!safeTo) {
    throw new AppError("Recipient email is required", 400, "INVALID_EMAIL");
  }
  if (!safeVerifyUrl) {
    throw new AppError("Verification URL is required", 500, "EMAIL_VERIFICATION_URL_MISSING");
  }

  const greeting = String(displayName || "").trim() || "there";
  const subject = "Confirm your Skin Alpha account";
  const text = [
    `Hi ${greeting},`,
    "",
    "Your account was created successfully.",
    "Confirm your email address to complete registration:",
    safeVerifyUrl,
    "",
    "If you did not create this account, you can ignore this email."
  ].join("\n");

  const html = `
    <div style="background:#090f1f;padding:24px;font-family:Inter,Arial,sans-serif;color:#d7def5;">
      <div style="max-width:560px;margin:0 auto;background:linear-gradient(140deg,#0e1730,#101a39);border:1px solid #1f2a4d;border-radius:14px;padding:24px;">
        <p style="margin:0 0 10px;color:#96a5d9;font-size:12px;letter-spacing:0.08em;text-transform:uppercase;">Skin Alpha</p>
        <h1 style="margin:0 0 14px;font-size:24px;line-height:1.2;color:#f2f5ff;">Confirm your email</h1>
        <p style="margin:0 0 18px;color:#c8d0ef;">Hi ${greeting}, your account was created successfully. Confirm your email to finish setup.</p>
        <p style="margin:0 0 22px;">
          <a href="${safeVerifyUrl}" style="display:inline-block;background:#3b82f6;color:#ffffff;text-decoration:none;padding:11px 16px;border-radius:10px;font-weight:600;">
            Confirm Email
          </a>
        </p>
        <p style="margin:0;color:#9aa7d6;font-size:13px;line-height:1.5;">If the button does not work, open this link:</p>
        <p style="margin:6px 0 0;word-break:break-all;color:#9fc2ff;font-size:13px;">${safeVerifyUrl}</p>
      </div>
    </div>
  `;

  return exports.sendEmail({
    to: safeTo,
    subject,
    html,
    text
  });
};
