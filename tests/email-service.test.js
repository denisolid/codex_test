const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");

const envPath = path.resolve(__dirname, "../src/config/env.js");
const emailServicePath = path.resolve(__dirname, "../src/services/emailService.js");

function primeModule(modulePath, exportsValue) {
  require.cache[modulePath] = {
    id: modulePath,
    filename: modulePath,
    loaded: true,
    exports: exportsValue
  };
}

function clearModule(modulePath) {
  delete require.cache[modulePath];
}

test("email service blocks console provider in production", async () => {
  clearModule(emailServicePath);
  clearModule(envPath);

  primeModule(envPath, {
    nodeEnv: "production",
    emailProvider: "console",
    resendApiKey: "",
    emailFrom: "",
    emailReplyTo: ""
  });

  const emailService = require(emailServicePath);

  await assert.rejects(
    () =>
      emailService.sendEmail({
        to: "user@example.com",
        subject: "Test",
        text: "Hello"
      }),
    (err) => {
      assert.equal(err.code, "EMAIL_PROVIDER_NOT_CONFIGURED");
      assert.equal(err.statusCode, 503);
      return true;
    }
  );
});

test("email service allows console provider in development", async () => {
  clearModule(emailServicePath);
  clearModule(envPath);

  primeModule(envPath, {
    nodeEnv: "development",
    emailProvider: "console",
    resendApiKey: "",
    emailFrom: "",
    emailReplyTo: ""
  });

  const emailService = require(emailServicePath);
  const originalLog = console.log;
  const logs = [];
  console.log = (...args) => logs.push(args.join(" "));

  try {
    const result = await emailService.sendEmail({
      to: "user@example.com",
      subject: "Test",
      text: "Hello"
    });

    assert.equal(result.provider, "console");
    assert.ok(logs.length > 0);
  } finally {
    console.log = originalLog;
  }
});
