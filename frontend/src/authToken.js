const AUTH_TOKEN_STORAGE_KEY = "cs2sa:access_token";

export function getAuthToken() {
  try {
    const token = String(localStorage.getItem(AUTH_TOKEN_STORAGE_KEY) || "").trim();
    return token || null;
  } catch (_err) {
    return null;
  }
}

export function setAuthToken(token) {
  const safeToken = String(token || "").trim();
  if (!safeToken) {
    clearAuthToken();
    return;
  }

  try {
    localStorage.setItem(AUTH_TOKEN_STORAGE_KEY, safeToken);
  } catch (_err) {
    // Ignore storage write errors (private mode, quota, etc.).
  }
}

export function clearAuthToken() {
  try {
    localStorage.removeItem(AUTH_TOKEN_STORAGE_KEY);
  } catch (_err) {
    // Ignore storage clear errors.
  }
}

export function withAuthHeaders(headers = {}) {
  const token = getAuthToken();
  if (!token) {
    return headers;
  }

  return {
    ...headers,
    Authorization: `Bearer ${token}`
  };
}
