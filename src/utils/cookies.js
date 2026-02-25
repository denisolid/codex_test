exports.getCookieValue = (cookieHeader, cookieName) => {
  if (!cookieHeader || !cookieName) return null;

  const parts = String(cookieHeader).split(";");
  for (const part of parts) {
    const [name, ...rest] = part.trim().split("=");
    if (name === cookieName) {
      return decodeURIComponent(rest.join("=") || "");
    }
  }

  return null;
};
