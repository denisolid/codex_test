const fallbackEscape = (value) =>
  String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");

function initialsFromLabel(label) {
  const chunks = String(label || "")
    .trim()
    .split(/\s+|@/g)
    .filter(Boolean);
  if (!chunks.length) return "U";
  if (chunks.length === 1) return chunks[0][0]?.toUpperCase() || "U";
  return `${chunks[0][0] || ""}${chunks[1][0] || ""}`.toUpperCase();
}

export function renderAvatarMenu({
  open = false,
  userLabel = "Signed in",
  userTitle = "",
  notificationCount = 0,
  escapeHtml = fallbackEscape
} = {}) {
  const safeCount = Math.max(Number(notificationCount || 0), 0);
  const badge = safeCount > 0 ? `<span class="avatar-badge">${safeCount > 99 ? "99+" : safeCount}</span>` : "";
  const initials = initialsFromLabel(userLabel);

  return `
    <div class="avatar-menu-root" data-avatar-menu-root>
      <button
        type="button"
        class="ghost-btn avatar-menu-toggle ${open ? "open" : ""}"
        id="avatar-menu-toggle"
        aria-label="Open account menu"
        aria-expanded="${open ? "true" : "false"}"
        aria-controls="avatar-menu-dropdown"
      >
        <span class="avatar-chip" title="${escapeHtml(userTitle)}">${escapeHtml(initials)}</span>
        ${badge}
      </button>
      ${
        open
          ? `
            <section class="avatar-menu-dropdown" id="avatar-menu-dropdown" role="menu" aria-label="Account menu">
              <button type="button" class="avatar-menu-item" role="menuitem" data-avatar-action="account-profile"><span>&#128100;</span>Account / Profile</button>
              <button type="button" class="avatar-menu-item" role="menuitem" data-avatar-action="account-api-keys"><span>&#128273;</span>API Keys</button>
              <button type="button" class="avatar-menu-item danger" role="menuitem" data-avatar-action="logout"><span>&#10231;</span>Logout</button>
            </section>
          `
          : ""
      }
    </div>
  `;
}
