import { renderButton } from "./uiPrimitives";

export function renderInspectModal({
  open = false,
  loading = false,
  error = "",
  heading = "Item Inspector",
  subheading = "",
  bodyMarkup = ""
} = {}) {
  if (!open) return "";

  const statusMarkup = loading
    ? `
      <div class="inspect-modal-status">
        <span class="spinner" aria-hidden="true"></span>
        <p>Loading inspect data...</p>
      </div>
    `
    : error
      ? `<div class="error inspect-modal-error" role="alert">${error}</div>`
      : bodyMarkup;

  return `
    <div class="inspect-modal-overlay" data-inspect-modal-overlay>
      <section
        class="inspect-modal-dialog"
        role="dialog"
        aria-modal="true"
        aria-label="Inspect item details"
      >
        <header class="inspect-modal-header">
          <div>
            <p class="inspect-modal-title">${heading}</p>
            ${subheading ? `<p class="inspect-modal-subtitle">${subheading}</p>` : ""}
          </div>
          ${renderButton({
            label: "Close",
            className: "ghost-btn inspect-modal-close-btn",
            extraAttrs: 'data-inspect-modal-close="1"'
          })}
        </header>
        <div class="inspect-modal-content">
          ${statusMarkup}
        </div>
      </section>
    </div>
  `;
}

