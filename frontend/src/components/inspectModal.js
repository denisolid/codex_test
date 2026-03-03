import { renderModal } from "./dashboardPrimitives";

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
      <div class="inspect-modal-skeleton" aria-hidden="true">
        <div class="inspect-modal-skeleton-media"></div>
        <div class="inspect-modal-skeleton-lines">
          <span></span>
          <span></span>
          <span></span>
        </div>
      </div>
    `
    : error
      ? `<div class="error inspect-modal-error" role="alert">${error}</div>`
      : bodyMarkup;

  return renderModal({
    open,
    title: heading,
    subtitle: subheading,
    bodyMarkup: statusMarkup,
    closeAttr: 'data-inspect-modal-close="1"',
    label: "Inspect item details",
    rootClassName: "inspect-modal-overlay",
    overlayAttr: 'data-inspect-modal-overlay="1"',
    panelAttr: 'data-inspect-modal-dialog="1"'
  });
}
