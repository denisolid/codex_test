function safe(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

export function renderButton({
  label = "",
  type = "button",
  id = "",
  className = "",
  extraAttrs = ""
} = {}) {
  const idAttr = id ? ` id="${safe(id)}"` : "";
  const classAttr = className ? ` class="${safe(className)}"` : "";
  const attrs = extraAttrs ? ` ${extraAttrs}` : "";
  return `<button type="${safe(type)}"${idAttr}${classAttr}${attrs}>${safe(label)}</button>`;
}

export function renderSection({
  eyebrow = "",
  title = "",
  description = "",
  body = "",
  className = ""
} = {}) {
  const classes = `ui-section${className ? ` ${className}` : ""}`;
  const eyebrowMarkup = eyebrow ? `<p class="ui-section-eyebrow">${safe(eyebrow)}</p>` : "";
  const titleMarkup = title ? `<h3 class="ui-section-title">${safe(title)}</h3>` : "";
  const descriptionMarkup = description
    ? `<p class="ui-section-description">${safe(description)}</p>`
    : "";

  return `
    <section class="${classes}">
      <header class="ui-section-head">
        ${eyebrowMarkup}
        ${titleMarkup}
        ${descriptionMarkup}
      </header>
      <div class="ui-section-body">
        ${body}
      </div>
    </section>
  `;
}

