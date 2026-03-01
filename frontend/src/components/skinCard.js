import {
  defaultCaseImage,
  defaultSkinImage,
  getRarityTheme,
  isCaseLikeItem,
  resolveItemImageUrl
} from "../rarity";

const fallbackEscape = (value) =>
  String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");

function formatSourceLabel(value) {
  const raw = String(value || "").trim().toLowerCase();
  if (!raw) return "N/A";
  if (raw === "csfloat") return "CSFloat";
  if (raw === "dmarket") return "DMarket";
  if (raw === "skinport") return "Skinport";
  if (raw === "steam") return "Steam";
  return raw.charAt(0).toUpperCase() + raw.slice(1);
}

export function renderSkinCard(item = {}, helpers = {}) {
  const escapeHtml = helpers.escapeHtml || fallbackEscape;
  const formatMoney = helpers.formatMoney || ((value) => String(value ?? "0"));
  const { rarity, color } = getRarityTheme(item);
  const imageUrl = resolveItemImageUrl(item);
  const fallbackImage = isCaseLikeItem(item) ? defaultCaseImage : defaultSkinImage;
  const unitPriceMarkup =
    item.currentPrice == null
      ? "-"
      : `${formatMoney(item.currentPrice, item.currency)} · ${formatSourceLabel(
          item.selectedPricingSource || item.currentPriceSource
        )}`;

  return `
    <article class="portfolio-skin-card" style="--rarity-color: ${color};">
      <div class="portfolio-skin-card-media">
        <div class="portfolio-skin-card-hex" aria-hidden="true"></div>
        <img
          src="${escapeHtml(imageUrl)}"
          alt="${escapeHtml(item.marketHashName || "CS2 item")}" 
          loading="lazy"
          onerror="this.onerror=null;this.src='${escapeHtml(fallbackImage)}';"
        />
      </div>
      <div class="portfolio-skin-card-body">
        <p class="portfolio-skin-card-name" title="${escapeHtml(item.marketHashName || "-")}">
          ${escapeHtml(item.marketHashName || "Unknown item")}
        </p>
        <p class="portfolio-skin-card-price">${escapeHtml(unitPriceMarkup)}</p>
        <div class="portfolio-skin-card-meta">
          <span class="rarity-tag" style="--rarity-color: ${color};">${escapeHtml(rarity)}</span>
          <span class="portfolio-skin-card-value">${formatMoney(item.lineValue, item.currency)}</span>
        </div>
      </div>
    </article>
  `;
}

export function renderSkinCardSkeleton(index = 0) {
  return `
    <article class="portfolio-skin-card is-skeleton" data-skeleton-index="${Number(index) || 0}">
      <div class="portfolio-skin-card-media">
        <div class="portfolio-skin-card-hex" aria-hidden="true"></div>
      </div>
      <div class="portfolio-skin-card-body">
        <div class="skeleton-line w-90"></div>
        <div class="portfolio-skin-card-meta">
          <div class="skeleton-line w-40"></div>
          <div class="skeleton-line w-30"></div>
        </div>
      </div>
    </article>
  `;
}
