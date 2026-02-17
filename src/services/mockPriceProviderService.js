function hashString(value) {
  let hash = 0;
  for (let i = 0; i < value.length; i += 1) {
    hash = (hash << 5) - hash + value.charCodeAt(i);
    hash |= 0;
  }
  return Math.abs(hash);
}

function round2(n) {
  return Number(n.toFixed(2));
}

exports.getLatestPrice = async (marketHashName, date = new Date()) => {
  const baseSeed = hashString(marketHashName);
  const base = 8 + (baseSeed % 180);

  const daySeed = Math.floor(date.getTime() / (1000 * 60 * 60 * 24));
  const wave = Math.sin((daySeed + (baseSeed % 13)) / 4.5) * 0.08;
  const drift = ((daySeed + baseSeed) % 11) / 200;
  const multiplier = 1 + wave + drift;

  return round2(base * multiplier);
};
