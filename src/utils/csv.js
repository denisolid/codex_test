function escapeCsvValue(value) {
  if (value == null) return "";
  const text = String(value);
  if (/[",\n\r]/.test(text)) {
    return `"${text.replace(/"/g, '""')}"`;
  }
  return text;
}

exports.toCsv = (headers = [], rows = []) => {
  const safeHeaders = Array.isArray(headers) ? headers : [];
  const safeRows = Array.isArray(rows) ? rows : [];
  const lines = [];

  lines.push(safeHeaders.map((header) => escapeCsvValue(header)).join(","));

  for (const row of safeRows) {
    const cells = safeHeaders.map((key) => escapeCsvValue(row?.[key]));
    lines.push(cells.join(","));
  }

  return `${lines.join("\n")}\n`;
};
