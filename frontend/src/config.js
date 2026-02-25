const rawApiUrl =
  import.meta.env.VITE_API_URL ||
  import.meta.env.VITE_API_BASE_URL ||
  "http://localhost:4000/api";

export const API_URL = String(rawApiUrl).replace(/\/+$/, "");
