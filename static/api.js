import { cachedFetch } from "./cache.js";

const API_BASE = ""; // same origin

export async function getQuote(symbol) {
  const url = `${API_BASE}/quote/${encodeURIComponent(symbol)}`;
  return cachedFetch(url, { ttlSeconds: 90 });
}

export async function getPlan(symbol, budget) {
  const url = `${API_BASE}/predict/${encodeURIComponent(symbol)}?budget=${encodeURIComponent(budget)}`;
  return cachedFetch(url, { ttlSeconds: 90 });
}
