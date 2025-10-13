const _cache = new Map();

export async function cachedFetch(url, { ttlSeconds = 90, ...opts } = {}) {
  const now = Date.now();
  const hit = _cache.get(url);
  if (hit && hit.expiresAt > now) return hit.data;

  const res = await fetch(url, opts);
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`HTTP ${res.status}: ${text}`);
  }
  const data = await res.json();
  _cache.set(url, { expiresAt: now + ttlSeconds * 1000, data });
  return data;
}
