import os
import time
import requests

class FinnhubError(Exception):
    pass

API_BASE = "https://finnhub.io/api/v1"
API_KEY = os.getenv("FINNHUB_API_KEY", "")

def _check_key():
    if not API_KEY:
        raise FinnhubError("FINNHUB_API_KEY is not set")

def _get(url, params=None):
    _check_key()
    params = dict(params or {})
    params["token"] = API_KEY
    r = requests.get(url, params=params, timeout=10)
    if r.status_code == 429:
        raise FinnhubError("rate")
    if not r.ok:
        raise FinnhubError(f"HTTP {r.status_code}: {r.text[:200]}")
    return r.json()

# --- tiny in-memory cache (key -> (exp_ts, data)) ---
_CACHE = {}

def cache_get(key):
    item = _CACHE.get(key)
    if not item: return None
    exp, data = item
    if time.time() > exp:
        _CACHE.pop(key, None)
        return None
    return data

def cache_set(key, data, ttl=30):
    _CACHE[key] = (time.time() + ttl, data)

# ---------- Quotes ----------
def fetch_quote(symbol: str):
    symbol = symbol.upper().strip()
    key = ("quote", symbol)
    hit = cache_get(key)
    if hit: return hit

    data = _get(f"{API_BASE}/quote", {"symbol": symbol})
    payload = {
        "symbol": symbol,
        "current": data.get("c"),
        "high": data.get("h"),
        "low": data.get("l"),
        "open": data.get("o"),
        "prev_close": data.get("pc"),
        "percent_change": round(((data.get("c", 0) - data.get("pc", 0)) / data.get("pc", 1)) * 100, 3) if data.get("pc") else None,
        "raw": data,
    }
    cache_set(key, payload, ttl=15)
    return payload

# ---------- Earnings (summary) ----------
def fetch_earnings(symbol: str, limit: int = 4):
    symbol = symbol.upper().strip()
    key = ("earnings", symbol, limit)
    hit = cache_get(key)
    if hit: return hit

    data = _get(f"{API_BASE}/stock/earnings", {"symbol": symbol})
    items = []
    for row in (data or [])[:limit]:
        items.append({
            "period": f"{row.get('year')}-{str(row.get('quarter')).zfill(2)}",
            "epsActual": row.get("actual"),
            "epsEstimate": row.get("estimate"),
            "surprise": (None if row.get("estimate") in (None, 0) else round((row.get("actual", 0) - row.get("estimate", 0)), 3)),
        })
    payload = {"count": len(items), "items": items, "raw": data}
    cache_set(key, payload, ttl=60)
    return payload

# ---------- Candle history ----------
def fetch_history(symbol: str, range_days: int = 30):
    """
    Returns daily candles for ~last `range_days` trading days.
    """
    symbol = symbol.upper().strip()
    key = ("history", symbol, range_days)
    hit = cache_get(key)
    if hit: return hit

    now = int(time.time())
    frm = now - range_days * 24 * 3600
    data = _get(f"{API_BASE}/stock/candle", {
        "symbol": symbol,
        "resolution": "D",
        "from": frm,
        "to": now
    })
    if data.get("s") != "ok":
        raise FinnhubError("history_unavailable")

    candles = []
    for t, c in zip(data.get("t", []), data.get("c", [])):
        candles.append({"t": t, "c": c})
    payload = {"symbol": symbol, "candles": candles, "raw": data}
    cache_set(key, payload, ttl=60)
    return payload

# ---------- Combined ----------
def get_quote_and_earnings(symbol: str):
    q = fetch_quote(symbol)
    e = fetch_earnings(symbol)
    return {"symbol": symbol.upper(), "quote": q, "earnings": e}










