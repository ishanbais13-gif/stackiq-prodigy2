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

def _get(path: str, params: dict):
    _check_key()
    p = dict(params or {})
    p["token"] = API_KEY
    try:
        r = requests.get(f"{API_BASE}{path}", params=p, timeout=10)
    except requests.RequestException as e:
        raise FinnhubError("Network error to Finnhub") from e

    if r.status_code == 429:
        raise FinnhubError("Finnhub rate limit (429)")
    if not r.ok:
        # short slice of body for debugging
        msg = r.text[:200] if r.text else r.reason
        raise FinnhubError(f"Finnhub HTTP {r.status_code}: {msg}")
    return r.json()

# --- micro cache (keeps us from hammering the API while testing)
_CACHE = {}  # key -> (expires_at, data)

def _cache_get(key):
    item = _CACHE.get(key)
    if not item:
        return None
    exp, data = item
    if time.time() > exp:
        _CACHE.pop(key, None)
        return None
    return data

def _cache_set(key, data, ttl=15):
    _CACHE[key] = (time.time() + ttl, data)

# --- public fetchers

def fetch_quote(symbol: str) -> dict:
    """
    Normalized quote for a symbol.
    Returns:
    {
      "symbol": "AAPL",
      "current": 226.71,
      "prev_close": 227.16,
      "high": 227.30,
      "low": 224.69,
      "open": 226.48,
      "percent_change": -0.20,
      "volume": null,
      "raw": {... original Finnhub payload ...}
    }
    """
    s = symbol.upper().strip()
    key = ("quote", s)
    hit = _cache_get(key)
    if hit:
        return hit

    data = _get("/quote", {"symbol": s})
    # Finnhub /quote fields: c(current), d(change), dp(percent), h, l, o, pc(prev close), t
    payload = {
        "symbol": s,
        "current": data.get("c"),
        "prev_close": data.get("pc"),
        "high": data.get("h"),
        "low": data.get("l"),
        "open": data.get("o"),
        "percent_change": data.get("dp"),
        "volume": None,   # Finnhub /quote doesn’t include volume
        "raw": data,
    }
    _cache_set(key, payload, ttl=10)
    return payload











