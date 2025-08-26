import os
import time
import requests

API_BASE = "https://finnhub.io/api/v1"
API_KEY = os.getenv("FINNHUB_API_KEY", "")

class FinnhubError(Exception):
    pass

def _check_key():
    if not API_KEY:
        raise FinnhubError("FINNHUB_API_KEY is not set")

def _get(path: str, params: dict | None = None):
    _check_key()
    p = dict(params or {})
    p["token"] = API_KEY
    r = requests.get(f"{API_BASE}{path}", params=p, timeout=10)
    # Rate limit -> surface clearly
    if r.status_code == 429:
        raise FinnhubError("Rate limit from Finnhub (429)")
    if not r.ok:
        # keep first 200 chars to avoid huge HTML
        raise FinnhubError(f"HTTP {r.status_code}: {r.text[:200]}")
    return r.json()

# ---- tiny in-memory cache to reduce rate hits ----
_CACHE: dict[tuple, tuple[float, dict]] = {}

def _cache_get(key: tuple):
    item = _CACHE.get(key)
    if not item:
        return None
    exp, data = item
    if time.time() > exp:
        _CACHE.pop(key, None)
        return None
    return data

def _cache_set(key: tuple, data: dict, ttl: int = 15):
    _CACHE[key] = (time.time() + ttl, data)

# ---- public fetchers ----
def fetch_quote(symbol: str) -> dict:
    sym = symbol.upper().strip()
    key = ("quote", sym)
    hit = _cache_get(key)
    if hit:
        return hit

    data = _get("/quote", {"symbol": sym})

    payload = {
        "symbol": sym,
        "current": data.get("c"),
        "high": data.get("h"),
        "low": data.get("l"),
        "open": data.get("o"),
        "prev_close": data.get("pc"),
        "percent_change": (
            None if data.get("pc") in (None, 0)
            else round(((data.get("c", 0) - data.get("pc", 0)) / data.get("pc", 1)) * 100, 3)
        ),
        "raw": data,
    }
    _cache_set(key, payload, ttl=15)
    return payload











