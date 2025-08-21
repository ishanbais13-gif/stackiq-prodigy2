# data_fetcher.py — Finnhub callers + simple cache
# Safe to paste over your entire file.

import os
import time
import requests
from typing import Any, Dict, Tuple

FINNHUB_API_KEY = os.environ.get("FINNHUB_API_KEY", "")
FINNHUB_BASE = "https://finnhub.io/api/v1"

# 30s cache (protect free-tier rate limits)
_TTL_SECONDS = 30
_cache: Dict[Tuple[str, str], Tuple[float, Any]] = {}

class FetchError(Exception):
    def __init__(self, public_message: str, status: str = "upstream_error", http_code: int = 502):
        super().__init__(public_message)
        self.public_message = public_message
        self.status = status
        self.http_code = http_code

def _cache_get(key):
    now = time.time()
    if key in _cache:
        ts, val = _cache[key]
        if now - ts <= _TTL_SECONDS:
            return val
        else:
            _cache.pop(key, None)
    return None

def _cache_set(key, val):
    _cache[key] = (time.time(), val)
    return val

def _require_key():
    if not FINNHUB_API_KEY:
        raise FetchError("API key missing. Set FINNHUB_API_KEY in Azure App Settings.", "missing_key", 500)

def _get(path: str, params: Dict[str, Any]) -> Any:
    _require_key()
    url = f"{FINNHUB_BASE}{path}"
    params = dict(params or {})
    params["token"] = FINNHUB_API_KEY

    key = ("GET", url + "?" + "&".join(f"{k}={v}" for k, v in sorted(params.items())))
    cached = _cache_get(key)
    if cached is not None:
        return cached

    try:
        r = requests.get(url, params=params, timeout=10)
        if r.status_code != 200:
            # Finnhub often returns 429 on rate limit, or 4xx for symbol errors
            raise FetchError(f"Upstream {r.status_code}", "upstream_error", r.status_code)
        data = r.json()
        return _cache_set(key, data)
    except requests.Timeout:
        raise FetchError("Upstream timeout", "timeout", 504)
    except requests.RequestException:
        raise FetchError("Network error contacting data provider", "network_error", 502)

def get_quote(ticker: str) -> Dict[str, Any]:
    """
    Returns Finnhub /quote fields:
      c: current, d: change, dp: change%, h: high, l: low, o: open, pc: prev close, t: timestamp
    """
    if not ticker:
        raise FetchError("Ticker required", "bad_request", 400)

    data = _get("/quote", {"symbol": ticker})
    # Finnhub sends 0s or {} for bad symbols — guard it
    if not isinstance(data, dict) or data.get("c") in (None, 0) and data.get("t") in (None, 0):
        raise FetchError("Ticker not found or no data", "not_found", 404)
    return {
        "c": data.get("c"),
        "d": data.get("d"),
        "dp": data.get("dp"),
        "h": data.get("h"),
        "l": data.get("l"),
        "o": data.get("o"),
        "pc": data.get("pc"),
        "t": data.get("t"),
    }

def get_next_earnings(ticker: str) -> Dict[str, Any]:
    """
    Returns the next earnings row from Finnhub earnings calendar.
    """
    if not ticker:
        raise FetchError("Ticker required", "bad_request", 400)

    data = _get("/calendar/earnings", {"symbol": ticker})
    cal = (data or {}).get("earningsCalendar") or []
    next_row = cal[0] if cal else None
    return {"earningsCalendar": [next_row] if next_row else []}
