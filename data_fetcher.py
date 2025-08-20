# data_fetcher.py — StackIQ data layer (Finnhub)
# Safe to paste over your entire file.

import os
import time
import requests

FINNHUB_API_KEY = os.environ.get("FINNHUB_API_KEY")
FINNHUB_BASE = "https://finnhub.io/api/v1"

# Simple in-memory cache: key -> (expires_at_epoch_seconds, data)
_cache = {}

def _get_cached(key: str):
    now = time.time()
    hit = _cache.get(key)
    if hit and hit[0] > now:
        return hit[1]

def _set_cached(key: str, data, ttl: int):
    _cache[key] = (time.time() + ttl, data)

def _need_key():
    return {"error": "FINNHUB_API_KEY not set in server env"}

def _safe_get(url: str, params: dict, timeout: int = 15):
    """GET with timeout + status raise; returns parsed JSON."""
    r = requests.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    # Finnhub returns JSON
    return r.json()

# ─── Public API ────────────────────────────────────────────────────────────────

def get_stock_price(ticker: str):
    """
    Returns Finnhub /quote payload for a ticker:
    {
      "c": current, "h": high, "l": low, "o": open, "pc": prevClose, "t": epoch
    }
    Cached 30s to reduce rate usage and improve UI snappiness.
    """
    if not FINNHUB_API_KEY:
        return _need_key()

    t = (ticker or "").upper().strip()
    key = f"quote:{t}"
    cached = _get_cached(key)
    if cached:
        return cached

    url = f"{FINNHUB_BASE}/quote"
    params = {"symbol": t, "token": FINNHUB_API_KEY}
    try:
        data = _safe_get(url, params, timeout=12)
    except requests.HTTPError as e:
        # Return a compact error object (surface status for debugging)
        return {"error": "http_error", "status": getattr(e.response, "status_code", None)}
    except requests.RequestException:
        return {"error": "network_error"}

    _set_cached(key, data, ttl=30)  # 30 seconds
    return data


def get_next_earnings(ticker: str):
    """
    Returns Finnhub /calendar/earnings payload for the symbol.
    Shape (example):
      {
        "earningsCalendar": [
          {"symbol":"AAPL","date":"2025-10-28", ...},
          ...
        ]
      }
    Cached 1 hour — earnings don’t change minute-to-minute.
    """
    if not FINNHUB_API_KEY:
        return _need_key()

    t = (ticker or "").upper().strip()
    key = f"earnings:{t}"
    cached = _get_cached(key)
    if cached:
        return cached

    url = f"{FINNHUB_BASE}/calendar/earnings"
    params = {"symbol": t, "token": FINNHUB_API_KEY}
    try:
        data = _safe_get(url, params, timeout=12)
    except requests.HTTPError as e:
        return {"error": "http_error", "status": getattr(e.response, "status_code", None)}
    except requests.RequestException:
        return {"error": "network_error"}

    _set_cached(key, data, ttl=3600)  # 1 hour
    return data

