import os
import time
import requests
from typing import Any, Dict, Optional, Tuple

# Alpaca endpoints
ALPACA_DATA_BASE = os.getenv("ALPACA_DATA_BASE", "https://data.alpaca.markets")
ALPACA_PAPER_BASE = os.getenv("ALPACA_PAPER_BASE", "https://paper-api.alpaca.markets")

class APIError(Exception):
    def __init__(self, message: str, status_code: int = 500, details: Optional[dict] = None):
        super().__init__(message)
        self.status_code = status_code
        self.details = details or {}

def _get_alpaca_headers() -> Dict[str, str]:
    key = os.getenv("ALPACA_API_KEY") or os.getenv("APCA_API_KEY_ID")
    secret = os.getenv("ALPACA_SECRET_KEY") or os.getenv("APCA_API_SECRET_KEY")

    if not key or not secret:
        raise APIError(
            "Missing ALPACA_API_KEY / ALPACA_SECRET_KEY in environment variables",
            status_code=500,
            details={"needed": ["ALPACA_API_KEY", "ALPACA_SECRET_KEY"]}
        )

    return {
        "APCA-API-KEY-ID": key,
        "APCA-API-SECRET-KEY": secret,
        "Accept": "application/json",
    }

def _request(method: str, url: str, params: Optional[Dict[str, Any]] = None, timeout: int = 12) -> Dict[str, Any]:
    headers = _get_alpaca_headers()
    params = params or {}

    try:
        resp = requests.request(method, url, headers=headers, params=params, timeout=timeout)
    except requests.RequestException as e:
        raise APIError("Alpaca request failed (network/timeout)", status_code=502, details={"error": str(e)})

    # Try json
    try:
        data = resp.json()
    except Exception:
        data = {"raw": resp.text[:5000]}

    if resp.status_code >= 400:
        raise APIError(
            "Alpaca returned error",
            status_code=resp.status_code,
            details={"status": resp.status_code, "url": url, "params": params, "body": data}
        )

    return data

# --- tiny in-memory cache (helps avoid rate issues) ---
_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}

def _cache_get(key: str) -> Optional[Dict[str, Any]]:
    item = _cache.get(key)
    if not item:
        return None
    expires_at, value = item
    if time.time() > expires_at:
        _cache.pop(key, None)
        return None
    return value

def _cache_set(key: str, value: Dict[str, Any], ttl_seconds: int) -> None:
    _cache[key] = (time.time() + ttl_seconds, value)

def get_quote(symbol: str, feed: str = "iex") -> Dict[str, Any]:
    """
    Alpaca latest quote:
    GET /v2/stocks/{symbol}/quotes/latest?feed=iex
    """
    symbol = symbol.upper().strip()
    cache_key = f"quote:{symbol}:{feed}"
    cached = _cache_get(cache_key)
    if cached:
        return cached

    url = f"{ALPACA_DATA_BASE}/v2/stocks/{symbol}/quotes/latest"
    data = _request("GET", url, params={"feed": feed}, timeout=10)

    # Alpaca usually returns {"quote": {...}, "symbol":"AAPL"}
    quote = data.get("quote") if isinstance(data, dict) else None
    if not quote:
        raise APIError("Unexpected Alpaca quote response", status_code=502, details={"data": data})

    out = {
        "symbol": data.get("symbol", symbol),
        "bid": quote.get("bp"),
        "ask": quote.get("ap"),
        "bid_size": quote.get("bs"),
        "ask_size": quote.get("as"),
        "timestamp": quote.get("t"),
        "raw": data,
    }

    _cache_set(cache_key, out, ttl_seconds=8)
    return out

def get_bars(
    symbol: str,
    timeframe: str,
    start: str,
    end: str,
    limit: int = 1000,
    feed: str = "iex",
    adjustment: str = "raw",
) -> Dict[str, Any]:
    """
    Alpaca bars:
    GET /v2/stocks/{symbol}/bars?timeframe=1Day&start=...&end=...&limit=1000&feed=iex&adjustment=raw
    start/end should be ISO 8601 with timezone, e.g. 2025-12-01T00:00:00Z
    """
    symbol = symbol.upper().strip()
    cache_key = f"bars:{symbol}:{timeframe}:{start}:{end}:{limit}:{feed}:{adjustment}"
    cached = _cache_get(cache_key)
    if cached:
        return cached

    url = f"{ALPACA_DATA_BASE}/v2/stocks/{symbol}/bars"
    params = {
        "timeframe": timeframe,
        "start": start,
        "end": end,
        "limit": limit,
        "feed": feed,
        "adjustment": adjustment,
    }
    data = _request("GET", url, params=params, timeout=15)

    bars = data.get("bars") if isinstance(data, dict) else None
    if bars is None:
        raise APIError("Unexpected Alpaca bars response", status_code=502, details={"data": data})

    out = {
        "symbol": data.get("symbol", symbol),
        "timeframe": timeframe,
        "start": start,
        "end": end,
        "count": len(bars),
        "bars": bars,      # list of {t,o,h,l,c,v,n,vw}
        "raw": data,
    }

    _cache_set(cache_key, out, ttl_seconds=30)
    return out
























































