from __future__ import annotations
from typing import Any, Dict, List
import os
import time
import requests

# =========================
# Errors
# =========================
class FinnhubError(Exception):
    """Generic data fetch error (name kept to avoid changing app.py)."""
    pass


# =========================
# Config / Globals
# =========================
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")

# tiny in-memory cache: key -> {"data": Any, "exp": unix_ts}
_cache: Dict[str, Dict[str, Any]] = {}
# simple per-symbol throttle (avoid hammering providers)
_last_call: Dict[str, float] = {}

# good UA helps avoid some 403/429 from Yahoo
UA_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124 Safari/537.36"
    )
}


# =========================
# Helpers
# =========================
def _get_cached(key: str) -> Any | None:
    item = _cache.get(key)
    if not item:
        return None
    if time.time() >= item["exp"]:
        _cache.pop(key, None)
        return None
    return item["data"]

def _set_cached(key: str, data: Any, ttl_seconds: int):
    _cache[key] = {"data": data, "exp": time.time() + ttl_seconds}

def _get_with_retries(url: str, *, headers=None, tries=3, backoff=0.6) -> requests.Response:
    """
    GET with small exponential backoff on 429/5xx and network errors.
    """
    delay = 0.0
    last_exc: Exception | None = None
    for _ in range(tries):
        if delay:
            time.sleep(delay)
        try:
            r = requests.get(url, timeout=10, headers=headers or UA_HEADERS)
            # retry on 429 or any 5xx
            if r.status_code == 429 or 500 <= r.status_code < 600:
                last_exc = requests.HTTPError(f"{r.status_code} {r.reason}")
                delay = backoff if delay == 0 else delay * 2
                continue
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            last_exc = e
            delay = backoff if delay == 0 else delay * 2
            continue
    raise last_exc or FinnhubError("Network error")


# =========================
# Quote Providers
# =========================
def _quote_from_finnhub(symbol: str) -> Dict[str, Any]:
    if not FINNHUB_API_KEY:
        raise FinnhubError("No FINNHUB_API_KEY")
    url = f"https://finnhub.io/api/v1/quote?symbol={symbol}&token={FINNHUB_API_KEY}"
    r = _get_with_retries(url, headers=None, tries=3)
    try:
        j = r.json()
    except Exception as e:
        raise FinnhubError(f"Finnhub JSON error: {e}, text={r.text[:200]}")
    if not j or "c" not in j:
        raise FinnhubError(f"Bad response from Finnhub: {j}")
    return {
        "currentPrice": j.get("c"),
        "previousClose": j.get("pc"),
        "open": j.get("o"),
        "dayHigh": j.get("h"),
        "dayLow": j.get("l"),
        "volume": j.get("v"),
        "source": "finnhub",
    }

def _quote_from_yahoo(symbol: str) -> Dict[str, Any]:
    urls = [
        f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={symbol}",
        f"https://query2.finance.yahoo.com/v6/finance/quote?symbols={symbol}",
    ]
    for url in urls:
        r = _get_with_retries(url, headers=UA_HEADERS, tries=3)
        try:
            d = r.json()
        except Exception:
            # try the other endpoint
            continue
        res = d.get("quoteResponse", {}).get("result", [])
        if not res:
            continue
        q = res[0]
        return {
            "currentPrice": q.get("regularMarketPrice"),
            "previousClose": q.get("regularMarketPreviousClose"),
            "open": q.get("regularMarketOpen"),
            "dayHigh": q.get("regularMarketDayHigh"),
            "dayLow": q.get("regularMarketDayLow"),
            "volume": q.get("regularMarketVolume"),
            "currency": q.get("currency"),
            "shortName": q.get("shortName"),
            "exchange": q.get("fullExchangeName"),
            "marketState": q.get("marketState"),
            "source": "yahoo",
        }
    raise FinnhubError("Yahoo quote unavailable or rate-limited")


# =========================
# Public API (used by app.py)
# =========================
def fetch_quote(symbol: str) -> Dict[str, Any]:
    """
    Get a quote with caching (60s) + throttle (10s per symbol).
    Prefer Finnhub (if key available), fallback to Yahoo.
    """
    symbol = symbol.upper().strip()
    if not symbol:
        raise FinnhubError("Empty symbol")

    cache_key = f"quote:{symbol}"
    cached = _get_cached(cache_key)
    if cached is not None:
        return cached

    # throttle repeated hits per symbol
    now = time.time()
    last = _last_call.get(symbol, 0.0)
    if now - last < 10 and cached is not None:
        return cached
    _last_call[symbol] = now

    try:
        # Primary path
        if FINNHUB_API_KEY:
            out = _quote_from_finnhub(symbol)
        else:
            out = _quote_from_yahoo(symbol)
        _set_cached(cache_key, out, ttl_seconds=60)
        return out
    except Exception:
        # Secondary path (fallback)
        try:
            out = _quote_from_yahoo(symbol) if FINNHUB_API_KEY else _quote_from_finnhub(symbol)
            _set_cached(cache_key, out, ttl_seconds=60)
            return out
        except Exception as e2:
            raise FinnhubError(f"Unable to fetch quote for {symbol}: {e2}")

def fetch_earnings(symbol: str) -> List[Dict[str, Any]]:
    """
    Recent quarterly earnings from Yahoo (cached 10 minutes).
    """
    symbol = symbol.upper().strip()
    if not symbol:
        raise FinnhubError("Empty symbol")

    cache_key = f"earnings:{symbol}"
    cached = _get_cached(cache_key)
    if cached is not None:
        return cached

    url = f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{symbol}?modules=earnings"
    r = _get_with_retries(url, headers=UA_HEADERS, tries=3)
    d = r.json()
    earnings = (
        d.get("quoteSummary", {})
         .get("result", [{}])[0]
         .get("earnings", {})
         .get("financialsChart", {})
         .get("quarterly", [])
    )

    rows: List[Dict[str, Any]] = []
    for row in earnings:
        def raw(v): return v.get("raw") if isinstance(v, dict) else v
        rows.append({
            "date": row.get("date"),
            "epsEstimate": raw(row.get("estimate")),
            "epsActual": raw(row.get("actual")),
            "surprisePercent": raw(row.get("surprisePercent")),
        })

    _set_cached(cache_key, rows, ttl_seconds=600)  # 10 minutes
    return rows

def get_quote_and_earnings(symbol: str) -> Dict[str, Any]:
    return {
        "quote": fetch_quote(symbol),
        "earnings": fetch_earnings(symbol),
    }

    }






