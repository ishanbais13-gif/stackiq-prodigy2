from __future__ import annotations
from typing import Any, Dict, List
import time
import requests

class FinnhubError(Exception):
    pass

# ------------ Simple in-memory cache ------------
_cache: Dict[str, Dict[str, Any]] = {}         # key -> {data, exp}
_last_call: Dict[str, float] = {}              # per-symbol throttle

def _get_cached(key: str) -> Any | None:
    item = _cache.get(key)
    if not item:
        return None
    if time.time() >= item["exp"]:
        _cache.pop(key, None)
        return None
    return item["data"]

def _set_cached(key: str, data: Any, ttl: int):
    _cache[key] = {"data": data, "exp": time.time() + ttl}

HEADERS = {
    # helps avoid some 403/429 responses
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"
}

def _get_with_retries(url: str, *, tries=3, backoff=0.6) -> requests.Response:
    """
    GET with small exponential backoff on 429/5xx.
    """
    delay = 0.0
    last_exc: Exception | None = None
    for i in range(tries):
        if delay:
            time.sleep(delay)
        try:
            r = requests.get(url, timeout=10, headers=HEADERS)
            # Retry on 429 or 5xx
            if r.status_code == 429 or 500 <= r.status_code < 600:
                delay = backoff if delay == 0 else delay * 2
                last_exc = requests.HTTPError(f"{r.status_code} {r.reason}")
                continue
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            # network hiccup: retry
            delay = backoff if delay == 0 else delay * 2
            last_exc = e
            continue
    # all tries failed
    if last_exc:
        raise last_exc
    raise FinnhubError("Unknown network error")

# ------------ Quote ------------
def fetch_quote(symbol: str) -> Dict[str, Any]:
    """
    Yahoo quote with cache (60s) + throttle (one call per 10s per symbol).
    Retries on 429/5xx a few times before failing.
    """
    symbol = symbol.upper().strip()
    if not symbol:
        raise FinnhubError("Empty symbol")

    cache_key = f"quote:{symbol}"
    # serve cache if fresh
    cached = _get_cached(cache_key)
    if cached is not None:
        return cached

    # simple throttle: if we queried this symbol <10s ago, wait or serve stale (if any)
    now = time.time()
    last = _last_call.get(symbol, 0)
    if now - last < 10 and cached is not None:
        return cached
    _last_call[symbol] = now

    # try query1, then query2 as fallback
    urls = [
        f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={symbol}",
        f"https://query2.finance.yahoo.com/v6/finance/quote?symbols={symbol}",
    ]

    for url in urls:
        try:
            r = _get_with_retries(url)
            data = r.json()
            results = data.get("quoteResponse", {}).get("result", [])
            if not results:
                continue
            q = results[0]
            out = {
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
            }
            _set_cached(cache_key, out, ttl=60)
            return out
        except Exception:
            # try next URL
            continue

    raise FinnhubError(f"Unable to fetch quote for {symbol} (rate-limited or unavailable)")

# ------------ Earnings ------------
def fetch_earnings(symbol: str) -> List[Dict[str, Any]]:
    """
    Yahoo earnings summary with 10-minute cache.
    """
    symbol = symbol.upper().strip()
    if not symbol:
        raise FinnhubError("Empty symbol")

    cache_key = f"earnings:{symbol}"
    cached = _get_cached(cache_key)
    if cached is not None:
        return cached

    url = f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{symbol}?modules=earnings"
    try:
        r = _get_with_retries(url)
        data = r.json()
        earnings = (
            data.get("quoteSummary", {})
                .get("result", [{}])[0]
                .get("earnings", {})
                .get("financialsChart", {})
                .get("quarterly", [])
        )
        rows: List[Dict[str, Any]] = []
        for row in earnings:
            def _raw(v):
                return v.get("raw") if isinstance(v, dict) else v
            rows.append({
                "date": row.get("date"),
                "epsEstimate": _raw(row.get("estimate")),
                "epsActual": _raw(row.get("actual")),
                "surprisePercent": _raw(row.get("surprisePercent")),
            })
        _set_cached(cache_key, rows, ttl=600)
        return rows
    except Exception as e:
        raise FinnhubError(f"Unable to fetch earnings for {symbol}: {e}")

# ------------ Combined ------------
def get_quote_and_earnings(symbol: str) -> Dict[str, Any]:
    return {
        "quote": fetch_quote(symbol),
        "earnings": fetch_earnings(symbol),
    }






