from typing import Any, Dict, List, Optional
import os
import time
import requests

# =========================
# Errors
# =========================
class FinnhubError(Exception):
    """Generic data fetch error (name kept to match app.py)."""
    pass


# =========================
# Config / Globals
# =========================
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")

# tiny in-memory cache: key -> {"data": Any, "exp": unix_ts}
_cache: Dict[str, Dict[str, Any]] = {}
# simple per-symbol throttle
_last_call: Dict[str, float] = {}

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
def _get_cached(key: str) -> Optional[Any]:
    item = _cache.get(key)
    if not item:
        return None
    if time.time() >= item.get("exp", 0):
        _cache.pop(key, None)
        return None
    return item.get("data")

def _set_cached(key: str, data: Any, ttl_seconds: int) -> None:
    _cache[key] = {"data": data, "exp": time.time() + ttl_seconds}

def _get_with_retries(url: str, headers: Optional[Dict[str, str]] = None,
                      tries: int = 3, backoff: float = 0.6) -> requests.Response:
    """
    GET with small exponential backoff on 429/5xx and network errors.
    """
    delay = 0.0
    last_exc: Optional[Exception] = None
    for _ in range(tries):
        if delay:
            time.sleep(delay)
        try:
            r = requests.get(url, timeout=10, headers=headers or UA_HEADERS)
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
    url = "https://finnhub.io/api/v1/quote?symbol={s}&token={t}".format(
        s=symbol, t=FINNHUB_API_KEY
    )
    r = _get_with_retries(url, headers=None, tries=3)
    try:
        j = r.json()
    except Exception as e:
        raise FinnhubError("Finnhub JSON error: {e}".format(e=e))
    if not j or "c" not in j:
        raise FinnhubError("Bad response from Finnhub: {j}".format(j=j))
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
        "https://query1.finance.yahoo.com/v7/finance/quote?symbols={s}".format(s=symbol),
        "https://query2.finance.yahoo.com/v6/finance/quote?symbols={s}".format(s=symbol),
    ]
    last_error: Optional[str] = None
    for url in urls:
        try:
            r = _get_with_retries(url, headers=UA_HEADERS, tries=3)
            d = r.json()
            res = d.get("quoteResponse", {}).get("result", [])
            if not res:
                last_error = "empty result"
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
        except Exception as e:
            last_error = str(e)
            continue
    raise FinnhubError("Yahoo quote unavailable or rate-limited ({e})".format(e=last_error))


# =========================
# Public API used by app.py
# =========================
def fetch_quote(symbol: str) -> Dict[str, Any]:
    """
    Get a quote with caching (60s) + throttle (10s per symbol).
    Prefer Finnhub (if key available), fallback to Yahoo.
    """
    symbol = symbol.upper().strip()
    if not symbol:
        raise FinnhubError("Empty symbol")

    cache_key = "quote:{s}".format(s=symbol)
    cached = _get_cached(cache_key)
    if cached is not None:
        return cached

    # throttle repeated hits per symbol
    now = time.time()
    last = _last_call.get(symbol, 0.0)
    if now - last < 10 and cached is not None:
        return cached
    _last_call[symbol] = now

    # primary path
    try:
        if FINNHUB_API_KEY:
            out = _quote_from_finnhub(symbol)
        else:
            out = _quote_from_yahoo(symbol)
        _set_cached(cache_key, out, ttl_seconds=60)
        return out
    except Exception:
        # fallback path
        try:
            out = _quote_from_yahoo(symbol) if FINNHUB_API_KEY else _quote_from_finnhub(symbol)
            _set_cached(cache_key, out, ttl_seconds=60)
            return out
        except Exception as e2:
            # final guarded error to avoid app crash pages
            raise FinnhubError("Unable to fetch quote for {s}: {e}".format(s=symbol, e=e2))

def fetch_earnings(symbol: str) -> List[Dict[str, Any]]:
    """
    Recent quarterly earnings from Yahoo (cached 10 minutes).
    """
    symbol = symbol.upper().strip()
    if not symbol:
        raise FinnhubError("Empty symbol")

    cache_key = "earnings:{s}".format(s=symbol)
    cached = _get_cached(cache_key)
    if cached is not None:
        return cached

    url = "https://query2.finance.yahoo.com/v10/finance/quoteSummary/{s}?modules=earnings".format(s=symbol)
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
        def raw(v):
            return v.get("raw") if isinstance(v, dict) else v
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







