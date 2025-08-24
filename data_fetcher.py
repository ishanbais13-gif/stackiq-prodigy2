from __future__ import annotations
from typing import Any, Dict, List
import time
import requests

# -------- Errors --------
class FinnhubError(Exception):
    """Generic data fetch error (kept name to avoid touching app.py)."""
    pass


# -------- Simple in-memory cache --------
# cache[key] -> {"data": Any, "exp": unix_timestamp}
_cache: Dict[str, Dict[str, Any]] = {}

def _get_cached(key: str) -> Any | None:
    item = _cache.get(key)
    if not item:
        return None
    if time.time() >= item["exp"]:
        _cache.pop(key, None)
        return None
    return item["data"]

def _set_cached(key: str, data: Any, ttl_seconds: int = 60) -> None:
    _cache[key] = {"data": data, "exp": time.time() + ttl_seconds}


# -------- Quote --------
def fetch_quote(symbol: str) -> Dict[str, Any]:
    """
    Fetch quote using Yahoo Finance (public JSON endpoint).
    Caches results for 60s to avoid rate limits (429).
    """
    symbol = symbol.upper().strip()
    if not symbol:
        raise FinnhubError("Empty symbol")

    cache_key = f"quote:{symbol}"
    cached = _get_cached(cache_key)
    if cached is not None:
        return cached

    url = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={symbol}"
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        results = data.get("quoteResponse", {}).get("result", [])
        if not results:
            raise FinnhubError(f"Ticker {symbol} not found")
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

        _set_cached(cache_key, out, ttl_seconds=60)
        return out

    except requests.HTTPError as e:
        # 429 or others
        status = e.response.status_code if e.response is not None else "HTTP"
        raise FinnhubError(f"Network error for {symbol}: {status} {e}")
    except requests.RequestException as e:
        raise FinnhubError(f"Network error for {symbol}: {e}")
    except ValueError as e:
        raise FinnhubError(f"Bad JSON for {symbol}: {e}")
    except Exception as e:
        raise FinnhubError(f"Error fetching quote for {symbol}: {e}")


# -------- Earnings --------
def fetch_earnings(symbol: str) -> List[Dict[str, Any]]:
    """
    Fetch recent quarterly earnings from Yahoo Finance.
    Caches results for 10 minutes to avoid repeated calls.
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
        r = requests.get(url, timeout=10)
        r.raise_for_status()
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
            def _raw(val):
                # sometimes Yahoo wraps numbers like {"raw": 1.23, "fmt": "1.23"}
                return val.get("raw") if isinstance(val, dict) else val

            rows.append({
                "date": row.get("date"),
                "epsEstimate": _raw(row.get("estimate")),
                "epsActual": _raw(row.get("actual")),
                "surprisePercent": _raw(row.get("surprisePercent")),
            })

        _set_cached(cache_key, rows, ttl_seconds=600)  # 10 min
        return rows

    except requests.HTTPError as e:
        status = e.response.status_code if e.response is not None else "HTTP"
        raise FinnhubError(f"Network error for {symbol}: {status} {e}")
    except requests.RequestException as e:
        raise FinnhubError(f"Network error for {symbol}: {e}")
    except ValueError as e:
        raise FinnhubError(f"Bad JSON for {symbol}: {e}")
    except Exception as e:
        raise FinnhubError(f"Error fetching earnings for {symbol}: {e}")


# -------- Combined --------
def get_quote_and_earnings(symbol: str) -> Dict[str, Any]:
    return {
        "quote": fetch_quote(symbol),
        "earnings": fetch_earnings(symbol),
    }






