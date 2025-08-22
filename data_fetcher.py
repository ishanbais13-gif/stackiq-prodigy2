# data_fetcher.py — Finnhub helpers used by /test/<ticker>
import os
import time
import json
import urllib.parse
from typing import Dict, Any, List
import requests

class FetchError(Exception):
    pass

FINNHUB_API_KEY = os.environ.get("FINNHUB_API_KEY")
BASE = "https://finnhub.io/api/v1"

def _require_key():
    if not FINNHUB_API_KEY:
        raise FetchError("FINNHUB_API_KEY not set in environment")

def _get(url: str, params: Dict[str, Any]) -> Dict[str, Any]:
    _require_key()
    # add token, basic retry
    params = dict(params or {})
    params["token"] = FINNHUB_API_KEY

    for attempt in range(3):
        r = requests.get(url, params=params, timeout=10)
        if r.status_code == 200:
            try:
                return r.json()
            except Exception:
                raise FetchError("Malformed JSON from provider")
        if r.status_code == 429:
            # rate-limited — small backoff, then retry
            time.sleep(0.8 * (attempt + 1))
            continue
        # other status -> raise with body snippet
        raise FetchError(f"HTTP {r.status_code}: {r.text[:180]}")
    raise FetchError("Too many retries")

def get_ticker_data(symbol: str) -> Dict[str, Any]:
    """
    Returns Finnhub quote payload:
      { c, d, dp, h, l, o, pc }
    """
    if not symbol:
        raise FetchError("Missing symbol")
    url = f"{BASE}/quote"
    data = _get(url, {"symbol": symbol})
    # Validate presence of 'c' etc.
    if "c" not in data:
        raise FetchError("Quote not found or no data")
    return data

def get_earnings_calendar(symbol: str) -> List[Dict[str, Any]]:
    """
    Returns a (possibly empty) list of the symbol's next/last earnings items.
    Finnhub: /calendar/earnings?symbol=SYMB
    """
    url = f"{BASE}/calendar/earnings"
    data = _get(url, {"symbol": symbol})
    items = data.get("earningsCalendar") or data.get("earningscalendar") or data.get("earnings") or []
    # Normalize to a list of simple dicts
    if isinstance(items, dict):
        items = [items]
    return items

def get_price_and_earnings(symbol: str) -> Dict[str, Any]:
    """
    Unified call used by /test/<ticker>.
    """
    quote = get_ticker_data(symbol)
    earnings = get_earnings_calendar(symbol)
    return {
        "ticker": symbol,
        "price": {
            "c": quote.get("c"),
            "d": quote.get("d"),
            "dp": quote.get("dp"),
            "h": quote.get("h"),
            "l": quote.get("l"),
            "o": quote.get("o"),
            "pc": quote.get("pc"),
        },
        "earnings": {
            "earningsCalendar": earnings
        },
    }

# Backwards-compatible aliases the app might look for
get_stock_data = get_ticker_data
get_quote_and_earnings = get_price_and_earnings
get_ticker = get_ticker_data
get = get_ticker_data
fetch = get_price_and_earnings

