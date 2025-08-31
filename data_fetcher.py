# data_fetcher.py
import os
import requests
from typing import Optional, Dict, Any

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
FINNHUB_BASE_URL = "https://finnhub.io/api/v1"


def _pct_change(current: Optional[float], prev_close: Optional[float]) -> Optional[float]:
    if current is None or prev_close in (None, 0):
        return None
    try:
        return round(((current - prev_close) / prev_close) * 100, 3)
    except Exception:
        return None


def fetch_quote(symbol: str) -> Optional[Dict[str, Any]]:
    """
    Returns a normalized quote dict or None if the symbol isn't found / request fails.
    Uses Finnhub /quote with FINNHUB_API_KEY.
    """
    if not symbol:
        return None

    if not FINNHUB_API_KEY:
        # No API key available — fail fast so the API can surface a clear error upstream if needed
        return None

    url = f"{FINNHUB_BASE_URL}/quote"
    params = {"symbol": symbol.upper(), "token": FINNHUB_API_KEY}

    try:
        resp = requests.get(url, params=params, timeout=10)
    except requests.RequestException:
        return None

    if resp.status_code != 200:
        return None

    data = resp.json() or {}

    # Finnhub returns 0/None for missing values; also 'c','h','l','o','pc' are standard keys
    current = data.get("c")
    high = data.get("h")
    low = data.get("l")
    open_ = data.get("o")
    prev_close = data.get("pc")

    # Some symbols return zeros when closed/invalid; treat all-zero as not found
    if all(v in (0, None) for v in [current, high, low, open_, prev_close]):
        return None

    return {
        "symbol": symbol.upper(),
        "current": current,
        "prev_close": prev_close,
        "high": high,
        "low": low,
        "open": open_,
        "percent_change": _pct_change(current, prev_close),
        # raw payload if you want to inspect it on the UI
        "raw": {"c": current, "pc": prev_close, "h": high, "l": low, "o": open_},
    }



























