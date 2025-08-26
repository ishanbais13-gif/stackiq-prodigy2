# data_fetcher.py
import os
import requests

API_BASE = "https://finnhub.io/api/v1"
API_KEY = os.getenv("FINNHUB_API_KEY", "")

class FinnhubError(Exception):
    pass

def _check_key():
    if not API_KEY:
        raise FinnhubError("FINNHUB_API_KEY is not set")

def fetch_quote(symbol: str) -> dict:
    """
    Fetch current/prev-close/high/low/open/volume for a ticker.
    Returns a small, clean dict. Raises FinnhubError on problems.
    """
    _check_key()
    url = f"{API_BASE}/quote"
    params = {"symbol": symbol.upper().strip(), "token": API_KEY}
    r = requests.get(url, params=params, timeout=10)
    if r.status_code == 429:
        raise FinnhubError("Rate limit hit")
    if not r.ok:
        raise FinnhubError(f"HTTP {r.status_code}: {r.text[:200]}")
    data = r.json() or {}
    # Finnhub fields: c=current, pc=prev close, h=high, l=low, o=open, v=volume
    return {
        "symbol": symbol.upper().strip(),
        "current": data.get("c"),
        "prev_close": data.get("pc"),
        "high": data.get("h"),
        "low": data.get("l"),
        "open": data.get("o"),
        "volume": data.get("v"),
        "raw": data,  # keep raw for debugging
    }










