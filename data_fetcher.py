import os
import requests
from datetime import datetime, timedelta

API_KEY = os.getenv("FINNHUB_API_KEY")
BASE_URL = "https://finnhub.io/api/v1"

def _require_key():
    if not API_KEY:
        # Make it obvious in logs if the key is missing
        raise RuntimeError("FINNHUB_API_KEY is not set")

def fetch_quote(symbol: str):
    """
    Returns a normalized quote dict or None on error.
    """
    try:
        _require_key()
        url = f"{BASE_URL}/quote"
        params = {"symbol": symbol, "token": API_KEY}
        r = requests.get(url, params=params, timeout=12)
        if r.status_code != 200:
            return None
        j = r.json() or {}
        # Finnhub returns { c, h, l, o, pc, d, dp, t? }
        if j.get("c") is None:
            return None
        return {
            "symbol": symbol.upper(),
            "current": j.get("c"),
            "prev_close": j.get("pc"),
            "high": j.get("h"),
            "low": j.get("l"),
            "open": j.get("o"),
            "percent_change": j.get("d"),  # absolute delta; dp is %
            "raw": j,
        }
    except Exception:
        return None

def fetch_history(symbol: str, range_key: str):
    """
    Fetch daily candles for {1M,3M,6M,1Y}.
    Returns: list of {time, close}
    """
    try:
        _require_key()

        days_map = {
            "1M": 30,
            "3M": 90,
            "6M": 180,
            "1Y": 365,
        }
        days = days_map.get((range_key or "").upper(), 30)

        end = datetime.utcnow()
        start = end - timedelta(days=days)

        url = f"{BASE_URL}/stock/candle"
        params = {
            "symbol": symbol,
            "resolution": "D",
            "from": int(start.timestamp()),
            "to": int(end.timestamp()),
            "token": API_KEY,
        }
        r = requests.get(url, params=params, timeout=20)
        if r.status_code != 200:
            return []

        j = r.json() or {}
        if j.get("s") != "ok":
            return []

        ts = j.get("t", [])
        closes = j.get("c", [])
        points = []
        for t, c in zip(ts, closes):
            points.append({"time": int(t), "close": float(c)})
        return points
    except Exception:
        return []






























