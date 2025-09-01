import os
import time
from datetime import datetime, timedelta
import requests

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "")
BASE_URL = "https://finnhub.io/api/v1"

def _get(url: str, params: dict) -> dict:
    if not FINNHUB_API_KEY:
        raise RuntimeError("FINNHUB_API_KEY is not set")
    params = {**params, "token": FINNHUB_API_KEY}
    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()
    return r.json()

# ------------------------
# Quotes / Summary helpers
# ------------------------
def fetch_quote(symbol: str) -> dict:
    """
    Returns Finnhub quote normalized into:
    {
      "symbol": "AAPL",
      "current": 232.14,
      "prev_close": 232.56,
      "high": 233.38,
      "low": 231.37,
      "open": 232.51,
      "percent_change": -0.181,
      "volume": null,
      "raw": { ... original fields ... }
    }
    """
    data = _get(f"{BASE_URL}/quote", {"symbol": symbol.upper()})
    # Finnhub fields: c=current, pc=prev close, h=high, l=low, o=open, t=timestamp, d (abs change), dp (percent change)
    return {
        "symbol": symbol.upper(),
        "current": data.get("c"),
        "prev_close": data.get("pc"),
        "high": data.get("h"),
        "low": data.get("l"),
        "open": data.get("o"),
        "percent_change": data.get("dp"),
        "volume": data.get("v"),
        "raw": data,
    }

# ------------------------
# History helper
# ------------------------
def _range_to_days(range_key: str) -> int:
    rk = (range_key or "1M").upper()
    if rk == "1M":
        return 30
    if rk == "3M":
        return 90
    if rk == "6M":
        return 180
    if rk == "1Y":
        return 365
    # default
    return 30

def fetch_history(symbol: str, range_key: str = "1M") -> dict:
    """
    Returns normalized history for the chart:
    {
      "symbol": "AAPL",
      "points": [ {"t": 1717027200, "c": 190.12}, ... ]   # t is unix seconds, c is close
    }
    """
    days = _range_to_days(range_key)
    # Finnhub /stock/candle uses 'from' and 'to' unix seconds, not 'count'
    to_ts = int(time.time())
    from_ts = int((datetime.utcnow() - timedelta(days=days)).timestamp())

    data = _get(
        f"{BASE_URL}/stock/candle",
        {"symbol": symbol.upper(), "resolution": "D", "from": from_ts, "to": to_ts},
    )

    # Finnhub returns: s = "ok" | "no_data"
    if data.get("s") != "ok":
        return {"symbol": symbol.upper(), "points": []}

    closes = data.get("c", [])
    times_ = data.get("t", [])
    points = []
    for i in range(min(len(closes), len(times_))):
        c = closes[i]
        t = times_[i]         # unix seconds
        if c is None or t is None:
            continue
        points.append({"t": int(t), "c": float(c)})

    return {"symbol": symbol.upper(), "points": points}
































