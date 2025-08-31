# --- Historical candles from Finnhub -----------------------------------------
import time
import os
import requests

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "")

def fetch_history(symbol: str, range_key: str):
    """
    Return a list of {t, c} points for the given symbol and range_key in:
      '1M', '3M', '6M', '1Y'
    Uses Finnhub stock/candle with daily resolution.
    """
    if not FINNHUB_API_KEY:
        return []

    range_key = (range_key or "3M").upper()
    now = int(time.time())

    days_map = {
        "1M": 30,
        "3M": 90,
        "6M": 180,
        "1Y": 365,
    }
    days = days_map.get(range_key, 90)

    frm = now - days * 24 * 60 * 60
    url = "https://finnhub.io/api/v1/stock/candle"
    params = {
        "symbol": symbol.upper(),
        "resolution": "D",
        "from": frm,
        "to": now,
        "token": FINNHUB_API_KEY,
    }

    try:
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        j = r.json()
        # Expected when OK: { "c": [...], "t": [...], "s": "ok", ... }
        if j.get("s") != "ok" or not j.get("c") or not j.get("t"):
            return []

        closes = j["c"]
        times = j["t"]
        points = [{"t": int(t), "c": float(c)} for t, c in zip(times, closes)]
        # Finnhub can return oldest->newest already; ensure sorted
        points.sort(key=lambda p: p["t"])
        return points
    except Exception:
        return []


























