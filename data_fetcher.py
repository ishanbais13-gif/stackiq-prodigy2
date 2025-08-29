import os
import time
import requests

FINNHUB_KEY = os.getenv("FINNHUB_API_KEY", "").strip()

def _get(url: str, params: dict):
    try:
        r = requests.get(url, params=params, timeout=10)
        if r.status_code != 200:
            return None
        return r.json()
    except Exception:
        return None

def fetch_quote(symbol: str):
    """
    Returns a normalized quote dict or None.
    Uses Finnhub's /quote endpoint.
    """
    if not FINNHUB_KEY:
        return None

    url = "https://finnhub.io/api/v1/quote"
    params = {"symbol": symbol.upper(), "token": FINNHUB_KEY}
    data = _get(url, params)
    if not data or "c" not in data:
        return None

    try:
        current = float(data.get("c") or 0)
        prev_close = float(data.get("pc") or 0)
        high = float(data.get("h") or 0)
        low = float(data.get("l") or 0)
        open_ = float(data.get("o") or 0)
        pct = ((current - prev_close) / prev_close * 100) if prev_close else 0.0
    except Exception:
        return None

    return {
        "symbol": symbol.upper(),
        "current": current,
        "prev_close": prev_close,
        "high": high,
        "low": low,
        "open": open_,
        "percent_change": round(pct, 3),
        "volume": None,
        "raw": {"c": current, "pc": prev_close, "h": high, "l": low, "o": open_},
    }

def fetch_candles(symbol: str, range_: str = "1M"):
    """
    Return daily candles for the symbol as list of {t, o, h, l, c, v}.
    Supported ranges: 1M, 3M, 6M, 1Y
    """
    if not FINNHUB_KEY:
        return []

    rng = (range_ or "1M").upper()
    days = {"1M": 30, "3M": 90, "6M": 180, "1Y": 365}.get(rng, 30)

    now = int(time.time())
    _from = now - days * 24 * 60 * 60

    url = "https://finnhub.io/api/v1/stock/candle"
    params = {
        "symbol": symbol.upper(),
        "resolution": "D",
        "from": _from,
        "to": now,
        "token": FINNHUB_KEY,
    }
    data = _get(url, params)
    if not data or data.get("s") != "ok":
        return []

    candles = []
    try:
        for i in range(len(data["t"])):
            candles.append({
                "t": data["t"][i],
                "o": data["o"][i],
                "h": data["h"][i],
                "l": data["l"][i],
                "c": data["c"][i],
                "v": data["v"][i],
            })
    except Exception:
        return []

    return candles

























