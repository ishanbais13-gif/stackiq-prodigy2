import os
import time
from typing import Optional, Dict, Any, List
import requests

FINNHUB_KEY = os.getenv("FINNHUB_API_KEY", "").strip()

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "stackiq-web/1.0"})


def _get_json(url: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        r = SESSION.get(url, params=params, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None


def fetch_quote(symbol: str) -> Optional[Dict[str, Any]]:
    """
    Fetch a real-time-ish quote from Finnhub.
    """
    if not FINNHUB_KEY:
        # Provide a helpful error if key is missing
        return None

    url = "https://finnhub.io/api/v1/quote"
    j = _get_json(url, {"symbol": symbol.upper(), "token": FINNHUB_KEY})
    if not j or "c" not in j:
        return None

    out = {
        "symbol": symbol.upper(),
        "current": float(j.get("c") or 0),
        "prev_close": float(j.get("pc") or 0),
        "high": float(j.get("h") or 0),
        "low": float(j.get("l") or 0),
        "open": float(j.get("o") or 0),
        "percent_change": 0.0,
        "volume": j.get("v"),
        "raw": j,
    }
    # daily percent change based on prev close
    if out["prev_close"]:
        out["percent_change"] = ((out["current"] - out["prev_close"]) / out["prev_close"]) * 100.0
    return out


def fetch_history(symbol: str, range_key: str = "1M") -> Optional[List[Dict[str, float]]]:
    """
    Fetch historical candles from Finnhub for ranges used in the UI.
    Returns a list like: [{"t": 1692748800, "c": 123.45}, ...]
    """
    if not FINNHUB_KEY:
        return None

    # Map range -> (resolution, seconds back)
    now = int(time.time())
    ranges = {
        "1M": ("D", 60 * 60 * 24 * 32),        # ~32 days
        "3M": ("D", 60 * 60 * 24 * 95),
        "6M": ("D", 60 * 60 * 24 * 190),
        "1Y": ("W", 60 * 60 * 24 * 370),       # weekly to keep points reasonable
    }
    res, back = ranges.get(range_key, ("D", 60 * 60 * 24 * 32))
    frm = now - back
    to = now

    url = "https://finnhub.io/api/v1/stock/candle"
    j = _get_json(
        url,
        {"symbol": symbol.upper(), "resolution": res, "from": frm, "to": to, "token": FINNHUB_KEY},
    )
    if not j or j.get("s") != "ok":
        return None

    t = j.get("t", [])  # timestamps
    c = j.get("c", [])  # close prices
    if not t or not c or len(t) != len(c):
        return None

    points = [{"t": int(ts), "c": float(cv)} for ts, cv in zip(t, c)]
    # Sort by time just in case
    points.sort(key=lambda p: p["t"])
    return points




























