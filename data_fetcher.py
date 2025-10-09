import os
import time
import requests
from typing import Dict, Any, List, Optional

FINNHUB_BASE = "https://finnhub.io/api/v1"
API_KEY = os.getenv("FINNHUB_API_KEY", "").strip()


class FinnhubError(Exception):
    pass


def _require_api_key():
    if not API_KEY:
        raise FinnhubError("Missing FINNHUB_API_KEY in environment.")


def _get(url: str, params: Dict[str, Any]) -> Dict[str, Any]:
    _require_api_key()
    params = dict(params or {})
    params["token"] = API_KEY
    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()
    data = r.json()
    # Finnhub sometimes returns {'error': '...'} or s='no_data' for candles
    if isinstance(data, dict) and data.get("error"):
        raise FinnhubError(data["error"])
    return data


def get_quote(symbol: str) -> Dict[str, Any]:
    """
    Returns:
      {
        "symbol": "AAPL",
        "current": 227.15,
        "change": -0.45,
        "percent": -0.20,
        "high": 230.0,
        "low": 226.5,
        "open": 228.0,
        "prev_close": 227.6,
        "timestamp": 1728422400
      }
    """
    url = f"{FINNHUB_BASE}/quote"
    data = _get(url, {"symbol": symbol.upper()})
    # Finnhub quote fields: c, d, dp, h, l, o, pc, t
    if not isinstance(data, dict) or "c" not in data:
        raise FinnhubError("Unexpected response for quote.")
    return {
        "symbol": symbol.upper(),
        "current": data.get("c"),
        "change": data.get("d"),
        "percent": data.get("dp"),
        "high": data.get("h"),
        "low": data.get("l"),
        "open": data.get("o"),
        "prev_close": data.get("pc"),
        "timestamp": data.get("t"),
    }


def get_candles(symbol: str, days: int = 30, resolution: str = "D") -> Dict[str, Any]:
    """
    Returns Finnhub candles normalized to:
      {
        "symbol": "AAPL",
        "resolution": "D",
        "candles": [
          {"t": 1726780800, "o": 220.1, "h": 221.2, "l": 219.8, "c": 220.9, "v": 32123456},
          ...
        ]
      }
    """
    now = int(time.time())
    frm = now - days * 86400

    url = f"{FINNHUB_BASE}/stock/candle"
    data = _get(url, {
        "symbol": symbol.upper(),
        "resolution": resolution,
        "from": frm,
        "to": now
    })

    if not isinstance(data, dict) or data.get("s") != "ok":
        # s can be 'no_data'
        status = data.get("s") if isinstance(data, dict) else "unknown"
        if status == "no_data":
            return {"symbol": symbol.upper(), "resolution": resolution, "candles": []}
        raise FinnhubError(f"Candle response status: {status}")

    # Finnhub returns arrays: t, o, h, l, c, v
    t = data.get("t", [])
    o = data.get("o", [])
    h = data.get("h", [])
    l = data.get("l", [])
    c = data.get("c", [])
    v = data.get("v", [])

    candles: List[Dict[str, Any]] = []
    for i in range(min(len(t), len(o), len(h), len(l), len(c), len(v))):
        candles.append({
            "t": t[i],
            "o": o[i],
            "h": h[i],
            "l": l[i],
            "c": c[i],
            "v": v[i],
        })

    return {
        "symbol": symbol.upper(),
        "resolution": resolution,
        "candles": candles
    }










































