import os
import time
import requests
from typing import Dict, Any, List, Optional, Tuple

_FINNHUB_KEY = os.getenv("FINNHUB_API_KEY", "").strip()
_FINN_BASE = "https://finnhub.io/api/v1"

def _get(url: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not _FINNHUB_KEY:
        return None
    try:
        params = {**params, "token": _FINNHUB_KEY}
        r = requests.get(url, params=params, timeout=10)
        if r.status_code != 200:
            return None
        return r.json()
    except Exception:
        return None

def fetch_quote(symbol: str) -> Optional[Dict[str, Any]]:
    """
    Returns:
      {
        "symbol": "AAPL",
        "current": 232.14,
        "prev_close": 232.56,
        "high": 233.38,
        "low": 231.37,
        "open": 232.51,
        "percent_change": -0.18,
        "volume": null,
        "raw": {...}
      }
    """
    j = _get(f"{_FINN_BASE}/quote", {"symbol": symbol.upper()})
    if not j or "c" not in j:
        return None
    cur = j.get("c")
    pc  = j.get("pc")
    hi  = j.get("h")
    lo  = j.get("l")
    op  = j.get("o")
    pct = None
    if isinstance(cur, (int, float)) and isinstance(pc, (int, float)) and pc:
        pct = ((cur - pc) / pc) * 100.0
    return {
        "symbol": symbol.upper(),
        "current": cur,
        "prev_close": pc,
        "high": hi,
        "low": lo,
        "open": op,
        "percent_change": pct,
        "volume": j.get("v"),
        "raw": j
    }

def _range_to_window(range_key: str) -> Tuple[int, str]:
    """
    Returns (seconds_back, resolution)
    - 1M -> last 30 days, 'D'
    - 3M -> 90 days, 'D'
    - 6M -> 180 days, 'D'
    - 1Y -> 365 days, 'W' (weekly to limit points)
    """
    rk = (range_key or "1M").upper()
    if rk == "1M":
        return (30 * 24 * 3600, "D")
    if rk == "3M":
        return (90 * 24 * 3600, "D")
    if rk == "6M":
        return (180 * 24 * 3600, "D")
    # default 1Y
    return (365 * 24 * 3600, "W")

def fetch_history(symbol: str, range_key: str = "1M") -> Optional[List[Dict[str, Any]]]:
    """
    Returns list of { "t": unix_sec, "c": close } sorted by time asc.
    """
    if not _FINNHUB_KEY:
        return None

    seconds_back, resolution = _range_to_window(range_key)
    now = int(time.time())
    frm = now - seconds_back

    j = _get(
        f"{_FINN_BASE}/stock/candle",
        {"symbol": symbol.upper(), "resolution": resolution, "from": frm, "to": now},
    )
    if not j or j.get("s") != "ok":
        # Could be 'no_data' or error — return empty list (front-end can handle)
        return []

    ts = j.get("t") or []
    cs = j.get("c") or []
    points: List[Dict[str, Any]] = []
    # Ensure same length and monotonic time order
    for t, c in zip(ts, cs):
        if isinstance(t, int) and isinstance(c, (int, float)):
            points.append({"t": int(t), "c": float(c)})

    # Finnhub returns ascending times already, but sort just in case
    points.sort(key=lambda p: p["t"])
    return points































