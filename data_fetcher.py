import os
import time
import requests
from typing import Dict, Any, List, Optional, Tuple

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "").strip()
FINNHUB_BASE = "https://finnhub.io/api/v1"

_session = requests.Session()
_session.headers.update({"User-Agent": "stackiq-web/1.0"})

def _get(url: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        r = _session.get(url, params=params, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None

def fetch_quote(symbol: str) -> Optional[Dict[str, Any]]:
    """
    Returns: {
      symbol, current, prev_close, high, low, open, percent_change, volume, raw
    }
    """
    if not FINNHUB_API_KEY:
        return None

    data = _get(
        f"{FINNHUB_BASE}/quote",
        {"symbol": symbol.upper(), "token": FINNHUB_API_KEY},
    )
    if not data or "c" not in data:
        return None

    c = data.get("c")  # current
    pc = data.get("pc")
    pct = None
    try:
        if c is not None and pc:
            pct = ((float(c) - float(pc)) / float(pc)) * 100.0
    except Exception:
        pct = None

    return {
        "symbol": symbol.upper(),
        "current": data.get("c"),
        "prev_close": data.get("pc"),
        "high": data.get("h"),
        "low": data.get("l"),
        "open": data.get("o"),
        "percent_change": pct,
        "volume": data.get("v") if isinstance(data.get("v"), (int, float)) else None,
        "raw": data,
    }

# ---------- History ----------

_RANGE_TO_SECONDS = {
    "1M": 60 * 60 * 24 * 30,
    "3M": 60 * 60 * 24 * 90,
    "6M": 60 * 60 * 24 * 180,
    "1Y": 60 * 60 * 24 * 365,
}

def _resolution_for_span(span_days: int) -> str:
    """Pick a Finnhub resolution that won’t blow up payload size."""
    if span_days <= 31:
        return "D"      # daily
    elif span_days <= 200:
        return "W"      # weekly
    return "M"          # monthly

def fetch_history(symbol: str, range_key: str) -> Optional[Dict[str, Any]]:
    """
    Returns: { symbol, range, resolution, points: [{t, p}] }
    where t is unix seconds, p is close price
    """
    if not FINNHUB_API_KEY:
        return None

    rk = range_key.upper()
    span = _RANGE_TO_SECONDS.get(rk)
    if not span:
        return None

    now = int(time.time())
    fro = now - span
    # map span to a sensible resolution
    days = span // (60 * 60 * 24)
    res = _resolution_for_span(days)

    data = _get(
        f"{FINNHUB_BASE}/stock/candle",
        {
            "symbol": symbol.upper(),
            "resolution": res,
            "from": fro,
            "to": now,
            "token": FINNHUB_API_KEY,
        },
    )
    if not data or data.get("s") != "ok":
        return None

    ts = data.get("t", [])  # timestamps
    closes = data.get("c", [])
    points: List[Dict[str, Any]] = []
    for t, p in zip(ts, closes):
        # Finnhub t is in seconds already; ensure numeric
        try:
            points.append({"t": int(t), "p": float(p)})
        except Exception:
            continue

    if not points:
        return None

    return {
        "symbol": symbol.upper(),
        "range": rk,
        "resolution": res,
        "points": points,
    }





























