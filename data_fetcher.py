import os
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "").strip()
FINNHUB_BASE = "https://finnhub.io/api/v1"


def _get(url: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not FINNHUB_API_KEY:
        # Still return shape so frontend doesn't crash
        return None
    p = dict(params)
    p["token"] = FINNHUB_API_KEY
    try:
        r = requests.get(url, params=p, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None


def fetch_quote(symbol: str) -> Optional[Dict[str, Any]]:
    """Return normalized quote for the symbol, or None on failure."""
    j = _get(f"{FINNHUB_BASE}/quote", {"symbol": symbol.upper()})
    if not j or "c" not in j:
        return None

    # Finnhub fields: c=current, pc=prev close, h=high, l=low, o=open, t=timestamp
    cur = float(j.get("c") or 0)
    prev = float(j.get("pc") or 0)
    pct = ((cur - prev) / prev * 100.0) if prev else 0.0

    return {
        "symbol": symbol.upper(),
        "current": cur,
        "prev_close": float(j.get("pc") or 0),
        "high": float(j.get("h") or 0),
        "low": float(j.get("l") or 0),
        "open": float(j.get("o") or 0),
        "percent_change": pct,
        "volume": j.get("v"),
        "raw": j,
    }


def fetch_history(
    symbol: str,
    start: datetime,
    end: datetime,
    resolution: str = "D",
) -> List[Dict[str, Any]]:
    """
    Fetch OHLC candles for [start, end] and return [{"t": unix, "c": close}, ...].
    Resolution: "1", "5", "15", "30", "60", "D", "W", "M".
    """
    if not FINNHUB_API_KEY:
        return []

    fr = int(start.timestamp())
    to = int(end.timestamp())
    j = _get(
        f"{FINNHUB_BASE}/stock/candle",
        {"symbol": symbol.upper(), "resolution": resolution, "from": fr, "to": to},
    )
    if not j or j.get("s") != "ok":
        return []
    t = j.get("t") or []
    c = j.get("c") or []
    out = []
    for ts, close in zip(t, c):
        try:
            out.append({"t": int(ts), "c": float(close)})
        except Exception:
            pass
    return out

































