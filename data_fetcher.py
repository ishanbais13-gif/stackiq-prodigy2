import os
import time
import requests
from typing import Any, Dict, List, Optional

FINNHUB_KEY = os.getenv("FINNHUB_API_KEY", "").strip()
BASE = "https://finnhub.io/api/v1"

def _get(url: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not FINNHUB_KEY:
        return None
    p = dict(params or {})
    p["token"] = FINNHUB_KEY
    try:
        r = requests.get(url, params=p, timeout=12)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None

def _pct_change(prev: Optional[float], curr: Optional[float]) -> Optional[float]:
    try:
        if prev in (None, 0, 0.0) or curr is None:
            return None
        return (float(curr) - float(prev)) / float(prev) * 100.0
    except Exception:
        return None

def fetch_quote(symbol: str) -> Optional[Dict[str, Any]]:
    sym = (symbol or "").upper().strip()
    if not sym:
        return None

    j = _get(f"{BASE}/quote", {"symbol": sym})
    if not j or "c" not in j:
        return None

    c = j.get("c")
    pc = j.get("pc")
    h = j.get("h")
    l = j.get("l")
    o = j.get("o")

    # Some invalid symbols return zeros for everything; treat as missing
    if all(v in (None, 0, 0.0) for v in [c, pc, h, l, o]):
        return None

    pct = _pct_change(pc, c)
    return {
        "symbol": sym,
        "current": c,
        "prev_close": pc,
        "high": h,
        "low": l,
        "open": o,
        "percent_change": None if pct is None else round(pct, 3),
        "volume": j.get("v"),
        "raw": j,
    }

def _range_days(key: str) -> int:
    return {"1M": 31, "3M": 93, "6M": 186, "1Y": 372}.get(key.upper(), 31)

def fetch_history(symbol: str, range_key: str = "1M") -> Optional[List[Dict[str, float]]]:
    """
    Returns list of {"t": epochSec, "c": close} points (or [] if none).
    """
    sym = (symbol or "").upper().strip()
    if not sym:
        return None

    now = int(time.time())
    start = now - _range_days(range_key) * 24 * 60 * 60

    # Finnhub /stock/candle uses keys: s(status), t(times[]), c(closes[])
    j = _get(f"{BASE}/stock/candle", {
        "symbol": sym,
        "resolution": "D",
        "from": start,     # NOTE: must be 'from', not '_from'
        "to": now,
    })
    if not j or j.get("s") != "ok":
        return []

    ts = j.get("t") or []
    cs = j.get("c") or []
    out: List[Dict[str, float]] = []
    for t, c in zip(ts, cs):
        try:
            out.append({"t": float(t), "c": float(c)})
        except Exception:
            continue
    return out



























