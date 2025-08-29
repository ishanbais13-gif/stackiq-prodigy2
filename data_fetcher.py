import os
import requests
from typing import Optional, Dict, Any

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "").strip()
FINNHUB_URL = "https://finnhub.io/api/v1/quote"

def _normalize(symbol: str) -> str:
    """
    Accepts 'AAPL', 'aapl', 'AAPL.US', etc. Returns the raw ticker FINNHUB expects.
    For US tickers, FINNHUB just wants 'AAPL'.
    If the user gave 'AAPL.US', return 'AAPL'.
    """
    s = (symbol or "").strip().upper()
    if not s:
        return ""
    if s.endswith(".US"):
        s = s[:-3]
    return s

def fetch_quote(symbol: str) -> Optional[Dict[str, Any]]:
    """
    Calls Finnhub /quote and maps fields to a consistent structure.
    Returns None on any failure we can't recover from.
    """
    sym = _normalize(symbol)
    if not sym or not FINNHUB_API_KEY:
        return None

    try:
        r = requests.get(
            FINNHUB_URL,
            params={"symbol": sym, "token": FINNHUB_API_KEY},
            timeout=10,
        )
    except Exception:
        return None

    if r.status_code != 200:
        return None

    data = r.json() or {}
    # Finnhub returns: c (current), pc (prev close), h, l, o, t (unix time)
    # On error it may return all zeros.
    if not isinstance(data, dict) or ("c" not in data):
        return None

    try:
        current = float(data.get("c") or 0.0)
        prev_close = float(data.get("pc") or 0.0)
        high = float(data.get("h") or 0.0)
        low = float(data.get("l") or 0.0)
        open_p = float(data.get("o") or 0.0)
    except Exception:
        return None

    if current == 0.0 and prev_close == 0.0 and high == 0.0 and low == 0.0:
        # Finnhub sometimes returns zeros for unknown symbols / off-hours + no data
        return None

    pct_change = 0.0
    if prev_close:
        pct_change = ((current - prev_close) / prev_close) * 100.0

    return {
        "symbol": sym,
        "current": round(current, 3),
        "prev_close": round(prev_close, 3),
        "high": round(high, 3),
        "low": round(low, 3),
        "open": round(open_p, 3),
        "percent_change": round(pct_change, 3),
        "volume": None,  # not in /quote response; leave None for now
        "raw": {
            "c": current, "pc": prev_close, "h": high, "l": low, "o": open_p,
        },
    }
























