import time
import requests
from typing import Optional, Dict, Any

# Simple in-memory cache to smooth over bursts / transient errors
# key: normalized symbol -> (expires_at_epoch, payload_dict)
_CACHE: Dict[str, Any] = {}
_TTL_SECONDS = 12  # short but helps the UI feel stable

_YF_URL = "https://query1.finance.yahoo.com/v7/finance/quote?symbols={symbol}"

def _normalize(symbol: str) -> str:
    """
    Normalize user input to what Yahoo expects.
    - Trim whitespace
    - Uppercase
    - If user typed like 'aapl.us' or 'aapl', keep user intent but uppercase.
    Yahoo handles plain 'AAPL', 'MSFT', 'TSLA', as well as region suffixes.
    """
    s = (symbol or "").strip().upper()
    return s

def _fetch_yahoo(symbol: str, timeout: float = 8.0) -> Optional[Dict[str, Any]]:
    """
    Call Yahoo's public quote endpoint and map to our unified schema.
    Returns None on any hard failure so the router can 404 cleanly.
    """
    headers = {
        # Some edge networks dislike "default" UA; this keeps responses consistent.
        "User-Agent": "stackiq-web/1.0 (+https://example.com)"
    }
    url = _YF_URL.format(symbol=symbol)
    try:
        r = requests.get(url, headers=headers, timeout=timeout)
    except Exception:
        return None

    if r.status_code != 200:
        return None

    try:
        payload = r.json()
        results = payload.get("quoteResponse", {}).get("result", [])
        if not results:
            return None
        q = results[0]

        # Pull common fields; fall back to None where missing
        current = q.get("regularMarketPrice")
        prev_close = q.get("regularMarketPreviousClose")
        high = q.get("regularMarketDayHigh")
        low = q.get("regularMarketDayLow")
        open_p = q.get("regularMarketOpen")
        volume = q.get("regularMarketVolume")
        sym_out = q.get("symbol") or symbol

        if current is None or prev_close is None:
            return None

        pct_change = 0.0
        try:
            if prev_close:
                pct_change = ((float(current) - float(prev_close)) / float(prev_close)) * 100.0
        except Exception:
            pct_change = 0.0

        out = {
            "symbol": str(sym_out),
            "current": round(float(current), 3),
            "prev_close": round(float(prev_close), 3),
            "high": round(float(high), 3) if high is not None else None,
            "low": round(float(low), 3) if low is not None else None,
            "open": round(float(open_p), 3) if open_p is not None else None,
            "percent_change": round(float(pct_change), 3),
            "volume": int(volume) if isinstance(volume, (int, float)) else None,
            "raw": {
                "c": current,
                "pc": prev_close,
                "h": high,
                "l": low,
                "o": open_p,
                "v": volume,
            },
        }
        return out
    except Exception:
        return None

def fetch_quote(symbol: str) -> Optional[Dict[str, Any]]:
    """
    Public function used by the API routes.
    - Normalizes the symbol
    - Serves from short cache
    - Retries Yahoo briefly on transient errors
    """
    sym = _normalize(symbol)
    if not sym:
        return None

    # Cache hit?
    now = time.time()
    cached = _CACHE.get(sym)
    if cached and cached[0] > now:
        return cached[1]

    # Light retry: try up to 2 attempts quickly
    last = None
    for _ in range(2):
        data = _fetch_yahoo(sym)
        if data:
            last = data
            break
        time.sleep(0.25)

    if last:
        _CACHE[sym] = (now + _TTL_SECONDS, last)
    return last






















