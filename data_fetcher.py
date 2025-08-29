import os
import time
from typing import Optional, Dict, Any
import requests

# ===== Config =====
# Set your key in the environment as FINNHUB_KEY (Azure App Settings → Configuration)
_FINNHUB_KEY = os.getenv("FINNHUB_KEY", "").strip()
_FINNHUB_URL = "https://finnhub.io/api/v1/quote?symbol={symbol}&token={token}"

# short cache to smooth over transient errors / rate limits
_CACHE: Dict[str, Any] = {}  # key -> (expires_at_epoch, payload)
_TTL_SECONDS = 12


def _normalize(symbol: str) -> str:
    """
    Finnhub expects plain tickers like AAPL, MSFT, TSLA.
    Uppercase and trim. If user typed 'aapl.us', keep AAPL part.
    """
    s = (symbol or "").strip().upper()
    # strip common suffixes users might type
    for suf in (".US", "-US", ".NYSE", ".NASDAQ"):
        if s.endswith(suf):
            s = s[: -len(suf)]
    if "." in s:  # e.g., "AAPL.US" -> "AAPL"
        s = s.split(".")[0]
    return s


def _fetch_finnhub(symbol: str, timeout: float = 8.0) -> Optional[Dict[str, Any]]:
    """
    Call Finnhub's /quote endpoint and map to our unified schema.
    Returns None on failure or if no data.
    """
    if not _FINNHUB_KEY:
        # Not configured – treat as no data so API returns 404 instead of 500
        return None

    url = _FINNHUB_URL.format(symbol=symbol, token=_FINNHUB_KEY)
    headers = {"User-Agent": "stackiq-web/1.0"}
    try:
        r = requests.get(url, headers=headers, timeout=timeout)
    except Exception:
        return None

    if r.status_code != 200:
        return None

    try:
        q = r.json() or {}
        # Finnhub fields: c=current, d=change, dp=percent, h=high, l=low, o=open, pc=prev close, t=ts
        c = q.get("c")
        pc = q.get("pc")
        if c in (None, 0) and pc in (None, 0):
            return None

        high = q.get("h")
        low = q.get("l")
        open_p = q.get("o")
        dp = q.get("dp")  # Finnhub already gives percent change
        if dp is None and (c is not None and pc not in (None, 0)):
            try:
                dp = ((float(c) - float(pc)) / float(pc)) * 100.0
            except Exception:
                dp = 0.0

        out = {
            "symbol": symbol,
            "current": round(float(c), 3) if c is not None else None,
            "prev_close": round(float(pc), 3) if pc is not None else None,
            "high": round(float(high), 3) if high is not None else None,
            "low": round(float(low), 3) if low is not None else None,
            "open": round(float(open_p), 3) if open_p is not None else None,
            "percent_change": round(float(dp), 3) if dp is not None else 0.0,
            "volume": None,  # Finnhub /quote does not return volume
            "raw": {
                "c": c,
                "pc": pc,
                "h": high,
                "l": low,
                "o": open_p,
                "dp": dp,
                "t": q.get("t"),
            },
        }
        return out
    except Exception:
        return None


def fetch_quote(symbol: str) -> Optional[Dict[str, Any]]:
    """
    Public function used by API routes.
    - normalize input
    - short cache
    - quick retry against Finnhub to ride out hiccups
    """
    sym = _normalize(symbol)
    if not sym:
        return None

    now = time.time()
    cached = _CACHE.get(sym)
    if cached and cached[0] > now:
        return cached[1]

    last = None
    for _ in range(2):  # tiny retry
        data = _fetch_finnhub(sym)
        if data:
            last = data
            break
        time.sleep(0.25)

    if last:
        _CACHE[sym] = (now + _TTL_SECONDS, last)
    return last























