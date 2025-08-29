import json
import os
import ssl
import urllib.parse
import urllib.request

# Finnhub only. NO Yahoo. This file must never crash at import time.

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "").strip()

# Build a very safe SSL context (Azure can be picky on some stacks)
_ssl_ctx = ssl.create_default_context()

def _http_get_json(url: str):
    """Tiny helper that returns parsed JSON or None (never raises)."""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "stackiq-web/1.0"})
        with urllib.request.urlopen(req, context=_ssl_ctx, timeout=10) as resp:
            data = resp.read()
        return json.loads(data.decode("utf-8"))
    except Exception:
        return None

def fetch_quote(symbol: str):
    """
    Return a normalized dict for the given symbol (AAPL, MSFT, etc.) using Finnhub.
    If anything fails (no key, bad symbol, network), return None.
    """
    if not symbol:
        return None

    sym = symbol.upper().strip()

    # Need a key to call Finnhub. If missing, fail gracefully (UI will show 404).
    if not FINNHUB_API_KEY:
        return None

    # Finnhub quote endpoint: fields: c (current), pc (prev close),
    # h (high), l (low), o (open). A valid response also has "t" timestamp.
    qs = urllib.parse.urlencode({"symbol": sym, "token": FINNHUB_API_KEY})
    url = f"https://finnhub.io/api/v1/quote?{qs}"

    payload = _http_get_json(url)
    if not payload or not isinstance(payload, dict):
        return None

    # When symbol is invalid, Finnhub often returns zeros; guard against that.
    c = float(payload.get("c") or 0.0)
    pc = float(payload.get("pc") or 0.0)
    h = float(payload.get("h") or 0.0)
    l = float(payload.get("l") or 0.0)
    o = float(payload.get("o") or 0.0)

    # If everything is zero, treat as not found.
    if c == 0.0 and pc == 0.0 and h == 0.0 and l == 0.0 and o == 0.0:
        return None

    pct = ( (c - pc) / pc * 100.0 ) if pc else 0.0

    return {
        "symbol": sym,
        "current": round(c, 3),
        "prev_close": round(pc, 3),
        "high": round(h, 3),
        "low": round(l, 3),
        "open": round(o, 3),
        "percent_change": round(pct, 3),
        "volume": None,  # Finnhub /quote doesn't include volume; leave None.
        "raw": {"c": c, "pc": pc, "h": h, "l": l, "o": o},
    }

























