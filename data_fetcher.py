import os
import requests

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "").strip()

def fetch_quote(symbol: str):
    if not FINNHUB_API_KEY:
        return None

    url = "https://finnhub.io/api/v1/quote"
    params = {"symbol": symbol.upper(), "token": FINNHUB_API_KEY}
    try:
        r = requests.get(url, params=params, timeout=8)
        r.raise_for_status()
        q = r.json() or {}
    except Exception:
        return None

    # Finnhub returns 0 when market closed/no data; still parse basics
    try:
        current = float(q.get("c") or 0)
        prev_close = float(q.get("pc") or 0)
        high = float(q.get("h") or 0)
        low = float(q.get("l") or 0)
        open_ = float(q.get("o") or 0)
    except Exception:
        return None

    pct_change = 0.0
    if prev_close:
        pct_change = (current - prev_close) / prev_close * 100.0

    return {
        "symbol": symbol.upper(),
        "current": current,
        "prev_close": prev_close,
        "high": high,
        "low": low,
        "open": open_,
        "percent_change": round(pct_change, 3),
        "volume": None,
        "raw": {"c": current, "pc": prev_close, "h": high, "l": low, "o": open_},
    }



























