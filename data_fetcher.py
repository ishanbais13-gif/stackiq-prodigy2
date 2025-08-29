import requests
from typing import Optional, Dict, Any

# Stooq CSV (daily). Example row after header:
# AAPL.US,2024-08-28,23:59:59,230.82,233.41,229.335,232.56,38074700
STOOQ_URL = "https://stooq.com/q/l/?s={symbol}&i=d"

def _normalize(symbol: str) -> str:
    s = (symbol or "").strip().lower()
    if not s:
        return ""
    if "." in s:
        return s
    return f"{s}.us"  # default to US ticker

def fetch_quote(symbol: str) -> Optional[Dict[str, Any]]:
    syn = _normalize(symbol)
    if not syn:
        return None

    try:
        r = requests.get(STOOQ_URL.format(symbol=syn), timeout=10)
        if r.status_code != 200 or not r.text:
            return None

        lines = [ln.strip() for ln in r.text.splitlines() if ln.strip()]
        if len(lines) < 2:
            return None

        # header: Symbol,Date,Time,Open,High,Low,Close,Volume
        parts = lines[1].split(",")
        if len(parts) < 8:
            return None

        raw_symbol = parts[0]          # e.g., aapl.us
        open_p = float(parts[3])
        high = float(parts[4])
        low = float(parts[5])
        close = float(parts[6])

        # If we don't have prior close from this feed, approximate with 'open'
        prev_close = open_p

        symbol_out = raw_symbol.split(".")[0].upper()
        current = close
        pct_change = ((current - prev_close) / prev_close * 100) if prev_close else 0.0

        volume = parts[7] if parts[7] not in ("", "0") else None

        return {
            "symbol": symbol_out,
            "current": round(current, 3),
            "prev_close": round(prev_close, 3),
            "high": round(high, 3),
            "low": round(low, 3),
            "open": round(open_p, 3),
            "percent_change": round(pct_change, 3),
            "volume": volume,
            "raw": {"c": current, "pc": prev_close, "h": high, "l": low, "o": open_p},
        }
    except Exception:
        return None





















