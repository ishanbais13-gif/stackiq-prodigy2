import requests
import logging

log = logging.getLogger("stackiq-web")

# CSV endpoint; one row of latest daily data
# Example: https://stooq.com/q/l/?s=aapl.us&i=d
STOOQ_URL = "https://stooq.com/q/l/?s={symbol}&i=d"

def _normalize(symbol: str) -> str:
    """
    Stooq expects US tickers like aapl.us (lowercase).
    If the user types AAPL or aapl, convert to aapl.us.
    If a suffix (e.g., .us, .gb) is already present, keep it.
    """
    s = (symbol or "").strip().lower()
    if not s:
        return ""
    if "." in s:
        return s
    return f"{s}.us"

def _parse_stooq_csv(text: str):
    """
    Stooq CSV (i=d) returns two lines:
      1) header: Symbol,Date,Time,Open,High,Low,Close,Volume
      2) data  : aapl.us,YYYY-MM-DD,HH:MM:SS,open,high,low,close,volume
    """
    if not text:
        return None
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if len(lines) < 2:
        return None
    parts = lines[1].split(",")
    if len(parts) < 8:
        return None

    raw_symbol = parts[0]
    try:
        open_p = float(parts[3])
        high = float(parts[4])
        low = float(parts[5])
        close = float(parts[6])
    except Exception:
        return None

    # Stooq “i=d” doesn’t include prev close; use open as an approximation
    prev_close = open_p

    symbol_out = raw_symbol.split(".")[0].upper()
    current = close
    pct_change = ((current - prev_close) / prev_close * 100) if prev_close else 0.0

    return {
        "symbol": symbol_out,
        "current": round(current, 3),
        "prev_close": round(prev_close, 3),
        "high": round(high, 3),
        "low": round(low, 3),
        "open": round(open_p, 3),
        "percent_change": round(pct_change, 3),
        "volume": None,
        "raw": {
            "c": current,
            "pc": prev_close,
            "h": high,
            "l": low,
            "o": open_p,
        },
    }

def fetch_quote(symbol: str):
    sym_norm = _normalize(symbol)
    if not sym_norm:
        return None

    headers = {
        # Some free endpoints require a UA to avoid being blocked
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    }

    try:
        url = STOOQ_URL.format(symbol=sym_norm)
        r = requests.get(url, timeout=8, headers=headers)
        if r.status_code != 200 or not r.text:
            log.warning("stooq bad response: code=%s body_len=%s", r.status_code, len(r.text or ""))
            return None
        parsed = _parse_stooq_csv(r.text)
        if not parsed:
            log.warning("stooq parse failed for %s:\n%s", sym_norm, r.text[:200])
        return parsed
    except Exception as e:
        log.exception("fetch_quote error for %s: %s", sym_norm, e)
        return None

















