import requests

STOOQ_URL = "https://stooq.com/q/l/?s={symbol}&i=d"


def _normalize(symbol: str) -> str:
    """
    Stooq expects tickers like aapl.us (lowercase).
    If user types AAPL or aapl, convert to aapl.us.
    If the user already includes a suffix (e.g., .us, .gb), keep it.
    """
    s = (symbol or "").strip().lower()
    if not s:
        return ""
    if "." in s:
        return s
    return f"{s}.us"


def fetch_quote(symbol: str):
    """
    Returns a dict like:
    {
      "symbol": "AAPL",
      "current": 232.56,
      "prev_close": 230.49,
      "high": 233.41,
      "low": 229.335,
      "open": 230.82,
      "percent_change": 0.89,
      "volume": None,
      "raw": { "c": ..., "pc": ..., "h": ..., "l": ..., "o": ... }
    }
    or None if not found / bad response.
    """
    sym_norm = _normalize(symbol)
    if not sym_norm:
        return None

    try:
        r = requests.get(STOOQ_URL.format(symbol=sym_norm), timeout=10)
        if r.status_code != 200 or not r.text:
            return None

        # CSV lines; example:
        # Symbol,Date,Time,Open,High,Low,Close,Volume
        # aapl.us,2024-08-28,22:00:05,230.82,233.41,229.335,232.56,0
        lines = [ln.strip() for ln in r.text.splitlines() if ln.strip()]
        if len(lines) < 2:
            return None

        parts = lines[1].split(",")
        if len(parts) < 7:
            return None

        raw_symbol = parts[0]      # e.g., aapl.us
        open_p = float(parts[3])
        high = float(parts[4])
        low = float(parts[5])
        close = float(parts[6])

        # Stooq "d" interval doesn’t include prev close; use open as approx.
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
    except Exception:
        return None















