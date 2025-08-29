import requests

# Try both HTTPS and HTTP (some hosts intermittently block one or the other from Azure IPs)
STOOQ_URLS = [
    "https://stooq.com/q/l/?s={symbol}&i=d",
    "http://stooq.com/q/l/?s={symbol}&i=d",
]

# A very plain UA helps avoid some “empty response for bots” filters
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; StackIQ/1.0; +https://example.com)"
}


def _normalize(symbol: str) -> str | None:
    """
    Stooq expects tickers like aapl.us (lowercase).
    If the user types AAPL or aapl, convert to aapl.us.
    If the user already includes a suffix (e.g., .us, .gb), keep it.
    """
    s = (symbol or "").strip().lower()
    if not s:
        return None
    if "." in s:
        return s
    return f"{s}.us"


def _fetch_stooq_csv(sym_norm: str, timeout: int = 10) -> tuple[int | None, str | None]:
    """
    Try both HTTPS and HTTP. Return (status_code, text) where either can be None on failure.
    """
    for base in STOOQ_URLS:
        url = base.format(symbol=sym_norm)
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout)
            if r.status_code == 200 and r.text:
                return 200, r.text
            # try next transport
        except Exception:
            # try next transport
            pass
    return None, None


def fetch_quote(symbol: str) -> dict | None:
    """
    Returns a normalized dict or None on failure (caller turns that into 404).
    """
    sym_norm = _normalize(symbol)
    if not sym_norm:
        return None

    status, text = _fetch_stooq_csv(sym_norm)
    if status != 200 or not text:
        return None

    # Stooq CSV format (daily):
    # Header: Symbol,Date,Time,Open,High,Low,Close,Volume
    # Data:   aapl.us,2024-08-28,22:00:05,231.23,233.41,229.33,232.56,123456
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if len(lines) < 2:
        return None

    parts = lines[1].split(",")
    if len(parts) < 7:
        return None

    try:
        raw_symbol = parts[0]            # e.g., aapl.us
        open_p = float(parts[3])
        high = float(parts[4])
        low = float(parts[5])
        close = float(parts[6])

        # Stooq daily row doesn't include previous close → use open as an approximation
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


def debug_stooq(symbol: str) -> dict:
    """
    Lightweight debug helper: shows what we asked for and a snippet of the CSV we got.
    """
    sym_norm = _normalize(symbol) or ""
    status, text = _fetch_stooq_csv(sym_norm)
    preview = (text or "")[:400]
    return {
        "normalized": sym_norm,
        "status": status,
        "has_text": bool(text),
        "preview": preview,
    }














