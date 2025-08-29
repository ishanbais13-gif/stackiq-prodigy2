import requests

# We’ll try both .com and .pl mirrors and both http/https for resilience
STOOQ_TEMPLATES = [
    "https://stooq.com/q/l/?s={sym}&i=d",
    "http://stooq.com/q/l/?s={sym}&i=d",
    "https://stooq.pl/q/l/?s={sym}&i=d",
    "http://stooq.pl/q/l/?s={sym}&i=d",
]

HEADERS = {
    "User-Agent": "stackiq-web/1.0 (+https://example.com)"
}

def _normalize(symbol: str) -> str:
    """
    Stooq expects US tickers like aapl.us (lowercase).
    If the user types AAPL or aapl, convert to aapl.us.
    If the user already includes a suffix (e.g., .us, .gb), keep it.
    """
    s = (symbol or "").strip().lower()
    if not s:
        return ""
    if "." in s:
        return s
    return f"{s}.us"

def _try_fetch(sym_norm: str, timeout=8):
    for tpl in STOOQ_TEMPLATES:
        url = tpl.format(sym=sym_norm)
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout)
            # Stooq returns HTTP 200 with CSV body; an unknown symbol often yields CSV with N/A
            if r.status_code == 200 and r.text:
                return r.text
        except Exception:
            # try next template
            continue
    return None

def fetch_quote(symbol: str):
    sym_norm = _normalize(symbol)
    if not sym_norm:
        return None

    raw = _try_fetch(sym_norm)
    if not raw:
        return None

    # CSV format (single line after header):
    # Symbol,Date,Time,Open,High,Low,Close,Volume
    lines = [ln.strip() for ln in raw.splitlines() if ln.strip()]
    if len(lines) < 2:
        return None

    parts = lines[1].split(",")
    if len(parts) < 8:
        return None

    raw_symbol = parts[0]          # e.g., aapl.us
    # stooq returns daily row; prev_close isn’t provided -> approximate with open
    try:
        open_p = float(parts[3]) if parts[3] not in ("-", "") else 0.0
        high   = float(parts[4]) if parts[4] not in ("-", "") else 0.0
        low    = float(parts[5]) if parts[5] not in ("-", "") else 0.0
        close  = float(parts[6]) if parts[6] not in ("-", "") else 0.0
    except ValueError:
        return None

    # If data is missing or zeroed, treat as not found
    if close == 0.0 and open_p == 0.0 and high == 0.0 and low == 0.0:
        return None

    prev_close = open_p if open_p else close
    symbol_out = raw_symbol.split(".")[0].upper()  # -> AAPL
    pct_change = ((close - prev_close) / prev_close * 100) if prev_close else 0.0

    return {
        "symbol": symbol_out,
        "current": round(close, 3),
        "prev_close": round(prev_close, 3),
        "high": round(high, 3),
        "low": round(low, 3),
        "open": round(open_p, 3),
        "percent_change": round(pct_change, 3),
        "volume": None,
        "raw": {
            "c": close,
            "pc": prev_close,
            "h": high,
            "l": low,
            "o": open_p,
        },
    }
















