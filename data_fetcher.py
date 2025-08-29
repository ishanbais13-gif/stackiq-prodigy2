import requests

# Stooq CSV endpoint. We'll try both TLDs just in case one is flaky.
_STOOQ_URLS = [
    "https://stooq.com/q/l/?s={symbol}&i=d",
    "https://stooq.pl/q/l/?s={symbol}&i=d",
]

# Normalize a user symbol into what Stooq expects (e.g., aapl.us)
def _normalize(symbol: str) -> str:
    s = (symbol or "").strip().lower()
    if not s:
        return ""
    # If user didn't include a suffix (like .us / .uk etc), assume US
    if "." not in s:
        s = f"{s}.us"
    return s

def _try_fetch_csv(symbol_norm: str) -> str | None:
    headers = {
        "User-Agent": "stackiq/1.0 (+https://example.com)"
    }
    for url_tpl in _STOOQ_URLS:
        url = url_tpl.format(symbol=symbol_norm)
        try:
            r = requests.get(url, headers=headers, timeout=10)
            if r.status_code == 200 and r.text:
                return r.text
        except Exception:
            continue
    return None

def fetch_quote(symbol: str) -> dict | None:
    sym_norm = _normalize(symbol)
    if not sym_norm:
        return None

    csv_text = _try_fetch_csv(sym_norm)
    if not csv_text:
        return None

    # CSV format (header then one line):
    # Symbol,Date,Time,Open,High,Low,Close,Volume
    lines = [ln.strip() for ln in csv_text.splitlines() if ln.strip()]
    if len(lines) < 2:
        return None

    parts = lines[1].split(",")
    if len(parts) < 8:
        return None

    raw_symbol = parts[0]            # e.g., aapl.us
    open_p = float(parts[3])
    high = float(parts[4])
    low = float(parts[5])
    close = float(parts[6])
    # Stooq daily row doesn't give "prev close" directly; use open as a proxy
    prev_close = open_p

    symbol_out = raw_symbol.split(".")[0].upper()  # AAPL
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


















