import csv
import io
import requests

# Try both stooq domains and request the classic CSV with explicit fields
STOOQ_URLS = [
    "https://stooq.com/q/l/?s={sym}&f=sd2t2ohlcv&h&e=csv",
    "https://stooq.pl/q/l/?s={sym}&f=sd2t2ohlcv&h&e=csv",
]

UA = {
    "User-Agent": "Mozilla/5.0 (compatible; StackIQ/1.0; +https://example.com)"
}


def _normalize(symbol: str) -> str:
    """
    Normalize to stooq US ticker format.
      - lowercase
      - append '.us' if no region suffix present
    """
    s = (symbol or "").strip().lower()
    if not s:
        return ""
    if "." in s:
        return s
    return f"{s}.us"


def fetch_quote(symbol: str):
    sym_norm = _normalize(symbol)
    if not sym_norm:
        return None

    text = None
    # Try both URLs (and succeed fast)
    for url in STOOQ_URLS:
        try:
            r = requests.get(url.format(sym=sym_norm), headers=UA, timeout=10)
            if r.status_code == 200 and r.text and "Symbol" in r.text:
                text = r.text
                break
        except Exception:
            continue

    if not text:
        return None

    # Parse CSV
    try:
        reader = csv.DictReader(io.StringIO(text))
        row = next(reader, None)
        if not row:
            return None

        # Stooq returns 'N/D' when data isn't available
        def _num(v):
            if v is None:
                return None
            v = v.strip()
            if not v or v.upper() == "N/D":
                return None
            return float(v)

        raw_symbol = (row.get("Symbol") or "").upper()
        open_p = _num(row.get("Open"))
        high = _num(row.get("High"))
        low = _num(row.get("Low"))
        close = _num(row.get("Close"))

        if not raw_symbol or open_p is None or high is None or low is None or close is None:
            return None

        # Stooq CSV here does not include prior close; use open as an approximation
        prev_close = open_p

        current = close
        pct_change = ((current - prev_close) / prev_close * 100) if prev_close else 0.0

        return {
            "symbol": raw_symbol.split(".")[0],  # e.g., AAPL from AAPL.US
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















