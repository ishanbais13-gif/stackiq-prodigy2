import requests

# Use the explicit CSV format Stooq documents
STOOQ_URL = "https://stooq.com/q/l/?s={symbol}&f=sd2t2ohlcv&h&e=csv"
HEADERS = {
    # Many hosts throttle/block default Python UA. Pretend to be a browser.
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0 Safari/537.36 StackIQ/1.0"
}

def _normalize(symbol: str) -> str:
    """
    Stooq expects tickers like aapl.us (lowercase). If the user types AAPL or aapl,
    convert to aapl.us. If a suffix already exists (e.g., .us, .gb), keep it.
    """
    s = (symbol or "").strip().lower()
    if not s:
        return ""
    if "." in s:      # already has a market suffix
        return s
    return f"{s}.us"  # default to US market

def _parse_csv(text: str):
    # Text should be two lines: header and data
    lines = [ln.strip() for ln in (text or "").splitlines() if ln.strip()]
    if len(lines) < 2:
        return None, "no-data"
    parts = lines[1].split(",")
    if len(parts) < 8:
        return None, "bad-csv"

    raw_symbol = parts[0]        # e.g., aapl.us
    date_s     = parts[1]        # not used
    time_s     = parts[2]        # not used
    open_s     = parts[3]
    high_s     = parts[4]
    low_s      = parts[5]
    close_s    = parts[6]
    vol_s      = parts[7]

    # Stooq uses "N/D" for missing
    if any(v in ("N/D", "", None) for v in (open_s, high_s, low_s, close_s)):
        return None, "nd"  # no data

    try:
        open_p  = float(open_s)
        high    = float(high_s)
        low     = float(low_s)
        close   = float(close_s)
    except Exception:
        return None, "parse"

    symbol_out = raw_symbol.split(".")[0].upper()
    prev_close = open_p  # Stooq daily CSV doesn't include previous close; use open as approx
    pct_change = ((close - prev_close) / prev_close * 100.0) if prev_close else 0.0

    data = {
        "symbol": symbol_out,
        "current": round(close, 3),
        "prev_close": round(prev_close, 3),
        "high": round(high, 3),
        "low": round(low, 3),
        "open": round(open_p, 3),
        "percent_change": round(pct_change, 3),
        "volume": None if vol_s == "N/D" else vol_s,
        "raw": {"c": close, "pc": prev_close, "h": high, "l": low, "o": open_p},
    }
    return data, None

def fetch_quote(symbol: str):
    sym_norm = _normalize(symbol)
    if not sym_norm:
        return None, "empty-symbol"

    url = STOOQ_URL.format(symbol=sym_norm)
    try:
        r = requests.get(url, headers=HEADERS, timeout=8)
    except Exception as e:
        return None, f"request-error:{type(e).__name__}"

    if r.status_code != 200 or not r.text:
        return None, f"http-{r.status_code}"

    data, perr = _parse_csv(r.text)
    if not data:
        return None, perr
    return data, None

def fetch_debug(symbol: str):
    sym_norm = _normalize(symbol)
    url = STOOQ_URL.format(symbol=sym_norm) if sym_norm else None
    info = {"normalized": sym_norm, "url": url}

    if not url:
        info["error"] = "empty-symbol"
        return info

    try:
        r = requests.get(url, headers=HEADERS, timeout=8)
        info["http_status"] = r.status_code
        info["text_sample"] = (r.text or "")[:500]
        data, perr = _parse_csv(r.text) if r.status_code == 200 else (None, f"http-{r.status_code}")
        info["parsed_ok"] = bool(data)
        if perr:
            info["parse_error"] = perr
    except Exception as e:
        info["error"] = f"request-error:{type(e).__name__}"
    return info




















