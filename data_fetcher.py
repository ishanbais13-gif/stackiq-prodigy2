import json
from typing import Dict, Optional, Tuple
import requests

# Stooq is simple CSV. We'll try it first, then fall back to Yahoo Finance JSON.
STOOQ_URL = "https://stooq.com/q/l/?s={symbol}&i=d"
YF_URL = "https://query1.finance.yahoo.com/v7/finance/quote?symbols={symbol}"

HEADERS = {
    # Some providers block default python UA; use a browser-ish UA
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
}


def _normalize(symbol: str) -> str:
    """
    Normalize to Stooq format (lowercase + .us if no suffix).
    'AAPL' -> 'aapl.us', 'nvda' -> 'nvda.us', 'tsla.us' stays 'tsla.us'
    """
    s = (symbol or "").strip().lower()
    if not s:
        return ""
    if "." not in s:
        return f"{s}.us"
    return s


def _from_stooq(symbol: str) -> Tuple[Optional[Dict], Optional[str], Dict]:
    """Fetch from Stooq; return (data, error, debug_info)."""
    dbg = {"provider": "stooq", "url": "", "status_code": None, "body_sample": ""}
    sym = _normalize(symbol)
    if not sym:
        return None, "empty symbol", dbg

    url = STOOQ_URL.format(symbol=sym)
    dbg["url"] = url
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        dbg["status_code"] = r.status_code
        dbg["body_sample"] = r.text[:200] if r.text else ""
        if r.status_code != 200 or not r.text:
            return None, f"http {r.status_code}", dbg

        lines = [ln.strip() for ln in r.text.splitlines() if ln.strip()]
        if len(lines) < 2:
            return None, "no data lines", dbg

        parts = lines[1].split(",")
        if len(parts) < 8:
            return None, "bad csv format", dbg

        raw_symbol = parts[0]            # e.g., 'aapl.us'
        open_p = float(parts[3])
        high = float(parts[4])
        low = float(parts[5])
        close = float(parts[6])

        symbol_out = raw_symbol.split(".")[0].upper()
        prev_close = open_p  # Stooq daily line doesn't include prev close; use open as approximation.
        pct_change = ((close - prev_close) / prev_close * 100) if prev_close else 0.0

        data = {
            "symbol": symbol_out,
            "current": round(close, 3),
            "prev_close": round(prev_close, 3),
            "high": round(high, 3),
            "low": round(low, 3),
            "open": round(open_p, 3),
            "percent_change": round(pct_change, 3),
            "volume": None,
            "raw": {"c": close, "pc": prev_close, "h": high, "l": low, "o": open_p},
            "provider": "stooq",
        }
        return data, None, dbg
    except Exception as e:
        return None, f"exception: {type(e).__name__}: {e}", dbg


def _from_yahoo(symbol: str) -> Tuple[Optional[Dict], Optional[str], Dict]:
    """Fallback: Yahoo Finance JSON."""
    dbg = {"provider": "yahoo", "url": "", "status_code": None, "body_sample": ""}
    # Yahoo expects plain ticker for US (AAPL, MSFT). Strip any .suffix
    sym = (symbol or "").strip().upper().split(".")[0]
    if not sym:
        return None, "empty symbol", dbg

    url = YF_URL.format(symbol=sym)
    dbg["url"] = url
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        dbg["status_code"] = r.status_code
        dbg["body_sample"] = r.text[:200] if r.text else ""
        if r.status_code != 200:
            return None, f"http {r.status_code}", dbg

        payload = r.json()
        results = payload.get("quoteResponse", {}).get("result", [])
        if not results:
            return None, "no results", dbg

        q = results[0]
        close = q.get("regularMarketPrice")
        open_p = q.get("regularMarketOpen")
        high = q.get("regularMarketDayHigh")
        low = q.get("regularMarketDayLow")
        prev_close = q.get("regularMarketPreviousClose")

        if close is None or open_p is None or high is None or low is None or prev_close is None:
            return None, "incomplete fields", dbg

        pct_change = ((close - prev_close) / prev_close * 100) if prev_close else 0.0

        data = {
            "symbol": sym,
            "current": round(float(close), 3),
            "prev_close": round(float(prev_close), 3),
            "high": round(float(high), 3),
            "low": round(float(low), 3),
            "open": round(float(open_p), 3),
            "percent_change": round(float(pct_change), 3),
            "volume": q.get("regularMarketVolume"),
            "raw": {
                "c": float(close),
                "pc": float(prev_close),
                "h": float(high),
                "l": float(low),
                "o": float(open_p),
            },
            "provider": "yahoo",
        }
        return data, None, dbg
    except Exception as e:
        return None, f"exception: {type(e).__name__}: {e}", dbg


def fetch_quote(symbol: str) -> Optional[Dict]:
    """Try Stooq, then Yahoo. Return unified dict or None."""
    data, err, _ = _from_stooq(symbol)
    if data:
        return data
    # fallback
    data, err2, _ = _from_yahoo(symbol)
    if data:
        return data
    return None


def fetch_debug(symbol: str) -> Dict:
    """Return detailed debug info trying both providers."""
    out = {"symbol_input": symbol}

    data, err, dbg = _from_stooq(symbol)
    out["stooq"] = {"data_found": bool(data), "error": err, "debug": dbg}
    if data:
        out["result"] = data
        out["provider_used"] = "stooq"
        return out

    data2, err2, dbg2 = _from_yahoo(symbol)
    out["yahoo"] = {"data_found": bool(data2), "error": err2, "debug": dbg2}
    if data2:
        out["result"] = data2
        out["provider_used"] = "yahoo"
        return out

    out["result"] = None
    out["provider_used"] = None
    return out



















