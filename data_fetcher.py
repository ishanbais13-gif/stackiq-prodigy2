from __future__ import annotations
import time
import math
import yfinance as yf

# -----------------------------
# Tiny in-memory TTL cache
# -----------------------------
class TTLCache:
    def __init__(self):
        self.store = {}
    def get(self, key):
        v = self.store.get(key)
        if not v:
            return None
        expires, data = v
        if time.time() > expires:
            self.store.pop(key, None)
            return None
        return data
    def set(self, key, data, ttl):
        self.store[key] = (time.time() + ttl, data)

_cache = TTLCache()

def _safe_round(v):
    try:
        if v is None or (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
            return None
        return round(float(v), 2)
    except Exception:
        return None

def _quote_raw(symbol: str):
    t = yf.Ticker(symbol)
    fi = getattr(t, "fast_info", {}) or {}
    return {
        "last_price": fi.get("last_price"),
        "previous_close": fi.get("previous_close"),
        "day_high": fi.get("day_high"),
        "day_low": fi.get("day_low"),
        "open": fi.get("open"),
    }

def get_quote(symbol: str):
    key = f"quote:{symbol.upper()}"
    cached = _cache.get(key)
    if cached:
        return cached

    raw = _quote_raw(symbol)
    lp = raw.get("last_price")
    pc = raw.get("previous_close")
    pct = None
    if lp is not None and pc not in (None, 0):
        try:
            pct = round(((lp - pc) / pc) * 100, 2)
        except Exception:
            pct = None

    data = {
        "symbol": symbol.upper(),
        "current": _safe_round(lp),
        "prev_close": _safe_round(pc),
        "high": _safe_round(raw.get("day_high")),
        "low": _safe_round(raw.get("day_low")),
        "open": _safe_round(raw.get("open")),
        "percent_change": pct,
    }

    _cache.set(key, data, ttl=45)
    return data

def get_summary(symbol: str):
    key = f"summary:{symbol.upper()}"
    cached = _cache.get(key)
    if cached:
        return cached

    q = get_quote(symbol)
    trend = (
        "up" if (q["percent_change"] or 0) > 0
        else "down" if (q["percent_change"] or 0) < 0
        else "unchanged"
    )
    pct = abs(q["percent_change"]) if q["percent_change"] is not None else 0
    summary = (
        f"{symbol.upper()}: {q['current']} ({trend} {pct}% on the day). "
        f"Session range: {q['low']}–{q['high']}. Prev close {q['prev_close']}."
    )

    data = {"symbol": symbol.upper(), "summary": summary, "quote": q}
    _cache.set(key, data, ttl=45)
    return data

def get_history(symbol: str, range: str = "1M"):
    key = f"history:{symbol.upper()}:{range}"
    cached = _cache.get(key)
    if cached:
        return cached

    t = yf.Ticker(symbol)
    range = (range or "1M").upper()

    if range == "1D":
        df = t.history(period="1d", interval="15m")
        ttl = 60
    elif range == "3M":
        df, ttl = t.history(period="3mo", interval="1d"), 300
    elif range == "6M":
        df, ttl = t.history(period="6mo", interval="1d"), 300
    elif range == "1Y":
        df, ttl = t.history(period="1y", interval="1d"), 300
    elif range == "5Y":
        df, ttl = t.history(period="5y", interval="1wk"), 600
    else:
        df, ttl = t.history(period="1mo", interval="1d"), 300

    points = []
    if df is not None and len(df):
        for ts, row in df.iterrows():
            close = row.get("Close")
            if close is None or (isinstance(close, float) and (math.isnan(close) or math.isinf(close))):
                continue
            points.append({"t": int(ts.timestamp()), "c": round(float(close), 2)})

    data = {"symbol": symbol.upper(), "range": range, "points": points}
    _cache.set(key, data, ttl=ttl)
    return data



































