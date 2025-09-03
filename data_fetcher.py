# data_fetcher.py
from __future__ import annotations
import time
import math
from typing import Optional, Tuple, Dict, Any, List
import yfinance as yf

# -----------------------------
# Tiny in-memory TTL cache
# -----------------------------
class TTLCache:
    def __init__(self) -> None:
        self.store: Dict[str, Tuple[float, Any]] = {}

    def get(self, key: str):
        v = self.store.get(key)
        if not v:
            return None
        expires, data = v
        if time.time() > expires:
            self.store.pop(key, None)
            return None
        return data

    def set(self, key: str, data: Any, ttl: int) -> None:
        self.store[key] = (time.time() + ttl, data)

_cache = TTLCache()


# -----------------------------
# Helpers
# -----------------------------
def _to_float(v) -> Optional[float]:
    """Convert to float, guard NaN/Inf/None."""
    try:
        f = float(v)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except Exception:
        return None

def _r2(v: Optional[float]) -> Optional[float]:
    return None if v is None else round(v, 2)

def _fast_info_dict(t: yf.Ticker) -> Dict[str, Any]:
    """yfinance.fast_info can vary across versions; make it a plain dict."""
    try:
        fi = t.fast_info
        # Many versions are Mapping-like; dict(fi) is safest.
        d = dict(fi) if fi is not None else {}
    except Exception:
        d = {}
    return d

def _last_two_closes(t: yf.Ticker) -> Tuple[Optional[float], Optional[float]]:
    """Fallback: get last & previous Close from recent daily history."""
    try:
        df = t.history(period="5d", interval="1d", auto_adjust=False, actions=False)
        if df is None or df.empty:
            return None, None
        closes = df.get("Close")
        if closes is None or closes.empty:
            # Some environments prefer 'Adj Close'
            closes = df.get("Adj Close")
            if closes is None or closes.empty:
                return None, None
        vals = [ _to_float(x) for x in list(closes.dropna()) ]
        vals = [x for x in vals if x is not None]
        if not vals:
            return None, None
        last = vals[-1]
        prev = vals[-2] if len(vals) > 1 else None
        return last, prev
    except Exception:
        return None, None

def _session_hilo_open(t: yf.Ticker) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """Fallback: today's High/Low/Open from daily bar."""
    try:
        df = t.history(period="1d", interval="1d", auto_adjust=False, actions=False)
        if df is None or df.empty:
            return None, None, None
        row = df.iloc[-1]
        hi = _to_float(row.get("High"))
        lo = _to_float(row.get("Low"))
        op = _to_float(row.get("Open"))
        return hi, lo, op
    except Exception:
        return None, None, None


# -----------------------------
# Public API used by app.py
# -----------------------------
def get_quote(symbol: str) -> Dict[str, Any]:
    key = f"quote:{symbol.upper()}"
    cached = _cache.get(key)
    if cached:
        return cached

    t = yf.Ticker(symbol)
    fi = _fast_info_dict(t)

    # Try fast_info first
    last = _to_float(fi.get("last_price"))
    prev = _to_float(fi.get("previous_close"))
    hi   = _to_float(fi.get("day_high"))
    lo   = _to_float(fi.get("day_low"))
    op   = _to_float(fi.get("open"))

    # Fallbacks from history if missing
    if last is None or prev is None:
        last2, prev2 = _last_two_closes(t)
        if last is None: last = last2
        if prev is None: prev = prev2

    if hi is None or lo is None or op is None:
        hi2, lo2, op2 = _session_hilo_open(t)
        if hi is None: hi = hi2
        if lo is None: lo = lo2
        if op is None: op = op2

    pct = None
    if last is not None and (prev is not None) and prev != 0:
        try:
            pct = round(((last - prev) / prev) * 100, 2)
        except Exception:
            pct = None

    data = {
        "symbol": symbol.upper(),
        "current": _r2(last),
        "prev_close": _r2(prev),
        "high": _r2(hi),
        "low": _r2(lo),
        "open": _r2(op),
        "percent_change": pct,
    }

    _cache.set(key, data, ttl=45)  # 45 seconds
    return data


def get_summary(symbol: str) -> Dict[str, Any]:
    key = f"summary:{symbol.upper()}"
    cached = _cache.get(key)
    if cached:
        return cached

    q = get_quote(symbol)
    pc = q.get("percent_change")
    trend = "unchanged"
    if isinstance(pc, (int, float)):
        if pc > 0: trend = "up"
        elif pc < 0: trend = "down"

    pct_abs = abs(pc) if isinstance(pc, (int, float)) else 0
    summary = (
        f"{symbol.upper()}: {q.get('current')} ({trend} {pct_abs}% on the day). "
        f"Session range: {q.get('low')}–{q.get('high')}. Prev close {q.get('prev_close')}."
    )

    data = {"symbol": symbol.upper(), "summary": summary, "quote": q}
    _cache.set(key, data, ttl=45)  # 45 seconds
    return data


def get_history(symbol: str, range: str = "1M") -> Dict[str, Any]:
    key = f"history:{symbol.upper()}:{(range or '1M').upper()}"
    cached = _cache.get(key)
    if cached:
        return cached

    t = yf.Ticker(symbol)
    r = (range or "1M").upper()

    # Choose interval + TTL
    ttl = 300
    if r == "1D":
        df = t.history(period="1d", interval="15m", auto_adjust=False, actions=False)
        ttl = 60
    elif r == "1M":
        df = t.history(period="1mo", interval="1d", auto_adjust=False, actions=False)
        ttl = 300
    elif r == "3M":
        df = t.history(period="3mo", interval="1d", auto_adjust=False, actions=False)
        ttl = 300
    elif r == "6M":
        df = t.history(period="6mo", interval="1d", auto_adjust=False, actions=False)
        ttl = 300
    elif r == "1Y":
        df = t.history(period="1y", interval="1d", auto_adjust=False, actions=False)
        ttl = 300
    elif r == "5Y":
        df = t.history(period="5y", interval="1wk", auto_adjust=False, actions=False)
        ttl = 600
    else:
        # default
        df = t.history(period="1mo", interval="1d", auto_adjust=False, actions=False)
        ttl = 300

    points: List[Dict[str, Any]] = []
    try:
        if df is not None and not df.empty:
            series = df.get("Close") if "Close" in df.columns else df.get("Adj Close")
            if series is not None:
                series = series.dropna()
                for ts, close in series.items():
                    c = _to_float(close)
                    if c is None:
                        continue
                    # handle tz-aware index
                    try:
                        epoch = int(getattr(ts, "timestamp", lambda: ts.to_pydatetime().timestamp())())
                    except Exception:
                        epoch = int(time.mktime(ts.timetuple()))
                    points.append({"t": epoch, "c": round(c, 2)})
    except Exception:
        # leave points empty on failure
        points = []

    data = {"symbol": symbol.upper(), "range": r, "points": points}
    _cache.set(key, data, ttl=ttl)
    return data




































