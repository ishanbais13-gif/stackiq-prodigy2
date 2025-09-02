import os
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "").strip()
FINNHUB_BASE = "https://finnhub.io/api/v1"


def _get(url: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not FINNHUB_API_KEY:
        return None
    p = dict(params)
    p["token"] = FINNHUB_API_KEY
    try:
        r = requests.get(url, params=p, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None


def fetch_quote(symbol: str) -> Optional[Dict[str, Any]]:
    """Return normalized quote for the symbol, or None on failure."""
    j = _get(f"{FINNHUB_BASE}/quote", {"symbol": symbol.upper()})
    if not j or "c" not in j:
        return None

    cur = float(j.get("c") or 0)
    prev = float(j.get("pc") or 0)
    pct = ((cur - prev) / prev * 100.0) if prev else 0.0

    return {
        "symbol": symbol.upper(),
        "current": cur,
        "prev_close": float(j.get("pc") or 0),
        "high": float(j.get("h") or 0),
        "low": float(j.get("l") or 0),
        "open": float(j.get("o") or 0),
        "percent_change": pct,
        "volume": j.get("v"),
        "raw": j,
    }


def _fetch_history_finnhub(symbol: str, start: datetime, end: datetime, resolution: str) -> List[Dict[str, Any]]:
    if not FINNHUB_API_KEY:
        return []
    fr = int(start.timestamp())
    to = int(end.timestamp())
    j = _get(
        f"{FINNHUB_BASE}/stock/candle",
        {"symbol": symbol.upper(), "resolution": resolution, "from": fr, "to": to},
    )
    if not j or j.get("s") != "ok":
        return []
    t = j.get("t") or []
    c = j.get("c") or []
    out = []
    for ts, close in zip(t, c):
        try:
            out.append({"t": int(ts), "c": float(close)})
        except Exception:
            pass
    return out


def _fetch_history_stooq(symbol: str, start: datetime, end: datetime) -> List[Dict[str, Any]]:
    """
    Public fallback without API key.
    Stooq daily CSV: https://stooq.com/q/d/l/?s=aapl&i=d
    Dates are UTC in YYYY-MM-DD.
    """
    url = "https://stooq.com/q/d/l/"
    # stooq wants lowercase + .us for US tickers; try both bare and .us
    candidates = [symbol.lower(), f"{symbol.lower()}.us"]
    for s in candidates:
        try:
            r = requests.get(url, params={"s": s, "i": "d"}, timeout=10)
            if r.status_code != 200 or not r.text or "Date,Open,High,Low,Close,Volume" not in r.text:
                continue
            lines = r.text.strip().splitlines()[1:]  # skip header
            out: List[Dict[str, Any]] = []
            start_date = start.date()
            end_date = end.date()
            for line in lines:
                parts = line.split(",")
                if len(parts) < 5:
                    continue
                d_str, _o, _h, _l, c_str = parts[:5]
                try:
                    d_obj = datetime.strptime(d_str, "%Y-%m-%d").date()
                    if d_obj < start_date or d_obj > end_date:
                        continue
                    close = float(c_str)
                    # convert date to unix (00:00 UTC)
                    ts = int(datetime(d_obj.year, d_obj.month, d_obj.day).timestamp())
                    out.append({"t": ts, "c": close})
                except Exception:
                    pass
            if out:
                return out
        except Exception:
            continue
    return []


def fetch_history(
    symbol: str,
    start: datetime,
    end: datetime,
    resolution: str = "D",
) -> List[Dict[str, Any]]:
    """
    Try Finnhub first; if empty, fallback to Stooq CSV.
    Always return a non-empty list if we can (falls back to last close from quote).
    """
    symbol = symbol.upper()

    # 1) Finnhub
    pts = _fetch_history_finnhub(symbol, start, end, resolution)
    if pts:
        return pts

    # 2) Stooq fallback
    pts = _fetch_history_stooq(symbol, start, end)
    if pts:
        return pts

    # 3) Last-resort: single point from quote so the chart renders
    q = fetch_quote(symbol)
    if q and q.get("current"):
        ts = int(time.time())
        return [{"t": ts, "c": float(q["current"])}]

    return []


































