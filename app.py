 # app.py
import math
from typing import Dict, Any, List

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

import yfinance as yf

app = FastAPI(title="StackIQ")

# allow your static site to call the API from /web
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def root():
    return {"status": "ok", "msg": "StackIQ backend is live."}


@app.get("/health")
def health():
    return {"ok": True}


def _safe_float(x) -> float:
    try:
        if x is None or (isinstance(x, float) and math.isnan(x)):
            return None
        return float(x)
    except Exception:
        return None


def _make_price_block(ticker: str) -> Dict[str, Any]:
    """
    Build the exact object your frontend expects:

    {
      "ticker": "AAPL",
      "price": {"c": ..., "d": ..., "dp": ..., "h": ..., "l": ..., "o": ..., "pc": ..., "v": ...}
    }
    """
    tk = yf.Ticker(ticker)
    # Use last 2 daily candles to compute current & previous close.
    hist = tk.history(period="5d", interval="1d", auto_adjust=False)

    if hist is None or hist.empty:
        raise HTTPException(status_code=404, detail="Ticker not found or no data")

    # Last row is "current day" (close is last official close, or current if market open with yfinance caching)
    last = hist.tail(1).iloc[0]
    c = _safe_float(last["Close"])
    o = _safe_float(last["Open"])
    h = _safe_float(last["High"])
    l = _safe_float(last["Low"])
    v = int(last["Volume"]) if not math.isnan(_safe_float(last["Volume"])) else None

    # Previous close (pc)
    if len(hist) >= 2:
        prev = hist.tail(2).iloc[0]
        pc = _safe_float(prev["Close"])
    else:
        pc = c

    d = _safe_float(c - pc) if (c is not None and pc is not None) else None
    dp = _safe_float((d / pc) * 100) if (d is not None and pc not in (None, 0)) else None

    return {
        "ticker": ticker.upper(),
        "price": {
            "c": c,
            "d": d,
            "dp": dp,
            "h": h,
            "l": l,
            "o": o,
            "pc": pc,
            "v": v,
        },
    }


def _make_earnings_block(ticker: str) -> Dict[str, List[Dict[str, Any]]]:
    """
    Return an 'earnings' object compatible with your UI.
    """
    tk = yf.Ticker(ticker)
    # Pull up to a few dates; if none, return empty list
    try:
        ed = tk.get_earnings_dates(limit=4)
    except Exception:
        ed = None

    items: List[Dict[str, Any]] = []
    if ed is not None and not ed.empty:
        # yfinance returns a DataFrame with columns like: "Earnings Date", "EPS Estimate", "Reported EPS", etc.
        # Normalize fields to match your earlier shape.
        for _, row in ed.reset_index().iterrows():
            item = {
                "date": str(row.get("Earnings Date") or row.get("index") or ""),
                "epsActual": _safe_float(row.get("Reported EPS")),
                "epsEstimate": _safe_float(row.get("EPS Estimate")),
                "hour": "amc",           # yfinance doesn’t always give this; placeholder
                "quarter": None,         # unknown; placeholder
                "revenueActual": None,   # unknown; placeholder
                "revenueEstimate": None, # unknown; placeholder
                "symbol": ticker.upper(),
                "year": None,
            }
            items.append(item)

    return {"earningsCalendar": items}


@app.get("/test/{ticker}")
def test_ticker(ticker: str, pretty: int | None = None):
    try:
        price = _make_price_block(ticker)
        earnings = _make_earnings_block(ticker)
        out = {**price, "earnings": earnings}
        return out
    except HTTPException:
        # bubble up not-found error as-is
        raise
    except Exception as e:
        # Any other error -> consistent JSON your UI can show
        return {"status": "error", "error": str(e)}







