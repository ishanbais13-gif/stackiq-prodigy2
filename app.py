import os
import time
import math
import logging
from typing import Any, Dict, List, Optional

import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

# -------------------------
# App & logging
# -------------------------
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("stackiq")

app = FastAPI(title="StackIQ API", version="0.2.2")

# -------------------------
# Constants & Keys
# -------------------------
FINNHUB_BASE = "https://finnhub.io/api/v1"
ALPHA_BASE   = "https://www.alphavantage.co/query"

FINNHUB_API_KEY     = os.getenv("FINNHUB_API_KEY", "").strip()
ALPHAVANTAGE_KEY    = os.getenv("ALPHAVANTAGE_KEY", "").strip()

# -------------------------
# Utilities
# -------------------------
def _require_key(value: str, name: str) -> None:
    if not value:
        raise HTTPException(status_code=502, detail=f"Missing {name} in environment")

def _get(url: str, params: Dict[str, Any], timeout: int = 15) -> Dict[str, Any]:
    """HTTP GET with basic error handling; raises HTTPException on problems."""
    try:
        r = requests.get(url, params=params, timeout=timeout)
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"Upstream request error: {e}")
    # Bubble specific upstream status
    if r.status_code == 403:
        # Helpful, clean error if a plan limitation hits
        raise HTTPException(status_code=403, detail="Upstream 403 (forbidden). Your plan may not allow this endpoint or time range.")
    try:
        r.raise_for_status()
    except requests.HTTPError as e:
        raise HTTPException(status_code=r.status_code, detail=f"Upstream HTTP {r.status_code}: {e}")
    try:
        return r.json()
    except ValueError:
        raise HTTPException(status_code=502, detail="Upstream returned non-JSON")

# -------------------------
# Alpha Vantage candles (Daily Adjusted)
# -------------------------
def av_daily_candles(symbol: str, outputsize: str = "compact") -> Dict[str, Any]:
    """
    Returns daily candles from Alpha Vantage (free-friendly).
    Output format matches typical OHLCV arrays:
      { symbol, t[], o[], h[], l[], c[], v[], source }
    """
    _require_key(ALPHAVANTAGE_KEY, "ALPHAVANTAGE_KEY")
    params = {
        "function": "TIME_SERIES_DAILY_ADJUSTED",
        "symbol": symbol.upper(),
        "outputsize": outputsize,  # "compact" ~100 bars | "full" full history
        "apikey": ALPHAVANTAGE_KEY,
    }
    data = _get(ALPHA_BASE, params)
    series = data.get("Time Series (Daily)")
    if not series:
        # Alpha Vantage often returns a "Note" when rate-limited
        note = data.get("Note") or data.get("Information") or data.get("Error Message")
        raise HTTPException(status_code=502, detail=f"Alpha Vantage response missing series: {note or 'unknown'}")

    # Sort by ascending date to build arrays
    rows = sorted(series.items())  # [(date_str, dict_fields), ...] oldest->newest
    t, o, h, l, c, v = [], [], [], [], [], []
    for date_str, fields in rows:
        # Convert date (YYYY-MM-DD) to UNIX timestamp at 00:00 UTC (approx)
        try:
            ts = int(time.mktime(time.strptime(date_str, "%Y-%m-%d")))
        except Exception:
            # If parsing fails, skip
            continue
        t.append(ts)
        o.append(float(fields["1. open"]))
        h.append(float(fields["2. high"]))
        l.append(float(fields["3. low"]))
        c.append(float(fields["4. close"]))
        v.append(float(fields["6. volume"]))

    return {"symbol": symbol.upper(), "t": t, "o": o, "h": h, "l": l, "c": c, "v": v, "source": "alpha_vantage"}

# -------------------------
# Finnhub quote
# -------------------------
def finnhub_quote(symbol: str) -> Dict[str, Any]:
    """
    Returns latest quote from Finnhub (works fine on free tier).
    Maps to a clean schema for the client.
    """
    _require_key(FINNHUB_API_KEY, "FINNHUB_API_KEY")
    params = {"symbol": symbol.upper(), "token": FINNHUB_API_KEY}
    data = _get(f"{FINNHUB_BASE}/quote", params)

    # Finnhub returns keys: c (current), d (change), dp (percent), h,l,o,pc,t
    if not all(k in data for k in ("c", "h", "l", "o", "pc")):
        raise HTTPException(status_code=502, detail="Unexpected quote payload")

    return {
        "symbol": symbol.upper(),
        "current": float(data.get("c", 0.0)),
        "change": float(data.get("d", 0.0)),
        "percent": float(data.get("dp", 0.0)),
        "high": float(data.get("h", 0.0)),
        "low": float(data.get("l", 0.0)),
        "open": float(data.get("o", 0.0)),
        "prev_close": float(data.get("pc", 0.0)),
        "timestamp": int(data.get("t", 0)) if data.get("t") else None,
        "source": "finnhub",
    }

# -------------------------
# Simple prediction (educational)
# -------------------------
def simple_momentum_pct(closes: List[float], lookback: int = 20) -> float:
    """
    A tiny educational momentum metric: percent difference of the
    latest close vs the simple moving average of the last N closes.
    """
    if not closes or len(closes) < max(2, lookback):
        return 0.0
    recent = closes[-lookback:]
    sma = sum(recent) / float(len(recent))
    last = closes[-1]
    if sma == 0:
        return 0.0
    return ((last - sma) / sma) * 100.0

# -------------------------
# Routes
# -------------------------
@app.get("/")
def root() -> Dict[str, Any]:
    return {"service": "StackIQ", "status": "ok"}

@app.get("/health")
def health() -> Dict[str, Any]:
    return {
        "status": "ok",
        "has_token": bool(FINNHUB_API_KEY or ALPHAVANTAGE_KEY),
        "service": "StackIQ",
        "version": app.version,
    }

@app.get("/quote/{symbol}")
def quote(symbol: str) -> Dict[str, Any]:
    return finnhub_quote(symbol)

@app.get("/candles/{symbol}")
def candles(symbol: str, outputsize: str = Query("compact", regex="^(compact|full)$")) -> Dict[str, Any]:
    """
    Always uses Alpha Vantage for daily candles to avoid Finnhub 403s on free plans.
    """
    return av_daily_candles(symbol, outputsize=outputsize)

@app.get("/predict/{symbol}")
def predict(symbol: str, budget: float = Query(1000.0, gt=0.0)) -> Dict[str, Any]:
    """
    Educational sample "plan":
      - Get live price from Finnhub
      - Get daily candles from Alpha Vantage (compact)
      - Compute simple momentum vs 20-day SMA
      - Suggest whole-share quantity within budget
    """
    # Live price
    q = finnhub_quote(symbol)
    price_now = q["current"] or q["prev_close"]
    if not price_now:
        raise HTTPException(status_code=502, detail="No price available for prediction")

    # Candles (for momentum)
    cd = av_daily_candles(symbol, outputsize="compact")
    closes = cd["c"]
    momentum_pct = simple_momentum_pct(closes, lookback=20)

    # Simple whole-share sizing
    shares = math.floor(budget / price_now)
    est_cost = round(shares * price_now, 2)

    return {
        "symbol": symbol.upper(),
        "price_now": round(price_now, 2),
        "momentum_pct": round(momentum_pct, 4),
        "using": {"quote": q["source"], "candles": cd["source"]},
        "buy_plan": {
            "budget": float(budget),
            "shares": int(shares),
            "estimated_cost": float(est_cost),
        },
        "note": "Educational sample strategy; not financial advice.",
    }

# -------------------------
# Local debug (optional)
# -------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")), reload=False)




























































