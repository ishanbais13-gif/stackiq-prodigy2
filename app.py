# app.py
import os
import time
from typing import Any, Dict, Optional

import requests
from fastapi import FastAPI, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse

# ----------------------------
# Config & helpers
# ----------------------------
SERVICE_NAME = "StackIQ"
SERVICE_VERSION = "0.2.2"

# Env keys (accept common variants so we don't get tripped up)
ALPHAVANTAGE_KEY = (
    os.getenv("ALPHAVANTAGE_KEY")
    or os.getenv("ALPHAVANTAGE_API_KEY")
    or ""
)
FINNHUB_KEY = (
    os.getenv("FINNHUB_API_KEY")
    or os.getenv("FINNHUB_APIKEY")
    or ""
)

ALPHA_TIMEOUT = 2.5  # seconds (keep it snappy to avoid 504s)

app = FastAPI(title=SERVICE_NAME)

# Serve the demo UI (your index.html lives in ./static)
if os.path.isdir("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")


def _pretty_json(data: Dict[str, Any], pretty: bool) -> JSONResponse:
    """
    Return consistent JSON; when pretty=1, indent for readability
    (handy for your in-browser testing and the demo UI).
    """
    if pretty:
        return JSONResponse(content=data, media_type="application/json", indent=2)
    return JSONResponse(content=data, media_type="application/json")


def _safe_float(x: Any) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0


# ----------------------------
# External data sources
# ----------------------------
def finnhub_quote(symbol: str) -> Dict[str, Any]:
    """Fetch real-time-ish quote from Finnhub and normalize the shape."""
    symbol = symbol.upper()
    url = "https://finnhub.io/api/v1/quote"
    params = {"symbol": symbol, "token": FINNHUB_KEY}
    r = requests.get(url, params=params, timeout=4.0)
    r.raise_for_status()
    j = r.json() or {}

    current = _safe_float(j.get("c"))
    prev_close = _safe_float(j.get("pc"))
    change = current - prev_close if (current and prev_close) else 0.0
    percent = (change / prev_close * 100.0) if prev_close else 0.0

    return {
        "symbol": symbol,
        "current": round(current, 2),
        "change": round(change, 2) if prev_close else None,
        "percent": round(percent, 4) if prev_close else None,
        "high": _safe_float(j.get("h")),
        "low": _safe_float(j.get("l")),
        "open": _safe_float(j.get("o")),
        "prev_close": round(prev_close, 2) if prev_close else 0.0,
        "timestamp": int(time.time()),
    }


def _alpha_daily_closes(symbol: str) -> list[float]:
    """
    Get latest daily closes (newest first) from Alpha Vantage.
    Fast timeout so we never hang the request.
    """
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_DAILY_ADJUSTED",
        "symbol": symbol,
        "outputsize": "compact",
        "apikey": ALPHAVANTAGE_KEY,
    }
    r = requests.get(url, params=params, timeout=ALPHA_TIMEOUT)
    r.raise_for_status()
    data = r.json() or {}
    series = data.get("Time Series (Daily)") or {}
    if not series:
        raise RuntimeError("alpha: empty series")

    # newest first (Alpha keys are ISO dates)
    closes = [float(v["4. close"]) for k, v in sorted(series.items(), reverse=True)[:10]]
    if len(closes) < 2:
        raise RuntimeError("alpha: not enough data")
    return closes


# ----------------------------
# Routes
# ----------------------------
@app.get("/")
def root(pretty: int = Query(0, description="Set to 1 for pretty JSON")):
    data = {"service": SERVICE_NAME, "status": "ok"}
    return _pretty_json(data, pretty == 1)


@app.get("/health")
def health(pretty: int = Query(0, description="Set to 1 for pretty JSON")):
    data = {
        "status": "ok",
        "has_token": bool(ALPHAVANTAGE_KEY or FINNHUB_KEY),
        "service": SERVICE_NAME,
        "version": SERVICE_VERSION,
    }
    return _pretty_json(data, pretty == 1)


@app.get("/quote/{symbol}")
def quote(symbol: str, pretty: int = Query(0, description="Set to 1 for pretty JSON")):
    """
    Normalize quote response:
      {
        "symbol": "AAPL",
        "current": 247.66,
        "change": 2.39,
        "percent": 0.9744,
        "high": 249.69,
        "low": 245.56,
        "open": 249.38,
        "prev_close": 245.27,
        "timestamp": 1706385600
      }
    """
    try:
        data = finnhub_quote(symbol)
    except Exception:
        # Keep shape stable, but indicate failure with zeros/nulls
        sym = symbol.upper()
        data = {
            "symbol": sym,
            "current": 0.0,
            "change": None,
            "percent": None,
            "high": 0.0,
            "low": 0.0,
            "open": 0.0,
            "prev_close": 0.0,
            "timestamp": 0,
        }
    return _pretty_json(data, pretty == 1)


@app.get("/predict/{symbol}")
def predict(
    symbol: str,
    budget: float = Query(1000.0, ge=0.0),
    pretty: int = Query(0, description="Set to 1 for pretty JSON"),
):
    """
    Build a simple 'buy plan':
      - Try Alpha Vantage daily closes to compute momentum and use the latest close as price.
      - If Alpha is slow/throttled/errored, fallback to Finnhub quote and skip momentum.
    """
    symbol = symbol.upper()
    try:
        closes = _alpha_daily_closes(symbol)
        price_now = closes[0]
        momentum = (closes[0] - closes[-1]) / closes[-1] * 100.0
        shares = int(budget // price_now) if price_now else 0
        data = {
            "symbol": symbol,
            "price_now": round(price_now, 2),
            "momentum_pct": round(momentum, 4),
            "using": "alpha_candles",
            "buy_plan": {
                "budget": budget,
                "shares": shares,
                "estimated_cost": round(shares * price_now, 2),
            },
            "note": "Educational sample strategy; not financial advice.",
        }
        return _pretty_json(data, pretty == 1)

    except Exception as e:
        # Any error/timeout → fast, graceful fallback to Finnhub to avoid 504s
        try:
            q = finnhub_quote(symbol)
            price_now = q.get("current") or 0.0
        except Exception:
            price_now = 0.0

        shares = int(budget // price_now) if price_now else 0
        data = {
            "symbol": symbol,
            "price_now": round(price_now, 2) if price_now else 0.0,
            "momentum_pct": None,
            "using": "quote_only_fallback",
            "plan_hint": str(e),
            "buy_plan": {
                "budget": budget,
                "shares": shares,
                "estimated_cost": round(shares * (price_now or 0.0), 2),
            },
            "note": "Educational sample strategy; not financial advice.",
        }
        return _pretty_json(data, pretty == 1)


































































