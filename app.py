# app.py
import os
import time
import logging
from typing import Any, Dict, List, Tuple

import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse, HTMLResponse, FileResponse

# -----------------------------------------------------------------------------
# Config & logging
# -----------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("stackiq")

VERSION = "0.2.2"

FINNHUB_BASE = "https://finnhub.io/api/v1"
AV_BASE = "https://www.alphavantage.co/query"

FINNHUB_API_KEY = (os.getenv("FINNHUB_API_KEY") or "").strip()
ALPHAVANTAGE_KEY = (os.getenv("ALPHAVANTAGE_KEY") or "").strip()

# -----------------------------------------------------------------------------
# Small in-memory cache (to protect free-tier limits)
# -----------------------------------------------------------------------------
# key -> (expires_at_epoch_ms, data)
_cache: Dict[str, Tuple[int, Any]] = {}

def cache_get(key: str) -> Any | None:
    now = int(time.time() * 1000)
    hit = _cache.get(key)
    if not hit:
        return None
    if hit[0] > now:
        return hit[1]
    # expired
    _cache.pop(key, None)
    return None

def cache_set(key: str, data: Any, ttl_seconds: int = 90) -> None:
    _cache[key] = (int(time.time() * 1000) + ttl_seconds * 1000, data)

def _req_key(url: str, params: Dict[str, Any]) -> str:
    items = "&".join(f"{k}={params[k]}" for k in sorted(params))
    return f"{url}?{items}"

# -----------------------------------------------------------------------------
# FastAPI app
# -----------------------------------------------------------------------------
app = FastAPI(title="StackIQ API", version=VERSION)

# CORS (safe default; tighten allow_origins to your UI origin later)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------------------------------------------------------
# Helper: HTTP fetch with caching and friendly errors
# -----------------------------------------------------------------------------
def _alpha_raise_if_note(data: Dict[str, Any]) -> None:
    note = data.get("Note") or data.get("Information") or data.get("Error Message")
    if note:
        # 429 is used so callers can detect quota/premium quickly
        raise HTTPException(status_code=429, detail=f"Alpha Vantage: {note}")

def get_json(url: str, params: Dict[str, Any], timeout: int = 15, ttl: int = 90) -> Dict[str, Any]:
    key = _req_key(url, params)
    cached = cache_get(key)
    if cached is not None:
        return cached  # cached dict

    try:
        r = requests.get(url, params=params, timeout=timeout)
        # Special-case friendly message for common Finnhub 403 while testing
        if r.status_code == 403 and "finnhub" in url:
            raise HTTPException(
                status_code=403,
                detail="Finnhub 403 (forbidden). Your key/plan may not allow this endpoint or range.",
            )
        r.raise_for_status()
        data = r.json()
        cache_set(key, data, ttl_seconds=ttl)
        return data
    except HTTPException:
        raise
    except Exception as e:
        log.exception("HTTP error")
        raise HTTPException(status_code=502, detail=f"Upstream error: {e}")

# -----------------------------------------------------------------------------
# Data sources
# -----------------------------------------------------------------------------
def alpha_daily(symbol: str) -> Dict[str, Any]:
    if not ALPHAVANTAGE_KEY:
        raise HTTPException(status_code=502, detail="Missing ALPHAVANTAGE_KEY")
    data = get_json(
        AV_BASE,
        {
            "function": "TIME_SERIES_DAILY",
            "symbol": symbol.upper(),
            "apikey": ALPHAVANTAGE_KEY,
            "outputsize": "compact",  # ~last 100 bars
        },
        ttl=90,
    )
    _alpha_raise_if_note(data)
    return data

def finnhub_quote(symbol: str) -> Dict[str, Any]:
    if not FINNHUB_API_KEY:
        # fall back to zero quote (friendly)
        return {
            "symbol": symbol.upper(),
            "current": 0,
            "change": None,
            "percent": None,
            "high": 0,
            "low": 0,
            "open": 0,
            "prev_close": 0,
            "timestamp": 0,
        }
    data = get_json(
        f"{FINNHUB_BASE}/quote",
        {"symbol": symbol.upper(), "token": FINNHUB_API_KEY},
        ttl=60,
    )
    # Finnhub quote response shape: c (current), d (change), dp (%), h, l, o, pc, t
    return {
        "symbol": symbol.upper(),
        "current": data.get("c") or 0,
        "change": data.get("d"),
        "percent": data.get("dp"),
        "high": data.get("h") or 0,
        "low": data.get("l") or 0,
        "open": data.get("o") or 0,
        "prev_close": data.get("pc") or 0,
        "timestamp": data.get("t") or 0,
    }

# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------
@app.get("/health")
def health():
    return {
        "status": "ok",
        "has_token": bool(FINNHUB_API_KEY or ALPHAVANTAGE_KEY),
        "service": "StackIQ",
        "version": VERSION,
    }

@app.get("/quote/{symbol}")
def quote(symbol: str):
    return finnhub_quote(symbol)

@app.get("/predict/{symbol}")
def predict(symbol: str, budget: float = Query(..., gt=0, description="USD budget to allocate")):
    """
    Simple demo strategy:
    - Try daily candles from Alpha Vantage for momentum calc.
    - If AV throttles/blocks (429), degrade gracefully to quote-only plan.
    """
    try:
        av = alpha_daily(symbol)
        c = av.get("Time Series (Daily)") or {}
        # last N closes (newest first after sorting)
        bars: List[Dict[str, str]] = [
            {"close": float(v["4. close"])} for k, v in sorted(c.items(), reverse=True)
        ]
        closes = [bar["close"] for bar in bars[:10]]
        if len(closes) < 2:
            raise ValueError("not enough data")

        momentum = (closes[0] - closes[-1]) / closes[-1] * 100.0
        price_now = closes[0]
        shares = int(budget // price_now)

        return {
            "symbol": symbol.upper(),
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

    except HTTPException as e:
        # If Alpha Vantage rate-limits, degrade gracefully to quote-only
        if e.status_code == 429:
            q = finnhub_quote(symbol)
            price_now = q.get("current") or 0.0
            shares = int(budget // price_now) if price_now else 0
            return {
                "symbol": symbol.upper(),
                "price_now": round(price_now or 0.0, 2),
                "momentum_pct": None,
                "using": "quote_only_fallback",
                "plan_hint": str(e.detail),
                "buy_plan": {
                    "budget": budget,
                    "shares": shares,
                    "estimated_cost": round(shares * (price_now or 0.0), 2),
                },
                "note": "Educational sample strategy; not financial advice.",
            }
        raise
    except Exception as e:
        log.exception("predict failed")
        raise HTTPException(status_code=500, detail=f"predict failed: {e}")

# -----------------------------------------------------------------------------
# Static files + root UI
# -----------------------------------------------------------------------------
# Serve /static/* from the ./static folder (cache handled by the browser)
app.mount("/static", StaticFiles(directory="static"), name="static")

# Serve the demo UI at site root
@app.get("/", response_class=HTMLResponse)
def root():
    # NOTE: This expects ./static/index.html to exist (we created it in repo)
    return FileResponse("static/index.html")

# -----------------------------------------------------------------------------
# Uvicorn entry (not used by Azure if you set a Startup Command, but harmless)
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    # Bind to all interfaces; PORT env is honored if present (Azure sets it)
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("app:app", host="0.0.0.0", port=port)

































































