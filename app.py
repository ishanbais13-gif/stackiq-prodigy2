# app.py  — StackIQ FastAPI backend
# - CORS enabled (safe defaults)
# - Tiny per-IP throttle (1 req/sec)
# - Light cache headers for static files
# - Simple client-side friendly JSON responses
# - Static / UI served from /static/* and root "/"

import os
import time
import logging
from typing import Any, Dict

import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse, HTMLResponse, FileResponse, Response
from starlette.requests import Request

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("stackiq")

FINNHUB_BASE = "https://finnhub.io/api/v1"
AV_BASE = "https://www.alphavantage.co/query"

FINNHUB_API_KEY = (os.getenv("FINNHUB_API_KEY") or "").strip()
ALPHAVANTAGE_KEY = (os.getenv("ALPHAVANTAGE_KEY") or "").strip()

VERSION = "0.2.3"

app = FastAPI(title="StackIQ API", version=VERSION)

# ----------------------------- CORS ---------------------------------
# Allow same-origin app + common local dev origins.
# If you later host the UI elsewhere, add that origin here.
ALLOWED_ORIGINS = [
    "https://stackiq-web-btbfdxekdfhrcaey.centralus-01.azurewebsites.net",
    "http://localhost:3000",
    "http://localhost:5173",
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# --------------------------------------------------------------------

# -------------- Tiny per-IP throttle + static cache headers ----------
_last_hit: Dict[str, float] = {}  # ip -> last timestamp (seconds)

@app.middleware("http")
async def throttle_and_cache_headers(request: Request, call_next):
    # throttle per IP (protects free-tier quotas during rapid clicking)
    ip = request.client.host if request.client else "unknown"
    now = time.time()
    last = _last_hit.get(ip, 0.0)
    if now - last < 1.0:  # ~1 request per second per IP
        return Response("Too Many Requests - slow down a bit", status_code=429)
    _last_hit[ip] = now

    response = await call_next(request)

    # light client caching for static assets
    p = request.url.path or ""
    if p.startswith("/static/"):
        if p.endswith(".js") or p.endswith(".css"):
            response.headers["Cache-Control"] = "public, max-age=90"
        elif p.endswith(".html"):
            response.headers["Cache-Control"] = "no-cache"

    return response
# --------------------------------------------------------------------


def _alpha_raise_if_note(data: Dict[str, Any]) -> None:
    note = data.get("Note") or data.get("Information") or data.get("Error Message")
    if note:
        # 429 is used so callers can detect quota/premium quickly
        raise HTTPException(status_code=429, detail=f"Alpha Vantage: {note}")


def _get_json(url: str, params: Dict[str, Any], timeout: int = 15) -> Dict[str, Any]:
    r = requests.get(url, params=params, timeout=timeout)
    # Clean explanation for Finnhub 403s
    if r.status_code == 403 and "finnhub" in url:
        raise HTTPException(
            status_code=403,
            detail="Finnhub 403 (forbidden). Your key/plan may not allow this endpoint or range.",
        )
    r.raise_for_status()
    return r.json()


def _alpha_daily(symbol: str) -> Dict[str, Any]:
    if not ALPHAVANTAGE_KEY:
        raise HTTPException(status_code=502, detail="Missing ALPHAVANTAGE_KEY")

    data = _get_json(
        AV_BASE,
        {
            "function": "TIME_SERIES_DAILY",
            "symbol": symbol.upper(),
            "apikey": ALPHAVANTAGE_KEY,
            "outputsize": "compact",  # free plan: last ~100 bars
        },
    )
    _alpha_raise_if_note(data)
    return data


def _finnhub_quote(symbol: str) -> Dict[str, Any]:
    if not FINNHUB_API_KEY:
        raise HTTPException(status_code=502, detail="Missing FINNHUB_API_KEY")

    try:
        q = _get_json(
            f"{FINNHUB_BASE}/quote",
            {"symbol": symbol.upper(), "token": FINNHUB_API_KEY},
        )
        # expected keys: c (current), d (change), dp (percent), h (high), l (low), o (open), pc (prev close), t (ts)
        for k in ("c", "d", "dp"):
            if k not in q:
                raise HTTPException(status_code=502, detail="Unexpected Finnhub quote payload")
        return q
    except HTTPException:
        raise
    except Exception as e:
        log.exception("Error calling Finnhub")
        raise HTTPException(status_code=502, detail=f"Finnhub error: {e}")


@app.get("/", response_class=JSONResponse)
def root():
    return {"service": "StackIQ", "status": "ok"}


@app.get("/health", response_class=JSONResponse)
def health():
    return {"status": "ok", "has_token": bool(FINNHUB_API_KEY or ALPHAVANTAGE_KEY), "service": "StackIQ", "version": VERSION}


@app.get("/quote/{symbol}", response_class=JSONResponse)
def quote(symbol: str):
    """
    Live quote via Finnhub. Shape is UI-friendly.
    """
    q = _finnhub_quote(symbol)
    return {
        "symbol": symbol.upper(),
        "current": q.get("c", 0),
        "change": q.get("d"),
        "percent": q.get("dp"),
        "high": q.get("h", 0),
        "low": q.get("l", 0),
        "open": q.get("o", 0),
        "prev_close": q.get("pc", 0),
        "timestamp": q.get("t", 0),
    }


@app.get("/predict/{symbol}", response_class=JSONResponse)
def predict(symbol: str, budget: float = Query(1000, ge=0)):
    """
    Simple demo strategy:
    - Try daily candles from Alpha Vantage for momentum calc.
    - If AV throttles/blocks (429), fall back to Finnhub quote only.
    """
    try:
        av = _alpha_daily(symbol)
        c = av.get("Time Series (Daily)", {}) or av.get("Time Series Daily", {})
        bars = [c[k] for k in sorted(c.keys())][-10:]  # last 10
        closes = [float(bar["4. close"]) for bar in bars] if bars else []
        if len(closes) < 2:
            raise ValueError("not enough data")

        momentum = (closes[-1] - closes[0]) / closes[0] * 100.0
        price_now = closes[-1]
        shares = int(budget // price_now) if price_now else 0

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
        # If the exception came from alpha (429), degrade gracefully to quote-only
        if e.status_code == 429:
            q = _finnhub_quote(symbol)
            price_now = q.get("c", 0.0)
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


# ---- static + index route ----
# Serve /static/* from the ./static folder
app.mount("/static", StaticFiles(directory="static"), name="static")

# Serve the demo UI at the site root
@app.get("/", response_class=HTMLResponse)
def serve_index():
    return FileResponse("static/index.html")
# ---- end static + index ----































































