# app.py — StackIQ API
# FastAPI app with CORS, logging, Finnhub-backed endpoints, and a baseline /predict.

from fastapi import FastAPI, HTTPException, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from statistics import mean
import logging, time, os

# Import your Finnhub client helpers
from data_fetcher import (
    get_quote,
    get_candles,
    get_company_profile,
)

# -------------------
# Config & CORS
# -------------------
def _parse_origins() -> list[str]:
    """
    Read allowed origins from env CORS_ALLOW_ORIGINS (comma-separated).
    Defaults to ["*"] for dev; tighten in prod:
      CORS_ALLOW_ORIGINS=https://your-frontend.com,http://localhost:3000
    """
    raw = os.getenv("CORS_ALLOW_ORIGINS", "").strip()
    if not raw:
        return ["*"]
    return [o.strip() for o in raw.split(",") if o.strip()]

USE_SANDBOX = os.getenv("FINNHUB_SANDBOX", "false").lower() == "true"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("stackiq.api")
logger.info(f"FINNHUB_SANDBOX={USE_SANDBOX}")

app = FastAPI(
    title="StackIQ API",
    version="0.3.0",
    docs_url="/docs",
    openapi_url="/openapi.json",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=_parse_origins(),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -------------------
# Simple request logging
# -------------------
@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = time.time()
    response = await call_next(request)
    dur_ms = int((time.time() - start) * 1000)
    logger.info(f"{request.method} {request.url.path} -> {response.status_code} ({dur_ms}ms)")
    return response

# -------------------
# Helpers
# -------------------
def _status_from_msg(msg: str) -> int:
    msg = msg or ""
    for code in ("403", "429", "404"):
        if code in msg:
            return int(code)
    return 500

def _sym(x: str) -> str:
    return (x or "").strip().upper()

# -------------------
# Routes
# -------------------
@app.get("/")
def root():
    # Quick jump to Swagger docs
    return RedirectResponse(url="/docs")

@app.get("/health")
def health():
    return {"status": "ok", "sandbox": USE_SANDBOX}

@app.get("/quote/{symbol}")
def quote(symbol: str):
    try:
        return get_quote(_sym(symbol))
    except Exception as e:
        msg = str(e)
        raise HTTPException(status_code=_status_from_msg(msg), detail=f"Quote error: {msg}")

@app.get("/candles/{symbol}")
def candles(
    symbol: str,
    resolution: str = Query("D", description="D/W/M or minute string like 1,5,15,60,240"),
    count: int = Query(60, ge=1, le=5000, description="Approx bars to fetch; we widen the time window automatically"),
):
    try:
        data = get_candles(_sym(symbol), resolution, count)
        # Finnhub may return {"s":"no_data"} with 200 OK — pass through to client
        return data
    except Exception as e:
        msg = str(e)
        # Helpful guidance for common case
        if "403" in msg and not USE_SANDBOX:
            raise HTTPException(
                status_code=403,
                detail="Your Finnhub plan doesn’t allow /stock/candle. Set FINNHUB_SANDBOX=true or upgrade your plan."
            )
        raise HTTPException(status_code=_status_from_msg(msg), detail=f"Candles error: {msg}")

@app.get("/profile/{symbol}")
def profile(symbol: str):
    try:
        return get_company_profile(_sym(symbol))
    except Exception as e:
        msg = str(e)
        raise HTTPException(status_code=_status_from_msg(msg), detail=f"Profile error: {msg}")

@app.get("/predict/{symbol}")
def predict(
    symbol: str,
    budget: float = Query(1000.0, ge=0.0, description="Dollars to size a position"),
):
    """
    Baseline signal: SMA10 vs SMA30 on daily candles.
    - If SMA10 > SMA30 -> 'buy'
      else if SMA10 < SMA30 -> 'sell'
      else 'hold'
    """
    sym = _sym(symbol)
    try:
        # 1) Get daily candles (about 60 bars) and compute signal
        candles = get_candles(sym, "D", 60)
        if candles.get("s") != "ok":
            # Pass through Finnhub's status with a stable shape
            return {
                "symbol": sym,
                "budget": budget,
                "recommendation": "hold",
                "confidence": 0.5,
                "reason": f"candles status {candles.get('s')}",
                "sma10": None,
                "sma30": None,
                "sandbox": USE_SANDBOX,
            }

        closes = candles.get("c") or []
        if len(closes) < 30:
            return {
                "symbol": sym,
                "budget": budget,
                "recommendation": "hold",
                "confidence": 0.5,
                "reason": "insufficient candle history (<30 closes)",
                "sma10": None,
                "sma30": None,
                "sandbox": USE_SANDBOX,
            }

        sma10 = mean(closes[-10:])
        sma30 = mean(closes[-30:])
        if sma10 > sma30:
            rec, conf, reason = "buy", 0.62, "SMA10 > SMA30 (uptrend)"
        elif sma10 < sma30:
            rec, conf, reason = "sell", 0.62, "SMA10 < SMA30 (downtrend)"
        else:
            rec, conf, reason = "hold", 0.5, "neutral"

        # 2) Size the position using last price
        q = get_quote(sym)
        last = q.get("c") or q.get("pc") or 0
        shares = round(budget / last, 2) if last else 0

        return {
            "symbol": sym,
            "budget": budget,
            "last_price": last,
            "position_size_shares": shares,
            "recommendation": rec,
            "confidence": conf,
            "reason": reason,
            "sma10": sma10,
            "sma30": sma30,
            "sandbox": USE_SANDBOX,
        }
    except Exception as e:
        msg = str(e)
        # If candles/quote raised HTTPError we map to a sensible status
        raise HTTPException(status_code=_status_from_msg(msg), detail=f"Predict error: {msg}")























































