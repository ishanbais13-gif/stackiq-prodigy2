import os
import time
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from data_fetcher import UpstreamAPIError, get_bars, get_latest_quote

APP_VERSION = "1.0.0"

app = FastAPI(
    title="StackIQ API",
    version=APP_VERSION,
)

# CORS (safe default: allow your frontend domains; for quick V1 allow all)
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "*").strip()
origins = ["*"] if ALLOWED_ORIGINS == "*" else [o.strip() for o in ALLOWED_ORIGINS.split(",") if o.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _mask(s: str, keep: int = 4) -> str:
    if not s:
        return ""
    if len(s) <= keep:
        return "*" * len(s)
    return ("*" * (len(s) - keep)) + s[-keep:]


@app.get("/")
def root() -> Dict[str, Any]:
    return {
        "name": "StackIQ API",
        "version": APP_VERSION,
        "status": "ok",
        "endpoints": ["/health", "/quote/{symbol}", "/candles/{symbol}"],
    }


@app.get("/health")
def health() -> Dict[str, Any]:
    # quick sanity about env vars (DON'T leak them)
    api_key = os.getenv("ALPACA_API_KEY", "")
    secret = os.getenv("ALPACA_SECRET_KEY", "")
    return {
        "status": "ok",
        "time": int(time.time()),
        "version": APP_VERSION,
        "alpaca": {
            "api_key_present": bool(api_key.strip()),
            "secret_present": bool(secret.strip()),
            "api_key_masked": _mask(api_key.strip(), keep=4),
        },
    }


@app.get("/quote/{symbol}")
def quote(symbol: str) -> Dict[str, Any]:
    try:
        return get_latest_quote(symbol)
    except ValueError as e:
        # missing env var or config problem
        raise HTTPException(status_code=500, detail=str(e))
    except UpstreamAPIError as e:
        raise HTTPException(status_code=e.status_code, detail=e.payload or {"message": str(e)})
    except Exception as e:
        raise HTTPException(status_code=500, detail={"message": "Unexpected error", "error": str(e)})


@app.get("/candles/{symbol}")
def candles(
    symbol: str,
    timeframe: str = Query(default="1Day", description="Examples: 1Min, 5Min, 15Min, 1Hour, 1Day"),
    days: int = Query(default=30, ge=1, le=365, description="How many past days to fetch"),
    limit: int = Query(default=200, ge=1, le=10000, description="Max candles"),
) -> Dict[str, Any]:
    try:
        return get_bars(symbol=symbol, timeframe=timeframe, days=days, limit=limit)
    except ValueError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except UpstreamAPIError as e:
        raise HTTPException(status_code=e.status_code, detail=e.payload or {"message": str(e)})
    except Exception as e:
        raise HTTPException(status_code=500, detail={"message": "Unexpected error", "error": str(e)})































