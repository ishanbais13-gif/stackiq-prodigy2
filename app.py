import logging
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from dotenv import load_dotenv

from data_fetcher import (
    UpstreamAPIError,
    get_latest_quote,
    get_bars,
    get_news,
    get_top_movers,
)

# Load .env if present (does NOT crash if missing)
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("stackiq")

app = FastAPI(title="StackIQ API", version="1.0.0")

# CORS for local frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _alpaca_503(detail: str) -> JSONResponse:
    return JSONResponse(
        status_code=503,
        content={
            "error": "alpaca_unavailable",
            "detail": detail,
            "hint": "Set ALPACA_API_KEY and ALPACA_SECRET_KEY in a .env file in the backend repo root (stackiq-prodigy2), then restart uvicorn.",
        },
    )


@app.exception_handler(UpstreamAPIError)
async def upstream_error_handler(request, exc: UpstreamAPIError):
    # Keep errors clean for you (no massive trace dumps)
    logger.warning("UpstreamAPIError: %s", exc.message)
    if exc.status_code == 503:
        return _alpaca_503(exc.message)
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": "upstream_error", "detail": exc.message},
    )


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/top-movers")
def top_movers(limit: int = Query(10, ge=1, le=50)) -> Dict[str, Any]:
    items = get_top_movers(limit=limit)
    return {"items": items}


@app.get("/news")
def news(limit: int = Query(10, ge=1, le=50)) -> Dict[str, Any]:
    items = get_news(limit=limit)
    return {"items": items}


@app.get("/quote/{symbol}")
def quote(symbol: str) -> Dict[str, Any]:
    symbol = symbol.upper().strip()
    if not symbol.isalnum():
        raise HTTPException(status_code=400, detail="Invalid symbol")
    return get_latest_quote(symbol)


@app.get("/candles/{symbol}")
def candles(
    symbol: str,
    timeframe: str = Query("1Day"),
    limit: int = Query(100, ge=1, le=1000),
) -> Dict[str, Any]:
    symbol = symbol.upper().strip()
    if not symbol.isalnum():
        raise HTTPException(status_code=400, detail="Invalid symbol")

    candles = get_bars(symbol=symbol, timeframe=timeframe, limit=limit)
    return {"symbol": symbol, "candles": candles}








































