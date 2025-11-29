from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, List
from pydantic import BaseModel

from data_fetcher import (
    get_quote,
    get_candles,
    get_news,
    get_price_targets,
    get_options_helper,
)

app = FastAPI(
    title="StackIQ Core API",
    version="1.0.0",
    description="StackIQ Alpaca-powered backend"
)

# ---------------------------------------------------------
# CORS (allow all for now)
# ---------------------------------------------------------

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------
# Health Check
# ---------------------------------------------------------

@app.get("/health")
def health():
    return {"status": "ok", "message": "StackIQ API is healthy."}


# ---------------------------------------------------------
# Quote Endpoint
# ---------------------------------------------------------

@app.get("/quote/{symbol}")
def quote(symbol: str):
    try:
        data = get_quote(symbol)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Quote error: {e}")
    return {"symbol": symbol.upper(), "data": data}


# ---------------------------------------------------------
# Candles Endpoint
# ---------------------------------------------------------

@app.get("/candles/{symbol}")
def candles(
    symbol: str,
    days: int = Query(default=60, ge=1, le=365),
    timeframe: str = Query(default="1Day")
):
    try:
        data = get_candles(symbol, days=days, timeframe=timeframe)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Candles error: {e}")
    return {"symbol": symbol.upper(), "data": data}


# ---------------------------------------------------------
# News Endpoint
# ---------------------------------------------------------

@app.get("/news/{symbol}")
def news(symbol: str, limit: int = Query(default=5, ge=1, le=50)):
    try:
        items = get_news(symbol, limit=limit)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"News error: {e}")
    return {"symbol": symbol.upper(), "items": items}


# ---------------------------------------------------------
# Price Targets (stub logic)
# ---------------------------------------------------------

@app.get("/targets/{symbol}")
def targets(symbol: str):
    data = get_price_targets(symbol)
    return {"symbol": symbol.upper(), "data": data}


# ---------------------------------------------------------
# Options Helper (stub logic)
# ---------------------------------------------------------

@app.get("/options/{symbol}")
def options(symbol: str, risk: str = Query(default="medium")):
    try:
        plan = get_options_helper(symbol, risk=risk)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Options error: {e}")
    return {"symbol": symbol.upper(), "plan": plan}


# ---------------------------------------------------------
# Prediction Endpoint (simple version)
# ---------------------------------------------------------

@app.get("/predict/{symbol}")
def predict(
    symbol: str,
    budget: float = Query(..., gt=0),
    risk: str = Query(default="medium"),
    fractional: bool = Query(default=True),
):
    """
    Basic prediction engine using recent candles + simple volatility/trend.
    You will get Prediction v2 tomorrow (Saturday).
    """
    try:
        quote = get_quote(symbol)
        candles = get_candles(symbol, days=30, timeframe="1Day")
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Predict data error: {e}")

    closes = [c["close"] for c in candles]
    if not closes:
        raise HTTPException(status_code=500, detail="No candle data returned")

    last_close = closes[-1]
    avg_close = sum(closes) / len(closes)
    volatility = max(closes) - min(closes)

    # Simple trend: positive or negative
    trend = last_close - avg_close

    # Simple target generation
    risk_mult = {"low": 0.5, "medium": 1.0, "high": 1.5}.get(risk.lower(), 1.0)
    move = (volatility / avg_close) * risk_mult if avg_close > 0 else 0

    bullish_target = round(last_close * (1 + move), 2)
    bearish_target = round(last_close * (1 - move), 2)

    shares = budget / last_close
    if not fractional:
        shares = int(shares)

    return {
        "symbol": symbol.upper(),
        "input": {
            "budget": budget,
            "fractional": fractional,
            "risk": risk,
        },
        "quote": quote,
        "analysis": {
            "last_close": last_close,
            "avg_close": avg_close,
            "volatility": volatility,
            "trend_score": trend,
            "bullish_target": bullish_target,
            "bearish_target": bearish_target,
        },
        "plan": {
            "shares_to_buy": shares,
            "estimated_cost": round(shares * last_close, 2),
        },
    }
























