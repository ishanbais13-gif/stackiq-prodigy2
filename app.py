from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional

from data_fetcher import (
    get_quote,
    get_candles,
    get_news,
    get_price_targets,
    get_options_helper,
)

app = FastAPI(title="StackIQ Core API", version="0.9.0-day9-alpaca")

# Allow local dev / frontends
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health():
    return {"status": "ok", "message": "StackIQ API is healthy."}


@app.get("/quote/{symbol}")
def quote(symbol: str, pretty: Optional[int] = Query(default=0, ge=0, le=1)):
    try:
        data = get_quote(symbol)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Alpaca error in quote: {e}")
    return {"symbol": symbol.upper(), "data": data}


@app.get("/candles/{symbol}")
def candles(
    symbol: str,
    days: int = Query(default=60, ge=1, le=365),
    timeframe: str = Query(default="1Day"),
):
    try:
        data = get_candles(symbol, days=days, timeframe=timeframe)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Alpaca error in candles: {e}")
    return {"symbol": symbol.upper(), "data": data}


@app.get("/news/{symbol}")
def news(symbol: str, limit: int = Query(default=5, ge=1, le=50)):
    try:
        items = get_news(symbol, limit=limit)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Alpaca error in news: {e}")
    return {"symbol": symbol.upper(), "items": items}


@app.get("/targets/{symbol}")
def targets(symbol: str):
    # Currently stubbed – does not hit Alpaca.
    data = get_price_targets(symbol)
    return {"symbol": symbol.upper(), "data": data}


@app.get("/options/{symbol}")
def options(symbol: str, risk: str = Query(default="medium")):
    # Currently stubbed – does not hit Alpaca.
    plan = get_options_helper(symbol, risk=risk)
    return {"symbol": symbol.upper(), "plan": plan}


@app.get("/predict/{symbol}")
def predict(
    symbol: str,
    budget: float = Query(..., gt=0),
    risk: str = Query(default="medium"),
    fractional: bool = Query(default=True),
):
    # Basic example prediction using latest close and recent volatility.
    try:
        quote = get_quote(symbol)
        candles = get_candles(symbol, days=30, timeframe="1Day")
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Alpaca error in predict: {e}")

    closes = [c["close"] for c in candles]
    if not closes:
        raise HTTPException(status_code=400, detail="No candle data available")

    last_close = closes[-1]
    avg_close = sum(closes) / len(closes)
    # simple "volatility" metric
    volatility = max(closes) - min(closes)

    risk_multiplier = {"low": 0.5, "medium": 1.0, "high": 1.5}.get(risk, 1.0)
    target_move = (volatility / avg_close) * risk_multiplier
    bullish_target = round(last_close * (1 + target_move), 2)
    bearish_target = round(last_close * (1 - target_move), 2)

    max_shares = budget / last_close
    if not fractional:
        max_shares = int(max_shares)
    shares = max_shares

    return {
        "symbol": symbol.upper(),
        "input": {
            "budget": budget,
            "risk": risk,
            "fractional": fractional,
        },
        "quote": quote,
        "analysis": {
            "last_close": last_close,
            "avg_close": avg_close,
            "volatility": volatility,
            "bullish_target": bullish_target,
            "bearish_target": bearish_target,
        },
        "plan": {
            "shares_to_buy": shares,
            "estimated_cost": shares * last_close,
            "targets": {
                "take_profit": bullish_target,
                "stop_loss": bearish_target,
            },
            "note": "Simplified demo logic – not financial advice.",
        },
    }























