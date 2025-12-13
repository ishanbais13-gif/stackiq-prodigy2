from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, Literal, Dict, Any
import os

from data_fetcher import (
    get_latest_quote,
    get_bars,
    get_news,
    run_predict_engine,
)

app = FastAPI(title="StackIQ V1", version="1.0.0")

# Allow your frontend to call this API (safe for V1)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # tighten later
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def root():
    return {"ok": True, "service": "stackiq", "version": "1.0.0"}

@app.get("/health")
def health():
    # IMPORTANT: should never call Alpaca here (keep it instant)
    return {"status": "ok"}

@app.get("/config")
def config():
    # Helps debug if Azure env vars are present (does NOT reveal keys)
    return {
        "alpaca_key_set": bool(os.getenv("ALPACA_API_KEY")),
        "alpaca_secret_set": bool(os.getenv("ALPACA_SECRET_KEY")),
        "alpaca_data_base_url": os.getenv("ALPACA_DATA_BASE_URL", "https://data.alpaca.markets"),
    }

@app.get("/quote/{symbol}")
def quote(symbol: str):
    try:
        q = get_latest_quote(symbol)
        return {
            "symbol": symbol.upper(),
            "bid": q.get("bp"),
            "ask": q.get("ap"),
            "bid_size": q.get("bs"),
            "ask_size": q.get("as"),
            "timestamp": q.get("t"),
            "raw": q,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/candles/{symbol}")
def candles(
    symbol: str,
    days: int = Query(30, ge=2, le=365),
    timeframe: Literal["1Day", "1Hour", "15Min", "5Min"] = "1Day",
    feed: Optional[Literal["iex", "sip"]] = "iex",
):
    """
    Returns OHLCV bars from Alpaca Data.
    For most free accounts, use feed=iex.
    """
    try:
        bars = get_bars(symbol=symbol, days=days, timeframe=timeframe, feed=feed)
        return {"symbol": symbol.upper(), "timeframe": timeframe, "days": days, "bars": bars}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/news/{symbol}")
def news(symbol: str, limit: int = Query(5, ge=1, le=20)):
    try:
        items = get_news(symbol=symbol, limit=limit)
        return {"symbol": symbol.upper(), "items": items}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/predict/{symbol}")
def predict(
    symbol: str,
    budget: float = Query(100.0, gt=0),
    risk: Literal["low", "medium", "high"] = "medium",
):
    """
    V1 prediction engine:
    - Uses recent bars to generate a simple trade plan your frontend can display.
    - Always returns JSON (never None).
    """
    try:
        result = run_predict_engine(symbol=symbol, budget=budget, risk=risk)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))






























