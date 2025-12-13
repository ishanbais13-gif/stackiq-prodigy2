 from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import os

# Import your existing functions from data_fetcher.py
# (Make sure these names match your file exactly.)
from data_fetcher import (
    get_quote,
    get_candles,
    get_news,
    run_predict_engine,
)

app = FastAPI(title="StackIQ API", version="1.0")

# CORS (lets your frontend call this API)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # tighten later
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def _env_check():
    # Only require keys for endpoints that actually hit Alpaca
    if not os.getenv("ALPACA_API_KEY") or not os.getenv("ALPACA_API_SECRET"):
        raise HTTPException(
            status_code=500,
            detail="Missing env vars: ALPACA_API_KEY and/or ALPACA_API_SECRET"
        )

@app.get("/")
def root():
    return {
        "name": "StackIQ API",
        "status": "running",
        "endpoints": ["/health", "/quote/{symbol}", "/candles/{symbol}", "/news/{symbol}", "/predict/{symbol}"]
    }

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/quote/{symbol}")
def quote(symbol: str):
    _env_check()
    try:
        return get_quote(symbol)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/candles/{symbol}")
def candles(symbol: str, days: int = 30, resolution: str = "D"):
    """
    days: how many trading days back (approx)
    resolution: "D", "60", "15", "5" (your data_fetcher maps these)
    """
    _env_check()
    try:
        data = get_candles(symbol, days=days, resolution=resolution)
        return {"symbol": symbol.upper(), "candles": data}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/news/{symbol}")
def news(symbol: str, limit: int = 5):
    _env_check()
    try:
        return get_news(symbol, limit=limit)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/predict/{symbol}")
def predict(symbol: str, budget: float = 100.0, risk: str = "medium"):
    """
    risk: low | medium | high
    """
    _env_check()
    try:
        return run_predict_engine(symbol=symbol, budget=budget, risk=risk)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



















































