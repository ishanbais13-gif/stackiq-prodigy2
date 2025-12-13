from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from data_fetcher import (
    get_quote,
    get_candles,
    get_news,
    run_predict_engine,
)

app = FastAPI(title="StackIQ API", version="0.1.0")

# CORS (safe for v1; lock down later)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/quote/{symbol}")
def quote(symbol: str):
    try:
        return get_quote(symbol)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/candles/{symbol}")
def candles(
    symbol: str,
    days: int = Query(30, ge=1, le=365),
    resolution: str = Query("D"),  # D, 60, 15, 5
):
    try:
        return {"symbol": symbol.upper(), "candles": get_candles(symbol, days=days, resolution=resolution)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/news/{symbol}")
def news(symbol: str, limit: int = Query(5, ge=1, le=50)):
    try:
        return get_news(symbol, limit=limit)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/predict/{symbol}")
def predict(
    symbol: str,
    budget: float = Query(..., gt=0),
    risk: str = Query("medium"),  # low | medium | high
):
    try:
        return run_predict_engine(symbol=symbol, budget=budget, risk=risk)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


























