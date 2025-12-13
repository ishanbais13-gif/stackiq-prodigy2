from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from data_fetcher import (
    get_latest_quote,
    get_daily_candles
)

app = FastAPI(title="StackIQ API v1")

# CORS (safe for V1)
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
        return get_latest_quote(symbol.upper())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/candles/{symbol}")
def candles(
    symbol: str,
    days: int = 30
):
    """
    Daily candles for last N days (default 30)
    """
    try:
        return {
            "symbol": symbol.upper(),
            "candles": get_daily_candles(symbol.upper(), days)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))





























