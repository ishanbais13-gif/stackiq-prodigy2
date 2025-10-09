import os
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from typing import Optional
from data_fetcher import get_quote, get_candles, FinnhubError

app = FastAPI(title="StackIQ API", version="0.1.0")

@app.get("/health")
def health():
    token = os.getenv("FINNHUB_API_KEY", "").strip()
    return {
        "status": "ok",
        "has_token": bool(token),
        "service": "StackIQ",
        "version": "0.1.0"
    }

@app.get("/quote/{symbol}")
def quote(symbol: str):
    try:
        data = get_quote(symbol)
        return JSONResponse(content=data)
    except FinnhubError as e:
        raise HTTPException(status_code=502, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {e}")

@app.get("/candles/{symbol}")
def candles(
    symbol: str,
    days: int = Query(30, ge=1, le=365),
    resolution: str = Query("D", pattern="^(1|5|15|30|60|D|W|M)$")
):
    try:
        data = get_candles(symbol, days=days, resolution=resolution)
        return JSONResponse(content=data)
    except FinnhubError as e:
        raise HTTPException(status_code=502, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {e}")
























































