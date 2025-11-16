from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from data_fetcher import get_quote, get_candles

app = FastAPI(
    title="StackIQ API",
    version="0.2.0",
    description="Backend for AI-powered stock analysis (Day 2 foundation).",
)

# Allow everything for now (you can tighten later)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def root():
    return {
        "message": "StackIQ backend is live.",
        "endpoints": [
            "/health",
            "/quote/{symbol}",
            "/candles/{symbol}?resolution=D&days=30",
        ],
    }


@app.get("/health")
def health():
    return {"status": "ok", "message": "StackIQ backend is running"}


@app.get("/quote/{symbol}")
def quote(symbol: str):
    data = get_quote(symbol)
    if data is None:
        raise HTTPException(
            status_code=502,
            detail="Failed to fetch quote from Finnhub. Check API key and symbol.",
        )
    return {
        "symbol": symbol.upper(),
        "quote": data,
    }


@app.get("/candles/{symbol}")
def candles(symbol: str, resolution: str = "D", days: int = 30):
    try:
        candles_data = get_candles(symbol, resolution=resolution, days=days)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    if candles_data is None:
        raise HTTPException(
            status_code=502,
            detail="Failed to fetch candles from Finnhub. Check API key and symbol.",
        )

    return {
        "symbol": symbol.upper(),
        "resolution": resolution,
        "days": days,
        "data": candles_data,
    }









































































