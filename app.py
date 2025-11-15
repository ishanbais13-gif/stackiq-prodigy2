# app.py
from fastapi import FastAPI, HTTPException
from typing import Dict, Any, List

import data_fetcher as df
import engine

app = FastAPI(
    title="StackIQ API",
    version="0.1.0",
    description="Stock analysis API for StackIQ"
)

# -----------------------
# Health check
# -----------------------
@app.get("/health")
def health() -> Dict[str, Any]:
    """
    Simple health check for Azure.
    """
    return {
        "status": "ok",
        "mode": "base",
        "message": "App is running, engine disabled for now."
    }

# -----------------------
# Basic data endpoints
# -----------------------
@app.get("/quote/{symbol}")
def get_quote(symbol: str) -> Dict[str, Any]:
    """
    Return latest quote for a ticker.
    """
    symbol = symbol.upper()
    try:
        quote = df.quote(symbol)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Quote fetch failed: {e}")

    if not quote:
        raise HTTPException(status_code=404, detail="Quote not found")

    return {
        "symbol": symbol,
        "quote": quote,
    }


@app.get("/candles/{symbol}")
def get_candles(symbol: str, days: int = 60) -> Dict[str, Any]:
    """
    Return OHLCV candles for the last N days.
    """
    symbol = symbol.upper()
    try:
        candles = df.candles(symbol, days=days)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Candles fetch failed: {e}")

    if not candles:
        raise HTTPException(status_code=404, detail="No candles found")

    return {
        "symbol": symbol,
        "days": days,
        "candles": candles,
    }

# -----------------------
# Simple prediction endpoint (Day 1)
# -----------------------
@app.get("/predict/{symbol}")
def predict(symbol: str, budget: float) -> Dict[str, Any]:
    """
    Basic prediction endpoint using engine.build_features + _score_from_features.
    Day 1 goal: return a confidence score and bullet points.
    """
    symbol = symbol.upper()

    # 1) Build feature set
    try:
        feats = engine.build_features(symbol)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Feature build failed: {e}")

    if not feats:
        raise HTTPException(
            status_code=400,
            detail="Not enough data to build features for this symbol",
        )

    # 2) Score features -> (0-100 score + bullets)
    try:
        score, bullets = engine._score_from_features(feats)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Scoring failed: {e}")

    # 3) Very simple position sizing for now (Day 1)
    price = feats.get("price")
    shares = None
    if isinstance(price, (int, float)) and price > 0:
        shares = int(budget // price)

    return {
        "symbol": symbol,
        "budget": budget,
        "price": price,
        "max_shares_with_budget": shares,
        "score": score,           # 0-100 confidence
        "bullets": bullets,       # explanation bullets
    }







































































