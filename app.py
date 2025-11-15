# app.py
from typing import List, Dict, Any

import os

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

import data_fetcher as df
import engine
import backtest as bt
import optimize as opt  # we'll use this later, safe to import now


app = FastAPI(
    title="StackIQ API",
    description="Stock analysis API for StackIQ, deployed on Azure.",
    version="0.1.0",
)


# ---------- Models ----------

class BatchPredictRequest(BaseModel):
    symbols: List[str]
    budget: float


# ---------- Helpers ----------

def _check_finnhub_key() -> bool:
    """Return True if FINNHUB_API_KEY looks set."""
    return bool(os.getenv("FINNHUB_API_KEY"))


# ---------- Basic routes ----------

@app.get("/")
def root() -> Dict[str, Any]:
    """
    Simple root endpoint so hitting the base URL doesn't 404.
    """
    return {
        "message": "StackIQ API is running.",
        "endpoints": {
            "health": "/health",
            "quote": "/quote/{symbol}",
            "predict (single)": "/predict/{symbol}?budget=...",
            "predict (batch)": "/predict/batch",
        },
    }


@app.get("/health")
def health() -> Dict[str, Any]:
    """
    Healthcheck endpoint used by you + Azure.
    """
    return {
        "status": "ok",
        "mode": "base",
        "engine_ready": _check_finnhub_key(),
        "message": "App is running. Engine uses Finnhub only.",
    }


# ---------- Data fetch endpoints ----------

@app.get("/quote/{symbol}")
def get_quote(symbol: str) -> Dict[str, Any]:
    """
    Return latest quote data for a symbol from Finnhub.
    """
    symbol = symbol.upper()

    try:
        quote = df.get_quote(symbol)
    except Exception as e:
        # Any unexpected error talking to Finnhub
        raise HTTPException(status_code=502, detail=f"Quote fetch failed: {e}")

    if not quote:
        raise HTTPException(status_code=404, detail=f"No data for symbol {symbol}")

    return {
        "symbol": symbol,
        "quote": quote,
    }


# ---------- Prediction endpoints ----------

@app.get("/predict/{symbol}")
def predict(symbol: str, budget: float = Query(..., gt=0)) -> Dict[str, Any]:
    """
    Main one-day prediction endpoint.

    Example:
      /predict/NVDA?budget=5000
    """
    symbol = symbol.upper()

    if not _check_finnhub_key():
        raise HTTPException(
            status_code=500,
            detail="FINNHUB_API_KEY is not set in environment variables.",
        )

    try:
        result = engine.predict(symbol, budget)
    except Exception as e:
        # Bubble useful info to the client
        raise HTTPException(status_code=500, detail=f"Prediction failed: {e}")

    # Ensure we always return a dict
    if not isinstance(result, dict):
        raise HTTPException(
            status_code=500,
            detail="Engine returned an unexpected result format.",
        )

    return result


@app.post("/predict/batch")
def predict_batch(payload: BatchPredictRequest) -> Dict[str, Any]:
    """
    Batch prediction for multiple symbols with the same budget.

    POST body example:
    {
      "symbols": ["NVDA", "AAPL", "TSLA"],
      "budget": 5000
    }
    """
    if not payload.symbols:
        raise HTTPException(status_code=400, detail="symbols list cannot be empty")

    if not _check_finnhub_key():
        raise HTTPException(
            status_code=500,
            detail="FINNHUB_API_KEY is not set in environment variables.",
        )

    try:
        results = engine.predict_batch(
            [s.upper() for s in payload.symbols],
            payload.budget,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Batch prediction failed: {e}")

    return {
        "symbols": [s.upper() for s in payload.symbols],
        "budget": payload.budget,
        "results": results,
    }








































































