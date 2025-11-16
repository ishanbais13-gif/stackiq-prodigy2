from math import floor
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from data_fetcher import get_quote, get_candles

app = FastAPI(
    title="StackIQ API",
    version="0.3.0",
    description="Backend for AI-powered stock analysis (Day 3: prediction v1).",
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
            "/predict/{symbol}?budget=...&risk=...",
        ],
    }


@app.get("/health")
def health():
    """
    Simple health check so you know the API is alive.
    """
    return {
        "status": "ok",
        "message": "StackIQ backend is running",
        "engine_version": "v1-predict-quote-only",
    }


@app.get("/quote/{symbol}")
def quote(symbol: str):
    """
    Return real-time quote data from Finnhub.
    """
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
    """
    Pass-through candles endpoint.
    NOTE: On your current Finnhub plan, this will likely return an error payload
    in data.error / data.http_status for /stock/candle.
    """
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


def _normalize_risk(risk: Optional[str]) -> str:
    if not risk:
        return "medium"
    risk = risk.lower().strip()
    if risk not in ("low", "medium", "high"):
        return "medium"
    return risk


def _risk_allocation_factor(risk: str) -> float:
    """
    How much of the budget to put into a single position.
    You can tweak these numbers later.
    """
    if risk == "low":
        return 0.25  # 25% of budget
    if risk == "high":
        return 0.75  # 75% of budget
    return 0.5  # medium = 50%


def _build_signal(change_pct: Optional[float]) -> dict:
    """
    Very simple rule-based signal using today's percent change.

    This is v1. Later we’ll replace / extend this with
    indicators, history, and ML models.
    """
    if change_pct is None:
        return {
            "label": "neutral",
            "confidence": 50,
            "reason": "No change percentage available; treating as neutral.",
        }

    # Example rules (you can tweak these later):
    cp = change_pct

    # Big downward move -> possible dip, but also risky
    if cp <= -4:
        return {
            "label": "speculative_dip_buy",
            "confidence": 55,
            "reason": "Price dropped sharply today (<= -4%). Could be a dip but carries higher risk.",
        }

    # Mild red day
    if -4 < cp < 0:
        return {
            "label": "cautious_buy",
            "confidence": 60,
            "reason": "Slightly down today; could be a mild discount if fundamentals are strong.",
        }

    # Flat-ish
    if 0 <= cp <= 2:
        return {
            "label": "steady_buy",
            "confidence": 65,
            "reason": "Small positive move (0–2%). Stable day; reasonable time to scale in.",
        }

    # Strong green day – might be chasing
    if 2 < cp <= 5:
        return {
            "label": "light_buy_or_wait",
            "confidence": 55,
            "reason": "Strong green day (2–5%). Momentum is up; consider smaller size or wait for a pullback.",
        }

    # Huge spike – usually better to wait
    if cp > 5:
        return {
            "label": "wait_for_pullback",
            "confidence": 60,
            "reason": "Very strong move (>5%). Often better to wait for a pullback instead of chasing.",
        }

    # Fallback
    return {
        "label": "neutral",
        "confidence": 50,
        "reason": "No strong edge detected based on today's move alone.",
    }


@app.get("/predict/{symbol}")
def predict(
    symbol: str,
    budget: float = Query(..., gt=0, description="Total amount of money to allocate (in dollars)."),
    risk: Optional[str] = Query("medium", description="Risk level: low, medium, or high."),
):
    """
    v1 prediction endpoint using ONLY real-time quote data.

    It:
    - pulls the latest quote
    - computes today's percent change
    - decides how much of the budget to risk based on risk profile
    - computes how many shares that buys
    - returns a simple rule-based signal + explanation

    NOTE: This is NOT financial advice. It's a demo logic engine
    you can tweak and improve over time.
    """
    quote_data = get_quote(symbol)
    if quote_data is None:
        raise HTTPException(
            status_code=502,
            detail="Failed to fetch quote from Finnhub. Check API key and symbol.",
        )

    current_price = quote_data.get("c") or quote_data.get("pc")
    if not current_price or current_price <= 0:
        raise HTTPException(
            status_code=502,
            detail="Quote data does not contain a usable current price.",
        )

    # Finnhub often includes dp (percent change), but we can also compute it
    change_pct = quote_data.get("dp")
    if change_pct is None:
        prev_close = quote_data.get("pc")
        if prev_close and prev_close > 0:
            change_pct = ((current_price - prev_close) / prev_close) * 100
        else:
            change_pct = None

    norm_risk = _normalize_risk(risk)
    alloc_factor = _risk_allocation_factor(norm_risk)

    max_allocation = budget * alloc_factor
    shares = floor(max_allocation / current_price) if current_price > 0 else 0
    estimated_cost = shares * current_price

    signal = _build_signal(change_pct)

    # Edge case: budget too small to buy even 1 share
    if shares == 0:
        signal = {
            "label": "budget_too_small",
            "confidence": 100,
            "reason": "Budget and risk level are too low to buy even 1 share at the current price.",
        }

    return {
        "symbol": symbol.upper(),
        "price": current_price,
        "change_pct_today": change_pct,
        "budget": budget,
        "risk_profile": norm_risk,
        "allocation": {
            "max_allocation": max_allocation,
            "shares": shares,
            "estimated_cost": estimated_cost,
        },
        "signal": signal,
        "raw_quote": quote_data,
        "disclaimer": "This output is for informational and educational purposes only and is not financial advice.",
    }









































































