from math import floor
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from data_fetcher import get_quote, get_candles

app = FastAPI(
    title="StackIQ API",
    version="0.4.0",
    description="Backend for AI-powered stock analysis (Day 4: enhanced prediction).",
)

# CORS – open for now, tighten later when you have a frontend domain
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
            "/predict/{symbol}?budget=...&risk=...&fractional=...",
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
        "engine_version": "v1.1-predict-quote-only-enhanced",
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


def _risk_sl_tp(risk: str) -> tuple[float, float]:
    """
    Stop-loss and take-profit percents based on risk profile.

    Returns (stop_loss_pct, take_profit_pct) as negative / positive percentages.
    Example: (-5.0, 10.0) means -5% stop, +10% target.
    """
    if risk == "low":
        return -5.0, 10.0
    if risk == "high":
        return -15.0, 30.0
    # medium
    return -10.0, 20.0


def _position_size_label(allocation_ratio: float) -> str:
    """
    Describe how aggressive the position size is relative to total budget.
    """
    if allocation_ratio <= 0.3:
        return "small"
    if allocation_ratio <= 0.6:
        return "medium"
    return "aggressive"


def _build_signal(change_pct: Optional[float]) -> dict:
    """
    Very simple rule-based signal using today's percent change.

    This is v1. Later we’ll replace / extend this with
    indicators, history, and ML models.
    """
    if change_pct is None:
        return {
            "label": "neutral",
            "score": 50,
            "reason": "No change percentage available; treating as neutral.",
        }

    cp = change_pct

    # Big downward move -> possible dip, but also risky
    if cp <= -4:
        return {
            "label": "speculative_dip_buy",
            "score": 55,
            "reason": "Price dropped sharply today (<= -4%). Could be a dip but carries higher risk.",
        }

    # Mild red day
    if -4 < cp < 0:
        return {
            "label": "cautious_buy",
            "score": 60,
            "reason": "Slightly down today; could be a mild discount if fundamentals are strong.",
        }

    # Flat-ish
    if 0 <= cp <= 2:
        return {
            "label": "steady_buy",
            "score": 65,
            "reason": "Small positive move (0–2%). Stable day; reasonable time to scale in.",
        }

    # Strong green day – might be chasing
    if 2 < cp <= 5:
        return {
            "label": "light_buy_or_wait",
            "score": 55,
            "reason": "Strong green day (2–5%). Momentum is up; consider smaller size or wait for a pullback.",
        }

    # Huge spike – usually better to wait
    if cp > 5:
        return {
            "label": "wait_for_pullback",
            "score": 60,
            "reason": "Very strong move (>5%). Often better to wait for a pullback instead of chasing.",
        }

    # Fallback
    return {
        "label": "neutral",
        "score": 50,
        "reason": "No strong edge detected based on today's move alone.",
    }


def _build_summary(
    symbol: str,
    price: float,
    change_pct: Optional[float],
    risk_profile: str,
    allocation_dollars: float,
    shares_int: int,
    shares_frac: Optional[float],
    fractional: bool,
    signal_label: str,
):
    symbol = symbol.upper()
    direction_text = "flat"
    if change_pct is not None:
        if change_pct > 1:
            direction_text = "up"
        elif change_pct < -1:
            direction_text = "down"

    if fractional and shares_frac and shares_frac > 0:
        size_text = f"about {shares_frac:.2f} shares"
    else:
        size_text = f"{shares_int} shares" if shares_int > 0 else "no shares"

    return (
        f"{symbol} is trading around ${price:.2f} today and is {direction_text} on the session. "
        f"Given your {risk_profile} risk profile, this engine would allocate roughly "
        f"${allocation_dollars:.2f} into this trade, which corresponds to {size_text}. "
        f"The current signal is '{signal_label}'."
    )


@app.get("/predict/{symbol}")
def predict(
    symbol: str,
    budget: float = Query(..., gt=0, description="Total amount of money to allocate (in dollars)."),
    risk: Optional[str] = Query("medium", description="Risk level: low, medium, or high."),
    fractional: bool = Query(
        False,
        description="If true, allow fractional shares in the position size calculation.",
    ),
):
    """
    v1.1 prediction endpoint using ONLY real-time quote data.

    It:
    - pulls the latest quote
    - computes today's percent change
    - decides how much of the budget to risk based on risk profile
    - computes how many shares that buys (integer and optional fractional)
    - suggests basic stop-loss and take-profit levels
    - returns a simple rule-based signal + explanation + summary

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
    allocation_ratio = max_allocation / budget if budget > 0 else 0.0
    size_label = _position_size_label(allocation_ratio)

    # Integer shares (for non-fractional mode)
    shares_int = floor(max_allocation / current_price) if current_price > 0 else 0

    # Fractional shares (optional)
    shares_frac = None
    if fractional and current_price > 0:
        shares_frac = max_allocation / current_price

    # Cost if using integer shares only
    estimated_cost_int = shares_int * current_price

    signal = _build_signal(change_pct)

    # Edge case: budget too small to buy even 1 share and fractional not allowed
    if shares_int == 0 and not fractional:
        signal = {
            "label": "budget_too_small",
            "score": 100,
            "reason": "Budget and risk level are too low to buy even 1 share at the current price.",
        }

    # Stop-loss / take-profit levels based on risk
    sl_pct, tp_pct = _risk_sl_tp(norm_risk)
    stop_loss_price = current_price * (1 + sl_pct / 100.0)
    take_profit_price = current_price * (1 + tp_pct / 100.0)

    summary = _build_summary(
        symbol=symbol,
        price=current_price,
        change_pct=change_pct,
        risk_profile=norm_risk,
        allocation_dollars=max_allocation,
        shares_int=shares_int,
        shares_frac=shares_frac,
        fractional=fractional,
        signal_label=signal["label"],
    )

    return {
        "symbol": symbol.upper(),
        "price": current_price,
        "change_pct_today": change_pct,
        "budget": budget,
        "risk_profile": norm_risk,
        "allocation": {
            "allocation_factor": alloc_factor,
            "position_size_label": size_label,
            "max_allocation": max_allocation,
            "shares_integer": shares_int,
            "shares_fractional": shares_frac,
            "estimated_cost_integer": estimated_cost_int,
            "fractional_mode": fractional,
        },
        "risk_management": {
            "stop_loss_pct": sl_pct,
            "take_profit_pct": tp_pct,
            "stop_loss_price": stop_loss_price,
            "take_profit_price": take_profit_price,
        },
        "signal": signal,
        "raw_quote": quote_data,
        "summary": summary,
        "disclaimer": "This output is for informational and educational purposes only and is not financial advice.",
    }










































































