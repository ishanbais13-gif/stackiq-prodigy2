from enum import Enum
from typing import List, Dict, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from data_fetcher import get_quote, get_candles


# ------------------------
# Enums & Request Models
# ------------------------

class RiskProfile(str, Enum):
    low = "low"
    medium = "medium"
    high = "high"


class BatchPredictRequest(BaseModel):
    symbols: List[str] = Field(
        ...,
        description="List of stock tickers to analyze",
        example=["NVDA", "SOFI", "OPEN"],
        min_items=1,
    )
    budget: float = Field(
        ...,
        gt=0,
        description="Total budget (USD) to allocate across this batch",
        example=500,
    )
    risk: RiskProfile = Field(
        default=RiskProfile.medium,
        description="Risk profile for this batch",
        example="medium",
    )
    fractional: bool = Field(
        default=True,
        description="Whether fractional shares are allowed",
        example=True,
    )


# ------------------------
# FastAPI app setup
# ------------------------

app = FastAPI(
    title="StackIQ API",
    version="1.0.0",
    description="Backend engine for StackIQ – Finnhub-only, production-safe.",
)

# Basic CORS so your future frontend can call this safely
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # you can lock this down later
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ------------------------
# Utility helpers
# ------------------------

def _safe_pct_change(current: float, previous: float) -> float:
    """Avoid division by zero when computing percent changes."""
    if previous is None or previous == 0:
        return 0.0
    return (current - previous) / previous * 100.0


def _allocation_factor_for_risk(risk: RiskProfile) -> float:
    if risk == RiskProfile.low:
        return 0.3
    if risk == RiskProfile.high:
        return 0.75
    return 0.5  # medium


def _position_label_for_risk(risk: RiskProfile) -> str:
    if risk == RiskProfile.low:
        return "small"
    if risk == RiskProfile.high:
        return "aggressive"
    return "medium"


def _risk_multipliers(risk: RiskProfile) -> Dict[str, float]:
    """
    Simple knobs for risk-based tuning.
    """
    if risk == RiskProfile.low:
        return {"stop_loss_pct": -5, "take_profit_pct": 10}
    if risk == RiskProfile.high:
        return {"stop_loss_pct": -20, "take_profit_pct": 40}
    return {"stop_loss_pct": -10, "take_profit_pct": 20}


def _label_from_price_action(change_pct_today: float) -> str:
    """
    Very simple intraday sentiment label based on price action.
    """
    if change_pct_today >= 3:
        return "momentum_buy"
    if 0.5 <= change_pct_today < 3:
        return "steady_buy"
    if -0.5 < change_pct_today < 0.5:
        return "neutral"
    if -3 < change_pct_today <= -0.5:
        return "cautious_buy"
    return "speculative_dip_buy"  # <= -3%


def _score_from_label(label: str, risk: RiskProfile) -> float:
    """
    Convert the label into a numeric score so we can rank symbols.
    Higher score = more attractive.
    """
    base_scores = {
        "momentum_buy": 80,
        "steady_buy": 70,
        "cautious_buy": 60,
        "speculative_dip_buy": 55,
        "neutral": 45,
        "hold": 40,
        "avoid": 20,
        "budget_too_small": 10,
    }
    base = base_scores.get(label, 40)

    # Slight tweak based on risk appetite
    if risk == RiskProfile.high and "buy" in label:
        base += 5
    if risk == RiskProfile.low and "speculative" in label:
        base -= 5

    # Clamp between 0 and 100
    return max(0.0, min(100.0, float(base)))


# ------------------------
# Core prediction builder
# ------------------------

def build_single_prediction(
    symbol: str,
    budget: float,
    risk: RiskProfile,
    fractional: bool,
) -> Dict:
    """
    Core logic that powers both /predict/{symbol} and /predict/batch.
    """
    # --- 1) Live quote from Finnhub ---
    quote = get_quote(symbol)
    if not quote or quote.get("c") in (None, 0):
        raise HTTPException(status_code=502, detail=f"Failed to fetch quote for {symbol}")

    price = float(quote["c"])
    prev_close = float(quote.get("pc") or 0)
    change_pct_today = _safe_pct_change(price, prev_close)

    # --- 2) Simple allocation logic ---
    allocation_factor = _allocation_factor_for_risk(risk)
    position_budget = budget * allocation_factor

    if price <= 0:
        max_shares = 0
    else:
        if fractional:
            max_shares = position_budget / price
        else:
            max_shares = int(position_budget // price)

    estimated_cost = max_shares * price

    allocation = {
        "allocation_factor": allocation_factor,
        "position_size_label": _position_label_for_risk(risk),
        "max_allocation": round(position_budget, 2),
        "shares_integer": int(max_shares) if not fractional else int(max_shares),
        "shares_fractional": float(max_shares),
        "estimated_cost_integer": int(estimated_cost),
        "fractional_mode": fractional,
    }

    # If we cannot even buy 1 share and fractional is false, call it budget_too_small
    if not fractional and allocation["shares_integer"] < 1:
        signal_label = "budget_too_small"
        signal_reason = "Budget and risk level are too low to buy even 1 share at the current price."
    else:
        signal_label = _label_from_price_action(change_pct_today)
        if signal_label == "momentum_buy":
            signal_reason = "Strong upside move today; riding positive momentum."
        elif signal_label == "steady_buy":
            signal_reason = "Small positive move (0–2%). Stable day; reasonable time to scale in."
        elif signal_label == "cautious_buy":
            signal_reason = "Slightly down today; could be a mild discount if fundamentals are strong."
        elif signal_label == "speculative_dip_buy":
            signal_reason = "Price dropped sharply today (≈ -3% or worse). Could be a dip but carries higher risk."
        elif signal_label == "neutral":
            signal_reason = "Flat or choppy day; patience recommended while trend develops."
        else:
            signal_reason = "No strong signal; preserve capital until setup improves."

    signal_score = _score_from_label(signal_label, risk)

    signal = {
        "label": signal_label,
        "score": signal_score,
        "reason": signal_reason,
    }

    # --- 3) Simple risk management plan ---
    rm_cfg = _risk_multipliers(risk)
    stop_loss_pct = rm_cfg["stop_loss_pct"]
    take_profit_pct = rm_cfg["take_profit_pct"]

    stop_loss_price = round(price * (1 + stop_loss_pct / 100.0), 3)
    take_profit_price = round(price * (1 + take_profit_pct / 100.0), 3)

    risk_management = {
        "stop_loss_pct": stop_loss_pct,
        "take_profit_pct": take_profit_pct,
        "stop_loss_price": stop_loss_price,
        "take_profit_price": take_profit_price,
    }

    # --- 4) Indicators (very light for now) ---
    # We keep this simple and inexpensive – candles are optional.
    indicators = {
        "rsi": None,
        "ema_fast": None,
        "ema_slow": None,
        "macd_hist": None,
        "volatility_score_numeric": None,
        "volatility_label": "unknown",
        "volume_spike": False,
        "indicator_score": 30,
        "indicator_trend_label": "unknown",
        "day_range_pct": None,
        "base_change_pct": change_pct_today,
        "prev_close": prev_close,
    }

    # Attempt to fetch recent candles – if Finnhub plan forbids it, we just keep indicators basic
    try:
        candles = get_candles(symbol, resolution="D", days=30)
        if candles and candles.get("s") == "ok":
            highs = candles.get("h") or []
            lows = candles.get("l") or []

            if highs and lows:
                recent_high = max(highs[-10:])
                recent_low = min(lows[-10:])
                if recent_low > 0:
                    day_range_pct = _safe_pct_change(recent_high, recent_low)
                else:
                    day_range_pct = 0.0
            else:
                day_range_pct = 0.0

            indicators["day_range_pct"] = day_range_pct

            # Crude volatility label
            if day_range_pct >= 10:
                indicators["volatility_label"] = "high"
                indicators["volatility_score_numeric"] = day_range_pct
                indicators["indicator_score"] = 40
            elif day_range_pct >= 5:
                indicators["volatility_label"] = "medium"
                indicators["volatility_score_numeric"] = day_range_pct
                indicators["indicator_score"] = 35
            else:
                indicators["volatility_label"] = "low"
                indicators["volatility_score_numeric"] = day_range_pct
                indicators["indicator_score"] = 30

            # Basic trend label from price change
            if change_pct_today > 0.5:
                indicators["indicator_trend_label"] = "bullish"
            elif change_pct_today < -0.5:
                indicators["indicator_trend_label"] = "bearish"
            else:
                indicators["indicator_trend_label"] = "neutral_or_choppy"

    except Exception:
        # If candles fail (403, plan limits, etc.), we just keep fallback indicators.
        pass

    # --- 5) Final decision (for UI & batch ranking) ---
    # Blend signal score + indicator_score
    final_score = (signal["score"] * 0.7) + (indicators["indicator_score"] * 0.3)

    if signal_label in ("budget_too_small", "avoid"):
        final_label = signal_label
    elif final_score >= 75:
        final_label = "strong_buy"
    elif final_score >= 60:
        final_label = "buy"
    elif final_score >= 50:
        final_label = "hold"
    else:
        final_label = "avoid"

    final_decision = {
        "label": final_label,
        "score": round(final_score, 1),
    }

    # --- 6) Natural language summary (for the app) ---
    summary = (
        f"{symbol.upper()} is trading around ${price:.2f} today. "
        f"Given your {risk.value} risk profile, this engine would allocate roughly "
        f"${allocation['max_allocation']:.2f} into this trade, "
        f"which corresponds to about {allocation['shares_fractional']:.2f} shares. "
        f"The current decision is '{final_label}'."
    )

    return {
        "symbol": symbol.upper(),
        "price": price,
        "change_pct_today": round(change_pct_today, 3),
        "budget": budget,
        "risk_profile": risk.value,
        "allocation": allocation,
        "risk_management": risk_management,
        "signal": signal,
        "indicators": indicators,
        "final_decision": final_decision,
        "raw_quote": quote,
        "summary": summary,
        "disclaimer": "This output is for informational and educational purposes only and is not financial advice.",
    }


# ------------------------
# Routes
# ------------------------

@app.get("/")
def root():
    return {
        "message": "StackIQ backend is live.",
        "endpoints": ["/health", "/quote/{symbol}", "/candles/{symbol}?resolution=D&days=30"],
    }


@app.get("/health")
def health():
    return {"status": "ok", "engine_ready": True, "message": "App is running. Engine uses Finnhub only."}


@app.get("/quote/{symbol}")
def quote(symbol: str):
    data = get_quote(symbol)
    if not data:
        raise HTTPException(status_code=502, detail="Failed to fetch quote from Finnhub.")
    return {"symbol": symbol.upper(), "quote": data}


@app.get("/candles/{symbol}")
def candles(
    symbol: str,
    resolution: str = Query("D", description="Finnhub resolution (e.g. 1, 5, 15, 30, 60, D, W, M)"),
    days: int = Query(30, gt=0, le=365, description="How many days back to fetch"),
):
    data = get_candles(symbol, resolution=resolution, days=days)
    if not data:
        raise HTTPException(status_code=502, detail="Failed to fetch candles from Finnhub.")
    return {"symbol": symbol.upper(), "resolution": resolution, "days": days, "data": data}


@app.get("/predict/{symbol}")
def predict_single(
    symbol: str,
    budget: float = Query(..., gt=0, description="Budget in USD for this single symbol"),
    risk: RiskProfile = Query(RiskProfile.medium, description="Risk profile (low, medium, high)"),
    fractional: bool = Query(True, description="Allow fractional shares"),
):
    """
    Single-symbol prediction engine.
    """
    prediction = build_single_prediction(symbol=symbol, budget=budget, risk=risk, fractional=fractional)
    return prediction


@app.post("/predict/batch")
def predict_batch(payload: BatchPredictRequest):
    """
    Multi-symbol prediction engine.

    - Runs the same core logic as /predict/{symbol}
    - Returns per-symbol predictions
    - Adds ranking + a best_pick object for your frontend
    """
    symbols_clean = [s.strip().upper() for s in payload.symbols if s.strip()]
    if not symbols_clean:
        raise HTTPException(status_code=400, detail="At least one non-empty symbol is required.")

    # Equal budget per symbol for now.
    per_symbol_budget = payload.budget / len(symbols_clean)

    results: Dict[str, Dict] = {}
    ranking: List[Dict] = []

    for sym in symbols_clean:
        pred = build_single_prediction(
            symbol=sym,
            budget=per_symbol_budget,
            risk=payload.risk,
            fractional=payload.fractional,
        )
        results[sym] = pred

        # Use final_decision score as main ranking metric.
        fd = pred.get("final_decision", {})
        ranking.append(
            {
                "symbol": sym,
                "label": fd.get("label", "unknown"),
                "score": float(fd.get("score", 0.0)),
            }
        )

    # Sort ranking high -> low
    ranking.sort(key=lambda x: x["score"], reverse=True)

    # Best pick = top of the ranking
    best_pick: Optional[Dict] = None
    if ranking:
        top = ranking[0]
        top_pred = results[top["symbol"]]
        best_pick = {
            "symbol": top["symbol"],
            "label": top["label"],
            "score": top["score"],
            "allocation": top_pred.get("allocation"),
            "risk_management": top_pred.get("risk_management"),
            "signal": top_pred.get("signal"),
            "summary": top_pred.get("summary"),
        }

    return {
        "symbols": symbols_clean,
        "meta": {
            "total_budget": payload.budget,
            "per_symbol_budget": round(per_symbol_budget, 2),
            "risk_profile": payload.risk.value,
            "fractional": payload.fractional,
        },
        "results": results,
        "ranking": ranking,
        "best_pick": best_pick,
        "disclaimer": "This output is for informational and educational purposes only and is not financial advice.",
    }













































































