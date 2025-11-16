import os
from typing import List, Literal, Optional, Dict, Any

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from data_fetcher import fetch_quote, fetch_candles

app = FastAPI(
    title="StackIQ API",
    version="1.0.0",
    description="Backend for StackIQ / Prodigynt stock analysis engine."
)


# -----------------------------
# Models
# -----------------------------

RiskProfile = Literal["low", "medium", "high"]


class BatchPredictRequest(BaseModel):
    symbols: List[str]
    budget: float
    risk: RiskProfile
    fractional: bool = True


# -----------------------------
# Helper: core prediction logic
# -----------------------------

def build_single_prediction(
    symbol: str,
    budget: float,
    risk_profile: RiskProfile,
    fractional: bool
) -> Dict[str, Any]:
    """
    Core logic used by /predict/{symbol} and /predict/batch.
    Fetches quote, computes allocation, simple risk management,
    a signal, indicators placeholder, and a final decision score.
    """

    quote = fetch_quote(symbol)
    if not quote or "c" not in quote or quote["c"] == 0:
        raise HTTPException(status_code=502, detail=f"Failed to fetch quote for {symbol}")

    price = quote["c"]
    prev_close = quote.get("pc", price)
    try:
        change_pct_today = round(((price - prev_close) / prev_close) * 100, 3)
    except ZeroDivisionError:
        change_pct_today = 0.0

    # ----- Allocation -----
    allocation_factor_map = {"low": 0.3, "medium": 0.5, "high": 0.75}
    allocation_factor = allocation_factor_map.get(risk_profile, 0.5)

    position_label = risk_profile
    max_allocation = budget

    shares_integer = int(budget // price) if price > 0 else 0
    shares_fractional = (budget / price) if price > 0 else 0.0

    # ----- Risk management -----
    stop_loss_pct_map = {"low": -5, "medium": -10, "high": -15}
    take_profit_pct_map = {"low": 5, "medium": 10, "high": 15}

    stop_loss_pct = stop_loss_pct_map.get(risk_profile, -10)
    take_profit_pct = take_profit_pct_map.get(risk_profile, 10)

    stop_loss_price = round(price * (1 + stop_loss_pct / 100), 3)
    take_profit_price = round(price * (1 + take_profit_pct / 100), 3)

    # ----- Signal + scoring -----
    # Very simple logic for now – we can make this smarter later.
    if change_pct_today > 3:
        signal_label = "bullish_breakout"
        signal_score = 80
        reason = "Strong upside momentum today; potential breakout."
    elif 0 < change_pct_today <= 3:
        signal_label = "steady_buy"
        signal_score = 65
        reason = "Small positive move; reasonable time to scale in."
    elif -2 <= change_pct_today <= 0:
        signal_label = "cautious_buy"
        signal_score = 60
        reason = "Slightly down or flat; could be a mild discount."
    elif -5 <= change_pct_today < -2:
        signal_label = "speculative_dip_buy" if risk_profile != "low" else "hold"
        signal_score = 55 if risk_profile != "low" else 45
        reason = "Price dropped noticeably; may be a dip but carries higher risk."
    else:
        signal_label = "hold"
        signal_score = 40
        reason = "Weak / uncertain price action; better to be patient."

    # final_decision roughly matches signal but could be tweaked later
    final_label = signal_label
    final_score = signal_score

    # ----- Indicators placeholder -----
    # (Later we can wire candles + RSI/EMA/etc here.)
    indicators = {
        "rsi": None,
        "ema_fast": None,
        "ema_slow": None,
        "macd_hist": None,
        "volatility_score_numeric": None,
        "volatility_label": "unknown",
        "volume_spike": False,
        "indicator_score": 0,
        "indicator_trend_label": "unknown",
        "day_range_pct": None,
        "base_change_pct": change_pct_today,
        "prev_close": prev_close,
    }

    # ----- Summary text -----
    summary = (
        f"{symbol} is trading around ${price:.2f} today. "
        f"Daily move is {change_pct_today:.2f}%. "
        f"Given your {risk_profile} risk profile, this engine would allocate roughly "
        f"${budget:.2f} into this trade."
    )

    # ----- Assemble prediction object -----
    prediction = {
        "symbol": symbol,
        "price": price,
        "change_pct_today": change_pct_today,
        "budget": budget,
        "risk_profile": risk_profile,
        "allocation": {
            "allocation_factor": allocation_factor,
            "position_size_label": position_label,
            "max_allocation": max_allocation,
            "shares_integer": shares_integer,
            "shares_fractional": round(shares_fractional, 6),
            "estimated_cost_integer": round(shares_integer * price, 2),
            "fractional_mode": fractional,
        },
        "risk_management": {
            "stop_loss_pct": stop_loss_pct,
            "take_profit_pct": take_profit_pct,
            "stop_loss_price": stop_loss_price,
            "take_profit_price": take_profit_price,
        },
        "signal": {
            "label": signal_label,
            "score": signal_score,
            "reason": reason,
        },
        "indicators": indicators,
        "raw_quote": quote,
        "final_decision": {
            "label": final_label,
            "score": final_score,
        },
        "summary": summary,
        "disclaimer": "This output is for informational and educational purposes only and is not financial advice.",
    }

    return prediction


# -----------------------------
# Basic endpoints
# -----------------------------

@app.get("/")
def root():
    return {
        "message": "StackIQ backend is live.",
        "endpoints": [
            "/health",
            "/quote/{symbol}",
            "/candles/{symbol}?resolution=D&days=30",
            "/predict/{symbol}?budget=100&risk=medium&fractional=true",
            "/predict/batch",
        ],
    }


@app.get("/health")
def health():
    return {
        "status": "ok",
        "message": "StackIQ backend is running"
    }


# -----------------------------
# Market data endpoints
# -----------------------------

@app.get("/quote/{symbol}")
def get_quote(symbol: str):
    data = fetch_quote(symbol)
    if not data:
        raise HTTPException(status_code=502, detail="Failed to fetch quote from Finnhub.")
    return {"symbol": symbol.upper(), "quote": data}


@app.get("/candles/{symbol}")
def get_candles(
    symbol: str,
    resolution: str = Query("D", description="Resolution, e.g. 1, 5, 15, 30, 60, D, W, M"),
    days: int = Query(30, ge=1, le=365, description="Number of days of history")
):
    data = fetch_candles(symbol, resolution=resolution, days=days)
    if not data:
        raise HTTPException(status_code=502, detail="Failed to fetch candles from Finnhub.")
    # We return raw data; frontend can handle error payload if Finnhub says 403, etc.
    return {
        "symbol": symbol.upper(),
        "resolution": resolution,
        "days": days,
        "data": data,
    }


# -----------------------------
# Prediction endpoints
# -----------------------------

@app.get("/predict/{symbol}")
def predict_symbol(
    symbol: str,
    budget: float = Query(..., gt=0),
    risk: RiskProfile = Query("medium"),
    fractional: bool = Query(True),
):
    """
    Single-symbol prediction. Uses core build_single_prediction logic.
    """
    prediction = build_single_prediction(
        symbol=symbol.upper(),
        budget=budget,
        risk_profile=risk,
        fractional=fractional,
    )
    return prediction


@app.post("/predict/batch")
def predict_batch(request: BatchPredictRequest):
    """
    Multi-symbol prediction.
    - Splits total budget evenly per symbol
    - Runs same logic as /predict/{symbol} per ticker
    - Adds ranking + best_pick based on final_decision score
    """
    try:
        symbols = [s.upper() for s in request.symbols if s.strip()]
        if not symbols:
            raise HTTPException(status_code=400, detail="No symbols provided.")

        total_budget = request.budget
        risk_profile = request.risk
        fractional = request.fractional

        per_symbol_budget = total_budget / len(symbols)

        results: Dict[str, Any] = {}
        ranking_rows: List[Dict[str, Any]] = []

        for sym in symbols:
            try:
                pred = build_single_prediction(
                    symbol=sym,
                    budget=per_symbol_budget,
                    risk_profile=risk_profile,
                    fractional=fractional,
                )
            except HTTPException:
                # Skip symbols that fail data fetch
                continue

            results[sym] = pred

            score = pred.get("final_decision", {}).get("score", 0)
            change_pct = pred.get("change_pct_today", 0.0)
            allocation_factor = pred.get("allocation", {}).get("allocation_factor", 0.5)

            ranking_rows.append(
                {
                    "symbol": sym,
                    "score": score,
                    "change_pct_today": change_pct,
                    "allocation_factor": allocation_factor,
                    "signal": pred.get("signal", {}).get("label", "unknown"),
                }
            )

        # Sort and rank
        ranking_rows.sort(key=lambda r: r["score"], reverse=True)

        best_pick: Optional[Dict[str, Any]] = ranking_rows[0] if ranking_rows else None

        for idx, row in enumerate(ranking_rows):
            row["rank"] = idx + 1

        response = {
            "symbols": symbols,
            "meta": {
                "total_budget": total_budget,
                "per_symbol_budget": per_symbol_budget,
                "risk_profile": risk_profile,
                "fractional": fractional,
            },
            "results": results,
            "rankings": ranking_rows,
            "best_pick": best_pick,
            "disclaimer": "This output is for informational and educational purposes only and is not financial advice.",
        }

        return response

    except HTTPException:
        # Re-raise HTTP exceptions as-is
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))














































































