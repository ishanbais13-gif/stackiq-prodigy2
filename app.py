import os
from enum import Enum
from typing import List, Dict, Any, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from data_fetcher import get_quote, get_candles  # uses Finnhub only

app = FastAPI(title="StackIQ API", version="1.0.0")

# --- CORS (so your frontend / test tools can hit this cleanly) ---

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # you can lock this down later
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DISCLAIMER_TEXT = (
    "This output is for informational and educational purposes only and is not financial advice."
)


# --- Risk profile enum & models ---

class RiskProfile(str, Enum):
    low = "low"
    medium = "medium"
    high = "high"


class BatchPredictRequest(BaseModel):
    symbols: List[str]
    budget: float
    risk: RiskProfile = RiskProfile.medium
    fractional: bool = True


# --- Utility: indicators + signals ------------------------------------------------


def compute_indicators(candles: Dict[str, Any]) -> Dict[str, Any]:
    """
    Compute some basic indicators from Finnhub candles.
    candles is expected to be a dict with keys:
    - c: list of close prices
    - h: list of highs
    - l: list of lows
    - o: list of opens
    - v: list of volume (optional)
    - t: list of timestamps
    """
    closes = candles.get("c") or []
    highs = candles.get("h") or []
    lows = candles.get("l") or []
    volumes = candles.get("v") or []
    timestamps = candles.get("t") or []

    if not closes or len(closes) < 5:
        return {
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
            "base_change_pct": None,
            "prev_close": None,
        }

    # basic stats
    last_close = closes[-1]
    prev_close = closes[-2] if len(closes) >= 2 else last_close

    day_range = (max(highs[-1], last_close) - min(lows[-1], last_close)) if highs and lows else 0
    day_range_pct = (day_range / last_close * 100) if last_close else 0
    base_change_pct = (last_close - prev_close) / prev_close * 100 if prev_close else 0

    # simple EMA
    def ema(values: List[float], period: int) -> Optional[float]:
        if len(values) < period:
            return None
        k = 2 / (period + 1)
        ema_val = values[0]
        for price in values[1:]:
            ema_val = price * k + ema_val * (1 - k)
        return ema_val

    ema_fast = ema(closes, 10)  # short EMA
    ema_slow = ema(closes, 21)  # long EMA

    macd_hist = None
    if ema_fast is not None and ema_slow is not None:
        macd = ema_fast - ema_slow
        macd_signal = macd * 0.8  # fake simple signal
        macd_hist = macd - macd_signal

    # simple volatility: std dev of last N closes
    import math

    window = closes[-20:] if len(closes) >= 20 else closes
    mean_price = sum(window) / len(window)
    variance = sum((p - mean_price) ** 2 for p in window) / len(window)
    std_dev = math.sqrt(variance) if len(window) > 1 else 0
    vol_pct = (std_dev / last_close * 100) if last_close else 0

    if vol_pct < 1.5:
        vol_label = "low"
        vol_score = 20
    elif vol_pct < 3:
        vol_label = "medium"
        vol_score = 40
    else:
        vol_label = "high"
        vol_score = 60

    # volume spike (if we have volume)
    volume_spike = False
    if volumes and len(volumes) >= 5:
        recent_vol = volumes[-1]
        avg_vol = sum(volumes[-5:]) / 5
        if avg_vol > 0 and recent_vol > 1.5 * avg_vol:
            volume_spike = True

    # indicator score: blend volatility and trend
    indicator_score = 0
    trend_label = "neutral_or_choppy"

    if ema_fast is not None and ema_slow is not None:
        if ema_fast > ema_slow and base_change_pct > 0:
            indicator_score += 25
            trend_label = "bullish"
        elif ema_fast < ema_slow and base_change_pct < 0:
            indicator_score += 10
            trend_label = "bearish"

    indicator_score += vol_score
    if volume_spike:
        indicator_score += 5

    # naive RSI (using price changes)
    gains = []
    losses = []
    for i in range(1, len(window)):
        diff = window[i] - window[i - 1]
        if diff >= 0:
            gains.append(diff)
        else:
            losses.append(-diff)
    avg_gain = sum(gains) / len(gains) if gains else 0.0001
    avg_loss = sum(losses) / len(losses) if losses else 0.0001
    rs = avg_gain / avg_loss if avg_loss != 0 else 100
    rsi = 100 - (100 / (1 + rs))

    return {
        "rsi": round(rsi, 2),
        "ema_fast": round(ema_fast, 4) if ema_fast is not None else None,
        "ema_slow": round(ema_slow, 4) if ema_slow is not None else None,
        "macd_hist": round(macd_hist, 6) if macd_hist is not None else None,
        "volatility_score_numeric": round(vol_pct, 4),
        "volatility_label": vol_label,
        "volume_spike": volume_spike,
        "indicator_score": round(indicator_score, 2),
        "indicator_trend_label": trend_label,
        "day_range_pct": round(day_range_pct, 4),
        "base_change_pct": round(base_change_pct, 4),
        "prev_close": round(prev_close, 2),
    }


def get_allocation_factor(risk: RiskProfile) -> float:
    if risk == RiskProfile.low:
        return 0.3
    if risk == RiskProfile.medium:
        return 0.5
    return 0.75  # high risk


def get_risk_mgmt_levels(price: float, risk: RiskProfile) -> Dict[str, Any]:
    # base settings
    if risk == RiskProfile.low:
        sl_pct, tp_pct = -5, 10
    elif risk == RiskProfile.medium:
        sl_pct, tp_pct = -10, 20
    else:  # high
        sl_pct, tp_pct = -15, 30

    stop_loss_price = price * (1 + sl_pct / 100.0)
    take_profit_price = price * (1 + tp_pct / 100.0)

    return {
        "stop_loss_pct": sl_pct,
        "take_profit_pct": tp_pct,
        "stop_loss_price": round(stop_loss_price, 3),
        "take_profit_price": round(take_profit_price, 3),
    }


def build_signal(
    change_pct_today: float,
    indicators: Dict[str, Any],
    risk: RiskProfile,
) -> Dict[str, Any]:
    """
    Combine basic daily change + indicators into a signal label & score.
    """
    base_change = indicators.get("base_change_pct") or change_pct_today
    indicator_score = indicators.get("indicator_score") or 0
    vol_label = indicators.get("volatility_label") or "unknown"
    rsi = indicators.get("rsi")

    score = 50  # start in the middle
    reason_bits = []

    # Price action contribution
    if base_change > 2:
        score += 10
        reason_bits.append("price is having a strong up day")
    elif base_change < -2:
        score -= 10
        reason_bits.append("price is having a weak/down day")

    # Trend / indicator score
    if indicator_score >= 60:
        score += 10
        reason_bits.append("technicals look strong")
    elif indicator_score <= 30:
        score -= 10
        reason_bits.append("technicals look soft or mixed")

    # RSI contribution
    if rsi is not None:
        if 40 <= rsi <= 60:
            score += 5
            reason_bits.append("RSI is in a healthy neutral zone")
        elif rsi < 30:
            score += 5
            reason_bits.append("RSI suggests the stock may be oversold")
        elif rsi > 70:
            score -= 5
            reason_bits.append("RSI suggests the stock may be overbought")

    # Volatility risk adjustment
    if vol_label == "high":
        score -= 5
        reason_bits.append("volatility is high, so risk is elevated")
    elif vol_label == "low":
        score += 5
        reason_bits.append("volatility is low/controlled")

    # Risk profile adjustment (more aggressive for high risk, more conservative for low)
    if risk == RiskProfile.low:
        score -= 5
        reason_bits.append("using a conservative tilt for low risk profile")
    elif risk == RiskProfile.high:
        score += 5
        reason_bits.append("using an aggressive tilt for high risk profile")

    # clamp score
    score = max(0, min(100, score))

    # label mapping
    if score >= 80:
        label = "strong_buy"
    elif score >= 60:
        label = "steady_buy"
    elif score >= 50:
        label = "cautious_buy"
    elif score >= 40:
        label = "speculative_dip_buy"
    else:
        label = "hold"

    reason = " ".join(reason_bits) if reason_bits else "Mixed signals; no strong edge either way."

    return {
        "label": label,
        "score": round(score, 1),
        "reason": reason,
    }


# --- Core prediction engine (single symbol) --------------------------------------


def run_prediction(
    symbol: str,
    budget: float,
    risk: RiskProfile,
    fractional: bool,
) -> Dict[str, Any]:
    symbol = symbol.upper().strip()
    if budget <= 0:
        raise HTTPException(status_code=400, detail="Budget must be positive.")

    # 1) Fetch live quote & candles
    quote = get_quote(symbol)
    if not quote or not isinstance(quote, dict) or quote.get("c") in (None, 0):
        raise HTTPException(status_code=502, detail=f"Failed to fetch quote for {symbol} from Finnhub.")

    price = float(quote.get("c", 0))
    prev_close = float(quote.get("pc", 0) or 0)
    change_pct_today = 0.0
    if prev_close:
        change_pct_today = (price - prev_close) / prev_close * 100.0

    candles = get_candles(symbol, resolution="D", days=30)

    # 2) Compute indicators
    indicators = compute_indicators(candles)

    # 3) Position sizing / allocation
    alloc_factor = get_allocation_factor(risk)
    max_allocation = budget * alloc_factor

    if price <= 0:
        raise HTTPException(status_code=400, detail="Price is invalid (<= 0).")

    if fractional:
        shares_fractional = max_allocation / price
        shares_integer = int(max_allocation // price)
        estimated_cost = shares_fractional * price
    else:
        shares_integer = int(max_allocation // price)
        shares_fractional = float(shares_integer)
        estimated_cost = shares_integer * price

    allocation = {
        "allocation_factor": alloc_factor,
        "position_size_label": "conservative" if risk == RiskProfile.low else ("medium" if risk == RiskProfile.medium else "aggressive"),
        "max_allocation": round(max_allocation, 2),
        "shares_integer": shares_integer,
        "shares_fractional": round(shares_fractional, 6),
        "estimated_cost_integer": round(estimated_cost, 2),
        "fractional_mode": fractional,
    }

    # 4) Risk management levels
    risk_mgmt = get_risk_mgmt_levels(price, risk)

    # 5) Build signal + final decision
    signal = build_signal(change_pct_today, indicators, risk)

    # final decision is basically the same but we keep a separate key in case we extend it later
    final_decision = {
        "label": signal["label"],
        "score": signal["score"],
    }

    # 6) Summary text
    risk_label = {
        RiskProfile.low: "low",
        RiskProfile.medium: "medium",
        RiskProfile.high: "high",
    }[risk]

    if allocation["shares_integer"] == 0 and not fractional:
        summary = (
            f"{symbol} is trading around ${price:.2f} today. With your {risk_label} risk profile and "
            f"a budget of ${budget:.2f}, this engine cannot buy even 1 full share. "
            f"You may want to enable fractional mode or increase the budget."
        )
    else:
        summary = (
            f"{symbol} is trading around ${price:.2f} today and is "
            f"{'up' if change_pct_today >= 0 else 'down'} on the session. "
            f"Given your {risk_label} risk profile, this engine would allocate roughly "
            f"${allocation['max_allocation']:.2f} into this trade, which corresponds to about "
            f"{allocation['shares_fractional']:.2f} shares. "
            f"The current decision is '{final_decision['label']}'."
        )

    return {
        "symbol": symbol,
        "price": round(price, 2),
        "change_pct_today": round(change_pct_today, 4),
        "budget": round(budget, 2),
        "risk_profile": risk_label,
        "allocation": allocation,
        "risk_management": risk_mgmt,
        "signal": signal,
        "indicators": indicators,
        "final_decision": final_decision,
        "raw_quote": {
            "c": quote.get("c"),
            "d": quote.get("d"),
            "dp": quote.get("dp"),
            "h": quote.get("h"),
            "l": quote.get("l"),
            "o": quote.get("o"),
            "pc": quote.get("pc"),
            "t": quote.get("t"),
        },
        "summary": summary,
        "disclaimer": DISCLAIMER_TEXT,
    }


# --- ROUTES ----------------------------------------------------------------------


@app.get("/")
async def root():
    return {
        "message": "StackIQ backend is live.",
        "endpoints": [
            "/health",
            "/quote/{symbol}",
            "/candles/{symbol}?resolution=D&days=30",
            "/predict/{symbol}?budget=...&risk=...",
            "/predict/batch",
        ],
    }


@app.get("/health")
async def health():
    return {"status": "ok", "mode": "base", "engine_ready": True, "message": "App is running"}


@app.get("/quote/{symbol}")
async def quote(symbol: str):
    data = get_quote(symbol.upper().strip())
    if not data:
        raise HTTPException(status_code=502, detail="Failed to fetch quote from Finnhub.")
    return {"symbol": symbol.upper().strip(), "quote": data}


@app.get("/candles/{symbol}")
async def candles(
    symbol: str,
    resolution: str = Query("D", description="Finnhub resolution (e.g., 1, 5, 15, 30, 60, D, W, M)"),
    days: int = Query(30, ge=1, le=365),
):
    data = get_candles(symbol.upper().strip(), resolution=resolution, days=days)
    if not data:
        raise HTTPException(status_code=502, detail="Failed to fetch candles from Finnhub.")
    return {
        "symbol": symbol.upper().strip(),
        "resolution": resolution,
        "days": days,
        "data": data,
    }


@app.get("/predict/{symbol}")
async def predict(
    symbol: str,
    budget: float = Query(..., gt=0),
    risk: RiskProfile = RiskProfile.medium,
    fractional: bool = False,
):
    """
    Single-symbol prediction using live Finnhub data.
    """
    return run_prediction(symbol, budget, risk, fractional)


@app.post("/predict/batch")
async def predict_batch(payload: BatchPredictRequest):
    """
    Multi-symbol prediction. Example body:

    {
      "symbols": ["NVDA", "SOFI", "OPEN"],
      "budget": 500,
      "risk": "medium",
      "fractional": true
    }
    """
    if not payload.symbols:
        raise HTTPException(status_code=400, detail="symbols list cannot be empty.")

    results: Dict[str, Any] = {}

    for raw_symbol in payload.symbols:
        sym = raw_symbol.upper().strip()
        if not sym:
            continue

        try:
            result = run_prediction(sym, payload.budget, payload.risk, payload.fractional)
            results[sym] = result
        except HTTPException as e:
            # keep the error in the payload instead of failing the whole batch
            results[sym] = {"error": e.detail}

    # Rank symbols that have valid final_decision scores
    scored: List[Dict[str, Any]] = []
    for sym, data in results.items():
        if isinstance(data, dict) and "final_decision" in data:
            score = data["final_decision"].get("score", 0)
            scored.append({"symbol": sym, "score": score, "data": data})

    scored.sort(key=lambda x: x["score"], reverse=True)

    top_pick = None
    if scored:
        best = scored[0]
        top_data = best["data"]
        top_pick = {
            "symbol": best["symbol"],
            "score": best["score"],
            "label": top_data["final_decision"].get("label"),
            "summary": top_data.get("summary"),
        }

    return {
        "symbols": payload.symbols,
        "results": results,
        "top_pick": top_pick,
        "disclaimer": DISCLAIMER_TEXT,
    }












































































