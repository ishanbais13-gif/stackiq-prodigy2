from typing import List, Literal, Optional, Dict, Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from data_fetcher import fetch_quote, fetch_candles


# -----------------------------
# FastAPI app setup
# -----------------------------

app = FastAPI(
    title="StackIQ API",
    version="1.0.0",
    description=(
        "Backend for StackIQ / Prodigynt.\n\n"
        "Features:\n"
        "- Live quotes via Finnhub\n"
        "- Historical candles\n"
        "- Single-symbol prediction & sizing\n"
        "- Multi-symbol batch prediction with best pick\n\n"
        "All outputs are for informational and educational purposes only and "
        "are **not** financial advice."
    ),
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # you can tighten this later for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# -----------------------------
# Models
# -----------------------------

RiskProfileLiteral = Literal["low", "medium", "high"]


class AllocationInfo(BaseModel):
    allocation_factor: float
    position_size_label: str
    max_allocation: float
    shares_integer: int
    shares_fractional: float
    estimated_cost_integer: float
    fractional_mode: bool


class RiskManagementInfo(BaseModel):
    stop_loss_pct: float
    take_profit_pct: float
    stop_loss_price: float
    take_profit_price: float


class SignalInfo(BaseModel):
    label: str
    score: float
    reason: str


class IndicatorInfo(BaseModel):
    rsi: Optional[float] = None
    ema_fast: Optional[float] = None
    ema_slow: Optional[float] = None
    macd_hist: Optional[float] = None
    volatility_score_numeric: Optional[float] = None
    volatility_label: str = "unknown"
    volume_spike: bool = False
    indicator_score: float = 0.0
    indicator_trend_label: str = "unknown"
    day_range_pct: Optional[float] = None
    base_change_pct: Optional[float] = None
    prev_close: Optional[float] = None


class FinalDecision(BaseModel):
    label: str
    score: float


class RawQuote(BaseModel):
    c: float  # current
    d: float  # change
    dp: float  # change %
    h: float  # high
    l: float  # low
    o: float  # open
    pc: float  # previous close
    t: int  # timestamp


class PredictResponse(BaseModel):
    symbol: str
    price: float
    change_pct_today: float
    budget: float
    risk_profile: RiskProfileLiteral
    allocation: AllocationInfo
    risk_management: RiskManagementInfo
    signal: SignalInfo
    indicators: IndicatorInfo
    final_decision: FinalDecision
    raw_quote: RawQuote

    # --- Day 8 additions ---
    expected_move: float
    confidence: float

    summary: str
    disclaimer: str



class BatchPredictRequest(BaseModel):
    symbols: List[str] = Field(..., description="Ticker symbols, e.g. ['NVDA', 'SOFI']")
    budget: float = Field(..., gt=0, description="Total portfolio budget across all symbols")
    risk: RiskProfileLiteral = Field("medium", description="Risk profile: low, medium, high")
    fractional: bool = Field(True, description="Allow fractional shares?")


class BatchMeta(BaseModel):
    total_budget: float
    per_symbol_budget: float
    risk_profile: RiskProfileLiteral
    fractional: bool


class BatchResult(BaseModel):
    symbols: List[str]
    meta: BatchMeta
    results: Dict[str, PredictResponse]
    best_pick: Dict[str, Any]


# -----------------------------
# Utility functions
# -----------------------------

RISK_CONFIG = {
    "low": {
        "allocation_factor": 0.3,
        "stop_loss_pct": -5.0,
        "take_profit_pct": 8.0,
    },
    "medium": {
        "allocation_factor": 0.5,
        "stop_loss_pct": -10.0,
        "take_profit_pct": 20.0,
    },
    "high": {
        "allocation_factor": 0.75,
        "stop_loss_pct": -15.0,
        "take_profit_pct": 30.0,
    },
}


def _safe_change_pct(quote: Dict[str, Any]) -> float:
    """Use dp if available, otherwise compute (c - pc) / pc * 100."""
    dp = quote.get("dp")
    if dp is not None:
        return float(dp)
    c = quote.get("c")
    pc = quote.get("pc")
    if c is None or pc in (None, 0):
        return 0.0
    return float((c - pc) / pc * 100.0)


def _build_signal(change_pct: float) -> SignalInfo:
    """
    Very simple day-trade style signal:
    - Strong up move => momentum_buy
    - Small up move => steady_buy
    - Flat / choppy => hold
    - Mild down => cautious_buy (possible discount)
    - Big down => speculative_dip_buy
    """
    if change_pct >= 3:
        return SignalInfo(
            label="momentum_buy",
            score=80,
            reason="Strong positive move today (>= +3%). Momentum could continue but risk is higher.",
        )
    if 0.5 <= change_pct < 3:
        return SignalInfo(
            label="steady_buy",
            score=65,
            reason="Small positive move (0.5–3%). Stable day; reasonable time to scale in.",
        )
    if -0.5 < change_pct < 0.5:
        return SignalInfo(
            label="hold",
            score=50,
            reason="Flat or slightly choppy day. No strong edge either way.",
        )
    if -3 <= change_pct <= -0.5:
        return SignalInfo(
            label="cautious_buy",
            score=60,
            reason="Slightly down today; could be a mild discount if fundamentals are strong.",
        )
    # change_pct < -3
    return SignalInfo(
        label="speculative_dip_buy",
        score=55,
        reason="Price dropped sharply today (<= -3%). Could be a dip but carries higher risk.",
    )


def _position_size_label(allocation_factor: float) -> str:
    if allocation_factor <= 0.35:
        return "small"
    if allocation_factor <= 0.6:
        return "medium"
    return "aggressive"


def _compute_predict_payload(
    symbol: str,
    quote: Dict[str, Any],
    budget: float,
    risk: RiskProfileLiteral,
    fractional: bool,
) -> PredictResponse:
    price = float(quote.get("c") or 0.0)
    if price <= 0:
        raise HTTPException(
            status_code=502,
            detail="Received invalid price from data provider.",
        )

    change_pct_today = _safe_change_pct(quote)
    risk_conf = RISK_CONFIG[risk]

    # --- Risk config / base params ---
    allocation_factor = risk_conf["allocation_factor"]
    stop_loss_pct = risk_conf["stop_loss_pct"]
    take_profit_pct = risk_conf["take_profit_pct"]

    # --- Day 8: expected move & intraday range ---
    day_high = float(quote.get("h") or 0.0)
    day_low = float(quote.get("l") or 0.0)

    if day_high > 0 and day_low > 0 and price > 0:
        raw_range = max(day_high - day_low, 0.0)
        day_range_pct = (raw_range / price) * 100.0 if price > 0 else None
    else:
        # Fallback: use today's % move as a proxy
        raw_range = abs(price * change_pct_today / 100.0)
        day_range_pct = None

    # Never let expected_move be tiny – at least 1% of price
    expected_move = round(max(raw_range, price * 0.01), 2)

    # --- Position sizing ---
    max_allocation = budget * allocation_factor
    if max_allocation <= 0:
        shares_fractional = 0.0
    else:
        shares_fractional = max_allocation / price

    shares_integer = int(shares_fractional) if fractional else int(max_allocation // price)
    if shares_integer < 0:
        shares_integer = 0

    estimated_cost_integer = round(shares_integer * price, 2)

    allocation = AllocationInfo(
        allocation_factor=allocation_factor,
        position_size_label=_position_size_label(allocation_factor),
        max_allocation=round(max_allocation, 2),
        shares_integer=shares_integer,
        shares_fractional=round(shares_fractional, 4),
        estimated_cost_integer=estimated_cost_integer,
        fractional_mode=fractional,
    )

    # --- Risk management: stop & target prices ---
    stop_loss_price = round(price * (1 + stop_loss_pct / 100.0), 3)
    take_profit_price = round(price * (1 + take_profit_pct / 100.0), 3)

    risk_mgmt = RiskManagementInfo(
        stop_loss_pct=stop_loss_pct,
        take_profit_pct=take_profit_pct,
        stop_loss_price=stop_loss_price,
        take_profit_price=take_profit_price,
    )

    # --- Signal & indicators ---
    signal = _build_signal(change_pct_today)

    indicators = IndicatorInfo(
        volatility_label="unknown",
        volatility_score_numeric=None,
        volume_spike=False,
        indicator_score=0.0,
        indicator_trend_label="unknown",
        day_range_pct=day_range_pct,
        base_change_pct=change_pct_today,
        prev_close=float(quote.get("pc") or 0.0),
        rsi=None,
        ema_fast=None,
        ema_slow=None,
        macd_hist=None,
    )

    # --- Final decision / score ---
    final_score = signal.score
    # Tiny adjustment based on risk profile + direction
    if risk == "high" and change_pct_today > 0:
        final_score += 5
    if risk == "low" and change_pct_today < 0:
        final_score += 5

    final_label = signal.label if final_score >= 50 else "avoid"
    final_decision = FinalDecision(label=final_label, score=final_score)

    # --- Day 8: confidence score (0.30–0.95) ---
    confidence = round(max(0.30, min(final_score / 100.0, 0.95)), 2)

    # --- Raw quote passthrough ---
    raw_quote = RawQuote(
        c=float(quote.get("c") or 0.0),
        d=float(quote.get("d") or 0.0),
        dp=float(quote.get("dp") or 0.0),
        h=day_high,
        l=day_low,
        o=float(quote.get("o") or 0.0),
        pc=float(quote.get("pc") or 0.0),
        t=int(quote.get("t") or 0),
    )

    summary = (
        f"{symbol.upper()} is trading around ${price:.2f} today. "
        f"Change on the session is {change_pct_today:+.2f}%. "
        f"Given your {risk} risk profile, this engine would allocate roughly "
        f"${max_allocation:.2f} into this trade, which corresponds to about "
        f"{shares_fractional:.2f} shares. The current decision is '{final_label}'."
    )

    disclaimer = (
        "This output is for informational and educational purposes only and "
        "is not financial advice."
    )

    return PredictResponse(
        symbol=symbol.upper(),
        price=price,
        change_pct_today=change_pct_today,
        budget=budget,
        risk_profile=risk,
        allocation=allocation,
        risk_management=risk_mgmt,
        signal=signal,
        indicators=indicators,
        final_decision=final_decision,
        raw_quote=raw_quote,
        expected_move=expected_move,
        confidence=confidence,
        summary=summary,
        disclaimer=disclaimer,
    )

async def root() -> Dict[str, Any]:
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
async def health() -> Dict[str, str]:
    return {"status": "ok", "message": "StackIQ backend is running"}


@app.get("/quote/{symbol}")
async def get_quote(symbol: str) -> Dict[str, Any]:
    try:
        quote = fetch_quote(symbol)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to fetch quote: {e}")
    if not quote or "c" not in quote:
        raise HTTPException(status_code=502, detail="Invalid quote received from Finnhub.")
    return {"symbol": symbol.upper(), "quote": quote}


@app.get("/candles/{symbol}")
async def get_candles(
    symbol: str,
    resolution: str = "D",
    days: int = 30,
) -> Dict[str, Any]:
    try:
        data = fetch_candles(symbol, resolution=resolution, days=days)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to fetch candles from Finnhub: {e}")

    # Finnhub returns { s: "ok"|"no_data"|"error", t: [...], c: [...], ... }
    status = data.get("s")
    if status != "ok":
        raise HTTPException(
            status_code=502,
            detail=f"Finnhub returned status '{status}' for candles.",
        )

    return {
        "symbol": symbol.upper(),
        "resolution": resolution,
        "days": days,
        "data": data,
    }


@app.get("/predict/{symbol}", response_model=PredictResponse)
async def predict_single(
    symbol: str,
    budget: float,
    risk: RiskProfileLiteral = "medium",
    fractional: bool = True,
) -> PredictResponse:
    if budget <= 0:
        raise HTTPException(status_code=400, detail="Budget must be positive.")

    try:
        quote = fetch_quote(symbol)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to fetch quote: {e}")

    if not quote or "c" not in quote:
        raise HTTPException(status_code=502, detail="Invalid quote received from Finnhub.")

    return _compute_predict_payload(symbol, quote, budget, risk, fractional)


@app.post("/predict/batch", response_model=BatchResult)
@app.post("/predict/batch", response_model=BatchResult)
async def predict_batch(request: BatchPredictRequest) -> BatchResult:
    if not request.symbols:
        raise HTTPException(status_code=400, detail="At least one symbol is required.")

    per_symbol_budget = request.budget / len(request.symbols)

    results: Dict[str, PredictResponse] = {}
    ranking: List[Dict[str, Any]] = []

    for raw_symbol in request.symbols:
        symbol = raw_symbol.upper()
        try:
            quote = fetch_quote(symbol)
            if not quote or "c" not in quote:
                raise ValueError("Invalid quote payload")

            prediction = _compute_predict_payload(
                symbol=symbol,
                quote=quote,
                budget=per_symbol_budget,
                risk=request.risk,
                fractional=request.fractional,
            )

            results[symbol] = prediction

            # Day 9: include expected_move + confidence for best_pick
            ranking.append(
                {
                    "symbol": symbol,
                    "score": prediction.final_decision.score,
                    "label": prediction.final_decision.label,
                    "change_pct_today": prediction.change_pct_today,
                    "expected_move": prediction.expected_move,
                    "confidence": prediction.confidence,
                }
            )

        except Exception as e:
            # On failure, capture a minimal error entry instead of killing the whole request
            results[symbol] = PredictResponse(
                symbol=symbol,
                price=0.0,
                change_pct_today=0.0,
                budget=per_symbol_budget,
                risk_profile=request.risk,
                allocation=AllocationInfo(
                    allocation_factor=RISK_CONFIG[request.risk]["allocation_factor"],
                    position_size_label="error",
                    max_allocation=0.0,
                    shares_integer=0,
                    shares_fractional=0.0,
                    estimated_cost_integer=0.0,
                    fractional_mode=request.fractional,
                ),
                risk_management=RiskManagementInfo(
                    stop_loss_pct=0.0,
                    take_profit_pct=0.0,
                    stop_loss_price=0.0,
                    take_profit_price=0.0,
                ),
                signal=SignalInfo(
                    label="error",
                    score=0.0,
                    reason=f"Failed to compute prediction: {e}",
                ),
                indicators=IndicatorInfo(
                    volatility_label="error",
                    volatility_score_numeric=None,
                    volume_spike=False,
                    indicator_score=0.0,
                    indicator_trend_label="unknown",
                    day_range_pct=None,
                    base_change_pct=0.0,
                    prev_close=0.0,
                    rsi=None,
                    ema_fast=None,
                    ema_slow=None,
                    macd_hist=None,
                ),
                final_decision=FinalDecision(label="error", score=0.0),
                raw_quote=RawQuote(
                    c=0.0,
                    d=0.0,
                    dp=0.0,
                    h=0.0,
                    l=0.0,
                    o=0.0,
                    pc=0.0,
                    t=0,
                ),
                expected_move=0.0,
                confidence=0.0,
                summary=f"Could not compute prediction for {symbol} due to an error.",
                disclaimer=(
                    "This output is for informational and educational purposes only "
                    "and is not financial advice."
                ),
            )

    # Day 9: rank by score then confidence
    ranking_sorted = sorted(
        ranking,
        key=lambda x: (x["score"], x["confidence"]),
        reverse=True,
    )

    best_pick = (
        ranking_sorted[0]
        if ranking_sorted
        else {"symbol": None, "score": 0.0, "confidence": 0.0}
    )

    meta = BatchMeta(
        total_budget=request.budget,
        per_symbol_budget=round(per_symbol_budget, 2),
        risk_profile=request.risk,
        fractional=request.fractional,
    )

    return BatchResult(
        symbols=[s.upper() for s in request.symbols],
        meta=meta,
        
    )

        
    )# ============================================================
# DAY 7 – BACKTEST + OPTIMIZATION ENGINE (FULL BLOCK)
# PASTE THIS DIRECTLY AFTER THE BatchResult RETURN
# ============================================================

from typing import Dict, Any, List
import statistics
import math


# ------------------------------------------------------------
# Helper: Simple backtest using daily candles
# ------------------------------------------------------------
def _run_simple_backtest(
    symbol: str,
    candles: Dict[str, Any],
    initial_budget: float,
) -> Dict[str, Any]:

    # Need at least 10 candles minimum
    if not candles or "c" not in candles or len(candles["c"]) < 10:
        return {
            "symbol": symbol,
            "error": "Not enough candle data",
            "profit_pct": 0.0,
            "final_value": initial_budget,
            "trades": 0,
            "notes": "Insufficient price history"
        }

    close = candles["c"]
    trades = 0
    cash = initial_budget
    position = 0  # shares
    last_price = close[0]

    for price in close[1:]:
        # If price jumps 1%+ above last day → buy
        if price > last_price * 1.01 and cash > price:
            position = cash / price
            cash = 0
            trades += 1

        # If price drops 1%+ → sell
        if position > 0 and price < last_price * 0.99:
            cash = position * price
            position = 0
            trades += 1

        last_price = price

    # Close any open position at end
    if position > 0:
        cash = position * close[-1]
        position = 0

    profit_pct = ((cash - initial_budget) / initial_budget) * 100.0

    return {
        "symbol": symbol,
        "profit_pct": round(profit_pct, 2),
        "final_value": round(cash, 2),
        "trades": trades,
        "notes": "Backtest complete"
    }


# ------------------------------------------------------------
# API: /backtest/{symbol}
# ------------------------------------------------------------
@app.get("/backtest/{symbol}")
async def backtest_symbol(symbol: str, budget: float = 1000.0):
    """
    Run a simple backtest using candle data from Finnhub
    """
    try:
        candles = fetch_candles(symbol.upper())
    except Exception as e:
        return {
            "symbol": symbol.upper(),
            "error": f"Failed to fetch candles: {e}"
        }

    result = _run_simple_backtest(symbol.upper(), candles, budget)
    return result


# ------------------------------------------------------------
# Optimization logic: Try multiple parameter combos
# ------------------------------------------------------------
def _run_optimization(
    symbol: str,
    candles: Dict[str, Any],
    budget: float
) -> Dict[str, Any]:

    if not candles or "c" not in candles or len(candles["c"]) < 10:
        return {
            "symbol": symbol,
            "error": "Not enough data for optimization"
        }

    best = None

    # Try multiple thresholds for buy/sell
    buy_thresholds = [0.005, 0.01, 0.02]
    sell_thresholds = [0.005, 0.01, 0.02]

    for buy_t in buy_thresholds:
        for sell_t in sell_thresholds:
            # Custom backtest simulation
            close = candles["c"]
            cash = budget
            position = 0
            last_price = close[0]

            for price in close[1:]:
                # BUY RULE
                if price > last_price * (1 + buy_t) and cash > price:
                    position = cash / price
                    cash = 0

                # SELL RULE
                if position > 0 and price < last_price * (1 - sell_t):
                    cash = position * price
                    position = 0

                last_price = price

            if position > 0:
                cash = position * close[-1]

            profit_pct = ((cash - budget) / budget) * 100

            config = {
                "buy_threshold": buy_t,
                "sell_threshold": sell_t,
                "profit_pct": round(profit_pct, 2)
            }

            if best is None or config["profit_pct"] > best["profit_pct"]:
                best = config

    return {
        "symbol": symbol.upper(),
        "best_strategy": best,
        "notes": "Optimization complete"
    }


# ------------------------------------------------------------
# API: /optimize/{symbol}
# ------------------------------------------------------------
@app.get("/optimize/{symbol}")
async def optimize_symbol(symbol: str, budget: float = 1000.0):
    """
    Try multiple backtest strategies & return the best result
    """
    try:
        candles = fetch_candles(symbol.upper())
    except Exception as e:
        return {
            "symbol": symbol.upper(),
            "error": f"Failed to fetch candles: {e}"
        }

    result = _run_optimization(symbol.upper(), candles, budget)
    return result
# -------------------------------------------------------------



































































