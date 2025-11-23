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
    allow_origins=["*"],  # tighten later for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# -----------------------------
# MODELS
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
    c: float
    d: float
    dp: float
    h: float
    l: float
    o: float
    pc: float
    t: int


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

    # Day 8
    expected_move: float
    confidence: float

    summary: str
    disclaimer: str


class BatchPredictRequest(BaseModel):
    symbols: List[str]
    budget: float = Field(..., gt=0)
    risk: RiskProfileLiteral = "medium"
    fractional: bool = True


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
# UTILITY
# -----------------------------

RISK_CONFIG = {
    "low": {"allocation_factor": 0.3, "stop_loss_pct": -5.0, "take_profit_pct": 8.0},
    "medium": {"allocation_factor": 0.5, "stop_loss_pct": -10.0, "take_profit_pct": 20.0},
    "high": {"allocation_factor": 0.75, "stop_loss_pct": -15.0, "take_profit_pct": 30.0},
}


def _safe_change_pct(q: Dict[str, Any]) -> float:
    dp = q.get("dp")
    if dp is not None:
        return float(dp)
    c, pc = q.get("c"), q.get("pc")
    if c is None or pc in (None, 0):
        return 0.0
    return (c - pc) / pc * 100.0


def _build_signal(ch: float) -> SignalInfo:
    if ch >= 3:
        return SignalInfo("momentum_buy", 80, "Strong positive move (>=3%).")
    if 0.5 <= ch < 3:
        return SignalInfo("steady_buy", 65, "Small stable gain (0.5–3%).")
    if -0.5 < ch < 0.5:
        return SignalInfo("hold", 50, "Flat or choppy.")
    if -3 <= ch <= -0.5:
        return SignalInfo("cautious_buy", 60, "Slight dip, possible discount.")
    return SignalInfo("speculative_dip_buy", 55, "Large dip (<=-3%).")


def _position_size_label(a: float) -> str:
    if a <= 0.35:
        return "small"
    if a <= 0.6:
        return "medium"
    return "aggressive"


# -----------------------------
# SINGLE PREDICT
# -----------------------------

def _compute_predict_payload(symbol, quote, budget, risk, fractional) -> PredictResponse:
    price = float(quote.get("c") or 0.0)
    if price <= 0:
        raise HTTPException(status_code=502, detail="Invalid price from Finnhub.")

    ch = _safe_change_pct(quote)
    cfg = RISK_CONFIG[risk]

    # Expected move (Day 8)
    h = float(quote.get("h") or 0.0)
    l = float(quote.get("l") or 0.0)
    raw = max(h - l, 0.0)
    expected_move = round(max(raw, price * 0.01), 2)

    alloc_factor = cfg["allocation_factor"]
    max_alloc = budget * alloc_factor
    shares_frac = max_alloc / price if max_alloc > 0 else 0.0
    shares_int = int(shares_frac) if fractional else int(max_alloc // price)

    alloc = AllocationInfo(
        allocation_factor=alloc_factor,
        position_size_label=_position_size_label(alloc_factor),
        max_allocation=round(max_alloc, 2),
        shares_integer=shares_int,
        shares_fractional=round(shares_frac, 4),
        estimated_cost_integer=round(shares_int * price, 2),
        fractional_mode=fractional,
    )

    # Risk mgmt
    sl = round(price * (1 + cfg["stop_loss_pct"] / 100.0), 3)
    tp = round(price * (1 + cfg["take_profit_pct"] / 100.0), 3)
    risk_mgmt = RiskManagementInfo(
        stop_loss_pct=cfg["stop_loss_pct"],
        take_profit_pct=cfg["take_profit_pct"],
        stop_loss_price=sl,
        take_profit_price=tp,
    )

    signal = _build_signal(ch)

    indicators = IndicatorInfo(
        day_range_pct=(raw / price * 100.0) if price > 0 else None,
        base_change_pct=ch,
        prev_close=float(quote.get("pc") or 0.0),
    )

    # Final score + confidence
    final_score = signal.score
    if risk == "high" and ch > 0:
        final_score += 5
    if risk == "low" and ch < 0:
        final_score += 5

    final_label = signal.label if final_score >= 50 else "avoid"

    final_decision = FinalDecision(label=final_label, score=final_score)
    confidence = round(max(0.30, min(final_score / 100.0, 0.95)), 2)

    raw_q = RawQuote(
        c=price,
        d=float(quote.get("d") or 0.0),
        dp=float(quote.get("dp") or 0.0),
        h=h,
        l=l,
        o=float(quote.get("o") or 0.0),
        pc=float(quote.get("pc") or 0.0),
        t=int(quote.get("t") or 0),
    )

    summary = (
        f"{symbol} trading at ${price:.2f}. "
        f"Move today: {ch:+.2f}%. Allocation: ${max_alloc:.2f}. "
        f"Decision: {final_label}."
    )

    disclaimer = "This output is for educational purposes only and not financial advice."

    return PredictResponse(
        symbol=symbol,
        price=price,
        change_pct_today=ch,
        budget=budget,
        risk_profile=risk,
        allocation=alloc,
        risk_management=risk_mgmt,
        signal=signal,
        indicators=indicators,
        final_decision=final_decision,
        raw_quote=raw_q,
        expected_move=expected_move,
        confidence=confidence,
        summary=summary,
        disclaimer=disclaimer,
    )


# -----------------------------
# ROUTES
# -----------------------------

@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/quote/{symbol}")
async def get_quote(symbol: str):
    quote = fetch_quote(symbol)
    if not quote or "c" not in quote:
        raise HTTPException(status_code=502, detail="Invalid quote.")
    return {"symbol": symbol.upper(), "quote": quote}


@app.get("/candles/{symbol}")
async def get_candles(symbol: str, resolution: str = "D", days: int = 30):
    data = fetch_candles(symbol, resolution=resolution, days=days)
    if data.get("s") != "ok":
        raise HTTPException(status_code=502, detail="Candle fetch error.")
    return {"symbol": symbol.upper(), "resolution": resolution, "days": days, "data": data}


@app.get("/predict/{symbol}", response_model=PredictResponse)
async def predict_single(symbol: str, budget: float, risk: RiskProfileLiteral = "medium", fractional: bool = True):
    quote = fetch_quote(symbol)
    if not quote or "c" not in quote:
        raise HTTPException(status_code=502, detail="Invalid quote.")
    return _compute_predict_payload(symbol.upper(), quote, budget, risk, fractional)


# -----------------------------
# DAY 9 — BATCH PREDICT
# -----------------------------

@app.post("/predict/batch", response_model=BatchResult)
async def predict_batch(request: BatchPredictRequest):
    if request.budget <= 0:
        raise HTTPException(status_code=400, detail="Budget must be positive.")

    per_symbol = request.budget / len(request.symbols)
    results = {}
    ranking = []

    for sym in request.symbols:
        s = sym.upper()
        try:
            quote = fetch_quote(s)
            if not quote or "c" not in quote:
                raise Exception("Bad quote")

            pred = _compute_predict_payload(
                symbol=s,
                quote=quote,
                budget=per_symbol,
                risk=request.risk,
                fractional=request.fractional,
            )

            results[s] = pred
            ranking.append(
                {
                    "symbol": s,
                    "score": pred.final_decision.score,
                    "confidence": pred.confidence,
                    "expected_move": pred.expected_move,
                }
            )

        except Exception as e:
            results[s] = PredictResponse(
                symbol=s,
                price=0.0,
                change_pct_today=0.0,
                budget=per_symbol,
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
                signal=SignalInfo(label="error", score=0.0, reason=f"Failed: {e}"),
                indicators=IndicatorInfo(),
                final_decision=FinalDecision(label="error", score=0.0),
                raw_quote=RawQuote(c=0, d=0, dp=0, h=0, l=0, o=0, pc=0, t=0),
                expected_move=0.0,
                confidence=0.0,
                summary=f"Could not compute prediction for {s}.",
                disclaimer="Educational use only.",
            )

    ranking_sorted = sorted(
        ranking,
        key=lambda x: (x["score"], x["confidence"]),
        reverse=True,
    )

    best_pick = ranking_sorted[0] if ranking_sorted else {"symbol": None}

    meta = BatchMeta(
        total_budget=request.budget,
        per_symbol_budget=round(per_symbol, 2),
        risk_profile=request.risk,
        fractional=request.fractional,
    )

    return BatchResult(
        symbols=[s.upper() for s in request.symbols],
        meta=meta,
        results=results,
        best_pick=best_pick,
    )


# -----------------------------
# SIMPLE BACKTEST (WORKING)
# -----------------------------

def _run_simple_backtest(symbol: str, candles: Dict[str, Any], budget: float):
    if not candles or "c" not in candles or len(candles["c"]) < 10:
        return {"symbol": symbol, "error": "Not enough data"}

    close = candles["c"]
    cash = budget
    position = 0
    trades = 0
    last = close[0]

    for p in close[1:]:
        if p > last * 1.01 and cash > p:
            position = cash / p
            cash = 0
            trades += 1

        if position > 0 and p < last * 0.99:
            cash = position * p
            position = 0
            trades += 1

        last = p

    final_val = cash + (position * last if position > 0 else 0)
    profit_pct = (final_val - budget) / budget * 100

    return {
        "symbol": symbol,
        "profit_pct": round(profit_pct, 2),
        "final_value": round(final_val, 2),
        "trades": trades,
    }


@app.get("/optimize/{symbol}")
async def optimize_symbol(symbol: str, budget: float = 1000.0):
    try:
        candles = fetch_candles(symbol.upper())
    except Exception as e:
        return {"symbol": symbol.upper(), "error": f"Failed to fetch candles: {e}"}

    return _run_simple_backtest(symbol.upper(), candles, budget)




















