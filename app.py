from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import List, Optional, Literal
from datetime import datetime, timedelta, timezone
import os
import math
import statistics
import requests

# --- App init -----------------------------------------------------------------

app = FastAPI(
    title="StackIQ Core API",
    version="0.9.0-day9-max",
    description="FastAPI backend for StackIQ with prediction, targets, options helper, and news sentiment."
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")

FINNHUB_BASE = "https://finnhub.io/api/v1"


# --- Helper functions ---------------------------------------------------------


def _require_api_key() -> None:
    if not FINNHUB_API_KEY:
        raise HTTPException(
            status_code=500,
            detail="FINNHUB_API_KEY is not configured on the server.",
        )


def _finnhub_get(path: str, params: Optional[dict] = None) -> dict:
    _require_api_key()
    params = params.copy() if params else {}
    params["token"] = FINNHUB_API_KEY
    url = f"{FINNHUB_BASE}{path}"
    try:
        resp = requests.get(url, params=params, timeout=10)
    except requests.RequestException as exc:
        raise HTTPException(status_code=502, detail=f"Error calling Finnhub: {exc}")
    if resp.status_code != 200:
        raise HTTPException(
            status_code=resp.status_code,
            detail=f"Finnhub error: {resp.text}",
        )
    try:
        return resp.json()
    except ValueError:
        raise HTTPException(status_code=502, detail="Invalid JSON from Finnhub")


def _safe_pct_change(new: float, old: float) -> float:
    if old == 0:
        return 0.0
    return (new - old) / old * 100.0


def _compute_basic_trend(close_prices: List[float]) -> dict:
    """
    Very lightweight momentum + volatility metrics from a list of closes.
    Assumes newest prices at the end of the list.
    """
    if len(close_prices) < 5:
        return {
            "trend_direction": 0.0,
            "trend_strength": 0.0,
            "volatility_pct": 0.0,
        }

    recent = close_prices[-5:]
    older = close_prices[-10:-5] if len(close_prices) >= 10 else close_prices[:-5]

    recent_mean = statistics.fmean(recent)
    older_mean = statistics.fmean(older) if older else recent_mean

    trend_direction = _safe_pct_change(recent_mean, older_mean)

    # Volatility as std dev of last N daily returns
    returns = []
    for i in range(1, len(close_prices)):
        if close_prices[i - 1] != 0:
            returns.append((close_prices[i] - close_prices[i - 1]) / close_prices[i - 1])
    vol = statistics.pstdev(returns) * 100 if len(returns) >= 2 else 0.0

    # Trend strength combines direction and volatility (reward strong trend, penalize chop)
    trend_strength = max(0.0, trend_direction) - max(0.0, vol - 3)

    return {
        "trend_direction": trend_direction,
        "trend_strength": trend_strength,
        "volatility_pct": vol,
    }


def _confidence_score(trend_strength: float, volatility_pct: float, days_of_data: int) -> int:
    """
    0–100 confidence score. This is NOT guaranteed accuracy – just a rough quality rating.
    """
    base = max(-20.0, min(20.0, trend_strength))  # clamp
    base_score = (base + 20) / 40 * 70  # 0–70 from trend

    # Penalize extreme vol, reward moderate vol
    if volatility_pct < 2:
        vol_score = 5
    elif volatility_pct < 5:
        vol_score = 15
    elif volatility_pct < 10:
        vol_score = 10
    elif volatility_pct < 20:
        vol_score = 5
    else:
        vol_score = -10

    # Small bonus if we have a lot of history
    history_bonus = min(10, days_of_data / 30)

    score = base_score + vol_score + history_bonus
    return int(max(5, min(95, score)))


def _risk_tier(volatility_pct: float) -> Literal["low", "medium", "high", "extreme"]:
    if volatility_pct < 3:
        return "low"
    if volatility_pct < 7:
        return "medium"
    if volatility_pct < 15:
        return "high"
    return "extreme"


def _expected_next_day_move(trend_direction: float, volatility_pct: float) -> float:
    """
    Super simple expected move in percent:
    - Follow trend direction but don't exceed 60% of recent vol.
    """
    if volatility_pct <= 0:
        return 0.0
    cap = volatility_pct * 0.6
    raw = trend_direction * 0.5
    if raw > 0:
        return min(raw, cap)
    else:
        return max(raw, -cap)


def _position_size_from_budget(budget: float, price: float, risk_pct: float = 1.0) -> int:
    """
    Risk a fraction (risk_pct) of capital on the move.
    Eg: budget=1000, risk_pct=1 -> risk $10, so shares = 10 / price.
    """
    if price <= 0 or budget <= 0:
        return 0
    risk_dollars = budget * (risk_pct / 100.0)
    if risk_dollars <= 0:
        return 0
    return max(1, int(risk_dollars // price))  # at least 1 share if possible


def _compute_targets(current_price: float, volatility_pct: float) -> dict:
    """
    Generates buy / take-profit / stop-loss zones from current price & volatility.
    """
    if current_price <= 0:
        raise ValueError("Invalid current price")

    # Use a fraction of volatility as trading band
    band = max(1.5, min(8.0, volatility_pct * 0.6))

    buy_low = current_price * (1 - band / 200)   # small dip buy
    buy_high = current_price * (1 + band / 400)  # chase a little if momentum

    tp1 = current_price * (1 + band / 100)       # first take profit
    tp2 = current_price * (1 + band / 60)        # extended target

    # Stop loss: around 0.5x band
    sl = current_price * (1 - band / 150)

    return {
        "band_pct": band,
        "buy_zone_low": round(buy_low, 2),
        "buy_zone_high": round(buy_high, 2),
        "take_profit_1": round(tp1, 2),
        "take_profit_2": round(tp2, 2),
        "stop_loss": round(sl, 2),
    }


def _shares_vs_options_hint(volatility_pct: float, price: float) -> str:
    if volatility_pct < 3:
        return "Stock-only zone: movement is slow, options may decay more than they move."
    if volatility_pct < 7:
        return "Mostly shares; you *can* use options, but focus on 30–45 DTE quality contracts if you do."
    if volatility_pct < 15:
        if price < 50:
            return "Shares or light options: the stock is volatile, but price is low enough that shares are still great."
        else:
            return "Prime options candidate: decent volatility and price – consider calls/puts instead of 100+ shares."
    return "High-risk options: volatility is extreme – if you trade options here, size *very* small and expect wild swings."


def _analyze_candles_for_symbol(symbol: str, days: int = 60) -> dict:
    """
    Pulls daily candles from Finnhub and returns closes + metrics.
    """
    now = datetime.now(timezone.utc)
    fro = int((now - timedelta(days=days + 2)).timestamp())
    to = int(now.timestamp())

    data = _finnhub_get(
        "/stock/candle",
        {"symbol": symbol.upper(), "resolution": "D", "from": fro, "to": to},
    )

    if data.get("s") != "ok":
        raise HTTPException(status_code=400, detail=f"No candle data for {symbol}")

    closes = data.get("c", [])
    timestamps = data.get("t", [])
    if not closes or not timestamps:
        raise HTTPException(status_code=400, detail=f"Empty candle data for {symbol}")

    metrics = _compute_basic_trend(closes)

    return {
        "closes": closes,
        "timestamps": timestamps,
        "metrics": metrics,
    }


# --- Pydantic models ----------------------------------------------------------


class HealthResponse(BaseModel):
    status: str = "ok"
    message: str = "StackIQ API is healthy."


class QuoteData(BaseModel):
    symbol: str
    current: float = Field(..., description="Current price")
    open: Optional[float] = Field(None, description="Open price of the day")
    high: Optional[float] = None
    low: Optional[float] = None
    previous_close: Optional[float] = Field(None, description="Previous close price")
    change: Optional[float] = Field(None, description="Dollar change from previous close")
    percent_change: Optional[float] = Field(None, description="Percent change from previous close")
    timestamp: Optional[int] = Field(None, description="Unix timestamp from Finnhub")


class QuoteResponse(BaseModel):
    symbol: str
    data: QuoteData


class CandlePoint(BaseModel):
    t: int
    c: float
    o: Optional[float] = None
    h: Optional[float] = None
    l: Optional[float] = None
    v: Optional[float] = None


class CandlesResponse(BaseModel):
    symbol: str
    count: int
    candles: List[CandlePoint]


class PredictionBreakdown(BaseModel):
    confidence_score: int = Field(..., description="0–100 internal quality rating, NOT guaranteed accuracy")
    expected_move_pct: float
    expected_move_direction: Literal["up", "down", "flat"]
    risk_tier: Literal["low", "medium", "high", "extreme"]
    volatility_pct: float
    trend_direction_pct: float
    trend_strength: float
    holding_period_days: int = Field(..., description="Rough holding period this model is tuned for (1 = next day)")


class PositionPlan(BaseModel):
    budget: float
    per_trade_risk_pct: float
    suggested_shares: int
    estimated_profit_if_correct: float
    estimated_loss_if_wrong: float


class PredictionResponse(BaseModel):
    symbol: str
    current_price: float
    as_of: datetime
    prediction: PredictionBreakdown
    position_plan: Optional[PositionPlan] = None
    notes: List[str]


class TargetsResponse(BaseModel):
    symbol: str
    current_price: float
    volatility_pct: float
    risk_tier: Literal["low", "medium", "high", "extreme"]
    targets: dict
    disclaimer: str


class OptionsHelperResponse(BaseModel):
    symbol: str
    current_price: float
    volatility_pct: float
    risk_tier: Literal["low", "medium", "high", "extreme"]
    shares_vs_options_hint: str
    rough_next_day_move_pct_range: List[float]
    notes: List[str]


class NewsItem(BaseModel):
    headline: str
    source: str
    summary: Optional[str] = None
    url: Optional[str] = None
    datetime: Optional[datetime] = None
    sentiment_score: int
    sentiment_label: Literal["very_bearish", "bearish", "neutral", "bullish", "very_bullish"]


class NewsResponse(BaseModel):
    symbol: str
    items: List[NewsItem]
    aggregated_sentiment_score: int
    aggregated_sentiment_label: Literal["very_bearish", "bearish", "neutral", "bullish", "very_bullish"]


# --- Sentiment helpers --------------------------------------------------------


def _score_headline_sentiment(text: str) -> int:
    """
    Tiny keyword-based sentiment model just to give some signal (0–100).
    This is purposely simple so it runs 100% on your backend.
    """
    if not text:
        return 50

    lowered = text.lower()
    score = 50

    bullish_words = [
        "beats", "beat", "surge", "surges", "record", "rally", "jumps", "soars",
        "upgrade", "upgrades", "optimistic", "strong", "growth", "bullish",
    ]
    bearish_words = [
        "misses", "miss", "plunge", "plunges", "fall", "falls", "downgrade",
        "downgrades", "cuts", "cut", "lawsuit", "sec probes", "probe",
        "concern", "concerns", "weak", "guidance cut", "bearish",
    ]

    for w in bullish_words:
        if w in lowered:
            score += 8
    for w in bearish_words:
        if w in lowered:
            score -= 8

    score = max(0, min(100, score))
    return score


def _label_from_score(score: int) -> str:
    if score <= 25:
        return "very_bearish"
    if score <= 40:
        return "bearish"
    if score < 60:
        return "neutral"
    if score < 75:
        return "bullish"
    return "very_bullish"


# --- Routes -------------------------------------------------------------------


@app.get("/health", response_model=HealthResponse)
def health_check():
    return HealthResponse()


@app.get("/quote/{symbol}", response_model=QuoteResponse)
def get_quote(symbol: str):
    """
    Live quote endpoint from Finnhub.
    """
    symbol = symbol.upper()
    data = _finnhub_get("/quote", {"symbol": symbol})

    current = data.get("c") or 0.0
    previous = data.get("pc") or 0.0
    change = current - previous if previous else None
    pct = _safe_pct_change(current, previous) if previous else None

    quote = QuoteData(
        symbol=symbol,
        current=current,
        open=data.get("o"),
        high=data.get("h"),
        low=data.get("l"),
        previous_close=previous or None,
        change=change,
        percent_change=pct,
        timestamp=int(datetime.now(timezone.utc).timestamp()),
    )

    return QuoteResponse(symbol=symbol, data=quote)


@app.get("/candles/{symbol}", response_model=CandlesResponse)
def get_candles(
    symbol: str,
    resolution: str = "D",
    days: int = 60,
):
    """
    General candles endpoint, defaults to daily candles for the last 60 days.
    """
    symbol = symbol.upper()
    now = datetime.now(timezone.utc)
    fro = int((now - timedelta(days=days + 2)).timestamp())
    to = int(now.timestamp())

    data = _finnhub_get(
        "/stock/candle",
        {"symbol": symbol, "resolution": resolution, "from": fro, "to": to},
    )

    if data.get("s") != "ok":
        raise HTTPException(status_code=400, detail=f"No candle data for {symbol}")

    closes = data.get("c", [])
    opens = data.get("o", [])
    highs = data.get("h", [])
    lows = data.get("l", [])
    vols = data.get("v", [])
    times = data.get("t", [])

    candles: List[CandlePoint] = []
    for i, ts in enumerate(times):
        candles.append(
            CandlePoint(
                t=ts,
                c=closes[i] if i < len(closes) else None,
                o=opens[i] if i < len(opens) else None,
                h=highs[i] if i < len(highs) else None,
                l=lows[i] if i < len(lows) else None,
                v=vols[i] if i < len(vols) else None,
            )
        )

    return CandlesResponse(symbol=symbol, count=len(candles), candles=candles)


@app.get("/predict/{symbol}", response_model=PredictionResponse)
def predict_next_day(
    symbol: str,
    budget: Optional[float] = None,
    days: int = 60,
):
    """
    Core prediction endpoint.
    - Uses daily candles to build simple trend & volatility profile
    - Produces next-day expected move range, confidence score, risk tier
    - Optionally designs a position plan based on budget and 1% risk
    """
    symbol = symbol.upper()

    quote_resp = get_quote(symbol)
    current_price = quote_resp.data.current
    if current_price <= 0:
        raise HTTPException(status_code=400, detail=f"Invalid current price for {symbol}")

    candles_info = _analyze_candles_for_symbol(symbol, days=days)
    closes = candles_info["closes"]
    metrics = candles_info["metrics"]

    trend_dir = metrics["trend_direction"]
    trend_strength = metrics["trend_strength"]
    vol_pct = metrics["volatility_pct"]

    expected_move_pct = _expected_next_day_move(trend_dir, vol_pct)
    direction: Literal["up", "down", "flat"]
    if abs(expected_move_pct) < 0.3:
        direction = "flat"
    elif expected_move_pct > 0:
        direction = "up"
    else:
        direction = "down"

    conf = _confidence_score(trend_strength, vol_pct, days_of_data=len(closes))
    risk = _risk_tier(vol_pct)

    notes = [
        "This is NOT financial advice; it's a simple quantitative edge-estimate.",
        "Confidence is an internal quality score, not a guaranteed win-rate.",
        f"Trend direction is based on recent vs older averages over ~{days} days.",
        "Volatility is derived from standard deviation of daily returns.",
    ]

    position_plan = None
    if budget and budget > 0:
        suggested_shares = _position_size_from_budget(budget, current_price, risk_pct=1.0)
        if suggested_shares > 0:
            move_abs = abs(expected_move_pct) / 100.0
            est_profit = suggested_shares * current_price * move_abs
            # Assume 1:1-ish downside move for rough sizing
            est_loss = suggested_shares * current_price * (move_abs * 0.9)

            position_plan = PositionPlan(
                budget=budget,
                per_trade_risk_pct=1.0,
                suggested_shares=suggested_shares,
                estimated_profit_if_correct=round(est_profit, 2),
                estimated_loss_if_wrong=round(est_loss, 2),
            )
        else:
            notes.append("Budget too small for the 1% risk rule at current price – no shares suggested.")

    pred = PredictionBreakdown(
        confidence_score=conf,
        expected_move_pct=round(expected_move_pct, 2),
        expected_move_direction=direction,
        risk_tier=risk,
        volatility_pct=round(vol_pct, 2),
        trend_direction_pct=round(trend_dir, 2),
        trend_strength=round(trend_strength, 2),
        holding_period_days=1,
    )

    return PredictionResponse(
        symbol=symbol,
        current_price=round(current_price, 2),
        as_of=datetime.now(timezone.utc),
        prediction=pred,
        position_plan=position_plan,
        notes=notes,
    )


@app.get("/targets/{symbol}", response_model=TargetsResponse)
def get_trade_targets(symbol: str, days: int = 60):
    """
    Generates buy / take-profit / stop-loss zones using volatility bands.
    """
    symbol = symbol.upper()
    quote_resp = get_quote(symbol)
    current_price = quote_resp.data.current

    candles_info = _analyze_candles_for_symbol(symbol, days=days)
    vol_pct = candles_info["metrics"]["volatility_pct"]
    risk = _risk_tier(vol_pct)

    try:
        targets = _compute_targets(current_price, vol_pct)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    disclaimer = (
        "These levels are auto-generated from recent volatility only. "
        "They are NOT guaranteed support/resistance and should be combined "
        "with your own risk management and technical analysis."
    )

    return TargetsResponse(
        symbol=symbol,
        current_price=round(current_price, 2),
        volatility_pct=round(vol_pct, 2),
        risk_tier=risk,
        targets=targets,
        disclaimer=disclaimer,
    )


@app.get("/options/{symbol}", response_model=OptionsHelperResponse)
def options_helper(symbol: str, days: int = 60):
    """
    Not a full options chain – this is a helper that decides whether the setup
    looks more like a 'shares' or 'options' play and gives a rough next-day
    move range that you can plug into your own option pricing thinking.
    """
    symbol = symbol.upper()
    quote_resp = get_quote(symbol)
    current_price = quote_resp.data.current

    candles_info = _analyze_candles_for_symbol(symbol, days=days)
    metrics = candles_info["metrics"]
    vol_pct = metrics["volatility_pct"]
    risk = _risk_tier(vol_pct)

    exp_move_pct = abs(_expected_next_day_move(metrics["trend_direction"], vol_pct))
    # give a range around expected move
    low = round(max(0.3, exp_move_pct * 0.7), 2)
    high = round(max(low + 0.1, exp_move_pct * 1.3), 2)

    hint = _shares_vs_options_hint(vol_pct, current_price)

    notes = [
        "This is NOT options pricing. It just tells you how violent the stock is and if options make sense.",
        "Combine this with IV, DTE and the option's own Greeks before taking any trade.",
        "High volatility with high price often makes options more capital-efficient than shares.",
    ]

    return OptionsHelperResponse(
        symbol=symbol,
        current_price=round(current_price, 2),
        volatility_pct=round(vol_pct, 2),
        risk_tier=risk,
        shares_vs_options_hint=hint,
        rough_next_day_move_pct_range=[low, high],
        notes=notes,
    )


@app.get("/news/{symbol}", response_model=NewsResponse)
def company_news(symbol: str, days: int = 7, max_items: int = 15):
    """
    Pulls recent company news from Finnhub and scores headline sentiment.
    """
    symbol = symbol.upper()
    _require_api_key()

    now = datetime.now(timezone.utc).date()
    fro = (now - timedelta(days=days)).isoformat()
    to = now.isoformat()

    raw_items = _finnhub_get(
        "/company-news",
        {"symbol": symbol, "from": fro, "to": to},
    )

    items: List[NewsItem] = []
    scores: List[int] = []

    for item in raw_items[:max_items]:
        headline = item.get("headline", "")
        source = item.get("source", "unknown")
        summary = item.get("summary") or None
        url = item.get("url") or None
        dt_ts = item.get("datetime")

        score = _score_headline_sentiment(headline)
        label = _label_from_score(score)

        scores.append(score)

        dt_obj = None
        if isinstance(dt_ts, (int, float)):
            try:
                dt_obj = datetime.fromtimestamp(dt_ts, tz=timezone.utc)
            except Exception:
                dt_obj = None

        items.append(
            NewsItem(
                headline=headline,
                source=source,
                summary=summary,
                url=url,
                datetime=dt_obj,
                sentiment_score=score,
                sentiment_label=label,
            )
        )

    agg_score = int(statistics.fmean(scores)) if scores else 50
    agg_label = _label_from_score(agg_score)

    return NewsResponse(
        symbol=symbol,
        items=items,
        aggregated_sentiment_score=agg_score,
        aggregated_sentiment_label=agg_label,
    )





















