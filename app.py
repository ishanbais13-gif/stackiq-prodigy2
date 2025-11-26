import os
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from data_fetcher import (
    get_quote,
    get_candles,
    get_company_news,
    get_trade_targets,
    get_prediction,
    get_options_helper,
)

APP_VERSION = "0.9.0-day9-alpaca"


# ---------- Pydantic Schemas ----------


class HealthResponse(BaseModel):
    status: str
    message: str
    version: str


class QuoteData(BaseModel):
    symbol: str
    current: float
    open: float
    high: float
    low: float
    previous_close: Optional[float] = None
    change: Optional[float] = None
    percent_change: Optional[float] = None
    timestamp: Optional[int] = None


class QuoteResponse(BaseModel):
    symbol: str
    data: QuoteData


class CandlePoint(BaseModel):
    time: str
    open: float
    high: float
    low: float
    close: float
    volume: float


class CandlesResponse(BaseModel):
    symbol: str
    candles: List[CandlePoint]


class NewsItem(BaseModel):
    id: Optional[str] = None
    headline: Optional[str] = None
    summary: Optional[str] = None
    url: Optional[str] = None
    source: Optional[str] = None
    created_at: Optional[str] = None


class NewsResponse(BaseModel):
    symbol: str
    news: List[NewsItem]


class TargetsResponse(BaseModel):
    symbol: str
    current_price: float
    momentum_score: float
    entry_price: float
    take_profit: float
    stop_loss: float
    position_size_shares: int
    estimated_position_cost: float
    risk_level: str


class PredictionAnalysis(BaseModel):
    short_ma: float
    long_ma: float
    momentum_score: float


class PredictionResponse(BaseModel):
    symbol: str
    prediction: str
    confidence: float
    current_price: float
    analysis: PredictionAnalysis
    budget: float
    risk_level: str


class SharesPlan(BaseModel):
    shares: int
    estimated_cost: float


class OptionsPlan(BaseModel):
    contracts: int
    estimated_premium_per_contract: float
    estimated_total_premium: float
    rough_leverage_factor: int


class OptionsHelperResponse(BaseModel):
    symbol: str
    current_price: float
    budget: float
    risk_level: str
    shares_plan: SharesPlan
    options_plan: OptionsPlan
    note: str


# ---------- App ----------

app = FastAPI(
    title="StackIQ Core API",
    version=APP_VERSION,
    description="FastAPI backend for StackIQ with Alpaca-based data, predictions, targets, options helper, and news.",
)


# ---------- Helpers ----------


def _safe_upper(symbol: str) -> str:
    return (symbol or "").upper().strip()


def _handle_error(exc: Exception, endpoint: str) -> None:
    msg = str(exc)
    # Make external API errors very clear
    if "Alpaca" in msg:
        raise HTTPException(status_code=502, detail=f"Alpaca error in {endpoint}: {msg}")
    raise HTTPException(status_code=500, detail=f"Internal error in {endpoint}: {msg}")


# ---------- Endpoints ----------


@app.get("/health", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    return HealthResponse(
        status="ok",
        message="StackIQ API is healthy.",
        version=APP_VERSION,
    )


@app.get("/quote/{symbol}", response_model=QuoteResponse)
async def get_quote_endpoint(
    symbol: str,
    pretty: int = Query(0, ge=0, le=1, description="Compatibility flag, ignored"),
) -> QuoteResponse:
    symbol = _safe_upper(symbol)
    try:
        data = get_quote(symbol)
    except Exception as exc:
        _handle_error(exc, "quote")

    return QuoteResponse(symbol=symbol, data=QuoteData(**data))


@app.get("/candles/{symbol}", response_model=CandlesResponse)
async def get_candles_endpoint(
    symbol: str,
    timeframe: str = Query("1Day", description="Alpaca timeframe, e.g. 1Min, 5Min, 15Min, 1Hour, 1Day"),
    limit: int = Query(60, ge=1, le=1000),
) -> CandlesResponse:
    symbol = _safe_upper(symbol)
    try:
        raw = get_candles(symbol, timeframe=timeframe, limit=limit)
    except Exception as exc:
        _handle_error(exc, "candles")

    candles = [CandlePoint(**c) for c in raw["candles"]]
    return CandlesResponse(symbol=symbol, candles=candles)


@app.get("/news/{symbol}", response_model=NewsResponse)
async def get_news_endpoint(
    symbol: str,
    limit: int = Query(10, ge=1, le=50),
) -> NewsResponse:
    symbol = _safe_upper(symbol)
    try:
        raw = get_company_news(symbol, limit=limit)
    except Exception as exc:
        _handle_error(exc, "news")

    items = [NewsItem(**n) for n in raw["news"]]
    return NewsResponse(symbol=symbol, news=items)


@app.get("/targets/{symbol}", response_model=TargetsResponse)
async def get_targets_endpoint(
    symbol: str,
    budget: float = Query(1000.0, gt=0),
    risk: str = Query("medium", description="low, medium, or high"),
) -> TargetsResponse:
    symbol = _safe_upper(symbol)
    try:
        raw = get_trade_targets(symbol, budget=budget, risk=risk)
    except Exception as exc:
        _handle_error(exc, "targets")

    return TargetsResponse(**raw)


@app.get("/predict/{symbol}", response_model=PredictionResponse)
async def predict_endpoint(
    symbol: str,
    budget: float = Query(1000.0, gt=0),
    risk: str = Query("medium", description="low, medium, or high"),
) -> PredictionResponse:
    symbol = _safe_upper(symbol)
    try:
        raw = get_prediction(symbol, budget=budget, risk=risk)
    except Exception as exc:
        _handle_error(exc, "predict")

    return PredictionResponse(**raw)


@app.get("/options/{symbol}", response_model=OptionsHelperResponse)
async def options_helper_endpoint(
    symbol: str,
    budget: float = Query(1000.0, gt=0),
    risk: str = Query("medium", description="low, medium, or high"),
) -> OptionsHelperResponse:
    symbol = _safe_upper(symbol)
    try:
        raw = get_options_helper(symbol, budget=budget, risk=risk)
    except Exception as exc:
        _handle_error(exc, "options")

    return OptionsHelperResponse(**raw)






















