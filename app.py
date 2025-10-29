# app.py
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import List, Dict, Optional
import numpy as np
import statistics as stats
import os

from data_fetcher import get_quote, get_candles, get_close_series, FinnhubError

app = FastAPI(title="StackIQ API", version="1.0.0")

# CORS (relaxed for now; tighten later)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------- Models ----------

class BatchPredictItem(BaseModel):
    symbol: str

class BatchPredictRequest(BaseModel):
    items: List[BatchPredictItem]

class BatchPredictResult(BaseModel):
    symbol: str
    current: float
    signal: str
    confidence: float

class BacktestRequest(BaseModel):
    symbol: str
    fast: int = Field(10, ge=2, description="Fast MA window")
    slow: int = Field(30, ge=3, description="Slow MA window")
    fee_bps: float = Field(1.0, ge=0, description="Per trade fee in basis points")

class OptimizeRequest(BaseModel):
    symbol: str
    fast_min: int = Field(5, ge=2)
    fast_max: int = Field(20, ge=3)
    slow_min: int = Field(25, ge=5)
    slow_max: int = Field(60, ge=6)
    fee_bps: float = Field(1.0, ge=0)

# ---------- Utilities ----------

def moving_average(values: List[float], window: int) -> np.ndarray:
    if window <= 0 or window > len(values):
        return np.array([])
    cumsum = np.cumsum(np.insert(values, 0, 0.0))
    out = (cumsum[window:] - cumsum[:-window]) / float(window)
    # left-pad to align lengths
    pad = np.full((window - 1,), np.nan)
    return np.concatenate([pad, out])

def crossover_signals(prices: List[float], fast: int, slow: int) -> List[int]:
    """
    Returns list of signals: 1=long, -1=flat/exit, 0=no change
    Simple rule: when fast MA crosses above slow => long; crosses below => exit.
    """
    ma_fast = moving_average(prices, fast)
    ma_slow = moving_average(prices, slow)
    sig = [0] * len(prices)
    last = 0
    for i in range(len(prices)):
        f, s = ma_fast[i], ma_slow[i]
        if np.isnan(f) or np.isnan(s):
            sig[i] = 0
            continue
        if f > s and last != 1:
            sig[i] = 1
            last = 1
        elif f < s and last != -1:
            sig[i] = -1
            last = -1
        else:
            sig[i] = 0
    return sig

def run_backtest(closes: List[float], fast: int, slow: int, fee_bps: float = 1.0) -> Dict:
    """
    Very simple backtest:
    - Enter long on +1 signal close->next open proxy (use close-to-close)
    - Exit to cash on -1 signal
    - Fees applied when position flips (bps of notional)
    Returns performance metrics + equity curve.
    """
    if slow <= fast:
        raise ValueError("slow must be > fast")
    if len(closes) < slow + 10:
        raise ValueError("Not enough data to backtest")

    signals = crossover_signals(closes, fast, slow)
    fee = fee_bps / 10000.0

    equity = [1.0]  # start with 1 unit
    pos = 0  # 1 long, 0 cash
    last_price = closes[0]
    trades = 0

    for i in range(1, len(closes)):
        price = closes[i]

        # apply daily pnl
        if pos == 1:
            ret = (price / last_price) - 1.0
        else:
            ret = 0.0

        new_equity = equity[-1] * (1.0 + ret)

        # react to signal at end of day
        if signals[i] == 1 and pos == 0:
            # opening long: pay fee
            new_equity *= (1.0 - fee)
            pos = 1
            trades += 1
        elif signals[i] == -1 and pos == 1:
            # closing long: pay fee
            new_equity *= (1.0 - fee)
            pos = 0
            trades += 1

        equity.append(new_equity)
        last_price = price

    curve = np.array(equity)
    total_return = float(curve[-1] - 1.0)
    rets = np.diff(curve) / curve[:-1]
    sharpe = float(np.mean(rets) / (np.std(rets) + 1e-12) * np.sqrt(252)) if len(rets) > 2 else 0.0
    max_dd = 0.0
    peak = curve[0]
    for x in curve:
        peak = max(peak, x)
        dd = (peak - x) / peak
        max_dd = max(max_dd, float(dd))

    return {
        "total_return": total_return,
        "sharpe": sharpe,
        "max_drawdown": max_dd,
        "trades": trades,
        "final_equity": float(curve[-1]),
        "equity_curve": [round(float(v), 6) for v in curve[-200:]],  # last 200 for size
        "params": {"fast": fast, "slow": slow, "fee_bps": fee_bps},
        "n_days": len(closes),
    }

# ---------- Routes ----------

@app.get("/health")
def health():
    return {"service": "StackIQ", "status": "ok"}

@app.get("/quote/{symbol}")
def quote(symbol: str):
    try:
        return get_quote(symbol)
    except FinnhubError as e:
        raise HTTPException(status_code=502, detail=str(e))

@app.get("/candles/{symbol}")
def candles(symbol: str, resolution: str = "D", lookback_days: int = 400):
    try:
        series = get_close_series(symbol, lookback_days)
        # return minimal for now
        return {
            "symbol": symbol.upper(),
            "t": [ts for ts, _ in series],
            "c": [float(px) for _, px in series],
        }
    except FinnhubError as e:
        raise HTTPException(status_code=502, detail=str(e))

@app.post("/predict/batch", response_model=List[BatchPredictResult])
def predict_batch(req: BatchPredictRequest):
    """
    Demo heuristic:
      - If 10D SMA > 30D SMA -> 'BUY'
      - If 10D SMA < 30D SMA -> 'SELL'
      - else 'HOLD'
    Confidence = normalized |SMA10 - SMA30| / price
    """
    out: List[BatchPredictResult] = []
    for item in req.items:
        symbol = item.symbol.upper()
        try:
            series = get_close_series(symbol, 120)
            if len(series) < 40:
                raise FinnhubError("Insufficient data for SMA calc")
            closes = [px for _, px in series]
            sma10 = float(np.nanmean(closes[-10:]))
            sma30 = float(np.nanmean(closes[-30:]))
            price = closes[-1]
            if sma10 > sma30:
                signal = "BUY"
            elif sma10 < sma30:
                signal = "SELL"
            else:
                signal = "HOLD"
            confidence = float(min(1.0, abs(sma10 - sma30) / max(1e-9, price)))
            out.append(BatchPredictResult(symbol=symbol, current=price, signal=signal, confidence=confidence))
        except FinnhubError as e:
            # bubble up as neutral
            out.append(BatchPredictResult(symbol=symbol, current=0.0, signal="ERROR", confidence=0.0))
    return out

@app.post("/backtest")
def backtest(req: BacktestRequest):
    try:
        series = get_close_series(req.symbol, 500)
        closes = [px for _, px in series]
        res = run_backtest(closes, req.fast, req.slow, req.fee_bps)
        return {"symbol": req.symbol.upper(), **res}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/optimize")
def optimize(req: OptimizeRequest):
    """
    Grid search MA fast/slow, return best by Sharpe (fallback to total_return if equal).
    """
    try:
        series = get_close_series(req.symbol, 700)
        closes = [px for _, px in series]
        best = None
        for fast in range(req.fast_min, req.fast_max + 1):
            for slow in range(max(fast + 1, req.slow_min), req.slow_max + 1):
                try:
                    res = run_backtest(closes, fast, slow, req.fee_bps)
                except Exception:
                    continue
                score = (res["sharpe"], res["total_return"])
                if best is None or score > best["score"]:
                    best = {"score": score, "result": res, "fast": fast, "slow": slow}
        if not best:
            raise ValueError("No feasible parameter set")
        return {
            "symbol": req.symbol.upper(),
            "best_params": {"fast": best["fast"], "slow": best["slow"], "fee_bps": req.fee_bps},
            "metrics": best["result"],
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))





































































