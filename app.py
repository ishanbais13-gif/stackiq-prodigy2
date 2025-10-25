# app.py
from fastapi import FastAPI, Query
from typing import List, Optional, Dict, Any

import engine
import backtest as bt
import optimize as opt

app = FastAPI(title="Prodigynt API", version="3.1")

@app.get("/")
def root():
    # simple status for landing checks
    return {"service": "StackIQ", "status": "ok"}

@app.get("/health")
def health():
    # you can extend this to check env/keys etc.
    try:
        has_token = True  # FINNHUB key checked inside data_fetcher calls
        return {"status": "ok", "has_token": has_token, "service": "StackIQ", "version": "3.1"}
    except Exception as e:
        return {"status": "error", "error": str(e)}

# ---------- Predict (single) ----------
@app.get("/predict/{symbol}")
def predict(symbol: str, budget: float = Query(1000.0, ge=0)):
    try:
        data = engine.predict(symbol.upper(), budget)
        return {"ok": True, "data": data}
    except Exception as e:
        return {"ok": False, "error": str(e)}

# ---------- Predict (batch) — FINNHUB-ONLY ----------
@app.get("/predict/batch")
def predict_batch(
    symbols: str = Query(..., description="Comma-separated symbols"),
    budget: float = Query(1000.0, ge=0),
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=100),
    sort: str = Query("confidence", description="confidence|price|symbol")
):
    """
    Returns ranked predictions for a universe with pagination.
    Example:
      /predict/batch?symbols=AAPL,MSFT,NVDA,TSLA&budget=1500&page=1&page_size=5
    """
    try:
        syms = [s.strip().upper() for s in symbols.split(",") if s.strip()]
        rows: List[Dict[str, Any]] = []
        for s in syms:
            try:
                rows.append(engine.predict(s, budget))
            except Exception as e:
                rows.append({"symbol": s, "error": str(e)})

        # rank only successful predictions; keep errors in a separate list
        ok_rows = [r for r in rows if "confidence" in r]
        err_rows = [r for r in rows if "error" in r]

        key = (lambda x: x.get("confidence", 0.0)) if sort == "confidence" else \
              (lambda x: x.get("price", 0.0)) if sort == "price" else \
              (lambda x: x.get("symbol", ""))
        reverse = sort in ("confidence", "price")
        ok_rows.sort(key=key, reverse=reverse)

        total = len(ok_rows)
        start = (page - 1) * page_size
        end = start + page_size
        page_items = ok_rows[start:end]

        return {
            "ok": True,
            "data": {
                "total": total,
                "page": page,
                "page_size": page_size,
                "items": page_items,
                "errors": err_rows  # keep visibility on failed symbols
            }
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}

# ---------- Backtest (walk-forward) ----------
@app.get("/backtest")
def backtest(
    symbols: str = Query(..., description="Comma-separated symbols"),
    start: str = Query(..., description="YYYY-MM-DD"),
    end: str = Query(..., description="YYYY-MM-DD"),
    budget: float = Query(10000.0, ge=100),
    hold_days: int = Query(1, ge=1, le=10),
    buy: float = Query(67.0, ge=50.0, le=90.0),
    sell: float = Query(33.0, ge=10.0, le=50.0),
    skip_earnings: bool = Query(False)
):
    """
    Walk-forward backtest using the ensemble score:
      - BUY when confidence >= buy
      - SELL when confidence <= sell
      - ELSE flat
      - Exit on stop/target or after hold_days
    """
    try:
        syms = [s.strip().upper() for s in symbols.split(",") if s.strip()]
        per_symbol: List[Dict[str, Any]] = []
        for s in syms:
            try:
                per_symbol.append(bt.run_backtest(s, start, end, budget, hold_days, buy, sell, skip_earnings))
            except Exception as e:
                per_symbol.append({"symbol": s, "error": str(e)})

        agg = bt.aggregate_metrics([r for r in per_symbol if r.get("metrics")])
        return {"ok": True, "data": {"summary": agg, "per_symbol": per_symbol}}
    except Exception as e:
        return {"ok": False, "error": str(e)}

# ---------- Optimize (grid search) ----------
@app.get("/optimize")
def optimize(
    symbols: str = Query(..., description="Comma-separated symbols"),
    start: str = Query(..., description="YYYY-MM-DD"),
    end: str = Query(..., description="YYYY-MM-DD"),
    budget: float = Query(10000.0, ge=100),
    hold_days: int = Query(1, ge=1, le=10),
    grid_scale: float = Query(0.2, ge=0.05, le=0.5),
    top: int = Query(5, ge=1, le=10)
):
    """
    Lightweight grid-search over ensemble weights and buy/sell thresholds.
    Returns top configurations ranked by Sharpe, then CAGR.
    """
    try:
        syms = [s.strip().upper() for s in symbols.split(",") if s.strip()]
        data = opt.grid_search(syms, start, end, budget, hold_days, grid_scale=grid_scale, top_k=top)
        return {"ok": True, "data": data}
    except Exception as e:
        return {"ok": False, "error": str(e)}




































































