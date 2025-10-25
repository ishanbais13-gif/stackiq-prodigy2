# app.py
from fastapi import FastAPI, Query
from typing import List, Optional, Dict, Any
import engine
import backtest as bt
import optimize as opt

app = FastAPI(title="Prodigynt API", version="3.0")

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/predict/{symbol}")
def predict(symbol: str, budget: float = Query(1000.0, ge=0)):
    try:
        data = engine.predict(symbol, budget)
        return {"ok": True, "data": data}
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.get("/predict/batch")
def predict_batch(symbols: str = Query(..., description="Comma-separated symbols"),
                  budget: float = Query(1000.0, ge=0),
                  page: int = Query(1, ge=1),
                  page_size: int = Query(10, ge=1, le=100),
                  sort: str = Query("confidence", description="confidence|price|symbol")):
    """
    Returns ranked predictions for a universe with pagination.
    Example:
      /predict/batch?symbols=AAPL,MSFT,NVDA&budget=1500&page=1&page_size=10
    """
    try:
        syms = [s.strip().upper() for s in symbols.split(",") if s.strip()]
        rows = []
        for s in syms:
            try:
                rows.append(engine.predict(s, budget))
            except Exception as e:
                rows.append({"symbol": s, "error": str(e)})
        # filter errored for ranking but keep count
        rankables = [r for r in rows if "confidence" in r or ("features" in r and "price" in r)]
        key = (lambda x: x.get("confidence", 0.0)) if sort == "confidence" else \
              (lambda x: x.get("price", 0.0)) if sort == "price" else \
              (lambda x: x.get("symbol", ""))
        rankables.sort(key=key, reverse=(sort in ("confidence","price")))
        # pagination
        total = len(rankables)
        start = (page-1) * page_size
        end = start + page_size
        page_items = rankables[start:end]
        return {"ok": True, "data": {"total": total, "page": page, "page_size": page_size, "items": page_items}}
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.get("/backtest")
def backtest(symbols: str = Query(..., description="Comma-separated symbols"),
             start: str = Query(..., description="YYYY-MM-DD"),
             end: str = Query(..., description="YYYY-MM-DD"),
             budget: float = Query(10000.0, ge=100),
             hold_days: int = Query(1, ge=1, le=10),
             buy: float = Query(67.0, ge=50.0, le=90.0),
             sell: float = Query(33.0, ge=10.0, le=50.0),
             skip_earnings: bool = Query(False)):
    """
    Walk-forward backtest using the ensemble score:
      - BUY when confidence >= buy
      - SELL when confidence <= sell
      - ELSE flat
      - Exit early on stop/target hit; otherwise after hold_days
    """
    try:
        syms = [s.strip().upper() for s in symbols.split(",") if s.strip()]
        results = []
        for s in syms:
            try:
                res = bt.run_backtest(s, start, end, budget, hold_days, buy, sell, skip_earnings)
                results.append(res)
            except Exception as e:
                results.append({"symbol": s, "error": str(e)})
        # aggregate metrics across symbols that succeeded
        agg = bt.aggregate_metrics([r for r in results if r.get("metrics")])
        return {"ok": True, "data": {"summary": agg, "per_symbol": results}}
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.get("/optimize")
def optimize(symbols: str = Query(..., description="Comma-separated symbols"),
             start: str = Query(..., description="YYYY-MM-DD"),
             end: str = Query(..., description="YYYY-MM-DD"),
             budget: float = Query(10000.0, ge=100),
             hold_days: int = Query(1, ge=1, le=10),
             grid_scale: float = Query(0.2, ge=0.05, le=0.5),
             top: int = Query(5, ge=1, le=10)):
    """
    Lightweight grid-search over ensemble weights and buy/sell thresholds.
    Returns top configurations ranked by Sharpe, then CAGR.
    """
    try:
        syms = [s.strip().upper() for s in symbols.split(",") if s.strip()]
        out = opt.grid_search(syms, start, end, budget, hold_days, grid_scale=grid_scale, top_k=top)
        return {"ok": True, "data": out}
    except Exception as e:
        return {"ok": False, "error": str(e)}



































































