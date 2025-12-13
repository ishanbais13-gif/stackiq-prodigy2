from fastapi import FastAPI, HTTPException
from data_fetcher import (
    get_quote,
    get_candles,
    run_predict_engine
)

app = FastAPI(title="StackIQ API", version="v1")


# ==========================
# Health Check (Azure)
# ==========================

@app.get("/health")
def health():
    return {"status": "ok"}


# ==========================
# Market Endpoints
# ==========================

@app.get("/quote/{symbol}")
def quote(symbol: str):
    try:
        return get_quote(symbol)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/candles/{symbol}")
def candles(symbol: str, days: int = 30):
    try:
        return {
            "symbol": symbol.upper(),
            "candles": get_candles(symbol, days)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==========================
# Prediction Endpoint
# ==========================

@app.get("/predict/{symbol}")
def predict(
    symbol: str,
    budget: float = 1000,
    risk: str = "medium"
):
    try:
        return run_predict_engine(symbol, budget, risk)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))




























