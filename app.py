from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import Any

import data_fetcher

app = FastAPI(title="StackIQ Backend", version="1.0.0")

# --- CORS so your React app + localhost + Azure can call this ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # you can tighten later
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# -------------------------------------------------------------
#  Health
# -------------------------------------------------------------
@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


# -------------------------------------------------------------
#  Quote
# -------------------------------------------------------------
@app.get("/quote/{symbol}")
def quote(symbol: str) -> Any:
    try:
        return data_fetcher.get_quote(symbol)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Quote error: {e}")


# -------------------------------------------------------------
#  Candles
# -------------------------------------------------------------
@app.get("/candles/{symbol}")
def candles(symbol: str, resolution: str = "D", days: int = 30) -> Any:
    """
    resolution: "D", "60", "15", "5", etc – we map to Alpaca internally.
    """
    try:
        data = data_fetcher.get_candles(symbol, days=days, resolution=resolution)
        return {"symbol": symbol.upper(), "resolution": resolution, "days": days, "candles": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Candles error: {e}")


# -------------------------------------------------------------
#  News
# -------------------------------------------------------------
@app.get("/news/{symbol}")
def news(symbol: str, limit: int = 5) -> Any:
    try:
        return data_fetcher.get_news(symbol, limit=limit)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"News error: {e}")


# -------------------------------------------------------------
#  Predict (mock engine for v1)
# -------------------------------------------------------------
@app.get("/predict/{symbol}")
def predict(symbol: str, budget: float, risk: str = "medium", fractional: bool = True) -> Any:
    """
    Main endpoint your UI will hit.
    Returns a mock-but-consistent trade idea so your app never crashes.
    """
    try:
        result = data_fetcher.run_predict_engine(symbol, budget, risk)
        # keep fractional in response for future real engine
        result["fractional"] = fractional
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Predict error: {e}")
























