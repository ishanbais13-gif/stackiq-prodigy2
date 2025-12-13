from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

import data_fetcher

app = FastAPI()

# Allow your frontend to call this API (adjust origins later if you want)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def root():
    return {"ok": True, "service": "stackiq-web", "endpoints": ["/health", "/quote/{symbol}", "/candles/{symbol}", "/news/{symbol}", "/predict/{symbol}"]}


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/quote/{symbol}")
def quote(symbol: str):
    try:
        return data_fetcher.get_quote(symbol)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/candles/{symbol}")
def candles(symbol: str, days: int = 30, resolution: str = "1Day"):
    try:
        return data_fetcher.get_candles(symbol, days=days, resolution=resolution)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/news/{symbol}")
def news(symbol: str, limit: int = 5):
    try:
        return data_fetcher.get_news(symbol, limit=limit)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/predict/{symbol}")
def predict(symbol: str, budget: float = 1000.0, risk: str = "medium"):
    try:
        return data_fetcher.run_predict_engine(symbol, budget=budget, risk=risk)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



























