from fastapi import FastAPI, HTTPException
from data_fetcher import get_quote, get_candles

app = FastAPI()

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/quote/{symbol}")
def quote(symbol: str):
    try:
        return get_quote(symbol)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/candles/{symbol}")
def candles(symbol: str, resolution: str = "D", count: int = 30):
    try:
        return get_candles(symbol, resolution, count)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))




















































