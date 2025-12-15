from fastapi import FastAPI, HTTPException
from data_fetcher import (
    get_latest_quote,
    get_bars,
    simple_predict_from_bars,
)

app = FastAPI(title="StackIQ API", version="1.0")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/quote/{symbol}")
def quote(symbol: str):
    try:
        return get_latest_quote(symbol)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/bars/{symbol}")
def bars(symbol: str, timeframe: str = "1Day", days: int = 30):
    try:
        return get_bars(symbol, timeframe=timeframe, days=days)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/predict/{symbol}")
def predict(symbol: str, timeframe: str = "1Day", days: int = 30):
    try:
        data = get_bars(symbol, timeframe=timeframe, days=days)
        prediction = simple_predict_from_bars(data["bars"])
        return {
            "symbol": symbol.upper(),
            "timeframe": timeframe,
            "prediction": prediction,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
































