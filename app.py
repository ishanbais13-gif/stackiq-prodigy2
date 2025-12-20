from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from data_fetcher import (
    load_alpaca_config,
    get_latest_quote,
    get_bars,
    get_news,
    simple_predict_from_bars,
    UpstreamAPIError,
)

app = FastAPI(title="StackIQ V1 API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # lock this down later
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

CFG = None


@app.on_event("startup")
def _startup():
    global CFG
    CFG = load_alpaca_config()


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/quote/{symbol}")
def quote(symbol: str):
    try:
        return get_latest_quote(CFG, symbol)
    except UpstreamAPIError as e:
        raise HTTPException(status_code=e.status_code, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/candles/{symbol}")
def candles(
    symbol: str,
    timeframe: str = Query(default="1Day", description="1Min,5Min,15Min,1Hour,1Day"),
    days: int = Query(default=30, ge=1, le=365),
    limit: int = Query(default=1000, ge=1, le=10000),
):
    try:
        return get_bars(CFG, symbol, timeframe=timeframe, days=days, limit=limit)
    except UpstreamAPIError as e:
        raise HTTPException(status_code=e.status_code, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/news/{symbol}")
def news(symbol: str, limit: int = Query(default=10, ge=1, le=50)):
    try:
        return get_news(CFG, symbol, limit=limit)
    except UpstreamAPIError as e:
        raise HTTPException(status_code=e.status_code, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/predict/{symbol}")
def predict(
    symbol: str,
    budget: float = Query(default=100.0, gt=0),
    risk: str = Query(default="medium"),
    fractional: bool = Query(default=True),
):
    """
    V1 predict:
    - pulls daily candles
    - runs simple trend+momentum heuristic
    """
    try:
        candles = get_bars(CFG, symbol, timeframe="1Day", days=60, limit=200)
        bars = candles.get("bars", [])
        pred = simple_predict_from_bars(bars)
        return {
            "symbol": symbol.upper(),
            "timeframe": "1Day",
            "budget": budget,
            "risk": risk,
            "fractional": fractional,
            "prediction": pred,
        }
    except UpstreamAPIError as e:
        raise HTTPException(status_code=e.status_code, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

































