from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

# If these modules exist, keep the imports.
# If a name doesn't exist yet, just comment that import out temporarily.
import data_fetcher
import engine
import backtest
import optimize

app = FastAPI(title="StackIQ API", version="1.0.0")

# Allow your front-end / local testing
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health_check():
    return {"status": "ok"}


@app.get("/quote/{symbol}")
def get_quote(symbol: str):
    """
    Thin wrapper around data_fetcher to return latest quote.
    """
    try:
        quote = data_fetcher.get_latest_quote(symbol)
        return {"symbol": symbol.upper(), "quote": quote}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/candles/{symbol}")
def get_candles(symbol: str, timeframe: str = "D", lookback_days: int = 30):
    """
    Return OHLCV candles from data_fetcher.
    """
    try:
        candles = data_fetcher.get_candles(symbol, timeframe=timeframe, lookback_days=lookback_days)
        return {"symbol": symbol.upper(), "timeframe": timeframe, "data": candles}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/predict/{symbol}")
def predict_symbol(symbol: str, budget: float = 1000.0):
    """
    Use your engine module to generate a prediction for a single symbol.
    """
    try:
        result = engine.predict_single(symbol, budget=budget)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/backtest")
def run_backtest(request: dict):
    """
    Run a backtest. 'request' can include symbol, start/end dates, strategy params, etc.
    """
    try:
        results = backtest.run_backtest(request)
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/optimize")
def optimize_portfolio(request: dict):
    """
    Optimize a portfolio given constraints in 'request'.
    """
    try:
        results = optimize.optimize_portfolio(request)
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))






































































