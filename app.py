import time
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from data_fetcher import get_quote, get_bars, APIError

app = FastAPI(title="StackIQ API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

def iso_z(dt: datetime) -> str:
    # Always Zulu time
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

@app.get("/health")
def health():
    return {"status": "ok", "time": int(time.time()), "version": "1.0.0"}

@app.get("/quote/{symbol}")
def quote(
    symbol: str,
    feed: str = Query(default="iex", description="Alpaca feed (usually 'iex'; 'sip' requires entitlement)"),
):
    try:
        return get_quote(symbol, feed=feed)
    except APIError as e:
        raise HTTPException(status_code=e.status_code, detail={"message": str(e), **e.details})
    except Exception as e:
        raise HTTPException(status_code=500, detail={"message": "Unexpected server error", "error": str(e)})

@app.get("/candles/{symbol}")
def candles(
    symbol: str,
    timeframe: str = Query(default="1Day", description="Examples: 1Min, 5Min, 15Min, 1Hour, 1Day"),
    days: int = Query(default=30, ge=1, le=365, description="Lookback window in days (if start/end not provided)"),
    start: Optional[str] = Query(default=None, description="ISO 8601 Z time, e.g. 2025-12-01T00:00:00Z"),
    end: Optional[str] = Query(default=None, description="ISO 8601 Z time, e.g. 2025-12-13T00:00:00Z"),
    limit: int = Query(default=1000, ge=1, le=10000),
    feed: str = Query(default="iex", description="Alpaca feed"),
    adjustment: str = Query(default="raw", description="raw or split or dividend or all"),
):
    # If user doesn't supply start/end, derive from `days`
    now = datetime.now(timezone.utc)
    if end is None:
        end = iso_z(now)
    if start is None:
        start = iso_z(now - timedelta(days=days))

    try:
        return get_bars(
            symbol=symbol,
            timeframe=timeframe,
            start=start,
            end=end,
            limit=limit,
            feed=feed,
            adjustment=adjustment,
        )
    except APIError as e:
        raise HTTPException(status_code=e.status_code, detail={"message": str(e), **e.details})
    except Exception as e:
        raise HTTPException(status_code=500, detail={"message": "Unexpected server error", "error": str(e)})

@app.get("/")
def root():
    return {
        "name": "StackIQ API",
        "version": "1.0.0",
        "endpoints": ["/health", "/quote/{symbol}", "/candles/{symbol}"],
        "required_env": ["ALPACA_API_KEY", "ALPACA_SECRET_KEY"],
    }































