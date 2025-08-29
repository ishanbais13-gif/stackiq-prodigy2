# app.py
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict, Any
from data_fetcher import get_quote, get_history

APP_NAME = "stackiq-web"
APP_VERSION = "1.0.0"

app = FastAPI(title=APP_NAME, version=APP_VERSION)

# CORS so /web/index.html can call the API from same origin
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}

@app.get("/version")
def version() -> Dict[str, str]:
    return {"app": APP_NAME, "version": APP_VERSION}

@app.get("/quote/{symbol}")
def quote(symbol: str) -> Dict[str, Any]:
    data = get_quote(symbol.upper())
    if not data:
        raise HTTPException(status_code=404, detail="Symbol not found")
    return data

@app.get("/summary/{symbol}")
def summary(symbol: str) -> Dict[str, Any]:
    q = get_quote(symbol.upper())
    if not q:
        raise HTTPException(status_code=404, detail="Symbol not found")
    pct = q.get("percent_change", 0.0)
    direction = "up" if pct >= 0 else "down"
    msg = f"{q['symbol']}: {q['current']:.3f} ({direction} {abs(pct):.2f}% on the day). " \
          f"Session range: {q['low']:.2f}–{q['high']:.2f}. Prev close: {q['prev_close']:.2f}."
    return {"symbol": q["symbol"], "summary": msg, "quote": q}

# Simple history endpoint used by the chart buttons in /web/index.html
# Ranges supported: 1m, 3m, 6m, 1y
@app.get("/history/{symbol}")
def history(symbol: str, range: str = "1m") -> Dict[str, Any]:
    ok_ranges = {"1m", "3m", "6m", "1y"}
    r = range.lower()
    if r not in ok_ranges:
        raise HTTPException(status_code=400, detail=f"range must be one of {sorted(ok_ranges)}")
    series = get_history(symbol.upper(), r)
    if series is None:
        # 502 so your Status card shows a “history error” instead of crashing the app
        raise HTTPException(status_code=502, detail="Failed to load history")
    return {"symbol": symbol.upper(), "range": r, "series": series}


























