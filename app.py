import os
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse

from data_fetcher import fetch_quote, fetch_history

APP_NAME = "stackiq-web"
APP_VERSION = "v1.0.0"

app = FastAPI(title=APP_NAME, version=APP_VERSION)

# CORS – open for demo; tighten later if you want
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve /web if folder exists
if os.path.isdir("web"):
    app.mount("/web", StaticFiles(directory="web", html=True), name="web")


@app.get("/", include_in_schema=False)
def root() -> Any:
    if os.path.isdir("web"):
        return RedirectResponse(url="/web/")
    return {"app": APP_NAME, "version": APP_VERSION}


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/version")
def version() -> Dict[str, str]:
    return {"app": APP_NAME, "version": APP_VERSION}


@app.get("/quote/{symbol}")
def quote(symbol: str) -> Dict[str, Any]:
    data = fetch_quote(symbol)
    if not data:
        raise HTTPException(status_code=404, detail="symbol not found")
    return data


@app.get("/summary/{symbol}")
def summary(symbol: str) -> Dict[str, Any]:
    data = fetch_quote(symbol)
    if not data:
        raise HTTPException(status_code=404, detail="symbol not found")

    pct = data.get("percent_change") or 0.0
    updown = "up" if pct >= 0 else "down"
    msg = (
        f"{data['symbol']}: {data['current']:.2f} "
        f"({updown} {abs(pct):.2f}% on the day). "
        f"Session range: {data['low']:.2f}–{data['high']:.2f}. "
        f"Prev close {data['prev_close']:.2f}."
    )
    return {"symbol": data["symbol"], "summary": msg, "quote": data}


# ---------- history ----------
RANGE_TO_DAYS = {
    "1M": 31,
    "3M": 95,
    "6M": 190,
    "1Y": 380,
}

@app.get("/history/{symbol}")
def history(
    symbol: str,
    range: str = Query("1M", pattern="^(1M|3M|6M|1Y)$")
) -> Dict[str, Any]:
    days = RANGE_TO_DAYS.get(range, 31)
    end = datetime.utcnow()
    start = end - timedelta(days=days)

    points = fetch_history(symbol, start, end, resolution="D")
    # points = list[{"t": unix_seconds, "c": close}]
    return {"symbol": symbol.upper(), "range": range, "points": points}



















































