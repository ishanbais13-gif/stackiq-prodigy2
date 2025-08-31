import os
from pathlib import Path
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse

from data_fetcher import fetch_quote, fetch_history

APP_NAME = "stackiq-web"
APP_VERSION = "1.0.0"

app = FastAPI(title=APP_NAME, version=APP_VERSION)

# CORS (open)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Resolve absolute /web folder path (works under Azure Oryx)
BASE_DIR = Path(__file__).resolve().parent
WEB_DIR = (BASE_DIR / "web").resolve()
if WEB_DIR.is_dir():
    app.mount("/web", StaticFiles(directory=str(WEB_DIR), html=True), name="web")

# Root -> redirect to /web if present
@app.get("/", include_in_schema=False)
def root():
    if WEB_DIR.is_dir():
        return RedirectResponse(url="/web/")
    return {"app": APP_NAME, "version": APP_VERSION}

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/version")
def version():
    return {"app": APP_NAME, "version": APP_VERSION}

@app.get("/quote/{symbol}")
def quote(symbol: str):
    data = fetch_quote(symbol)
    if not data:
        raise HTTPException(status_code=404, detail="symbol not found")
    return data

@app.get("/summary/{symbol}")
def summary(symbol: str):
    data = fetch_quote(symbol)
    if not data:
        raise HTTPException(status_code=404, detail="symbol not found")

    pct = data.get("percent_change")
    pct_val = 0.0 if pct is None else float(pct)
    updown = "up" if pct_val >= 0 else "down"
    msg = (
        f"{data['symbol']}: {data['current']} ({updown} {abs(pct_val):.2f}% on the day). "
        f"Session range: {data['low']}–{data['high']}. Prev close {data['prev_close']}."
    )
    return {"symbol": data["symbol"], "summary": msg, "quote": data}

@app.get("/history/{symbol}")
def history(symbol: str, range: str = "1M"):
    """
    Historical close prices for charting.
    range ∈ {1M, 3M, 6M, 1Y}
    Returns: {"symbol","range","points":[{"t": epochSec, "c": close}, ...]}
    """
    pts = fetch_history(symbol, range)
    if pts is None:
        raise HTTPException(status_code=404, detail="history not found")
    return {"symbol": symbol.upper(), "range": range, "points": pts}












































