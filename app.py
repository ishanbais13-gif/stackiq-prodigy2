import os
import os.path
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse

from data_fetcher import fetch_quote, fetch_history

APP_NAME = "stackiq-web"
APP_VERSION = "v1.0.0"

app = FastAPI(title=APP_NAME, version=APP_VERSION)

# --- CORS (open; tighten later if you add a real domain) ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Serve static site if /web exists ---
if os.path.isdir("web"):
    app.mount("/web", StaticFiles(directory="web", html=True), name="web")

# --- Root: redirect to UI (if present) ---
@app.get("/", include_in_schema=False)
def root():
    if os.path.isdir("web"):
        return RedirectResponse(url="/web/")
    return {"app": APP_NAME, "version": APP_VERSION}

# --- Health/version ---
@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/version")
def version():
    return {"app": APP_NAME, "version": APP_VERSION}

# --- Quote & Summary ---
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
    pct = data.get("percent_change") or 0.0
    updown = "up" if pct >= 0 else "down"
    msg = (
        f"{symbol.upper()}: {data['current']:.2f} ({updown} "
        f"{abs(pct):.2f}% on the day). "
        f"Session range: {data['low']:.2f}-{data['high']:.2f}. "
        f"Prev close {data['prev_close']:.2f}."
    )
    return {"symbol": data["symbol"], "summary": msg, "quote": data}

# --- History (drives the chart) ---
@app.get("/history/{symbol}")
def history(symbol: str, range: str = "1M"):
    """
    range ∈ {1M,3M,6M,1Y}
    Returns: {"symbol": "...", "range": "1M", "points": [{"time": ts, "close": c}, ...]}
    """
    points = fetch_history(symbol, range)
    if not points:
        # 404 to keep UI's "History error" state meaningful
        raise HTTPException(status_code=404, detail="history not found")
    return {"symbol": symbol.upper(), "range": range.upper(), "points": points}















































