import os
import os.path
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse

from data_fetcher import fetch_quote, fetch_history

APP_NAME = "stackiq-web"
APP_VERSION = "v1.0.0"

app = FastAPI(title=APP_NAME, version=APP_VERSION)

# CORS: open for demo
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve /web if present
if os.path.isdir("web"):
    app.mount("/web", StaticFiles(directory="web", html=True), name="web")

@app.get("/", include_in_schema=False)
def root():
    if os.path.isdir("web"):
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
    pct = data.get("percent_change") or 0.0
    updown = "up" if pct >= 0 else "down"
    msg = (
        f"{data['symbol']}: {data['current']:.2f} ({updown} {abs(pct):.2f}% on the day). "
        f"Session range: {data['low']}–{data['high']}. Prev close {data['prev_close']}."
    )
    return {"symbol": data["symbol"], "summary": msg, "quote": data}

@app.get("/history/{symbol}")
def history(symbol: str, range: str = Query("1M", pattern="^(1M|3M|6M|1Y)$")):
    data = fetch_history(symbol, range)
    if not data:
        raise HTTPException(status_code=404, detail="history not available")
    return data














































