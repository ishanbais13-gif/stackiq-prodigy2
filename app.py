import os
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse

from data_fetcher import fetch_quote

APP_NAME = "stackiq-web"
APP_VERSION = "1.0.0"

app = FastAPI(title=APP_NAME, version=APP_VERSION)

# CORS (keep simple/lenient for now)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve the web UI at /web/ (absolute path so it works on Azure)
WEB_DIR = (Path(__file__).resolve().parent / "web")
if WEB_DIR.exists():
    app.mount(
        "/web/",
        StaticFiles(directory=str(WEB_DIR), html=True),
        name="web",
    )

# Root → redirect to /web/
@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/web/")

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
        raise HTTPException(status_code=404, detail="Symbol not found")
    return data

@app.get("/summary/{symbol}")
def summary(symbol: str):
    data = fetch_quote(symbol)
    if not data:
        raise HTTPException(status_code=404, detail="Symbol not found")

    pct = data.get("percent_change")
    updown = "up" if (pct or 0) >= 0 else "down"
    msg = (
        f"{data['symbol']}: {data['current']} "
        f"({updown} {abs(pct):.2f}% on the day). "
        f"Session range: {data['low']}–{data['high']}. "
        f"Prev close: {data['prev_close']}."
    )
    return {"symbol": data["symbol"], "summary": msg, "quote": data}




























