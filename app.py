import os
import os.path as p

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse, JSONResponse

from data_fetcher import fetch_quote

APP_NAME = "stackiq-web"
APP_VERSION = "1.0.0"

app = FastAPI(title=APP_NAME, version=APP_VERSION)

# CORS (simple/lenient)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve the web UI at /web/
if p.isdir("web"):
    app.mount("/web", StaticFiles(directory="web", html=True), name="web")

# Root -> redirect to /web/
@app.get("/", include_in_schema=False)
def root():
    if p.isdir("web"):
        return RedirectResponse(url="/web/")
    return JSONResponse({"app": APP_NAME, "version": APP_VERSION})

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
        # Keep it 404 so the UI shows a clean “not found” msg (no app error page)
        raise HTTPException(status_code=404, detail="Symbol not found")
    return data

@app.get("/summary/{symbol}")
def summary(symbol: str):
    data = fetch_quote(symbol)
    if not data:
        raise HTTPException(status_code=404, detail="Symbol not found")

    pct = data.get("percent_change", 0.0)
    updown = "up" if pct >= 0 else "down"
    msg = (
        f"{data['symbol']}: {data['current']:.3f} "
        f"({updown} {abs(pct):.2f}% on the day). "
        f"Session range: {data['low']:.3f}–{data['high']:.3f}. "
        f"Prev close: {data['prev_close']:.3f}."
    )
    return {"symbol": data["symbol"], "summary": msg, "quote": data}
































