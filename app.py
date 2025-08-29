import os
import logging
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse, JSONResponse

from data_fetcher import fetch_quote, fetch_debug

APP_NAME = "stackiq-web"
APP_VERSION = "1.0.0"

# Basic logging to stdout (shows in Azure "Log stream")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("stackiq")

app = FastAPI(title=APP_NAME, version=APP_VERSION)

# Permissive CORS (simple)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve the static web UI at /web
if os.path.isdir("web"):
    app.mount("/web", StaticFiles(directory="web", html=True), name="web")


# Root -> redirect to /web/
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
        # If upstream failed or symbol unknown, return 404 for the UI
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
        f"{data['symbol']}: {data['current']} ({updown} {abs(pct):.2f}% on the day). "
        f"Session range: {data['low']}–{data['high']}. Prev close: {data['prev_close']}."
    )
    return {"symbol": data["symbol"], "summary": msg, "quote": data}


@app.get("/debug/{symbol}")
def debug(symbol: str):
    """Returns detailed info about upstream calls. Use this to see why a symbol fails."""
    info = fetch_debug(symbol)
    # 200 even if no result — this endpoint is for diagnostics.
    return JSONResponse(info)

































