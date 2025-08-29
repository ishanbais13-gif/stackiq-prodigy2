import os
import os.path as path
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse
from data_fetcher import fetch_quote

APP_NAME = "stackiq-web"
APP_VERSION = "1.0.0"

app = FastAPI(title=APP_NAME, version=APP_VERSION)

# CORS (keep simple)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve the web UI at /web
if os.path.isdir("web"):
    # html=True allows serving index.html automatically
    app.mount("/web", StaticFiles(directory="web", html=True), name="web")

# Root -> redirect to /web (so we never show a blank page)
@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/web/")

@app.get("/health")
def health():
    # simple liveness check; avoid any network calls here
    return {"status": "ok"}

@app.get("/version")
def version():
    return {"app": APP_NAME, "version": APP_VERSION}

@app.get("/quote/{symbol}")
def quote(symbol: str):
    data = fetch_quote(symbol)
    if not data:
        # IMPORTANT: we intentionally return 404 with JSON,
        # NOT an unhandled error, so Azure never shows its error page.
        raise HTTPException(status_code=404, detail="Symbol not found")
    return data

@app.get("/summary/{symbol}")
def summary(symbol: str):
    data = fetch_quote(symbol)
    if not data:
        raise HTTPException(status_code=404, detail="Symbol not found")

    pct = data.get("percent_change") or 0.0
    updown = "up" if pct >= 0 else "down"
    msg = (
        f"{data['symbol']}: {data['current']} ({updown} {abs(pct):.2f}% on the day). "
        f"Session range: {data['low']}–{data['high']}. Prev close: {data['prev_close']}."
    )
    return {"symbol": data["symbol"], "summary": msg, "quote": data}






























