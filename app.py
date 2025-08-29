import os
import os.path as p
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

# Serve the web UI at /web/
if p.isdir("web"):
    # html=True lets /web/ serve index.html automatically
    app.mount("/web", StaticFiles(directory="web", html=True), name="web")


# Root -> redirect to /web/ (so you never see raw JSON unless you go to an API)
@app.get("/", include_in_schema=False)
def root():
    if p.isdir("web"):
        return RedirectResponse(url="/web/")
    return {"app": APP_NAME, "version": APP_VERSION}


@app.get("/health")
def health():
    # Keep health endpoint minimal and always OK if app booted.
    return {"status": "ok"}


@app.get("/version")
def version():
    return {"app": APP_NAME, "version": APP_VERSION}


@app.get("/quote/{symbol}")
def quote(symbol: str):
    data = fetch_quote(symbol)
    if not data:
        # Keep 404 but with stable JSON body. The UI watches for this.
        raise HTTPException(status_code=404, detail="Symbol not found")
    return data


@app.get("/summary/{symbol}")
def summary(symbol: str):
    data = fetch_quote(symbol)
    if not data:
        raise HTTPException(status_code=404, detail="Symbol not found")

    pct = data.get("percent_change")
    updown = "up" if pct is not None and pct >= 0 else "down"
    pct_abs = abs(pct) if pct is not None else 0.0

    msg = (
        f"{data['symbol']}: {data['current']} ({updown} {pct_abs:.2f}% on the day). "
        f"Session range: {data['low']}–{data['high']}. Prev close: {data['prev_close']}."
    )
    return {"symbol": data["symbol"], "summary": msg, "quote": data}


































