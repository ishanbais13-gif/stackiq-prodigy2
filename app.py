import os
import os.path as path
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse, JSONResponse

from data_fetcher import fetch_quote, fetch_history

APP_NAME = "stackiq-web"
APP_VERSION = "v1.0.0"

app = FastAPI(title=APP_NAME, version=APP_VERSION)

# CORS (loose — fine for this demo)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve static site if /web exists
if path.isdir("web"):
    app.mount("/web", StaticFiles(directory="web", html=True), name="web")

# ------------- basic ----------
@app.get("/", include_in_schema=False)
def root():
    # If web folder exists, go to UI
    if path.isdir("web"):
        return RedirectResponse(url="/web/")
    return {"app": APP_NAME, "version": APP_VERSION}

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/version")
def version():
    return {"app": APP_NAME, "version": APP_VERSION}

# ------------- data api -------
@app.get("/quote/{symbol}")
def quote(symbol: str):
    try:
        data = fetch_quote(symbol)
        if not data or data.get("current") is None:
            raise HTTPException(status_code=404, detail="symbol not found")
        return data
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/summary/{symbol}")
def summary(symbol: str):
    try:
        q = fetch_quote(symbol)
        if not q or q.get("current") is None:
            raise HTTPException(status_code=404, detail="symbol not found")

        pct = q.get("percent_change") or 0.0
        updown = "up" if pct >= 0 else "down"
        msg = (
            f"{q['symbol']}: {q['current']} ({updown} {abs(pct):.2f}% on the day). "
            f"Session range: {q['low']}–{q['high']}. Prev close {q['prev_close']}."
        )
        return {"symbol": q["symbol"], "summary": msg, "quote": q}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/history/{symbol}")
def history(symbol: str, range: str = "1M"):
    try:
        data = fetch_history(symbol, range)
        return data
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

















































