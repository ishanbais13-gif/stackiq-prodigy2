import os
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from starlette.responses import JSONResponse
from starlette.requests import Request

from data_fetcher import get_price_and_earnings

APP = FastAPI(title="StackIQ")

# CORS (allow your web front-end to call the API)
APP.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------- Routes ----------

@APP.get("/health")
async def health():
    return {"ok": True}

@APP.get("/test/{ticker}")
async def test_ticker(ticker: str, pretty: int | None = None):
    """
    Returns a shape the UI expects:
    {
      "ticker": "AAPL",
      "price": {"c": ..., "d": ..., "dp": ..., "h": ..., "l": ..., "o": ..., "pc": ..., "v": ...},
      "earnings": {"earningsCalendar": [ ... ] }
    }
    """
    data = await get_price_and_earnings(ticker.strip().upper())

    if not data or "price" not in data or "c" not in data["price"]:
        # exact message the UI shows when no data:
        raise HTTPException(status_code=404, detail="Ticker not found or no data")

    # Optionally pretty-print if ?pretty=1 (handy in browser)
    if pretty:
        return JSONResponse(data, media_type="application/json")
    return data

# Serve the static web app from /web  (folder: ./web)
if os.path.isdir("web"):
    APP.mount("/web", StaticFiles(directory="web", html=True), name="web")








