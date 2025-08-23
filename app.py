# app.py
import os
import time
from typing import Dict, Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles

START_TIME = time.time()
APP_VERSION = os.getenv("STACKIQ_VERSION", "0.2.0")

app = FastAPI(title="StackIQ", version=APP_VERSION)

# --- Optional import of your real fetcher ---
fetch_impl = None
try:
    import data_fetcher  # your file
    # pick a callable that exists
    for name in [
        "get_quote_and_earnings",
        "get_price_and_earnings",
        "get_stock_data",
        "get_ticker_data",
        "get",
        "fetch",
    ]:
        if hasattr(data_fetcher, name):
            fetch_impl = getattr(data_fetcher, name)
            break
except Exception:
    fetch_impl = None

# --- Health/Status ---
@app.get("/health", response_class=JSONResponse)
def health():
    return {"ok": True, "service": "stackiq-web"}

@app.get("/version", response_class=JSONResponse)
def version():
    return {"version": APP_VERSION}

@app.get("/status", response_class=JSONResponse)
def status():
    return {
        "app": "StackIQ",
        "status": "ok",
        "uptime_seconds": int(time.time() - START_TIME),
        "version": APP_VERSION,
    }

# --- Test API (safe wrapper) ---
def _shape_quote(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize a few common quote keys so the UI doesn't break."""
    if not isinstance(raw, dict):
        return {}
    price = raw.get("price") or raw.get("c") or raw.get("current") or raw.get("last")
    change = raw.get("change") or raw.get("d")
    pct = raw.get("percent_change") or raw.get("dp")
    high = raw.get("high") or raw.get("h")
    low = raw.get("low") or raw.get("l")
    open_ = raw.get("open") or raw.get("o")
    prev = raw.get("prevClose") or raw.get("pc")
    vol = raw.get("volume") or raw.get("v")
    return {
        "price": price,
        "change": change,
        "percent_change": pct,
        "high": high,
        "low": low,
        "open": open_,
        "prev_close": prev,
        "volume": vol,
    }

@app.get("/test/{ticker}", response_class=JSONResponse)
def test_ticker(ticker: str, pretty: int = 0):
    if not ticker:
        raise HTTPException(status_code=400, detail="ticker required")

    # If you have a real fetcher, use it; otherwise return a stub so the UI renders.
    if fetch_impl:
        try:
            data = fetch_impl(ticker)
            # Accept either dict with "price"/"earnings" or any shape we can normalize
            if isinstance(data, dict) and ("price" in data or "earnings" in data):
                return data
            # try to normalize generic quote-only responses
            quote = _shape_quote(data if isinstance(data, dict) else {})
            return {"ticker": ticker.upper(), "price": quote, "earnings": {"earningsCalendar": []}}
        except HTTPException:
            raise
        except Exception as e:
            # Surface fetch errors but keep the process alive
            raise HTTPException(status_code=500, detail=str(e))
    else:
        # Fallback stub so the page stays up even if data_fetcher is broken
        return {
            "ticker": ticker.upper(),
            "price": {"c": 100.0, "d": 0.0, "dp": 0.0, "h": 101.0, "l": 99.0, "o": 100.5, "pc": 100.2, "v": 1000000},
            "earnings": {"earningsCalendar": []},
        }

# --- Static UI (/web) ---
web_dir = os.path.join(os.path.dirname(__file__), "web")
if os.path.isdir(web_dir):
    app.mount("/web", StaticFiles(directory=web_dir, html=True), name="web")
else:
    @app.get("/web", response_class=PlainTextResponse)
    def web_placeholder():
        return "StackIQ backend is live. (No /web directory found.)"

# Root convenience
@app.get("/", response_class=PlainTextResponse)
def root():
    return "StackIQ backend is live."






