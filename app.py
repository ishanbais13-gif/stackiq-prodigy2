# app.py
import os
from typing import Optional, Dict, Any
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles

# ---- data layer (your existing helpers) ----
# These must already exist in data_fetcher.py.
# We only import the callables your earlier error message listed.
try:
    from data_fetcher import (
        get_price_and_earnings,  # -> Dict[str, Any]
        get_ticker_data,         # optional alt
    )
except Exception as e:
    # Fall back to a helpful error at runtime if imports break
    def _import_error(*_, **__):
        raise RuntimeError(
            "data_fetcher.py is missing required functions; "
            "expected: get_price_and_earnings / get_ticker_data"
        )
    get_price_and_earnings = _import_error
    get_ticker_data = _import_error

VERSION = "0.2.0"
SERVICE = "stackiq-web"

app = FastAPI(title="StackIQ", version=VERSION)

# --- CORS (allow your web UI + local dev) ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten later (add your domain)
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Static front-end under /web (./web/index.html) ---
if os.path.isdir("web"):
    app.mount("/web", StaticFiles(directory="web", html=True), name="web")

# ---------- Health / meta ----------
@app.get("/", response_class=PlainTextResponse)
def root():
    return "StackIQ backend is live."

@app.get("/health")
def health():
    return {"ok": True, "service": SERVICE}

@app.get("/status")
def status():
    # “uptime_seconds” is optional; leaving as 0 keeps it simple
    return {"app": "StackIQ", "status": "ok", "uptime_seconds": 0, "version": VERSION}

@app.get("/version")
def version():
    return {"version": VERSION}

@app.get("/envcheck")
def envcheck():
    # Minimal check that your API key is present (don’t leak the value)
    has_key = bool(os.getenv("STOCK_API_KEY") or os.getenv("FINNHUB_API_KEY") or os.getenv("RAPIDAPI_KEY"))
    return {"has_key": has_key}

# ---------- Data test endpoint ----------
@app.get("/test/{ticker}")
def test_ticker(
    ticker: str,
    pretty: Optional[int] = Query(default=0, description="Return pretty JSON when =1")
):
    """
    Returns combined price + earnings for a ticker.
    The structure matches what your front-end expects.
    """
    try:
        data = get_price_and_earnings(ticker.upper())
        # enforce the expected shape to avoid KeyError in UI
        price: Dict[str, Any] = data.get("price") or {}
        # Required keys your UI reads: c,d,dp,h,l,o,pc,v
        mapped_price = {
            "c": price.get("c"),     # current price
            "d": price.get("d"),     # absolute change
            "dp": price.get("dp"),   # % change
            "h": price.get("h"),
            "l": price.get("l"),
            "o": price.get("o"),
            "pc": price.get("pc"),
            "v": price.get("v"),
        }
        out = {
            "ticker": ticker.upper(),
            "price": mapped_price,
            "earnings": data.get("earnings", {}),
        }
        if pretty:
            return JSONResponse(out, media_type="application/json")
        return out
    except HTTPException:
        raise
    except KeyError as e:
        # This is the “Error: 'price'” you saw; return a clear message
        raise HTTPException(status_code=502, detail=f"Upstream payload missing key: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ---------- Recommendation engine ----------
@app.get("/recommend")
def recommend(
    ticker: str,
    horizon: str = Query(..., description="short|medium|long"),
    risk: str = Query(..., description="low|medium|high"),
    conviction: int = Query(5, ge=0, le=10),
):
    """
    Tiny rules engine:
      - looks at short-term % change (dp)
      - blends with risk & conviction to produce BUY / HOLD / AVOID
    """
    try:
        payload = get_price_and_earnings(ticker.upper())
        price = payload.get("price") or {}
        dp = price.get("dp")  # % change
        if dp is None:
            raise HTTPException(status_code=502, detail="Missing dp (percent change) for ticker.")

        # Normalize inputs
        hz = horizon.lower()
        r  = risk.lower()

        # Base score from % change (negative motion can be a buy opportunity)
        # Clamp dp to a sensible band
        dp_clamped = max(min(float(dp), 15.0), -15.0)
        score = 0.0 - (dp_clamped / 10.0)  # down -> positive score (value buy), up -> negative (froth)

        # Horizon adjustments
        if hz.startswith("short"):
            score *= 0.8  # be more conservative short-term
        elif hz.startswith("long"):
            score *= 1.2

        # Risk appetite
        if r == "low":
            score -= 0.3
        elif r == "high":
            score += 0.3

        # Conviction: nudge toward action when user is confident
        score += (conviction - 5) / 20.0  # -0.25..+0.25

        # Map score to label
        if score >= 0.25:
            rec = "BUY"
        elif score <= -0.25:
            rec = "AVOID"
        else:
            rec = "HOLD"

        return {
            "ticker": ticker.upper(),
            "inputs": {"horizon": hz, "risk": r, "conviction": conviction},
            "signals": {"dp": dp_clamped, "score": round(score, 3)},
            "recommendation": rec,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ---------- Local dev ----------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")), reload=True)





