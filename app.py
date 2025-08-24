import os
import logging
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from data_fetcher import (
    get_quote_and_earnings,
    fetch_quote,
    fetch_earnings,
    FinnhubError,
)

# ---------- Logging ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("stackiq")

# ---------- App ----------
app = FastAPI(
    title="StackIQ",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# ---------- CORS ----------
origins_env = os.getenv("ALLOWED_ORIGINS", "*")
allow_origins = [o.strip() for o in origins_env.split(",")] if origins_env != "*" else ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------- Static frontend (optional) ----------
# If a /web directory exists in the repo, serve it at /web
if os.path.isdir("web"):
    app.mount("/web", StaticFiles(directory="web", html=True), name="web")

# ---------- Routes ----------
@app.get("/")
def root():
    return {"status": "ok", "message": "StackIQ API is running"}

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/debug")
def debug():
    """Quick check to confirm env is wired (won't reveal the key)."""
    return {"has_finnhub_key": bool(os.getenv("FINNHUB_API_KEY"))}

@app.get("/quote/{symbol}")
def quote(symbol: str):
    try:
        data = fetch_quote(symbol)
        return {"symbol": symbol.upper(), "quote": data}
    except FinnhubError as e:
        raise HTTPException(status_code=429 if "rate" in str(e).lower() else 400, detail=str(e))
    except Exception as e:
        log.exception("Unhandled error in /quote")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/earnings/{symbol}")
def earnings(symbol: str):
    try:
        data = fetch_earnings(symbol)
        return {"symbol": symbol.upper(), "earnings": data}
    except FinnhubError as e:
        raise HTTPException(status_code=429 if "rate" in str(e).lower() else 400, detail=str(e))
    except Exception as e:
        log.exception("Unhandled error in /earnings")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/test/{symbol}")
def test(symbol: str):
    try:
        payload = get_quote_and_earnings(symbol)
        payload["symbol"] = symbol.upper()
        return payload
    except FinnhubError as e:
        raise HTTPException(status_code=429 if "rate" in str(e).lower() else 400, detail=str(e))
    except Exception as e:
        log.exception("Unhandled error in /test")
        raise HTTPException(status_code=500, detail="Internal server error")














