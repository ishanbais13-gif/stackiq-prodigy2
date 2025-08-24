import os
import logging
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from data_fetcher import (
    get_quote_and_earnings,
    fetch_quote,
    fetch_earnings,
    fetch_history,
    FinnhubError,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("stackiq")

app = FastAPI(
    title="StackIQ",
    version="1.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# ---- CORS ----
origins_env = os.getenv("ALLOWED_ORIGINS", "*")
allow_origins = [o.strip() for o in origins_env.split(",")] if origins_env != "*" else ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---- Static UI (/web) ----
if os.path.isdir("web"):
    app.mount("/web", StaticFiles(directory="web", html=True), name="web")

# ---- Routes ----
@app.get("/")
def root():
    return {"status": "ok", "message": "StackIQ API is running"}

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/version")
def version():
    return {"version": app.version}

@app.get("/quote/{symbol}")
def quote(symbol: str):
    try:
        data = fetch_quote(symbol)
        return {"symbol": symbol.upper(), "quote": data}
    except FinnhubError as e:
        raise HTTPException(status_code=429 if "rate" in str(e).lower() else 400, detail=str(e))
    except Exception as e:
        log.exception("quote error")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/earnings/{symbol}")
def earnings(symbol: str):
    try:
        data = fetch_earnings(symbol)
        return {"symbol": symbol.upper(), "earnings": data}
    except FinnhubError as e:
        raise HTTPException(status_code=429 if "rate" in str(e).lower() else 400, detail=str(e))
    except Exception as e:
        log.exception("earnings error")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/history/{symbol}")
def history(symbol: str, days: int = 60):
    try:
        days = max(5, min(days, 365))
        data = fetch_history(symbol, days)
        return data
    except FinnhubError as e:
        raise HTTPException(status_code=429 if "rate" in str(e).lower() else 400, detail=str(e))
    except Exception as e:
        log.exception("history error")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/test/{symbol}")
def test(symbol: str):
    """Convenience composite for the UI and manual testing."""
    try:
        payload = get_quote_and_earnings(symbol)
        payload["symbol"] = symbol.upper()
        # also include a small slice of history for the UI preview
        payload["history"] = fetch_history(symbol, 30)
        return payload
    except FinnhubError as e:
        raise HTTPException(status_code=429 if "rate" in str(e).lower() else 400, detail=str(e))
    except Exception as e:
        log.exception("test error")
        raise HTTPException(status_code=500, detail="Internal server error")

        )















