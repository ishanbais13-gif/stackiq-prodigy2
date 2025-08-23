import os
import json
import logging
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse

from data_fetcher import get_quote_and_earnings, fetch_quote, fetch_earnings, FinnhubError

# ---------- Logging ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("stackiq")

# ---------- App ----------
app = FastAPI(
    title="StackIQ",
    version="1.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# --------- CORS ----------
origins_env = os.getenv("ALLOWED_ORIGINS", "*")
allow_origins = [o.strip() for o in origins_env.split(",")] if origins_env != "*" else ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------- Utilities ----------
def pretty_json(data: Any, pretty: Optional[int]) -> Any:
    """If ?pretty=1 return a pretty-printed JSONResponse (for browser testing)."""
    if pretty:
        return PlainTextResponse(json.dumps(data, indent=2, sort_keys=False), media_type="application/json")
    return JSONResponse(content=data)


# ---------- Health ----------
@app.get("/health")
def health():
    return {"ok": True}


# ---------- API: combined ----------
@app.get("/test/{ticker}")
def test_ticker(ticker: str, pretty: Optional[int] = None):
    try:
        data = get_quote_and_earnings(ticker)
    except FinnhubError as e:
        # Clear server/config issue (missing key, etc.)
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:  # noqa
        log.exception("Upstream error while fetching %s", ticker)
        raise HTTPException(status_code=500, detail="Upstream error")

    if not data:
        raise HTTPException(status_code=404, detail="Ticker not found or no data")
    return pretty_json(data, pretty)


# ---------- API: quote-only ----------
@app.get("/quote/{ticker}")
def quote_only(ticker: str, pretty: Optional[int] = None):
    try:
        q = fetch_quote(ticker)
    except FinnhubError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception:
        log.exception("Error fetching quote for %s", ticker)
        raise HTTPException(status_code=500, detail="Upstream error")

    if not q:
        raise HTTPException(status_code=404, detail="Ticker not found or no data")

    return pretty_json(q, pretty)


# ---------- API: earnings-only ----------
@app.get("/earnings/{ticker}")
def earnings_only(ticker: str, pretty: Optional[int] = None):
    try:
        e = fetch_earnings(ticker)
    except FinnhubError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception:
        log.exception("Error fetching earnings for %s", ticker)
        raise HTTPException(status_code=500, detail="Upstream error")

    return pretty_json(e or {"earningsCalendar": []}, pretty)


# ---------- Static UI ----------
if os.path.isdir("web"):
    app.mount("/web", StaticFiles(directory="web"), name="web")
    log.info("Mounted static /web")

@app.get("/web")
def web_index_redirect():
    # Always serve the SPA/HTML if present
    path = os.path.join("web", "index.html")
    if os.path.isfile(path):
        return FileResponse(path)
    return JSONResponse({"detail": "web/index.html not found"}, status_code=404)


# ---------- Root ----------
@app.get("/")
def root():
    return {"message": "StackIQ backend is Live."}


# ---------- Favicon (avoid 404 noise) ----------
@app.get("/favicon.ico")
def favicon():
    return PlainTextResponse("", media_type="text/plain")


# ---------- Error Handlers ----------
@app.exception_handler(HTTPException)
async def http_exc_handler(_: Request, exc: HTTPException):
    return JSONResponse({"detail": exc.detail}, status_code=exc.status_code)

@app.exception_handler(Exception)
async def unhandled_exc_handler(_: Request, exc: Exception):
    log.exception("Unhandled error: %s", exc)
    return JSONResponse({"detail": "Internal Server Error"}, status_code=500)


# ---------- Local run (optional) ----------
if __name__ == "__main__":
    # For local testing only: uvicorn app:app --reload
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))











