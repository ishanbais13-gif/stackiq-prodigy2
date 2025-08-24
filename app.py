import os
import json
import logging
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse

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

# ---------- CORS ----------
origins_env = os.getenv("ALLOWED_ORIGINS", "*")
allow_origins = [o.strip() for o in origins_env.split(",")] if origins_env != "*" else ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------- Static web (serves /web/index.html if present) ----------
if os.path.isdir("web"):
    app.mount("/web", StaticFiles(directory="web", html=True), name="web")

# ---------- Routes ----------
@app.get("/")
def root():
    return {"status": "ok", "message": "StackIQ API is running"}

@app.get("/quote/{symbol}")
def quote(symbol: str):
    try:
        data = fetch_quote(symbol)
        return {"symbol": symbol.upper(), "quote": data}
    except FinnhubError as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/earnings/{symbol}")
def earnings(symbol: str):
    try:
        data = fetch_earnings(symbol)
        return {"symbol": symbol.upper(), "earnings": data}
    except FinnhubError as e:
        raise HTTPException(status_code=400, detail=str(e))

# Combined endpoint you tried: /test/{symbol}
@app.get("/test/{symbol}")
def test(symbol: str):
    try:
        return {"symbol": symbol.upper(), **get_quote_and_earnings(symbol)}
    except FinnhubError as e:
        raise HTTPException(status_code=400, detail=str(e))













