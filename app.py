from __future__ import annotations

import os
import time
from typing import Any, Dict, List, Optional

import requests
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

APP_NAME = "StackIQ API"
VERSION = "1.0.0"

# ---- CORS (frontend on localhost) ----
ALLOWED_ORIGINS = [
    "http://localhost:3000",
    "http://127.0.0.1:3000",
]

app = FastAPI(title=APP_NAME, version=VERSION)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS + ["*"],  # dev-friendly; lock down later
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---- Alpaca env (OPTIONAL) ----
ALPACA_API_KEY = os.getenv("ALPACA_API_KEY", "").strip()
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY", "").strip()

# Use paper by default (safe). You can change later.
ALPACA_BASE_URL = os.getenv("ALPACA_BASE_URL", "https://paper-api.alpaca.markets").strip()
ALPACA_DATA_URL = os.getenv("ALPACA_DATA_URL", "https://data.alpaca.markets").strip()

def _now_ms() -> int:
    return int(time.time() * 1000)

def _ok(data: Any) -> Dict[str, Any]:
    return {"ok": True, "data": data}

def _fail(msg: str, code: str = "UPSTREAM_ERROR") -> Dict[str, Any]:
    return {"ok": False, "error": {"code": code, "message": msg}}

def _alpaca_headers() -> Dict[str, str]:
    return {
        "APCA-API-KEY-ID": ALPACA_API_KEY,
        "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
    }

def _alpaca_ready() -> bool:
    return bool(ALPACA_API_KEY and ALPACA_SECRET_KEY)

def _alpaca_get(url: str, params: Optional[Dict[str, Any]] = None) -> Any:
    """
    Safe Alpaca GET wrapper. Raises on bad status.
    """
    r = requests.get(url, headers=_alpaca_headers(), params=params or {}, timeout=12)
    r.raise_for_status()
    return r.json()

# ---- ROUTES ----

@app.get("/")
def root() -> Dict[str, Any]:
    return {
        "name": APP_NAME,
        "version": VERSION,
        "status": "ok",
        "ts": _now_ms(),
        "alpaca_ready": _alpaca_ready(),
        "endpoints": ["/health", "/top-movers", "/signals", "/watchlist", "/news"],
    }

@app.get("/health")
def health() -> Dict[str, Any]:
    # This must NEVER crash. It drives your dashboard ONLINE/OFFLINE.
    return {
        "status": "ok",
        "app": APP_NAME,
        "ts": _now_ms(),
        "alpaca_ready": _alpaca_ready(),
    }

@app.get("/top-movers")
def top_movers() -> Dict[str, Any]:
    """
    If Alpaca keys exist, try to return real movers (basic placeholder logic).
    Otherwise return [] so frontend doesn't die.
    """
    try:
        if not _alpaca_ready():
            return _ok([])

        # Alpaca doesn't give a single universal "top movers" endpoint.
        # We’ll do a simple "most active" using latest trades for a small universe.
        # You can expand later.
        universe = ["AAPL", "MSFT", "NVDA", "TSLA", "AMZN", "META", "GOOGL"]
        out: List[Dict[str, Any]] = []

        for sym in universe:
            # Latest trade
            trade = _alpaca_get(f"{ALPACA_DATA_URL}/v2/stocks/{sym}/trades/latest")
            t = (trade or {}).get("trade") or {}
            price = t.get("p")

            # Latest quote (for spread-ish)
            quote = _alpaca_get(f"{ALPACA_DATA_URL}/v2/stocks/{sym}/quotes/latest")
            q = (quote or {}).get("quote") or {}
            bid = q.get("bp")
            ask = q.get("ap")

            out.append({
                "symbol": sym,
                "price": price,
                "bid": bid,
                "ask": ask,
            })

        return _ok(out)

    except Exception as e:
        # Never throw -> frontend should still render
        return _fail(f"top-movers failed: {e}")

@app.get("/signals")
def signals() -> Dict[str, Any]:
    """
    Your model/logic later.
    For now: safe empty list (or demo list if you want).
    """
    try:
        # Keep it empty unless you want to show demo picks:
        return _ok([])
    except Exception as e:
        return _fail(f"signals failed: {e}")

@app.get("/watchlist")
def watchlist() -> Dict[str, Any]:
    """
    Eventually: DB/user-specific. For now: safe empty list.
    """
    try:
        return _ok([])
    except Exception as e:
        return _fail(f"watchlist failed: {e}")

@app.get("/news")
def news() -> Dict[str, Any]:
    """
    If you have a news provider later, plug it in.
    For now: safe empty list.
    """
    try:
        return _ok([])
    except Exception as e:
        return _fail(f"news failed: {e}")


































