from __future__ import annotations

import os
import time
from typing import Any, Dict, List, Optional

import requests
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# -----------------------------
# Config
# -----------------------------
APP_NAME = "StackIQ API"
DEFAULT_PORT = 8000

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "").strip()
FINNHUB_BASE = "https://finnhub.io/api/v1"

# Allow local dev + (optionally) your Azure frontend origin later
ALLOWED_ORIGINS = [
    "http://localhost:3000",
    "http://127.0.0.1:3000",
]

# -----------------------------
# App
# -----------------------------
app = FastAPI(title=APP_NAME, version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS + ["*"],  # dev-friendly; tighten later
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------
# Helpers
# -----------------------------
def _now_ms() -> int:
    return int(time.time() * 1000)

def _ok(payload: Any) -> Dict[str, Any]:
    return {"ok": True, "data": payload}

def _fail(msg: str, *, code: str = "UPSTREAM_ERROR") -> Dict[str, Any]:
    return {"ok": False, "error": {"code": code, "message": msg}}

def _finnhub_get(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Safe Finnhub GET helper.
    Returns dict (parsed JSON) or raises requests.HTTPError.
    """
    if not FINNHUB_API_KEY:
        raise RuntimeError("FINNHUB_API_KEY is not set")

    params = params or {}
    params["token"] = FINNHUB_API_KEY

    url = f"{FINNHUB_BASE}{path}"
    r = requests.get(url, params=params, timeout=12)
    r.raise_for_status()
    return r.json()

# -----------------------------
# Routes
# -----------------------------
@app.get("/health")
def health() -> Dict[str, Any]:
    # Frontend uses this to show ONLINE/OFFLINE
    return {
        "status": "ok",
        "app": APP_NAME,
        "ts": _now_ms(),
        "finnhub_key_loaded": bool(FINNHUB_API_KEY),
    }

@app.get("/top-movers")
def top_movers() -> Dict[str, Any]:
    """
    Your frontend expects a list/array. We return:
      { ok: true, data: [...] }
    If Finnhub key isn't set, we return a small placeholder list.
    """
    try:
        # Finnhub doesn't provide a direct universal "top movers" endpoint
        # without extra logic. For now we provide placeholders until you
        # implement a real screener.
        if not FINNHUB_API_KEY:
            return _ok([
                {"symbol": "AAPL", "change": 0.0, "changePercent": 0.0, "price": 0.0},
                {"symbol": "MSFT", "change": 0.0, "changePercent": 0.0, "price": 0.0},
                {"symbol": "NVDA", "change": 0.0, "changePercent": 0.0, "price": 0.0},
            ])

        # Minimal example: quote a small set of tickers
        sample = ["AAPL", "MSFT", "NVDA", "TSLA", "AMZN"]
        movers: List[Dict[str, Any]] = []
        for sym in sample:
            q = _finnhub_get("/quote", {"symbol": sym})
            # Finnhub quote: c=current, d=change, dp=percent change, pc=prev close
            movers.append({
                "symbol": sym,
                "price": q.get("c"),
                "change": q.get("d"),
                "changePercent": q.get("dp"),
                "prevClose": q.get("pc"),
            })
        # sort by abs(changePercent) desc
        movers.sort(key=lambda x: abs(x.get("changePercent") or 0), reverse=True)
        return _ok(movers)

    except Exception as e:
        return _fail(f"Failed to load top movers: {e}")

@app.get("/signals")
def signals() -> Dict[str, Any]:
    """
    Frontend expects 'today’s picks + confidence'.
    Return a list of signals (even if empty).
    """
    try:
        # Placeholder until your signal engine is wired.
        # You can replace with your model/engine output later.
        demo = [
            {"symbol": "AAPL", "signal": "HOLD", "confidence": 0.55, "reason": "Demo placeholder"},
            {"symbol": "NVDA", "signal": "BUY", "confidence": 0.62, "reason": "Demo placeholder"},
        ]
        # If you want empty when no key:
        if not FINNHUB_API_KEY:
            return _ok([])

        return _ok(demo)

    except Exception as e:
        return _fail(f"Failed to load signals: {e}")

@app.get("/watchlist")
def watchlist() -> Dict[str, Any]:
    """
    Return watchlist items.
    Later: pull from DB/user profile. For now: placeholder.
    """
    try:
        # Placeholder watchlist
        items = [
            {"symbol": "AAPL", "notes": "Core tech"},
            {"symbol": "MSFT", "notes": "Cloud"},
        ]
        if not FINNHUB_API_KEY:
            return _ok([])  # or return _ok(items) if you want a demo list
        return _ok(items)

    except Exception as e:
        return _fail(f"Failed to load watchlist: {e}")

@app.get("/news")
def news() -> Dict[str, Any]:
    """
    Return latest news articles.
    If FINNHUB_API_KEY is set, pull from Finnhub general news.
    """
    try:
        if not FINNHUB_API_KEY:
            return _ok([])

        # Finnhub general news: category can be "general", "forex", etc.
        data = _finnhub_get("/news", {"category": "general"})
        # Normalize a small set of fields to keep frontend stable
        articles: List[Dict[str, Any]] = []
        for a in (data or [])[:25]:
            articles.append({
                "headline": a.get("headline"),
                "source": a.get("source"),
                "url": a.get("url"),
                "datetime": a.get("datetime"),
                "summary": a.get("summary"),
                "image": a.get("image"),
            })
        return _ok(articles)

    except Exception as e:
        return _fail(f"Failed to load news: {e}")

# Optional: root
@app.get("/")
def root() -> Dict[str, Any]:
    return {
        "name": APP_NAME,
        "status": "ok",
        "endpoints": ["/health", "/top-movers", "/signals", "/watchlist", "/news"],
    }

































