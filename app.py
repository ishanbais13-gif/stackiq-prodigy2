# app.py
import os
import math
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import JSONResponse

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
if not FINNHUB_API_KEY:
    # Fail fast with a clear message in logs and a friendly 500 at runtime.
    raise RuntimeError("Set FINNHUB_API_KEY environment variable.")

FINNHUB_BASE = "https://finnhub.io/api/v1"

app = FastAPI(title="stackiq-web", version="1.0.0")

# CORS (allow your web app to call these endpoints)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # lock down if you have a specific domain
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- tiny in-memory cache to avoid rate limits (free tier) ---
_cache: Dict[str, Dict[str, Any]] = {}  # key -> {"data":..., "exp": datetime}
CACHE_SECS = 5

def _get_cache(key: str) -> Optional[Dict[str, Any]]:
    item = _cache.get(key)
    if not item:
        return None
    if datetime.utcnow() >= item["exp"]:
        _cache.pop(key, None)
        return None
    return item["data"]

def _set_cache(key: str, data: Dict[str, Any]):
    _cache[key] = {"data": data, "exp": datetime.utcnow() + timedelta(seconds=CACHE_SECS)}

def _pretty(resp: Any, pretty: bool) -> JSONResponse:
    return JSONResponse(resp, media_type="application/json")


@app.get("/health")
async def health():
    return {"status": "ok"}


async def _finnhub_get(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    params = {**params, "token": FINNHUB_API_KEY}
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get(f"{FINNHUB_BASE}{path}", params=params)
        # Finnhub returns 200 even for some errors; normalize it.
        if r.status_code != 200:
            raise HTTPException(status_code=502, detail=f"Finnhub error {r.status_code}")
        data = r.json()
        if isinstance(data, dict) and data.get("error"):
            # Finnhub explicit error
            raise HTTPException(status_code=502, detail=data["error"])
        return data


def _normalize_quote(symbol: str, q: Dict[str, Any]) -> Dict[str, Any]:
    # Finnhub quote fields: c (current), pc (prev close), h, l, o, t (ts)
    c = q.get("c")
    pc = q.get("pc")
    h = q.get("h")
    l = q.get("l")
    o = q.get("o")

    # If symbol invalid, Finnhub often returns zeros/None
    if c in (None, 0) and pc in (None, 0) and h in (None, 0) and l in (None, 0) and o in (None, 0):
        raise HTTPException(status_code=404, detail="Symbol not found")

    pct = None
    try:
        if c is not None and pc not in (None, 0):
            pct = ((float(c) - float(pc)) / float(pc)) * 100.0
    except Exception:
        pct = None

    return {
        "symbol": symbol.upper(),
        "current": c,
        "prev_close": pc,
        "high": h,
        "low": l,
        "open": o,
        "percent_change": None if pct is None or math.isnan(pct) else round(pct, 3),
        "volume": None,  # Finnhub's quote endpoint doesn't include volume; keep field for UI
        "raw": {"c": c, "pc": pc, "h": h, "l": l, "o": o},
    }


@app.get("/quote/{symbol}")
async def quote(symbol: str, pretty: int = Query(default=0, ge=0, le=1)):
    key = f"quote:{symbol.upper()}"
    cached = _get_cache(key)
    if cached:
        return _pretty(cached, bool(pretty))

    data = await _finnhub_get("/quote", {"symbol": symbol.upper()})
    payload = _normalize_quote(symbol, data)
    _set_cache(key, payload)
    return _pretty(payload, bool(pretty))


@app.get("/summary/{symbol}")
async def summary(symbol: str, pretty: int = Query(default=0, ge=0, le=1)):
    key = f"summary:{symbol.upper()}"
    cached = _get_cache(key)
    if cached:
        return _pretty(cached, bool(pretty))

    q = await _finnhub_get("/quote", {"symbol": symbol.upper()})
    normalized = _normalize_quote(symbol, q)

    # Build a small human summary using the quote
    cur = normalized["current"]
    pc = normalized["prev_close"]
    h = normalized["high"]
    l = normalized["low"]
    pct = normalized["percent_change"]

    # Defensive formatting
    def fmt(x):
        return "-" if x in (None, 0) else f"{x:.3f}"

    pct_str = "-" if pct is None else f"{pct:.3f}%"
    text = (
        f"{symbol.upper()}: {fmt(cur)} "
        f"({pct_str} vs prev close). "
        f"Session range: {fmt(l)}–{fmt(h)}. "
        f"Prev close: {fmt(pc)}."
    )

    payload = {
        "symbol": symbol.upper(),
        "summary": text,
        "quote": normalized,
    }
    _set_cache(key, payload)
    return _pretty(payload, bool(pretty))






































