import os
import time
import datetime as dt
from typing import Literal, Optional, Dict, Any

import requests
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles

APP_NAME = "stackiq-web"
APP_VERSION = "1.0.0"

FINNHUB_KEY = os.getenv("FINNHUB_API_KEY", "").strip()

app = FastAPI(title=APP_NAME, version=APP_VERSION)

# Serve ./web as a tiny frontend
if os.path.isdir("web"):
    app.mount("/web", StaticFiles(directory="web", html=True), name="web")


# ---------- helpers ----------
def _finnhub_get(url: str, params: Dict[str, Any]) -> Dict[str, Any]:
    if not FINNHUB_KEY:
        raise HTTPException(status_code=500, detail="FINNHUB_API_KEY not set")
    p = dict(params or {})
    p["token"] = FINNHUB_KEY
    r = requests.get(url, params=p, timeout=12)
    if r.status_code != 200:
        raise HTTPException(status_code=502, detail=f"Finnhub error {r.status_code}")
    data = r.json()
    return data


def _quote(symbol: str) -> Dict[str, Any]:
    """Return normalized quote payload."""
    q = _finnhub_get("https://finnhub.io/api/v1/quote", {"symbol": symbol})
    # Finnhub quote keys: c(cur), d(change), dp(%), h, l, o, pc, t
    if "c" not in q:
        raise HTTPException(status_code=502, detail="Invalid quote response")
    return {
        "symbol": symbol.upper(),
        "current": q.get("c"),
        "prev_close": q.get("pc"),
        "high": q.get("h"),
        "low": q.get("l"),
        "open": q.get("o"),
        "percent_change": q.get("dp"),
        "volume": None,  # not supplied by this endpoint
        "raw": q,
    }


# ---------- health / meta ----------
@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/version")
def version():
    return {"app": APP_NAME, "version": APP_VERSION}


# ---------- core endpoints you already use ----------
@app.get("/quote/{symbol}")
def quote(symbol: str):
    return _quote(symbol)

@app.get("/summary/{symbol}")
def summary(symbol: str):
    q = _quote(symbol)
    # small human string
    change = q["percent_change"]
    updown = "up" if (change or 0) >= 0 else "down"
    summary_txt = (
        f"{q['symbol']}: {q['current']} ({updown} {abs(change or 0):.2f}% on the day). "
        f"Session range: {q['low']}–{q['high']}. Prev close: {q['prev_close']}."
    )
    return {"symbol": q["symbol"], "summary": summary_txt, "quote": q}


# ---------- NEW: price history ----------
_RANGE_TO_SECONDS = {
    "1mo": 30 * 24 * 3600,
    "3mo": 90 * 24 * 3600,
    "6mo": 180 * 24 * 3600,
    "1y": 365 * 24 * 3600,
}

def _resolution_for_range(r: str) -> str:
    # Keep it simple and readable on the chart
    if r == "1mo":
        return "60"     # 60-minute candles
    if r == "3mo":
        return "D"      # daily
    if r == "6mo":
        return "D"
    if r == "1y":
        return "D"
    return "D"

@app.get("/history/{symbol}")
def history(
    symbol: str,
    range: Literal["1mo", "3mo", "6mo", "1y"] = "1y"
):
    now = int(time.time())
    frm = now - _RANGE_TO_SECONDS[range]
    res = _resolution_for_range(range)

    data = _finnhub_get(
        "https://finnhub.io/api/v1/stock/candle",
        {"symbol": symbol, "resolution": res, "from": frm, "to": now},
    )
    # Finnhub returns {s: "ok"|"no_data", t:[...], c:[...], h,l,o,v}
    if data.get("s") != "ok" or not data.get("t"):
        raise HTTPException(status_code=404, detail="No history data")

    # Normalize to ms + close prices
    points = [{"t": int(t) * 1000, "c": float(c)} for t, c in zip(data["t"], data["c"])]
    return {
        "symbol": symbol.upper(),
        "range": range,
        "resolution": res,
        "points": points,
        "count": len(points),
    }


# ---------- root convenience ----------
@app.get("/", response_class=HTMLResponse)
def root():
    if os.path.isdir("web"):
        # Redirect hint
        return HTMLResponse('<meta http-equiv="refresh" content="0; url=/web/">')
    return HTMLResponse("<h1>stackiq-web</h1><p>Add a /web folder to serve the UI.</p>")
























