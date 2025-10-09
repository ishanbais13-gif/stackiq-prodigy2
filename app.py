import os
import time
import logging
from typing import Dict, Any, List
import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

# -------- Logging (helps in Azure Log Stream) --------
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("stackiq")

# -------- Config --------
FINNHUB_BASE = "https://finnhub.io/api/v1"
API_KEY = os.getenv("FINNHUB_API_KEY", "").strip()

app = FastAPI(title="StackIQ API", version="0.1.0")


# -------- Helpers --------
class FinnhubError(Exception):
    pass


def _require_api_key():
    if not API_KEY:
        raise FinnhubError("Missing FINNHUB_API_KEY in environment.")


def _get(url: str, params: Dict[str, Any]) -> Dict[str, Any]:
    _require_api_key()
    p = dict(params or {})
    p["token"] = API_KEY
    try:
        r = requests.get(url, params=p, timeout=15)
        r.raise_for_status()
        data = r.json()
        # Finnhub sometimes returns {'error': '...'} or s='no_data'
        if isinstance(data, dict) and data.get("error"):
            raise FinnhubError(data["error"])
        return data
    except requests.HTTPError as e:
        log.exception("HTTP error calling Finnhub")
        raise FinnhubError(f"Finnhub HTTP error: {e}") from e
    except Exception as e:
        log.exception("Unexpected error calling Finnhub")
        raise FinnhubError(f"Finnhub request failed: {e}") from e


def get_quote(symbol: str) -> Dict[str, Any]:
    url = f"{FINNHUB_BASE}/quote"
    data = _get(url, {"symbol": symbol.upper()})
    # Expected keys: c, d, dp, h, l, o, pc, t
    if not isinstance(data, dict) or "c" not in data:
        raise FinnhubError("Unexpected response for quote.")
    return {
        "symbol": symbol.upper(),
        "current": data.get("c"),
        "change": data.get("d"),
        "percent": data.get("dp"),
        "high": data.get("h"),
        "low": data.get("l"),
        "open": data.get("o"),
        "prev_close": data.get("pc"),
        "timestamp": data.get("t"),
    }


def get_candles(symbol: str, days: int = 30, resolution: str = "D") -> Dict[str, Any]:
    now = int(time.time())
    frm = now - days * 86400
    url = f"{FINNHUB_BASE}/stock/candle"
    data = _get(url, {
        "symbol": symbol.upper(),
        "resolution": resolution,
        "from": frm,
        "to": now
    })

    if not isinstance(data, dict):
        raise FinnhubError("Candle response not a dict")

    if data.get("s") == "no_data":
        return {"symbol": symbol.upper(), "resolution": resolution, "candles": []}
    if data.get("s") != "ok":
        raise FinnhubError(f"Candle response status: {data.get('s')}")

    t = data.get("t", [])
    o = data.get("o", [])
    h = data.get("h", [])
    l = data.get("l", [])
    c = data.get("c", [])
    v = data.get("v", [])

    candles: List[Dict[str, Any]] = []
    for i in range(min(len(t), len(o), len(h), len(l), len(c), len(v))):
        candles.append({"t": t[i], "o": o[i], "h": h[i], "l": l[i], "c": c[i], "v": v[i]})

    return {"symbol": symbol.upper(), "resolution": resolution, "candles": candles}


# -------- Routes --------
@app.get("/")
def root():
    return {"service": "StackIQ", "status": "ok"}

@app.get("/health")
def health():
    return {
        "status": "ok",
        "has_token": bool(API_KEY),
        "service": "StackIQ",
        "version": "0.1.0"
    }

@app.get("/quote/{symbol}")
def quote(symbol: str):
    try:
        return JSONResponse(content=get_quote(symbol))
    except FinnhubError as e:
        raise HTTPException(status_code=502, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {e}")

@app.get("/candles/{symbol}")
def candles(
    symbol: str,
    days: int = Query(30, ge=1, le=365),
    resolution: str = Query("D", pattern="^(1|5|15|30|60|D|W|M)$")
):
    try:
        return JSONResponse(content=get_candles(symbol, days=days, resolution=resolution))
    except FinnhubError as e:
        raise HTTPException(status_code=502, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {e}")

























































