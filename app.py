import os, time, math, logging, requests
from typing import Dict, Any, List
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

# -------- Logging --------
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("stackiq")

# -------- Config --------
FINNHUB_BASE = "https://finnhub.io/api/v1"
API_KEY = os.getenv("FINNHUB_API_KEY", "").strip()

app = FastAPI(title="StackIQ API", version="0.2.0")

# -------- Errors --------
class FinnhubError(Exception):
    pass

# -------- Finnhub helpers --------
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
    data = _get(f"{FINNHUB_BASE}/quote", {"symbol": symbol.upper()})
    if not isinstance(data, dict) or "c" not in data:
        raise FinnhubError("Unexpected response for quote.")
    return {
        "symbol": symbol.upper(),
        "current": float(data.get("c") or 0),
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
    data = _get(f"{FINNHUB_BASE}/stock/candle", {
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

    t, o, h, l, c, v = (data.get(k, []) for k in ("t","o","h","l","c","v"))
    candles: List[Dict[str, Any]] = []
    for i in range(min(len(t), len(o), len(h), len(l), len(c), len(v))):
        candles.append({"t": t[i], "o": o[i], "h": h[i], "l": l[i], "c": c[i], "v": v[i]})
    return {"symbol": symbol.upper(), "resolution": resolution, "candles": candles}

# -------- Simple strategy for /predict --------
def simple_predict(symbol: str, budget: float) -> Dict[str, Any]:
    quote = get_quote(symbol)
    price = quote["current"]
    if not price or price <= 0:
        raise FinnhubError("No valid current price for prediction.")

    # Pull last 10 daily candles to compute a micro trend
    candles = get_candles(symbol, days=15, resolution="D")["candles"]
    last_closes = [bar["c"] for bar in candles[-10:]] if candles else []
    momentum = None
    if len(last_closes) >= 2:
        momentum = (last_closes[-1] - last_closes[0]) / last_closes[0] * 100.0

    # Position sizing
    shares = math.floor(budget / price) if budget and budget > 0 else 0
    cost = round(shares * price, 2)

    # Basic risk/target: -3% stop, +5% target
    stop = round(price * 0.97, 2)
    target = round(price * 1.05, 2)
    est_profit = round(shares * (target - price), 2)
    max_loss = round(shares * (price - stop), 2)

    # Very light signal: bias from momentum
    signal = "hold"
    if momentum is not None:
        if momentum > 1.0:
            signal = "buy"
        elif momentum < -1.0:
            signal = "avoid"

    return {
        "symbol": symbol.upper(),
        "now_price": price,
        "momentum_10d_pct": round(momentum, 2) if momentum is not None else None,
        "signal": signal,
        "sizing": {"budget": budget, "shares": shares, "estimated_cost": cost},
        "risk": {"stop_loss": stop, "max_drawdown_if_stopped": max_loss},
        "target": {"take_profit": target, "est_profit_at_target": est_profit},
        "note": "Educational sample strategy; not financial advice."
    }

# -------- Routes --------
@app.get("/")
def root(): return {"service": "StackIQ", "status": "ok"}

@app.get("/health")
def health():
    return {"status": "ok", "has_token": bool(API_KEY), "service": "StackIQ", "version": "0.2.0"}

@app.get("/quote/{symbol}")
def quote(symbol: str):
    try: return JSONResponse(content=get_quote(symbol))
    except FinnhubError as e: raise HTTPException(status_code=502, detail=str(e))
    except Exception as e: raise HTTPException(status_code=500, detail=f"Unexpected error: {e}")

@app.get("/candles/{symbol}")
def candles(
    symbol: str,
    days: int = Query(30, ge=1, le=365),
    resolution: str = Query("D", pattern="^(1|5|15|30|60|D|W|M)$")
):
    try: return JSONResponse(content=get_candles(symbol, days=days, resolution=resolution))
    except FinnhubError as e: raise HTTPException(status_code=502, detail=str(e))
    except Exception as e: raise HTTPException(status_code=500, detail=f"Unexpected error: {e}")

@app.get("/predict/{symbol}")
def predict(symbol: str, budget: float = Query(..., gt=0)):
    try: return JSONResponse(content=simple_predict(symbol, budget))
    except FinnhubError as e: raise HTTPException(status_code=502, detail=str(e))
    except Exception as e: raise HTTPException(status_code=500, detail=f"Unexpected error: {e}")


























































