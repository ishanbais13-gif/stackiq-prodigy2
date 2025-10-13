# app.py
import os, time, logging, requests
from typing import Dict, Any, List
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse, HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("stackiq")

FINNHUB_BASE = "https://finnhub.io/api/v1"
AV_BASE = "https://www.alphavantage.co/query"
FINNHUB_API_KEY = (os.getenv("FINNHUB_API_KEY") or "").strip()
ALPHAVANTAGE_KEY = (os.getenv("ALPHAVANTAGE_KEY") or "").strip()

VERSION = "0.2.2"

app = FastAPI(title="StackIQ API", version=VERSION)

def _alpha_raise_if_note(data: Dict[str, Any]) -> None:
    note = data.get("Note") or data.get("Information") or data.get("Error Message")
    if note:
        raise HTTPException(status_code=429, detail=f"Alpha Vantage: {note}")

def _get_json(url: str, params: Dict[str, Any], timeout: int = 15) -> Dict[str, Any]:
    r = requests.get(url, params=params, timeout=timeout)
    if r.status_code == 403 and "finnhub" in url:
        raise HTTPException(
            status_code=403,
            detail="Finnhub 403 (forbidden). Your key/plan may not allow this endpoint or range.",
        )
    r.raise_for_status()
    return r.json()

def _finnhub_quote(symbol: str) -> Dict[str, Any]:
    if not FINNHUB_API_KEY:
        raise HTTPException(status_code=502, detail="Missing FINNHUB_API_KEY")
    q = _get_json(
        f"{FINNHUB_BASE}/quote",
        {"symbol": symbol.upper(), "token": FINNHUB_API_KEY},
    )
    if not q or "c" not in q:
        raise HTTPException(status_code=502, detail="Unexpected Finnhub quote payload")
    return {
        "symbol": symbol.upper(),
        "current": q.get("c") or 0.0,
        "change": q.get("d"),
        "percent": q.get("dp"),
        "high": q.get("h"),
        "low": q.get("l"),
        "open": q.get("o"),
        "prev_close": q.get("pc"),
        "timestamp": q.get("t") or 0,
    }

def _alpha_daily(symbol: str) -> Dict[str, Any]:
    if not ALPHAVANTAGE_KEY:
        raise HTTPException(status_code=502, detail="Missing ALPHAVANTAGE_KEY")
    data = _get_json(
        AV_BASE,
        {
            "function": "TIME_SERIES_DAILY",
            "symbol": symbol.upper(),
            "apikey": ALPHAVANTAGE_KEY,
            "outputsize": "compact",
        },
    )
    _alpha_raise_if_note(data)
    return data

@app.get("/health")
def health():
    return {"status": "ok", "has_token": bool(FINNHUB_API_KEY), "service": "StackIQ", "version": VERSION}

@app.get("/quote/{symbol}")
def quote(symbol: str):
    return _finnhub_quote(symbol)

@app.get("/predict/{symbol}")
def predict(symbol: str, budget: float = Query(..., gt=0, le=1_000_000)):
    try:
        av = _alpha_daily(symbol)
        c = av.get("Time Series (Daily)")
        if not c:
            raise ValueError("no daily series returned")
        bars: List[Dict[str, Any]] = [
            {"t": k, "close": float(v["4. close"])} for k, v in sorted(c.items())
        ]
        closes = [bar["close"] for bar in bars][-10:]
        if len(closes) < 2:
            raise ValueError("not enough data")
        momentum = (closes[-1] - closes[0]) / closes[0] * 100.0
        price_now = closes[-1]
        shares = int(budget / price_now)
        return {
            "symbol": symbol.upper(),
            "price_now": round(price_now, 2),
            "momentum_pct": round(momentum, 4),
            "using": "alpha_candles",
            "buy_plan": {
                "budget": budget,
                "shares": shares,
                "estimated_cost": round(shares * price_now, 2),
            },
            "note": "Educational sample strategy; not financial advice.",
        }
    except HTTPException as e:
        if e.status_code == 429:
            q = _finnhub_quote(symbol)
            price_now = q["current"] or 0.0
            shares = int(budget / price_now) if price_now else 0
            return {
                "symbol": symbol.upper(),
                "price_now": round(price_now or 0.0, 2),
                "momentum_pct": None,
                "using": "quote_only_fallback",
                "plan_hint": str(e.detail),
                "buy_plan": {
                    "budget": budget,
                    "shares": shares,
                    "estimated_cost": round(shares * (price_now or 0.0), 2),
                },
                "note": "Educational sample strategy; not financial advice.",
            }
        raise

# ---- static + index route ----
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/", response_class=HTMLResponse)
def root():
    return FileResponse("static/index.html")






























































