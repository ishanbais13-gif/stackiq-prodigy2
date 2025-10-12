# app.py  (free-plan friendly)
import os, time, logging, requests
from typing import Dict, Any, List
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("stackiq")

FINNHUB_BASE = "https://finnhub.io/api/v1"
AV_BASE = "https://www.alphavantage.co/query"
FINNHUB_API_KEY = (os.getenv("FINNHUB_API_KEY") or "").strip()
ALPHAVANTAGE_KEY = (os.getenv("ALPHAVANTAGE_KEY") or "").strip()

app = FastAPI(title="StackIQ API", version="0.2.1")

# ---------- helpers

def _raise_if_av_note(data: Dict[str, Any]):
    # AlphaVantage returns these when rate limited / wrong fn / premium
    note = data.get("Note") or data.get("Information") or data.get("Error Message")
    if note:
        raise HTTPException(status_code=429, detail=f"Alpha Vantage: {note}")

def _get_json(url: str, params: Dict[str, Any], timeout=15) -> Dict[str, Any]:
    r = requests.get(url, params=params, timeout=timeout)
    if r.status_code == 403:
        # be explicit; this is what you were seeing previously
        raise HTTPException(
            status_code=403,
            detail="Finnhub 403 (forbidden). Your key/plan may not allow this endpoint or range.",
        )
    r.raise_for_status()
    return r.json()

# ---------- providers

def get_quote(symbol: str) -> Dict[str, Any]:
    """Current price from Finnhub; falls back to last daily close if needed."""
    if not FINNHUB_API_KEY:
        raise HTTPException(502, detail="Missing FINNHUB_API_KEY")

    try:
        q = _get_json(
            f"{FINNHUB_BASE}/quote",
            {"symbol": symbol.upper(), "token": FINNHUB_API_KEY},
        )
        if not {"c", "d", "dp"} <= q.keys():
            raise HTTPException(502, detail="Unexpected Finnhub quote payload")
        return {
            "symbol": symbol.upper(),
            "current": q["c"],
            "change": q["d"],
            "percent": q["dp"],
            "high": q.get("h"),
            "low": q.get("l"),
            "open": q.get("o"),
            "prev_close": q.get("pc"),
            "timestamp": int(time.time()),
        }
    except HTTPException:
        raise
    except Exception as e:
        log.warning("quote fallback via AV daily: %s", e)
        # fallback: last close from AV daily if Finnhub unavailable
        daily = get_daily_closes(symbol, limit=2)
        last = daily[-1]["c"]
        prev = daily[-2]["c"] if len(daily) > 1 else last
        return {
            "symbol": symbol.upper(),
            "current": last,
            "change": last - prev,
            "percent": ((last / prev) - 1) * 100 if prev else 0.0,
            "high": last,
            "low": last,
            "open": last,
            "prev_close": prev,
            "timestamp": int(time.time()),
        }

def get_daily_closes(symbol: str, limit: int = 60) -> List[Dict[str, Any]]:
    """Free-plan daily candles via TIME_SERIES_DAILY_ADJUSTED."""
    if not ALPHAVANTAGE_KEY:
        raise HTTPException(502, detail="Missing ALPHAVANTAGE_KEY")

    data = _get_json(
        AV_BASE,
        {
            "function": "TIME_SERIES_DAILY_ADJUSTED",
            "symbol": symbol.upper(),
            "apikey": ALPHAVANTAGE_KEY,
            "outputsize": "compact",  # last ~100 days (free)
        },
    )
    _raise_if_av_note(data)

    ts = data.get("Time Series (Daily)")
    if not isinstance(ts, dict):
        raise HTTPException(502, detail="Unexpected AV daily payload")

    # newest -> oldest in the dict; sort to oldest -> newest
    days = sorted(ts.items(), key=lambda kv: kv[0])
    # map to OHLCV candles (t in seconds)
    candles = []
    for date_str, row in days[-limit:]:
        # row keys: '1. open', '2. high', '3. low', '4. close', '6. volume'
        candles.append(
            {
                "t": int(time.mktime(time.strptime(date_str, "%Y-%m-%d"))),
                "o": float(row["1. open"]),
                "h": float(row["2. high"]),
                "l": float(row["3. low"]),
                "c": float(row["4. close"]),
                "v": float(row.get("6. volume", 0)),
            }
        )
    return candles

# ---------- routes

@app.get("/")
def root():
    return {"service": "StackIQ", "status": "ok"}

@app.get("/health")
def health():
    return {"status": "ok", "has_token": bool(FINNHUB_API_KEY or ALPHAVANTAGE_KEY), "service": "StackIQ", "version": "0.2.1"}

@app.get("/quote/{symbol}")
def quote(symbol: str):
    return get_quote(symbol)

@app.get("/candles/{symbol}")
def candles(symbol: str, limit: int = Query(60, ge=10, le=100)):
    """Return last N daily candles (free-plan safe)."""
    return {"symbol": symbol.upper(), "granularity": "1d", "candles": get_daily_closes(symbol, limit)}

@app.get("/predict/{symbol}")
def predict(symbol: str, budget: float = Query(1000.0, ge=50.0, le=1_000_000.0)):
    """
    Simple educational strategy using free data:
      - momentum over last 20 trading days from daily closes
      - buy shares with given budget at current quote price
    """
    # daily momentum (free)
    candles = get_daily_closes(symbol, limit=21)  # 21 daily bars ~ 1 trading month
    closes = [c["c"] for c in candles]
    if len(closes) < 2:
        raise HTTPException(502, detail="Not enough daily data")

    last = closes[-1]
    prev = closes[0]
    momentum_pct = ((last / prev) - 1) * 100 if prev else 0.0

    # live-ish price (Finnhub) with fallback already inside
    q = get_quote(symbol)
    price = q["current"] or last

    shares = max(int(budget // price), 0)
    est_cost = round(shares * price, 2)

    return {
        "symbol": symbol.upper(),
        "price_now": round(price, 2),
        "momentum_pct": round(momentum_pct, 4),
        "using": "daily_free_candles+quote",
        "plan_hint": "All endpoints used here are free: Finnhub quote + Alpha Vantage TIME_SERIES_DAILY_ADJUSTED.",
        "buy_plan": {"budget": float(budget), "shares": shares, "estimated_cost": est_cost},
        "note": "Educational sample strategy; not financial advice.",
    }





























































