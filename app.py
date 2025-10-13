import os, time, logging, math, requests
from typing import Dict, Any, List, Tuple
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

# ---------------------------
# Config & Keys
# ---------------------------
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("stackiq")

VERSION = "0.3.0"

FINNHUB_BASE = "https://finnhub.io/api/v1"
AV_BASE      = "https://www.alphavantage.co/query"

FINNHUB_API_KEY     = (os.getenv("FINNHUB_API_KEY") or "").strip()
ALPHAVANTAGE_KEY    = (os.getenv("ALPHAVANTAGE_KEY") or "").strip()

# TTLs suited for free plans
QUOTE_TTL_SEC   = 60          # cache quotes for 1 minute
CANDLES_TTL_SEC = 20 * 60     # cache daily candles for 20 minutes

# ---------------------------
# Tiny in-memory cache
# ---------------------------
_cache: Dict[str, Tuple[float, Any]] = {}  # key -> (expiry_epoch, data)

def cache_get(key: str):
    item = _cache.get(key)
    if not item: 
        return None
    exp, data = item
    if time.time() > exp:
        _cache.pop(key, None)
        return None
    return data

def cache_set(key: str, data: Any, ttl: int):
    _cache[key] = (time.time() + ttl, data)

# ---------------------------
# Helpers
# ---------------------------
def _alpha_raise_if_note(data: Dict[str, Any]) -> None:
    # Alpha Vantage returns "Note" / "Information" / "Error Message" on limits/premium
    note = data.get("Note") or data.get("Information") or data.get("Error Message")
    if note:
        # 429 so callers can detect quota/premium quickly
        raise HTTPException(status_code=429, detail=f"Alpha Vantage: {note}")

def _get_json(url: str, params: Dict[str, Any], timeout: int = 15) -> Dict[str, Any]:
    r = requests.get(url, params=params, timeout=timeout)
    # Clean message when Finnhub 403s on free candle endpoints
    if r.status_code == 403 and "finnhub" in url:
        raise HTTPException(
            status_code=403,
            detail="Finnhub 403 (forbidden). Your key/plan may not allow this endpoint or range.",
        )
    r.raise_for_status()
    return r.json()

def _alpha_daily(symbol: str) -> Dict[str, Any]:
    """Fetch daily candles from Alpha Vantage (compact ~100 bars), with caching."""
    if not ALPHAVANTAGE_KEY:
        raise HTTPException(status_code=502, detail="Missing ALPHAVANTAGE_KEY")

    key = f"av_daily:{symbol.upper()}"
    cached = cache_get(key)
    if cached:
        return cached

    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol.upper(),
        "apikey": ALPHAVANTAGE_KEY,
        "outputsize": "compact",
    }
    data = _get_json(AV_BASE, params)
    _alpha_raise_if_note(data)

    series = data.get("Time Series (Daily)") or {}
    if not isinstance(series, dict) or not series:
        raise HTTPException(status_code=502, detail="Unexpected Alpha Vantage payload")

    # Convert to ascending list of bars: [{t, open, high, low, close, volume}]
    bars: List[Dict[str, Any]] = []
    for day, row in series.items():
        try:
            bars.append({
                "t": day,
                "open":  float(row["1. open"]),
                "high":  float(row["2. high"]),
                "low":   float(row["3. low"]),
                "close": float(row["4. close"]),
                "volume": float(row["5. volume"]),
            })
        except Exception:
            continue

    bars.sort(key=lambda b: b["t"])
    payload = {"symbol": symbol.upper(), "bars": bars, "source": "alphavantage", "version": VERSION}
    cache_set(key, payload, CANDLES_TTL_SEC)
    return payload

def _finnhub_quote(symbol: str) -> Dict[str, Any]:
    """Fetch a real-time-ish quote from Finnhub, with caching."""
    if not FINNHUB_API_KEY:
        raise HTTPException(status_code=502, detail="Missing FINNHUB_API_KEY")

    key = f"fh_quote:{symbol.upper()}"
    cached = cache_get(key)
    if cached:
        return cached

    data = _get_json(f"{FINNHUB_BASE}/quote", {"symbol": symbol.upper(), "token": FINNHUB_API_KEY})
    # Finnhub quote payload keys: c=current, d=change, dp=percent, h=high, l=low, o=open, pc=prev close, t=epoch
    for k in ("c", "h", "l", "o", "pc"):
        if k not in data:
            raise HTTPException(status_code=502, detail="Unexpected Finnhub quote payload")

    out = {
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
    cache_set(key, out, QUOTE_TTL_SEC)
    return out

def _momentum_pct(bars: List[Dict[str, Any]], lookback: int = 20) -> float:
    """Simple momentum: (close[-1] - close[-lookback]) / close[-lookback] * 100."""
    if len(bars) < lookback + 1:
        return 0.0
    last = bars[-1]["close"]
    prev = bars[-1 - lookback]["close"]
    if prev == 0:
        return 0.0
    return round((last - prev) / prev * 100.0, 4)

# ---------------------------
# FastAPI
# ---------------------------
app = FastAPI(title="StackIQ API", version=VERSION)

@app.get("/")
def root():
    return {"service": "StackIQ", "status": "ok"}

@app.get("/health")
def health():
    return {
        "status": "ok",
        "has_token": bool(ALPHAVANTAGE_KEY or FINNHUB_API_KEY),
        "service": "StackIQ",
        "version": VERSION,
    }

@app.get("/quote/{symbol}")
def quote(symbol: str):
    return _finnhub_quote(symbol)

@app.get("/candles/{symbol}")
def candles(symbol: str):
    """Daily bars from Alpha Vantage (compact); good for basic charts."""
    return _alpha_daily(symbol)

@app.get("/predict/{symbol}")
def predict(symbol: str, budget: float = 1000.0):
    """
    Sample strategy:
    - price from Finnhub quote (fast)
    - momentum from Alpha daily candles (free)
    """
    q = _finnhub_quote(symbol)
    av = _alpha_daily(symbol)

    price_now = float(q["current"] or 0.0)
    bars = av["bars"]
    mom = _momentum_pct(bars, lookback=20)

    shares = int(math.floor(budget / price_now)) if price_now > 0 else 0
    est_cost = round(shares * price_now, 2)

    return JSONResponse(
        {
            "symbol": symbol.upper(),
            "price_now": price_now,
            "momentum_pct": mom,
            "using": "alpha_candles",
            "buy_plan": {"budget": budget, "shares": shares, "estimated_cost": est_cost},
            "note": "Educational sample strategy; not financial advice.",
            "version": VERSION,
        }
    )






























































