# app.py  — StackIQ API (free-plan friendly)
import os, time, math, logging, requests
from typing import Dict, Any, List, Optional
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("stackiq")

# ---- Config ----
FINNHUB_BASE = "https://finnhub.io/api/v1"
AV_BASE       = "https://www.alphavantage.co/query"
FINNHUB_API_KEY     = os.getenv("FINNHUB_API_KEY","").strip()
ALPHAVANTAGE_KEY    = os.getenv("ALPHAVANTAGE_KEY","").strip()

app = FastAPI(title="StackIQ API", version="0.2.1")

# ---- Helpers ----
class UpstreamError(Exception): ...
def _rget(url: str, params: Dict[str, Any], timeout=15) -> Dict[str, Any]:
    r = requests.get(url, params=params, timeout=timeout)
    if r.status_code == 403 and "finnhub" in url:
        # Clear explanation for free Finnhub key limits
        raise HTTPException(
            status_code=403,
            detail="Finnhub 403 (forbidden). Your key/plan may not allow this endpoint or range."
        )
    r.raise_for_status()
    try:
        return r.json()
    except Exception as e:
        raise UpstreamError(f"Non-JSON response from {url}: {e}")

def _require_env(var: str):
    val = os.getenv(var, "").strip()
    if not val:
        raise HTTPException(status_code=502, detail=f"Missing {var} in environment.")
    return val

def _ts(): return int(time.time())

# ---- Endpoints ----
@app.get("/")
def root():
    return {"service":"StackIQ", "status":"ok"}

@app.get("/health")
def health():
    return {
        "status":"ok",
        "has_token": bool(ALPHAVANTAGE_KEY or FINNHUB_API_KEY),
        "service":"StackIQ",
        "version": app.version
    }

# -------- Quotes (Finnhub) --------
@app.get("/quote/{symbol}")
def quote(symbol: str):
    """Live-ish quote via Finnhub (works on free plan for /quote).
       If you remove FINNHUB_API_KEY, this will 502 with a helpful message.
    """
    if not FINNHUB_API_KEY:
        raise HTTPException(status_code=502, detail="Missing FINNHUB_API_KEY")
    data = _rget(f"{FINNHUB_BASE}/quote", {"symbol": symbol.upper(), "token": FINNHUB_API_KEY})
    # Finnhub returns keys: c(current), d(change), dp(percent), h,l,o,pc,t
    needed = {"c","d","dp","h","l","o","pc","t"}
    if not needed.issubset(data.keys()):
        raise HTTPException(status_code=502, detail="Unexpected quote payload")
    return {
        "symbol": symbol.upper(),
        "current": data["c"],
        "change": data["d"],
        "percent": data["dp"],
        "high": data["h"],
        "low": data["l"],
        "open": data["o"],
        "prev_close": data["pc"],
        "timestamp": data["t"],
    }

# -------- Candles (Alpha Vantage Daily — FREE) --------
@app.get("/candles/{symbol}")
def candles(
    symbol: str,
    outputsize: str = Query("compact", regex="^(compact|full)$")  # compact=~100 days (free), full=~20y (heavier)
):
    """Daily candles via Alpha Vantage (free plan).
       Returns arrays t,o,h,l,c,v (latest first).
    """
    key = _require_env("ALPHAVANTAGE_KEY")
    params = {
        "function": "TIME_SERIES_DAILY_ADJUSTED",
        "symbol": symbol.upper(),
        "outputsize": outputsize,
        "apikey": key,
        "datatype": "json",
    }
    data = _rget(AV_BASE, params)
    # AV happy path
    series = data.get("Time Series (Daily)") or data.get("Time Series (Daily Adjusted)")
    note   = data.get("Note") or data.get("Information")  # throttling or premium/info notes
    if not series:
        # If AV returns a message instead of data, surface it but still 200
        return JSONResponse(
            {
                "symbol": symbol.upper(),
                "t": [], "o": [], "h": [], "l": [], "c": [], "v": [],
                "plan_hint": note or "Alpha Vantage did not return daily series (rate limit or key issue).",
            },
            status_code=200
        )

    # series is { "YYYY-MM-DD": {...} }. Sort newest->oldest for UI convenience
    rows = sorted(series.items(), key=lambda kv: kv[0], reverse=True)
    t,o,h,l,c,v = [],[],[],[],[],[]
    for day, row in rows:
        # AV keys: '1. open','2. high','3. low','4. close','6. volume'
        try:
            t.append(day)
            o.append(float(row["1. open"]))
            h.append(float(row["2. high"]))
            l.append(float(row["3. low"]))
            c.append(float(row["4. close"]))
            v.append(float(row.get("6. volume", row.get("5. volume", 0.0))))
        except Exception:
            # skip malformed row
            continue

    return {"symbol": symbol.upper(), "t": t, "o": o, "h": h, "l": l, "c": c, "v": v, "plan_hint": note or ""}

# -------- Simple Momentum “Predict” --------
@app.get("/predict/{symbol}")
def predict(symbol: str, budget: float = 1000.0):
    """Toy signal:
       - Try Alpha Vantage daily candles (free).
       - If unavailable (rate limited), fall back to Finnhub quote only.
       - momentum_pct = (last_close - 20d SMA) / 20d SMA * 100  (if candles available)
    """
    sym = symbol.upper()
    price_now: Optional[float] = None
    momentum_pct: Optional[float] = None
    using = ""
    plan_hint = ""

    # 1) Try AV daily candles first
    try:
        key = _require_env("ALPHAVANTAGE_KEY")
        params = {
            "function": "TIME_SERIES_DAILY_ADJUSTED",
            "symbol": sym,
            "outputsize": "compact",
            "apikey": key,
            "datatype": "json",
        }
        data = _rget(AV_BASE, params)
        series = data.get("Time Series (Daily)") or data.get("Time Series (Daily Adjusted)")
        note   = data.get("Note") or data.get("Information")
        if series:
            rows = sorted(series.items(), key=lambda kv: kv[0], reverse=True)
            closes = []
            for _d, row in rows:
                try:
                    closes.append(float(row["4. close"]))
                except Exception:
                    continue
            if closes:
                price_now = closes[0]
                # 20-day SMA if we have enough, else SMA over available
                n = min(20, len(closes))
                sma = sum(closes[:n]) / n
                if sma > 0:
                    momentum_pct = (price_now - sma) / sma * 100.0
            using = "alpha_vantage_daily"
            plan_hint = note or ""
        else:
            using = "quote_only_fallback"
            plan_hint = note or "Alpha Vantage did not return candles (rate limit or key issue)."
    except HTTPException as he:
        # Probably missing key
        using = "quote_only_fallback"
        plan_hint = str(he.detail)
    except Exception as e:
        using = "quote_only_fallback"
        plan_hint = f"AV error: {e}"

    # 2) If we still need price, get Finnhub quote (works on free for /quote)
    if price_now is None:
        if FINNHUB_API_KEY:
            try:
                q = _rget(f"{FINNHUB_BASE}/quote", {"symbol": sym, "token": FINNHUB_API_KEY})
                price_now = float(q.get("c") or 0.0) or None
            except Exception as e:
                plan_hint = f"{plan_hint} | Finnhub quote error: {e}"
        else:
            plan_hint = f"{plan_hint} | Missing FINNHUB_API_KEY for live quote."

    if price_now is None:
        raise HTTPException(status_code=502, detail="Could not obtain a price from either source.")

    shares = int(math.floor(budget / price_now))
    est_cost = round(shares * price_now, 2)

    return {
        "symbol": sym,
        "price_now": round(price_now, 2),
        "momentum_pct": None if momentum_pct is None else round(momentum_pct, 4),
        "using": using,
        "plan_hint": plan_hint or "",
        "buy_plan": {"budget": budget, "shares": shares, "estimated_cost": est_cost},
        "note": "Educational sample strategy; not financial advice."
    }





























































