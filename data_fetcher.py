import os
import requests
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List, Literal

# IMPORTANT:
# - Do NOT raise errors at import-time (Azure will 504)
# - Only validate keys inside request functions

ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")

# Use Alpaca DATA domain for quotes/bars/news:
ALPACA_DATA_BASE_URL = os.getenv("ALPACA_DATA_BASE_URL", "https://data.alpaca.markets").rstrip("/")

DEFAULT_TIMEOUT = 10  # seconds

def _headers() -> Dict[str, str]:
    # read latest values (in case Azure injects after boot)
    key = os.getenv("ALPACA_API_KEY")
    secret = os.getenv("ALPACA_SECRET_KEY")
    if not key or not secret:
        raise Exception("Missing ALPACA_API_KEY / ALPACA_SECRET_KEY in environment variables")
    return {
        "APCA-API-KEY-ID": key,
        "APCA-API-SECRET-KEY": secret,
    }

def _get(url: str, params: Optional[dict] = None) -> Dict[str, Any]:
    r = requests.get(url, headers=_headers(), params=params or {}, timeout=DEFAULT_TIMEOUT)
    r.raise_for_status()
    return r.json()

# -------------------------
# QUOTE (latest)
# -------------------------
def get_latest_quote(symbol: str) -> Dict[str, Any]:
    sym = symbol.upper().strip()
    url = f"{ALPACA_DATA_BASE_URL}/v2/stocks/{sym}/quotes/latest"
    data = _get(url)
    q = data.get("quote") or {}
    if not q:
        raise Exception("Quote not found (check symbol or Alpaca permissions)")
    return q

# -------------------------
# BARS / CANDLES
# -------------------------
def get_bars(
    symbol: str,
    days: int = 30,
    timeframe: Literal["1Day", "1Hour", "15Min", "5Min"] = "1Day",
    feed: Optional[Literal["iex", "sip"]] = "iex",
) -> List[Dict[str, Any]]:
    """
    Alpaca bars endpoint expects timeframe like 1Day/1Hour/15Min/5Min
    feed=iex works for most free accounts; sip may 403 without subscription.
    """
    sym = symbol.upper().strip()

    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days + 2)

    params = {
        "timeframe": timeframe,
        "start": start.isoformat(timespec="seconds").replace("+00:00", "Z"),
        "end": end.isoformat(timespec="seconds").replace("+00:00", "Z"),
        "limit": min(1000, max(2, days * 5)),
    }

    # only include feed if provided
    if feed:
        params["feed"] = feed

    url = f"{ALPACA_DATA_BASE_URL}/v2/stocks/{sym}/bars"
    data = _get(url, params=params)
    bars = data.get("bars", [])

    # Normalize output for frontend (v1)
    out: List[Dict[str, Any]] = []
    for b in bars:
        out.append(
            {
                "t": b.get("t"),   # time
                "o": b.get("o"),
                "h": b.get("h"),
                "l": b.get("l"),
                "c": b.get("c"),
                "v": b.get("v"),
            }
        )
    return out

# -------------------------
# NEWS
# -------------------------
def get_news(symbol: str, limit: int = 5) -> List[Dict[str, Any]]:
    sym = symbol.upper().strip()
    # Alpaca data news endpoint:
    url = f"{ALPACA_DATA_BASE_URL}/v1beta1/news"
    params = {"symbols": sym, "limit": limit}
    data = _get(url, params=params)

    items: List[Dict[str, Any]] = []
    for item in data if isinstance(data, list) else []:
        items.append(
            {
                "id": item.get("id"),
                "headline": item.get("headline"),
                "summary": item.get("summary"),
                "url": item.get("url"),
                "created_at": item.get("created_at"),
                "source": item.get("source"),
            }
        )
    return items

# -------------------------
# SIMPLE V1 "PREDICT" ENGINE
# -------------------------
def run_predict_engine(symbol: str, budget: float, risk: str = "medium") -> Dict[str, Any]:
    sym = symbol.upper().strip()
    risk = (risk or "medium").lower()

    # Get recent daily bars
    bars = get_bars(sym, days=30, timeframe="1Day", feed="iex")

    if len(bars) < 5:
        return {
            "symbol": sym,
            "risk": risk,
            "error": "Not enough bar data to generate setup",
            "setup": None,
        }

    last_close = float(bars[-1]["c"])
    prev_close = float(bars[-2]["c"])
    change = last_close - prev_close
    direction = "up" if change >= 0 else "down"

    # Simple risk presets (V1)
    if risk == "high":
        buy_low_mult, buy_high_mult, target_mult, stop_mult = 0.985, 0.995, 1.06, 0.94
        confidence = 55
        projected_roi = 12
    elif risk == "low":
        buy_low_mult, buy_high_mult, target_mult, stop_mult = 0.993, 0.998, 1.03, 0.97
        confidence = 72
        projected_roi = 5
    else:
        buy_low_mult, buy_high_mult, target_mult, stop_mult = 0.99, 0.997, 1.045, 0.955
        confidence = 64
        projected_roi = 8

    buy_low = round(last_close * buy_low_mult, 2)
    buy_high = round(last_close * buy_high_mult, 2)
    target = round(last_close * target_mult, 2)
    stop = round(last_close * stop_mult, 2)

    # position sizing: risk 2% of budget, approximate stop distance
    max_risk = budget * 0.02
    per_share_risk = max(last_close - stop, 0.01)
    shares = max(int(max_risk // per_share_risk), 1)

    return {
        "symbol": sym,
        "last_price": last_close,
        "direction": direction,
        "risk": risk,
        "buy_zone": f"{buy_low} - {buy_high}",
        "target": target,
        "stop": stop,
        "position_size": f"{shares} shares",
        "confidence": confidence,
        "projected_roi": projected_roi,
        "notes": "V1 mock engine: uses recent daily momentum + fixed risk presets. Replace with real model later.",
    }























































