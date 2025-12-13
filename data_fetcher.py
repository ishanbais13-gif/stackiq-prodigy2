import os
import requests
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional


ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_API_SECRET = os.getenv("ALPACA_API_SECRET")

# IMPORTANT:
# Market data endpoints live on data.alpaca.markets
ALPACA_DATA_URL = os.getenv("ALPACA_DATA_URL", "https://data.alpaca.markets")

# Trading endpoints live on paper-api (paper) or api (live).
ALPACA_TRADING_URL = os.getenv("ALPACA_TRADING_URL", "https://paper-api.alpaca.markets")


def _require_keys():
    if not ALPACA_API_KEY or not ALPACA_API_SECRET:
        raise RuntimeError("Missing ALPACA_API_KEY or ALPACA_API_SECRET environment variables.")


def _headers() -> Dict[str, str]:
    _require_keys()
    return {
        "APCA-API-KEY-ID": ALPACA_API_KEY,
        "APCA-API-SECRET-KEY": ALPACA_API_SECRET,
    }


def _get(base_url: str, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
    url = f"{base_url}{path}"
    r = requests.get(url, headers=_headers(), params=params or {}, timeout=15)
    r.raise_for_status()
    return r.json()


# ---------------------------
# Quote
# ---------------------------
def get_quote(symbol: str) -> Dict[str, Any]:
    """
    Latest quote via Alpaca Market Data v2
    """
    sym = symbol.upper()
    data = _get(ALPACA_DATA_URL, f"/v2/stocks/{sym}/quotes/latest")
    quote = data.get("quote", {}) or data.get("quotes", {})  # defensive
    if not quote:
        return {"symbol": sym, "quote": None, "raw": data}

    return {
        "symbol": sym,
        "bid": quote.get("bp"),
        "ask": quote.get("ap"),
        "bid_size": quote.get("bs"),
        "ask_size": quote.get("as"),
        "timestamp": quote.get("t"),
        "raw": quote,
    }


# ---------------------------
# Candles / Bars
# ---------------------------
def _resolution_to_timeframe(resolution: str) -> str:
    r = str(resolution).upper()
    if r in ("D", "1D", "DAY"):
        return "1Day"
    if r in ("60", "60MIN", "1H"):
        return "1Hour"
    if r in ("15", "15MIN"):
        return "15Min"
    if r in ("5", "5MIN"):
        return "5Min"
    return "1Day"


def get_candles(symbol: str, days: int = 30, resolution: str = "D") -> List[Dict[str, Any]]:
    sym = symbol.upper()
    timeframe = _resolution_to_timeframe(resolution)

    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days + 3)

    params = {
        "timeframe": timeframe,
        "start": start.isoformat(timespec="seconds").replace("+00:00", "Z"),
        "end": end.isoformat(timespec="seconds").replace("+00:00", "Z"),
        "limit": 1000,
    }

    data = _get(ALPACA_DATA_URL, f"/v2/stocks/{sym}/bars", params=params)
    bars = data.get("bars", []) or []

    candles: List[Dict[str, Any]] = []
    for b in bars:
        candles.append(
            {
                "time": b.get("t"),
                "open": b.get("o"),
                "high": b.get("h"),
                "low": b.get("l"),
                "close": b.get("c"),
                "volume": b.get("v"),
            }
        )
    return candles


# ---------------------------
# News (Market Data News)
# ---------------------------
def get_news(symbol: str, limit: int = 5) -> Dict[str, Any]:
    sym = symbol.upper()
    params = {"symbols": sym, "limit": limit}
    data = _get(ALPACA_DATA_URL, "/v1beta1/news", params=params)

    items = []
    for item in (data or []):
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

    return {"symbol": sym, "items": items}


# ---------------------------
# Simple v1 "Predict" (always returns something)
# ---------------------------
def run_predict_engine(symbol: str, budget: float, risk: str = "medium") -> Dict[str, Any]:
    sym = symbol.upper()
    risk = (risk or "medium").lower()

    try:
        candles = get_candles(sym, days=25, resolution="D")
    except Exception:
        candles = []

    if len(candles) < 2:
        return {
            "symbol": sym,
            "direction": "unknown",
            "buy_zone": "N/A",
            "target": None,
            "stop": None,
            "position_size": None,
            "risk": risk,
            "confidence": 0,
            "projected_roi": 0,
            "notes": "Not enough candle data to generate a setup.",
        }

    last = candles[-1]
    prev = candles[-2]
    price = float(last["close"])
    prev_price = float(prev["close"])

    change = price - prev_price
    direction = "up" if change >= 0 else "down"

    # Risk multipliers (tweak later)
    if risk == "high":
        buy_low_mult, buy_high_mult, target_mult, stop_mult = 0.97, 0.99, 1.08, 0.94
        conf = 58
    elif risk == "low":
        buy_low_mult, buy_high_mult, target_mult, stop_mult = 0.985, 0.995, 1.03, 0.97
        conf = 72
    else:
        buy_low_mult, buy_high_mult, target_mult, stop_mult = 0.98, 0.99, 1.05, 0.95
        conf = 65

    buy_low = round(price * buy_low_mult, 2)
    buy_high = round(price * buy_high_mult, 2)
    target = round(price * target_mult, 2)
    stop = round(price * stop_mult, 2)

    # Position sizing: cap risk to 2% of budget
    max_risk_per_trade = budget * 0.02
    per_share_risk = max(price - stop, 0.01)
    shares = max(int(max_risk_per_trade // per_share_risk), 1)

    projected_roi = round(((target - price) / price) * 100, 2)

    return {
        "symbol": sym,
        "last_price": price,
        "direction": direction,
        "buy_zone": f"{buy_low} - {buy_high}",
        "target": target,
        "stop": stop,
        "position_size": f"{shares} shares",
        "risk": risk,
        "confidence": conf,
        "projected_roi": projected_roi,
        "notes": f"Simple v1 setup based on recent daily momentum ({direction}).",
    }





















































