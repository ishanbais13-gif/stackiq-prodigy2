 import os
import requests
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

# Alpaca Market Data base
ALPACA_DATA_BASE = os.getenv("ALPACA_DATA_BASE", "https://data.alpaca.markets")

# Keys (these MUST be set in Azure App Service env vars)
ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_API_SECRET = os.getenv("ALPACA_API_SECRET")


def _headers() -> Dict[str, str]:
    if not ALPACA_API_KEY or not ALPACA_API_SECRET:
        raise RuntimeError("Missing ALPACA_API_KEY or ALPACA_API_SECRET environment variables.")
    return {
        "APCA-API-KEY-ID": ALPACA_API_KEY,
        "APCA-API-SECRET-KEY": ALPACA_API_SECRET,
    }


def _get(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    url = f"{ALPACA_DATA_BASE}{path}"
    r = requests.get(url, headers=_headers(), params=params or {}, timeout=15)
    r.raise_for_status()
    return r.json()


def get_quote(symbol: str) -> Dict[str, Any]:
    symbol = symbol.upper()
    # Force free feed
    data = _get(f"/v2/stocks/{symbol}/quotes/latest", params={"feed": "iex"})
    quote = data.get("quote") or {}
    return {
        "symbol": symbol,
        "bid": quote.get("bp"),
        "ask": quote.get("ap"),
        "bid_size": quote.get("bs"),
        "ask_size": quote.get("as"),
        "timestamp": quote.get("t"),
        "raw": quote,
    }


def _resolution_to_timeframe(resolution: str) -> str:
    r = (resolution or "1Day").strip().upper()
    if r in ("D", "1D", "DAY", "1DAY"):
        return "1Day"
    if r in ("60", "60MIN", "1H", "HOUR", "1HOUR"):
        return "1Hour"
    if r in ("15", "15MIN"):
        return "15Min"
    if r in ("5", "5MIN"):
        return "5Min"
    return "1Day"


def get_candles(symbol: str, days: int = 30, resolution: str = "1Day") -> Dict[str, Any]:
    symbol = symbol.upper()
    timeframe = _resolution_to_timeframe(resolution)

    end = datetime.now(timezone.utc)
    start = end - timedelta(days=max(2, days))

    params = {
        "timeframe": timeframe,
        "start": start.isoformat(timespec="seconds").replace("+00:00", "Z"),
        "end": end.isoformat(timespec="seconds").replace("+00:00", "Z"),
        "limit": 1000,
        # THIS is the important part for your 403:
        "feed": "iex",
    }

    data = _get(f"/v2/stocks/{symbol}/bars", params=params)
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

    return {"symbol": symbol, "timeframe": timeframe, "count": len(candles), "candles": candles}


def get_news(symbol: str, limit: int = 5) -> Dict[str, Any]:
    symbol = symbol.upper()
    # News endpoint is /v1beta1/news (still market-data domain)
    data = _get("/v1beta1/news", params={"symbols": symbol, "limit": int(limit)})
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
    return {"symbol": symbol, "items": items}


def run_predict_engine(symbol: str, budget: float, risk: str = "medium") -> Dict[str, Any]:
    """
    Simple v1 placeholder: uses recent candles to output a stable "trade idea"
    so your frontend has something consistent to render.
    """
    risk = (risk or "medium").lower()
    try:
        candles = get_candles(symbol, days=20, resolution="1Day")["candles"]
    except Exception:
        candles = []

    if len(candles) < 2:
        return {
            "symbol": symbol.upper(),
            "direction": "unknown",
            "buy_zone": "N/A",
            "target": None,
            "stop": None,
            "position_size": None,
            "risk": risk,
            "confidence": 0,
            "projected_roi": 0,
            "notes": "Not enough recent candle data.",
        }

    last = candles[-1]
    prev = candles[-2]
    price = float(last["close"])
    prev_price = float(prev["close"])
    change = price - prev_price
    direction = "up" if change >= 0 else "down"

    # Risk presets (tweak later)
    if risk == "high":
        buy_mult, target_mult, stop_mult, conf = 0.99, 1.08, 0.93, 58
        roi = 14
    elif risk == "low":
        buy_mult, target_mult, stop_mult, conf = 0.995, 1.03, 0.97, 72
        roi = 6
    else:
        buy_mult, target_mult, stop_mult, conf = 0.99, 1.05, 0.95, 65
        roi = 10

    buy = round(price * buy_mult, 2)
    target = round(price * target_mult, 2)
    stop = round(price * stop_mult, 2)

    max_risk_per_trade = float(budget) * 0.02
    per_share_risk = max(price - stop, 0.01)
    shares = max(int(max_risk_per_trade // per_share_risk), 1)

    return {
        "symbol": symbol.upper(),
        "last_price": price,
        "direction": direction,
        "buy_zone": f"{buy}",
        "target": target,
        "stop": stop,
        "position_size": f"{shares} shares",
        "risk": risk,
        "confidence": conf,
        "projected_roi": roi,
        "notes": f"Mock v1 setup using IEX daily candles. Direction: {direction}.",
    }





















































