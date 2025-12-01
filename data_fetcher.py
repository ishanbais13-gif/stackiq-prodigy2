import os
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

# --- Alpaca config ---

ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_API_SECRET = os.getenv("ALPACA_API_SECRET")
ALPACA_BASE_URL = "https://data.alpaca.markets"


def _get_headers() -> Dict[str, str]:
    if not ALPACA_API_KEY or not ALPACA_API_SECRET:
        # Still return something graceful; app will surface a clean error
        raise RuntimeError("Missing ALPACA_API_KEY or ALPACA_API_SECRET environment variables.")
    return {
        "APCA-API-KEY-ID": ALPACA_API_KEY,
        "APCA-API-SECRET-KEY": ALPACA_API_SECRET,
    }


def _alpaca_get(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    url = f"{ALPACA_BASE_URL}{path}"
    resp = requests.get(url, headers=_get_headers(), params=params or {}, timeout=10)
    resp.raise_for_status()
    return resp.json()


# --------------------------------------------------------------------
#  Quote
# --------------------------------------------------------------------
def get_quote(symbol: str) -> Dict[str, Any]:
    """
    Get the latest quote for a symbol from Alpaca.
    """
    symbol = symbol.upper()
    data = _alpaca_get(f"/v2/stocks/{symbol}/quotes/latest")

    quote = data.get("quote", {})
    return {
        "symbol": symbol,
        "bid": quote.get("bp"),
        "ask": quote.get("ap"),
        "bid_size": quote.get("bs"),
        "ask_size": quote.get("as"),
        "timestamp": quote.get("t"),
        "raw": quote,
    }


# --------------------------------------------------------------------
#  Candles / Bars
# --------------------------------------------------------------------
def _resolution_to_timeframe(resolution: str) -> str:
    """
    Map a generic resolution to Alpaca timeframe strings.
    """
    resolution = str(resolution).upper()
    if resolution in ("D", "1D", "DAY"):
        return "1Day"
    if resolution in ("60", "60MIN", "1H"):
        return "1Hour"
    if resolution in ("15", "15MIN"):
        return "15Min"
    if resolution in ("5", "5MIN"):
        return "5Min"
    return "1Day"


def get_candles(symbol: str, days: int = 30, resolution: str = "D") -> List[Dict[str, Any]]:
    """
    Get recent OHLCV bars for a symbol.
    """
    symbol = symbol.upper()
    timeframe = _resolution_to_timeframe(resolution)

    end = datetime.utcnow()
    start = end - timedelta(days=days + 2)

    params = {
        "timeframe": timeframe,
        "start": start.isoformat(timespec="seconds") + "Z",
        "end": end.isoformat(timespec="seconds") + "Z",
        "limit": 1000,
    }

    data = _alpaca_get(f"/v2/stocks/{symbol}/bars", params=params)
    bars = data.get("bars", [])

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


# --------------------------------------------------------------------
#  News
# --------------------------------------------------------------------
def get_news(symbol: str, limit: int = 5) -> Dict[str, Any]:
    """
    Get recent news for a symbol.
    """
    symbol = symbol.upper()
    params = {"symbols": symbol, "limit": limit}
    data = _alpaca_get("/v1beta1/news", params=params)

    items = []
    for item in data:
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


# --------------------------------------------------------------------
#  TEMP PREDICT ENGINE (mock but NEVER returns None)
# --------------------------------------------------------------------
def run_predict_engine(symbol: str, budget: float, risk: str = "medium") -> Dict[str, Any]:
    """
    Temporary mock predict engine.
    Uses recent candles to build a simple but stable trade idea.
    This is intentionally simple so your UI always has data.
    """
    symbol = symbol.upper()
    risk = (risk or "medium").lower()

    try:
        candles = get_candles(symbol, days=20, resolution="D")
    except Exception:
        candles = []

    if not candles or len(candles) < 2:
        # Safe fallback – still returns something your frontend can render.
        return {
            "symbol": symbol,
            "buy_zone": "N/A",
            "target": None,
            "stop": None,
            "position_size": None,
            "risk": risk,
            "confidence": 0,
            "projected_roi": 0,
            "notes": "Not enough recent data to generate a mock setup.",
        }

    last = candles[-1]
    prev = candles[-2]

    price = float(last["close"])
    prev_price = float(prev["close"])

    change = price - prev_price
    direction = "up" if change >= 0 else "down"

    # Risk preset multipliers
    if risk == "high":
        buy_low_mult, buy_high_mult, target_mult, stop_mult = 0.96, 0.99, 1.08, 0.93
        conf = 58
        roi = 14
    elif risk == "low":
        buy_low_mult, buy_high_mult, target_mult, stop_mult = 0.985, 0.995, 1.03, 0.97
        conf = 72
        roi = 6
    else:  # medium
        buy_low_mult, buy_high_mult, target_mult, stop_mult = 0.97, 0.99, 1.05, 0.95
        conf = 65
        roi = 10

    buy_low = round(price * buy_low_mult, 2)
    buy_high = round(price * buy_high_mult, 2)
    target = round(price * target_mult, 2)
    stop = round(price * stop_mult, 2)

    # Simple position sizing: risk ~2% of budget per trade
    max_risk_per_trade = budget * 0.02
    per_share_risk = max(price - stop, 0.01)
    shares = max(int(max_risk_per_trade // per_share_risk), 1)

    return {
        "symbol": symbol,
        "last_price": price,
        "direction": direction,
        "buy_zone": f"{buy_low}–{buy_high}",
        "target": target,
        "stop": stop,
        "position_size": f"{shares} shares",
        "risk": risk,
        "confidence": conf,
        "projected_roi": roi,
        "notes": f"Mock {risk} setup based on recent daily momentum ({direction}).",
    }


















































