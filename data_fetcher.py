import os
import requests
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

# --- Alpaca credentials (Environment Variables) ---
ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_API_SECRET = os.getenv("ALPACA_API_SECRET")

# IMPORTANT:
# Set ALPACA_BASE_URL to "https://data.alpaca.markets" (NO /v2)
ALPACA_BASE_URL = os.getenv("ALPACA_BASE_URL", "https://data.alpaca.markets").rstrip("/")

def _get_headers() -> Dict[str, str]:
    if not ALPACA_API_KEY or not ALPACA_API_SECRET:
        raise RuntimeError("Missing ALPACA_API_KEY or ALPACA_API_SECRET environment variables.")
    return {
        "APCA-API-KEY-ID": ALPACA_API_KEY,
        "APCA-API-SECRET-KEY": ALPACA_API_SECRET,
    }

def _normalize_base(base: str) -> str:
    """
    If someone accidentally sets base like .../v2, strip the trailing /v2
    so we don't create /v2/v2 paths.
    """
    b = base.rstrip("/")
    if b.endswith("/v2"):
        b = b[:-3]
    return b

def _alpaca_get(path: str, params: Optional[Dict[str, Any]] = None) -> Any:
    base = _normalize_base(ALPACA_BASE_URL)
    path = path if path.startswith("/") else f"/{path}"
    url = f"{base}{path}"
    resp = requests.get(url, headers=_get_headers(), params=params or {}, timeout=10)
    resp.raise_for_status()
    return resp.json()

# -------------------------
# Quote
# -------------------------
def get_quote(symbol: str) -> Dict[str, Any]:
    symbol = symbol.upper().strip()
    data = _alpaca_get(f"/v2/stocks/{symbol}/quotes/latest")
    quote = data.get("quote") or {}
    # Return a stable shape
    return {
        "symbol": symbol,
        "bid": quote.get("bp"),
        "ask": quote.get("ap"),
        "bid_size": quote.get("bs"),
        "ask_size": quote.get("as"),
        "timestamp": quote.get("t"),
        "raw": quote,
    }

# -------------------------
# Candles / Bars
# -------------------------
def _resolution_to_timeframe(resolution: str) -> str:
    r = str(resolution).upper().strip()
    if r in ("D", "1D", "DAY"):
        return "1Day"
    if r in ("60", "60MIN", "1H", "H"):
        return "1Hour"
    if r in ("15", "15MIN"):
        return "15Min"
    if r in ("5", "5MIN"):
        return "5Min"
    return "1Day"

def get_candles(symbol: str, days: int = 30, resolution: str = "D") -> List[Dict[str, Any]]:
    symbol = symbol.upper().strip()
    timeframe = _resolution_to_timeframe(resolution)

    end = datetime.utcnow()
    start = end - timedelta(days=max(days, 1) + 5)

    params = {
        "timeframe": timeframe,
        "start": start.isoformat(timespec="seconds") + "Z",
        "end": end.isoformat(timespec="seconds") + "Z",
        "limit": 1000,
    }

    data = _alpaca_get(f"/v2/stocks/{symbol}/bars", params=params)
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

# -------------------------
# News
# -------------------------
def get_news(symbol: str, limit: int = 5) -> Dict[str, Any]:
    symbol = symbol.upper().strip()
    params = {"symbols": symbol, "limit": int(limit)}
    data = _alpaca_get("/v1beta1/news", params=params)

    items = []
    for item in data or []:
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

# -------------------------
# Simple v1 Predict Engine (mock but stable)
# -------------------------
def run_predict_engine(symbol: str, budget: float, risk: str = "medium") -> Dict[str, Any]:
    """
    Simple, stable v1 output so your frontend always gets something.
    Uses recent daily candles to decide up/down momentum and outputs a trade plan.
    """
    symbol = symbol.upper().strip()
    risk = (risk or "medium").lower().strip()

    candles = get_candles(symbol, days=20, resolution="D")
    if len(candles) < 2:
        return {
            "symbol": symbol,
            "last_price": None,
            "direction": "unknown",
            "buy_zone": "N/A",
            "target": None,
            "stop": None,
            "position_size": "0 shares",
            "risk": risk,
            "confidence": 0,
            "projected_roi": 0,
            "notes": "Not enough data to generate setup.",
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
        conf, roi = 58, 14
    elif risk == "low":
        buy_low_mult, buy_high_mult, target_mult, stop_mult = 0.985, 0.995, 1.03, 0.97
        conf, roi = 72, 6
    else:  # medium
        buy_low_mult, buy_high_mult, target_mult, stop_mult = 0.97, 0.99, 1.05, 0.95
        conf, roi = 65, 10

    buy_low = round(price * buy_low_mult, 2)
    buy_high = round(price * buy_high_mult, 2)
    target = round(price * target_mult, 2)
    stop = round(price * stop_mult, 2)

    # Position sizing: risk ~2% of budget per trade
    max_risk_per_trade = float(budget) * 0.02
    per_share_risk = max(price - stop, 0.01)
    shares = max(int(max_risk_per_trade // per_share_risk), 1)

    return {
        "symbol": symbol,
        "last_price": price,
        "direction": direction,
        "buy_zone": f"{buy_low} - {buy_high}",
        "target": target,
        "stop": stop,
        "position_size": f"{shares} shares",
        "risk": risk,
        "confidence": conf,
        "projected_roi": roi,
        "notes": f"Mock {risk} setup based on recent daily momentum ({direction}).",
    }




















































