import os
import requests
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

# ==========================
# Alpaca Configuration
# ==========================

ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_API_SECRET = os.getenv("ALPACA_API_SECRET")
ALPACA_BASE_URL = os.getenv("ALPACA_BASE_URL", "https://data.alpaca.markets")

if not ALPACA_API_KEY or not ALPACA_API_SECRET:
    raise RuntimeError("Missing Alpaca API credentials")

HEADERS = {
    "APCA-API-KEY-ID": ALPACA_API_KEY,
    "APCA-API-SECRET-KEY": ALPACA_API_SECRET
}


# ==========================
# Internal Helpers
# ==========================

def _alpaca_get(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    url = f"{ALPACA_BASE_URL}{path}"
    response = requests.get(url, headers=HEADERS, params=params, timeout=10)
    response.raise_for_status()
    return response.json()


# ==========================
# Market Data
# ==========================

def get_quote(symbol: str) -> Dict[str, Any]:
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
        "raw": quote
    }


def get_candles(symbol: str, days: int = 30) -> List[Dict[str, Any]]:
    symbol = symbol.upper()
    end = datetime.utcnow()
    start = end - timedelta(days=days)

    params = {
        "timeframe": "1Day",
        "start": start.isoformat() + "Z",
        "end": end.isoformat() + "Z",
        "limit": 1000
    }

    data = _alpaca_get(f"/v2/stocks/{symbol}/bars", params=params)
    bars = data.get("bars", [])

    candles = []
    for bar in bars:
        candles.append({
            "time": bar.get("t"),
            "open": bar.get("o"),
            "high": bar.get("h"),
            "low": bar.get("l"),
            "close": bar.get("c"),
            "volume": bar.get("v")
        })

    return candles


# ==========================
# Predict Engine (V1)
# ==========================

def run_predict_engine(
    symbol: str,
    budget: float,
    risk: str = "medium"
) -> Dict[str, Any]:
    symbol = symbol.upper()
    risk = risk.lower()

    candles = get_candles(symbol, days=20)
    if len(candles) < 2:
        return {
            "symbol": symbol,
            "error": "Not enough historical data"
        }

    last = candles[-1]["close"]
    prev = candles[-2]["close"]
    direction = "up" if last >= prev else "down"

    # Risk profiles
    if risk == "low":
        target_mult = 1.03
        stop_mult = 0.97
        confidence = 72
    elif risk == "high":
        target_mult = 1.08
        stop_mult = 0.93
        confidence = 58
    else:
        target_mult = 1.05
        stop_mult = 0.95
        confidence = 65

    target = round(last * target_mult, 2)
    stop = round(last * stop_mult, 2)

    max_risk = budget * 0.02
    per_share_risk = max(last - stop, 0.01)
    shares = int(max_risk // per_share_risk)

    return {
        "symbol": symbol,
        "last_price": last,
        "direction": direction,
        "target": target,
        "stop": stop,
        "shares": max(shares, 1),
        "budget": budget,
        "risk": risk,
        "confidence": confidence,
        "notes": "V1 momentum-based prediction using recent daily candles"
    }






















































