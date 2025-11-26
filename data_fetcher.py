import os
from typing import Any, Dict, List, Optional

import requests

# Environment variables (already set in Azure)
ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET")
ALPACA_BASE_URL = os.getenv("ALPACA_BASE_URL", "https://paper-api.alpaca.markets/v2")
ALPACA_DATA_URL = os.getenv("ALPACA_DATA_URL", "https://data.alpaca.markets")


def _alpaca_get(
    base: str, path: str, params: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Low-level helper to GET from Alpaca with auth headers + basic error handling.
    """
    if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
        raise RuntimeError("ALPACA_API_KEY or ALPACA_SECRET is not set in env vars")

    url = base.rstrip("/") + path
    headers = {
        "APCA-API-KEY-ID": ALPACA_API_KEY,
        "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
    }

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=10)
    except requests.RequestException as e:
        raise RuntimeError(f"Network error talking to Alpaca: {e}")

    if resp.status_code != 200:
        raise RuntimeError(f"Alpaca error {resp.status_code}: {resp.text[:300]}")

    return resp.json()


# ---------- Core data helpers ----------


def get_candles(
    symbol: str, days: int = 60, timeframe: str = "1Day"
) -> List[Dict[str, Any]]:
    """
    Fetch recent OHLCV candles using Alpaca market data.
    Uses bars endpoint: https://data.alpaca.markets/v2/stocks/{symbol}/bars
    """
    symbol = symbol.upper()
    params = {
        "timeframe": timeframe,
        "limit": days,
        "adjustment": "raw",
        "feed": "iex",
    }
    data = _alpaca_get(ALPACA_DATA_URL, f"/v2/stocks/{symbol}/bars", params=params)

    bars = data.get("bars", [])
    candles: List[Dict[str, Any]] = []
    for bar in bars:
        candles.append(
            {
                "timestamp": bar.get("t"),
                "open": bar.get("o"),
                "high": bar.get("h"),
                "low": bar.get("l"),
                "close": bar.get("c"),
                "volume": bar.get("v"),
            }
        )
    return candles


def get_quote(symbol: str) -> Dict[str, Any]:
    """
    Build a quote-style snapshot from the most recent daily bars.
    (We fake a "quote" using the latest daily close + previous close.)
    """
    candles = get_candles(symbol, days=2, timeframe="1Day")
    if not candles:
        raise RuntimeError("No candle data returned from Alpaca")

    last = candles[-1]
    prev = candles[-2] if len(candles) > 1 else last

    current = last["close"]
    previous_close = prev["close"]
    change = None
    percent_change = None
    if current is not None and previous_close not in (None, 0):
        change = current - previous_close
        percent_change = (change / previous_close) * 100

    return {
        "symbol": symbol.upper(),
        "current": current,
        "open": last.get("open"),
        "high": last.get("high"),
        "low": last.get("low"),
        "previous_close": previous_close,
        "change": change,
        "percent_change": percent_change,
        "timestamp": last.get("timestamp"),
    }


def get_news(symbol: str, limit: int = 5) -> List[Dict[str, Any]]:
    """
    Fetch latest news for a symbol using Alpaca's news endpoint.
    If the endpoint fails, we just return [] instead of crashing the API.
    """
    symbol = symbol.upper()
    params = {"symbols": symbol, "limit": limit}
    try:
        data = _alpaca_get(ALPACA_DATA_URL, "/v1beta1/news", params=params)
    except Exception:
        return []

    items: List[Dict[str, Any]] = []
    for item in data.get("news", []):
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


# ---------- Stub helpers (safe, no external calls) ----------


def get_price_targets(symbol: str) -> Dict[str, Any]:
    """
    Placeholder price-targets helper.

    Alpaca's public API does not expose analyst price targets directly,
    so for now we return a simple stub structure that the frontend can use.
    """
    return {
        "symbol": symbol.upper(),
        "note": "Price targets are not available from Alpaca free API yet.",
        "median_target": None,
        "high_target": None,
        "low_target": None,
        "rating": None,
    }


def get_options_helper(symbol: str, risk: str = "medium") -> Dict[str, Any]:
    """
    Placeholder options helper – does NOT place trades or call any options API.
    It just returns a simple plan structure based on the risk level.
    """
    symbol = symbol.upper()
    risk = risk.lower()
    if risk not in {"low", "medium", "high"}:
        risk = "medium"

    leverage = {"low": 0.5, "medium": 1.0, "high": 1.5}[risk]

    return {
        "symbol": symbol,
        "risk": risk,
        "leverage_hint": leverage,
        "note": (
            "This is a placeholder options helper. "
            "In a future version we can integrate real options chains."
        ),
    }


















































