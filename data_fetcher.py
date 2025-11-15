# data_fetcher.py
from __future__ import annotations

import os
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import requests


FINNHUB_BASE = "https://finnhub.io/api/v1"


class FinnhubError(Exception):
    """Simple custom error so the rest of the app can catch API issues."""
    pass


def _require_api_key() -> str:
    key = os.getenv("FINNHUB_API_KEY")
    if not key:
        raise FinnhubError("FINNHUB_API_KEY is not set in environment variables.")
    return key


def _get(path: str, params: Optional[Dict[str, Any]] = None) -> Any:
    """
    Low-level helper for calling Finnhub.
    Always attaches the token and does basic error checking.
    """
    token = _require_api_key()
    url = f"{FINNHUB_BASE}{path}"

    params = dict(params or {})
    params.setdefault("token", token)

    try:
        resp = requests.get(url, params=params, timeout=8)
    except Exception as e:
        raise FinnhubError(f"Network error calling Finnhub: {e}") from e

    if resp.status_code != 200:
        raise FinnhubError(f"Finnhub HTTP {resp.status_code}: {resp.text[:200]}")

    try:
        return resp.json()
    except Exception as e:
        raise FinnhubError(f"Failed to decode Finnhub JSON: {e}") from e


# ---------------------------------------------------------------------
# Public helpers used by the rest of the app
# ---------------------------------------------------------------------

def quote(symbol: str) -> Dict[str, Any]:
    """
    Get latest quote for a symbol.
    Normalized dictionary used by the rest of the app.
    """
    data = _get("/quote", {"symbol": symbol})

    # Finnhub fields: c=current, o=open, h=high, l=low, pc=prev close, t=timestamp
    return {
        "symbol": symbol.upper(),
        "price": data.get("c"),
        "open": data.get("o"),
        "high": data.get("h"),
        "low": data.get("l"),
        "prev_close": data.get("pc"),
        "timestamp": data.get("t"),
        "raw": data,
    }


def candles(
    symbol: str,
    days: int = 260,
    resolution: str = "D",
) -> Dict[str, Any]:
    """
    Get OHLCV candles for the past `days` days.
    Returns a normalized dict with time + o/h/l/c/v lists.
    """
    now = int(time.time())
    start = now - days * 24 * 60 * 60

    data = _get(
        "/stock/candle",
        {
            "symbol": symbol,
            "resolution": resolution,
            "from": start,
            "to": now,
        },
    )

    if data.get("s") != "ok":
        # Finnhub sometimes returns {"s": "no_data", ...}
        raise FinnhubError(f"Finnhub candle error for {symbol}: {data!r}")

    return {
        "symbol": symbol.upper(),
        "t": data.get("t") or [],
        "o": data.get("o") or [],
        "h": data.get("h") or [],
        "l": data.get("l") or [],
        "c": data.get("c") or [],
        "v": data.get("v") or [],
        "raw": data,
    }


# Backwards-compat aliases so older code still works:
def get_quote(symbol: str) -> Dict[str, Any]:
    return quote(symbol)


def get_candles(
    symbol: str,
    days: int = 260,
    resolution: str = "D",
) -> Dict[str, Any]:
    return candles(symbol, days=days, resolution=resolution)


# ---------------------------------------------------------------------
# Extra endpoints used by engine.py + backtest.py
# ---------------------------------------------------------------------

def recommendation_trends(symbol: str) -> Dict[str, Any]:
    """
    Wraps /stock/recommendation.

    Returns the most recent recommendation row, or {} if none.
    The engine only needs counts of buy/sell/hold-ish.
    """
    try:
        data = _get("/stock/recommendation", {"symbol": symbol})
    except FinnhubError:
        return {}

    if isinstance(data, list) and data:
        return data[0]  # latest entry
    return {}


def news_sentiment(symbol: str) -> Dict[str, Any]:
    """
    Wraps /news-sentiment.

    Returns the whole JSON blob (engine picks out bullishPercent, etc).
    """
    try:
        data = _get("/news-sentiment", {"symbol": symbol})
    except FinnhubError:
        return {}

    return data or {}


def earnings_calendar(symbol: str) -> Dict[str, Any]:
    """
    Wraps /calendar/earnings.

    Returns the next upcoming earnings item in a small dict, or {}.
    """
    today = datetime.utcnow().date()
    # Range: 1 week back, 4 weeks forward
    start = today - timedelta(days=7)
    end = today + timedelta(days=28)

    try:
        data = _get(
            "/calendar/earnings",
            {
                "symbol": symbol,
                "from": start.isoformat(),
                "to": end.isoformat(),
            },
        )
    except FinnhubError:
        return {}

    items = data.get("earningsCalendar") or []
    if not items:
        return {}

    # Just return the first upcoming item
    return items[0]











































