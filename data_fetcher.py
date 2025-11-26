import os
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

import requests

# ---------------------------------------------------------------------------
# Environment / Config
# ---------------------------------------------------------------------------

ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_SECRET = os.getenv("ALPACA_SECRET")
# You set this in Azure as https://data.alpaca.markets
ALPACA_BASE_URL = os.getenv("ALPACA_BASE_URL", "https://data.alpaca.markets")

# News lives under the same host, different path
ALPACA_NEWS_PATH = "/v1beta1/news"

if not ALPACA_API_KEY or not ALPACA_SECRET:
    # Don't crash the app, but make it obvious in logs
    print("WARNING: ALPACA_API_KEY or ALPACA_SECRET is not set in environment variables.")


# ---------------------------------------------------------------------------
# Low-level Alpaca helper
# ---------------------------------------------------------------------------

def _alpaca_get(base: str, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Low-level helper to GET from Alpaca with auth headers and basic error handling.
    """
    url = base.rstrip("/") + path
    headers = {
        "APCA-API-KEY-ID": ALPACA_API_KEY or "",
        "APCA-API-SECRET-KEY": ALPACA_SECRET or "",
    }

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=10)
    except requests.RequestException as e:
        raise RuntimeError(f"Network error talking to Alpaca: {e}") from e

    if resp.status_code == 200:
        return resp.json()

    # Bubble up a short, readable error
    raise RuntimeError(f"Alpaca error {resp.status_code}: {resp.text[:300]}")


# ---------------------------------------------------------------------------
# Quotes
# ---------------------------------------------------------------------------

def get_quote(symbol: str) -> Dict[str, Any]:
    """
    Get a snapshot for a single symbol and normalize it into a simple quote dict.

    Uses:
      GET /v2/stocks/{symbol}/snapshot
    """
    symbol = symbol.upper()
    data = _alpaca_get(ALPACA_BASE_URL, f"/v2/stocks/{symbol}/snapshot")

    latest_trade = data.get("latestTrade") or {}
    latest_quote = data.get("latestQuote") or {}
    daily_bar = data.get("dailyBar") or {}
    prev_daily_bar = data.get("prevDailyBar") or {}

    last_price = latest_trade.get("p") or daily_bar.get("c") or prev_daily_bar.get("c")
    open_price = daily_bar.get("o")
    high_price = daily_bar.get("h")
    low_price = daily_bar.get("l")
    prev_close = prev_daily_bar.get("c")
    volume = daily_bar.get("v") or 0

    # Change vs previous close if both exist
    change = None
    change_pct = None
    if last_price is not None and prev_close is not None and prev_close != 0:
        change = last_price - prev_close
        change_pct = (change / prev_close) * 100

    # Timestamps come back as RFC3339 / ISO strings; we just pass them through
    trade_ts = latest_trade.get("t")

    return {
        "symbol": symbol,
        "last": last_price,
        "open": open_price,
        "high": high_price,
        "low": low_price,
        "prev_close": prev_close,
        "change": change,
        "change_percent": change_pct,
        "volume": volume,
        "bid": latest_quote.get("bp"),
        "ask": latest_quote.get("ap"),
        "bid_size": latest_quote.get("bs"),
        "ask_size": latest_quote.get("as"),
        "timestamp": trade_ts,
        "raw": data,  # keep full snapshot if the API layer wants anything else
    }


# ---------------------------------------------------------------------------
# Candles (historical bars)
# ---------------------------------------------------------------------------

def get_candles(
    symbol: str,
    timeframe: str = "1Day",
    limit: int = 60,
) -> List[Dict[str, Any]]:
    """
    Fetch historical bars for a single symbol.

    Uses:
      GET /v2/stocks/{symbol}/bars

    timeframe examples:
      "1Min", "5Min", "15Min", "1Hour", "1Day"
    """
    symbol = symbol.upper()

    # Alpaca expects ISO8601 UTC timestamps with 'Z'
    now = datetime.utcnow()
    # Go back a bit further than limit so we don't hit weekends/holidays
    start = (now - timedelta(days=limit * 2)).replace(microsecond=0).isoformat() + "Z"
    end = now.replace(microsecond=0).isoformat() + "Z"

    params = {
        "timeframe": timeframe,
        "start": start,
        "end": end,
        "limit": limit,
    }

    data = _alpaca_get(ALPACA_BASE_URL, f"/v2/stocks/{symbol}/bars", params=params)
    bars = data.get("bars", [])

    normalized: List[Dict[str, Any]] = []
    for b in bars:
        normalized.append(
            {
                "t": b.get("t"),  # ISO8601 time
                "o": b.get("o"),
                "h": b.get("h"),
                "l": b.get("l"),
                "c": b.get("c"),
                "v": b.get("v"),
            }
        )

    return normalized


# ---------------------------------------------------------------------------
# News
# ---------------------------------------------------------------------------

def get_news(symbol: str, limit: int = 20) -> List[Dict[str, Any]]:
    """
    Fetch recent news for a symbol.

    Uses:
      GET /v1beta1/news?symbols={symbol}&limit={limit}
    """
    symbol = symbol.upper()

    params = {
        "symbols": symbol,
        "limit": limit,
        "sort": "desc",
    }

    data = _alpaca_get(ALPACA_BASE_URL, ALPACA_NEWS_PATH, params=params)
    # Alpaca returns a list of articles at top level
    articles = data if isinstance(data, list) else data.get("news", [])

    normalized: List[Dict[str, Any]] = []
    for item in articles:
        normalized.append(
            {
                "id": item.get("id"),
                "headline": item.get("headline"),
                "summary": item.get("summary"),
                "source": item.get("source"),
                "url": item.get("url"),
                "symbols": item.get("symbols") or [],
                "created_at": item.get("created_at"),
            }
        )

    return normalized


# ---------------------------------------------------------------------------
# Synthetic “targets” (no extra API, just logic)
# ---------------------------------------------------------------------------

def get_targets(symbol: str) -> Dict[str, Any]:
    """
    Synthetic analyst-style price targets based on current price.
    This avoids any premium endpoints while still giving StackIQ
    something useful to show.

    Strategy:
      - Use latest price
      - Target_low  = -10%
      - Target_avg  = +5%
      - Target_high = +20%
    """
    q = get_quote(symbol)
    current = q.get("last")

    if current is None:
        raise RuntimeError(f"Unable to compute targets, missing last price for {symbol}")

    low = round(current * 0.90, 2)
    avg = round(current * 1.05, 2)
    high = round(current * 1.20, 2)

    return {
        "symbol": q["symbol"],
        "current": current,
        "target_low": low,
        "target_average": avg,
        "target_high": high,
        "source": "StackIQ synthetic targets (Alpaca price-based)",
    }


# ---------------------------------------------------------------------------
# Synthetic options-helper (also pure logic)
# ---------------------------------------------------------------------------

def get_options_helper(symbol: str) -> Dict[str, Any]:
    """
    Very lightweight "options helper" that *doesn't* hit any options API
    (Alpaca doesn't expose a free options chain anyway).

    We just:
      - Grab the latest price
      - Suggest 3 strikes around the money
    The FastAPI layer can turn this into whatever UX it wants.
    """
    q = get_quote(symbol)
    last = q.get("last")

    if last is None:
        raise RuntimeError(f"Unable to compute options helper, missing last price for {symbol}")

    # Round strikes to neat numbers
    base = round(last / 5) * 5  # nearest 5 dollars
    strikes = sorted(
        {
            round(base - 5, 2),
            round(base, 2),
            round(base + 5, 2),
        }
    )

    return {
        "symbol": q["symbol"],
        "last": last,
        "suggested_strikes": strikes,
        "notes": "Synthetic strikes based on current price; not a real options chain.",
    }

















































