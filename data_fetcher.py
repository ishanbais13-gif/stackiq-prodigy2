import os
import time
from typing import Dict, Any

import requests

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
FINNHUB_BASE_URL = "https://finnhub.io/api/v1"


class FinnhubError(Exception):
    """Custom error for Finnhub-related problems."""
    pass


def _require_api_key() -> str:
    """
    Ensure we have a Finnhub API key and return it.
    We only fail when a function is actually called – so import won't crash.
    """
    api_key = FINNHUB_API_KEY
    if not api_key:
        raise FinnhubError(
            "FINNHUB_API_KEY is not set in environment variables on Azure."
        )
    return api_key


def fetch_quote(symbol: str) -> Dict[str, Any]:
    """
    Fetch the latest quote for a symbol from Finnhub.

    Returns the raw Finnhub response dict, for example:
    {
        "c": 123.45,  # current price
        "d": 1.23,    # change
        "dp": 1.0,    # percent change
        "h": 125.0,   # high
        "l": 120.0,   # low
        "o": 121.0,   # open
        "pc": 122.0,  # previous close
        "t": 1234567890
    }
    """
    api_key = _require_api_key()

    url = f"{FINNHUB_BASE_URL}/quote"
    params = {
        "symbol": symbol.upper(),
        "token": api_key,
    }

    resp = requests.get(url, params=params, timeout=10)
    if resp.status_code == 403:
        # Plan / permissions issue – don't crash the whole app
        return {
            "error": "You don't have access to this resource.",
            "http_status": 403,
        }

    if not resp.ok:
        raise FinnhubError(
            f"Finnhub quote failed for {symbol} "
            f"({resp.status_code}): {resp.text[:200]}"
        )

    data = resp.json()
    # Finnhub returns c=0 for invalid symbol sometimes
    if not isinstance(data, dict) or data.get("c") in (None, 0):
        raise FinnhubError(f"Invalid quote data from Finnhub for {symbol}: {data}")

    return data


def fetch_candles(symbol: str, resolution: str = "D", days: int = 30) -> Dict[str, Any]:
    """
    Fetch OHLCV candles for a symbol from Finnhub.

    resolution: "D", "60", "30", etc.
    days: how many calendar days of history to pull back.
    """
    api_key = _require_api_key()

    # Finnhub wants UNIX timestamps (seconds)
    now = int(time.time())
    frm = now - days * 24 * 60 * 60

    url = f"{FINNHUB_BASE_URL}/stock/candle"
    params = {
        "symbol": symbol.upper(),
        "resolution": resolution,
        "from": frm,
        "to": now,
        "token": api_key,
    }

    resp = requests.get(url, params=params, timeout=10)
    if resp.status_code == 403:
        return {
            "symbol": symbol.upper(),
            "resolution": resolution,
            "days": days,
            "data": {
                "error": "You don't have access to this resource.",
                "http_status": 403,
            },
        }

    if not resp.ok:
        raise FinnhubError(
            f"Finnhub candles failed for {symbol} "
            f"({resp.status_code}): {resp.text[:200]}"
        )

    data = resp.json()
    status = data.get("s")
    if status != "ok":
        # Could be "no_data" or error – just pass it through so the API can show it
        return {
            "symbol": symbol.upper(),
            "resolution": resolution,
            "days": days,
            "data": data,
        }

    return {
        "symbol": symbol.upper(),
        "resolution": resolution,
        "days": days,
        "data": data,
    }















































