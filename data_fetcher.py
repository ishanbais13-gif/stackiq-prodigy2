import os
import time
from typing import Optional, Dict, Any

import requests

# Finnhub config
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
BASE_URL = "https://finnhub.io/api/v1"


def _request(path: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    """
    Low-level helper to call Finnhub with the API key attached.

    Returns:
      - dict with Finnhub JSON on success (including error payloads like {"error": ...})
      - None only on hard failures (no API key, network error, JSON parse error)
    """
    if params is None:
        params = {}

    if not FINNHUB_API_KEY:
        print("ERROR: FINNHUB_API_KEY is not set in environment")
        return None

    params["token"] = FINNHUB_API_KEY
    url = f"{BASE_URL}{path}"

    try:
        resp = requests.get(url, params=params, timeout=10)
    except Exception as e:
        print(f"Finnhub request error: {e}")
        return None

    # Even if status is not 200, try to return the JSON body so the caller can see the error
    try:
        data = resp.json()
    except Exception as e:
        print(f"Finnhub JSON parse error (status {resp.status_code}): {e}")
        return None

    if resp.status_code != 200:
        # Attach HTTP status so we can see what went wrong
        data.setdefault("http_status", resp.status_code)
        print(f"Finnhub HTTP {resp.status_code}: {data}")

    return data


def get_quote(symbol: str) -> Optional[Dict[str, Any]]:
    """
    Get real-time quote for a symbol from Finnhub.

    Returns a dict like:
      {
        "c": 261.74,  # current price
        "h": 263.31,  # high of day
        "l": 260.68,  # low of day
        "o": 261.07,  # open
        "pc": 259.45, # previous close
        "t": 1582641000
      }
    or None on hard failure (missing key, network/JSON error).
    """
    symbol = symbol.upper()
    return _request("/quote", {"symbol": symbol})


def get_candles(
    symbol: str,
    resolution: str = "D",
    days: int = 30
) -> Optional[Dict[str, Any]]:
    """
    Get OHLC candles for the last `days` days for a given symbol.

    resolution options (Finnhub): "1","5","15","30","60","D","W","M"

    Returns:
      - Finnhub candle payload (whatever Finnhub sends), e.g.
        {
          "s": "ok",
          "c": [...],  # close
          "o": [...],  # open
          "h": [...],  # high
          "l": [...],  # low
          "t": [...],  # unix timestamps
          "v": [...]   # volume (sometimes)
        }
      - or a dict with Finnhub error info if the API rejected it
      - or None only on hard failure (missing key, network/JSON error)
    """
    if days <= 0:
        raise ValueError("days must be a positive integer")

    symbol = symbol.upper()
    now = int(time.time())
    frm = now - days * 24 * 60 * 60  # N days ago

    params = {
        "symbol": symbol,
        "resolution": resolution,
        "from": frm,
        "to": now,
    }

    return _request("/stock/candle", params)














































