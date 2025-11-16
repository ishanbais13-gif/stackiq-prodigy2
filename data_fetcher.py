import os
import time
import requests

# Read Finnhub API key from environment (set in Azure App Service > Configuration)
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
BASE_URL = "https://finnhub.io/api/v1"


def _call_finnhub(path: str, params: dict | None = None):
    """Low-level helper to call Finnhub with the API key attached."""
    if params is None:
        params = {}

    if not FINNHUB_API_KEY:
        print("ERROR: FINNHUB_API_KEY is not set in environment")
        return None

    # Attach token
    params["token"] = FINNHUB_API_KEY
    url = f"{BASE_URL}{path}"

    try:
        resp = requests.get(url, params=params, timeout=10)
    except Exception as e:
        print(f"Finnhub request error: {e}")
        return None

    if resp.status_code != 200:
        print(f"Finnhub HTTP {resp.status_code}: {resp.text}")
        return None

    try:
        data = resp.json()
    except Exception as e:
        print(f"Finnhub JSON parse error: {e}")
        return None

    return data


def get_quote(symbol: str):
    """
    Get real-time quote for a symbol from Finnhub.

    Returns a dict like:
    {
      "c": 261.74,  # Current price
      "h": 263.31,  # High price of the day
      "l": 260.68,  # Low price of the day
      "o": 261.07,  # Open price of the day
      "pc": 259.45, # Previous close price
      "t": 1582641000
    }
    or None on failure.
    """
    symbol = symbol.upper()
    data = _call_finnhub("/quote", {"symbol": symbol})

    if data is None:
        return None

    # Basic sanity check: if everything is zero, it's probably bad symbol / no data
    if all(str(data.get(k, 0)) in ("0", "0.0", "None") for k in ("c", "h", "l", "o", "pc")):
        print(f"Warning: quote for {symbol} looks empty: {data}")

    return data


def get_candles(symbol: str, resolution: str = "D", days: int = 30):
    """
    Get OHLC candles for the last `days` days for a given symbol.

    resolution: "1", "5", "15", "30", "60", "D", "W", "M"
    Returns the raw Finnhub candle payload on success, or None on failure.

    Successful response looks like:
    {
      "c": [...],  # close
      "h": [...],  # high
      "l": [...],  # low
      "o": [...],  # open
      "s": "ok",
      "t": [...],  # unix timestamps
      "v": [...]   # volume (sometimes)
    }
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

    data = _call_finnhub("/stock/candle", params)

    if data is None:
        # _call_finnhub already logged the issue
        return None

    status = data.get("s")
    if status == "ok":
        return data

    # Finnhub sometimes returns "no_data" if the range is wrong / symbol invalid
    print(f"Finnhub candle status for {symbol}: {status}, payload: {data}")
    return None













































