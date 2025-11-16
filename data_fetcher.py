import os
import time
from datetime import datetime, timedelta

import requests

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
BASE_URL = "https://finnhub.io/api/v1"


def _get(path: str, params: dict | None = None):
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

    if resp.status_code != 200:
        print(f"Finnhub HTTP {resp.status_code}: {resp.text}")
        return None

    try:
        return resp.json()
    except Exception as e:
        print(f"Finnhub JSON parse error: {e}")
        return None


def get_quote(symbol: str):
    symbol = symbol.upper()
    return _get("/quote", {"symbol": symbol})


def get_candles(symbol: str, resolution: str = "D", days: int = 30):
    if days <= 0:
        raise ValueError("days must be a positive integer")

    symbol = symbol.upper()
    now = int(time.time())
    frm = now - days * 24 * 60 * 60

    params = {
        "symbol": symbol,
        "resolution": resolution,
        "from": frm,
        "to": now,
    }

    data = _get("/stock/candle", params)

    if not data or data.get("s") != "ok":
        print(f"Finnhub candle response not ok for {symbol}: {data}")
        return None

    return data












































