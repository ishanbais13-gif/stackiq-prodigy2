import os
import requests
from datetime import datetime, timedelta

ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")

if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
    raise RuntimeError("Alpaca API keys not set")

HEADERS = {
    "APCA-API-KEY-ID": ALPACA_API_KEY,
    "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY
}

DATA_BASE_URL = "https://data.alpaca.markets"
PAPER_BASE_URL = "https://paper-api.alpaca.markets"

# -------------------------
# QUOTE
# -------------------------
def get_latest_quote(symbol: str):
    url = f"{DATA_BASE_URL}/v2/stocks/{symbol}/quotes/latest"
    r = requests.get(url, headers=HEADERS, timeout=10)
    r.raise_for_status()
    return r.json()["quote"]

# -------------------------
# DAILY CANDLES
# -------------------------
def get_daily_candles(symbol: str, days: int):
    end = datetime.utcnow()
    start = end - timedelta(days=days)

    params = {
        "timeframe": "1Day",
        "start": start.isoformat() + "Z",
        "end": end.isoformat() + "Z",
        "limit": days
    }

    url = f"{DATA_BASE_URL}/v2/stocks/{symbol}/bars"

    r = requests.get(url, headers=HEADERS, params=params, timeout=10)

    # 🔥 THIS prevents silent crashes
    if r.status_code == 403:
        raise Exception("403 Forbidden – Alpaca plan does not allow this data")
    if r.status_code == 429:
        raise Exception("Rate limit hit")
    r.raise_for_status()

    data = r.json()

    return data.get("bars", [])






















































