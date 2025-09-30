import os
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone

load_dotenv()

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "")

# Toggle sandbox by setting FINNHUB_SANDBOX=true in Azure App Settings
USE_SANDBOX = os.getenv("FINNHUB_SANDBOX", "false").lower() == "true"
BASE_URL = "https://sandbox.finnhub.io/api/v1" if USE_SANDBOX else "https://finnhub.io/api/v1"

def _raise_if_error(r: requests.Response):
    if r.status_code >= 400:
        # Bubble up Finnhub's message so app.py can show a friendly reason
        raise requests.HTTPError(f"{r.status_code} from Finnhub: {r.text}")

def get_quote(symbol: str):
    url = f"{BASE_URL}/quote"
    params = {"symbol": symbol, "token": FINNHUB_API_KEY}
    r = requests.get(url, params=params, timeout=15)
    _raise_if_error(r)
    return r.json()

def get_candles(symbol: str, resolution="D", count=30):
    # Build from/to window with buffer for weekends/holidays
    now = datetime.now(timezone.utc)
    if resolution == "D":
        delta = timedelta(days=count * 2)
    else:
        try:
            minutes = int(resolution)
            delta = timedelta(minutes=minutes * count * 2)
        except ValueError:
            delta = timedelta(days=count * 2)

    to_ts = int(now.timestamp())
    from_ts = int((now - delta).timestamp())

    url = f"{BASE_URL}/stock/candle"
    params = {
        "symbol": symbol,
        "resolution": resolution,
        "from": from_ts,
        "to": to_ts,
        "token": FINNHUB_API_KEY
    }
    r = requests.get(url, params=params, timeout=20)
    _raise_if_error(r)
    return r.json()







































