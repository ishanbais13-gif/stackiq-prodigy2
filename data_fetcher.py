import os
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone

load_dotenv()

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")

# Toggle sandbox via env: FINNHUB_SANDBOX=true
USE_SANDBOX = (os.getenv("FINNHUB_SANDBOX", "false").lower() == "true")
BASE_URL = "https://sandbox.finnhub.io/api/v1" if USE_SANDBOX else "https://finnhub.io/api/v1"

def get_quote(symbol: str):
    url = f"{BASE_URL}/quote"
    params = {"symbol": symbol, "token": FINNHUB_API_KEY}
    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()
    return r.json()

def get_candles(symbol: str, resolution="D", count=30):
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
    if r.status_code >= 400:
        # Bubble up the API’s message so we can see the reason in the response
        raise requests.HTTPError(f"{r.status_code} from Finnhub: {r.text}")
    return r.json()






































