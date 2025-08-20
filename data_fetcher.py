import os, requests

FINNHUB_API_KEY = os.environ.get("FINNHUB_API_KEY")

def _need_key():
    return {"error": "FINNHUB_API_KEY not set in server env"}

def get_stock_price(ticker: str):
    if not FINNHUB_API_KEY:
        return _need_key()
    url = f"https://finnhub.io/api/v1/quote?symbol={ticker.upper()}&token={FINNHUB_API_KEY}"
    r = requests.get(url, timeout=15)
    return r.json()

def get_next_earnings(ticker: str):
    if not FINNHUB_API_KEY:
        return _need_key()
    # Next earnings (uses calendar; narrow range for speed)
    url = f"https://finnhub.io/api/v1/calendar/earnings?symbol={ticker.upper()}&token={FINNHUB_API_KEY}"
    r = requests.get(url, timeout=15)
    return r.json()
