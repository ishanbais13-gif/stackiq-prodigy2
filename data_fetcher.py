import os
import requests

BASE = "https://finnhub.io/api/v1"
API_KEY = os.getenv("FINNHUB_API_KEY")

class FinnhubError(Exception):
    pass

def _get(path: str, params: dict) -> dict:
    if not API_KEY:
        raise FinnhubError("FINNHUB_API_KEY is missing on the server")
    all_params = {**(params or {}), "token": API_KEY}
    r = requests.get(f"{BASE}/{path}", params=all_params, timeout=10)
    r.raise_for_status()
    return r.json()

def get_quote_and_earnings(ticker: str) -> dict | None:
    """Return {"ticker":..., "price": {...}, "earnings": {...}} or None if no quote."""
    t = (ticker or "").upper().strip()
    if not t:
        return None

    # Quote
    q = _get("quote", {"symbol": t})
    # Finnhub returns c=0 when it can’t find or market closed with no last price
    if q is None or "c" not in q or q.get("c") in (None, 0):
        return None

    # Earnings window (wide range so something is returned)
    e = _get("calendar/earnings", {"symbol": t, "from": "2024-01-01", "to": "2026-12-31"})

    return {
        "ticker": t,
        "price": {
            "c": q.get("c"),
            "d": q.get("d"),
            "dp": q.get("dp"),
            "h": q.get("h"),
            "l": q.get("l"),
            "o": q.get("o"),
            "pc": q.get("pc"),
        },
        "earnings": e or {"earningsCalendar": []},
    }

    }



