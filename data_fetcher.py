import os
from typing import Any, Dict, Optional, List
import requests

BASE = "https://finnhub.io/api/v1"
API_KEY = os.getenv("FINNHUB_API_KEY")
TIMEOUT = 10


class FinnhubError(Exception):
    """Raised when there is a configuration / Finnhub-side error."""
    pass


def _need_key() -> None:
    if not API_KEY:
        # Surface a clear server/config error (mapped to 503 by the API)
        raise FinnhubError("FINNHUB_API_KEY is missing on the server")


def _get(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Small helper around requests.get with key + timeout + raise_for_status."""
    _need_key()
    all_params = {**(params or {}), "token": API_KEY}
    r = requests.get(f"{BASE}/{path}", params=all_params, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()


def fetch_quote(ticker: str) -> Optional[Dict[str, Any]]:
    t = (ticker or "").upper().strip()
    if not t:
        return None
    q = _get("quote", {"symbol": t})
    # finnhub returns c=0 when no recent price / not found
    if q is None or "c" not in q or q.get("c") in (None, 0):
        return None
    return q


def fetch_earnings(ticker: str, from_="2024-01-01", to_="2026-12-31") -> Dict[str, Any]:
    t = (ticker or "").upper().strip()
    if not t:
        return {"earningsCalendar": []}
    return _get("calendar/earnings", {"symbol": t, "from": from_, "to": to_}) or {"earningsCalendar": []}


def normalize_quote(q: Dict[str, Any]) -> Dict[str, Any]:
    """Keep keys stable for the UI."""
    return {
        "c": q.get("c"),   # current
        "d": q.get("d"),   # change
        "dp": q.get("dp"), # % change
        "h": q.get("h"),   # high
        "l": q.get("l"),   # low
        "o": q.get("o"),   # open
        "pc": q.get("pc"), # prev close
    }


def get_quote_and_earnings(ticker: str) -> Optional[Dict[str, Any]]:
    t = (ticker or "").upper().strip()
    if not t:
        return None

    q = fetch_quote(t)
    if not q:
        return None

    e = fetch_earnings(t)
    return {
        "ticker": t,
        "price": normalize_quote(q),
        "earnings": e or {"earningsCalendar": []},
    }



