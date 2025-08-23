import os
import httpx
from datetime import datetime, timedelta

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")  # set this in Azure App Settings

BASE = "https://finnhub.io/api/v1"

# Simple in-memory cache to avoid rate limits during quick tests
_cache: dict[str, dict] = {}
_cache_ttl = timedelta(seconds=30)

def _cache_key(ticker: str) -> str:
    return f"quote:{ticker}"

def _cache_get(key: str):
    entry = _cache.get(key)
    if not entry:
        return None
    if datetime.utcnow() > entry["exp"]:
        _cache.pop(key, None)
        return None
    return entry["val"]

def _cache_set(key: str, val: dict, ttl=_cache_ttl):
    _cache[key] = {"val": val, "exp": datetime.utcnow() + ttl}

async def _get_json(url: str, params: dict) -> dict | None:
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(url, params=params)
            if r.status_code != 200:
                return None
            return r.json()
    except Exception:
        return None

async def _get_quote(ticker: str) -> dict | None:
    """
    Finnhub /quote returns:
      c: current, d: change, dp: percent, h: high, l: low, o: open, pc: prev close
    We’ll also add 'v' (volume) if available (Finnhub has /stock/metric, but
    to keep it simple we’ll omit volume if we can’t fetch it).
    """
    if not FINNHUB_API_KEY:
        return None

    ck = _cache_key(ticker)
    cached = _cache_get(ck)
    if cached:
        return cached

    quote = await _get_json(f"{BASE}/quote", {"symbol": ticker, "token": FINNHUB_API_KEY})
    if not quote or not quote.get("c"):
        return None

    # Volume isn't in /quote; try /scan/technical? Keep it simple: omit volume if not available.
    price = {
        "c": quote.get("c"),
        "d": quote.get("d"),
        "dp": quote.get("dp"),
        "h": quote.get("h"),
        "l": quote.get("l"),
        "o": quote.get("o"),
        "pc": quote.get("pc"),
        "v": None,  # placeholder; UI tolerates missing/empty
    }
    _cache_set(ck, price)
    return price

async def _get_earnings_calendar(ticker: str) -> list:
    """
    Keep UI happy: return an array (can be empty if unavailable).
    Finnhub has /calendar/earnings but may require plan; if it fails, return [].
    """
    if not FINNHUB_API_KEY:
        return []

    # Try earnings calendar; if it fails, return []
    data = await _get_json(f"{BASE}/calendar/earnings", {"symbol": ticker, "token": FINNHUB_API_KEY})
    items = []
    try:
        # Finnhub response shape: {"earningsCalendar":[{date, epsActual, epsEstimate, hour, quarter, year, revenueActual, revenueEstimate, ...}]}
        # If different or missing, we normalize lightly.
        cal = data.get("earningsCalendar") if isinstance(data, dict) else None
        if isinstance(cal, list):
            for row in cal:
                items.append({
                    "date": row.get("date"),
                    "epsActual": row.get("epsActual"),
                    "epsEstimate": row.get("epsEstimate"),
                    "hour": row.get("hour"),
                    "quarter": row.get("quarter"),
                    "year": row.get("year"),
                    "revenueActual": row.get("revenueActual"),
                    "revenueEstimate": row.get("revenueEstimate"),
                    "symbol": ticker,
                })
    except Exception:
        pass
    return items

async def get_price_and_earnings(ticker: str) -> dict | None:
    """
    Returns a dict shaped exactly the way the front-end expects.
    """
    price = await _get_quote(ticker)
    if not price:
        return None

    earnings_list = await _get_earnings_calendar(ticker)
    return {
        "ticker": ticker,
        "price": price,
        "earnings": {"earningsCalendar": earnings_list},
    }


