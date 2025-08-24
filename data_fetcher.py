import os
import time
import logging
from typing import Any, Dict, List
import requests

log = logging.getLogger("stackiq.fetcher")

class FinnhubError(Exception):
    """Raised for any upstream data error we want to surface to the API."""
    pass

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
FINNHUB_BASE = "https://finnhub.io/api/v1"

# A small, reusable HTTP helper
def _get(url: str, params: Dict[str, Any] | None = None, *, timeout: int = 10) -> Dict[str, Any]:
    if not FINNHUB_API_KEY:
        raise FinnhubError("FINNHUB_API_KEY is not set")

    params = dict(params or {})
    params["token"] = FINNHUB_API_KEY

    try:
        r = requests.get(
            url,
            params=params,
            timeout=timeout,
            headers={"User-Agent": "stackiq/1.0 (+https://example.com)"},
        )
    except requests.RequestException as e:
        raise FinnhubError(f"Network error: {e}") from e

    # 429 = rate limit, 401/403 = auth, others = general API failure
    if r.status_code == 429:
        raise FinnhubError("rate-limited by Finnhub (HTTP 429)")
    if r.status_code in (401, 403):
        raise FinnhubError(f"Unauthorized from Finnhub (HTTP {r.status_code})")
    if r.status_code >= 400:
        raise FinnhubError(f"Upstream error from Finnhub (HTTP {r.status_code})")

    try:
        data = r.json()
    except ValueError as e:
        raise FinnhubError(f"Invalid JSON from Finnhub: {e}") from e

    return data

def fetch_quote(symbol: str) -> Dict[str, Any]:
    """
    Returns the latest quote using Finnhub /quote endpoint.
    https://finnhub.io/docs/api/quote
    """
    sym = symbol.upper().strip()
    if not sym:
        raise FinnhubError("Empty symbol")

    url = f"{FINNHUB_BASE}/quote"
    data = _get(url, {"symbol": sym})

    # Finnhub returns { c, h, l, o, pc, dp } etc.
    if not isinstance(data, dict) or "c" not in data:
        raise FinnhubError("Unexpected quote payload from Finnhub")

    return {
        "current": data.get("c"),
        "high": data.get("h"),
        "low": data.get("l"),
        "open": data.get("o"),
        "prev_close": data.get("pc"),
        "percent_change": data.get("dp"),
        "raw": data,
    }

def fetch_earnings(symbol: str) -> Dict[str, Any]:
    """
    Returns recent EPS calendar using Finnhub /stock/earnings.
    https://finnhub.io/docs/api/earnings-calendar
    """
    sym = symbol.upper().strip()
    if not sym:
        raise FinnhubError("Empty symbol")

    url = f"{FINNHUB_BASE}/stock/earnings"
    data = _get(url, {"symbol": sym, "limit": 8})

    # Expected: a list of earnings objects
    if not isinstance(data, list):
        # Some accounts return {"data":[...]} – handle both shapes safely.
        if isinstance(data, dict) and isinstance(data.get("data"), list):
            rows = data["data"]
        else:
            raise FinnhubError("Unexpected earnings payload from Finnhub")
    else:
        rows = data

    # Normalize a tiny summary
    items: List[Dict[str, Any]] = []
    for row in rows:
        items.append(
            {
                "period": row.get("period") or row.get("quarter") or row.get("date"),
                "epsActual": row.get("actual") or row.get("epsActual"),
                "epsEstimate": row.get("estimate") or row.get("epsEstimate"),
                "surprise": row.get("surprise"),
            }
        )

    return {"count": len(items), "items": items[:8], "raw": rows}

def get_quote_and_earnings(symbol: str) -> Dict[str, Any]:
    quote = fetch_quote(symbol)
    earnings = fetch_earnings(symbol)
    return {"quote": quote, "earnings": earnings}








