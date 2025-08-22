# app.py
import os
import json
import time
from typing import Any, Dict, Optional

from fastapi import FastAPI, Query, Response
from fastapi.middleware.cors import CORSMiddleware

# ---- Import your fetcher (already in your repo) -----------------------------
# It should return a dict like:
# {
#   "ticker": "AAPL",
#   "price": {"c": 224.9, "d": -1.11, "dp": -0.49, "h": ..., "l": ..., "o": ..., "pc": ..., "v": ...},
#   "earnings": {...}
# }
from data_fetcher import get_stock_data  # <-- keep this import

# ---- App metadata -----------------------------------------------------------
SERVICE_NAME = "stackiq-web"
VERSION = "0.2.0"
START_TS = time.time()

app = FastAPI(title="StackIQ API", version=VERSION)

# CORS (safe default: allow browser calls from anywhere)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---- Helpers ----------------------------------------------------------------
def json_response(payload: Dict[str, Any], pretty: bool = False, status: int = 200) -> Response:
    """Return JSON in the exact format the frontend wants."""
    if pretty:
        body = json.dumps(payload, indent=2, ensure_ascii=False)
    else:
        body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    return Response(content=body, status_code=status, media_type="application/json")


def normalize_price(price: Dict[str, Any]) -> Dict[str, Optional[float]]:
    """
    Convert short vendor keys to the stable keys the frontend expects.
    - c: current
    - d: change
    - dp: percent_change
    - h: high
    - l: low
    - o: open
    - pc: prev_close
    - v: volume
    Unknown/missing values come back as None (frontend can handle that).
    """
    if not isinstance(price, dict):
        price = {}

    return {
        "current": price.get("c"),
        "change": price.get("d"),
        "percent_change": price.get("dp"),
        "high": price.get("h"),
        "low": price.get("l"),
        "open": price.get("o"),
        "prev_close": price.get("pc"),
        "volume": price.get("v"),
    }


def normalize_payload(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build the final object the web UI reads.
    Keeps earnings as-is, but ensures `price` has the stable keys.
    """
    if not isinstance(raw, dict):
        return {"error": "bad_response", "status": "error"}

    ticker = raw.get("ticker") or raw.get("symbol")
    earnings = raw.get("earnings", {})

    # Some fetchers might nest as {"quote": {...}}; prefer raw["price"], then raw["quote"]
    price_obj = raw.get("price") or raw.get("quote") or {}

    return {
        "ticker": ticker,
        "price": normalize_price(price_obj),
        "earnings": earnings,
    }


# ---- Routes -----------------------------------------------------------------
@app.get("/", include_in_schema=False)
def root() -> Response:
    # Keep the tiny text page you were seeing before
    return Response(content="StackIQ backend is live.", media_type="text/plain")


@app.get("/health")
def health(pretty: bool = Query(default=False)) -> Response:
    return json_response({"ok": True, "service": SERVICE_NAME}, pretty=pretty)


@app.get("/version")
def version(pretty: bool = Query(default=False)) -> Response:
    return json_response({"version": VERSION}, pretty=pretty)


@app.get("/status")
def status(pretty: bool = Query(default=False)) -> Response:
    uptime = int(time.time() - START_TS)
    return json_response({"app": "StackIQ", "status": "ok", "uptime_seconds": uptime, "version": VERSION}, pretty=pretty)


@app.get("/envcheck")
def envcheck(pretty: bool = Query(default=False)) -> Response:
    # We just verify the API key variable exists (name can match your fetcher)
    has_key = bool(os.getenv("FINNHUB_API_KEY") or os.getenv("STOCK_API_KEY") or os.getenv("API_KEY"))
    return json_response({"has_key": has_key}, pretty=pretty)


@app.get("/raw/{ticker}")
def raw_ticker(
    ticker: str,
    pretty: bool = Query(default=False),
) -> Response:
    """
    Useful for debugging. Returns the unmodified fetcher payload.
    """
    data = get_stock_data(ticker.strip().upper())
    status = 200 if isinstance(data, dict) and "error" not in data else 500
    return json_response(data, pretty=pretty, status=status)


@app.get("/test/{ticker}")
def test_ticker(
    ticker: str,
    pretty: bool = Query(default=False),
) -> Response:
    """
    Main endpoint the web UI calls.
    Fetch, normalize, and return stable keys for price/earnings.
    """
    raw = get_stock_data(ticker.strip().upper())

    # Surface upstream errors directly
    if not isinstance(raw, dict):
        return json_response({"error": "bad_response_from_fetcher", "status": "error"}, pretty=pretty, status=500)
    if "error" in raw:
        # keep original error message
        return json_response(raw, pretty=pretty, status=500)

    normalized = normalize_payload(raw)

    # If price normalization produced all Nones, tell the UI clearly
    price_obj = normalized.get("price") or {}
    if all(price_obj.get(k) is None for k in ["current", "change", "percent_change", "high", "low", "open", "prev_close", "volume"]):
        return json_response({"error": "price", "status": "invalid_price_payload"}, pretty=pretty, status=500)

    return json_response(normalized, pretty=pretty, status=200)


# Optional: run locally
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")), reload=False)




