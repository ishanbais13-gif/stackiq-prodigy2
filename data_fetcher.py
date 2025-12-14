import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple
import requests


class UpstreamAPIError(Exception):
    """Raised when Alpaca returns a non-2xx response."""
    def __init__(self, message: str, status_code: int, payload: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.status_code = status_code
        self.payload = payload or {}


@dataclass
class AlpacaConfig:
    api_key: str
    secret_key: str
    data_base_url: str
    trading_base_url: str
    base_url: str
    feed: str
    timeout_seconds: float = 12.0
    retries: int = 2


def _require_env(name: str) -> str:
    val = os.getenv(name, "").strip()
    if not val:
        raise ValueError(f"Missing required environment variable: {name}")
    return val


def load_config() -> AlpacaConfig:
    """
    Uses the env vars you already set in Azure:
    - ALPACA_API_KEY
    - ALPACA_SECRET_KEY
    - ALPACA_DATA_BASE_URL (ex: https://data.alpaca.markets)
    - ALPACA_TRADING_BASE_URL (ex: https://paper-api.alpaca.markets)
    - ALPACA_BASE_URL (ex: https://paper-api.alpaca.markets/v2)  [optional but supported]
    - ALPACA_DATA_FEED (iex or sip)
    """
    api_key = _require_env("ALPACA_API_KEY")
    secret_key = _require_env("ALPACA_SECRET_KEY")

    data_base = os.getenv("ALPACA_DATA_BASE_URL", "https://data.alpaca.markets").strip()
    trading_base = os.getenv("ALPACA_TRADING_BASE_URL", "https://paper-api.alpaca.markets").strip()
    base_url = os.getenv("ALPACA_BASE_URL", "https://paper-api.alpaca.markets/v2").strip()
    feed = os.getenv("ALPACA_DATA_FEED", "iex").strip() or "iex"

    return AlpacaConfig(
        api_key=api_key,
        secret_key=secret_key,
        data_base_url=data_base,
        trading_base_url=trading_base,
        base_url=base_url,
        feed=feed,
    )


def _headers(cfg: AlpacaConfig) -> Dict[str, str]:
    return {
        "APCA-API-KEY-ID": cfg.api_key,
        "APCA-API-SECRET-KEY": cfg.secret_key,
        "Accept": "application/json",
    }


def _request_json(
    cfg: AlpacaConfig,
    method: str,
    url: str,
    params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Small retry wrapper for Alpaca calls.
    Retries on timeouts, 429, and 5xx.
    """
    last_exc: Optional[Exception] = None
    for attempt in range(cfg.retries + 1):
        try:
            resp = requests.request(
                method=method,
                url=url,
                headers=_headers(cfg),
                params=params,
                timeout=cfg.timeout_seconds,
            )

            # Retry-worthy statuses
            if resp.status_code in (429, 500, 502, 503, 504) and attempt < cfg.retries:
                time.sleep(0.6 * (attempt + 1))
                continue

            if not (200 <= resp.status_code < 300):
                try:
                    payload = resp.json()
                except Exception:
                    payload = {"text": resp.text}
                raise UpstreamAPIError(
                    message=f"Alpaca API error {resp.status_code} for {url}",
                    status_code=resp.status_code,
                    payload=payload,
                )

            return resp.json()

        except (requests.Timeout, requests.ConnectionError) as e:
            last_exc = e
            if attempt < cfg.retries:
                time.sleep(0.6 * (attempt + 1))
                continue
            raise UpstreamAPIError(
                message=f"Network error contacting Alpaca: {type(e).__name__}",
                status_code=503,
                payload={"error": str(e)},
            )

    # Should never reach
    raise UpstreamAPIError(
        message="Unknown Alpaca request failure",
        status_code=503,
        payload={"error": str(last_exc) if last_exc else "unknown"},
    )


def get_latest_quote(symbol: str) -> Dict[str, Any]:
    """
    Returns a normalized quote payload:
    {
      "symbol": "AAPL",
      "bid": 123.45,
      "ask": 123.46,
      "bid_size": 100,
      "ask_size": 200,
      "timestamp": "2025-12-14T18:00:00Z",
      "raw": {...}
    }
    """
    cfg = load_config()
    sym = symbol.upper().strip()

    # Alpaca Market Data v2: latest quote
    url = f"{cfg.data_base_url}/v2/stocks/{sym}/quotes/latest"
    params = {"feed": cfg.feed}

    raw = _request_json(cfg, "GET", url, params=params)
    quote = raw.get("quote") or {}

    # Fields vary slightly, but these are common:
    bid = quote.get("bp")  # bid price
    ask = quote.get("ap")  # ask price
    bid_size = quote.get("bs")
    ask_size = quote.get("as")
    ts = quote.get("t")

    return {
        "symbol": sym,
        "bid": bid,
        "ask": ask,
        "bid_size": bid_size,
        "ask_size": ask_size,
        "timestamp": ts,
        "raw": raw,
    }


def _iso_utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def get_bars(
    symbol: str,
    timeframe: str = "1Day",
    days: int = 30,
    limit: int = 200,
) -> Dict[str, Any]:
    """
    Returns Alpaca bars (candles) for the last N days.

    Query notes:
    - timeframe examples: 1Min, 5Min, 15Min, 1Hour, 1Day
    - We default to 'raw' adjustment (stable for simple charts).
    """
    cfg = load_config()
    sym = symbol.upper().strip()

    # Compute start/end as RFC3339-ish ISO
    now = datetime.now(timezone.utc)
    start = now.timestamp() - (max(days, 1) * 86400)
    start_dt = datetime.fromtimestamp(start, tz=timezone.utc).isoformat().replace("+00:00", "Z")
    end_dt = _iso_utc_now()

    url = f"{cfg.data_base_url}/v2/stocks/{sym}/bars"
    params = {
        "timeframe": timeframe,
        "start": start_dt,
        "end": end_dt,
        "limit": max(1, min(limit, 10000)),
        "feed": cfg.feed,
        "adjustment": "raw",
    }

    raw = _request_json(cfg, "GET", url, params=params)

    # Normalize bars into a super predictable format
    bars = raw.get("bars") or []
    norm = []
    for b in bars:
        norm.append({
            "t": b.get("t"),  # timestamp
            "o": b.get("o"),
            "h": b.get("h"),
            "l": b.get("l"),
            "c": b.get("c"),
            "v": b.get("v"),
        })

    return {
        "symbol": sym,
        "timeframe": timeframe,
        "start": start_dt,
        "end": end_dt,
        "count": len(norm),
        "bars": norm,
        "raw": raw,
    }



























































