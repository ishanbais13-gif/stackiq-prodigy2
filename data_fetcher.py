import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional
import requests


# ---------- Errors ----------
class UpstreamAPIError(Exception):
    def __init__(self, message: str, status_code: int, payload=None):
        super().__init__(message)
        self.status_code = status_code
        self.payload = payload or {}


# ---------- Config ----------
@dataclass
class AlpacaConfig:
    api_key: str
    secret_key: str
    data_base_url: str
    trading_base_url: str
    feed: str
    timeout: float = 12.0
    retries: int = 2


def _require_env(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise ValueError(f"Missing env var: {name}")
    return val


def load_config() -> AlpacaConfig:
    return AlpacaConfig(
        api_key=_require_env("ALPACA_API_KEY"),
        secret_key=_require_env("ALPACA_SECRET_KEY"),
        data_base_url=os.getenv("ALPACA_DATA_BASE_URL", "https://data.alpaca.markets"),
        trading_base_url=os.getenv("ALPACA_TRADING_BASE_URL", "https://paper-api.alpaca.markets"),
        feed=os.getenv("ALPACA_DATA_FEED", "iex"),
    )


def _headers(cfg: AlpacaConfig) -> Dict[str, str]:
    return {
        "APCA-API-KEY-ID": cfg.api_key,
        "APCA-API-SECRET-KEY": cfg.secret_key,
        "Accept": "application/json",
    }


def _request_json(cfg: AlpacaConfig, method: str, url: str, params=None) -> Dict[str, Any]:
    for attempt in range(cfg.retries + 1):
        try:
            r = requests.request(
                method,
                url,
                headers=_headers(cfg),
                params=params,
                timeout=cfg.timeout,
            )

            if r.status_code in (429, 500, 502, 503) and attempt < cfg.retries:
                time.sleep(0.6 * (attempt + 1))
                continue

            if not r.ok:
                raise UpstreamAPIError(
                    f"Alpaca error {r.status_code}",
                    r.status_code,
                    r.text,
                )

            return r.json()

        except requests.RequestException as e:
            if attempt >= cfg.retries:
                raise UpstreamAPIError("Network error", 503, str(e))


# ---------- Market Data ----------
def get_latest_quote(symbol: str) -> Dict[str, Any]:
    cfg = load_config()
    symbol = symbol.upper()

    url = f"{cfg.data_base_url}/v2/stocks/{symbol}/quotes/latest"
    raw = _request_json(cfg, "GET", url, {"feed": cfg.feed})

    q = raw.get("quote", {})
    return {
        "symbol": symbol,
        "bid": q.get("bp"),
        "ask": q.get("ap"),
        "timestamp": q.get("t"),
        "raw": raw,
    }


def get_bars(symbol: str, timeframe="1Day", days=30, limit=200) -> Dict[str, Any]:
    cfg = load_config()
    symbol = symbol.upper()

    now = datetime.now(timezone.utc)
    start = now.timestamp() - days * 86400

    params = {
        "timeframe": timeframe,
        "start": datetime.fromtimestamp(start, timezone.utc).isoformat().replace("+00:00", "Z"),
        "end": now.isoformat().replace("+00:00", "Z"),
        "limit": min(limit, 10000),
        "feed": cfg.feed,
        "adjustment": "raw",
    }

    url = f"{cfg.data_base_url}/v2/stocks/{symbol}/bars"
    raw = _request_json(cfg, "GET", url, params)

    bars = raw.get("bars", [])
    norm = [{"t": b["t"], "o": b["o"], "h": b["h"], "l": b["l"], "c": b["c"], "v": b["v"]} for b in bars]

    return {"symbol": symbol, "bars": norm, "count": len(norm)}


# ---------- Prediction ----------
def simple_predict_from_bars(bars: list) -> dict:
    if len(bars) < 10:
        return {"direction": "neutral", "confidence": 0.0, "reason": "Not enough data"}

    closes = [b["c"] for b in bars if b.get("c") is not None]
    if len(closes) < 10:
        return {"direction": "neutral", "confidence": 0.0, "reason": "Insufficient data"}

    short_ma = sum(closes[-5:]) / 5
    long_ma = sum(closes[-10:]) / 10
    momentum = closes[-1] - closes[-5]

    if short_ma > long_ma and momentum > 0:
        return {
            "direction": "bullish",
            "confidence": min(abs(momentum) / closes[-1], 1.0),
            "reason": "Uptrend + momentum",
        }

    if short_ma < long_ma and momentum < 0:
        return {
            "direction": "bearish",
            "confidence": min(abs(momentum) / closes[-1], 1.0),
            "reason": "Downtrend + momentum",
        }

    return {"direction": "neutral", "confidence": 0.3, "reason": "Mixed signals"}



























































