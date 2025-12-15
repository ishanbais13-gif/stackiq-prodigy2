import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional, List
import requests


class UpstreamAPIError(Exception):
    def __init__(self, message: str, status_code: int, payload: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.status_code = status_code
        self.payload = payload or {}


def _require_env(name: str) -> str:
    v = os.getenv(name)
    if not v or not v.strip():
        raise ValueError(f"Missing required environment variable: {name}")
    return v.strip()


@dataclass(frozen=True)
class AlpacaConfig:
    api_key: str
    secret_key: str
    base_url: str              # trading (paper/live) base with /v2
    data_base_url: str         # https://data.alpaca.markets
    data_feed: str             # iex or sip (sip needs paid)


def load_alpaca_config() -> AlpacaConfig:
    api_key = _require_env("ALPACA_API_KEY")
    secret_key = _require_env("ALPACA_SECRET_KEY")

    base_url = os.getenv("ALPACA_BASE_URL", "https://paper-api.alpaca.markets/v2").strip()
    data_base_url = os.getenv("ALPACA_DATA_BASE_URL", "https://data.alpaca.markets").strip()
    data_feed = (os.getenv("ALPACA_DATA_FEED", "iex").strip() or "iex").lower()

    return AlpacaConfig(
        api_key=api_key,
        secret_key=secret_key,
        base_url=base_url,
        data_base_url=data_base_url,
        data_feed=data_feed,
    )


def _headers(cfg: AlpacaConfig) -> Dict[str, str]:
    return {
        "APCA-API-KEY-ID": cfg.api_key,
        "APCA-API-SECRET-KEY": cfg.secret_key,
        "Accept": "application/json",
    }


def _request_json(method: str, url: str, headers: Dict[str, str], params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    r = requests.request(method=method, url=url, headers=headers, params=params, timeout=20)
    if r.status_code // 100 != 2:
        try:
            payload = r.json()
        except Exception:
            payload = {"text": r.text}
        raise UpstreamAPIError(f"Alpaca error {r.status_code}", r.status_code, payload)
    return r.json()


def get_latest_quote(cfg: AlpacaConfig, symbol: str) -> Dict[str, Any]:
    symbol = symbol.upper().strip()
    url = f"{cfg.data_base_url}/v2/stocks/{symbol}/quotes/latest"
    params = {"feed": cfg.data_feed}
    raw = _request_json("GET", url, _headers(cfg), params=params)

    q = raw.get("quote", {}) if isinstance(raw, dict) else {}
    ts = q.get("t") or datetime.now(timezone.utc).isoformat()

    return {
        "symbol": symbol,
        "bid": q.get("bp"),
        "ask": q.get("ap"),
        "timestamp": ts,
        "raw": raw,
    }


def get_bars(cfg: AlpacaConfig, symbol: str, timeframe: str = "1Day", days: int = 30, limit: int = 1000) -> Dict[str, Any]:
    """
    timeframe examples: 1Min, 5Min, 15Min, 1Hour, 1Day
    """
    symbol = symbol.upper().strip()
    timeframe = (timeframe or "1Day").strip()
    days = max(1, min(int(days), 365))
    limit = max(1, min(int(limit), 10000))

    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt.replace()  # copy
    # rough day window; Alpaca accepts RFC3339
    start_dt = end_dt - __import__("datetime").timedelta(days=days)

    url = f"{cfg.data_base_url}/v2/stocks/{symbol}/bars"
    params = {
        "timeframe": timeframe,
        "start": start_dt.isoformat().replace("+00:00", "Z"),
        "end": end_dt.isoformat().replace("+00:00", "Z"),
        "limit": limit,
        "feed": cfg.data_feed,
        "adjustment": "raw",
    }
    raw = _request_json("GET", url, _headers(cfg), params=params)

    bars = raw.get("bars") or []
    norm = []
    for b in bars:
        norm.append({
            "t": b.get("t"),
            "o": b.get("o"),
            "h": b.get("h"),
            "l": b.get("l"),
            "c": b.get("c"),
            "v": b.get("v"),
        })

    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "start": params["start"],
        "end": params["end"],
        "count": len(norm),
        "bars": norm,
    }


def get_news(cfg: AlpacaConfig, symbol: str, limit: int = 10) -> Dict[str, Any]:
    """
    Alpaca news endpoint (works for many accounts):
    GET https://data.alpaca.markets/v1beta1/news?symbols=AAPL&limit=10
    """
    symbol = symbol.upper().strip()
    limit = max(1, min(int(limit), 50))

    url = f"{cfg.data_base_url}/v1beta1/news"
    params = {"symbols": symbol, "limit": limit}
    raw = _request_json("GET", url, _headers(cfg), params=params)

    # raw often has {"news":[...], "next_page_token":...}
    items = raw.get("news") if isinstance(raw, dict) else None
    if items is None:
        items = raw if isinstance(raw, list) else []

    return {
        "symbol": symbol,
        "count": len(items),
        "items": items,
        "raw": raw,
    }


def simple_predict_from_bars(bars: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    V1: trend + momentum heuristic (clean + deterministic).
    """
    closes = [b.get("c") for b in bars if b.get("c") is not None]
    if len(closes) < 10:
        return {"direction": "neutral", "confidence": 0.0, "reason": "Not enough data"}

    short_ma = sum(closes[-5:]) / 5.0
    long_ma = sum(closes[-10:]) / 10.0
    momentum = closes[-1] - closes[-5]

    if short_ma > long_ma and momentum > 0:
        conf = min(abs(momentum) / max(closes[-1], 1e-9), 1.0)
        return {"direction": "bullish", "confidence": conf, "reason": "Uptrend + momentum"}

    if short_ma < long_ma and momentum < 0:
        conf = min(abs(momentum) / max(closes[-1], 1e-9), 1.0)
        return {"direction": "bearish", "confidence": conf, "reason": "Downtrend + momentum"}

    return {"direction": "neutral", "confidence": 0.3, "reason": "Mixed signals"}




























































