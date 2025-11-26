import os
import time
from typing import List, Dict, Any, Optional

import requests

ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET") or os.getenv("ALPACA_SECRET_KEY")
ALPACA_BASE_URL = os.getenv("ALPACA_BASE_URL", "https://paper-api.alpaca.markets")
ALPACA_DATA_URL = "https://data.alpaca.markets"


if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
    # We don't raise here so /health still works, but endpoints will error with a clear message.
    print("WARNING: ALPACA_API_KEY or ALPACA_SECRET_KEY is not set in environment variables.")


def _alpaca_get(base: str, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Low-level helper to GET from Alpaca with auth headers and basic error handling."""
    url = base.rstrip("/") + path
    headers = {
        "APCA-API-KEY-ID": ALPACA_API_KEY or "",
        "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY or "",
    }
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=10)
    except requests.RequestException as e:
        raise RuntimeError(f"Network error talking to Alpaca: {e}")

    if resp.status_code != 200:
        raise RuntimeError(
            f"Alpaca error {resp.status_code}: {resp.text[:300]}"
        )
    try:
        return resp.json()
    except ValueError:
        raise RuntimeError("Failed to decode JSON from Alpaca response")


# ---------- QUOTE / SNAPSHOT ----------


def get_quote(symbol: str) -> Dict[str, Any]:
    """
    Use Alpaca snapshot endpoint to build a Finnhub-style quote:
    {
        "symbol": "AAPL",
        "current": 271.4,
        "open": ...,
        "high": ...,
        "low": ...,
        "previous_close": ...,
        "change": ...,
        "percent_change": ...,
        "timestamp": 1234567890
    }
    """
    symbol = symbol.upper()
    data = _alpaca_get(ALPACA_BASE_URL, f"/v2/stocks/{symbol}/snapshot")

    daily = data.get("dailyBar") or {}
    prev = data.get("prevDailyBar") or {}
    latest_trade = data.get("latestTrade") or {}

    current = latest_trade.get("p") or daily.get("c")
    open_price = daily.get("o")
    high = daily.get("h")
    low = daily.get("l")
    prev_close = prev.get("c")

    change = None
    percent_change = None
    if current is not None and prev_close not in (None, 0):
        change = current - prev_close
        percent_change = (change / prev_close) * 100

    # latestTrade.t is RFC3339 string; we just convert to epoch seconds best-effort
    timestamp = None
    t_str = latest_trade.get("t")
    if isinstance(t_str, str):
        # very rough: we just use "now" if parsing is annoying
        timestamp = int(time.time())

    return {
        "symbol": symbol,
        "current": float(current) if current is not None else 0.0,
        "open": float(open_price) if open_price is not None else 0.0,
        "high": float(high) if high is not None else 0.0,
        "low": float(low) if low is not None else 0.0,
        "previous_close": float(prev_close) if prev_close is not None else None,
        "change": float(change) if change is not None else None,
        "percent_change": float(percent_change) if percent_change is not None else None,
        "timestamp": timestamp,
    }


# ---------- CANDLES / BARS ----------


def get_candles(symbol: str, timeframe: str = "1Day", limit: int = 60) -> Dict[str, Any]:
    """
    Return OHLCV candles from Alpaca bars endpoint:
    {
      "symbol": "AAPL",
      "candles": [
        {"time": "...", "open": ..., "high": ..., "low": ..., "close": ..., "volume": ...},
        ...
      ]
    }
    """
    symbol = symbol.upper()
    params = {
        "timeframe": timeframe,
        "limit": min(limit, 1000),
        "adjustment": "all",
        "feed": "iex",  # free data
    }
    data = _alpaca_get(ALPACA_BASE_URL, f"/v2/stocks/{symbol}/bars", params=params)
    bars: List[Dict[str, Any]] = data.get("bars", [])

    candles = []
    for bar in bars:
        candles.append(
            {
                "time": bar.get("t"),  # ISO timestamp string
                "open": float(bar.get("o", 0.0)),
                "high": float(bar.get("h", 0.0)),
                "low": float(bar.get("l", 0.0)),
                "close": float(bar.get("c", 0.0)),
                "volume": float(bar.get("v", 0.0)),
            }
        )

    return {"symbol": symbol, "candles": candles}


# ---------- NEWS ----------


def get_company_news(symbol: str, limit: int = 10) -> Dict[str, Any]:
    """
    Use Alpaca News API. If not available on the account, caller will see a clear error.
    Response shape:
    {
      "symbol": "AAPL",
      "news": [ { "headline": ..., "summary": ..., "url": ..., "source": ..., "created_at": ... }, ... ]
    }
    """
    symbol = symbol.upper()
    params = {
        "symbols": symbol,
        "limit": min(limit, 50),
        "sort": "desc",
        "include_content": "false",
    }

    data = _alpaca_get(ALPACA_DATA_URL, "/v1beta1/news", params=params)
    items = data.get("news") or data.get("data") or data  # Alpaca changed this a few times

    news_items = []
    if isinstance(items, list):
        for item in items:
            news_items.append(
                {
                    "id": item.get("id"),
                    "headline": item.get("headline"),
                    "summary": item.get("summary"),
                    "url": item.get("url"),
                    "source": item.get("source"),
                    "created_at": item.get("created_at"),
                }
            )

    return {"symbol": symbol, "news": news_items}


# ---------- SIMPLE ANALYTICS / TARGETS / PREDICTION ----------


def _compute_momentum_score(candles: List[Dict[str, Any]]) -> float:
    """Very simple momentum indicator: recent trend / volatility."""
    if len(candles) < 5:
        return 0.0

    closes = [c["close"] for c in candles]
    recent = closes[-5:]
    older = closes[:-5] or closes

    recent_avg = sum(recent) / len(recent)
    older_avg = sum(older) / len(older)

    if older_avg == 0:
        return 0.0

    return (recent_avg - older_avg) / older_avg * 100.0


def get_trade_targets(symbol: str, budget: float, risk: str = "medium") -> Dict[str, Any]:
    """
    Generate basic buy/sell targets using price + momentum.
    This does NOT use any restricted endpoint, just our own logic.
    """
    quote = get_quote(symbol)
    candles_resp = get_candles(symbol, timeframe="1Day", limit=30)
    candles = candles_resp["candles"]

    current = quote["current"] or 0.0
    if current <= 0:
        raise RuntimeError("Cannot compute targets without a valid current price.")

    momentum = _compute_momentum_score(candles)

    risk = (risk or "medium").lower()
    if risk == "low":
        tp_mult = 1.05
        sl_mult = 0.97
    elif risk == "high":
        tp_mult = 1.15
        sl_mult = 0.90
    else:
        tp_mult = 1.10
        sl_mult = 0.93

    entry = current
    take_profit = current * tp_mult * (1 + momentum / 1000.0)
    stop_loss = current * sl_mult

    shares = max(int(budget // entry), 0)

    return {
        "symbol": symbol.upper(),
        "current_price": current,
        "momentum_score": momentum,
        "entry_price": round(entry, 2),
        "take_profit": round(take_profit, 2),
        "stop_loss": round(stop_loss, 2),
        "position_size_shares": shares,
        "estimated_position_cost": round(shares * entry, 2),
        "risk_level": risk,
    }


def get_prediction(symbol: str, budget: float, risk: str = "medium") -> Dict[str, Any]:
    """
    Very simple next-day direction prediction using moving averages + momentum.
    """
    symbol = symbol.upper()
    candles_resp = get_candles(symbol, timeframe="1Day", limit=40)
    candles = candles_resp["candles"]

    if len(candles) < 10:
        raise RuntimeError("Not enough price history to make a prediction.")

    closes = [c["close"] for c in candles]
    short_ma = sum(closes[-5:]) / 5
    long_ma = sum(closes[-20:]) / min(20, len(closes))
    momentum = _compute_momentum_score(candles)

    bias = short_ma - long_ma
    if bias > 0 and momentum > 0:
        direction = "up"
        confidence = min(90.0, 60.0 + momentum / 2)
    elif bias < 0 and momentum < 0:
        direction = "down"
        confidence = min(90.0, 60.0 + abs(momentum) / 2)
    else:
        direction = "sideways"
        confidence = 50.0

    quote = get_quote(symbol)
    current = quote["current"] or 0.0

    return {
        "symbol": symbol,
        "prediction": direction,
        "confidence": round(confidence, 1),
        "current_price": current,
        "analysis": {
            "short_ma": round(short_ma, 2),
            "long_ma": round(long_ma, 2),
            "momentum_score": round(momentum, 2),
        },
        "budget": budget,
        "risk_level": risk,
    }


def get_options_helper(symbol: str, budget: float, risk: str = "medium") -> Dict[str, Any]:
    """
    Options helper is *logic-only* for now (no external API calls).
    It compares buying shares vs. buying a hypothetical at-the-money call.
    """
    quote = get_quote(symbol)
    current = quote["current"] or 0.0
    if current <= 0:
        raise RuntimeError("Cannot compute options helper without a valid price.")

    shares = int(budget // current)
    shares_cost = shares * current

    # Hypothetical ATM call pricing
    risk = (risk or "medium").lower()
    if risk == "low":
        option_price_mult = 0.12
        leverage = 4
    elif risk == "high":
        option_price_mult = 0.20
        leverage = 8
    else:
        option_price_mult = 0.16
        leverage = 6

    est_option_price = current * option_price_mult
    contracts = int(budget // (est_option_price * 100)) if est_option_price > 0 else 0
    option_cost = contracts * est_option_price * 100

    return {
        "symbol": symbol.upper(),
        "current_price": current,
        "budget": budget,
        "risk_level": risk,
        "shares_plan": {
            "shares": shares,
            "estimated_cost": round(shares_cost, 2),
        },
        "options_plan": {
            "contracts": contracts,
            "estimated_premium_per_contract": round(est_option_price, 2),
            "estimated_total_premium": round(option_cost, 2),
            "rough_leverage_factor": leverage,
        },
        "note": "Options stats are illustrative only, based on an estimated at-the-money call price.",
    }
















































