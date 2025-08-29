# data_fetcher.py
import requests
from typing import Dict, Any, List, Optional

# Yahoo endpoints (no API key)
YH_CHART = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"

# Map UI ranges to Yahoo ranges/intervals
RANGE_MAP = {
    "1m": {"range": "1mo", "interval": "1d"},
    "3m": {"range": "3mo", "interval": "1d"},
    "6m": {"range": "6mo", "interval": "1d"},
    "1y": {"range": "1y",  "interval": "1d"},
}

def _pull_chart(symbol: str, range_str: str, interval: str) -> Optional[Dict[str, Any]]:
    try:
        url = YH_CHART.format(symbol=symbol)
        params = {"range": range_str, "interval": interval, "includePrePost": "false"}
        r = requests.get(url, params=params, timeout=10)
        if r.status_code != 200:
            return None
        data = r.json()
        if not data or "chart" not in data or not data["chart"].get("result"):
            return None
        return data["chart"]["result"][0]
    except Exception:
        return None

def get_quote(symbol: str) -> Optional[Dict[str, Any]]:
    """
    Returns:
      {
        "symbol": "AAPL",
        "current": 232.56,
        "prev_close": 230.49,
        "high": 233.41,
        "low": 229.33,
        "open": 230.82,
        "percent_change": 0.89,
        "volume": null,
        "raw": {...yahoo meta snapshot...}
      }
    """
    result = _pull_chart(symbol, "1d", "1m")
    if result is None:
        # Fallback to 5d if 1d is unavailable (some tickers)
        result = _pull_chart(symbol, "5d", "1m")
    if result is None:
        return None

    meta = result.get("meta", {})
    indicators = result.get("indicators", {})
    quote_arr = indicators.get("quote", [])
    closes = quote_arr[0]["close"] if quote_arr else []

    # Compute current (last non-null close)
    current = next((c for c in reversed(closes) if c is not None), None)
    if current is None:
        return None

    prev_close = meta.get("previousClose")
    high = meta.get("regularMarketDayHigh") or meta.get("chartPreviousClose") or current
    low = meta.get("regularMarketDayLow") or current
    opn = meta.get("regularMarketOpen") or current

    # percent change from previous close if available
    if prev_close:
        pct = ((current - prev_close) / prev_close) * 100.0
    else:
        pct = 0.0

    return {
        "symbol": symbol,
        "current": float(current),
        "prev_close": float(prev_close) if prev_close else float(current),
        "high": float(high) if high is not None else float(current),
        "low": float(low) if low is not None else float(current),
        "open": float(opn) if opn is not None else float(current),
        "percent_change": float(pct),
        "volume": None,
        "raw": {
            "c": float(current),
            "pc": float(prev_close) if prev_close else float(current),
            "h": float(high) if high is not None else float(current),
            "l": float(low) if low is not None else float(current),
            "o": float(opn) if opn is not None else float(current),
        },
    }

def get_history(symbol: str, range_key: str) -> Optional[List[List[float]]]:
    """
    Returns a list of [timestamp, close] pairs for charting.
    We downsample to daily for simplicity.
    """
    cfg = RANGE_MAP.get(range_key)
    if not cfg:
        return None

    result = _pull_chart(symbol, cfg["range"], cfg["interval"])
    if result is None:
        return None

    timestamps = result.get("timestamp") or []
    indicators = result.get("indicators", {})
    quote_arr = indicators.get("quote", [])
    closes = quote_arr[0]["close"] if quote_arr else []

    # pair t & c and drop nulls
    series: List[List[float]] = []
    for t, c in zip(timestamps, closes):
        if c is None:
            continue
        # chart expects numbers; keep timestamp as epoch seconds
        series.append([float(t), float(c)])

    if not series:
        return None

    return series











