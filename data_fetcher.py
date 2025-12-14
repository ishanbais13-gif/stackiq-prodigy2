import os
import time
import requests
from typing import Any, Dict, Optional, Tuple


class AlpacaClient:
    """
    Minimal, stable Alpaca client for:
    - Latest quote
    - Historical bars (candles)
    Uses ONLY environment variables (Azure App Service compatible).
    """

    def __init__(self) -> None:
        self.api_key = os.getenv("ALPACA_API_KEY", "").strip()

        # Support both names because your env vars have been inconsistent before.
        # Your Azure screenshot shows ALPACA_API_SECRET and ALPACA_SECRET_KEY.
        self.secret_key = (
            os.getenv("ALPACA_SECRET_KEY", "").strip()
            or os.getenv("ALPACA_API_SECRET", "").strip()
            or os.getenv("ALPACA_API_SECRET_KEY", "").strip()
        )

        # You already set these in Azure:
        self.data_base_url = os.getenv("ALPACA_DATA_BASE_URL", "https://data.alpaca.markets").strip()
        self.trading_base_url = os.getenv("ALPACA_TRADING_BASE_URL", "https://paper-api.alpaca.markets").strip()
        self.feed = os.getenv("ALPACA_DATA_FEED", "iex").strip()  # iex recommended for free tiers

        # Optional knobs
        self.timeout_s = float(os.getenv("HTTP_TIMEOUT_SECONDS", "15").strip())
        self.max_retries = int(os.getenv("HTTP_MAX_RETRIES", "2").strip())
        self.retry_sleep_s = float(os.getenv("HTTP_RETRY_SLEEP_SECONDS", "0.6").strip())

        self._validate()

    def _validate(self) -> None:
        missing = []
        if not self.api_key:
            missing.append("ALPACA_API_KEY")
        if not self.secret_key:
            missing.append("ALPACA_SECRET_KEY (or ALPACA_API_SECRET)")
        if missing:
            raise RuntimeError(f"Missing Alpaca environment variables: {', '.join(missing)}")

    def _headers(self) -> Dict[str, str]:
        return {
            "APCA-API-KEY-ID": self.api_key,
            "APCA-API-SECRET-KEY": self.secret_key,
        }

    def _request(self, method: str, url: str, params: Optional[Dict[str, Any]] = None) -> Tuple[int, Dict[str, Any]]:
        last_err: Optional[Exception] = None

        for attempt in range(self.max_retries + 1):
            try:
                resp = requests.request(
                    method=method,
                    url=url,
                    headers=self._headers(),
                    params=params,
                    timeout=self.timeout_s,
                )
                try:
                    data = resp.json() if resp.content else {}
                except Exception:
                    data = {"raw_text": resp.text}

                return resp.status_code, data

            except Exception as e:
                last_err = e
                if attempt < self.max_retries:
                    time.sleep(self.retry_sleep_s)

        raise RuntimeError(f"HTTP request failed after retries: {last_err}")

    # -----------------------
    # Public API
    # -----------------------

    def get_latest_quote(self, symbol: str) -> Dict[str, Any]:
        symbol = symbol.upper().strip()
        url = f"{self.data_base_url}/v2/stocks/{symbol}/quotes/latest"
        status, data = self._request("GET", url, params={"feed": self.feed})

        if status >= 400:
            raise RuntimeError(f"Alpaca error {status}: {data}")

        # Alpaca returns {"quote": {...}} for this endpoint
        quote = data.get("quote") or data
        return {
            "symbol": symbol,
            "timestamp": quote.get("t"),
            "bid": quote.get("bp"),
            "ask": quote.get("ap"),
            "bid_size": quote.get("bs"),
            "ask_size": quote.get("as"),
            "exchange": quote.get("bx") or quote.get("ax"),
            "raw": data,
        }

    def get_bars(
        self,
        symbol: str,
        timeframe: str = "1Day",
        start: Optional[str] = None,
        end: Optional[str] = None,
        limit: int = 1000,
        adjustment: str = "raw",
    ) -> Dict[str, Any]:
        """
        timeframe examples: 1Min, 5Min, 15Min, 1Hour, 1Day
        start/end should be ISO8601 strings, e.g. 2025-12-01T00:00:00Z
        """
        symbol = symbol.upper().strip()
        url = f"{self.data_base_url}/v2/stocks/{symbol}/bars"

        params: Dict[str, Any] = {
            "timeframe": timeframe,
            "limit": int(limit),
            "adjustment": adjustment,
            "feed": self.feed,
        }
        if start:
            params["start"] = start
        if end:
            params["end"] = end

        status, data = self._request("GET", url, params=params)
        if status >= 400:
            raise RuntimeError(f"Alpaca error {status}: {data}")

        bars = data.get("bars", [])
        return {
            "symbol": symbol,
            "timeframe": timeframe,
            "start": start,
            "end": end,
            "limit": limit,
            "count": len(bars),
            "bars": bars,
            "raw": data,
        }


























































