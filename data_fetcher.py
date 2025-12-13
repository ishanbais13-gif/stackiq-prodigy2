import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

import requests


class AlpacaClient:
    """
    Alpaca Data API client (stocks).
    Env vars required:
      - ALPACA_API_KEY
      - ALPACA_SECRET_KEY

    Optional:
      - ALPACA_DATA_BASE_URL (default: https://data.alpaca.markets)
      - ALPACA_FEED (default: iex)  # iex works for most free accounts; sip requires paid
    """

    def __init__(self) -> None:
        self.api_key = os.getenv("ALPACA_API_KEY", "").strip()
        self.secret_key = os.getenv("ALPACA_SECRET_KEY", "").strip()

        if not self.api_key or not self.secret_key:
            raise ValueError("Missing ALPACA_API_KEY / ALPACA_SECRET_KEY in environment variables")

        self.data_base_url = os.getenv("ALPACA_DATA_BASE_URL", "https://data.alpaca.markets").strip().rstrip("/")
        self.feed = os.getenv("ALPACA_FEED", "iex").strip()

        self.session = requests.Session()
        self.session.headers.update(
            {
                "APCA-API-KEY-ID": self.api_key,
                "APCA-API-SECRET-KEY": self.secret_key,
                "Accept": "application/json",
            }
        )

    def _get(self, url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        try:
            resp = self.session.get(url, params=params or {}, timeout=15)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError as e:
            # Try to include Alpaca error body if present
            detail = ""
            try:
                detail = resp.text  # type: ignore[name-defined]
            except Exception:
                pass
            raise RuntimeError(f"Alpaca HTTP error: {e}. Body: {detail}") from e
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Network error calling Alpaca: {e}") from e
        except ValueError as e:
            raise RuntimeError(f"Invalid JSON from Alpaca: {e}") from e

    @staticmethod
    def _iso(dt: datetime) -> str:
        # Alpaca accepts RFC3339/ISO timestamps
        return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

    def get_latest_quote(self, symbol: str) -> Dict[str, Any]:
        symbol = symbol.upper().strip()
        url = f"{self.data_base_url}/v2/stocks/{symbol}/quotes/latest"
        params = {"feed": self.feed}
        data = self._get(url, params=params)

        # Alpaca returns {"quote": {...}} for this endpoint
        q = data.get("quote") or {}
        return {
            "symbol": symbol,
            "bid": q.get("bp"),
            "ask": q.get("ap"),
            "bid_size": q.get("bs"),
            "ask_size": q.get("as"),
            "timestamp": q.get("t"),
            "raw": q,
        }

    def get_bars(
        self,
        symbol: str,
        timeframe: str = "1Day",
        start: Optional[str] = None,
        end: Optional[str] = None,
        limit: int = 200,
        adjustment: str = "raw",
    ) -> Dict[str, Any]:
        """
        timeframe examples: 1Min, 5Min, 15Min, 1Hour, 1Day
        start/end: ISO strings (YYYY-MM-DD or full ISO). If missing, defaults to last 30 days.
        """

        symbol = symbol.upper().strip()
        url = f"{self.data_base_url}/v2/stocks/{symbol}/bars"

        now = datetime.now(timezone.utc)
        if not start and not end:
            start_dt = now - timedelta(days=30)
            end_dt = now
            start = self._iso(start_dt)
            end = self._iso(end_dt)

        params = {
            "timeframe": timeframe,
            "start": start,
            "end": end,
            "limit": max(1, min(int(limit), 10000)),
            "adjustment": adjustment,
            "feed": self.feed,
        }

        data = self._get(url, params=params)

        bars = data.get("bars") or []
        # Normalize output to something your frontend can use
        normalized = [
            {
                "t": b.get("t"),
                "o": b.get("o"),
                "h": b.get("h"),
                "l": b.get("l"),
                "c": b.get("c"),
                "v": b.get("v"),
                "n": b.get("n"),
                "vw": b.get("vw"),
            }
            for b in bars
        ]

        return {
            "symbol": symbol,
            "timeframe": timeframe,
            "start": start,
            "end": end,
            "limit": params["limit"],
            "feed": self.feed,
            "bars": normalized,
            "raw": data,
        }

























































