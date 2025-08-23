import os
from datetime import datetime
from typing import Dict, Any, List, Optional

import pandas as pd
import yfinance as yf

try:
    import httpx  # optional; used only if FINNHUB_API_KEY is present
except Exception:  # pragma: no cover
    httpx = None  # fallback if not installed

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "").strip()


# ---------- Helpers ----------

def _format_price_block(row_today: pd.Series, row_prev: Optional[pd.Series]) -> Dict[str, Any]:
    """Return the 'price' block expected by the frontend."""
    curr = float(row_today["Close"])
    prev_close = float(row_prev["Close"]) if row_prev is not None else float(row_today["Open"])
    change = curr - prev_close
    pct = (change / prev_close) * 100 if prev_close else 0.0

    return {
        "c": round(curr, 2),                 # current price
        "d": round(change, 2),               # absolute change
        "dp": round(pct, 2),                 # % change
        "h": round(float(row_today["High"]), 2),
        "l": round(float(row_today["Low"]), 2),
        "o": round(float(row_today["Open"]), 2),
        "pc": round(prev_close, 2),          # previous close
        "v": int(row_today.get("Volume", 0)),
    }


def _format_earnings_block_yf(ticker: yf.Ticker) -> Dict[str, List[Dict[str, Any]]]:
    """Try to fetch the next/most-recent earnings from yfinance in a simple, consistent format."""
    try:
        # yfinance returns a DataFrame for earnings dates
        df = ticker.get_earnings_dates(limit=1)
        if isinstance(df, pd.DataFrame) and len(df) > 0:
            row = df.iloc[0]
            # Columns can vary slightly across tickers; guard with .get
            date = (row.get("Earnings Date") or row.get("EarningsDate") or row.name)
            date_str = pd.to_datetime(date).strftime("%Y-%m-%d")

            eps_actual = float(row.get("EPS Actual") or row.get("EPSActual") or 0.0)
            eps_est = float(row.get("EPS Estimate") or row.get("EPSEstimate") or 0.0)
            qtr = int(row.get("Quarter") or 0)
            rev_actual = int(row.get("Revenue") or row.get("Revenue Actual") or 0) or None
            rev_est = int(row.get("Revenue Estimate") or 0) or None

            return {
                "earningsCalendar": [
                    {
                        "date": date_str,
                        "epsActual": round(eps_actual, 4),
                        "epsEstimate": round(eps_est, 4),
                        "hour": "amc",
                        "quarter": qtr or 0,
                        "revenueActual": rev_actual,
                        "revenueEstimate": rev_est,
                        "symbol": str(ticker.ticker).upper(),
                        "year": pd.to_datetime(date).year,
                    }
                ]
            }
    except Exception:
        pass

    # Default empty if not available
    return {"earningsCalendar": []}


async def _fetch_from_finnhub(symbol: str) -> Optional[Dict[str, Any]]:
    """If FINNHUB_API_KEY is set, try Finnhub for a quote + earnings date."""
    if not FINNHUB_API_KEY or httpx is None:
        return None

    base = "https://finnhub.io/api/v1"
    headers = {"Accept": "application/json"}

    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            # Quote
            q = await client.get(f"{base}/quote", params={"symbol": symbol, "token": FINNHUB_API_KEY}, headers=headers)
            q.raise_for_status()
            quote = q.json()  # keys: c,d,dp,h,l,o,pc, t

            # Earnings calendar (limit to one upcoming/recent)
            e = await client.get(
                f"{base}/calendar/earnings",
                params={"symbol": symbol, "token": FINNHUB_API_KEY},
                headers=headers,
            )
            e.raise_for_status()
            e_json = e.json() or {}
            eps_list = e_json.get("earningsCalendar") or []
            earnings_block = {"earningsCalendar": eps_list[:1]} if isinstance(eps_list, list) else {"earningsCalendar": []}

            return {
                "ticker": symbol.upper(),
                "price": {
                    "c": quote.get("c"),
                    "d": quote.get("d"),
                    "dp": quote.get("dp"),
                    "h": quote.get("h"),
                    "l": quote.get("l"),
                    "o": quote.get("o"),
                    "pc": quote.get("pc"),
                    "v": quote.get("t"),  # Finnhub doesn't return volume in /quote; reusing t as timestamp
                },
                "earnings": earnings_block,
            }
        except Exception:
            return None


def _fetch_from_yfinance(symbol: str) -> Dict[str, Any]:
    """Reliable fallback using yfinance (no API key required)."""
    t = yf.Ticker(symbol)
    # last two days to compute prev close
    hist = t.history(period="2d")
    if hist is None or hist.empty:
        raise ValueError("Ticker not found or no data")

    hist = hist.reset_index(drop=False).rename(columns=str.title)
    # today row (last), prev row (second last, if present)
    today = hist.iloc[-1]
    prev = hist.iloc[-2] if len(hist) > 1 else None

    price_block = _format_price_block(today, prev)
    earnings_block = _format_earnings_block_yf(t)

    return {
        "ticker": symbol.upper(),
        "price": price_block,
        "earnings": earnings_block,
    }


# ---------- Public callables (names your app expects) ----------

def get_price_and_earnings(symbol: str) -> Dict[str, Any]:
    """
    Primary callable used by the API. Tries Finnhub (if key is set) then falls back to Yahoo.
    """
    # Try Finnhub asynchronously if key exists
    if FINNHUB_API_KEY and httpx is not None:
        try:
            import anyio  # anyio is bundled with Starlette/Uvicorn; safe to use
            result = anyio.run(_fetch_from_finnhub, symbol)
            if result:
                return result
        except Exception:
            pass

    # Fallback to yfinance
    return _fetch_from_yfinance(symbol)


# Extra names for compatibility with earlier code paths:
def get_ticker_data(symbol: str) -> Dict[str, Any]:
    return get_price_and_earnings(symbol)


def get_stock_data(symbol: str) -> Dict[str, Any]:
    return get_price_and_earnings(symbol)


def get_quote_and_earnings(symbol: str) -> Dict[str, Any]:
    return get_price_and_earnings(symbol)


# Minimal aliases some earlier versions checked for:
def get(symbol: str) -> Dict[str, Any]:
    return get_price_and_earnings(symbol)


def fetch(symbol: str) -> Dict[str, Any]:
    return get_price_and_earnings(symbol)

    }


