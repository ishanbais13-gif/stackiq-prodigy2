from typing import Any, Dict, List
import requests
import yfinance as yf

class FinnhubError(Exception):
    pass

def _pick(d: Dict[str, Any], *keys: str):
    for k in keys:
        if k in d:
            return d[k]
    return None

def fetch_quote(symbol: str) -> Dict[str, Any]:
    """
    Try yfinance.fast_info first (stable), then fall back to Yahoo quote API.
    """
    symbol = symbol.upper()

    # 1) Primary: yfinance fast_info
    try:
        t = yf.Ticker(symbol)
        fi = dict(getattr(t, "fast_info", {}) or {})
        if fi:
            return {
                "currentPrice": _pick(fi, "last_price", "lastPrice"),
                "previousClose": _pick(fi, "previous_close", "previousClose"),
                "open": _pick(fi, "open"),
                "dayHigh": _pick(fi, "day_high", "dayHigh"),
                "dayLow": _pick(fi, "day_low", "dayLow"),
                "volume": _pick(fi, "volume"),
            }
    except Exception:
        # fall through to Yahoo HTTP endpoint
        pass

    # 2) Fallback: Yahoo finance HTTP endpoint
    try:
        url = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={symbol}"
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        results = data.get("quoteResponse", {}).get("result", [])
        if not results:
            raise FinnhubError(f"Ticker {symbol} not found")
        q = results[0]
        return {
            "currentPrice": q.get("regularMarketPrice"),
            "previousClose": q.get("regularMarketPreviousClose"),
            "open": q.get("regularMarketOpen"),
            "dayHigh": q.get("regularMarketDayHigh"),
            "dayLow": q.get("regularMarketDayLow"),
            "volume": q.get("regularMarketVolume"),
        }
    except Exception as e:
        raise FinnhubError(f"Error fetching quote for {symbol}: {str(e)}")

def fetch_earnings(symbol: str) -> List[Dict[str, Any]]:
    """
    Use yfinance earnings info. Return a simple list of records.
    """
    symbol = symbol.upper()
    try:
        t = yf.Ticker(symbol)
        # get_earnings_dates returns recent + future dates with surprises
        try:
            ed = t.get_earnings_dates(limit=12)
            if ed is not None:
                # Normalize to list of dicts
                records = []
                for idx, row in ed.reset_index().iterrows():
                    records.append({
                        "earningsDate": str(row.get("Earnings Date")),
                        "epsEstimate": row.get("EPS Estimate"),
                        "epsActual": row.get("Reported EPS"),
                        "surprise": row.get("Surprise(%)"),
                    })
                return records
        except Exception:
            pass

        # Older fallback
        try:
            df = t.earnings
            if df is not None and not df.empty:
                df = df.reset_index().rename(columns={"Year": "year"})
                return df.to_dict(orient="records")
        except Exception:
            pass

        return []
    except Exception as e:
        raise FinnhubError(f"Error fetching earnings for {symbol}: {str(e)}")

def get_quote_and_earnings(symbol: str) -> Dict[str, Any]:
    return {
        "quote": fetch_quote(symbol),
        "earnings": fetch_earnings(symbol),
    }






