from typing import Any, Dict, List
import requests

class FinnhubError(Exception):
    pass

def fetch_quote(symbol: str) -> Dict[str, Any]:
    """
    Fetch quote from Yahoo Finance JSON endpoint.
    Returns a small dict with current price/ohlc/volume.
    """
    symbol = symbol.upper()
    url = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={symbol}"
    try:
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
            "currency": q.get("currency"),
            "shortName": q.get("shortName"),
        }
    except requests.RequestException as e:
        raise FinnhubError(f"Network error for {symbol}: {e}")
    except ValueError as e:
        raise FinnhubError(f"Bad JSON for {symbol}: {e}")
    except Exception as e:
        raise FinnhubError(f"Error fetching quote for {symbol}: {e}")

def fetch_earnings(symbol: str) -> List[Dict[str, Any]]:
    """
    Fetch earnings summary from Yahoo Finance.
    Returns a simple list of quarterly records: date, estimate, actual, surprise%.
    """
    symbol = symbol.upper()
    url = f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{symbol}?modules=earnings"
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        earnings = (
            data.get("quoteSummary", {})
                .get("result", [{}])[0]
                .get("earnings", {})
                .get("financialsChart", {})
                .get("quarterly", [])
        )
        out: List[Dict[str, Any]] = []
        for row in earnings:
            out.append({
                "date": row.get("date"),
                "epsEstimate": (row.get("estimate", {}) or {}).get("raw") if isinstance(row.get("estimate"), dict) else row.get("estimate"),
                "epsActual": (row.get("actual", {}) or {}).get("raw") if isinstance(row.get("actual"), dict) else row.get("actual"),
                "surprisePercent": (row.get("surprisePercent", {}) or {}).get("raw") if isinstance(row.get("surprisePercent"), dict) else row.get("surprisePercent"),
            })
        return out
    except requests.RequestException as e:
        raise FinnhubError(f"Network error for {symbol}: {e}")
    except ValueError as e:
        raise FinnhubError(f"Bad JSON for {symbol}: {e}")
    except Exception as e:
        raise FinnhubError(f"Error fetching earnings for {symbol}: {e}")

def get_quote_and_earnings(symbol: str) -> Dict[str, Any]:
    return {
        "quote": fetch_quote(symbol),
        "earnings": fetch_earnings(symbol),
    }






