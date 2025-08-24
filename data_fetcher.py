import yfinance as yf
import requests

class FinnhubError(Exception):
    pass

def fetch_quote(symbol: str):
    try:
        ticker = yf.Ticker(symbol)
        data = ticker.info
        return {
            "currentPrice": data.get("currentPrice"),
            "previousClose": data.get("previousClose"),
            "open": data.get("open"),
            "dayHigh": data.get("dayHigh"),
            "dayLow": data.get("dayLow"),
            "volume": data.get("volume"),
        }
    except Exception as e:
        raise FinnhubError(f"Error fetching quote for {symbol}: {str(e)}")

def fetch_earnings(symbol: str):
    try:
        ticker = yf.Ticker(symbol)
        earnings = ticker.earnings
        if earnings is not None:
            return earnings.to_dict()
        return {}
    except Exception as e:
        raise FinnhubError(f"Error fetching earnings for {symbol}: {str(e)}")

def get_quote_and_earnings(symbol: str):
    return {
        "quote": fetch_quote(symbol),
        "earnings": fetch_earnings(symbol),
    }




