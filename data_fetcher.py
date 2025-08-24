import yfinance as yf

class FinnhubError(Exception):
    pass

def fetch_quote(symbol: str):
    try:
        ticker = yf.Ticker(symbol)
        data = ticker.fast_info
        return {
            "currentPrice": data.get("lastPrice"),
            "previousClose": data.get("previousClose"),
            "open": data.get("open"),
            "dayHigh": data.get("dayHigh"),
            "dayLow": data.get("dayLow"),
            "volume": data.get("volume"),
        }
    except Exception as e:
        raise FinnhubError(f"Error fetching quote for {symbol}: {str(e)}")





