import yfinance as yf

def get_quote(symbol: str):
    ticker = yf.Ticker(symbol)
    info = ticker.info
    return {
        "symbol": symbol.upper(),
        "current": info.get("currentPrice", 0),
        "high": info.get("dayHigh", 0),
        "low": info.get("dayLow", 0),
        "open": info.get("open", 0),
        "prev_close": info.get("previousClose", 0),
        "percent_change": round(
            ((info.get("currentPrice", 0) - info.get("previousClose", 0)) / info.get("previousClose", 1)) * 100, 2
        ) if info.get("previousClose") else 0,
    }

def get_summary(symbol: str):
    ticker = yf.Ticker(symbol)
    hist = ticker.history(period="1d")
    if hist.empty:
        return {"symbol": symbol.upper(), "summary": "No data"}
    last = hist.iloc[-1]
    return {
        "symbol": symbol.upper(),
        "summary": f"{symbol.upper()} {last['Close']:.2f}",
        "quote": get_quote(symbol)
    }

def get_history(symbol: str, range: str = "3mo"):
    ticker = yf.Ticker(symbol)
    hist = ticker.history(period=range)
    return [
        {"date": str(idx.date()), "price": row["Close"]}
        for idx, row in hist.iterrows()
    ]











