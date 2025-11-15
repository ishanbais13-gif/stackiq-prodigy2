# ml/data_loader.py

import time
import requests
import pandas as pd
from datetime import datetime, timedelta
from ml.config import (
    FINNHUB_API_KEY,
    DEFAULT_SYMBOLS,
    RAW_DATA_DIR,
    YEARS_BACK
)

FINNHUB_CANDLE_URL = "https://finnhub.io/api/v1/stock/candle"


def fetch_candles(symbol: str, resolution="D", years=YEARS_BACK):
    """
    Fetch historical candlestick data for a symbol using Finnhub.
    Returns a Pandas DataFrame or None if the request fails.
    """

    end = int(time.time())
    start = int((datetime.now() - timedelta(days=years * 365)).timestamp())

    params = {
        "symbol": symbol,
        "resolution": resolution,
        "from": start,
        "to": end,
        "token": FINNHUB_API_KEY
    }

    print(f"[INFO] Fetching {symbol} candles from Finnhub...")

    r = requests.get(FINNHUB_CANDLE_URL, params=params)

    if r.status_code != 200:
        print(f"[ERROR] Finnhub error for {symbol}: HTTP {r.status_code}")
        return None

    data = r.json()

    if data.get("s") != "ok":
        print(f"[WARN] No valid candle data for {symbol}. Finnhub response: {data}")
        return None

    df = pd.DataFrame({
        "t": data["t"],
        "o": data["o"],
        "h": data["h"],
        "l": data["l"],
        "c": data["c"],
        "v": data["v"]
    })

    df["t"] = pd.to_datetime(df["t"], unit="s")
    df.set_index("t", inplace=True)

    return df


def save_raw(df: pd.DataFrame, symbol: str):
    """
    Save raw data to /data/raw/{symbol}.csv
    """
    path = RAW_DATA_DIR / f"{symbol}.csv"
    df.to_csv(path)
    print(f"[SAVED] {path}")


def run_full_download(symbols=DEFAULT_SYMBOLS):
    """
    Download raw candle data for all symbols.
    """

    if not FINNHUB_API_KEY:
        print("[ERROR] FINNHUB_API_KEY is missing! Set it before running loader.")
        return

    print("[INFO] Starting full stock data download...")

    for symbol in symbols:
        df = fetch_candles(symbol)
        if df is not None:
            save_raw(df, symbol)
        time.sleep(1)  # avoid API rate limits

    print("[DONE] All symbols downloaded!")


if __name__ == "__main__":
    run_full_download()
