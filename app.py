from fastapi import FastAPI, HTTPException
import os
import requests

app = FastAPI()

# --- Alpaca Environment Variables ---
ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_API_SECRET = os.getenv("ALPACA_API_SECRET")
ALPACA_BASE_URL = os.getenv("ALPACA_BASE_URL", "https://data.alpaca.markets")

HEADERS = {
    "APCA-API-KEY-ID": ALPACA_API_KEY,
    "APCA-API-SECRET-KEY": ALPACA_API_SECRET
}

# --- Health Check (CRITICAL FOR AZURE) ---
@app.get("/health")
def health():
    return {"status": "ok"}

# --- Quote Endpoint ---
@app.get("/quote/{symbol}")
def get_quote(symbol: str):
    if not ALPACA_API_KEY or not ALPACA_API_SECRET:
        raise HTTPException(status_code=500, detail="Alpaca API keys not set")

    url = f"{ALPACA_BASE_URL}/v2/stocks/{symbol.upper()}/quotes/latest"

    try:
        r = requests.get(url, headers=HEADERS, timeout=5)
        r.raise_for_status()
        data = r.json()

        quote = data.get("quote")
        if not quote:
            raise HTTPException(status_code=404, detail="Quote not found")

        return {
            "symbol": symbol.upper(),
            "bid_price": quote.get("bp"),
            "ask_price": quote.get("ap"),
            "bid_size": quote.get("bs"),
            "ask_size": quote.get("as"),
            "timestamp": quote.get("t")
        }

    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=500, detail=str(e))

























