from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import requests
import os

app = FastAPI()

# Allow frontend to call backend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mock / simple external API
MOCK_API = "https://api.polygon.io/v2/aggs/ticker/{symbol}/prev?apiKey={api_key}"

API_KEY = os.getenv("POLYGON_API_KEY", "demo")  # Replace with real key in Azure env

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/quote/{symbol}")
def get_quote(symbol: str):
    try:
        url = MOCK_API.format(symbol=symbol.upper(), api_key=API_KEY)
        r = requests.get(url)
        if r.status_code != 200:
            raise HTTPException(status_code=404, detail="Symbol not found")
        data = r.json()

        # Handle Polygon format
        if "results" not in data or not data["results"]:
            raise HTTPException(status_code=404, detail="Symbol not found")

        result = data["results"][0]
        return {
            "symbol": symbol.upper(),
            "current": result.get("c"),
            "prev_close": result.get("o"),
            "high": result.get("h"),
            "low": result.get("l"),
            "open": result.get("o"),
            "percent_change": (
                ((result.get("c") - result.get("o")) / result.get("o")) * 100
                if result.get("c") and result.get("o") else None
            ),
            "volume": result.get("v"),
            "raw": result
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/summary/{symbol}")
def get_summary(symbol: str):
    try:
        quote = get_quote(symbol)

        # Defensive checks
        if not quote or not quote.get("current"):
            raise HTTPException(status_code=404, detail="Symbol not found")

        summary = {
            "symbol": symbol.upper(),
            "trend": "bullish" if quote["current"] > quote["prev_close"] else "bearish",
            "support": round((quote["low"] + quote["open"]) / 2, 2) if quote["low"] and quote["open"] else None,
            "resistance": round((quote["high"] + quote["prev_close"]) / 2, 2) if quote["high"] and quote["prev_close"] else None,
            "momentum": "positive" if quote["percent_change"] and quote["percent_change"] > 0 else "negative",
        }
        return summary
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



































