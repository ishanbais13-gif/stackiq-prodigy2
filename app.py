from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, FileResponse, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware
from pathlib import Path

import data_fetcher as df

app = FastAPI(title="stackiq", version="1.0")

# CORS: open for now
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"], allow_credentials=False,
)

ROOT = Path(__file__).parent.resolve()

def _pretty_if_wanted(request: Request, payload):
    # add ?pretty=1 to pretty-print
    if request.query_params.get("pretty"):
        return JSONResponse(payload, media_type="application/json", indent=2)
    return JSONResponse(payload)

@app.get("/health")
def health():
    return {"status": "ok"}

# --- Quote ---
@app.get("/quote/{symbol}")
def quote(symbol: str, request: Request):
    data = df.fetch_quote(symbol)
    return _pretty_if_wanted(request, data)

# --- Earnings (summary) ---
@app.get("/earnings/{symbol}")
def earnings(symbol: str, request: Request, limit: int = 4):
    data = df.fetch_earnings(symbol, limit=limit)
    return _pretty_if_wanted(request, data)

# --- History (daily candles) ---
@app.get("/history/{symbol}")
def history(symbol: str, request: Request, days: int = 30):
    data = df.fetch_history(symbol, range_days=days)
    return _pretty_if_wanted(request, data)

# --- Combined quick test ---
@app.get("/test/{symbol}")
def combined(symbol: str, request: Request):
    data = df.get_quote_and_earnings(symbol)
    return _pretty_if_wanted(request, data)

# --- Serve simple UI if you want it later ---
@app.get("/")
def root():
    index = ROOT / "web" / "index.html"
    if index.exists():
        return FileResponse(index)
    return PlainTextResponse("stackiq API is running. Try /health, /quote/AAPL, /earnings/MSFT, /history/NVDA")



















