from fastapi import FastAPI, HTTPException
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
import os, requests

app = FastAPI(title="StackIQ", version="1.0")

# --- Serve your frontend ---
# /web/* will serve files from the web/ folder
app.mount("/web", StaticFiles(directory="web", html=True), name="web")

# Make the root URL load the UI
@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/web/")

# --- Health ---
@app.get("/health")
def health():
    return {"status": "ok"}

# --- Simple quote & summary API (what you already have working) ---
_API = "https://finnhub.io/api/v1"
_KEY = os.getenv("FINNHUB_API_KEY")

def _need_key():
    if not _KEY:
        raise HTTPException(status_code=500, detail="FINNHUB_API_KEY not set")

@app.get("/quote/{symbol}")
def quote(symbol: str):
    _need_key()
    r = requests.get(f"{_API}/quote", params={"symbol": symbol, "token": _KEY})
    if r.status_code != 200:
        raise HTTPException(status_code=r.status_code, detail=f"Quote error {r.status_code}")
    q = r.json()
    return {
        "symbol": symbol.upper(),
        "current": q.get("c"),
        "prev_close": q.get("pc"),
        "high": q.get("h"),
        "low": q.get("l"),
        "open": q.get("o"),
        "percent_change": q.get("dp"),
        "volume": q.get("v"),
        "raw": q,
    }

@app.get("/summary/{symbol}")
def summary(symbol: str):
    data = quote(symbol)  # reuse the same call/format
    c, pc, h, l = data["current"], data["prev_close"], data["high"], data["low"]
    pct = data["percent_change"]
    updown = "up" if (pct or 0) >= 0 else "down"
    return {
        "symbol": data["symbol"],
        "summary": f"{data['symbol']}: {c} ({updown} {abs(pct or 0):.2f}% on the day). "
                   f"Session range: {l}–{h}. Prev close: {pc}.",
        "quote": data,
    }























