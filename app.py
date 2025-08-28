from fastapi import FastAPI, HTTPException
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
import os, requests

app = FastAPI(title="stackiq-web", version="1.0.0")

# --- static site ---
BASE_DIR = os.path.dirname(__file__)
WEB_DIR = os.path.join(BASE_DIR, "web")
app.mount("/web", StaticFiles(directory=WEB_DIR, html=True), name="web")

@app.get("/")
def root():
    # send root to the UI
    return RedirectResponse(url="/web/")

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/version")
def version():
    # simple version endpoint (you can wire this to git SHA later)
    return {"app": "stackiq-web", "version": app.version}

# --- Finnhub quote helpers ---
FINNHUB_KEY = os.getenv("FINNHUB_API_KEY", "")

def _fetch_quote(symbol: str) -> dict:
    if not FINNHUB_KEY:
        raise HTTPException(status_code=500, detail="Missing FINNHUB_API_KEY")
    url = "https://finnhub.io/api/v1/quote"
    try:
        r = requests.get(url, params={"symbol": symbol, "token": FINNHUB_KEY}, timeout=10)
        r.raise_for_status()
    except requests.HTTPError as e:
        raise HTTPException(status_code=e.response.status_code, detail=e.response.text)
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"Upstream error: {e}")

    j = r.json()
    c, pc = j.get("c"), j.get("pc")
    pct = round(((c - pc) / pc) * 100, 4) if (c is not None and pc) else None
    return {
        "symbol": symbol.upper(),
        "current": c,
        "prev_close": pc,
        "high": j.get("h"),
        "low": j.get("l"),
        "open": j.get("o"),
        "percent_change": pct,
        "volume": None,  # Finnhub 'quote' endpoint doesn’t return volume reliably here
        "raw": j,
    }

@app.get("/quote/{symbol}")
def quote(symbol: str, pretty: int | None = None):
    return _fetch_quote(symbol)

@app.get("/summary/{symbol}")
def summary(symbol: str, pretty: int | None = None):
    q = _fetch_quote(symbol)
    pct = q["percent_change"]
    updown = "up" if (pct is not None and pct >= 0) else "down"
    text = (
        f"{q['symbol']}: {q['current']} ({updown} {abs(pct):.2f}% on the day). "
        f"Session range: {q['low']}–{q['high']}. Prev close: {q['prev_close']}."
    )
    return {"symbol": q["symbol"], "summary": text, "quote": q}
























