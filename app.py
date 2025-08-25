from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from pathlib import Path
import os

from data_fetcher import (
    fetch_quote, fetch_earnings, fetch_history, get_quote_and_earnings, FinnhubError
)

app = FastAPI(title="StackIQ Web", version="1.0.0")

WEB_DIR = Path(__file__).parent / "web"

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/version")
def version():
    return {"version": app.version}

@app.get("/", response_class=HTMLResponse)
def root():
    idx = WEB_DIR / "index.html"
    if idx.exists():
        return idx.read_text(encoding="utf-8")
    return HTMLResponse("<h1>StackIQ</h1><p>index.html not found.</p>", status_code=200)

@app.get("/web/{path:path}", response_class=HTMLResponse)
def web_assets(path: str):
    p = WEB_DIR / path
    if p.is_file():
        return HTMLResponse(p.read_text(encoding="utf-8"))
    raise HTTPException(status_code=404, detail="Not found")

@app.get("/quote/{symbol}")
def quote(symbol: str, pretty: int = 0):
    try:
        data = fetch_quote(symbol)
        return JSONResponse(data, media_type="application/json", indent=2 if pretty else None)
    except FinnhubError as e:
        raise HTTPException(status_code=502, detail=str(e)) from e

@app.get("/earnings/{symbol}")
def earnings(symbol: str, pretty: int = 0, limit: int = 4):
    try:
        data = fetch_earnings(symbol, limit=limit)
        return JSONResponse(data, media_type="application/json", indent=2 if pretty else None)
    except FinnhubError as e:
        raise HTTPException(status_code=502, detail=str(e)) from e

@app.get("/history/{symbol}")
def history(symbol: str, pretty: int = 0, range_days: int = 30):
    try:
        data = fetch_history(symbol, range_days=range_days)
        return JSONResponse(data, media_type="application/json", indent=2 if pretty else None)
    except FinnhubError as e:
        raise HTTPException(status_code=502, detail=str(e)) from e

@app.get("/test/{symbol}")
def test(symbol: str, pretty: int = 0):
    try:
        data = get_quote_and_earnings(symbol)
        return JSONResponse(data, media_type="application/json", indent=2 if pretty else None)
    except FinnhubError as e:
        raise HTTPException(status_code=502, detail=str(e)) from e
















