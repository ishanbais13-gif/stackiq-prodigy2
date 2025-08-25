import os
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from starlette.staticfiles import StaticFiles

import data_fetcher as df

app = FastAPI(title="StackIQ", version="1.0.0")

# CORS (optional, but harmless)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---- Health & version ----
@app.get("/health")
def health():
    return {"ok": True}

@app.get("/version")
def version():
    return {"version": app.version}

# ---- API endpoints used by web/index.html ----
@app.get("/quote/{symbol}")
def quote(symbol: str):
    try:
        return df.fetch_quote(symbol)
    except df.FinnhubError as e:
        raise HTTPException(status_code=502, detail=str(e))

@app.get("/earnings/{symbol}")
def earnings(symbol: str, limit: int = 4):
    try:
        return df.fetch_earnings(symbol, limit=limit)
    except df.FinnhubError as e:
        raise HTTPException(status_code=502, detail=str(e))

@app.get("/history/{symbol}")
def history(symbol: str, range_days: int = 30):
    try:
        return df.fetch_history(symbol, range_days=range_days)
    except df.FinnhubError as e:
        raise HTTPException(status_code=502, detail=str(e))

# Combined test endpoint used by your page’s JS: /test/{symbol}
@app.get("/test/{symbol}")
def test(symbol: str):
    try:
        return df.get_quote_and_earnings(symbol)
    except df.FinnhubError as e:
        raise HTTPException(status_code=502, detail=str(e))

# ---- Static site (your /web/index.html) ----
WEB_DIR = os.path.join(os.path.dirname(__file__), "web")
if os.path.isdir(WEB_DIR):
    app.mount("/web", StaticFiles(directory=WEB_DIR, html=True), name="web")

# Root can show a tiny hint (optional)
@app.get("/", response_class=HTMLResponse)
def root():
    if os.path.isdir(WEB_DIR):
        return '<p>OK. UI is at <a href="/web/">/web/</a></p>'
    return "<p>OK</p>"

# ---- Error handler (nicer JSON) ----
@app.exception_handler(Exception)
async def unhandled(request, exc):
    # Don’t leak internals; log would go to stdout for Kudu
    return JSONResponse(status_code=500, content={"error": "internal_error"})
















