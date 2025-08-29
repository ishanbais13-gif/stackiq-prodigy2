import os
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, RedirectResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles

from data_fetcher import fetch_quote

APP_NAME = "stackiq-web"
APP_VERSION = "1.0.0"

app = FastAPI(title=APP_NAME, version=APP_VERSION)

# CORS (simple & lenient)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve /web if the directory exists (won't crash app if it doesn't)
if os.path.isdir("web"):
    app.mount("/web", StaticFiles(directory="web", html=True), name="web")

# -------- Global error handlers (no more ugly platform error page) --------
@app.exception_handler(HTTPException)
async def http_exc_handler(_: Request, exc: HTTPException):
    return JSONResponse(status_code=exc.status_code, content={"detail": str(exc.detail)})

@app.exception_handler(Exception)
async def unhandled_exc_handler(_: Request, exc: Exception):
    # Log details to server console; return clean JSON to client
    print("UNHANDLED ERROR:", repr(exc))
    return JSONResponse(status_code=500, content={"detail": "Internal Server Error"})

# ------------------------------ Routes ------------------------------------
@app.get("/", include_in_schema=False)
def root():
    # Always give the UI a stable place
    return RedirectResponse(url="/web/")

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/version")
def version():
    return {"app": APP_NAME, "version": APP_VERSION}

def _pretty(obj: Any, pretty: Optional[int]) -> Any:
    # FastAPI handles JSON formatting; keeping hook here in case you later want custom pretty
    return obj

@app.get("/quote/{symbol}")
def quote(symbol: str, pretty: Optional[int] = None) -> Any:
    data: Optional[Dict[str, Any]] = fetch_quote(symbol)
    if not data:
        # 404 when we can't resolve the symbol or remote returns empty
        raise HTTPException(status_code=404, detail="Symbol not found")
    return _pretty(data, pretty)

@app.get("/summary/{symbol}")
def summary(symbol: str, pretty: Optional[int] = None) -> Any:
    data = fetch_quote(symbol)
    if not data:
        raise HTTPException(status_code=404, detail="Symbol not found")

    pct = data.get("percent_change")
    updown = "up" if (isinstance(pct, (int, float)) and pct >= 0) else "down"

    msg = (
        f"{data['symbol']}: {data['current']} ({updown} {abs(pct):.2f}% on the day). "
        f"Session range: {data['low']}–{data['high']}. Prev close: {data['prev_close']}."
    )

    return _pretty({"symbol": data["symbol"], "summary": msg, "quote": data}, pretty)

# --------- Fallback mini UI when /web folder is missing ---------
@app.get("/web/", include_in_schema=False)
def web_fallback():
    if os.path.isdir("web"):
        # StaticFiles will serve index.html; this is only hit when /web dir exists but no index path matched.
        return HTMLResponse("<!doctype html><meta charset='utf-8'><title>StackIQ</title>")
    html = f"""
    <!doctype html>
    <meta charset="utf-8" />
    <title>StackIQ</title>
    <style>
      body {{ background:#0b1220; color:#e7ecf2; font:16px/1.5 system-ui, -apple-system, Segoe UI, Roboto, sans-serif; padding:40px; }}
      code {{ background:#0f1a2b; padding:2px 6px; border-radius:6px; }}
      .card {{ max-width:760px; background:#0f1a2b; border:1px solid #1f2a44; border-radius:12px; padding:20px; }}
    </style>
    <div class="card">
      <h1>StackIQ</h1>
      <p>Static UI not found. API is running.</p>
      <p>Try: <code>/quote/AAPL</code> or <code>/summary/AAPL</code></p>
      <p>Health: <code>/health</code> · Version: <code>/version</code></p>
    </div>
    """
    return HTMLResponse(html)




































