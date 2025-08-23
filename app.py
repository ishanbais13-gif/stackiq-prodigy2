from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import os

from data_fetcher import get_quote_and_earnings, FinnhubError

app = FastAPI(title="StackIQ")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/test/{ticker}")
def test_ticker(ticker: str, pretty: int | None = None):
    try:
        data = get_quote_and_earnings(ticker)
    except FinnhubError as e:
        # 503 makes it obvious it’s a server/config issue (e.g., missing key)
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Upstream error") from e

    if not data:
        raise HTTPException(status_code=404, detail="Ticker not found or no data")
    return data

# ---- Static web (your UI) ----
# Serve /web assets
if os.path.isdir("web"):
    app.mount("/web", StaticFiles(directory="web"), name="web")

@app.get("/web")
def web_index_redirect():
    return FileResponse(os.path.join("web", "index.html"))

@app.get("/")
def root():
    return {"message": "StackIQ backend is Live."}










