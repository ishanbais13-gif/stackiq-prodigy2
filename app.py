from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from data_fetcher import get_quote, get_candles
import logging
import time

# --- Logging setup ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("stackiq")

app = FastAPI(
    title="StackIQ API",
    version="0.1.0"
)

# --- CORS (open for now; tighten later to your domain) ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],          # TODO: replace with ["https://your-frontend.com"]
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Simple request logging middleware ---
@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = time.time()
    response = await call_next(request)
    duration_ms = int((time.time() - start) * 1000)
    logger.info(f"{request.method} {request.url.path} -> {response.status_code} ({duration_ms}ms)")
    return response

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/quote/{symbol}")
def quote(symbol: str):
    try:
        data = get_quote(symbol)
        return data
    except Exception as e:
        logger.exception("Error in /quote")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/candles/{symbol}")
def candles(symbol: str, resolution: str = "D", count: int = 30):
    try:
        data = get_candles(symbol, resolution, count)
        return data
    except Exception as e:
        logger.exception("Error in /candles")
        raise HTTPException(status_code=500, detail=str(e))





















































