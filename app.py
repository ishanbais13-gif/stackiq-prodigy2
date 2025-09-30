from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from data_fetcher import get_quote, get_candles
import logging, time, os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("stackiq")

app = FastAPI(title="StackIQ API", version="0.1.1")

# CORS: open for now; lock to your domains later
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

USE_SANDBOX = os.getenv("FINNHUB_SANDBOX", "false").lower() == "true"
logger.info(f"FINNHUB_SANDBOX={USE_SANDBOX}")

@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = time.time()
    response = await call_next(request)
    dur = int((time.time() - start) * 1000)
    logger.info(f"{request.method} {request.url.path} -> {response.status_code} ({dur}ms)")
    return response

@app.get("/health")
def health():
    return {"status": "ok", "sandbox": USE_SANDBOX}

@app.get("/quote/{symbol}")
def quote(symbol: str):
    try:
        return get_quote(symbol)
    except Exception as e:
        msg = str(e)
        status = 403 if "403" in msg else 500
        raise HTTPException(status_code=status, detail=f"Quote error: {msg}")

@app.get("/candles/{symbol}")
def candles(symbol: str, resolution: str = "D", count: int = 30):
    try:
        return get_candles(symbol, resolution, count)
    except Exception as e:
        msg = str(e)
        # If Finnhub said 403, give a clear hint
        if "403" in msg:
            if USE_SANDBOX:
                raise HTTPException(status_code=403, detail="403 from Finnhub (sandbox): check key/limits.")
            else:
                raise HTTPException(status_code=403, detail="Your Finnhub plan doesn’t allow /stock/candle. Enable sandbox or upgrade.")
        raise HTTPException(status_code=500, detail=f"Candles error: {msg}")






















































