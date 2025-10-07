from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from data_fetcher import get_quote, get_candles
from statistics import mean
import logging, time, os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("stackiq")

app = FastAPI(title="StackIQ API", version="0.2.0")

# Open CORS for now (tighten later)
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
    logger.info(f"{request.method} {request.url.path} -> {response.status_code} ({int((time.time()-start)*1000)}ms)")
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
        raise HTTPException(status_code=403 if "403" in msg else 500, detail=f"Quote error: {msg}")

def simple_predict_logic(symbol: str):
    data = get_candles(symbol, "D", 60)  # ~60 daily bars
    if data.get("s") != "ok":
        return {"recommendation": "hold", "confidence": 0.5, "reason": f"candles status {data.get('s')}"}
    closes = data.get("c") or []
    if len(closes) < 30:
        return {"recommendation": "hold", "confidence": 0.5, "reason": "insufficient data (<30 closes)"}

    sma10 = mean(closes[-10:])
    sma30 = mean(closes[-30:])
    if sma10 > sma30:
        rec, conf, reason = "buy", 0.62, "SMA10 > SMA30 (uptrend)"
    elif sma10 < sma30:
        rec, conf, reason = "sell", 0.62, "SMA10 < SMA30 (downtrend)"
    else:
        rec, conf, reason = "hold", 0.5, "neutral"
    return {"recommendation": rec, "confidence": conf, "reason": reason, "sma10": sma10, "sma30": sma30}

@app.get("/candles/{symbol}")
def candles(symbol: str, resolution: str = "D", count: int = 30):
    try:
        return get_candles(symbol, resolution, count)
    except Exception as e:
        msg = str(e)
        if "403" in msg:
            if USE_SANDBOX:
                raise HTTPException(status_code=403, detail="403 from Finnhub (sandbox): check key/limits.")
            else:
                raise HTTPException(status_code=403, detail="Your Finnhub plan doesn’t allow /stock/candle. Enable sandbox or upgrade.")
        raise HTTPException(status_code=500, detail=f"Candles error: {msg}")

@app.get("/predict/{symbol}")
def predict(symbol: str, budget: float = 1000.0):
    try:
        base = simple_predict_logic(symbol)
        q = get_quote(symbol)
        last = q.get("c") or q.get("pc") or 0
        shares = round(budget / last, 2) if last else 0
        return {
            "symbol": symbol.upper(),
            "budget": budget,
            "last_price": last,
            "position_size_shares": shares,
            **base
        }
    except Exception as e:
        msg = str(e)
        raise HTTPException(status_code=500, detail=f"Predict error: {msg}")























































