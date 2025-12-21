from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime
import random

app = FastAPI(title="StackIQ API", version="1.0.0")

# CORS for React dev server
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
def health():
    return {"status": "ok", "ts": datetime.utcnow().isoformat()}

# --- Dashboard: Top movers ---
@app.get("/top-movers")
def top_movers(limit: int = Query(10, ge=1, le=50)):
    # Dummy movers so frontend has something
    symbols = ["AAPL", "TSLA", "NVDA", "MSFT", "AMZN", "META", "AMD", "NFLX", "GOOGL", "SPY", "QQQ"]
    random.shuffle(symbols)
    out = []
    for s in symbols[:limit]:
        out.append({
            "symbol": s,
            "price": round(random.uniform(50, 600), 2),
            "change": round(random.uniform(-5, 5), 2),
            "changePct": round(random.uniform(-3, 3), 2),
            "volume": random.randint(1_000_000, 80_000_000),
        })
    return {"items": out}

# --- News ---
@app.get("/news")
def news(limit: int = Query(10, ge=1, le=50)):
    # Dummy news
    items = []
    for i in range(limit):
        items.append({
            "title": f"Market headline #{i+1}",
            "source": "StackIQ Demo Feed",
            "url": "https://example.com",
            "publishedAt": datetime.utcnow().isoformat(),
            "summary": "Placeholder news item so the UI can render.",
        })
    return {"items": items}

# --- Signals ---
@app.get("/signals")
def signals(limit: int = Query(10, ge=1, le=50)):
    symbols = ["AAPL", "TSLA", "NVDA", "MSFT", "AMZN", "META"]
    items = []
    for s in symbols[:limit]:
        side = random.choice(["BUY", "SELL", "HOLD"])
        items.append({
            "symbol": s,
            "side": side,
            "confidence": round(random.uniform(0.55, 0.92), 2),
            "reason": "Demo signal (wire backend logic later).",
            "ts": datetime.utcnow().isoformat(),
        })
    return {"items": items}

# --- Watchlist ---
WATCHLIST = ["AAPL", "TSLA", "NVDA"]

@app.get("/watchlist")
def get_watchlist():
    return {"items": [{"symbol": s} for s in WATCHLIST]}

@app.post("/watchlist/add")
def add_watchlist(symbol: str = Query(..., min_length=1)):
    sym = symbol.upper().strip()
    if sym not in WATCHLIST:
        WATCHLIST.append(sym)
    return {"items": [{"symbol": s} for s in WATCHLIST]}

@app.post("/watchlist/remove")
def remove_watchlist(symbol: str = Query(..., min_length=1)):
    sym = symbol.upper().strip()
    if sym in WATCHLIST:
        WATCHLIST.remove(sym)
    return {"items": [{"symbol": s} for s in WATCHLIST]}






































