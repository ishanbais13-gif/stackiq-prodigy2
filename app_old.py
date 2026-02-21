import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

import requests

# Alpaca SDK (alpaca-trade-api)
from alpaca_trade_api.rest import REST

# -----------------------
# Env / Alpaca Client
# -----------------------
load_dotenv()

ALPACA_API_KEY = os.getenv("ALPACA_API_KEY", "").strip()
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY", "").strip()
ALPACA_DATA_BASE_URL = os.getenv("ALPACA_DATA_BASE_URL", "https://data.alpaca.markets").strip()
ALPACA_DATA_FEED = os.getenv("ALPACA_DATA_FEED", "iex").strip()

if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
    raise RuntimeError("Missing Alpaca API keys")

# Trading base is separate from data base; use default paper endpoint unless you override it.
ALPACA_TRADING_BASE_URL = os.getenv("ALPACA_TRADING_BASE_URL", "https://paper-api.alpaca.markets").strip()

alpaca = REST(
    key_id=ALPACA_API_KEY,
    secret_key=ALPACA_SECRET_KEY,
    base_url=ALPACA_TRADING_BASE_URL,
    api_version="v2",
)

DATA_HEADERS = {
    "APCA-API-KEY-ID": ALPACA_API_KEY,
    "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
}


# -----------------------
# App
# -----------------------
app = FastAPI(title="StackIQ API", version="0.2.0")

# CORS for local dev
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@app.get("/health")
def health() -> Dict[str, Any]:
    return {
        "status": "ok",
        "ts": _now_iso(),
        "alpaca_data_base": ALPACA_DATA_BASE_URL,
        "alpaca_feed": ALPACA_DATA_FEED,
        "trading_base": ALPACA_TRADING_BASE_URL,
    }


# -----------------------
# Market clock (for top bar "Market" tab)
# -----------------------
@app.get("/market/clock")
def market_clock() -> Dict[str, Any]:
    try:
        clock = alpaca.get_clock()
        return {
            "is_open": bool(clock.is_open),
            "next_open": str(clock.next_open),
            "next_close": str(clock.next_close),
            "timestamp": _now_iso(),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch market clock: {e}")


# -----------------------
# Portfolio summary (for top bar "Portfolio" tab)
# -----------------------
@app.get("/portfolio/summary")
def portfolio_summary() -> Dict[str, Any]:
    try:
        acct = alpaca.get_account()
        return {
            "equity": float(acct.equity),
            "cash": float(acct.cash),
            "buying_power": float(acct.buying_power),
            "portfolio_value": float(acct.portfolio_value),
            "status": str(acct.status),
            "timestamp": _now_iso(),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch portfolio summary: {e}")


# -----------------------
# Top movers (real Alpaca snapshot data)
# -----------------------
@app.get("/top-movers")
def top_movers(
    limit: int = Query(12, ge=1, le=50),
) -> Dict[str, Any]:
    """
    "Top movers" using Alpaca snapshots for a liquid symbol set.
    Real data (no mock). You can expand the universe later.
    """
    universe = [
        "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "AMD",
        "NFLX", "INTC", "BAC", "JPM", "SPY", "QQQ", "IWM", "DIA",
        "PLTR", "UBER", "COIN", "SNAP", "DIS", "NKE", "KO", "PEP",
    ]

    url = f"{ALPACA_DATA_BASE_URL}/v2/stocks/snapshots"
    params = {"symbols": ",".join(universe), "feed": ALPACA_DATA_FEED}

    try:
        r = requests.get(url, headers=DATA_HEADERS, params=params, timeout=12)
        r.raise_for_status()
        data = r.json()

        movers: List[Dict[str, Any]] = []
        for sym, snap in (data or {}).items():
            daily = (snap or {}).get("dailyBar") or {}
            prev = (snap or {}).get("prevDailyBar") or {}
            last = (snap or {}).get("latestTrade") or {}

            last_price = last.get("p")
            prev_close = prev.get("c")
            day_open = daily.get("o")
            day_high = daily.get("h")
            day_low = daily.get("l")
            day_volume = daily.get("v")

            pct = None
            if prev_close and last_price:
                try:
                    pct = ((float(last_price) - float(prev_close)) / float(prev_close)) * 100.0
                except Exception:
                    pct = None

            movers.append(
                {
                    "symbol": sym,
                    "last": float(last_price) if last_price is not None else None,
                    "prev_close": float(prev_close) if prev_close is not None else None,
                    "pct_change": float(pct) if pct is not None else None,
                    "open": float(day_open) if day_open is not None else None,
                    "high": float(day_high) if day_high is not None else None,
                    "low": float(day_low) if day_low is not None else None,
                    "volume": int(day_volume) if day_volume is not None else None,
                }
            )

        # Sort by absolute % change (biggest movers)
        movers = [m for m in movers if m.get("pct_change") is not None]
        movers.sort(key=lambda x: abs(x["pct_change"]), reverse=True)

        return {"movers": movers[:limit], "timestamp": _now_iso()}

    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"Alpaca data request failed: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to compute top movers: {e}")


# -----------------------
# News (simple, real news via Alpaca)
# -----------------------
@app.get("/news")
def news(
    limit: int = Query(8, ge=1, le=50),
    symbols: Optional[str] = Query(None, description="Comma-separated tickers"),
) -> Dict[str, Any]:
    """
    Alpaca News endpoint (real data).
    If symbols omitted, returns general market news.
    """
    try:
        # Alpaca python SDK has get_news but versions differ; safest is raw request.
        url = "https://data.alpaca.markets/v1beta1/news"
        params: Dict[str, Any] = {"limit": limit}
        if symbols:
            params["symbols"] = symbols

        r = requests.get(url, headers=DATA_HEADERS, params=params, timeout=12)
        r.raise_for_status()
        items = r.json().get("news", [])
        cleaned = []
        for it in items:
            cleaned.append(
                {
                    "headline": it.get("headline"),
                    "summary": it.get("summary"),
                    "author": it.get("author"),
                    "source": it.get("source"),
                    "url": it.get("url"),
                    "created_at": it.get("created_at"),
                    "symbols": it.get("symbols", []),
                }
            )
        return {"news": cleaned, "timestamp": _now_iso()}

    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"News request failed: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch news: {e}")


