import os
from dotenv import load_dotenv

load_dotenv()  # 🔥 MUST COME FIRST

import time
import hashlib
import json
import requests

from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

import alpaca_trade_api as tradeapi

# =========================================================
# ANALYSIS CONTEXT HELPERS (ADDITIVE)
# =========================================================

def iso_now():
    return datetime.now(timezone.utc).isoformat()

def normalize_score_to_10(score_0_100: float) -> float:
    try:
        return round(max(1.0, min(10.0, score_0_100 / 10)), 1)
    except Exception:
        return 5.0

# ✅ NEW: needed for recommendation cache TTL (additive, safe)
# ============================
# PHASE 3 DETERMINISTIC ENTROPY
# ============================
# One seed per server boot (used ONLY for tie-breaks)
_PHASE3_RUN_SEED = int(time.time()) & 0xFFFFFFFF

def _phase3_jitter(symbol: str) -> float:
    s = f"{_PHASE3_RUN_SEED}:{symbol}".encode()
    h = hashlib.sha256(s).hexdigest()
    v = int(h[:8], 16) / 0xFFFFFFFF
    return (v - 0.5) * 2.0  # range -1..+1

# ✅ NEW: wire indicator engine (additive; doesn't break existing keys)
from indicators import compute_indicators

# --- Env (YOUR permanent standard) ---
ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")
ALPACA_DATA_BASE_URL = os.getenv("ALPACA_DATA_BASE_URL", "https://data.alpaca.markets")
ALPACA_DATA_FEED = os.getenv("ALPACA_DATA_FEED", "sip")
ALPACA_TRADING_BASE_URL = os.getenv("ALPACA_TRADING_BASE_URL", "https://paper-api.alpaca.markets")

# Map to alpaca_trade_api expected env var names (prevents “Key ID must be given…”)
if ALPACA_API_KEY and not os.getenv("APCA_API_KEY_ID"):
    os.environ["APCA_API_KEY_ID"] = ALPACA_API_KEY
if ALPACA_SECRET_KEY and not os.getenv("APCA_API_SECRET_KEY"):
    os.environ["APCA_API_SECRET_KEY"] = ALPACA_SECRET_KEY


def require_keys():
    if not os.getenv("APCA_API_KEY_ID") or not os.getenv("APCA_API_SECRET_KEY"):
        raise HTTPException(status_code=500, detail="Missing ALPACA_API_KEY / ALPACA_SECRET_KEY in .env")


def trade_client() -> tradeapi.REST:
    require_keys()
    return tradeapi.REST(
        key_id=os.getenv("APCA_API_KEY_ID"),
        secret_key=os.getenv("APCA_API_SECRET_KEY"),
        base_url=ALPACA_TRADING_BASE_URL,
        api_version="v2",
    )


def data_headers() -> Dict[str, str]:
    require_keys()
    return {
        "APCA-API-KEY-ID": os.getenv("APCA_API_KEY_ID"),
        "APCA-API-SECRET-KEY": os.getenv("APCA_API_SECRET_KEY"),
        "Accept": "application/json",
    }


app = FastAPI(title="StackIQ API", version="1.0.0")

# CORS for local Vite
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------
# Core endpoints expected by frontend
# -----------------------------

@app.get("/health")
def health():
    return {
        "ok": True,
        "feed": ALPACA_DATA_FEED,
        "data_base": ALPACA_DATA_BASE_URL,
        "trading_base": ALPACA_TRADING_BASE_URL,
        "ts": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/clock")
def clock():
    try:
        c = trade_client().get_clock()
        return {
            "is_open": bool(c.is_open),
            "next_open": c.next_open.isoformat() if c.next_open else None,
            "next_close": c.next_close.isoformat() if c.next_close else None,
            "timestamp": c.timestamp.isoformat() if c.timestamp else None,
            "updated": datetime.now(timezone.utc).isoformat(),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =========================================================
# MARKET STATE RESOLUTION (ADDITIVE)
# =========================================================

def resolve_market_state(trade_client):
    try:
        clock = trade_client.get_clock()

        if clock.is_open:
            return {
                "state": "OPEN",
                "chart_mode": "LIVE",
                "label": "Market open — live data"
            }

        return {
            "state": "CLOSED",
            "chart_mode": "LAST_SESSION",
            "label": "Market closed — showing last session"
        }

    except Exception:
        return {
            "state": "UNKNOWN",
            "chart_mode": "EXPLANATION_ONLY",
            "label": "Market status unavailable"
        }


def clock():
    try:
        c = trade_client().get_clock()
        return {
            "is_open": bool(c.is_open),
            "next_open": c.next_open.isoformat() if c.next_open else None,
            "next_close": c.next_close.isoformat() if c.next_close else None,
            "timestamp": c.timestamp.isoformat() if c.timestamp else None,
            "updated": datetime.now(timezone.utc).isoformat(),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/account")
def account():
    try:
        a = trade_client().get_account()
        return {
            "status": getattr(a, "status", None),
            "cash": float(getattr(a, "cash", 0) or 0),
            "equity": float(getattr(a, "equity", 0) or 0),
            "portfolio_value": float(getattr(a, "portfolio_value", 0) or 0),
            "buying_power": float(getattr(a, "buying_power", 0) or 0),
            "updated": datetime.now(timezone.utc).isoformat(),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# -----------------------------
# Movers (KEEP WORKING EXACTLY)
# -----------------------------

def _get_snapshot(symbol: str) -> Dict[str, Any]:
    url = f"{ALPACA_DATA_BASE_URL.rstrip('/')}/v2/stocks/{symbol}/snapshot"
    r = requests.get(url, headers=data_headers(), timeout=15)
    if r.status_code != 200:
        raise HTTPException(status_code=502, detail=f"Alpaca snapshot error for {symbol}: {r.status_code} {r.text}")
    return r.json() or {}


_DEFAULT_MOVERS_SYMBOLS = "AAPL,MSFT,NVDA,TSLA,AMZN,META,GOOGL,SPY,QQQ,AMD"


def _to_float(x):
    try:
        return float(x)
    except Exception:
        return None


def _normalize_pct(v):
    """
    Alpaca screener movers may return % as:
      - already percent (e.g., 2.34)
      - decimal (e.g., 0.0234)
    We'll normalize to percent.
    """
    f = _to_float(v)
    if f is None:
        return None
    # Heuristic: if it's a tiny decimal, treat as fraction.
    if -1.5 <= f <= 1.5:
        return f * 100.0
    return f
# ============================
# app.py — PART 2 / 3
# FULL FILE TRANSMISSION
# NO OMISSIONS
# NO EDITS
# NO REORDERING
# ============================

def _try_real_movers_from_screener() -> List[Dict[str, Any]]:
    """
    Uses Alpaca Market Data screener endpoint:
      GET /v1beta1/screener/stocks/movers
    """
    url = f"{ALPACA_DATA_BASE_URL.rstrip('/')}/v1beta1/screener/stocks/movers"
    r = requests.get(url, headers=data_headers(), timeout=15)
    if r.status_code != 200:
        raise Exception(f"screener movers error {r.status_code}: {r.text[:200]}")

    data = r.json() or {}

    # Common shapes we might see
    gainers = data.get("gainers") or data.get("top_gainers") or (data.get("movers") or {}).get("gainers") or []
    losers = data.get("losers") or data.get("top_losers") or (data.get("movers") or {}).get("losers") or []

    combined = []
    if isinstance(gainers, list):
        combined.extend(gainers)
    if isinstance(losers, list):
        combined.extend(losers)

    out: List[Dict[str, Any]] = []
    for it in combined:
        if not isinstance(it, dict):
            continue

        sym = (it.get("symbol") or it.get("ticker") or it.get("s") or "").strip().upper()
        if not sym:
            continue

        last = (
            it.get("price")
            or it.get("last_price")
            or it.get("last")
            or it.get("close")
            or it.get("c")
        )

        ch_pct = (
            it.get("percent_change")
            or it.get("change_percent")
            or it.get("change_pct")
            or it.get("pct_change")
            or it.get("p")
        )

        vol = it.get("volume") or it.get("v")

        out.append(
            {
                "symbol": sym,
                "last": _to_float(last),
                "prev": None,
                "open": None,
                "high": None,
                "low": None,
                "change_percent": _normalize_pct(ch_pct),
                "volume": _to_float(vol),
                "source": "screener_movers",
            }
        )

    # Sort by absolute % move, biggest first (same as your old behavior)
    def key_fn(x):
        v = x.get("change_percent")
        return abs(v) if isinstance(v, (int, float)) else -1

    out.sort(key=key_fn, reverse=True)
    return out


@app.get("/movers")
def movers(
    symbols: str = Query(
        default=_DEFAULT_MOVERS_SYMBOLS,
        description="Comma-separated symbols to compute movers for",
    )
):
    # If frontend is calling the default list, use REAL movers (Option A),
    # otherwise keep the old snapshot-computed behavior for custom symbol lists.
    symbols_clean = (symbols or "").strip()
    use_real = symbols_clean.replace(" ", "") == _DEFAULT_MOVERS_SYMBOLS.replace(" ", "")

    if use_real:
        try:
            items = _try_real_movers_from_screener()
            # keep response shape identical
            return {"items": items[:10]}
        except Exception:
            # Hard fallback: keep current working behavior
            pass

    syms = [s.strip().upper() for s in (symbols or "").split(",") if s.strip()]
    if not syms:
        return {"items": []}

    items: List[Dict[str, Any]] = []
    for sym in syms:
        try:
            snap = _get_snapshot(sym)

            latest_trade = snap.get("latestTrade") or {}
            daily_bar = snap.get("dailyBar") or {}
            prev_bar = snap.get("prevDailyBar") or {}

            last = latest_trade.get("p") or daily_bar.get("c")
            open_ = daily_bar.get("o")
            high = daily_bar.get("h")
            low = daily_bar.get("l")
            volume = daily_bar.get("v")

            prev_close = prev_bar.get("c")
            change_percent = None
            if prev_close not in (None, 0) and daily_bar.get("c") is not None:
                change_percent = ((daily_bar["c"] - prev_close) / prev_close) * 100

            items.append(
                {
                    "symbol": sym,
                    "last": last,
                    "prev": prev_close,
                    "open": open_,
                    "high": high,
                    "low": low,
                    "change_percent": change_percent,
                    "volume": volume,
                    "source": f"snapshot_{ALPACA_DATA_FEED}",
                }
            )
        except Exception as e:
            items.append(
                {
                    "symbol": sym,
                    "last": None,
                    "prev": None,
                    "open": None,
                    "high": None,
                    "low": None,
                    "change_percent": None,
                    "volume": None,
                    "source": "error",
                    "error": str(e),
                }
            )

    def key_fn(x):
        v = x.get("change_percent")
        return abs(v) if isinstance(v, (int, float)) else -1

    items.sort(key=key_fn, reverse=True)
    return {"items": items}


# -----------------------------
# News (KEEP WORKING)
# -----------------------------

@app.get("/news")
def news(symbols: str = Query(default="NVDA,TSLA,AAPL,MSFT", description="Comma-separated symbols")):
    syms = [s.strip().upper() for s in (symbols or "").split(",") if s.strip()]
    url = f"{ALPACA_DATA_BASE_URL.rstrip('/')}/v1beta1/news"
    params = {
        "symbols": ",".join(syms) if syms else None,
        "limit": 20,
    }
    try:
        r = requests.get(url, headers=data_headers(), params=params, timeout=15)
        if r.status_code != 200:
            return {"items": []}
        data = r.json() or {}
        items = data.get("news") or data.get("items") or []
        out = []
        for n in items:
            out.append(
                {
                    "headline": n.get("headline") or n.get("title"),
                    "source": n.get("source"),
                    "url": n.get("url"),
                    "summary": n.get("summary"),
                    "published_at": n.get("created_at") or n.get("published_at"),
                    "ticker": (n.get("symbols") or [None])[0] if isinstance(n.get("symbols"), list) else None,
                }
            )
        return {"items": out}
    except Exception:
        return {"items": []}


from datetime import datetime as _ndt, timezone as _ntz
import math

# Sector-aware keyword map (deterministic)
_SECTOR_KEYWORDS = {
    "tech": [
        "ai", "chip", "semiconductor", "cloud", "software", "data",
        "upgrade", "price target", "beats", "guidance"
    ],
    "biotech": [
        "fda", "approval", "trial", "clinical", "phase",
        "drug", "treatment"
    ],
    "energy": [
        "oil", "gas", "production", "opec", "reserves",
        "contract", "supply"
    ],
    "general": [
        "earnings", "revenue", "profit", "loss",
        "merger", "acquisition", "buyback", "dividend",
        "investigation", "lawsuit"
    ],
}

# Simple symbol → sector heuristic (extend later if needed)
def _infer_sector(symbol: str) -> str:
    s = symbol.upper()
    if s in {"NVDA", "AMD", "AAPL", "MSFT", "META", "GOOGL", "AMZN"}:
        return "tech"
    if s.startswith(("BIO", "MRNA", "VRTX")):
        return "biotech"
    if s.startswith(("XOM", "CVX", "OXY")):
        return "energy"
    return "general"


def _hours_ago(ts: str) -> float:
    try:
        if not ts:
            return 999.0
        dt = _ndt.fromisoformat(ts.replace("Z", "+00:00"))
        delta = _ndt.now(_ntz.utc) - dt
        return delta.total_seconds() / 3600.0
    except Exception:
        return 999.0


def _recency_weight(hours_ago: float) -> float:
    if hours_ago <= 24:
        return 1.0
    if hours_ago <= 72:
        return 0.6
    if hours_ago <= 168:
        return 0.3
    return 0.1


def _news_catalyst_score(items: list, symbol: str = "") -> int:
    """
    Deterministic news catalyst scoring with:
    - Recency decay
    - Duplicate headline penalty
    - Sector-aware keywords
    """
    if not items:
        return 0

    sector = _infer_sector(symbol)
    keywords = set(_SECTOR_KEYWORDS.get(sector, [])) | set(_SECTOR_KEYWORDS["general"])

    seen = {}
    score = 0.0

    for n in items[:10]:
        headline = (n.get("headline") or "").strip().lower()
        summary = (n.get("summary") or "").strip().lower()
        text = f"{headline} {summary}"

        if not text:
            continue

        # Duplicate / recycled headline penalty
        seen[headline] = seen.get(headline, 0) + 1
        duplicate_penalty = 1.0 / seen[headline]

        # Keyword hits
        hits = sum(1 for k in keywords if k in text)
        if hits == 0:
            continue

        # Recency decay
        hours = _hours_ago(n.get("published_at"))
        recency = _recency_weight(hours)

        # Base contribution
        contrib = hits * 8.0
        contrib *= recency
        contrib *= duplicate_penalty

        score += contrib

    # Hard cap — news should never dominate the engine
    return int(min(math.ceil(score), 30))

def llm_news_digest(symbol: str, news_items: list) -> dict:
    try:
        # If feature disabled, return neutral structure
        if os.getenv("USE_LLM_NEWS", "false").lower() not in ("1", "true", "yes"):
            return {"sentiment": "neutral", "score": 0, "confidence": 0.0, "summary": ""}

        if not news_items:
            return {"sentiment": "neutral", "score": 0, "confidence": 0.0, "summary": ""}

        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            return {"sentiment": "neutral", "score": 0, "confidence": 0.0, "summary": ""}

        snippets = []
        for n in (news_items or [])[:8]:
            h = (n.get("headline") or "").strip()
            s = (n.get("summary") or "").strip()
            if h and s:
                snippets.append(f"- {h} — {s}")
            elif h:
                snippets.append(f"- {h}")

        prompt = (
            f"You are a concise financial assistant. Given the ticker '{symbol}' and the following recent news items:\n\n"
            + "\n".join(snippets)
            + "\n\nRespond ONLY with a JSON object with keys: sentiment (one of 'bullish','bearish','neutral'), "
            "score (integer between -20 and 20), confidence (0.0-1.0), summary (one short sentence)."
        )

        try:
            import openai as _openai
            _openai.api_key = api_key
            # Use a ChatCompletion-compatible call; tolerant to client versions
            resp = _openai.ChatCompletion.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=120,
                temperature=0.0,
                n=1,
                timeout=15,
            )
            content = ""
            if isinstance(resp, dict):
                choices = resp.get("choices") or []
                if choices and isinstance(choices, list):
                    content = choices[0].get("message", {}).get("content") or choices[0].get("text", "") or ""
            else:
                content = str(resp)
        except Exception:
            return {"sentiment": "neutral", "score": 0, "confidence": 0.0, "summary": ""}

        try:
            j = json.loads(content.strip())
            sentiment = j.get("sentiment", "neutral")
            score = int(j.get("score", 0))
            confidence = float(j.get("confidence", 0.0))
            summary = str(j.get("summary", "")).strip()
        except Exception:
            return {"sentiment": "neutral", "score": 0, "confidence": 0.0, "summary": ""}

        if sentiment not in ("bullish", "bearish", "neutral"):
            sentiment = "neutral"
        score = max(-20, min(20, score))
        confidence = max(0.0, min(1.0, confidence))
        summary = summary[:200] if summary else ""

        return {"sentiment": sentiment, "score": score, "confidence": confidence, "summary": summary}
    except Exception:
        return {"sentiment": "neutral", "score": 0, "confidence": 0.0, "summary": ""}

# -----------------------------
# Analyze (FIXED so it works)
# -----------------------------

def _clamp(x, lo, hi):
    return max(lo, min(hi, x))

def _sma(vals):
    return sum(vals) / len(vals) if vals else None

def _ema(series, period: int):
    if series is None or len(series) < period:
        return None
    k = 2 / (period + 1)
    ema = series[0]
    for v in series[1:]:
        ema = v * k + ema * (1 - k)
    return ema

def _atr(highs, lows, closes, period: int = 14):
    if len(closes) < period + 1:
        return None
    trs = []
    for i in range(1, len(closes)):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1]),
        )
        trs.append(tr)
    if len(trs) < period:
        return None
    return sum(trs[-period:]) / period

def _iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def _get_daily_bars(symbol: str, limit: int = 260):
    """
    Make bars retrieval reliable so snapshot fallback only happens when truly necessary.
    """
    sym = (symbol or "").strip().upper()
    if not sym:
        return []

    base = ALPACA_DATA_BASE_URL.rstrip("/")

    def _request(url: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        r = requests.get(url, headers=data_headers(), params=params, timeout=20)
        if r.status_code != 200:
            raise HTTPException(status_code=502, detail=f"Alpaca bars error: {r.status_code} {r.text[:200]}")
        data = r.json() or {}
        bars_obj = data.get("bars")

        if isinstance(bars_obj, dict):
            return bars_obj.get(sym, []) or []
        if isinstance(bars_obj, list):
            return bars_obj

        return []

    common_params = {
        "timeframe": "1Day",
        "limit": int(limit),
        "feed": ALPACA_DATA_FEED,
        "adjustment": "raw",
    }

    url_multi = f"{base}/v2/stocks/bars"
    params_multi = dict(common_params)
    params_multi["symbols"] = sym
    bars = _request(url_multi, params_multi)
    if isinstance(bars, list) and len(bars) >= 20:
        return bars

    url_sym = f"{base}/v2/stocks/{sym}/bars"
    params_sym = dict(common_params)
    bars2 = _request(url_sym, params_sym)
    if isinstance(bars2, list) and len(bars2) >= 20:
        return bars2

    now = datetime.now(timezone.utc)
    start = now - timedelta(days=400)

    params_window = dict(common_params)
    params_window["start"] = _iso_z(start)
    params_window["end"] = _iso_z(now)

    bars3 = _request(url_sym, params_window)
    if isinstance(bars3, list) and len(bars3) >= 20:
        return bars3

    params_window_multi = dict(params_window)
    params_window_multi["symbols"] = sym
    bars4 = _request(url_multi, params_window_multi)
    if isinstance(bars4, list) and len(bars4) >= 20:
        return bars4

    best = bars4 if isinstance(bars4, list) and len(bars4) > 0 else (
        bars3 if isinstance(bars3, list) and len(bars3) > 0 else (
            bars2 if isinstance(bars2, list) and len(bars2) > 0 else bars
        )
    )
    return best or []


def _is_num(x) -> bool:
    return isinstance(x, (int, float)) and x == x


def _clean_bars(bars: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for b in (bars or []):
        if not isinstance(b, dict):
            continue
        o = b.get("o")
        h = b.get("h")
        l = b.get("l")
        c = b.get("c")
        if not (_is_num(o) and _is_num(h) and _is_num(l) and _is_num(c)):
            continue
        out.append(b)
    return out
# ============================
# app.py — PART 3 / 3
# FULL FILE TRANSMISSION
# NO OMISSIONS
# NO EDITS
# NO REORDERING
# ============================

@app.get("/analyze/{symbol}")
def analyze(symbol: str):
    sym = symbol.strip().upper()
    if not sym or len(sym) > 10:
        raise HTTPException(status_code=400, detail="Invalid symbol")

    # ✅ NEW: safe market-open flag (never breaks analyze if clock fails)
    is_open = None
    try:
        is_open = bool(trade_client().get_clock().is_open)
    except Exception:
        is_open = None

    bars_raw = _get_daily_bars(sym, limit=260)
    bars = _clean_bars(bars_raw)

    # Snapshot fallback only when truly necessary
    if not bars or len(bars) < 2:
        snap = _get_snapshot(sym)
        latest_trade = snap.get("latestTrade") or {}
        daily_bar = snap.get("dailyBar") or {}
        prev_bar = snap.get("prevDailyBar") or {}

        last = latest_trade.get("p") or daily_bar.get("c")
        prev_close = prev_bar.get("c")

        change_pct = None
        if prev_close not in (None, 0) and daily_bar.get("c") is not None:
            change_pct = ((daily_bar["c"] - prev_close) / prev_close) * 100

        score = 50
        reasons = [{"factor": "data", "impact": 0, "detail": "Using snapshot fallback (insufficient valid bars)."}]

        if isinstance(change_pct, (int, float)):
            if change_pct >= 2:
                score += 8
                reasons.append({"factor": "momentum", "impact": 8, "detail": f"Strong daily move ({change_pct:.2f}%)"})
            elif change_pct <= -2:
                score -= 8
                reasons.append({"factor": "momentum", "impact": -8, "detail": f"Weak daily move ({change_pct:.2f}%)"})

        score = int(_clamp(score, 0, 100))

        if score >= 70:
            rec = "BUY"
        elif score <= 40:
            rec = "HOLD / CAUTION"
        else:
            rec = "WATCH"

        h = daily_bar.get("h")
        l = daily_bar.get("l")
        atr_guess = (h - l) if (isinstance(h, (int, float)) and isinstance(l, (int, float)) and h > l) else (0.02 * last if last else 1)

        buy_zone = None
        targets = []
        if isinstance(last, (int, float)):
            buy_zone = {"low": round(last - 0.4 * atr_guess, 2), "high": round(last + 0.1 * atr_guess, 2)}
            targets = [round(last + 1.0 * atr_guess, 2), round(last + 2.0 * atr_guess, 2)]

        indicators = None
        try:
            if isinstance(last, (int, float)):
                quote_for_ind = {
                    "h": daily_bar.get("h") if daily_bar.get("h") is not None else last,
                    "l": daily_bar.get("l") if daily_bar.get("l") is not None else last,
                    "pc": prev_close if prev_close is not None else last,
                    "o": daily_bar.get("o"),
                }
                indicators = compute_indicators(
                    price=float(last),
                    change_pct=float(change_pct) if isinstance(change_pct, (int, float)) else None,
                    quote=quote_for_ind,
                    is_market_open=is_open,
                )
        except Exception:
            indicators = None

        market_ctx = resolve_market_state(trade_client())

        score_10 = normalize_score_to_10(score)

        explanation = []
        if rec == "BUY":
            explanation = [
                "Risk/reward favorable at current price",
                "Momentum and structure supportive"
            ]
        elif rec.startswith("HOLD"):
            explanation = [
                "Risk elevated or timing suboptimal",
                "Waiting for confirmation"
            ]
        else:
            explanation = [
                "Mixed signals — monitoring setup"
            ]

        return {
            "symbol": sym,
            "price": round(last, 4) if isinstance(last, (int, float)) else None,
            "change_pct": round(change_pct, 3) if isinstance(change_pct, (int, float)) else None,
            "score": score,
            "recommendation": rec,
            "buy_zone": buy_zone,
            "targets": targets,
            "risk": "unknown",
            "confidence": 0.35,
            "reasons": reasons,
            "indicators": indicators,
            "engine": "StackIQ Analyze v1.0 (snapshot fallback)",
            "ts": datetime.now(timezone.utc).isoformat(),
            "analysis_id": int(time.time()),
            "analysis_context": {
                "analysis_ts": iso_now(),
                "score_10": score_10,
                "market": {
                    "state": market_ctx["state"],
                    "label": market_ctx["label"]
                },
                "charts": {
                    "mode": market_ctx["chart_mode"],
                    "can_render": market_ctx["chart_mode"] != "EXPLANATION_ONLY"
                },
                "explanation": explanation
            },
        }

    closes = [b["c"] for b in bars]
    highs = [b["h"] for b in bars]
    lows = [b["l"] for b in bars]
    vols = [b.get("v", 0) for b in bars]

    last = closes[-1]
    prev = closes[-2]
    change_pct = ((last - prev) / prev) if prev else 0.0

    n = len(closes)
    limited = n < 60

    ema21 = _ema(closes[-120:], 21) if n >= 21 else None
    ema50 = _ema(closes[-180:], 50) if n >= 50 else None
    ema200 = _ema(closes[-260:], 200) if n >= 200 else None

    atr14 = _atr(highs, lows, closes, 14) if n >= 15 else None
    atr_pct = (atr14 / last) if (atr14 and last) else None

    vol_ratio = None
    if n >= 21:
        v20 = _sma(vols[-21:-1])
        if v20 and v20 > 0:
            vol_ratio = vols[-1] / v20

    trend_strength = 0.0
    trend_up = False

    if ema21 and ema50:
        trend_strength += 0.5
        if ema21 > ema50:
            trend_up = True
            trend_strength += 0.2

    if ema50 and ema200:
        trend_strength += 0.2
        if ema50 > ema200:
            trend_strength += 0.1

    if ema21:
        trend_strength += 0.2 if last >= ema21 else 0.0

    trend_strength = _clamp(trend_strength, 0.0, 1.0)

    score = 50
    reasons = []

    if trend_up:
        add = int(18 * trend_strength)
        score += add
        reasons.append({"factor": "trend", "impact": add, "detail": "Bullish EMA alignment + price above key averages"})
    else:
        score -= 10
        reasons.append({"factor": "trend", "impact": -10, "detail": "No bullish EMA alignment"})

    if change_pct >= 0.02:
        score += 6
        reasons.append({"factor": "momentum", "impact": 6, "detail": f"Strong daily momentum ({change_pct*100:.2f}%)"})
    elif change_pct <= -0.02:
        score -= 6
        reasons.append({"factor": "momentum", "impact": -6, "detail": f"Weak daily momentum ({change_pct*100:.2f}%)"})

    if vol_ratio is not None:
        if vol_ratio >= 1.4:
            score += 5
            reasons.append({"factor": "volume", "impact": 5, "detail": f"Volume spike ({vol_ratio:.2f}x 20d avg)"})
        elif vol_ratio <= 0.6:
            score -= 3
            reasons.append({"factor": "volume", "impact": -3, "detail": f"Low volume ({vol_ratio:.2f}x 20d avg)"})

    if atr_pct is not None:
        if atr_pct > 0.06:
            score -= 8
            reasons.append({"factor": "volatility", "impact": -8, "detail": f"High ATR ({atr_pct*100:.2f}%)"})
        elif atr_pct > 0.04:
            score -= 4
            reasons.append({"factor": "volatility", "impact": -4, "detail": f"Elevated ATR ({atr_pct*100:.2f}%)"})

    if limited:
        score = min(score, 68)
        reasons.append({"factor": "data", "impact": 0, "detail": f"Limited daily history ({n} bars). Confidence reduced."})

    score = int(_clamp(score, 0, 100))

    if score >= 78:
        rec = "STRONG BUY"
    elif score >= 64:
        rec = "BUY"
    elif score >= 52:
        rec = "WATCH"
    elif score >= 40:
        rec = "HOLD / CAUTION"
    else:
        rec = "AVOID"

    anchor = ema21 if ema21 is not None else last
    atr = atr14 if atr14 is not None else max(0.012 * last, 0.5)

    buy_low = anchor - (0.55 - 0.20 * trend_strength) * atr
    buy_high = anchor + (0.10 + 0.10 * trend_strength) * atr

    t1 = anchor + (1.2 + 0.8 * trend_strength) * atr
    t2 = anchor + (2.2 + 1.0 * trend_strength) * atr
    t3 = anchor + (3.2 + 1.2 * trend_strength) * atr

    buy_zone = {"low": round(buy_low, 2), "high": round(buy_high, 2)}
    targets = [round(t1, 2), round(t2, 2), round(t3, 2)]

    risk = "medium"
    if atr_pct is None:
        risk = "unknown"
    elif atr_pct > 0.06:
        risk = "high"
    elif atr_pct < 0.025:
        risk = "low"

    score_strength = abs(score - 50) / 50.0
    base_conf = 0.25
    score_conf = 0.40 * score_strength
    trend_conf = 0.35 * trend_strength

    vol_penalty = 0.0
    if atr_pct is not None:
        if atr_pct > 0.06:
            vol_penalty = 0.20
        elif atr_pct > 0.045:
            vol_penalty = 0.12
        elif atr_pct > 0.03:
            vol_penalty = 0.06

    data_penalty = 0.12 if limited else 0.0

    conf = base_conf + score_conf + trend_conf - vol_penalty - data_penalty
    conf = _clamp(conf, 0.20, 0.90)

    indicators = None
    try:
        quote_for_ind = {
            "h": highs[-1],
            "l": lows[-1],
            "pc": prev,
            "o": bars[-1].get("o"),
        }
        indicators = compute_indicators(
            price=float(last),
            change_pct=float(change_pct * 100.0),
            quote=quote_for_ind,
            is_market_open=is_open,
        )
    except Exception:
        indicators = None

    return {
        "symbol": sym,
        "price": round(last, 4),
        "change_pct": round(change_pct * 100, 3),
        "score": score,
        "recommendation": rec,
        "buy_zone": buy_zone,
        "targets": targets,
        "risk": risk,
        "confidence": round(conf, 2),
        "reasons": reasons,
        "indicators": indicators,
        "engine": "StackIQ Analyze v1.0 (deterministic multi-factor)",
        "ts": datetime.now(timezone.utc).isoformat(),
    }


# ======================================================================
# PHASE 2 FULL FIX PATCH (APPENDED ONLY — DOES NOT MODIFY ABOVE)
# ======================================================================

from datetime import datetime as _dt, timezone as _tz
from typing import Optional as _Optional, Any as _Any, Dict as _Dict, List as _List

def _phase2_iso_now() -> str:
    return _dt.now(_tz.utc).isoformat()

def _phase2_parse_ts_to_epoch_sec(tval: _Any) -> int:
    try:
        if isinstance(tval, (int, float)):
            return int(tval)
        if isinstance(tval, str) and tval:
            s = tval.strip()
            if s.endswith("Z"):
                s = s.replace("Z", "+00:00")
            dt = _dt.fromisoformat(s)
            return int(dt.timestamp())
    except Exception:
        pass
    return 0

def _phase2_safe_float(x: _Any, default: _Optional[float]=None) -> _Optional[float]:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default

def _phase2_market_state() -> _Dict[str, _Any]:
    try:
        c = trade_client().get_clock()
        if bool(getattr(c, "is_open", False)):
            return {"state": "OPEN", "chart_mode": "LIVE", "label": "Market open — live data", "is_open": True}
        return {"state": "CLOSED", "chart_mode": "LAST_SESSION", "label": "Market closed — showing last session", "is_open": False}
    except Exception:
        return {"state": "UNKNOWN", "chart_mode": "EXPLANATION_ONLY", "label": "Market status unavailable", "is_open": None}

# -----------------------------
# DEFINITIVE CLOCK (overrides earlier duplicates)
# -----------------------------
@app.get("/clock", include_in_schema=True)
def clock_phase2():
    try:
        c = trade_client().get_clock()
        return {
            "is_open": bool(getattr(c, "is_open", False)),
            "next_open": c.next_open.isoformat() if getattr(c, "next_open", None) else None,
            "next_close": c.next_close.isoformat() if getattr(c, "next_close", None) else None,
            "timestamp": c.timestamp.isoformat() if getattr(c, "timestamp", None) else None,
            "updated": _phase2_iso_now(),
            "market": _phase2_market_state(),
        }
    except Exception as e:
        return {
            "is_open": False,
            "next_open": None,
            "next_close": None,
            "timestamp": None,
            "updated": _phase2_iso_now(),
            "market": _phase2_market_state(),
            "error": str(e),
        }

# -----------------------------
# BARS (charts) — stable daily candles
# -----------------------------
@app.get("/bars/{symbol}")
def bars(symbol: str, tf: str = "1D", limit: int = 260):
    sym = (symbol or "").strip().upper()
    if not sym or len(sym) > 10:
        raise HTTPException(status_code=400, detail="Invalid symbol")

    market = _phase2_market_state()

    try:
        bars_raw = _get_daily_bars(sym, limit=int(limit))
    except Exception:
        bars_raw = []

    bars_clean = _clean_bars(bars_raw) if "_clean_bars" in globals() else (bars_raw or [])

    items: _List[_Dict[str, _Any]] = []
    for b in (bars_clean or []):
        if not isinstance(b, dict):
            continue
        items.append(
            {
                "t": _phase2_parse_ts_to_epoch_sec(b.get("t")),
                "o": _phase2_safe_float(b.get("o"), 0.0),
                "h": _phase2_safe_float(b.get("h"), 0.0),
                "l": _phase2_safe_float(b.get("l"), 0.0),
                "c": _phase2_safe_float(b.get("c"), 0.0),
                "v": _phase2_safe_float(b.get("v"), 0.0),
            }
        )

    can_render = bool(items) and any(i.get("t") for i in items)
    mode = market.get("chart_mode") or ("LAST_SESSION" if can_render else "EXPLANATION_ONLY")
    if mode == "EXPLANATION_ONLY" and can_render:
        mode = "LAST_SESSION"

    return {
        "symbol": sym,
        "tf": tf,
        "items": items,
        "mode": mode,
        "can_render": bool(can_render),
        "market": market,
        "updated_at": _phase2_iso_now(),
    }

# -----------------------------
# QUOTE — stable last price for portfolio refresh
# -----------------------------
@app.get("/quote/{symbol}")
def quote(symbol: str):
    sym = (symbol or "").strip().upper()
    if not sym or len(sym) > 10:
        raise HTTPException(status_code=400, detail="Invalid symbol")
    try:
        snap = _get_snapshot(sym)
        latest_trade = snap.get("latestTrade") or {}
        daily_bar = snap.get("dailyBar") or {}
        prev_bar = snap.get("prevDailyBar") or {}
        last = latest_trade.get("p") or daily_bar.get("c")
        prev = prev_bar.get("c")
        chg = None
        if isinstance(prev, (int, float)) and prev not in (None, 0) and isinstance(daily_bar.get("c"), (int, float)):
            chg = ((daily_bar["c"] - prev) / prev) * 100.0
        return {
            "symbol": sym,
            "price": _phase2_safe_float(last, None),
            "change_pct": round(chg, 3) if isinstance(chg, (int, float)) else None,
            "updated_at": _phase2_iso_now(),
        }
    except Exception as e:
        return {"symbol": sym, "price": None, "change_pct": None, "updated_at": _phase2_iso_now(), "error": str(e)}

# -----------------------------
# DEFINITIVE BEST PICK (PHASE 3 — DETERMINISTIC, NO EXTERNAL AI)
# -----------------------------

@app.get("/best_pick", include_in_schema=True)
def best_pick_phase3():
    """
    Golden-stock selector (short-term 1–3 trading days)

    Guarantees:
    - No external AI calls by default
    - Phase-3 deterministic engine unchanged unless USE_LLM_NEWS=true
    - Scarcity: returns 0 or 1 pick
    """

    import inspect
    from math import fabs

    market = _phase2_market_state()

    def _unwrap_default(v):
        try:
            if hasattr(v, "default"):
                return v.default
        except Exception:
            pass
        return v

    def _call(fn, **kwargs):
        try:
            sig = inspect.signature(fn)
            built = {}
            for name, param in sig.parameters.items():
                if name in kwargs:
                    built[name] = kwargs[name]
                elif param.default is not inspect._empty:
                    built[name] = _unwrap_default(param.default)
            return fn(**built)
        except Exception:
            return fn(**kwargs)

    def _iso_now2():
        return _dt.now(_tz.utc).isoformat()

    def authoritative_price(symbol):
        try:
            snap = trade_client().get_snapshot(symbol)
            if snap and snap.latest_trade and getattr(snap.latest_trade, "price", None):
                return float(snap.latest_trade.price)
            if snap and snap.daily_bar and getattr(snap.daily_bar, "close", None):
                return float(snap.daily_bar.close)
        except Exception:
            pass
        return None

    def compute_recent_change(symbol):
        try:
            bars_raw = _get_daily_bars(symbol, limit=5)
            bars0 = _clean_bars(bars_raw)
            if len(bars0) < 2:
                return None
            lastc = bars0[-1].get("c")
            prevc = bars0[-2].get("c")
            if not isinstance(lastc, (int, float)) or not isinstance(prevc, (int, float)) or prevc <= 0:
                return None
            return round(((lastc - prevc) / prevc) * 100.0, 2)
        except Exception:
            return None

    universe = set()

    try:
        movers_data = _call(movers)
        items = movers_data.get("items", []) if isinstance(movers_data, dict) else []
        for it in items[:30]:
            s = (it.get("symbol") or "").strip().upper()
            if s:
                universe.add(s)
    except Exception:
        pass

    if not universe:
        universe.update(["AAPL", "MSFT", "NVDA", "AMD", "TSLA", "META", "AMZN"])

    candidates = []

    for sym in list(universe)[:50]:
        try:
            res = _call(analyze, symbol=sym)
        except Exception:
            continue

        if not isinstance(res, dict):
            continue

        price = res.get("price")
        if not isinstance(price, (int, float)) or price < 2.0:
            continue

        auth = authoritative_price(sym)
        if auth and fabs(auth - float(price)) / auth > 0.15:
            continue

        bz = res.get("buy_zone") or {}
        low = _phase2_safe_float(bz.get("low"), None)
        high = _phase2_safe_float(bz.get("high"), None)
        if not isinstance(low, (int, float)) or not isinstance(high, (int, float)):
            continue
        if low <= 0 or high <= 0 or high <= low:
            continue

        # Kill fake zones: must be close to current price (tight band)
        if low < price * 0.93 or high > price * 1.07:
            continue

        # Must be in/near zone
        if not (low <= price <= high * 1.02):
            continue

        # 1–3 day only: reject if already ripped or dumped hard on last day
        chg = compute_recent_change(sym)
        if isinstance(chg, (int, float)):
            if chg > 8.5 or chg < -8.5:
                continue

        reasons = res.get("reasons") or []
        factors = [str(r.get("factor", "")).lower() for r in reasons]

        # Pre-move structural score
        score = 0
        score += 30 if any("volatility" in f for f in factors) else 0
        score += 25 if any("momentum" in f for f in factors) else 0
        score += 20 if any("volume" in f for f in factors) else 0
        score += 15 if any("trend" in f for f in factors) else 0

        if score < 55:
            continue

        # Buy-zone context nudge: closer to zone center = better
        center = (low + high) / 2.0
        if center > 0:
            dist = abs(price - center) / center
            if dist <= 0.01:
                score += 6
            elif dist <= 0.02:
                score += 3

        # ============================
        # NEWS CATALYST (DETERMINISTIC, NO LLM)
        # ============================
        news_items = news(symbols=sym).get("items", [])
        catalyst = _news_catalyst_score(news_items, symbol=sym)
        score += int(catalyst * 0.6)  # max +18

        # Attach for transparency
        res["_news"] = news_items

        # FINALIZE (ONLY ONCE)
        res["_pre_move_score"] = int(_clamp(score, 0, 100))
        candidates.append(res)

    # =========================================================
    # FINAL SELECTION (SCARCE)
    # =========================================================
    if not candidates:
        return {
            "status": "no_pick",
            "message": "No golden stock found — market lacks clean pre-move setups.",
            "market": market,
            "updated_at": _iso_now2(),
        }

    # tie-break variability via tiny jitter; never dominates the score
    def _sort_key(x):
        return (
            x.get("_pre_move_score", 0),
            _phase3_jitter(x.get("symbol",""))
        )

    candidates.sort(key=_sort_key, reverse=True)
    candidates = candidates[:5]
    golden = candidates[0]

    # strict scarcity gate
    if int(golden.get("_pre_move_score") or 0) < 62:
        return {
            "status": "no_pick",
            "message": "No golden stock found — market lacks clean pre-move setups.",
            "market": market,
            "updated_at": _phase2_iso_now(),
        }

    recent_change = compute_recent_change(golden.get("symbol"))

    # Phase-4 optional LLM blending (additive ONLY; deterministic selection unchanged)
    final_score = int(golden.get("_pre_move_score") or 0)
    try:
        if os.getenv("USE_LLM_NEWS", "false").lower() in ("1", "true", "yes"):
            llm_out = llm_news_digest(golden.get("symbol"), golden.get("_news") or [])
            if isinstance(llm_out, dict):
                # attach for transparency
                golden["_news_llm"] = llm_out
                llm_score = int(_clamp(llm_out.get("score", 0), -20, 20))
                llm_conf = float(_clamp(llm_out.get("confidence", 0.0), 0.0, 1.0))
                delta = int(llm_score * llm_conf * 0.4)
                delta = max(-8, min(8, delta))
                final_score = int(_clamp(final_score + delta, 0, 100))
    except Exception:
        final_score = int(golden.get("_pre_move_score") or 0)

    # Build a simple heatmap (deterministic)
    reasons =