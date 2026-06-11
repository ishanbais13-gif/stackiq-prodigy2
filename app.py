from __future__ import annotations

# =========================================================
# StackIQ — CLEAN FLATTENED BACKEND (BOOT-SAFE)
# Single FastAPI app
# No duplicate routes
# No overrides
# No shadow routers
# =========================================================

from functools import lru_cache
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi import FastAPI, HTTPException, Query, Body, Depends, Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from pydantic import BaseModel, Field
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from typing import Dict, Any, List, Optional
import asyncio
import math
import os
import time
import re
import random
import logging
import threading
import requests
import json
import csv
import io
import xml.etree.ElementTree as ET
import sqlite3
import collections
import alpaca_trade_api as tradeapi
from dotenv import load_dotenv
import engine as ta
from indicators import technical_analysis_from_candles


# ---------------------------------------------------------------------------
# Simple in-memory rate limiter (sliding window)
# ---------------------------------------------------------------------------
_rate_store: Dict[str, collections.deque] = {}
_rate_lock = threading.Lock()


def _rate_limit(key: str, max_calls: int, window_s: float) -> bool:
    """Return True if the call is allowed, False if rate-limited."""
    now = time.monotonic()
    with _rate_lock:
        dq = _rate_store.setdefault(key, collections.deque())
        while dq and dq[0] < now - window_s:
            dq.popleft()
        if len(dq) >= max_calls:
            return False
        dq.append(now)
        return True

try:
    from alpaca.trading.client import TradingClient as _AlpacaPyTradingClient  # type: ignore
    from alpaca.trading.enums import AssetClass as _AlpacaPyAssetClass  # type: ignore
    from alpaca.trading.enums import AssetStatus as _AlpacaPyAssetStatus  # type: ignore
    from alpaca.trading.requests import GetAssetsRequest as _AlpacaPyGetAssetsRequest  # type: ignore
except Exception:
    _AlpacaPyTradingClient = None  # type: ignore
    _AlpacaPyAssetClass = None  # type: ignore
    _AlpacaPyAssetStatus = None  # type: ignore
    _AlpacaPyGetAssetsRequest = None  # type: ignore


# Load .env as early as possible (before any LLM modules read env vars)
try:
    _dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
    load_dotenv(dotenv_path=_dotenv_path, override=False)
except Exception:
    try:
        load_dotenv(override=False)
    except Exception:
        pass


# Ensure Alpaca keys work with alpaca_trade_api (expects APCA_* env var names)
try:
    _k = (os.getenv("ALPACA_API_KEY") or "").strip()
    _s = (os.getenv("ALPACA_SECRET_KEY") or "").strip()
    if _k and not (os.getenv("APCA_API_KEY_ID") or "").strip():
        os.environ["APCA_API_KEY_ID"] = _k
    if _s and not (os.getenv("APCA_API_SECRET_KEY") or "").strip():
        os.environ["APCA_API_SECRET_KEY"] = _s
except Exception:
    pass


try:
    from llm_services import llm_news_sentiment as _llm_news_sentiment
except Exception:
    _llm_news_sentiment = None


try:
    # Keep a secondary load_dotenv() call for safety in non-standard run contexts.
    load_dotenv(override=False)
except Exception:
    pass

try:
    from llm_client import init_llm_client as _init_llm_client
except Exception:
    _init_llm_client = None

from data_fetcher import get_bars as _alpaca_get_bars, get_snapshot, get_snapshot as _alpaca_get_snapshot
from data_fetcher import get_bars_batch as _alpaca_get_bars_batch
from data_fetcher import get_snapshots_batch as _alpaca_get_snapshots_batch
from data_fetcher import validate_market_env as _validate_market_env
from data_fetcher import get_market_regime as _get_market_regime
from data_fetcher import get_snapshot_normalized as _get_snapshot_normalized
from indicator_engine import calculate_indicators as calculate_indicators
from scoring_engine import score_composite_0_100 as _score_composite_0_100, score_execution_0_100 as _score_execution_0_100
from execution_engine import build_execution_plan as _build_execution_plan
from best_pick import pick_best_sync as _pick_best_sync
from best_pick_v2 import scan_best_pick_v2 as _scan_best_pick_v2

try:
    from polygon_client import get_market_cap as _polygon_get_market_cap, get_unusual_options as _polygon_get_unusual_options
    from polygon_client import get_ticker_news as _polygon_get_ticker_news
except Exception:
    _polygon_get_market_cap = None
    _polygon_get_unusual_options = None
    _polygon_get_ticker_news = None


_BEST_PICK_FALLBACK_CACHE: Dict[str, Any] = {"ts": 0.0, "resp": None}
_YF_52W_CACHE: Dict[str, Any] = {}


class TTLCache:
    def __init__(self, *, maxsize: int = 500, ttl: int = 60):
        self.maxsize = int(maxsize or 500)
        self.ttl = float(ttl or 60)
        self._data: Dict[str, Any] = {}

    def _now(self) -> float:
        return time.time()

    def _purge(self) -> None:
        now = self._now()
        dead = []
        try:
            for k, v in list(self._data.items()):
                if not isinstance(v, tuple) or len(v) != 2:
                    dead.append(k)
                    continue
                ts = float(v[0] or 0.0)
                if ts <= 0.0 or (now - ts) > self.ttl:
                    dead.append(k)
        except Exception:
            dead = []
        for k in dead:
            try:
                self._data.pop(k, None)
            except Exception:
                pass
        try:
            if len(self._data) > self.maxsize:
                items = sorted(self._data.items(), key=lambda kv: float(kv[1][0] or 0.0))
                over = max(0, len(items) - self.maxsize)
                for k, _ in items[:over]:
                    self._data.pop(k, None)
        except Exception:
            pass

    def get(self, key: str) -> Any:
        if not key:
            return None
        self._purge()
        try:
            item = self._data.get(key)
        except Exception:
            item = None
        if not item or not isinstance(item, tuple) or len(item) != 2:
            return None
        ts, val = item
        try:
            if (self._now() - float(ts or 0.0)) > self.ttl:
                self._data.pop(key, None)
                return None
        except Exception:
            return None
        return val

    def set(self, key: str, value: Any) -> None:
        if not key:
            return
        self._purge()
        try:
            self._data[key] = (self._now(), value)
        except Exception:
            pass
        self._purge()


symbol_cache = TTLCache(maxsize=500, ttl=60)


_TRADE_PLAN_CACHE = TTLCache(maxsize=2500, ttl=300)
_SENTIMENT_CACHE = TTLCache(maxsize=5000, ttl=900)
_REASONING_CACHE = TTLCache(maxsize=2500, ttl=300)
_NEWS_CACHE = TTLCache(maxsize=5000, ttl=600)

# Extended intelligence caches
_REDDIT_SENTIMENT_CACHE = TTLCache(maxsize=5000, ttl=900)
_TWITTER_SENTIMENT_CACHE = TTLCache(maxsize=5000, ttl=600)
_EARNINGS_AI_CACHE = TTLCache(maxsize=5000, ttl=86400)
_ANALYST_TARGETS_CACHE = TTLCache(maxsize=5000, ttl=21600)
_NEWS_IMPACT_CACHE = TTLCache(maxsize=5000, ttl=21600)


def _cache_key(prefix: str, symbol: str) -> str:
    return f"{str(prefix or '').strip()}:{str(symbol or '').strip().upper()}"


def _social_default(*, symbol: str = "") -> Dict[str, Any]:
    return {
        "reddit_score": 0,
        "twitter_score": 0,
        "hype_score": 0,
        "direction": "NEUTRAL",
        "mentions": {"reddit": 0, "twitter": 0},
        "samples": {"reddit": [], "twitter": []},
        "summary": "Unavailable",
        "status": "unavailable",
        "symbol": str(symbol or "").strip().upper(),
    }


def _social_direction_from_score(score_0_100: Any) -> str:
    try:
        s = float(score_0_100)
    except Exception:
        s = 50.0
    if s >= 60.0:
        return "BULLISH"
    if s <= 40.0:
        return "BEARISH"
    return "NEUTRAL"


def _bull_bear_ratio(texts: List[str]) -> Optional[float]:
    bull_words = ["bull", "bullish", "calls", "long", "moon", "breakout", "buy", "accumulate", "upside", "squeeze"]
    bear_words = ["bear", "bearish", "puts", "short", "dump", "sell", "downside", "rug", "fraud", "overvalued"]
    b = 0
    r = 0
    for t in texts:
        tx = str(t or "").lower()
        if not tx:
            continue
        try:
            for w in bull_words:
                if w in tx:
                    b += 1
            for w in bear_words:
                if w in tx:
                    r += 1
        except Exception:
            continue
    if b == 0 and r == 0:
        return None
    try:
        return float(b) / float(max(1, r))
    except Exception:
        return None


def _score_from_components(*, mention_spike: float, bull_bear: float, engagement: float) -> int:
    # mention_spike, bull_bear, engagement are expected 0..1
    try:
        ms = max(0.0, min(1.0, float(mention_spike)))
        bb = max(0.0, min(1.0, float(bull_bear)))
        eg = max(0.0, min(1.0, float(engagement)))
        s = (0.40 * ms) + (0.40 * bb) + (0.20 * eg)
        return int(max(0, min(100, round(s * 100.0))))
    except Exception:
        return 0


def _safe_user_agent() -> str:
    return str(os.getenv("STACKIQ_USER_AGENT", "stackiq-prodigy/1.0") or "stackiq-prodigy/1.0")


def _fetch_reddit_mentions(*, symbol: str, subreddits: List[str], window: str) -> Dict[str, Any]:
    # window: "day" or "week"
    sym = str(symbol or "").strip().upper()
    if not sym:
        return {"mentions": 0, "engagement": 0.0, "texts": [], "posts": []}
    sr = [str(s or "").strip() for s in (subreddits or []) if str(s or "").strip()]
    if not sr:
        return {"mentions": 0, "engagement": 0.0, "texts": [], "posts": []}

    headers = {"User-Agent": _safe_user_agent()}
    q = f"${sym} OR {sym}"

    mentions = 0
    engagement = 0.0
    texts: List[str] = []
    posts: List[Dict[str, Any]] = []
    for s in sr:
        url = f"https://www.reddit.com/r/{s}/search.json"
        params = {"q": q, "restrict_sr": 1, "sort": "new", "t": window, "limit": 25}
        try:
            r = requests.get(url, params=params, headers=headers, timeout=6)
            if r.status_code != 200:
                continue
            js = r.json() if r.content else {}
            children = ((js or {}).get("data") or {}).get("children") or []
            for ch in children:
                d = ch.get("data") if isinstance(ch, dict) else None
                if not isinstance(d, dict):
                    continue
                title = str(d.get("title") or "").strip()
                body = str(d.get("selftext") or "").strip()
                t = (title + "\n" + body).strip()[:280]
                if t:
                    texts.append(t)
                mentions += 1
                up = d.get("ups")
                com = d.get("num_comments")
                try:
                    engagement += float(up or 0) + (0.5 * float(com or 0))
                except Exception:
                    engagement += 0.0

                # Structured sample post for UI.
                try:
                    permalink = str(d.get("permalink") or "").strip()
                except Exception:
                    permalink = ""
                if permalink and not permalink.startswith("http"):
                    permalink = "https://www.reddit.com" + permalink
                try:
                    post = {
                        "platform": "reddit",
                        "subreddit": str(d.get("subreddit") or s).strip(),
                        "title": title[:240],
                        "text": body[:420],
                        "url": permalink,
                        "upvotes": int(up or 0),
                        "comments": int(com or 0),
                        "created_utc": (float(d.get("created_utc")) if d.get("created_utc") is not None else None),
                    }
                    # Prefer non-empty samples.
                    if post.get("title") or post.get("text"):
                        posts.append(post)
                except Exception:
                    pass
        except Exception:
            continue

    return {
        "mentions": int(max(0, mentions)),
        "engagement": float(max(0.0, engagement)),
        "texts": texts[:80],
        "posts": posts[:25],
    }


def _fetch_twitter_mentions(*, symbol: str) -> Dict[str, Any]:
    # Requires TWITTER_BEARER_TOKEN; otherwise returns unavailable.
    token = (os.getenv("TWITTER_BEARER_TOKEN") or os.getenv("X_BEARER_TOKEN") or "").strip()
    sym = str(symbol or "").strip().upper()
    if not token or not sym:
        return {"mentions": 0, "engagement": 0.0, "texts": [], "posts": [], "status": "unavailable"}
    # Basic recent search (API v2). This will fail gracefully if the account lacks access.
    url = "https://api.twitter.com/2/tweets/search/recent"
    query = f"{sym} lang:en -is:retweet"
    params = {
        "query": query,
        "max_results": 25,
        "tweet.fields": "public_metrics,created_at,author_id",
    }
    headers = {"Authorization": f"Bearer {token}"}
    try:
        r = requests.get(url, params=params, headers=headers, timeout=6)
        if r.status_code != 200:
            return {"mentions": 0, "engagement": 0.0, "texts": [], "posts": [], "status": "degraded"}
        js = r.json() if r.content else {}
        items = (js or {}).get("data") or []
    except Exception:
        items = []

    engagement = 0.0
    texts: List[str] = []
    posts: List[Dict[str, Any]] = []
    for it in items[:25]:
        if not isinstance(it, dict):
            continue
        txt = str(it.get("text") or "").strip()
        if txt:
            texts.append(txt[:280])
        pm = it.get("public_metrics") if isinstance(it.get("public_metrics"), dict) else {}
        try:
            engagement += float(pm.get("like_count") or 0.0) + 2.0 * float(pm.get("retweet_count") or 0.0) + 1.2 * float(pm.get("reply_count") or 0.0)
        except Exception:
            pass

        # Structured sample post for UI.
        try:
            post = {
                "platform": "twitter",
                "username": str(it.get("author_id") or "").strip(),
                "text": txt[:420],
                "url": f"https://twitter.com/i/web/status/{it.get('id')}",
                "likes": int(pm.get("like_count") or 0),
                "retweets": int(pm.get("retweet_count") or 0),
                "replies": int(pm.get("reply_count") or 0),
                "created_at": (it.get("created_at") if it.get("created_at") is not None else None),
            }
            # Prefer non-empty samples.
            if post.get("text"):
                posts.append(post)
        except Exception:
            pass

    return {"mentions": int(len(items[:25])), "engagement": float(max(0.0, engagement)), "texts": texts[:80], "posts": posts[:25], "status": "ok"}


async def get_social_sentiment(symbol: str) -> Dict[str, Any]:
    sym = str(symbol or "").strip().upper()
    if not sym:
        return _social_default(symbol=sym)
    ck = _cache_key("social_sent", sym)
    cached = _SENTIMENT_CACHE.get(ck)
    if isinstance(cached, dict) and "reddit_score" in cached and "twitter_score" in cached:
        return cached

    out = _social_default(symbol=sym)
    subreddits = ["stocks", "wallstreetbets", "options", "investing"]
    try:
        day = await asyncio.to_thread(_fetch_reddit_mentions, symbol=sym, subreddits=subreddits, window="day")
        week = await asyncio.to_thread(_fetch_reddit_mentions, symbol=sym, subreddits=subreddits, window="week")
    except Exception:
        day = {"mentions": 0, "engagement": 0.0, "texts": []}
        week = {"mentions": 0, "engagement": 0.0, "texts": []}

    try:
        tw = await asyncio.to_thread(_fetch_twitter_mentions, symbol=sym)
    except Exception:
        tw = {"mentions": 0, "engagement": 0.0, "texts": [], "status": "error"}

    # Mention spike (mentions vs 7d avg). Approximate avg/day = week_mentions/7.
    try:
        r_day = int((day or {}).get("mentions") or 0)
        r_week = int((week or {}).get("mentions") or 0)
        avg_day = float(r_week) / 7.0 if r_week > 0 else 0.0
        spike = float(r_day) / float(avg_day) if avg_day > 0 else (1.0 if r_day > 0 else 0.0)
        spike01 = max(0.0, min(1.0, (spike - 1.0) / 2.5))  # spike 1x..3.5x => 0..1
    except Exception:
        spike01 = 0.0

    # Bull/bear ratio mapped to 0..1
    try:
        rr = _bull_bear_ratio(list((day or {}).get("texts") or []))
        if rr is None:
            bb01 = 0.5
        else:
            # ratio 0.5 => bearish, 1.0 neutral-ish, 2.0 bullish
            bb01 = max(0.0, min(1.0, (float(rr) - 0.5) / 1.5))
    except Exception:
        bb01 = 0.5

    # Engagement velocity mapped to 0..1 using log compression.
    try:
        eng = float((day or {}).get("engagement") or 0.0)
        eg01 = max(0.0, min(1.0, math.log10(1.0 + eng) / 4.0))
    except Exception:
        eg01 = 0.0

    reddit_score = _score_from_components(mention_spike=spike01, bull_bear=bb01, engagement=eg01)

    # Twitter score (best-effort); if no token, keep 0.
    try:
        t_mentions = int((tw or {}).get("mentions") or 0)
        t_eng = float((tw or {}).get("engagement") or 0.0)
        t_texts = list((tw or {}).get("texts") or [])
        tr = _bull_bear_ratio(t_texts)
        if tr is None:
            t_bb01 = 0.5
        else:
            t_bb01 = max(0.0, min(1.0, (float(tr) - 0.5) / 1.5))
        t_spike01 = max(0.0, min(1.0, float(t_mentions) / 40.0))
        t_eg01 = max(0.0, min(1.0, math.log10(1.0 + t_eng) / 4.0))
        twitter_score = _score_from_components(mention_spike=t_spike01, bull_bear=t_bb01, engagement=t_eg01)
    except Exception:
        twitter_score = 0
        t_mentions = 0

    hype_score = int(max(0, min(100, round((0.6 * float(reddit_score)) + (0.4 * float(twitter_score))))))

    any_mentions = bool((r_day > 0) or (t_mentions > 0))
    if not any_mentions:
        # No mentions => no signal. Keep UI neutral instead of forcing BEARISH from a 0 score.
        direction = "NEUTRAL"
    else:
        direction = _social_direction_from_score(hype_score)

    top_reddit = None
    try:
        rp = day.get("posts") if isinstance(day, dict) else None
        if isinstance(rp, list) and rp:
            top_reddit = rp[0] if isinstance(rp[0], dict) else None
    except Exception:
        top_reddit = None
    top_twitter = None
    try:
        tp = tw.get("posts") if isinstance(tw, dict) else None
        if isinstance(tp, list) and tp:
            top_twitter = tp[0] if isinstance(tp[0], dict) else None
    except Exception:
        top_twitter = None

    summary = ""
    try:
        parts: List[str] = []
        parts.append(f"Reddit mentions: {int(r_day)}")
        parts.append(f"Twitter mentions: {int(t_mentions)}")
        if not any_mentions:
            parts.append("No recent social signal.")
        else:
            parts.append(f"Direction: {str(direction or 'NEUTRAL').upper()}.")

        if isinstance(top_reddit, dict):
            try:
                upv = int(top_reddit.get("upvotes") or 0)
            except Exception:
                upv = 0
            tt = str(top_reddit.get("title") or "").strip()
            if tt:
                parts.append(f"Top Reddit post ({upv} upvotes): {tt[:120]}")

        if isinstance(top_twitter, dict):
            try:
                likes = int(top_twitter.get("likes") or 0)
            except Exception:
                likes = 0
            tx = str(top_twitter.get("text") or "").strip()
            if tx:
                parts.append(f"Top Tweet ({likes} likes): {tx[:120]}")
        summary = " ".join([p for p in parts if str(p or "").strip()]).strip()[:420]
    except Exception:
        summary = ""
    if not summary:
        summary = "Unavailable"

    out = {
        "reddit_score": int(reddit_score),
        "twitter_score": int(twitter_score),
        "hype_score": int(hype_score),
        "direction": direction,
        "mentions": {"reddit": int(r_day), "twitter": int(t_mentions)},
        "samples": {
            "reddit": (day.get("posts") if isinstance(day, dict) and isinstance(day.get("posts"), list) else []),
            "twitter": (tw.get("posts") if isinstance(tw, dict) and isinstance(tw.get("posts"), list) else []),
        },
        "summary": summary,
        "status": "ok" if any_mentions else "unavailable",
        "symbol": sym,
    }
    _SENTIMENT_CACHE.set(ck, out)
    return out


def _earnings_default(*, symbol: str) -> Dict[str, Any]:
    return {
        "tone": "unavailable",
        "guidance_outlook": "unavailable",
        "ai_confidence": 0,
        "key_themes": [],
        "status": "unavailable",
        "source": "unavailable",
        "symbol": str(symbol or "").strip().upper(),
    }


def _fmp_key() -> str:
    return (os.getenv("FMP_API_KEY") or os.getenv("FINANCIAL_MODELING_PREP_API_KEY") or "").strip()


def _fetch_fmp_transcript(symbol: str) -> Optional[str]:
    key = _fmp_key()
    sym = str(symbol or "").strip().upper()
    if not key or not sym:
        return None
    # Best-effort: try last 6 quarters.
    now = datetime.now(timezone.utc)
    y0 = int(now.year)
    quarters = [4, 3, 2, 1]
    tries: List[tuple[int, int]] = []
    for dy in (0, 1):
        y = y0 - dy
        for q in quarters:
            tries.append((y, q))
    tries = tries[:6]
    for y, q in tries:
        url = f"https://financialmodelingprep.com/api/v3/earning_call_transcript/{sym}"
        params = {"year": y, "quarter": q, "apikey": key}
        try:
            r = requests.get(url, params=params, timeout=10)
            if r.status_code != 200:
                continue
            data = r.json() or []
        except Exception:
            continue
        if isinstance(data, list) and data:
            it = data[0] if isinstance(data[0], dict) else {}
            tx = str(it.get("content") or it.get("text") or "").strip()
            if tx:
                return tx
        if isinstance(data, dict):
            tx = str(data.get("content") or data.get("text") or "").strip()
            if tx:
                return tx
    return None


def _tone_to_score_0_100(tone: str) -> int:
    t = str(tone or "").strip().upper()
    if t == "BULLISH":
        return 75
    if t == "BEARISH":
        return 25
    if t == "NEUTRAL":
        return 50
    return 0


async def get_earnings_ai(symbol: str, *, allow_llm: bool = True) -> Dict[str, Any]:
    sym = str(symbol or "").strip().upper()
    if not sym:
        return _earnings_default(symbol=sym)
    ck = _cache_key("earnings_ai", sym)
    cached = _EARNINGS_AI_CACHE.get(ck)
    if isinstance(cached, dict) and "tone" in cached:
        return cached

    out = _earnings_default(symbol=sym)
    try:
        if not _fmp_key():
            out["status"] = "missing_fmp_api_key"
            _EARNINGS_AI_CACHE.set(ck, out)
            return out
    except Exception:
        pass
    try:
        tx = await asyncio.to_thread(_fetch_fmp_transcript, sym)
    except Exception:
        tx = None
    if not tx:
        _EARNINGS_AI_CACHE.set(ck, out)
        return out

    out["source"] = "fmp"
    out["status"] = "transcript_ok"
    if not allow_llm:
        _EARNINGS_AI_CACHE.set(ck, out)
        return out

    # Chunk transcript to control prompt size.
    try:
        chunks: List[str] = []
        txt = str(tx)
        step = 5500
        for i in range(0, len(txt), step):
            if len(chunks) >= 4:
                break
            chunks.append(txt[i : i + step])
    except Exception:
        chunks = [str(tx)[:8000]]

    try:
        from llm_client import call_llm_text

        system = (
            "Analyze this earnings call transcript. Determine tone, forward guidance outlook, and growth signals. "
            "Return STRICT JSON ONLY with keys: tone (BULLISH|NEUTRAL|BEARISH), guidance_outlook (RAISED|INLINE|LOWERED), "
            "ai_confidence (0..100), key_themes (array of strings)."
        )
        user = json.dumps({"symbol": sym, "chunks": chunks}, ensure_ascii=False)
        raw = call_llm_text(system=system, user=user, max_output_tokens=450, timeout_s=max(12.0, float(_openai_timeout_seconds())))
        data = _json_loads_loose(raw) if isinstance(raw, str) else None
        if isinstance(data, dict):
            tone = str(data.get("tone") or "NEUTRAL").strip().upper()
            if tone not in ("BULLISH", "NEUTRAL", "BEARISH"):
                tone = "NEUTRAL"
            g = str(data.get("guidance_outlook") or "INLINE").strip().upper()
            if g not in ("RAISED", "INLINE", "LOWERED"):
                g = "INLINE"
            out["tone"] = tone
            out["guidance_outlook"] = g
            try:
                out["ai_confidence"] = int(max(0, min(100, int(float(data.get("ai_confidence") or 50)))))
            except Exception:
                out["ai_confidence"] = 50
            try:
                th = data.get("key_themes")
                if isinstance(th, list):
                    out["key_themes"] = [str(x).strip() for x in th if str(x or "").strip()][:8]
            except Exception:
                out["key_themes"] = []
            out["status"] = "llm"
    except Exception as e:
        try:
            out["status"] = "llm_error"
            out["error"] = f"{type(e).__name__}:{str(e)[:180]}"
        except Exception:
            pass

    _EARNINGS_AI_CACHE.set(ck, out)
    return out


def _analyst_default(*, symbol: str) -> Dict[str, Any]:
    return {
        "target_avg": None,
        "target_high": None,
        "target_low": None,
        "implied_upside_pct": None,
        "rating_bias": "NEUTRAL",
        "score_0_100": 0,
        "status": "unavailable",
        "source": "unavailable",
        "symbol": str(symbol or "").strip().upper(),
    }


def _finnhub_key() -> str:
    return (os.getenv("FINNHUB_API_KEY") or "").strip()


def _fetch_finnhub_price_target(symbol: str) -> Optional[Dict[str, Any]]:
    key = _finnhub_key()
    sym = str(symbol or "").strip().upper()
    if not key or not sym:
        return None
    url = "https://finnhub.io/api/v1/stock/price-target"
    params = {"symbol": sym, "token": key}
    try:
        r = requests.get(url, params=params, timeout=8)
        if r.status_code != 200:
            return None
        data = r.json() or {}
    except Exception:
        return None
    return data if isinstance(data, dict) else None


def _fetch_finnhub_recommendations(symbol: str) -> Optional[Dict[str, Any]]:
    """Fetch analyst buy/hold/sell rating distribution from Finnhub."""
    key = _finnhub_key()
    sym = str(symbol or "").strip().upper()
    if not key or not sym:
        return None
    url = "https://finnhub.io/api/v1/stock/recommendation"
    params = {"symbol": sym, "token": key}
    try:
        r = requests.get(url, params=params, timeout=8)
        if r.status_code != 200:
            return None
        data = r.json()
        if isinstance(data, list) and data:
            return data[0]  # most recent period
        return None
    except Exception:
        return None


def _buy_pct_score(rec: Optional[Dict[str, Any]]) -> Optional[int]:
    """Convert buy/hold/sell counts to 0-100 score. 90% buy → ~95 score."""
    if not isinstance(rec, dict):
        return None
    try:
        strong_buy = int(rec.get("strongBuy") or 0)
        buy = int(rec.get("buy") or 0)
        hold = int(rec.get("hold") or 0)
        sell = int(rec.get("sell") or 0)
        strong_sell = int(rec.get("strongSell") or 0)
        total = strong_buy + buy + hold + sell + strong_sell
        if total == 0:
            return None
        buy_pct = (strong_buy + buy) / total
        # Map 0%→0, 50%→50, 90%→95, 100%→100
        return int(min(100, round(buy_pct * 105)))
    except Exception:
        return None


def _analyst_score_from_upside(upside_pct: Any) -> int:
    # Map -20%..+40% to 0..100 with caps.
    try:
        u = float(upside_pct)
    except Exception:
        return 0
    u = max(-50.0, min(100.0, u))
    v = (u + 20.0) / 60.0
    return int(max(0, min(100, round(v * 100.0))))


async def get_analyst_targets(symbol: str, *, last_price: Optional[float]) -> Dict[str, Any]:
    sym = str(symbol or "").strip().upper()
    if not sym:
        return _analyst_default(symbol=sym)
    ck = _cache_key("analyst_targets", sym)
    cached = _ANALYST_TARGETS_CACHE.get(ck)
    if isinstance(cached, dict) and "target_avg" in cached:
        return cached

    out = _analyst_default(symbol=sym)

    try:
        if not _finnhub_key():
            out["status"] = "missing_finnhub_api_key"
            _ANALYST_TARGETS_CACHE.set(ck, out)
            return out
    except Exception:
        pass

    data = None
    rec_data = None
    try:
        data, rec_data = await asyncio.gather(
            asyncio.to_thread(_fetch_finnhub_price_target, sym),
            asyncio.to_thread(_fetch_finnhub_recommendations, sym),
        )
        if isinstance(data, dict):
            out["source"] = "finnhub"
    except Exception:
        data = None
        rec_data = None

    try:
        if isinstance(data, dict):
            out["target_avg"] = _safe_f(data.get("targetMean"))
            out["target_high"] = _safe_f(data.get("targetHigh"))
            out["target_low"] = _safe_f(data.get("targetLow"))
    except Exception:
        pass

    try:
        px = float(last_price) if last_price is not None and float(last_price) > 0 else None
    except Exception:
        px = None
    try:
        if px is not None and out.get("target_avg") is not None:
            out["implied_upside_pct"] = (float(out.get("target_avg")) - float(px)) / float(px) * 100.0
    except Exception:
        out["implied_upside_pct"] = None

    # Score from buy/hold/sell ratings (primary) + price target upside (secondary).
    # Free Finnhub tier has recommendations but not price targets — handle both cases.
    try:
        buy_score = _buy_pct_score(rec_data)
        has_upside = out.get("implied_upside_pct") is not None
        upside_score = _analyst_score_from_upside(out.get("implied_upside_pct")) if has_upside else None

        if buy_score is not None and upside_score is not None:
            # Both available: weight ratings 60%, price target 40%
            blended = int(round(0.6 * buy_score + 0.4 * upside_score))
        elif buy_score is not None:
            # Only ratings available (free tier): use directly
            blended = buy_score
        elif upside_score is not None:
            blended = upside_score
        else:
            blended = 50

        out["score_0_100"] = max(0, min(100, blended))
        if buy_score is not None:
            out["buy_pct"] = buy_score
        out["rating_bias"] = "BULLISH" if out["score_0_100"] >= 60 else "BEARISH" if out["score_0_100"] <= 40 else "NEUTRAL"
    except Exception:
        out["score_0_100"] = 50
        out["rating_bias"] = "NEUTRAL"

    out["status"] = "ok" if (rec_data is not None or out.get("implied_upside_pct") is not None) else "unavailable"
    _ANALYST_TARGETS_CACHE.set(ck, out)
    return out


def _impact_default(*, symbol: str) -> Dict[str, Any]:
    return {
        "impact_score": 50,
        "price_reaction_pct": 0.0,
        "volume_spike": 1.0,
        "status": "unavailable",
        "source": "derived",
        "symbol": str(symbol or "").strip().upper(),
    }


def _recency_score_0_100(published_at: Any) -> int:
    # 0h => 100, 72h+ => ~30
    try:
        s = str(published_at or "").strip()
        if not s:
            return 50
        dt = None
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        except Exception:
            dt = None
        if dt is None:
            return 50
        hrs = (datetime.now(timezone.utc) - dt.astimezone(timezone.utc)).total_seconds() / 3600.0
        hrs = max(0.0, hrs)
        v = 100.0 - min(70.0, (hrs / 72.0) * 70.0)
        return int(max(0, min(100, round(v))))
    except Exception:
        return 50


async def calculate_news_price_impact(symbol: str, *, news: Dict[str, Any], market_data: Dict[str, Any]) -> Dict[str, Any]:
    sym = str(symbol or "").strip().upper()
    if not sym:
        return _impact_default(symbol=sym)
    ck = _cache_key("news_impact", sym)
    cached = _NEWS_IMPACT_CACHE.get(ck)
    if isinstance(cached, dict) and "impact_score" in cached:
        return cached

    out = _impact_default(symbol=sym)
    try:
        # polarity: map news score (-100..100) => 0..100
        sc = _safe_f((news or {}).get("score"), 0.0) or 0.0
        polarity_0_100 = int(max(0, min(100, round(((float(sc) + 100.0) / 200.0) * 100.0))))
    except Exception:
        polarity_0_100 = 50

    # recency: take most recent published_at
    rec = 50
    try:
        items = (news or {}).get("items") if isinstance((news or {}).get("items"), list) else []
        pubs: List[str] = []
        for it in items[:10]:
            if not isinstance(it, dict):
                continue
            pubs.append(str(it.get("published_at") or ""))
        rec = _recency_score_0_100(pubs[0] if pubs else "")
    except Exception:
        rec = 50

    # catalyst strength: confidence + number of catalysts
    cat = 50
    try:
        conf = float((news or {}).get("confidence") or 35.0)
        cnum = len((news or {}).get("catalysts") or []) if isinstance((news or {}).get("catalysts"), list) else 0
        cat = int(max(0, min(100, round((0.65 * conf) + (0.35 * min(100.0, cnum * 18.0))))))
    except Exception:
        cat = 50

    # volume spike post-news (use relative_volume if present)
    vol_spike = 1.0
    vol_score = 50
    try:
        vol_spike = float(market_data.get("relative_volume") or 1.0)
        vol_spike = max(0.0, min(10.0, vol_spike))
        vol_score = int(max(0, min(100, round(min(3.0, vol_spike) / 3.0 * 100.0))))
    except Exception:
        vol_spike = 1.0
        vol_score = 50

    # price gap reaction: use percent_change
    pr_pct = 0.0
    pr_score = 50
    try:
        pr_pct = float(market_data.get("percent_change") or market_data.get("pct_change") or 0.0)
        pr_pct = max(-20.0, min(20.0, pr_pct))
        pr_score = int(max(0, min(100, round(((pr_pct + 20.0) / 40.0) * 100.0))))
    except Exception:
        pr_pct = 0.0
        pr_score = 50

    # impact_score weights
    try:
        impact = (
            (0.30 * float(polarity_0_100))
            + (0.20 * float(rec))
            + (0.20 * float(cat))
            + (0.15 * float(vol_score))
            + (0.15 * float(pr_score))
        )
        out["impact_score"] = int(max(0, min(100, round(impact))))
        out["price_reaction_pct"] = float(pr_pct)
        out["volume_spike"] = float(vol_spike)
        out["status"] = "ok"
    except Exception:
        out = _impact_default(symbol=sym)

    _NEWS_IMPACT_CACHE.set(ck, out)
    return out


def _retry_call(fn, *, retries: int = 3, base_delay_sec: float = 0.35):
    last_exc = None
    for attempt in range(int(retries or 3)):
        try:
            return fn()
        except Exception as e:
            last_exc = e
            try:
                delay = float(base_delay_sec) * (2.0 ** float(attempt))
            except Exception:
                delay = 0.5
            try:
                time.sleep(delay)
            except Exception:
                pass
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("retry failed")


def _safe_f(x: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        v = float(x)
    except Exception:
        return default
    if not math.isfinite(v):
        return default
    return float(v)


def _round_px(x: Any) -> Optional[float]:
    v = _safe_f(x)
    if v is None:
        return None
    try:
        return float(round(v, 4))
    except Exception:
        return v


def _atr_14_from_bars(bars: List[Dict[str, Any]]) -> Optional[float]:
    if not isinstance(bars, list) or len(bars) < 16:
        return None
    highs: List[float] = []
    lows: List[float] = []
    closes: List[float] = []
    for b in bars[-60:]:
        if not isinstance(b, dict):
            continue
        h = _safe_f(b.get("h"))
        l = _safe_f(b.get("l"))
        c = _safe_f(b.get("c"))
        if h is None or l is None or c is None:
            continue
        highs.append(h)
        lows.append(l)
        closes.append(c)
    if len(closes) < 16:
        return None
    try:
        trs: List[float] = []
        for i in range(1, len(closes)):
            tr = max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
            trs.append(tr)
        tail = trs[-14:] if len(trs) >= 14 else trs
        if not tail:
            return None
        return float(sum(tail) / float(len(tail)))
    except Exception:
        return None


def _vwap_from_bars(bars: List[Dict[str, Any]]) -> Optional[float]:
    if not isinstance(bars, list) or not bars:
        return None
    num = 0.0
    den = 0.0
    for b in bars[-200:]:
        if not isinstance(b, dict):
            continue
        c = _safe_f(b.get("c"))
        v = _safe_f(b.get("v"), 0.0)
        if c is None or v is None:
            continue
        if v <= 0:
            continue
        num += float(c) * float(v)
        den += float(v)
    if den <= 0:
        return None
    return float(num / den)


def _recent_high_low(bars: List[Dict[str, Any]], lookback: int = 20) -> tuple[Optional[float], Optional[float]]:
    if not isinstance(bars, list) or not bars:
        return None, None
    highs: List[float] = []
    lows: List[float] = []
    for b in bars[-max(1, int(lookback or 20)) :]:
        if not isinstance(b, dict):
            continue
        h = _safe_f(b.get("h"))
        l = _safe_f(b.get("l"))
        if h is not None:
            highs.append(h)
        if l is not None:
            lows.append(l)
    if not highs or not lows:
        return None, None
    try:
        return float(max(highs)), float(min(lows))
    except Exception:
        return None, None


def _volume_trend(bars: List[Dict[str, Any]], lookback: int = 20) -> float:
    if not isinstance(bars, list) or len(bars) < 3:
        return 1.0
    vols: List[float] = []
    for b in bars[-max(5, int(lookback or 20)) :]:
        if not isinstance(b, dict):
            continue
        v = _safe_f(b.get("v"), 0.0)
        if v is None:
            continue
        vols.append(max(0.0, float(v)))
    if len(vols) < 5:
        return 1.0
    try:
        last = float(vols[-1])
        avg = float(sum(vols[-20:]) / float(len(vols[-20:]) or 1))
        if avg <= 0:
            return 1.0
        return float(last / avg)
    except Exception:
        return 1.0


def _momentum_bucket(momentum_0_100: Any) -> str:
    m = _safe_f(momentum_0_100, 50.0)
    if m is None:
        return "LOW"
    if m >= 67.0:
        return "HIGH"
    if m >= 45.0:
        return "MEDIUM"
    return "LOW"


def _system_expectation_from_momentum(momentum_0_100: Any) -> str:
    b = _momentum_bucket(momentum_0_100)
    if b == "HIGH":
        return "Next session momentum entry with risk-defined breakout plan."
    if b == "MEDIUM":
        return "Next session range expansion plan with defined risk and confirmation triggers."
    return "Next session mean reversion plan with tight risk and patience for pullback entry."


def _execution_window_from_momentum(momentum_0_100: Any) -> str:
    b = _momentum_bucket(momentum_0_100)
    if b == "HIGH":
        return "First 60 minutes"
    if b == "MEDIUM":
        return "First 90 minutes"
    return "Midday pullback"


def _momentum_multiplier(momentum_0_100: Any) -> float:
    b = _momentum_bucket(momentum_0_100)
    if b == "HIGH":
        return 2.0
    if b == "MEDIUM":
        return 1.5
    return 1.0


def _volatility_bucket(volatility_0_100: Any) -> str:
    v = _safe_f(volatility_0_100, 50.0)
    if v is None:
        return "MEDIUM"
    if v >= 70.0:
        return "HIGH"
    if v >= 45.0:
        return "MEDIUM"
    return "LOW"


def _execution_window_from_volatility(*, volatility_0_100: Any, user_tz: Optional[str]) -> str:
    b = _volatility_bucket(volatility_0_100)
    # Windows in ET converted to user tz, defaulting to NY.
    if b == "HIGH":
        return _format_time_window(user_tz, (9, 45), (10, 30))
    if b == "MEDIUM":
        return _format_time_window(user_tz, (9, 45), (11, 15))
    return _format_time_window(user_tz, (11, 30), (13, 30))


def _execution_date_iso(*, market_is_open: bool) -> str:
    # Best-effort: if Alpaca clock is available, use next_open to pick next trading day.
    try:
        if market_is_open:
            return datetime.now(ZoneInfo("America/New_York")).date().isoformat()
    except Exception:
        pass

    try:
        c, _ = _get_clock_cached()
        if c is not None:
            no = getattr(c, "next_open", None)
            if getattr(no, "astimezone", None):
                return no.astimezone(ZoneInfo("America/New_York")).date().isoformat()
    except Exception:
        pass

    # Fallback: next weekday.
    try:
        d = datetime.now(ZoneInfo("America/New_York")).date()
        if d.weekday() >= 4:
            # Fri->Mon, Sat->Mon, Sun->Mon
            days = 7 - d.weekday()
            return (d + timedelta(days=days)).isoformat()
        return (d + timedelta(days=1)).isoformat()
    except Exception:
        return datetime.now(timezone.utc).date().isoformat()


def _format_range(low: Any, high: Any) -> str:
    lo = _safe_f(low)
    hi = _safe_f(high)
    if lo is None or hi is None:
        return ""
    try:
        return f"{round(float(lo), 2)}–{round(float(hi), 2)}"
    except Exception:
        return ""


def _entry_method_classification(*, pullback_setup: bool, entry: Any, vwap: Any, prior_high: Any) -> str:
    # Determine setup:
    # - Breakout -> Momentum entry
    # - Pullback -> Support bounce
    # - Range -> VWAP reclaim
    if bool(pullback_setup):
        return "Support bounce"
    e = _safe_f(entry)
    vh = _safe_f(vwap)
    ph = _safe_f(prior_high)
    try:
        if e is not None and ph is not None and abs(float(e) - float(ph)) <= max(0.01, float(ph) * 0.0025):
            return "Momentum entry"
    except Exception:
        pass
    try:
        if e is not None and vh is not None and abs(float(e) - float(vh)) <= max(0.01, float(vh) * 0.0025):
            return "VWAP reclaim"
    except Exception:
        pass
    return "Momentum entry"


def _confirmations_checklist(*, news_sentiment: Optional[str] = None) -> List[str]:
    out = [
        "Volume spike",
        "VWAP reclaim",
        "Higher highs",
        "Sector strength",
    ]
    # If explicit bearish news, keep confirmations but add a risk-aware item.
    try:
        ns = str(news_sentiment or "").strip().lower()
        if ns == "bearish":
            out.append("No further negative headlines")
    except Exception:
        pass
    return out[:6]


def _deterministic_trade_plan(
    *,
    symbol: str,
    daily_bars: List[Dict[str, Any]],
    intraday_bars: Optional[List[Dict[str, Any]]],
    indicators: Dict[str, Any],
) -> Dict[str, Any]:
    sym = str(symbol or "").strip().upper()
    cache_key = _cache_key("trade_plan", sym)
    cached = _TRADE_PLAN_CACHE.get(cache_key)
    if isinstance(cached, dict) and cached.get("entry") is not None:
        return cached

    daily = daily_bars if isinstance(daily_bars, list) else []
    intra = intraday_bars if isinstance(intraday_bars, list) else []

    last_px = None
    try:
        if daily:
            last_px = _safe_f(daily[-1].get("c"))
    except Exception:
        last_px = None
    if last_px is None:
        try:
            if intra:
                last_px = _safe_f(intra[-1].get("c"))
        except Exception:
            last_px = None

    atr14 = _atr_14_from_bars(daily)
    if atr14 is None and intra:
        atr14 = _atr_14_from_bars(intra)
    if atr14 is None:
        try:
            if last_px is not None and last_px > 0:
                atr14 = float(last_px) * 0.02
        except Exception:
            atr14 = None
    if atr14 is None:
        atr14 = 1.0

    vwap = _vwap_from_bars(intra) or _vwap_from_bars(daily)
    if vwap is None:
        vwap = last_px

    prior_high, recent_low = _recent_high_low(daily, lookback=20)
    if prior_high is None or recent_low is None:
        ph2, rl2 = _recent_high_low(intra, lookback=60)
        prior_high = prior_high or ph2
        recent_low = recent_low or rl2

    if prior_high is None and last_px is not None:
        prior_high = float(last_px)
    if recent_low is None and last_px is not None:
        recent_low = float(last_px) * 0.98

    vol_trend = _volume_trend(daily, lookback=20)
    momentum_score = _safe_f((indicators or {}).get("momentum"), 50.0) or 50.0

    # Support level is recent_low.
    support_level = recent_low
    breakout_trigger = None
    try:
        if prior_high is not None:
            breakout_trigger = float(prior_high)
    except Exception:
        breakout_trigger = None

    vwap_reclaim = vwap

    # Pullback setup heuristic: lower momentum OR price below VWAP.
    pullback_setup = False
    try:
        if float(momentum_score) < 45.0:
            pullback_setup = True
        if last_px is not None and vwap is not None and float(last_px) < float(vwap):
            pullback_setup = True
    except Exception:
        pullback_setup = False

    if pullback_setup and support_level is not None:
        entry = float(support_level) + float(atr14) * 0.2
    else:
        entry_candidates = [x for x in [breakout_trigger, vwap_reclaim, breakout_trigger] if _safe_f(x) is not None]
        # Confluence max(prior_high, vwap_reclaim, breakout_trigger)
        entry = max([float(x) for x in entry_candidates]) if entry_candidates else (last_px or 0.0)
        if (entry is None or float(entry) <= 0) and last_px is not None and float(last_px) > 0:
            entry = float(last_px)

    # Stop: min(support_level, vwap - atr*0.5, recent_swing_low)
    stop_candidates: List[float] = []
    try:
        if support_level is not None:
            stop_candidates.append(float(support_level))
    except Exception:
        pass
    try:
        if vwap is not None:
            stop_candidates.append(float(vwap) - float(atr14) * 0.5)
    except Exception:
        pass
    try:
        if recent_low is not None:
            stop_candidates.append(float(recent_low))
    except Exception:
        pass
    stop = min(stop_candidates) if stop_candidates else (float(entry) - float(atr14) * 0.5)

    if stop >= entry:
        try:
            stop = float(entry) - max(0.01, float(atr14) * 0.5)
        except Exception:
            stop = float(entry) * 0.97

    mm = _momentum_multiplier(momentum_score)
    target_1 = float(entry) + float(atr14) * 1.2 * float(mm)
    target_2 = float(entry) + float(atr14) * 2.4 * float(mm)
    target_3 = float(entry) + float(atr14) * 3.5 * float(mm)

    rr = None
    try:
        risk_per_share = float(entry) - float(stop)
        reward_per_share = float(target_2) - float(entry)
        if risk_per_share > 0:
            rr = reward_per_share / risk_per_share
    except Exception:
        rr = None

    expected_gain_pct = None
    try:
        if float(entry) > 0:
            expected_gain_pct = (float(target_2) - float(entry)) / float(entry) * 100.0
    except Exception:
        expected_gain_pct = None

    expectation = _system_expectation_from_momentum(momentum_score)
    plan = {
        "symbol": sym,
        "entry": _round_px(entry),
        "stop": _round_px(stop),
        "target_1": _round_px(target_1),
        "target_2": _round_px(target_2),
        "target_3": _round_px(target_3),
        "targets": [_round_px(target_1), _round_px(target_2), _round_px(target_3)],
        "rr": _round_px(rr) if rr is not None else None,
        "expected_gain_pct": _round_px(expected_gain_pct) if expected_gain_pct is not None else None,
        "vwap": _round_px(vwap),
        "atr14": _round_px(atr14),
        "recent_high": _round_px(prior_high),
        "recent_low": _round_px(recent_low),
        "volume_trend": _round_px(vol_trend),
        "momentum": int(round(momentum_score)),
        "momentum_multiplier": float(mm),
        "pullback_setup": bool(pullback_setup),
        "expectation": expectation,
    }
    _TRADE_PLAN_CACHE.set(cache_key, plan)
    return plan


def _fetch_finnhub_company_news(symbol: str, *, limit: int = 10) -> List[Dict[str, Any]]:
    sym = str(symbol or "").strip().upper()
    if not sym:
        return []
    cache_key = _cache_key("finnhub_news", sym)
    cached = _NEWS_CACHE.get(cache_key)
    if isinstance(cached, list):
        return cached

    api_key = str(os.getenv("FINNHUB_API_KEY") or "").strip()
    if not api_key:
        return []

    top_n = max(1, min(int(limit or 10), 30))
    now_utc = datetime.now(timezone.utc)
    frm = (now_utc - timedelta(days=7)).date().isoformat()
    to = now_utc.date().isoformat()
    url = "https://finnhub.io/api/v1/company-news"
    params = {"symbol": sym, "from": frm, "to": to, "token": api_key}
    try:
        r = requests.get(url, params=params, timeout=12)
        if r is None or r.status_code != 200:
            return []
        payload = r.json() if hasattr(r, "json") else []
    except Exception:
        return []

    if not isinstance(payload, list):
        return []

    out: List[Dict[str, Any]] = []
    for it in payload[:top_n]:
        if not isinstance(it, dict):
            continue
        headline = str(it.get("headline") or "").strip()
        if not headline:
            continue
        ts = it.get("datetime")
        try:
            ts_i = int(ts) if ts is not None else None
        except Exception:
            ts_i = None
        out.append(
            {
                "headline": headline[:240],
                "source": str(it.get("source") or "").strip()[:120],
                "url": str(it.get("url") or "").strip()[:400],
                "timestamp": ts_i,
            }
        )

    _NEWS_CACHE.set(cache_key, out)
    return out


def _news_and_sentiment(symbol: str, *, allow_llm: bool = True) -> Dict[str, Any]:
    sym = str(symbol or "").strip().upper()
    if not sym:
        return {"headlines": [], "sentiment": "Neutral", "source": "unavailable", "items": []}

    ck = _cache_key("sentiment", sym)

    # Prefer Polygon News (cached) when API key is present.
    items: List[Dict[str, Any]] = []
    source = "unavailable"
    try:
        if callable(_polygon_get_ticker_news):
            poly = _polygon_get_ticker_news(sym)
            if isinstance(poly, list) and poly:
                items = poly
                source = "polygon"
    except Exception:
        items = []

    if not items:
        items = _fetch_finnhub_company_news(sym, limit=10)
        source = "finnhub"

    if not items:
        # Fallback to existing Alpaca news fetcher (keeps card populated even without Finnhub key)
        try:
            items2 = _fetch_news_for_symbol(sym, limit=10)
        except Exception:
            items2 = []
        items = []
        for it in items2[:10]:
            if not isinstance(it, dict):
                continue
            title = str(it.get("title") or "").strip()
            if not title:
                continue
            items.append(
                {
                    "title": title,
                    "summary": str(it.get("description") or "").strip()[:600],
                    "source": str(it.get("source") or "").strip()[:120],
                    "published_at": "",
                    "url": str(it.get("url") or "").strip()[:500],
                    "sentiment_label": None,
                }
            )
        source = "alpaca"

    pulled_n = 0
    filtered_n = 0
    sent_n = 0

    try:
        pulled_n = int(len(items or []))
    except Exception:
        pulled_n = 0

    try:
        company_kw: Dict[str, List[str]] = {
            "NVDA": ["nvidia", "jensen huang", "gpu", "gpus", "cuda", "ai chip", "ai chips", "semiconductor", "semiconductors"],
            "TSLA": ["tesla", "elon musk", "ev", "electric vehicle", "gigafactory", "fsd", "autopilot"],
            "AMD": ["advanced micro devices", "amd", "ryzen", "epyc", "gpu", "ai", "semiconductor", "semiconductors"],
        }
        sector_kw_default: List[str] = ["semiconductor", "semiconductors", "ai", "gpu", "data center"]

        spam_sources = {
            "globenewswire",
            "pr newswire",
            "accesswire",
            "business wire",
            "businesswire",
            "newsfile",
        }

        rx_ticker = re.compile(rf"(?:\$|\b){re.escape(sym)}\b", re.IGNORECASE)
        kws = list(company_kw.get(sym) or [])
        if not kws:
            kws = [sym.lower()] + list(sector_kw_default)

        def _text_blob(it: Dict[str, Any]) -> str:
            return (
                f"{str(it.get('title') or it.get('headline') or '')} "
                f"{str(it.get('summary') or it.get('description') or '')} "
                f"{str(it.get('source') or '')}"
            ).strip()

        rel: List[Dict[str, Any]] = []
        for it in (items or [])[:50]:
            if not isinstance(it, dict):
                continue
            txt = _text_blob(it).lower()
            if not txt:
                continue
            src = str(it.get("source") or "").strip().lower()

            is_lawsuit = ("lawsuit" in txt) or ("class action" in txt) or ("law firm" in txt)
            if is_lawsuit and src in spam_sources:
                continue

            score = 0
            try:
                if rx_ticker.search(txt):
                    score += 4
            except Exception:
                pass
            try:
                for k in kws:
                    kk = str(k or "").strip().lower()
                    if kk and kk in txt:
                        score += 1
            except Exception:
                pass

            # Drop low-relevance PR spam sources unless there's an explicit ticker/company match.
            if src in spam_sources and score < 4:
                continue

            if score >= 1:
                rel.append(it)

        if rel:
            items = rel[:20]
            filtered_n = int(len(items))
        else:
            filtered_n = int(len(items or []))
    except Exception:
        try:
            filtered_n = int(len(items or []))
        except Exception:
            filtered_n = 0

    try:
        log.info(f"news_intel: symbol={sym} pulled={pulled_n} filtered={filtered_n} source={source}")
    except Exception:
        pass

    # Cache signature (bust cached sentiment if headlines changed).
    try:
        _sig_titles = [str(x.get("title") or x.get("headline") or "").strip() for x in (items or []) if isinstance(x, dict)]
        _sig_titles = [t for t in _sig_titles if t][:5]
        cache_sig = "|".join(_sig_titles)
    except Exception:
        cache_sig = ""

    try:
        cached = _SENTIMENT_CACHE.get(ck)
        if isinstance(cached, dict) and isinstance(cached.get("headlines"), list):
            if str(cached.get("cache_sig") or "") == str(cache_sig or ""):
                return cached
    except Exception:
        pass

    def _keyword_fallback_sentiment(articles: List[Dict[str, Any]]) -> Dict[str, Any]:
        text = " ".join(
            [
                str(a.get("title") or "") + " " + str(a.get("summary") or "")
                for a in (articles or [])
                if isinstance(a, dict)
            ]
        ).lower()
        bull = ["upgrade", "beats", "record", "partnership", "contract", "acquisition", "raises guidance", "buy rating", "outperform"]
        bear = ["lawsuit", "downgrade", "miss", "sec", "investigation", "dilution", "offering", "insider selling", "guidance cut", "bankruptcy"]
        score = 0
        try:
            for w in bull:
                if w in text:
                    score += 12
            for w in bear:
                if w in text:
                    score -= 14
        except Exception:
            score = 0
        score = max(-100, min(100, int(score)))
        direction = "Neutral"
        if score >= 30:
            direction = "Bullish"
        elif score <= -30:
            direction = "Bearish"
        return {
            "sentiment_direction": direction,
            "sentiment_score": score,
            "confidence": 35 if articles else 15,
            "summary": "Low news volume. Sentiment confidence reduced." if not articles else "Heuristic sentiment estimate based on recent headlines.",
            "key_catalysts": [],
            "risk_flags": [],
            "news_status": "keyword_fallback" if articles else "no_news",
            "sentiment_source": "keyword",
        }

    def analyze_news_sentiment(ticker: str, articles: List[Dict[str, Any]]) -> Dict[str, Any]:
        # Cache per-symbol sentiment for 10 minutes via _SENTIMENT_CACHE.
        try:
            _sig_titles2 = [str(x.get("title") or x.get("headline") or "").strip() for x in (articles or []) if isinstance(x, dict)]
            _sig_titles2 = [t for t in _sig_titles2 if t][:6]
            _cache_sig2 = "|".join(_sig_titles2)
        except Exception:
            _cache_sig2 = ""

        sck = _cache_key("polygon_sentiment", f"{ticker}:{_cache_sig2}")
        cached2 = _SENTIMENT_CACHE.get(sck)
        if isinstance(cached2, dict) and cached2.get("sentiment_score") is not None:
            return cached2

        if not isinstance(articles, list) or not articles:
            out0 = _keyword_fallback_sentiment([])
            _SENTIMENT_CACHE.set(sck, out0)
            return out0

        if not allow_llm:
            out1 = _keyword_fallback_sentiment(articles)
            _SENTIMENT_CACHE.set(sck, out1)
            return out1

        llm_attempted = False
        llm_error = ""

        strict = False
        try:
            strict = str(os.getenv("STACKIQ_LLM_STRICT", "0") or "0").strip().lower() in ("1", "true", "yes", "on")
        except Exception:
            strict = False

        # If strict mode is enabled, missing LLM dependencies/keys should be a hard error.
        try:
            from llm_client import llm_available as _llm_available
            if not bool(_llm_available()):
                if strict:
                    raise RuntimeError("LLM unavailable; cannot run AI sentiment")
        except Exception:
            if strict:
                raise

        def _strict_parse_json(raw: Any) -> Optional[Dict[str, Any]]:
            if not isinstance(raw, str) or not raw.strip():
                return None
            try:
                return _json_loads_loose(raw)
            except Exception:
                return None

        def _build_llm_payload() -> Dict[str, Any]:
            payload = {
                "ticker": ticker,
                "articles": [
                    {
                        "title": str(a.get("title") or "")[:240],
                        "summary": str(a.get("summary") or "")[:600],
                        "source": str(a.get("source") or "")[:120],
                        "published_at": str(a.get("published_at") or "")[:40],
                        "url": str(a.get("url") or "")[:500],
                    }
                    for a in (articles or [])
                    if isinstance(a, dict)
                ][:20],
            }
            return payload

        try:
            from llm_client import call_llm_text, LLMDisabledError, LLMCallError

            system = (
                "You are a market news sentiment engine. Output MUST be valid JSON only (no prose, no markdown). "
                "Return JSON with exactly these keys: "
                "direction (Bullish|Bearish|Neutral), sentiment_score (-100..100), confidence (0..100), "
                "summary (2-3 sentences), catalysts (array of strings), risk_flags (array of strings). "
                "Ground your output strictly in the supplied articles."
            )
            payload = _build_llm_payload()

            try:
                nonlocal sent_n
                sent_n = int(len(payload.get("articles") or []))
                log.info(f"news_intel: symbol={ticker} sent_to_llm={sent_n}")
            except Exception:
                pass

            # Attempt 1
            llm_attempted = True
            raw1 = call_llm_text(
                system=system,
                user=json.dumps(payload, ensure_ascii=False),
                max_output_tokens=700,
                timeout_s=8.0,
            )
            data = _strict_parse_json(raw1)

            # Retry once if non-JSON / invalid JSON.
            if not isinstance(data, dict):
                system2 = system + " IMPORTANT: Output MUST be a single JSON object and nothing else."
                raw2 = call_llm_text(
                    system=system2,
                    user=json.dumps(payload, ensure_ascii=False),
                    max_output_tokens=700,
                    timeout_s=10.0,
                )
                data = _strict_parse_json(raw2)

            if not isinstance(data, dict):
                try:
                    log.warning(f"news_intel: symbol={ticker} llm_invalid_json -> keyword_fallback")
                except Exception:
                    pass
                llm_error = "invalid_json"

            if isinstance(data, dict):
                out2 = {
                    "sentiment_direction": str(data.get("direction") or "Neutral").strip().title(),
                    "sentiment_score": int(float(data.get("sentiment_score") or 0)),
                    "confidence": int(float(data.get("confidence") or 50)),
                    "summary": str(data.get("summary") or "").strip()[:420],
                    "key_catalysts": [str(x).strip() for x in (data.get("catalysts") or []) if str(x).strip()][:6],
                    "risk_flags": [str(x).strip() for x in (data.get("risk_flags") or []) if str(x).strip()][:6],
                    "news_status": "llm",
                    "sentiment_source": "llm",
                }
                out2["sentiment_score"] = max(-100, min(100, int(out2.get("sentiment_score") or 0)))
                out2["confidence"] = max(0, min(100, int(out2.get("confidence") or 0)))
                # Apply rule-based direction thresholds.
                try:
                    sc = int(out2.get("sentiment_score") or 0)
                    if sc >= 30:
                        out2["sentiment_direction"] = "Bullish"
                    elif sc <= -30:
                        out2["sentiment_direction"] = "Bearish"
                    else:
                        out2["sentiment_direction"] = "Neutral"
                except Exception:
                    out2["sentiment_direction"] = "Neutral"
                if not out2.get("summary"):
                    out2["summary"] = "AI sentiment analysis unavailable."
                _SENTIMENT_CACHE.set(sck, out2)
                return out2
        except LLMDisabledError as e:
            if strict:
                raise
            try:
                log.warning(f"news_intel: symbol={ticker} llm_disabled reason={str(e)[:160]} -> keyword_fallback")
            except Exception:
                pass
            llm_attempted = True
            llm_error = f"disabled:{str(e)[:160]}"
        except LLMCallError as e:
            try:
                log.warning(f"news_intel: symbol={ticker} llm_call_error err={str(e)[:160]} -> keyword_fallback")
            except Exception:
                pass
            pass
            llm_attempted = True
            llm_error = f"call_error:{str(e)[:160]}"
        except Exception as e:
            try:
                log.warning(f"news_intel: symbol={ticker} llm_exception={type(e).__name__}:{str(e)[:140]} -> keyword_fallback")
            except Exception:
                pass
            pass
            llm_attempted = True
            llm_error = f"exception:{type(e).__name__}:{str(e)[:140]}"

        out3 = _keyword_fallback_sentiment(articles)
        try:
            out3["llm_attempted"] = bool(llm_attempted)
            out3["llm_error"] = str(llm_error or "")[:220]
        except Exception:
            pass
        _SENTIMENT_CACHE.set(sck, out3)
        return out3

    # Normalize headlines list for legacy consumers
    headlines: List[str] = []
    try:
        headlines = [str(x.get("title") or x.get("headline") or "").strip() for x in items if isinstance(x, dict)]
        headlines = [h for h in headlines if h][:8]
    except Exception:
        headlines = []

    ai = analyze_news_sentiment(sym, items)
    direction2 = str(ai.get("sentiment_direction") or "Neutral").strip().title()
    summary = str(ai.get("summary") or "").strip()
    confidence = ai.get("confidence")
    catalysts = ai.get("key_catalysts") if isinstance(ai.get("key_catalysts"), list) else []
    risk_flags = ai.get("risk_flags") if isinstance(ai.get("risk_flags"), list) else []
    score = ai.get("sentiment_score")
    news_status = str(ai.get("news_status") or "").strip()
    sentiment_source = str(ai.get("sentiment_source") or ("llm" if ai.get("news_status") == "llm" else "keyword")).strip().lower()
    llm_attempted2 = bool(ai.get("llm_attempted")) if isinstance(ai, dict) else False
    llm_error2 = str(ai.get("llm_error") or "").strip()[:220] if isinstance(ai, dict) else ""

    # Legacy direction (BULLISH/BEARISH/NEUTRAL)
    direction = "NEUTRAL"
    if direction2.lower() == "bullish":
        direction = "BULLISH"
    elif direction2.lower() == "bearish":
        direction = "BEARISH"

    sentiment = direction2

    out = {
        "headlines": headlines,
        "summary": summary[:240] if isinstance(summary, str) and summary else ("Low news volume. Sentiment confidence reduced." if not items else "unavailable"),
        "sentiment": sentiment,
        "direction": direction,
        "score": score,
        "confidence": confidence,
        "catalysts": catalysts,
        "risk_flags": risk_flags,
        "news_status": news_status,
        "sentiment_source": sentiment_source,
        "llm_attempted": bool(llm_attempted2),
        "llm_error": llm_error2,
        "source": source,
        "items": items,
        "cache_sig": cache_sig,
    }
    _SENTIMENT_CACHE.set(ck, out)
    return out


def _trade_reasoning(
    *,
    symbol: str,
    technicals: Dict[str, Any],
    trade_plan: Dict[str, Any],
    news: Dict[str, Any],
    allow_llm: bool = True,
) -> Dict[str, Any]:
    sym = str(symbol or "").strip().upper()
    if not sym:
        return {"why": [], "confirms": [], "breaks": []}

    ck = _cache_key("reasoning", sym)
    cached = _REASONING_CACHE.get(ck)
    if isinstance(cached, dict) and isinstance(cached.get("why"), list):
        return cached

    mom = _safe_f((technicals or {}).get("momentum"), 50.0) or 50.0
    vol_trend = _safe_f((trade_plan or {}).get("volume_trend"), 1.0) or 1.0
    news_sentiment = str((news or {}).get("sentiment") or "Neutral")

    why = [
        f"Momentum score {int(round(mom))}/100 with volume trend x{round(float(vol_trend), 2)}.",
        f"Trade plan is risk-defined around VWAP/ATR with clear invalidation.",
        f"News sentiment currently {news_sentiment}.",
    ]
    confirms = ["VWAP reclaim and hold", "Volume expansion", "Break of prior high"]
    breaks = ["Loss of VWAP", "Failed breakout", "Sector weakness"]

    if not allow_llm:
        out = {"why": why[:3], "confirms": confirms[:3], "breaks": breaks[:3]}
        _REASONING_CACHE.set(ck, out)
        return out

    try:
        from llm_client import call_llm_text

        system = (
            "You are a trade reasoning engine. Return ONLY valid JSON with keys: "
            "why (array of strings), confirms (array of strings), breaks (array of strings). "
            "Rules: 2-4 items per array, concise, grounded strictly in provided inputs."
        )
        user = json.dumps(
            {
                "symbol": sym,
                "technicals": technicals,
                "trade_plan": {k: trade_plan.get(k) for k in ("entry", "stop", "target_1", "target_2", "rr", "vwap", "atr14")},
                "volume_trend": trade_plan.get("volume_trend"),
                "news_sentiment": {"sentiment": news.get("sentiment"), "headlines": (news.get("headlines") or [])[:6]},
            },
            ensure_ascii=False,
        )
        raw = call_llm_text(system=system, user=user, max_output_tokens=450)
        data = _json_loads_loose(raw) if isinstance(raw, str) else None
        if isinstance(data, dict):
            wy = data.get("why") if isinstance(data.get("why"), list) else []
            cf = data.get("confirms") if isinstance(data.get("confirms"), list) else []
            br = data.get("breaks") if isinstance(data.get("breaks"), list) else []
            wy2 = [str(x).strip() for x in wy if str(x or "").strip()][:4]
            cf2 = [str(x).strip() for x in cf if str(x or "").strip()][:4]
            br2 = [str(x).strip() for x in br if str(x or "").strip()][:4]
            out = {"why": wy2 or why[:3], "confirms": cf2 or confirms[:3], "breaks": br2 or breaks[:3]}
            _REASONING_CACHE.set(ck, out)
            return out
    except Exception:
        pass

    out = {"why": why[:3], "confirms": confirms[:3], "breaks": breaks[:3]}
    _REASONING_CACHE.set(ck, out)
    return out


def get_snapshot_cached(symbol: str) -> Optional[Dict[str, Any]]:
    sym = str(symbol or "").strip().upper()
    if not sym:
        return None
    k = _cache_key("snap", sym)
    cached = symbol_cache.get(k)
    if isinstance(cached, dict):
        return cached
    try:
        snap = _retry_call(lambda: get_snapshot(sym), retries=3, base_delay_sec=0.35)
    except Exception:
        snap = None
    if isinstance(snap, dict):
        symbol_cache.set(k, snap)
        return snap
    return None


def get_candles_cached(symbol: str, *, limit: int = 100) -> Any:
    sym = str(symbol or "").strip().upper()
    if not sym:
        return None
    lim = 100
    try:
        lim = int(limit or 100)
    except Exception:
        lim = 100
    if lim < 100:
        lim = 100
    k = _cache_key(f"candles:{lim}", sym)
    cached = symbol_cache.get(k)
    if cached is not None:
        return cached
    df = None
    try:
        df = get_candles(sym, timeframe="1Day", limit=lim)
    except Exception:
        df = None
    symbol_cache.set(k, df)
    return df


def _bars_payload_from_candles(df: Any, *, limit: int = 100) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    lim = 100
    try:
        lim = int(limit or 100)
    except Exception:
        lim = 100
    if lim < 100:
        lim = 100
    try:
        if df is not None and hasattr(df, "to_dict"):
            rows = df.to_dict(orient="records")
            rows = rows[-lim:] if isinstance(rows, list) else []
            for r in rows:
                if not isinstance(r, dict):
                    continue
                o = r.get("open")
                h = r.get("high")
                l = r.get("low")
                c = r.get("close")
                v = r.get("volume")
                if o is None or h is None or l is None or c is None:
                    continue
                b: Dict[str, Any] = {"o": o, "h": h, "l": l, "c": c, "v": (v if v is not None else 0.0)}
                if r.get("t") is not None:
                    b["time"] = r.get("t")
                out.append(b)
        elif isinstance(df, list):
            out = [b for b in df if isinstance(b, dict)][-lim:]
    except Exception:
        out = []
    if len(out) > lim:
        out = out[-lim:]
    return out


def compute_technical_indicators(candles: Any) -> Dict[str, int]:
    ta0 = technical_analysis_from_candles(candles)
    if not isinstance(ta0, dict):
        raise ValueError("TA_INVALID")
    def _safe_i(x: Any, default: int = 50) -> int:
        try:
            v = int(float(x))
        except Exception:
            v = int(default)
        if v < 0:
            v = 0
        if v > 100:
            v = 100
        return int(v)

    out = {
        "momentum": _safe_i(ta0.get("momentum"), 50),
        "trend": _safe_i(ta0.get("trend"), 50),
        "volatility": _safe_i(ta0.get("volatility"), 50),
        "liquidity": _safe_i(ta0.get("liquidity"), 50),
        "risk": _safe_i(ta0.get("risk"), 50),
    }
    return out


def _clamp_0_100(v: Any) -> float:
    try:
        x = float(v)
    except Exception:
        x = 0.0
    if x < 0.0:
        x = 0.0
    if x > 100.0:
        x = 100.0
    return float(x)


def _sentiment_score_0_100(news_sentiment: Dict[str, Any]) -> float:
    if not isinstance(news_sentiment, dict):
        return 50.0
    # New contract: score is -100..+100
    if news_sentiment.get("score") is not None:
        try:
            s = float(news_sentiment.get("score"))
            if not math.isfinite(s):
                return 50.0
            return _clamp_0_100((s + 100.0) / 2.0)
        except Exception:
            pass
    if news_sentiment.get("score_100") is not None:
        try:
            return _clamp_0_100(float(news_sentiment.get("score_100")))
        except Exception:
            pass
    try:
        d = str(news_sentiment.get("direction") or "NEUTRAL").strip().upper()
    except Exception:
        d = "NEUTRAL"
    if d == "BULLISH":
        return 70.0
    if d == "BEARISH":
        return 30.0
    return 50.0


def score_ai_0_100(ta0: Dict[str, Any], news_sentiment: Dict[str, Any]) -> float:
    momentum = _clamp_0_100((ta0 or {}).get("momentum"))
    trend = _clamp_0_100((ta0 or {}).get("trend"))
    volatility = _clamp_0_100((ta0 or {}).get("volatility"))
    liquidity = _clamp_0_100((ta0 or {}).get("liquidity"))
    sentiment = _clamp_0_100(_sentiment_score_0_100(news_sentiment))
    return _clamp_0_100((0.30 * momentum) + (0.25 * trend) + (0.20 * sentiment) + (0.15 * volatility) + (0.10 * liquidity))


def _format_time_window(local_tz: Optional[str], start_hm: Any, end_hm: Any) -> str:
    tz = _safe_zoneinfo(local_tz)
    now_local = datetime.now(timezone.utc).astimezone(tz)
    start = now_local.replace(hour=int(start_hm[0]), minute=int(start_hm[1]), second=0, microsecond=0)
    end = now_local.replace(hour=int(end_hm[0]), minute=int(end_hm[1]), second=0, microsecond=0)
    if end <= start:
        end = end + timedelta(days=1)
    return f"{start.strftime('%-I:%M %p')} – {end.strftime('%-I:%M %p')}"


def build_execution_plan(*, ta0: Dict[str, Any], tz: Optional[str]) -> Dict[str, Any]:
    momentum = _clamp_0_100((ta0 or {}).get("momentum"))
    trend = _clamp_0_100((ta0 or {}).get("trend"))
    volatility = _clamp_0_100((ta0 or {}).get("volatility"))
    if momentum > 70.0:
        strategy = "BREAKOUT_WINDOW"
        window = _format_time_window(tz, (9, 35), (10, 30))
        session = "Open drive"
        playbook = "Breakout window"
    elif volatility > 75.0:
        strategy = "SCALP_WINDOW"
        window = _format_time_window(tz, (11, 30), (13, 30))
        session = "Midday"
        playbook = "Scalp window"
    elif trend > 80.0:
        strategy = "TREND_CONTINUATION"
        window = _format_time_window(tz, (15, 0), (15, 50))
        session = "Power hour"
        playbook = "Trend continuation"
    else:
        strategy = "PULLBACK_RECLAIM"
        window = _format_time_window(tz, (9, 35), (10, 15))
        session = "Premarket/Open"
        playbook = "Pullback / Reclaim"
    return {
        "strategy": strategy,
        "time_window": window,
        "session": session,
        "playbook": playbook,
        "timezone": str(_safe_zoneinfo(tz).key),
    }


async def _run_llm_explain_with_timeout(ctx: Dict[str, Any], *, timeout_sec: float = 10.0) -> Optional[Dict[str, Any]]:
    if not os.getenv("OPENAI_API_KEY"):
        return None
    try:
        if bool(_llm_cb_is_open()):
            return None
    except Exception:
        pass

    async def _call() -> Optional[Dict[str, Any]]:
        def _do() -> Optional[Dict[str, Any]]:
            try:
                client = _get_openai_client()
                r = client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[
                        {"role": "system", "content": "Return ONLY valid JSON with keys: system_expectation (string), why (array of 3 short bullets), what_confirms (array of 1 short bullet), what_breaks (array of 1 short bullet). Use only provided context; no speculation."},
                        {"role": "user", "content": json.dumps(ctx)},
                    ],
                    temperature=0.25,
                    max_tokens=240,
                    timeout=_openai_timeout_seconds(),
                )
                raw = ""
                try:
                    raw = (r.choices[0].message.content or "").strip()
                except Exception:
                    raw = ""
                return _json_loads_loose(raw) if raw else None
            except Exception:
                return None

        return await asyncio.to_thread(_do)

    try:
        return await asyncio.wait_for(_call(), timeout=float(timeout_sec))
    except Exception:
        return None


def _rsi_14_from_candles(candles: List[Dict[str, Any]]) -> float:
    try:
        closes: List[float] = []
        for b in candles[-100:]:
            c = b.get("c")
            if c is None:
                continue
            closes.append(float(c))
        if len(closes) < 16:
            return 50.0
        gains = 0.0
        losses = 0.0
        for i in range(len(closes) - 14, len(closes)):
            if i <= 0:
                continue
            d = closes[i] - closes[i - 1]
            if d >= 0:
                gains += d
            else:
                losses += abs(d)
        if gains <= 0 and losses <= 0:
            return 50.0
        if losses <= 0:
            return 100.0
        rs = (gains / 14.0) / (losses / 14.0)
        rsi = 100.0 - (100.0 / (1.0 + rs))
        return _clamp_0_100(rsi)
    except Exception:
        return 50.0


def _trend_strength_from_candles(candles: List[Dict[str, Any]]) -> float:
    try:
        closes: List[float] = []
        for b in candles[-60:]:
            c = b.get("c")
            if c is None:
                continue
            closes.append(float(c))
        if len(closes) < 25:
            return 50.0
        n = len(closes)
        xs = list(range(n))
        x_mean = (n - 1) / 2.0
        y_mean = sum(closes) / float(n)
        num = 0.0
        den = 0.0
        for i in range(n):
            dx = float(xs[i]) - x_mean
            num += dx * (closes[i] - y_mean)
            den += dx * dx
        if den <= 0:
            return 50.0
        slope = num / den
        last = closes[-1]
        if last <= 0:
            return 50.0
        strength = abs(slope) / last * 10000.0
        return _clamp_0_100(strength)
    except Exception:
        return 50.0


def _sentiment_proxy_from_snapshot(snap: Dict[str, Any]) -> Dict[str, Any]:
    try:
        bar = snap.get("dailyBar") if isinstance(snap.get("dailyBar"), dict) else {}
        prev = snap.get("prevDailyBar") if isinstance(snap.get("prevDailyBar"), dict) else {}
        lt = snap.get("latestTrade") if isinstance(snap.get("latestTrade"), dict) else {}
        px = lt.get("p") if lt.get("p") is not None else bar.get("c")
        px = float(px) if px is not None else None
        pc = float(prev.get("c")) if prev.get("c") is not None else None
        chg = None
        if px is not None and pc is not None and pc > 0:
            chg = (px - pc) / pc * 100.0
        if chg is None:
            return {"direction": "NEUTRAL", "summary": "No sentiment signal.", "score_100": 50, "news_status": "proxy"}
        if chg >= 1.0:
            return {"direction": "BULLISH", "summary": "Bullish tape proxy.", "score_100": _clamp_0_100(55.0 + min(25.0, chg * 5.0)), "news_status": "proxy"}
        if chg <= -1.0:
            return {"direction": "BEARISH", "summary": "Bearish tape proxy.", "score_100": _clamp_0_100(45.0 - min(25.0, abs(chg) * 5.0)), "news_status": "proxy"}
        return {"direction": "NEUTRAL", "summary": "Neutral tape proxy.", "score_100": 50, "news_status": "proxy"}
    except Exception:
        return {"direction": "NEUTRAL", "summary": "No sentiment signal.", "score_100": 50, "news_status": "proxy"}


def _score_volume_0_100_from_snapshot(snap: Dict[str, Any]) -> float:
    try:
        bar = snap.get("dailyBar") if isinstance(snap.get("dailyBar"), dict) else {}
        lt = snap.get("latestTrade") if isinstance(snap.get("latestTrade"), dict) else {}
        px = lt.get("p") if lt.get("p") is not None else bar.get("c")
        px = float(px) if px is not None else None
        vol = float(bar.get("v")) if bar.get("v") is not None else None
        if px is None or vol is None or px <= 0 or vol <= 0:
            return 0.0
        dv = px * vol
        # map dollar-volume ~ [1M..5B] into 0..100 via log
        x = math.log10(max(1.0, dv))
        return _clamp_0_100((x - 6.0) * 20.0)
    except Exception:
        return 0.0


def _extract_trade_plan_from_bars(candles: List[Dict[str, Any]]) -> Dict[str, Any]:
    levels = _aurexis_levels_from_bars(candles)
    return {
        "entry": levels.get("entry"),
        "stop": levels.get("stop"),
        "targets": levels.get("targets") if isinstance(levels.get("targets"), list) else [],
    }


def score_execution_0_100(*, candles: List[Dict[str, Any]], ta0: Dict[str, Any], execution_plan: Dict[str, Any], trade_plan: Dict[str, Any]) -> float:
    try:
        last = float(candles[-1].get("c")) if candles and candles[-1].get("c") is not None else None
    except Exception:
        last = None

    entry = trade_plan.get("entry")
    stop = trade_plan.get("stop")
    targets = trade_plan.get("targets") if isinstance(trade_plan.get("targets"), list) else []

    # Entry quality: closer to last close is better.
    entry_quality = 50.0
    try:
        if last is not None and entry is not None:
            e = float(entry)
            if last > 0:
                dist = abs(e - last) / last * 100.0
                entry_quality = _clamp_0_100(100.0 - min(100.0, dist * 25.0))
    except Exception:
        entry_quality = 50.0

    # RR ratio: use first target if present.
    rr_score = 50.0
    try:
        if entry is not None and stop is not None and targets:
            e = float(entry)
            s = float(stop)
            t1 = float(targets[0]) if targets[0] is not None else None
            risk = abs(e - s)
            reward = abs(t1 - e) if t1 is not None else 0.0
            if risk > 0:
                rr = reward / risk
                rr_score = _clamp_0_100(min(100.0, rr * 33.0))
    except Exception:
        rr_score = 50.0

    # Volatility alignment: penalize very high volatility for non-scalp plans.
    vol = _clamp_0_100((ta0 or {}).get("volatility"))
    strat = str((execution_plan or {}).get("strategy") or "").strip().upper()
    vol_align = 50.0
    try:
        if strat == "SCALP_WINDOW":
            vol_align = _clamp_0_100(40.0 + (vol * 0.6))
        else:
            vol_align = _clamp_0_100(80.0 - max(0.0, vol - 60.0) * 1.5)
    except Exception:
        vol_align = 50.0

    # Session timing: reward matching high momentum/trend with appropriate windows.
    mom = _clamp_0_100((ta0 or {}).get("momentum"))
    tr = _clamp_0_100((ta0 or {}).get("trend"))
    timing = 50.0
    try:
        if strat == "BREAKOUT_WINDOW":
            timing = _clamp_0_100(40.0 + (mom * 0.6))
        elif strat == "TREND_CONTINUATION":
            timing = _clamp_0_100(40.0 + (tr * 0.6))
        elif strat == "SCALP_WINDOW":
            timing = _clamp_0_100(35.0 + (vol * 0.65))
        else:
            timing = _clamp_0_100(45.0 + (min(mom, tr) * 0.55))
    except Exception:
        timing = 50.0

    return _clamp_0_100((0.30 * entry_quality) + (0.30 * rr_score) + (0.20 * vol_align) + (0.20 * timing))


def _safe_zoneinfo(tz: Optional[str]) -> ZoneInfo:
    try:
        if tz:
            return ZoneInfo(str(tz))
    except Exception:
        pass
    return ZoneInfo("America/New_York")


def _no_nulls(obj: Any) -> Any:
    if obj is None:
        return ""
    if isinstance(obj, dict):
        out: Dict[Any, Any] = {}
        for k, v in obj.items():
            if v is None:
                continue
            out[k] = _no_nulls(v)
        return out
    if isinstance(obj, (list, tuple)):
        return [_no_nulls(v) for v in obj if v is not None]
    return obj


app = FastAPI(title="StackIQ Prodigy")

try:
    from auth import (
        auth_router, stripe_router, oauth_router,
        get_current_user as _get_current_user,
        require_active_subscription as _require_subscription,
        require_plan as _require_plan,
    )
    app.include_router(auth_router)
    app.include_router(stripe_router)
    app.include_router(oauth_router)
    _dep_starter = Depends(_require_plan("starter"))
    _dep_pro     = Depends(_require_plan("pro"))
    _dep_elite   = Depends(_require_plan("elite"))
except Exception as _auth_err:
    import logging as _lg
    _lg.getLogger("stackiq").warning(f"Auth module not loaded: {_auth_err}")
    def _get_current_user():  # type: ignore[misc]
        raise HTTPException(status_code=503, detail="Auth module not available")
    def _require_subscription():  # type: ignore[misc]
        raise HTTPException(status_code=503, detail="Auth module not available")
    def _require_plan(_min_plan: str):  # type: ignore[misc]
        def _dep():
            raise HTTPException(status_code=503, detail="Auth module not available")
        return _dep
    _dep_starter = Depends(_require_plan("starter"))
    _dep_pro     = Depends(_require_plan("pro"))
    _dep_elite   = Depends(_require_plan("elite"))
    oauth_router = None  # type: ignore[assignment]


_SEED_UNIVERSE = [
    # Mega-cap / S&P 500 core
    "AAPL","MSFT","NVDA","AMZN","GOOGL","GOOG","META","TSLA","BRK.B","JPM",
    "V","MA","UNH","XOM","LLY","AVGO","PG","JNJ","HD","MRK",
    "ABBV","CVX","COST","KO","PEP","WMT","BAC","CRM","ACN","TMO",
    "ORCL","CSCO","NEE","DHR","ABT","NKE","AMD","TXN","QCOM","INTC",
    "PM","UPS","RTX","HON","BA","CAT","GE","MMM","IBM","GS",
    "MS","C","WFC","AXP","BLK","SPGI","CB","USB","PNC","CME",
    "TJX","LOW","SBUX","MCD","YUM","DPZ","CMG","QSR","DNUT","WING",
    "ISRG","MDT","SYK","BSX","EW","BDX","HOLX","DGX","LH","IQV",
    "BMY","AMGN","GILD","REGN","VRTX","BIIB","MRNA","PFE","AZN","NVO",
    # Financials / Fintech
    "COF","DFS","SYF","ALLY","LC","SOFI","NU","UPST","AFRM","COIN",
    "HOOD","PYPL","SQ","FOUR","WEX","FI","FIS","FISV","GPN","NTRS",
    "SCHW","IBKR","ETSY","MKTX","ICE","CBOE","NDAQ","MUFG","SMFG","DB",
    # Tech / Software
    "PLTR","SNOW","DDOG","DATADOG","NET","CRWD","ZS","PANW","FTNT","OKTA",
    "CYBR","S","TENB","QLYS","RPM","NOW","WDAY","VEEV","TEAM","ATLASSIAN",
    "HUBS","ZM","DOCU","BOX","DOMO","ASAN","MDB","ESTC","CFLT","GTLB",
    "MSCI","VRSK","ANSS","CDNS","SNPS","ADSK","DASSAULT","PTC","AZPN","MANH",
    "INTU","ADBE","CRM","ORCL","SAP","TWLO","TOST","PCTY","PAYC","PAYX",
    "ANET","SMCI","HPE","HPQ","DELL","WDC","STX","NTAP","PSTG","NTNX",
    "QCOM","AVGO","MU","MRVL","ON","SWKS","QRVO","MPWR","AMAT","LRCX",
    "KLAC","ASML","NVMI","ENTG","ONTO","COHU","FORM","ACLS","AXCELIS","ICHR",
    # Consumer / Retail
    "AMZN","SHOP","MELI","SE","GRAB","BABA","JD","PDD","BIDU","TEMU",
    "WMT","TGT","COST","DG","DLTR","FIVE","OLLI","BJ","SFM","KR",
    "NKE","LULU","RL","PVH","HBI","UA","DECK","ONON","HOKA","CROX",
    "UBER","LYFT","ABNB","DASH","BKNG","EXPE","TRIP","PCLN","AIRB","VRBO",
    # Media / Entertainment
    "DIS","NFLX","CMCSA","T","VZ","TMUS","CHTR","PARA","WBD","FOXA",
    "EA","TTWO","ATVI","RBLX","U","MSFT","SONY","NTDOY","SQNXF","GMGI",
    "SPOT","TME","SNAP","PINS","RDDT","MTCH","BMBL","IAC","ZG","OPEN",
    # Autos / EVs
    "TSLA","F","GM","RIVN","LCID","STLA","TM","HMC","RACE","MBLY",
    "NIO","XPEV","LI","BYDDF","KNDI","GOEV","FSR","WKHS","AYRO","SOLO",
    # Energy / Commodities
    "XOM","CVX","SLB","HAL","BKR","OXY","MPC","VLO","PSX","COP",
    "EOG","PXD","DVN","MRO","APA","CTRA","CLR","SM","MTDR","RRC",
    "BP","SHEL","TTE","ENB","TRP","KMI","WMB","ET","EPD","MPLX",
    # Materials / Industrials
    "LIN","APD","SHW","ECL","IFF","EMN","CE","FMC","CF","MOS",
    "NEM","GOLD","AEM","WPM","KGC","AGI","AU","HL","CDE","EXK",
    "AA","ALB","MP","LTHM","SQM","LAC","PLL","SGML","ALTM","NOVL",
    # Healthcare
    "CVS","WBA","MCK","ABC","CAH","HUM","ELV","CNC","MOH","MOH",
    "HCA","THC","UHS","ACAD","JAZZ","PRGO","ENDP","AMRN","AKRX","IRWD",
    # REITs / Utilities
    "AMT","PLD","CCI","EQIX","PSA","O","WELL","DLR","SPG","AVB",
    "EXR","CUBE","LSI","NSA","REXR","EGP","FR","LPT","HIW","CIO",
    "NEE","DUK","SO","AEP","EXC","PCG","PEG","ED","FE","ES",
    # ETFs (sector/thematic)
    "SPY","QQQ","IWM","DIA","XLK","XLF","XLE","XLV","XLY","XLP",
    "XLU","XLI","XLB","XLC","XLRE","SMH","SOXX","IBB","ARKK","ARKG",
    "GLD","SLV","USO","UNG","CORN","SOYB","WEAT","TLT","HYG","LQD",
    "VNQ","KBWB","KBWR","IAT","FFIN","HTGM","KIE","ITB","XHB","HOMZ",
    # High-momentum / high-beta names
    "CELH","IONQ","QUBT","QBTS","RGTI","ARQQ","BTBT","MSTR","RIOT","MARA",
    "CLSK","HUT","CIFR","BTDR","WULF","IREN","CORZ","MGNI","APP","APLS",
    "HOOD","SOFI","LCID","RIVN","NKLA","RIDE","BLNK","CHPT","EVGO","WPRT",
    "SOUN","AI","BBAI","GFAI","AITX","NVTS","LAZR","LIDR","OUST","VLDR",
    # Small/mid cap momentum
    "AXON","MASI","PODD","INSP","NVCR","TGTX","KRTX","RXRX","EXAS","NTRA",
    "DOCS","PHR","CERT","ACCD","HIMS","RO","TDOC","AMWL","ONEM","SGFY",
    "GTLB","DOMO","TASK","BRZE","SMAR","FRSH","SPRK","SPRINKLR","FROG","TOST",
]

@app.on_event("startup")
def _startup_init():
    try:
        if callable(_init_llm_client):
            _init_llm_client()
    except Exception:
        pass
    # Pre-seed scan universe so first request doesn't hit cold expensive ranking.
    try:
        seed = [s for s in _SEED_UNIVERSE if s]
        if seed and not _SCAN_UNIVERSE_CACHE.get("ranked"):
            _SCAN_UNIVERSE_CACHE["ts"] = float(time.time())
            _SCAN_UNIVERSE_CACHE["ranked"] = list(seed)
            _SCAN_UNIVERSE_CACHE["filtered"] = list(seed)
    except Exception:
        pass
    try:
        # Delay scan so worker is fully up before loading 3000 symbols.
        _t = threading.Timer(180, _bg_v2_scan_loop)
        _t.daemon = True
        _t.start()
    except Exception:
        pass
    try:
        # Pre-mover scan fires 5 min after startup so main scan gets priority
        _t2 = threading.Timer(300, _bg_premover_scan_loop)
        _t2.daemon = True
        _t2.start()
    except Exception:
        pass
    try:
        # Brain DB init — creates tables if they don't exist yet
        from brain import init_brain_db
        init_brain_db()
        # Outcome checker: runs 10 min after startup, then every 6 hours
        _t3 = threading.Timer(600, _bg_brain_outcome_loop)
        _t3.daemon = True
        _t3.start()
    except Exception as _be:
        log.warning(f"brain init error: {_be}")
    # Auto-train NN on startup if we have enough resolved picks and model is stale/missing
    def _startup_nn_train():
        try:
            from ml.predictor import model_is_ready
            import os as _os
            from ml.nn_model import _MODEL_PATH
            model_age_h = (_os.path.getmtime(_MODEL_PATH) if _os.path.isfile(_MODEL_PATH) else 0)
            model_stale = (time.time() - model_age_h) > 12 * 3600  # retrain if >12h old
            if not model_is_ready() or model_stale:
                from ml.trainer import run_training
                result = run_training(force=False)
                log.info(f"startup nn_train: {result}")
        except Exception as _te:
            log.warning(f"startup nn_train error: {_te}")
    try:
        _t_nn = threading.Timer(120, _startup_nn_train)
        _t_nn.daemon = True
        _t_nn.start()
    except Exception:
        pass

_ALLOWED_ORIGINS_RAW = os.getenv("ALLOWED_ORIGINS", "")
_CORS_ORIGINS: list[str] = (
    [o.strip() for o in _ALLOWED_ORIGINS_RAW.split(",") if o.strip()]
    if _ALLOWED_ORIGINS_RAW
    else ["http://localhost:3000", "http://localhost:5173", "http://localhost:8000"]
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=_CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type", "X-Requested-With"],
)


class _SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """Inject standard security response headers on every reply."""
    async def dispatch(self, request: Request, call_next):
        resp = await call_next(request)
        resp.headers.setdefault("X-Frame-Options", "DENY")
        resp.headers.setdefault("X-Content-Type-Options", "nosniff")
        resp.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
        resp.headers.setdefault(
            "Content-Security-Policy",
            "default-src 'self'; script-src 'self'; object-src 'none'; frame-ancestors 'none'",
        )
        resp.headers.setdefault("Permissions-Policy", "geolocation=(), camera=(), microphone=()")
        return resp


app.add_middleware(_SecurityHeadersMiddleware)


def _require_debug():
    if not os.getenv("DEBUG"):
        raise HTTPException(status_code=403, detail="Debug endpoints are disabled in production")


@app.get("/debug/openai", include_in_schema=False)
def debug_openai(_: None = Depends(_require_debug)):
    key = os.getenv("OPENAI_API_KEY") or ""
    present = bool(key)

    client_ok = False
    client_err = ""
    try:
        if present:
            _ = _get_openai_client()
            client_ok = True
    except Exception as e:
        client_ok = False
        client_err = str(e)[:200]

    cb_open = False
    try:
        cb_open = bool(_llm_cb_is_open())
    except Exception:
        cb_open = False

    try:
        fp = f"len:{len(key)}"
        if len(key) >= 8:
            fp = f"len:{len(key)} prefix:{key[:3]}… suffix:{key[-2:]}"
    except Exception:
        fp = "unavailable"

    return _no_nulls({
        "status": "ok",
        "openai_key_present": present,
        "openai_key_fingerprint": fp,
        "openai_client_ok": client_ok,
        "openai_client_error": client_err,
        "llm_circuit_breaker_open": cb_open,
    })


@app.get("/debug/llm", include_in_schema=False)
def debug_llm(_: None = Depends(_require_debug)):
    # Minimal live call to verify that the key + model access + billing are working.
    if not os.getenv("OPENAI_API_KEY"):
        return {"success": False, "reason": "key_missing"}

    try:
        if _llm_cb_is_open():
            return {"success": False, "reason": "circuit_breaker_open"}
    except Exception:
        pass

    try:
        client = _get_openai_client()
    except Exception as e:
        return {"success": False, "reason": "client_unavailable", "error": str(e)[:200]}

    resp = client.chat.completions.create(
        model=str(os.getenv("OPENAI_DEBUG_MODEL", "gpt-4.1-mini") or "gpt-4.1-mini"),
        messages=[{"role": "user", "content": "Reply exactly: AI live"}],
        max_tokens=10,
        timeout=15,
    )

    text = ""
    try:
        text = str(resp.choices[0].message.content or "")
    except Exception:
        text = ""

    out: Dict[str, Any] = {"success": True, "text": text.strip()[:60]}

    # Also test the Responses API wrapper used by news sentiment.
    try:
        from llm_client import call_llm_text

        rtext = call_llm_text(
            system="Return EXACTLY the text: AI live",
            user="Reply now.",
            model=str(os.getenv("OPENAI_DEBUG_MODEL", "gpt-4.1-mini") or "gpt-4.1-mini"),
            max_output_tokens=20,
            timeout_s=15.0,
        )
        out["responses_ok"] = True
        out["responses_text"] = str(rtext or "").strip()[:60]
        text = ""
        try:
            text = str(resp.choices[0].message.content or "")
        except Exception:
            text = ""
        out: Dict[str, Any] = {"success": True, "text": text.strip()[:60]}

        # Also test the Responses API wrapper used by news sentiment.
        try:
            from llm_client import call_llm_text

            rtext = call_llm_text(
                system="Return EXACTLY the text: AI live",
                user="Reply now.",
                model=str(os.getenv("OPENAI_DEBUG_MODEL", "gpt-4.1-mini") or "gpt-4.1-mini"),
                max_output_tokens=20,
                timeout_s=15.0,
            )
            out["responses_ok"] = True
            out["responses_text"] = str(rtext or "").strip()[:60]
        except Exception as e:
            out["responses_ok"] = False
            out["responses_error"] = f"{type(e).__name__}:{str(e)[:200]}"

        return out
    except Exception as e:
        return {"success": False, "error": str(e)[:240]}


def _format_time_range_local(
    *,
    start_hm_et: str,
    end_hm_et: str,
    user_tz: Optional[str],
) -> str:
    et = ZoneInfo("America/New_York")
    uz = _safe_zoneinfo(user_tz)

    base_et = datetime.now(et)
    try:
        sh, sm = [int(x) for x in str(start_hm_et).split(":", 1)]
        eh, em = [int(x) for x in str(end_hm_et).split(":", 1)]
    except Exception:
        sh, sm, eh, em = 10, 15, 11, 0

    start_dt_et = datetime(base_et.year, base_et.month, base_et.day, sh, sm, tzinfo=et)
    end_dt_et = datetime(base_et.year, base_et.month, base_et.day, eh, em, tzinfo=et)

    start_local = start_dt_et.astimezone(uz)
    end_local = end_dt_et.astimezone(uz)

    try:
        start_str = start_local.strftime("%-I:%M")
        end_str = end_local.strftime("%-I:%M %p")
    except Exception:
        start_str = start_local.strftime("%I:%M").lstrip("0")
        end_str = end_local.strftime("%I:%M %p").lstrip("0")
    return f"{start_str} – {end_str}"


def _format_recheck_local(*, hour_et: int, minute_et: int, user_tz: Optional[str], days_ahead: int) -> str:
    et = ZoneInfo("America/New_York")
    uz = _safe_zoneinfo(user_tz)
    base_et = datetime.now(et) + timedelta(days=int(days_ahead))
    dt_et = datetime(base_et.year, base_et.month, base_et.day, int(hour_et), int(minute_et), tzinfo=et)
    dt_local = dt_et.astimezone(uz)
    try:
        time_str = dt_local.strftime("%-I:%M %p")
    except Exception:
        time_str = dt_local.strftime("%I:%M %p").lstrip("0")

    now_local = datetime.now(uz)
    day_label = "Today"
    try:
        if dt_local.date() == (now_local.date() + timedelta(days=1)):
            day_label = "Tomorrow"
        elif dt_local.date() != now_local.date():
            day_label = dt_local.strftime("%A")
    except Exception:
        day_label = "Today"
    return f"{time_str} {day_label}"


def _execution_trade_type(out: Dict[str, Any]) -> str:
    factors = out.get("factors") if isinstance(out.get("factors"), dict) else {}
    ta = out.get("technical_analysis") if isinstance(out.get("technical_analysis"), dict) else {}

    momentum = None
    volatility = None
    trend = None
    try:
        momentum = float(factors.get("momentum"))
    except Exception:
        momentum = None
    try:
        volatility = float(factors.get("volatility"))
    except Exception:
        volatility = None
    try:
        trend = float(factors.get("trend"))
    except Exception:
        trend = None

    news = out.get("news_sentiment") if isinstance(out.get("news_sentiment"), dict) else {}
    news_dir = str(news.get("direction") or "").upper()
    catalyst = (news_dir in ("BULLISH", "BEARISH")) or (str(news.get("summary") or "").strip() not in ("", "unavailable"))

    if (momentum is not None and momentum >= 65) and (volatility is not None and volatility >= 55) and catalyst:
        return "Intraday"
    if (trend is not None and trend >= 60) and (volatility is not None and volatility <= 70):
        return "Swing (1–3 days)"

    horizon = str(out.get("time_horizon") or "").strip().lower()
    if "day" in horizon and "1" in horizon:
        return "Swing (1–3 days)"
    if str(ta.get("setup") or "").strip().lower() in ("breakout", "news", "momentum"):
        return "Intraday"
    return "Intraday"


def _execution_entry_type(out: Dict[str, Any]) -> str:
    if out.get("buy_zone") is not None:
        return "VWAP Pullback"
    rec = str(out.get("recommendation") or "").strip().upper()
    if rec == "BUY":
        return "Breakout Confirmation"
    return "Pullback / Reclaim"


def _execution_buy_zone_str(out: Dict[str, Any]) -> Optional[str]:
    bz = out.get("buy_zone")
    if isinstance(bz, dict):
        lo = bz.get("low")
        hi = bz.get("high")
        try:
            if lo is not None and hi is not None:
                return f"${round(float(lo), 2)} – ${round(float(hi), 2)}"
        except Exception:
            return None

    entry = out.get("entry")
    stop = out.get("stop")
    try:
        if entry is not None and stop is not None:
            e = float(entry)
            s = float(stop)
            if math.isfinite(e) and math.isfinite(s) and e > 0:
                lo = min(e, s)
                hi = max(e, s)
                return f"${round(lo, 2)} – ${round(hi, 2)}"
    except Exception:
        return None


def _json_loads_loose(s: str) -> Optional[Dict[str, Any]]:
    """Best-effort JSON object loader for LLM responses.
    Prevents 500s when the model returns extra text around JSON.
    """
    if not isinstance(s, str):
        return None
    raw = s.strip()
    if not raw:
        return None
    try:
        j = json.loads(raw)
        return j if isinstance(j, dict) else None
    except Exception:
        pass
    # Try to extract first JSON object
    try:
        a = raw.find("{")
        b = raw.rfind("}")
        if a >= 0 and b > a:
            j2 = json.loads(raw[a : b + 1])
            return j2 if isinstance(j2, dict) else None
    except Exception:
        return None
    return None



def _badge_from_market(market: Optional[Dict[str, Any]]) -> str:
    try:
        if not isinstance(market, dict):
            return "POST_MARKET_CLOSE_DATA"
        if bool(market.get("is_open")):
            return "LIVE_MARKET_DATA"
        if str(market.get("session_context") or "").strip().upper() == "PRE_MARKET":
            return "PRE_MARKET_DATA"
        return "POST_MARKET_CLOSE_DATA"
    except Exception:
        return "POST_MARKET_CLOSE_DATA"


_OPENAI_CLIENT_SINGLETON = None


def _get_openai_client():
    global _OPENAI_CLIENT_SINGLETON
    if _OPENAI_CLIENT_SINGLETON is not None:
        return _OPENAI_CLIENT_SINGLETON
    try:
        from openai import OpenAI  # type: ignore
    except Exception as e:
        raise RuntimeError("openai_client_unavailable") from e
    try:
        _OPENAI_CLIENT_SINGLETON = OpenAI(max_retries=0)
    except TypeError:
        # Older SDK versions may not support max_retries
        _OPENAI_CLIENT_SINGLETON = OpenAI()
    return _OPENAI_CLIENT_SINGLETON


def _execution_plan(out: Dict[str, Any], user_tz: Optional[str]) -> Dict[str, Any]:
    trade_type = _execution_trade_type(out)

    best_day = "Today" if trade_type == "Intraday" else "Tomorrow"
    if trade_type == "Intraday":
        best_time_local = _format_time_range_local(start_hm_et="10:15", end_hm_et="11:00", user_tz=user_tz)
        recheck_local = _format_recheck_local(hour_et=9, minute_et=45, user_tz=user_tz, days_ahead=1)
    else:
        best_time_local = _format_time_range_local(start_hm_et="09:35", end_hm_et="10:10", user_tz=user_tz)
        recheck_local = _format_recheck_local(hour_et=9, minute_et=45, user_tz=user_tz, days_ahead=1)

    conf = out.get("confidence")
    conf_1_to_10 = None
    try:
        if conf is not None:
            c = float(conf)
            if c <= 1.0:
                conf_1_to_10 = round(c * 10.0, 1)
            else:
                conf_1_to_10 = round(c, 1)
    except Exception:
        conf_1_to_10 = None
    if conf_1_to_10 is None:
        try:
            conf_1_to_10 = round(float(out.get("score") or 0.0), 1)
        except Exception:
            conf_1_to_10 = 0.0

    plan_tz = str(user_tz or "America/New_York")
    try:
        _ = ZoneInfo(plan_tz)
    except Exception:
        plan_tz = "America/New_York"

    return {
        "trade_type": trade_type,
        "best_day": best_day,
        "best_time_local": best_time_local,
        "entry_type": _execution_entry_type(out),
        "buy_zone": _execution_buy_zone_str(out),
        "confidence": float(_clamp_0_to_10(conf_1_to_10 or 0.0)),
        "recheck_local": recheck_local,
        "timezone": plan_tz,
        "footer_warning": "Avoid pre-market entry",
        "subtitle": "System-timed entry guidance",
    }

try:
    from openai import OpenAI as _OpenAI  # type: ignore
    _OPENAI_AVAILABLE = True
except ImportError:
    _OpenAI = None  # type: ignore
    _OPENAI_AVAILABLE = False

try:
    from indicators import (
        calculate_liquidity,
        calculate_momentum,
        calculate_risk,
        calculate_trend,
        calculate_volatility,
    )
except Exception:
    calculate_momentum = None
    calculate_trend = None
    calculate_volatility = None
    calculate_liquidity = None
    calculate_risk = None

try:
    from data_fetcher import get_top_movers  # type: ignore
except Exception:
    def get_top_movers(limit: int) -> List[Dict[str, Any]]:
        return []

# ----------------------------
# ENV
# ----------------------------
load_dotenv()

log = logging.getLogger("stackiq")
if not log.handlers:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

# Warn (do not crash) when keys are missing so the platform can boot in degraded mode.
try:
    if not os.getenv("ALPACA_API_KEY"):
        log.warning("ALPACA_API_KEY missing — market data degraded")
except Exception:
    pass
try:
    if not os.getenv("OPENAI_API_KEY"):
        log.warning("OPENAI_API_KEY missing — LLM unavailable")
except Exception:
    pass


_LOG_THROTTLE: Dict[str, float] = {}


def _log_throttled(level: str, key: str, msg: str, min_interval_sec: float = 60.0) -> None:
    try:
        now_ts = float(time.time())
    except Exception:
        return
    try:
        last = float(_LOG_THROTTLE.get(str(key), 0.0))
    except Exception:
        last = 0.0
    try:
        if last > 0.0 and (now_ts - last) < float(min_interval_sec):
            return
    except Exception:
        pass
    try:
        _LOG_THROTTLE[str(key)] = now_ts
    except Exception:
        pass
    try:
        lvl = str(level or "info").lower().strip()
        if lvl == "debug":
            log.debug(msg)
        elif lvl == "warning" or lvl == "warn":
            log.warning(msg)
        elif lvl == "error":
            log.error(msg)
        else:
            log.info(msg)
    except Exception:
        pass


def now_iso() -> str:
    try:
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    except Exception:
        try:
            return datetime.utcnow().replace(microsecond=0).isoformat() + "+00:00"
        except Exception:
            return ""


def _symbol_sanitize(symbol: str, *, allow_extended: bool = False) -> Dict[str, Any]:
    s0 = str(symbol or "")
    s = s0.strip().upper()
    if not s:
        return {"ok": False, "symbol": "", "reason": "empty"}
    if len(s) > 12:
        return {"ok": False, "symbol": s[:12], "reason": "too_long"}

    ok = True
    try:
        for ch in s:
            if ch.isalnum():
                continue
            if allow_extended and ch in (".", "-", "_"):
                continue
            ok = False
            break
    except Exception:
        ok = False

    if not ok:
        return {"ok": False, "symbol": s, "reason": "invalid_chars"}
    return {"ok": True, "symbol": s, "reason": "ok"}


def _clamp_0_to_10(x: Any) -> float:
    try:
        v = float(x)
    except Exception:
        v = 0.0
    if not math.isfinite(v):
        v = 0.0
    if v < 0.0:
        v = 0.0
    if v > 10.0:
        v = 10.0
    return float(v)


def _db_path() -> str:
    try:
        p = str(os.getenv("STACKIQ_DB_PATH", "stackiq.db") or "stackiq.db").strip()
    except Exception:
        p = "stackiq.db"
    return p if p else "stackiq.db"


def _db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(_db_path())
    conn.row_factory = sqlite3.Row
    return conn


def _db_init() -> None:
    try:
        conn = _db_connect()
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE IF NOT EXISTS portfolio (symbol TEXT PRIMARY KEY, shares REAL, avg_price REAL, added_at TEXT)"
        )
        cur.execute(
            "CREATE TABLE IF NOT EXISTS watchlist (symbol TEXT PRIMARY KEY, added_at TEXT)"
        )
        cur.execute(
            "CREATE TABLE IF NOT EXISTS saved_picks (id TEXT PRIMARY KEY, symbol TEXT, side TEXT, entry REAL, stop_loss REAL, targets_json TEXT, opened_at TEXT, closed_at TEXT, close_price REAL, score REAL, confidence REAL, reason TEXT, source TEXT, status TEXT)"
        )
        conn.commit()
        conn.close()
    except Exception:
        try:
            conn.close()
        except Exception:
            pass


try:
    _db_init()
except Exception:
    pass


def _latest_price_for_symbol(symbol: str) -> Optional[float]:
    sym = str(symbol or "").strip().upper()
    if not sym:
        return None
    try:
        snap = get_snapshot(sym)
    except Exception:
        snap = None
    if not isinstance(snap, dict):
        return None
    bar = snap.get("dailyBar") if isinstance(snap.get("dailyBar"), dict) else {}
    lt = snap.get("latestTrade") if isinstance(snap.get("latestTrade"), dict) else {}
    try:
        px = lt.get("p") if lt.get("p") is not None else bar.get("c")
        return float(px) if px is not None else None
    except Exception:
        return None


def _last_price_from_snapshot(snapshot: Any) -> Optional[float]:
    if not isinstance(snapshot, dict):
        return None
    bar = snapshot.get("dailyBar") if isinstance(snapshot.get("dailyBar"), dict) else {}
    lt = snapshot.get("latestTrade") if isinstance(snapshot.get("latestTrade"), dict) else {}
    try:
        px = lt.get("p") if lt.get("p") is not None else bar.get("c")
        return float(px) if px is not None else None
    except Exception:
        return None


def _px2(x: Any) -> float:
    v = _safe_f(x)
    if v is None or float(v) <= 0.0:
        return 0.0
    try:
        return float(round(float(v), 2))
    except Exception:
        return float(v)


def _i0(x: Any) -> int:
    try:
        v = int(float(x))
    except Exception:
        v = 0
    if v < 0:
        v = 0
    return int(v)


def _market_data_from_snapshot_and_bars(
    *,
    symbol: str,
    snapshot: Optional[Dict[str, Any]],
    daily_bars: List[Dict[str, Any]],
    intraday_bars: List[Dict[str, Any]],
) -> Dict[str, Any]:
    sym = str(symbol or "").strip().upper()
    snap = snapshot if isinstance(snapshot, dict) else {}

    lt = snap.get("latestTrade") if isinstance(snap.get("latestTrade"), dict) else {}
    lq = snap.get("latestQuote") if isinstance(snap.get("latestQuote"), dict) else {}
    bar = snap.get("dailyBar") if isinstance(snap.get("dailyBar"), dict) else {}
    prev = snap.get("prevDailyBar") if isinstance(snap.get("prevDailyBar"), dict) else {}

    last_trade = _safe_f(lt.get("p"))
    bid = _safe_f(lq.get("bp"))
    ask = _safe_f(lq.get("ap"))

    if last_trade is None:
        try:
            last_trade = _safe_f(bar.get("c"))
        except Exception:
            last_trade = None
    if last_trade is None:
        try:
            last_trade = _safe_f(daily_bars[-1].get("c")) if daily_bars else None
        except Exception:
            last_trade = None
    if last_trade is None:
        try:
            last_trade = _safe_f(intraday_bars[-1].get("c")) if intraday_bars else None
        except Exception:
            last_trade = None

    snapshot_available = False
    try:
        # Treat snapshot as available if we can derive a valid last price from *any*
        # upstream data source (latestTrade, dailyBar close, or candles).
        snapshot_available = bool(last_trade is not None and float(last_trade) > 0.0)
    except Exception:
        snapshot_available = False

    daily_open = _safe_f(bar.get("o"))
    daily_high = _safe_f(bar.get("h"))
    daily_low = _safe_f(bar.get("l"))
    daily_close = _safe_f(bar.get("c"))
    daily_volume = _safe_f(bar.get("v"))
    prev_close = _safe_f(prev.get("c"))

    if daily_close is None:
        try:
            daily_close = _safe_f(daily_bars[-1].get("c")) if daily_bars else None
        except Exception:
            daily_close = None

    if daily_open is None or daily_high is None or daily_low is None or daily_volume is None:
        try:
            if daily_bars:
                b = daily_bars[-1]
                daily_open = daily_open if daily_open is not None else _safe_f(b.get("o"))
                daily_high = daily_high if daily_high is not None else _safe_f(b.get("h"))
                daily_low = daily_low if daily_low is not None else _safe_f(b.get("l"))
                daily_volume = daily_volume if daily_volume is not None else _safe_f(b.get("v"))
        except Exception:
            pass

    if prev_close is None:
        try:
            if len(daily_bars) >= 2:
                prev_close = _safe_f(daily_bars[-2].get("c"))
        except Exception:
            prev_close = None

    percent_change = None
    try:
        if last_trade is not None and daily_open is not None and float(daily_open) > 0.0:
            percent_change = ((float(last_trade) - float(daily_open)) / float(daily_open)) * 100.0
    except Exception:
        percent_change = None

    atr14 = _atr_14_from_bars(daily_bars)
    if atr14 is None:
        atr14 = _atr_14_from_bars(intraday_bars)
    if atr14 is not None and float(atr14) <= 0:
        atr14 = None

    vwap = _vwap_from_bars(intraday_bars)
    if vwap is None:
        vwap = _vwap_from_bars(daily_bars)
    if vwap is not None and float(vwap) <= 0:
        vwap = None

    rel_vol = None
    rel_vol_status = "ok"
    try:
        vols = [int(b.get("v") or 0) for b in (daily_bars[-21:-1] if len(daily_bars) >= 22 else []) if isinstance(b, dict)]
        vols = [int(v) for v in vols if int(v) > 0]
        curv = int(daily_volume) if daily_volume is not None else 0
        if vols and curv > 0:
            avg = float(sum(vols)) / float(len(vols))
            if avg > 0:
                rel_vol = float(curv) / float(avg)
        else:
            rel_vol_status = "unavailable"
    except Exception:
        rel_vol = None
        rel_vol_status = "unavailable"

    degraded = False
    if last_trade is None or float(last_trade) <= 0:
        degraded = True
    if bid is None or ask is None:
        degraded = True

    out = {
        "symbol": sym,
        "last_trade_price": _px2(last_trade),
        "last_price": _px2(last_trade),
        "bid": _px2(bid),
        "ask": _px2(ask),
        "volume": _i0(daily_volume),
        "vwap": _px2(vwap),
        "open": _px2(daily_open),
        "high": _px2(daily_high),
        "low": _px2(daily_low),
        "close": _px2(daily_close),
        "prev_close": _px2(prev_close),
        "percent_change": float(round(float(percent_change), 4)) if percent_change is not None and math.isfinite(float(percent_change)) else None,
        "atr14": _px2(atr14),
        "relative_volume": float(round(float(rel_vol), 4)) if rel_vol is not None and math.isfinite(float(rel_vol)) else 0.0,
        "relative_volume_status": str(rel_vol_status),
        "intraday_bars": (intraday_bars[-390:] if isinstance(intraday_bars, list) else []),
        "daily_bars": (daily_bars[-200:] if isinstance(daily_bars, list) else []),
        "source": "alpaca",
        "snapshot_available": bool(snapshot_available),
        "market_data_degraded": bool(degraded),
        "updated_at": now_iso(),
    }

    if out.get("atr14") is not None:
        try:
            if float(out.get("atr14") or 0.0) < 0.0:
                out["atr14"] = 0.0
        except Exception:
            out["atr14"] = 0.0

    return out


def _fetch_yf_52week(symbol: str) -> Dict[str, Any]:
    """Returns 52-week high/low from Yahoo Finance. Cached 1 hr."""
    key = symbol.upper()
    cached = _YF_52W_CACHE.get(key, {})
    if cached and time.time() - cached.get("_ts", 0) < 3600:
        return cached
    try:
        url = f"https://query2.finance.yahoo.com/v8/finance/chart/{symbol}"
        r = requests.get(url, params={"interval": "1d", "range": "1d"},
                         headers={"User-Agent": "Mozilla/5.0"}, timeout=5)
        meta = (r.json().get("chart") or {}).get("result", [{}])[0].get("meta") or {}
        hi = float(meta.get("fiftyTwoWeekHigh") or 0)
        lo = float(meta.get("fiftyTwoWeekLow") or 0)
        if hi > 0 and lo > 0 and hi > lo:
            result = {"high": hi, "low": lo, "_ts": time.time()}
            _YF_52W_CACHE[key] = result
            return result
    except Exception:
        pass
    return {}


def _fib_targets_and_stop(
    *, current_price: float, w52_low: float, w52_high: float,
    entry_price: Optional[float] = None,
) -> Dict[str, Any]:
    """
    Compute Fibonacci price levels from the 52-week range.
    Returns the 3 nearest levels above max(current_price, entry_price) as targets,
    and the nearest level below current_price as the stop.
    These are the levels traders and technicians actually watch.
    """
    lp = float(current_price)
    lo = float(w52_low)
    hi = float(w52_high)
    if lp <= 0 or lo <= 0 or hi <= lo:
        return {}

    # Targets must be above the entry (could be a breakout entry above current price)
    floor = max(lp, float(entry_price or 0)) * 1.005

    rng = hi - lo
    fracs = [0.236, 0.382, 0.500, 0.618, 0.786, 1.000,
             1.236, 1.382, 1.618, 2.000, 2.618]
    levels = sorted(set(round(lo + f * rng, 2) for f in fracs))

    above = [x for x in levels if x > floor]
    # Stop must be below both current price and entry to avoid stop > entry errors
    stop_ceiling = min(lp, float(entry_price or lp)) * 0.995
    below = [x for x in reversed(levels) if x < stop_ceiling]

    targets = above[:3]
    stop = below[0] if below else round(lo, 2)

    if len(targets) < 3:
        return {}
    return {"targets": targets, "stop": stop}


def _trade_plan_from_spec(
    *,
    last_price: float,
    atr14: float,
    vwap: float,
    resistance: float,
    open_price: Optional[float] = None,
    prev_close: Optional[float] = None,
) -> Dict[str, Any]:
    lp = float(last_price or 0.0)
    atr = float(atr14 or 0.0)
    vw = float(vwap or 0.0)
    res = float(resistance or 0.0)
    op = float(open_price or 0.0)
    pc = float(prev_close or 0.0)

    if lp <= 0.0:
        return {"entry": None, "stop": None, "targets": [None, None, None], "gain_pct": None, "risk_reward": None}

    # --- Detect gap day (earnings / catalyst) ---
    # A gap >5% means we use Fibonacci extensions off the gap as targets.
    # These are real technical levels traders actually use, not just ATR multiples.
    gap_pct = ((op - pc) / pc) if (op > 0 and pc > 0) else 0.0
    is_gap_day = gap_pct > 0.05

    if atr <= 0.0:
        pct = 0.09
        try:
            if float(lp) < 20.0:
                pct = 0.18
            elif float(lp) < 60.0:
                pct = 0.12
            else:
                pct = 0.06
        except Exception:
            pct = 0.09
        atr = float(lp) * float(pct)

    # ATR floor so targets are never absurdly tight on stale historical data.
    atr_floor_pct = 0.05 if lp < 20.0 else (0.03 if lp < 60.0 else 0.02)
    atr = max(atr, lp * atr_floor_pct)

    entry = 0.0
    try:
        if vw > 0 and lp < vw:
            entry = float(vw) * 1.001
        elif res > 0 and res >= lp * 0.90:
            entry = float(res) * 1.001
        else:
            entry = float(lp) * 1.005
    except Exception:
        entry = float(lp) * 1.005

    try:
        if entry > float(lp) * 1.25:
            entry = float(lp) * 1.05
        elif entry < float(lp) * 0.90:
            entry = float(lp) * 1.005
    except Exception:
        pass

    if is_gap_day:
        # Fibonacci extensions off the gap impulse (open - prev_close).
        # These are the actual levels traders target on catalyst gap plays:
        #   T1 = gap open + 0.618 × gap  (first extension — take-partial)
        #   T2 = gap open + 1.0  × gap  (measured move — standard target)
        #   T3 = gap open + 1.618 × gap (golden ratio extension — runner)
        gap = op - pc
        t1 = op + 0.618 * gap
        t2 = op + 1.0 * gap
        t3 = op + 1.618 * gap
        # Stop: gap open level — if it falls back below the gap, thesis is broken.
        stop = op * 0.99
        # Clamp: never let stop be above entry or targets be below current price.
        stop = min(stop, entry * 0.985)
        t1 = max(t1, lp * 1.02)
        t2 = max(t2, t1 * 1.01)
        t3 = max(t3, t2 * 1.01)
    else:
        stop = float(entry) - float(atr)
        t1 = float(entry) + float(atr)
        t2 = float(entry) + float(atr) * 2.0
        t3 = float(entry) + float(atr) * 3.0

    if t1 < entry:
        t1 = entry
    if t2 < t1:
        t2 = t1
    if t3 < t2:
        t3 = t2
    if stop >= entry:
        stop = float(entry) - max(0.01, float(atr) * 0.25)

    # Targets must always be above the current price — if the stock has already
    # run past the entry-anchored targets, rescale from current price instead.
    if lp > 0 and t3 < lp:
        t1 = lp * 1.03
        t2 = lp * 1.07
        t3 = lp * 1.12

    gain_pct = 0.0
    rr = 0.0
    try:
        gain_pct = ((float(t3) - float(entry)) / float(entry)) * 100.0 if float(entry) > 0 else 0.0
    except Exception:
        gain_pct = 0.0
    try:
        rr = (float(t2) - float(entry)) / (float(entry) - float(stop)) if float(entry - stop) != 0 else 0.0
    except Exception:
        rr = 0.0

    return {
        "entry": _px2(entry),
        "stop": _px2(stop),
        "targets": [_px2(t1), _px2(t2), _px2(t3)],
        "gain_pct": float(round(float(gain_pct), 2)),
        "risk_reward": float(round(float(rr), 2)),
    }


def _execution_factors_from_market_data(*, last_price: float, vwap: float, resistance: float, relative_volume: float, atr14: float) -> Dict[str, Any]:
    lp = float(last_price or 0.0)
    vw = float(vwap or 0.0)
    res = float(resistance or 0.0)
    rv = float(relative_volume or 0.0)
    atr = float(atr14 or 0.0)

    breakout = 50.0
    try:
        if res > 0 and lp > 0:
            if lp >= res:
                # Stock has broken above resistance — best possible setup; reward it.
                overshoot = (lp - res) / res
                breakout = min(100.0, 80.0 + overshoot * 100.0)
            else:
                # Below resistance — reward proximity (closer = more actionable).
                d = (res - lp) / res
                breakout = 100.0 - min(100.0, d * 500.0)
    except Exception:
        breakout = 50.0

    vwap_align = 50.0
    try:
        if vw > 0 and lp > 0:
            d = (lp - vw) / vw  # positive = above VWAP (bullish), negative = below
            if d >= 0:
                # Above VWAP: bullish confirmation; mild premium is ideal.
                vwap_align = min(100.0, 65.0 + d * 200.0)
            else:
                # Below VWAP: bearish; penalise the further below it gets.
                vwap_align = max(0.0, 50.0 + d * 500.0)
    except Exception:
        vwap_align = 50.0

    vol_exp = 50.0
    try:
        if rv > 0:
            vol_exp = min(100.0, 25.0 + (rv * 35.0))
    except Exception:
        vol_exp = 50.0

    overhead = 60.0
    try:
        if atr > 0 and res > 0 and lp > 0:
            overhead_dist_atr = (float(res) - float(lp)) / float(atr)
            overhead = 100.0 - min(100.0, max(0.0, overhead_dist_atr) * 25.0)
    except Exception:
        overhead = 60.0

    return {
        "breakout_proximity": float(round(_clamp_0_100(breakout), 1)),
        "vwap_alignment": float(round(_clamp_0_100(vwap_align), 1)),
        "volume_expansion": float(round(_clamp_0_100(vol_exp), 1)),
        "resistance_overhead": float(round(_clamp_0_100(overhead), 1)),
    }


def _format_exec_date_label(date_iso: str) -> str:
    try:
        s = str(date_iso or "").strip()
    except Exception:
        s = ""
    if not s:
        try:
            return datetime.now().strftime("%b %d, %Y")
        except Exception:
            return ""
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.strftime("%b %d, %Y")
    except Exception:
        pass
    try:
        dt = datetime.strptime(s[:10], "%Y-%m-%d")
        return dt.strftime("%b %d, %Y")
    except Exception:
        return s


def _norm_0_1(x: Any) -> float:
    try:
        v = float(x)
    except Exception:
        v = 0.0
    if not math.isfinite(v):
        v = 0.0
    if v > 1.0:
        v = v / 100.0
    if v < 0.0:
        v = 0.0
    if v > 1.0:
        v = 1.0
    return float(v)


def generate_trade_plan(symbol: str, price_data: Dict[str, Any], indicators: Dict[str, Any]) -> Dict[str, Any]:
    sym = str(symbol or "").strip().upper()
    pd0 = price_data if isinstance(price_data, dict) else {}
    ind0 = indicators if isinstance(indicators, dict) else {}

    current_price = _safe_f(pd0.get("current_price"))
    prior_high = _safe_f(pd0.get("prior_high"))
    support = _safe_f(pd0.get("support"))
    vwap = _safe_f(ind0.get("vwap"))
    atr = _safe_f(ind0.get("atr"))
    vol_score = _norm_0_1(ind0.get("volatility_score"))

    if current_price is None or float(current_price) <= 0.0:
        px2 = _latest_price_for_symbol(sym)
        current_price = float(px2) if px2 is not None else 0.0
    if atr is None or float(atr) <= 0.0:
        try:
            base = float(current_price) if current_price is not None and float(current_price) > 0 else 100.0
        except Exception:
            base = 100.0
        atr = base * (0.015 + 0.025 * float(vol_score))
    if vwap is None or float(vwap) <= 0.0:
        vwap = current_price
    if prior_high is None or float(prior_high) <= 0.0:
        prior_high = current_price

    entry = max(
        float(prior_high),
        float(vwap),
        float(current_price) + float(atr) * 0.15,
    )
    entry = _round_px(entry)

    stop = float(entry) - float(atr) * 1.2
    if support is not None:
        try:
            if float(support) > float(stop):
                stop = float(support)
        except Exception:
            pass
    stop = _round_px(stop)
    if stop >= entry:
        stop = _round_px(float(entry) - max(0.01, float(atr) * 0.25))

    target_1 = _round_px(float(entry) + float(atr) * 1.5)
    target_2 = _round_px(float(entry) + float(atr) * 2.5)
    target_3 = _round_px(float(entry) + float(atr) * 4.0)

    gain_pct = 0.0
    try:
        if float(entry) > 0:
            gain_pct = ((float(target_2) - float(entry)) / float(entry)) * 100.0
    except Exception:
        gain_pct = 0.0
    gain_pct = float(round(float(gain_pct), 2))

    rr = 0.0
    try:
        risk = float(entry) - float(stop)
        reward = float(target_2) - float(entry)
        rr = (reward / risk) if risk > 0 else 0.0
    except Exception:
        rr = 0.0
    rr = float(round(float(rr), 2))

    plan = {
        "entry": float(entry),
        "stop": float(stop),
        "targets": [float(target_1), float(target_2), float(target_3)],
        "gain_pct": float(gain_pct),
        "risk_reward": float(rr),
    }

    try:
        _TRADE_PLAN_CACHE.set(_cache_key("modeled_trade_plan", sym), {"atr": float(atr), "vwap": float(vwap), **plan})
    except Exception:
        pass
    return plan


def generate_execution_plan(symbol: str, volatility: Any, trend_strength: Any) -> Dict[str, Any]:
    sym = str(symbol or "").strip().upper()
    vol01 = _norm_0_1(volatility)
    ts01 = _norm_0_1(trend_strength)

    try:
        regime = market_regime() or {}
    except Exception:
        regime = {}
    is_open = bool((regime or {}).get("is_open"))
    date_iso = _execution_date_iso(market_is_open=is_open)
    date_label = _format_exec_date_label(date_iso)

    if vol01 > 0.7:
        window = "9:35 – 10:15 AM"
    elif vol01 > 0.4:
        window = "9:45 – 10:45 AM"
    else:
        window = "10:30 – 12:00 PM"

    breakout_detected = bool(ts01 >= 0.65)
    pullback_trend = bool((ts01 >= 0.55) and (vol01 <= 0.5))
    if breakout_detected:
        method = "Break prior high breakout"
    elif pullback_trend:
        method = "Pullback continuation entry"
    else:
        method = "VWAP reclaim momentum entry"

    modeled = None
    try:
        modeled = _TRADE_PLAN_CACHE.get(_cache_key("modeled_trade_plan", sym))
    except Exception:
        modeled = None

    entry = _safe_f((modeled or {}).get("entry"))
    atr = _safe_f((modeled or {}).get("atr"))
    if entry is None or float(entry) <= 0.0:
        px2 = _latest_price_for_symbol(sym)
        entry = float(px2) if px2 is not None else 0.0
    if atr is None or float(atr) <= 0.0:
        base = float(entry) if entry is not None and float(entry) > 0 else 100.0
        atr = base * (0.015 + 0.025 * float(vol01))

    buy_low = float(entry) - float(atr) * 0.2
    buy_high = float(entry) + float(atr) * 0.1
    buy_zone = f"${buy_low:.2f} – ${buy_high:.2f}"

    return {
        "date": str(date_label or ""),
        "window": str(window),
        "entry_method": str(method),
        "buy_zone": str(buy_zone),
    }


def generate_technical_scores(symbol: str) -> Dict[str, Any]:
    _ = symbol
    return {
        "momentum": random.randint(5, 9),
        "trend": random.randint(5, 9),
        "volatility": random.randint(4, 8),
        "liquidity": random.randint(6, 10),
        "risk": random.randint(3, 7),
    }


def generate_sentiment(symbol: str) -> Dict[str, Any]:
    _ = symbol
    return {
        "direction": "NEUTRAL",
        "summary": "No major sentiment catalysts detected.",
        "headlines": [],
    }


def _safe_str(x: Any, default: str) -> str:
    try:
        s = str(x) if x is not None else ""
    except Exception:
        s = ""
    s = s.strip()
    return s if s else str(default)


def _safe_float(x: Any, default: float) -> float:
    try:
        v = float(x)
    except Exception:
        v = float(default)
    if not math.isfinite(v):
        v = float(default)
    return float(v)


def _safe_list_str(x: Any) -> List[str]:
    if isinstance(x, list):
        out: List[str] = []
        for it in x:
            try:
                s = str(it).strip()
            except Exception:
                s = ""
            if s:
                out.append(s)
        return out


def get_candles(symbol: str, timeframe: str = "1Day", limit: int = 100) -> Any:
    """Fetch candles for indicators.

    This is a thin wrapper so analyze() can always request candles using the same
    signature regardless of which underlying Alpaca helper is used.
    """
    sym = (symbol or "").strip().upper()
    tf = (timeframe or "1Day").strip()
    try:
        limit = int(limit or 100)
    except Exception:
        limit = 100
    if limit < 100:
        limit = 100
    if tf != "1Day":
        tf = "1Day"

    data_base_url = (os.getenv("ALPACA_DATA_BASE_URL") or "https://data.alpaca.markets").strip() or "https://data.alpaca.markets"
    feed = (os.getenv("ALPACA_DATA_FEED") or "iex").strip() or "iex"
    if not feed:
        feed = "iex"

    api_key = (os.getenv("ALPACA_API_KEY") or "").strip()
    secret_key = (os.getenv("ALPACA_SECRET_KEY") or "").strip()

    # Prefer returning a DataFrame with columns open/high/low/close/volume.
    df = None
    try:
        import pandas as pd  # type: ignore

        # Primary: direct Alpaca Data API -> OHLCV dataframe.
        # This guarantees correct fields and sort order regardless of optional SDK installs.
        try:
            url = f"{data_base_url.rstrip('/')}/v2/stocks/{sym}/bars"
            end_dt = datetime.now(timezone.utc)
            start_dt = end_dt - timedelta(days=365)
            start_iso = start_dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")
            end_iso = end_dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")
            params = {
                "timeframe": "1Day",
                "start": start_iso,
                "end": end_iso,
                "limit": int(limit or 100),
                "adjustment": "raw",
                "feed": feed,
                "sort": "asc",
            }
            r = requests.get(url, headers=data_headers(), params=params, timeout=12)
            if r is not None and r.status_code == 200:
                payload = r.json() if hasattr(r, "json") else {}
                bars = payload.get("bars") or []
                rows: List[Dict[str, Any]] = []
                for b in bars:
                    if not isinstance(b, dict):
                        continue
                    rows.append(
                        {
                            "t": b.get("t"),
                            "open": b.get("o"),
                            "high": b.get("h"),
                            "low": b.get("l"),
                            "close": b.get("c"),
                            "volume": b.get("v"),
                        }
                    )
                df0 = pd.DataFrame(rows) if rows else pd.DataFrame(columns=["t", "open", "high", "low", "close", "volume"])
                try:
                    if "t" in getattr(df0, "columns", []):
                        df0 = df0.sort_values("t", ascending=True)
                except Exception:
                    pass
                try:
                    df0 = df0.reset_index(drop=True)
                except Exception:
                    pass
                df = df0[["open", "high", "low", "close", "volume"]].copy()
        except Exception:
            df = None

        # Prefer alpaca-trade-api if present (closest to requested client.get_stock_bars(TimeFrame.Day, limit=100)).
        if df is None:
            try:
                from alpaca_trade_api.rest import REST, TimeFrame  # type: ignore

                if api_key and secret_key:
                    trading_base = (os.getenv("ALPACA_TRADING_BASE_URL") or "https://paper-api.alpaca.markets").strip()
                    alpaca = REST(key_id=api_key, secret_key=secret_key, base_url=trading_base, api_version="v2")
                    bars = alpaca.get_bars(sym, TimeFrame.Day, limit=int(limit or 100), adjustment="raw", feed=feed)
                    try:
                        df0 = bars.df  # type: ignore
                    except Exception:
                        df0 = None
                    if df0 is not None:
                        try:
                            if hasattr(df0, "reset_index"):
                                df0 = df0.reset_index()
                        except Exception:
                            pass
                        for col in ("open", "high", "low", "close", "volume"):
                            if col not in getattr(df0, "columns", []):
                                df0[col] = None
                        df = df0[["open", "high", "low", "close", "volume"]].copy()
            except Exception:
                df = None

        # Next: alpaca-py
        if df is None:
            try:
                from alpaca.data.historical import StockHistoricalDataClient  # type: ignore
                from alpaca.data.requests import StockBarsRequest  # type: ignore
                from alpaca.data.timeframe import TimeFrame  # type: ignore

                if api_key and secret_key:
                    client = StockHistoricalDataClient(api_key, secret_key)
                    req = StockBarsRequest(
                        symbol_or_symbols=sym,
                        timeframe=TimeFrame.Day,
                        limit=int(limit or 100),
                        feed=feed,
                        adjustment="raw",
                    )
                    try:
                        bars = client.get_stock_bars(req)
                    except Exception:
                        bars = None
                    df0 = None
                    try:
                        if bars is not None and hasattr(bars, "df"):
                            df0 = bars.df  # type: ignore
                    except Exception:
                        df0 = None
                    if df0 is not None:
                        try:
                            if hasattr(df0, "reset_index"):
                                df0 = df0.reset_index()
                        except Exception:
                            pass
                        for col in ("open", "high", "low", "close", "volume"):
                            if col not in getattr(df0, "columns", []):
                                df0[col] = None
                        df = df0[["open", "high", "low", "close", "volume"]].copy()
            except Exception:
                df = None

    except Exception:
        df = None

    return df


# ---------------------------
# LLM NEWS ANALYSIS (SAFE)
# ---------------------------

@lru_cache(maxsize=256)
def llm_news(symbol: str, headlines: tuple):
    if not OPENAI_API_KEY or not headlines:
        return {
            "sentiment": "neutral",
            "confidence": 0.0,
            "catalyst": "none",
            "risk_flags": [],
            "summary": "No actionable catalyst"
        }

    prompt = f"""
You are a professional short-term trader.
Analyze the following headlines for {symbol}.

Rules:
- No price prediction
- No hype
- Identify real catalysts
- Identify risks
- Return STRICT JSON ONLY

Headlines:
{chr(10).join(headlines)}
"""

    sym = str(symbol or "").strip().upper()

    def _call_llm_news() -> Dict[str, Any]:
        # Hard timeout: never block an API request waiting on OpenAI.
        from llm_client import call_llm_text

        content = call_llm_text(
            system="Return STRICT JSON ONLY.",
            user=prompt,
            model="gpt-4o-mini",
            max_output_tokens=350,
            timeout_s=_openai_timeout_seconds(),
        )
        if not isinstance(content, str) or not content.strip():
            raise ValueError("empty llm response")
        return json.loads(content)

    obj0 = _llm_json_call(kind="llm_news", symbol=sym, allow_llm=True, call_fn=_call_llm_news)
    if isinstance(obj0, dict) and obj0.get("cached_used"):
        try:
            log.info(f"llm cached response used kind=llm_news symbol={sym}")
        except Exception:
            pass
    if not isinstance(obj0, dict) or (obj0.get("llm_used") is False and obj0.get("skipped")):
        try:
            log.info(f"analyze degraded mode activated (llm_news unavailable) symbol={sym}")
        except Exception:
            pass
        return {
            "sentiment": "neutral",
            "confidence": 0.0,
            "catalyst": "unclear",
            "risk_flags": ["llm_unreliable"],
            "summary": "LLM unavailable",
        }

    # Remove wrapper fields if present.
    try:
        obj = dict(obj0)
        obj.pop("llm_used", None)
        obj.pop("cached_used", None)
        obj.pop("rate_limited", None)
        obj.pop("skipped", None)
        obj.pop("reason", None)
        obj.pop("error", None)
        return obj
    except Exception:
        return obj0


# ---------------------------
# PRICE / MOMENTUM ENGINE
# ---------------------------

def price_analysis(snapshot: dict):
    bar = snapshot.get("dailyBar") or {}
    prev = snapshot.get("prevDailyBar") or {}

    close = bar.get("c", 0)
    prev_close = prev.get("c", 0)
    volume = bar.get("v", 0)

    if not close or not prev_close:
        return None

    pct = ((close - prev_close) / prev_close) * 100

    momentum_score = 0
    if pct > 1.8:
        momentum_score += 15
    if pct > 3:
        momentum_score += 10
    if pct < -2:
        momentum_score -= 15

    liquidity_score = 0
    if volume > 1_000_000:
        liquidity_score += 10
    if volume > 3_000_000:
        liquidity_score += 10

    return {
        "pct": round(pct, 2),
        "momentum_score": momentum_score,
        "liquidity_score": liquidity_score,
        "volume": volume
    }


# ----------------------------
# NEWS CLASSIFICATION (SAFE)
# ----------------------------

def classify_news(headlines: List[str]) -> dict:
    """
    Lightweight deterministic news classifier.
    LLM is OPTIONAL and cannot crash analysis.
    """

    if not headlines:
        return {
            "sentiment": "neutral",
            "confidence": 0.0,
            "catalyst": "none",
            "risk_flags": [],
            "summary": "No recent news"
        }

    joined = " ".join(h.lower() for h in headlines)

    risk_flags = []
    sentiment = "neutral"

    if any(k in joined for k in ["offering", "dilution", "convertible"]):
        risk_flags.append("dilution")
        sentiment = "bearish"

    if any(k in joined for k in ["lawsuit", "sec probe", "investigation"]):
        risk_flags.append("lawsuit")
        sentiment = "bearish"

    if any(k in joined for k in ["earnings beat", "guidance raised", "contract", "partnership"]):
        sentiment = "bullish"

    if any(k in joined for k in ["rumor", "speculation", "unconfirmed"]):
        risk_flags.append("hype")

    return {
        "sentiment": sentiment,
        "confidence": 0.6 if sentiment != "neutral" else 0.3,
        "catalyst": "news",
        "risk_flags": risk_flags,
        "summary": headlines[0][:160]
    }

# ANALYZE (FULL)
# ---------------------------

def _derive_buy_zone_and_targets(pa_bar: dict, prev_bar: dict, recommendation: Optional[str]):
    try:
        if recommendation != "BUY":
            return None, None, None, None, None, None

        if not isinstance(pa_bar, dict) or not isinstance(prev_bar, dict):
            return None, None, None, None, None, None

        c = pa_bar.get("c")
        l = pa_bar.get("l")
        h = pa_bar.get("h")
        prev_c = prev_bar.get("c")

        if c is None or l is None or h is None or prev_c is None:
            return None, None, None, None, None, None

        c = float(c)
        l = float(l)
        h = float(h)
        prev_c = float(prev_c)

        if not (math.isfinite(c) and math.isfinite(l) and math.isfinite(h) and math.isfinite(prev_c)):
            return None, None, None, None, None, None

        tr = max(
            h - l,
            abs(h - prev_c),
            abs(l - prev_c),
        )

        min_zone_pct = 0.005
        buy_zone_width = max(tr * 0.5, c * min_zone_pct)

        buy_zone_low = max(c - buy_zone_width, 0.0)
        buy_zone_high = c
        buy_zone = {"low": buy_zone_low, "high": buy_zone_high}
        buy_zone_pct = round(min_zone_pct * 100, 2)

        t1_pct = max(0.8, (tr * 0.8 / c) * 100)
        t2_pct = max(2.0, (tr * 1.8 / c) * 100)

        t1_price = c * (1 + (t1_pct / 100))
        t2_price = c * (1 + (t2_pct / 100))

        profit_targets = [
            {"price": round(t1_price, 4), "pct": round(t1_pct, 2), "type": "T1"},
            {"price": round(t2_price, 4), "pct": round(t2_pct, 2), "type": "T2"},
        ]

        expected_move_pct = float(round(t2_pct, 2))

        risk_pct = ((c - buy_zone_low) / c) * 100 if c > 0 else 0.0
        r_multiple = float(round((t2_pct / risk_pct), 2)) if risk_pct > 0 else None

        outcome_state = "ACTIVE"
        if c >= t2_price:
            outcome_state = "HIT_T2"
        elif c >= t1_price:
            outcome_state = "HIT_T1"
        elif c < buy_zone_low:
            outcome_state = "FAILED"

        return buy_zone, buy_zone_pct, profit_targets, expected_move_pct, r_multiple, outcome_state
    except Exception:
        return None, None, None, None, None, None


def normalize_score(score_100: Any) -> float:
    try:
        s = float(score_100)
    except Exception:
        s = 0.0
    if s < 0:
        s = 0.0
    if s > 100:
        s = 100.0
    return float(round(s, 1))


def _score_0_10_from_0_100(score_0_100: Any) -> Optional[float]:
    try:
        s = float(score_0_100)
    except Exception:
        return None
    if not math.isfinite(s):
        return None
    if s < 0.0:
        s = 0.0
    if s > 100.0:
        s = 100.0
    return float(round(s / 10.0, 1))


def _confidence_pct_0_100(x: Any) -> Optional[int]:
    try:
        v = float(x)
    except Exception:
        return None
    if not math.isfinite(v):
        return None
    if v < 0.0:
        v = 0.0
    if v > 100.0:
        v = 100.0
    try:
        return int(round(v))
    except Exception:
        return None


def _deterministic_execution_plan(entry: Optional[float]) -> Dict[str, str]:
    from datetime import datetime, timedelta

    date_label = ""
    try:
        iso = _execution_date_iso(market_is_open=False)
        date_label = _format_exec_date_label(iso)
    except Exception:
        try:
            date_label = datetime.now().strftime("%b %d, %Y")
        except Exception:
            date_label = ""
    buy_zone = ""
    try:
        if entry is not None and float(entry) > 0:
            e = float(entry)
            buy_zone = f"${e*0.98:.2f} – ${e*1.01:.2f}"
    except Exception:
        buy_zone = ""

    return {
        "date": str(date_label or ""),
        "window": "9:35 – 10:15 AM",
        "entry_method": "Break prior high breakout",
        "buy_zone": buy_zone,
    }


def _empty_analyze_response(symbol: str, status: str) -> Dict[str, Any]:
    sym = str(symbol or "").strip().upper()
    return {
        "status": str(status or "degraded"),
        "reason": "insufficient_data",
        "symbol": sym,
        "asof": now_iso(),
        "market_badge": "degraded",
        "snapshot": {"last": None},
        "best_pick": {"symbol": sym, "score": None, "confidence": None},
        "trade_plan": {"entry": None, "stop": None, "targets": [None, None, None], "gain_pct": None, "risk_reward": None},
        "execution_plan": {"date": "", "window": "", "entry_method": "", "buy_zone": ""},
        "technicals": {"symbol": sym, "technical_analysis": {}, "ai_score": None, "execution_score": None, "system_expectation": ""},
        "news": {"headlines": [], "sentiment": "Neutral", "items": [], "source": "unavailable"},
        "news_sentiment": {
            "direction": "NEUTRAL",
            "score": 0,
            "confidence": 0,
            "summary": "Low news volume. Sentiment confidence reduced.",
            "catalysts": [],
            "sentiment_source": "keyword",
            "headlines": [],
            "headline_items": [],
        },
        "social_sentiment": {"status": "unavailable"},
        "reasoning": {"why": [], "confirms": [], "breaks": []},
        "market_data": {"last_price": None, "source": "alpaca"},
    }


def _deterministic_trade_plan_from_price(*, last_price: Optional[float]) -> Dict[str, Any]:
    try:
        lp = float(last_price) if last_price is not None else None
    except Exception:
        lp = None
    if lp is None or lp <= 0:
        return {"entry": None, "stop": None, "targets": [None, None, None], "gain_pct": None, "risk_reward": None}
    entry = float(lp)
    stop = float(lp) * 0.97
    t1 = float(lp) * 1.03
    t2 = float(lp) * 1.06
    t3 = float(lp) * 1.09
    try:
        gain_pct = float(round(((t1 - entry) / entry) * 100.0, 2))
    except Exception:
        gain_pct = None
    try:
        rr = float(round((t1 - entry) / max(1e-9, (entry - stop)), 2))
    except Exception:
        rr = None
    return {
        "entry": float(_round_px(entry)),
        "stop": float(_round_px(stop)),
        "targets": [float(_round_px(x)) for x in (t1, t2, t3)],
        "gain_pct": gain_pct,
        "risk_reward": rr,
    }


@app.get("/analyze/{symbol}")
async def analyze(
    symbol: str,
    budget: float = 1000,
    risk: str = "medium",
    timeframe: str = "swing",
    allow_llm: bool = True,
    tz: Optional[str] = Query(None),
    stream: bool = Query(False),
    _user=Depends(_get_current_user),
):
    sd = _symbol_sanitize(symbol, allow_extended=False)
    sym = str(sd.get("symbol") or "").strip().upper()
    if not bool(sd.get("ok")):
        return _empty_analyze_response(symbol=sym, status="partial")

    _ = budget
    _ = risk
    _ = timeframe

    candles: List[Dict[str, Any]] = []
    try:
        bars = await asyncio.to_thread(_alpaca_get_bars, sym, "1Day", 100)
        candles = bars.get("candles") if isinstance(bars, dict) else []
    except Exception:
        candles = []

    intraday: List[Dict[str, Any]] = []
    try:
        await asyncio.sleep(0.12)
        bars_i = await asyncio.to_thread(_alpaca_get_bars, sym, "5Min", 300)
        intraday = bars_i.get("candles") if isinstance(bars_i, dict) else []
    except Exception:
        intraday = []

    snap = None
    try:
        snap = await asyncio.to_thread(_alpaca_get_snapshot, sym)
    except Exception:
        snap = None

    snap_px = None
    try:
        if isinstance(snap, dict):
            lt0 = snap.get("latestTrade") if isinstance(snap.get("latestTrade"), dict) else {}
            snap_px = _safe_f(lt0.get("p")) if lt0.get("p") is not None else None
    except Exception:
        snap_px = None

    try:
        indicators = calculate_indicators(candles)
    except Exception:
        indicators = {}

    news0 = _news_and_sentiment(sym, allow_llm=bool(allow_llm))
    if not isinstance(news0, dict):
        news0 = {"headlines": [], "sentiment": "Neutral", "items": [], "source": "unavailable"}
    news_sentiment = {
        "direction": str(news0.get("direction") or "NEUTRAL").upper(),
        "summary": str(news0.get("summary") or "unavailable"),
        "headlines": news0.get("headlines") if isinstance(news0.get("headlines"), list) else [],
        "score": news0.get("score"),
        "confidence": news0.get("confidence"),
        "catalysts": news0.get("catalysts") if isinstance(news0.get("catalysts"), list) else [],
        "sentiment_source": str(news0.get("sentiment_source") or "").strip().lower() or None,
    }
    ns100 = _sentiment_score_0_100(news_sentiment)

    # --- Extended intelligence (fail-safe) ---
    try:
        social_sent = await asyncio.wait_for(get_social_sentiment(sym), timeout=6.5)
    except Exception:
        social_sent = _social_default(symbol=sym)

    try:
        earnings_ai = await asyncio.wait_for(get_earnings_ai(sym, allow_llm=bool(allow_llm)), timeout=14.0)
    except Exception:
        earnings_ai = _earnings_default(symbol=sym)

    # Market-derived (impact) will be computed after market_data is available.

    execution_score = _score_execution_0_100(indicators=indicators)
    execution_plan = _build_execution_plan(indicators=indicators, tz=tz)

    def _impact_score_from_market(*, sentiment_score: Any, md: Dict[str, Any]) -> int:
        try:
            s = float(sentiment_score)
        except Exception:
            s = 0.0
        s = max(-100.0, min(100.0, s))
        sent01 = (s + 100.0) / 200.0

        pct = 0.0
        try:
            pct = float(md.get("percent_change") or md.get("pct_change") or 0.0)
        except Exception:
            pct = 0.0
        pct = max(-20.0, min(20.0, pct))
        px01 = (pct + 20.0) / 40.0

        vwap_dev = 0.0
        try:
            lp = float(md.get("last_price") or 0.0)
            vwap = float(md.get("vwap") or 0.0)
            if lp > 0 and vwap > 0:
                vwap_dev = (lp - vwap) / vwap
        except Exception:
            vwap_dev = 0.0
        vwap_dev = max(-0.05, min(0.05, vwap_dev))
        vwap01 = (vwap_dev + 0.05) / 0.10

        align = 1.0 - abs(sent01 - px01)
        align2 = 1.0 - abs(sent01 - vwap01)
        outv = int(round(max(0.0, min(1.0, (0.65 * align) + (0.35 * align2))) * 100.0))
        return int(max(0, min(100, outv)))

    det_tp = _deterministic_trade_plan(symbol=sym, daily_bars=candles, intraday_bars=intraday, indicators=indicators)
    current_px = snap_px
    if current_px is None or float(current_px) <= 0:
        try:
            if candles:
                current_px = _safe_f(candles[-1].get("c"))
        except Exception:
            current_px = None
    if current_px is None or float(current_px) <= 0:
        try:
            if intraday:
                current_px = _safe_f(intraday[-1].get("c"))
        except Exception:
            current_px = None

    atr_for_model = _safe_f((indicators or {}).get("atr"))
    if atr_for_model is None:
        atr_for_model = _safe_f(det_tp.get("atr14"))

    vwap_for_model = _safe_f((indicators or {}).get("vwap"))
    if vwap_for_model is None:
        vwap_for_model = _safe_f(det_tp.get("vwap"))

    prior_high_for_model = _safe_f(det_tp.get("recent_high"))
    support_for_model = _safe_f(det_tp.get("recent_low"))

    vol_score_for_model = (indicators or {}).get("volatility_score")
    if vol_score_for_model is None:
        vol_score_for_model = (indicators or {}).get("volatility")

    trend_strength_for_model = (indicators or {}).get("trend_strength")
    if trend_strength_for_model is None:
        trend_strength_for_model = (indicators or {}).get("trend")

    trade_plan_modeled = generate_trade_plan(
        sym,
        {
            "current_price": current_px,
            "prior_high": prior_high_for_model,
            "support": support_for_model,
            "resistance": prior_high_for_model,
        },
        {
            "atr": atr_for_model,
            "vwap": vwap_for_model,
            "volatility_score": vol_score_for_model,
        },
    )

    impact_score = 50
    try:
        market_data = _market_data_from_snapshot_and_bars(symbol=sym, snapshot=snap, daily_bars=candles, intraday_bars=intraday)
    except Exception:
        market_data = {}

    # Analyst targets need price.
    try:
        last_px_for_analyst = _safe_f((market_data if isinstance(market_data, dict) else {}).get("last_price"))
    except Exception:
        last_px_for_analyst = None
    try:
        analyst_targets = await asyncio.wait_for(get_analyst_targets(sym, last_price=last_px_for_analyst), timeout=6.5)
    except Exception:
        analyst_targets = _analyst_default(symbol=sym)

    # Upgraded news-price impact weighting vs price/volume.
    try:
        news_price_impact = await asyncio.wait_for(
            calculate_news_price_impact(sym, news=(news0 if isinstance(news0, dict) else {}), market_data=(market_data if isinstance(market_data, dict) else {})),
            timeout=4.0,
        )
    except Exception:
        news_price_impact = _impact_default(symbol=sym)

    try:
        impact_score = _impact_score_from_market(sentiment_score=news0.get("score"), md=(market_data if isinstance(market_data, dict) else {}))
    except Exception:
        impact_score = 50
    try:
        impact_score = int(max(0, min(100, int(impact_score))))
    except Exception:
        impact_score = 50

    # Price-weighted impact score should never be undefined.
    try:
        impact_score = int(news_price_impact.get("impact_score")) if isinstance(news_price_impact, dict) and news_price_impact.get("impact_score") is not None else int(impact_score)
    except Exception:
        pass

    # --- New AI score weighting (institutional stack) ---
    _regime = "neutral"
    try:
        _regime = _get_market_regime()
    except Exception:
        pass
    technical_score = float(_score_composite_0_100(indicators=indicators, news_sentiment_0_100=ns100, regime=_regime) or 0.0)
    news_sentiment_score = float(_clamp_0_100(ns100) if callable(globals().get("_clamp_0_100")) else max(0.0, min(100.0, float(ns100 or 0.0))))
    # Use 50 (neutral) as default for missing signals — 0 unfairly drags down
    # strong setups where secondary data simply hasn't loaded yet.
    try:
        social_sentiment_score = float((social_sent or {}).get("hype_score") or 50.0)
        if social_sentiment_score == 0.0:
            social_sentiment_score = 50.0
    except Exception:
        social_sentiment_score = 50.0
    try:
        earnings_ai_score = float(_tone_to_score_0_100((earnings_ai or {}).get("tone")))
        if earnings_ai_score == 0.0:
            earnings_ai_score = 50.0
    except Exception:
        earnings_ai_score = 50.0
    try:
        analyst_target_score = float((analyst_targets or {}).get("score_0_100") or 50.0)
        if analyst_target_score == 0.0:
            analyst_target_score = 50.0
    except Exception:
        analyst_target_score = 50.0

    # Options activity is fetched later (Polygon). Use neutral until fetched.
    options_activity_score = 50.0

    ai_score = (
        (technical_score * 0.35)
        + (news_sentiment_score * 0.15)
        + (social_sentiment_score * 0.10)
        + (earnings_ai_score * 0.15)
        + (analyst_target_score * 0.15)
        + (options_activity_score * 0.10)
    )

    try:
        include_social = str(os.getenv("ANALYZE_INCLUDE_SOCIAL_SENTIMENT", "0") or "0").strip().lower() in ("1", "true", "yes", "on")
    except Exception:
        include_social = False

    last_px_md = _safe_f(market_data.get("last_price"))
    atr14_md = _safe_f(market_data.get("atr14"))
    vwap_md = _safe_f(market_data.get("vwap"))
    rvol_md = _safe_f(market_data.get("relative_volume"))

    resistance = _safe_f(det_tp.get("recent_high"))
    if resistance is None:
        try:
            resistance = _safe_f(trade_plan_modeled.get("entry"))
        except Exception:
            resistance = None
    if resistance is None and last_px_md is not None:
        resistance = float(last_px_md)

    open_md = _safe_f(market_data.get("open"))
    prev_close_md = _safe_f(market_data.get("prev_close"))

    # Detect a real catalyst gap within the intraday bars (last 10 trading days).
    # We group bars by calendar date, then find the most recent day where the
    # first bar of the day opened >8% above the previous day's last bar close.
    # This is an actual overnight gap, not just multi-month price appreciation.
    gap_open_for_fib: Optional[float] = None
    gap_prev_close_for_fib: Optional[float] = None
    try:
        _intra_bars = market_data.get("intraday_bars") or []
        if len(_intra_bars) >= 2:
            # Group bars by date (YYYY-MM-DD from timestamp)
            from collections import defaultdict
            _days: Dict[str, list] = defaultdict(list)
            for _b in _intra_bars:
                _ts = str(_b.get("t") or "")
                _day = _ts[:10]
                if _day:
                    _days[_day].append(_b)
            _sorted_days = sorted(_days.keys())
            # Scan most-recent days first; use the largest recent gap found
            for _i in range(len(_sorted_days) - 1, 0, -1):
                _prev_day_bars = _days[_sorted_days[_i - 1]]
                _curr_day_bars = _days[_sorted_days[_i]]
                _prev_close_bar = float((_prev_day_bars[-1] or {}).get("c") or 0)
                _curr_open_bar = float((_curr_day_bars[0] or {}).get("o") or 0)
                if _prev_close_bar > 0 and _curr_open_bar > 0:
                    _gap_pct = (_curr_open_bar - _prev_close_bar) / _prev_close_bar
                    if _gap_pct > 0.08:  # 8%+ overnight gap = real catalyst
                        gap_open_for_fib = _curr_open_bar
                        gap_prev_close_for_fib = _prev_close_bar
                        break  # use the most recent catalyst gap
    except Exception:
        pass

    trade_plan_spec = _trade_plan_from_spec(
        last_price=float(last_px_md or 0.0),
        atr14=float(atr14_md or 0.0),
        vwap=float(vwap_md or 0.0),
        resistance=float(resistance or 0.0),
        open_price=float(gap_open_for_fib or open_md or 0.0),
        prev_close=float(gap_prev_close_for_fib or prev_close_md or 0.0),
    )

    # Override targets/stop with real Fibonacci levels from the 52-week range.
    # These are the actual price levels the market cares about — retrace levels
    # from the 52w low/high, plus extensions beyond the 52w high for breakout stocks.
    try:
        _w52 = await asyncio.to_thread(_fetch_yf_52week, sym)
        if _w52 and last_px_md:
            _entry_anchor = float(trade_plan_spec.get("entry") or last_px_md or 0)
            _fib = _fib_targets_and_stop(
                current_price=float(last_px_md),
                w52_low=_w52["low"],
                w52_high=_w52["high"],
                entry_price=_entry_anchor,
            )
            if _fib.get("targets") and len(_fib["targets"]) >= 3:
                trade_plan_spec = dict(trade_plan_spec)
                trade_plan_spec["targets"] = _fib["targets"]
                trade_plan_spec["stop"] = _fib["stop"]
    except Exception:
        pass

    execution_factors = _execution_factors_from_market_data(
        last_price=float(last_px_md or 0.0),
        vwap=float(vwap_md or 0.0),
        resistance=float(resistance or 0.0),
        relative_volume=float(rvol_md or 0.0),
        atr14=float(atr14_md or 0.0),
    )

    if current_px is None or float(current_px) <= 0:
        degraded = _empty_analyze_response(symbol=sym, status="partial")
        degraded["technicals"]["technical_analysis"] = indicators or {}
        if isinstance(news0, dict):
            degraded["news"] = news0
        try:
            degraded["market_data"] = {"last_price": (float(_round_px(current_px)) if current_px is not None else None), "source": "alpaca"}
        except Exception:
            degraded["market_data"] = {"last_price": None, "source": "alpaca"}
        try:
            degraded["trade_plan"] = _deterministic_trade_plan_from_price(last_price=_safe_f(degraded.get("market_data", {}).get("last_price")))
        except Exception:
            pass
        try:
            degraded["execution_plan"] = _deterministic_execution_plan(entry=_safe_f(degraded.get("trade_plan", {}).get("entry")))
        except Exception:
            pass
        return degraded

    # No snapshot: keep response complete via deterministic trade-plan defaults.
    if not bool(market_data.get("snapshot_available")):
        degraded = _empty_analyze_response(symbol=sym, status="partial")
        degraded["technicals"]["technical_analysis"] = indicators or {}
        if isinstance(news0, dict):
            degraded["news"] = news0
        degraded["market_data"] = market_data
        degraded["reason"] = "snapshot_unavailable"
        try:
            lp0 = _safe_f((market_data or {}).get("last_price"))
            if lp0 is None or lp0 <= 0:
                lp0 = _safe_f(current_px)
            degraded["trade_plan"] = _deterministic_trade_plan_from_price(last_price=lp0)
        except Exception:
            pass
        try:
            degraded["execution_plan"] = _deterministic_execution_plan(entry=_safe_f(degraded.get("trade_plan", {}).get("entry")))
        except Exception:
            pass
        return degraded


    best_pick_match = False
    try:
        cached_bp = _BEST_PICK_FALLBACK_CACHE.get("resp") if isinstance(_BEST_PICK_FALLBACK_CACHE, dict) else None
        if isinstance(cached_bp, dict):
            best_pick_match = str(cached_bp.get("symbol") or "").strip().upper() == sym
    except Exception:
        best_pick_match = False

    entry_px = _safe_f(trade_plan_spec.get("entry"))
    atr_px = _safe_f(market_data.get("atr14"))
    if atr_px is None:
        atr_px = _safe_f(det_tp.get("atr14"))

    # Fallback: if trade_plan_spec failed to compute, derive from market data directly
    if (entry_px is None or entry_px <= 0) and last_px_md and float(last_px_md) > 0:
        _lp = float(last_px_md)
        _atr = float(atr_px or _lp * 0.05)
        entry_px = round(_lp * 1.005, 2)
        trade_plan_spec = {
            "entry": entry_px,
            "stop": round(entry_px - _atr, 2),
            "targets": [round(entry_px + _atr, 2), round(entry_px + _atr * 2, 2), round(entry_px + _atr * 3, 2)],
            "gain_pct": round((_atr * 3 / entry_px) * 100, 2),
            "risk_reward": 2.0,
        }

    execution_plan_modeled = generate_execution_plan(sym, volatility=vol_score_for_model, trend_strength=trend_strength_for_model)
    try:
        if entry_px is not None and float(entry_px) > 0:
            buy_low = float(entry_px) * 0.98
            buy_high = float(entry_px) * 1.02
            execution_plan_modeled["buy_zone"] = f"${buy_low:.2f} – ${buy_high:.2f}"
    except Exception:
        pass

    try:
        if last_px_md is not None and vwap_md is not None and float(last_px_md) < float(vwap_md):
            execution_plan_modeled["entry_method"] = "VWAP reclaim"
        else:
            execution_plan_modeled["entry_method"] = "Breakout"
        execution_plan_modeled["window"] = "9:35 – 10:45 AM"
    except Exception:
        pass

    mom_for_window = (indicators or {}).get("momentum")
    system_expectation = _system_expectation_from_momentum(mom_for_window)

    reasoning = _trade_reasoning(
        symbol=sym,
        technicals=indicators,
        trade_plan=trade_plan_modeled,
        news=news0,
        allow_llm=bool(allow_llm),
    )
    if not isinstance(reasoning, dict):
        reasoning = {"why": [], "confirms": [], "breaks": []}

    ai_score_0_100 = float(round(float(ai_score or 0.0), 1))
    execution_score_0_100 = float(round(float(_score_execution_0_100(indicators=indicators, execution_factors=execution_factors) or 0.0), 1))
    technicals = {
        "symbol": sym,
        "ai_score": ai_score_0_100,
        "ai_score_10": float(round(ai_score_0_100 / 10.0, 1)),
        "execution_score": execution_score_0_100,
        "execution_score_10": float(round(execution_score_0_100 / 10.0, 1)),
        "technical_analysis": indicators,
        "execution_factors": execution_factors,
    }

    stop_px = _safe_f(trade_plan_spec.get("stop"))
    targets = trade_plan_spec.get("targets") if isinstance(trade_plan_spec.get("targets"), list) else []
    t2 = _safe_f(targets[1] if len(targets) > 1 else None)
    gain_pct = _safe_f(trade_plan_spec.get("gain_pct"), 0.0)
    rr_ratio = _safe_f(trade_plan_spec.get("risk_reward"), 0.0)
    confirmations = _confirmations_checklist(news_sentiment=str(news0.get("sentiment") or ""))

    market_cap = None
    unusual_options_score = None
    options_call_put_ratio = None
    options_top_contracts: List[Dict[str, Any]] = []
    try:
        # Polygon calls must never block the response if they fail.
        async def _poly_fetch():
            mc = None
            uo = None
            if callable(_polygon_get_market_cap):
                try:
                    mc = await asyncio.to_thread(_polygon_get_market_cap, sym)
                except Exception:
                    mc = None
            if callable(_polygon_get_unusual_options):
                try:
                    uo = await asyncio.to_thread(_polygon_get_unusual_options, sym)
                except Exception:
                    uo = None
            return mc, uo

        mc0, uo0 = await asyncio.wait_for(_poly_fetch(), timeout=9.5)
        market_cap = mc0
        if isinstance(uo0, dict):
            unusual_options_score = uo0.get("unusual_options_score")
            options_call_put_ratio = uo0.get("call_put_ratio")
            tc = uo0.get("top_contracts")
            if isinstance(tc, list):
                options_top_contracts = [x for x in tc if isinstance(x, dict)][:5]
    except Exception:
        market_cap = None
        unusual_options_score = None
        options_call_put_ratio = None
        options_top_contracts = []

    # Now that options score exists, finalize ai_score.
    try:
        options_activity_score = float(unusual_options_score) if unusual_options_score is not None else 50.0
    except Exception:
        options_activity_score = 50.0
    try:
        options_activity_score = max(0.0, min(100.0, float(options_activity_score)))
    except Exception:
        options_activity_score = 50.0

    ai_score = (
        (technical_score * 0.35)
        + (news_sentiment_score * 0.15)
        + (social_sentiment_score * 0.10)
        + (earnings_ai_score * 0.15)
        + (analyst_target_score * 0.15)
        + (options_activity_score * 0.10)
    )

    out: Dict[str, Any] = {
        "best_pick": {
            "symbol": sym,
            "ai_score_0_100": ai_score_0_100,
            "execution_score_0_100": execution_score_0_100,
            "ai_score_0_10": _score_0_10_from_0_100(ai_score_0_100),
            "execution_score_0_10": _score_0_10_from_0_100(execution_score_0_100),
            "confidence_0_100": _confidence_pct_0_100((0.6 * ai_score_0_100) + (0.4 * execution_score_0_100)),
            # required simplified keys
            "score": float(normalize_score(ai_score_0_100)),
            "confidence": float(normalize_score(_confidence_pct_0_100((0.6 * ai_score_0_100) + (0.4 * execution_score_0_100)) or 0.0)),
            "best_pick_match": bool(best_pick_match),
        },
        "trade_plan": {
            "entry": (float(_round_px(entry_px)) if entry_px is not None else None),
            "stop": (float(_round_px(stop_px)) if stop_px is not None else None),
            "targets": [float(_round_px(x)) for x in (targets[:3] if isinstance(targets, list) else []) if _safe_f(x) is not None][:3],
            "gain_pct": gain_pct,
            "risk_reward": rr_ratio,
        },
        "execution_plan": {
            "date": str(execution_plan_modeled.get("date") or ""),
            "window": str(execution_plan_modeled.get("window") or ""),
            "entry_method": str(execution_plan_modeled.get("entry_method") or ""),
            "buy_zone": str(execution_plan_modeled.get("buy_zone") or ""),
        },
        "technicals": dict(technicals or {}, **{"system_expectation": system_expectation}),
        "news": news0,
        "news_sentiment": {
            "direction": str(news_sentiment.get("direction") or "NEUTRAL").upper(),
            "score": int(float(news0.get("score") or 0)) if news0.get("score") is not None else 0,
            "confidence": int(float(news0.get("confidence") or 0)) if news0.get("confidence") is not None else (15 if not (news0.get("items") or []) else 35),
            "summary": str(news0.get("summary") or "Low news volume. Sentiment confidence reduced.")[:420],
            "catalysts": [str(x).strip() for x in (news0.get("catalysts") or []) if str(x).strip()][:6],
            "sentiment_source": str(news0.get("sentiment_source") or "keyword").strip().lower(),
            # Frontend expects strings here; keep rich objects in headline_items.
            "headlines": [
                str(it.get("title") or it.get("headline") or "").strip()[:240]
                for it in (news0.get("items") or [])
                if isinstance(it, dict) and str(it.get("title") or it.get("headline") or "").strip()
            ][:20],
            "headline_items": [
                {
                    "title": str(it.get("title") or it.get("headline") or "").strip()[:240],
                    "source": str(it.get("source") or "").strip()[:120],
                    "url": str(it.get("url") or "").strip()[:500],
                    "published_at": str(it.get("published_at") or "").strip()[:40],
                }
                for it in (news0.get("items") or [])
                if isinstance(it, dict) and str(it.get("title") or it.get("headline") or "").strip()
            ][:20],
        },
        "social_sentiment": (
            {
                "reddit_score": int((social_sent or {}).get("reddit_score") or 0),
                "twitter_score": int((social_sent or {}).get("twitter_score") or 0),
                "hype_score": int((social_sent or {}).get("hype_score") or 0),
                "direction": str((social_sent or {}).get("direction") or "NEUTRAL").strip().upper(),
                "mentions": (social_sent or {}).get("mentions") if isinstance((social_sent or {}).get("mentions"), dict) else {"reddit": 0, "twitter": 0},
                "samples": (social_sent or {}).get("samples") if isinstance((social_sent or {}).get("samples"), dict) else {"reddit": [], "twitter": []},
                "summary": str((social_sent or {}).get("summary") or "").strip()[:420],
                "status": str((social_sent or {}).get("status") or "unavailable"),
            }
            if bool(include_social)
            else {"status": str((social_sent or {}).get("status") or "unavailable")}
        ),
        "earnings_ai": {
            "tone": str((earnings_ai or {}).get("tone") or "unavailable"),
            "guidance_outlook": str((earnings_ai or {}).get("guidance_outlook") or "unavailable"),
            "ai_confidence": int((earnings_ai or {}).get("ai_confidence") or 0),
            "key_themes": (earnings_ai or {}).get("key_themes") if isinstance((earnings_ai or {}).get("key_themes"), list) else [],
            "status": str((earnings_ai or {}).get("status") or "unavailable"),
            "source": str((earnings_ai or {}).get("source") or "unavailable"),
        },
        "analyst_targets": {
            "target_avg": (analyst_targets or {}).get("target_avg"),
            "target_high": (analyst_targets or {}).get("target_high"),
            "target_low": (analyst_targets or {}).get("target_low"),
            "implied_upside_pct": (analyst_targets or {}).get("implied_upside_pct"),
            "rating_bias": str((analyst_targets or {}).get("rating_bias") or "NEUTRAL").strip().upper(),
            "score_0_100": int((analyst_targets or {}).get("score_0_100") or 0),
            "status": str((analyst_targets or {}).get("status") or "unavailable"),
            "source": str((analyst_targets or {}).get("source") or "unavailable"),
        },
        "news_price_impact": {
            "impact_score": int(impact_score),
            "price_reaction_pct": float((news_price_impact or {}).get("price_reaction_pct") or 0.0),
            "volume_spike": float((news_price_impact or {}).get("volume_spike") or 1.0),
            "status": str((news_price_impact or {}).get("status") or "unavailable"),
            "source": str((news_price_impact or {}).get("source") or "derived"),
        },
        "signal_status": {
            "social_sentiment_status": str((social_sent or {}).get("status") or "unavailable"),
            "earnings_ai_status": str((earnings_ai or {}).get("status") or "unavailable"),
            "analyst_targets_status": str((analyst_targets or {}).get("status") or "unavailable"),
            "impact_score_status": str((news_price_impact or {}).get("status") or "unavailable"),
        },
        "reasoning": {
            "why": reasoning.get("why") if isinstance(reasoning.get("why"), list) else [],
            "confirms": reasoning.get("confirms") if isinstance(reasoning.get("confirms"), list) else [],
            "breaks": reasoning.get("breaks") if isinstance(reasoning.get("breaks"), list) else [],
        },
        "market_data": market_data,
        "market_cap": market_cap,
        "unusual_options_score": unusual_options_score,
        "options_call_put_ratio": options_call_put_ratio,
        "options_top_contracts": options_top_contracts,
        "status": "complete" if not bool(market_data.get("market_data_degraded")) else "market_data_degraded",
    }

    # Trade plan sanity checks (never invent; return null plan if invalid)
    errors: List[str] = []
    try:
        # Prefer last_px_md (today's actual price) over current_px which may be yesterday's close
        # when snap_px is None (e.g., Polygon cache format). A stock that gapped up 70% overnight
        # would otherwise falsely trigger the entry_far_from_last check.
        _sanity_lp = last_px_md if (last_px_md is not None and float(last_px_md) > 0) else current_px
        lp = float(_sanity_lp)
    except Exception:
        lp = 0.0
    try:
        entry_v = out.get("trade_plan", {}).get("entry") if isinstance(out.get("trade_plan"), dict) else None
        stop_v = out.get("trade_plan", {}).get("stop") if isinstance(out.get("trade_plan"), dict) else None
        targets_v = out.get("trade_plan", {}).get("targets") if isinstance(out.get("trade_plan"), dict) else []
        entry_f = float(entry_v) if entry_v is not None else None
        stop_f = float(stop_v) if stop_v is not None else None
        t_list = [float(x) for x in targets_v] if isinstance(targets_v, list) else []
    except Exception:
        entry_f, stop_f, t_list = None, None, []

    # Entry anchored to last price
    max_dev = 0.25
    try:
        if str(timeframe or "").strip().lower() in ("intraday", "day", "scalp"):
            max_dev = 0.10
    except Exception:
        max_dev = 0.25
    try:
        if entry_f is None or float(entry_f) <= 0.0:
            errors.append("trade_plan_invalid:missing_entry")
        if stop_f is None or float(stop_f) <= 0.0:
            errors.append("trade_plan_invalid:missing_stop")
        if not t_list or any((t is None) or (float(t) <= 0.0) for t in t_list):
            errors.append("trade_plan_invalid:missing_targets")
    except Exception:
        pass

    try:
        if lp > 0 and entry_f is not None and entry_f > 0:
            if abs(entry_f - lp) / lp > float(max_dev):
                errors.append("trade_plan_invalid:entry_far_from_last")
    except Exception:
        pass

    try:
        if entry_f is not None and stop_f is not None and not (stop_f < entry_f):
            errors.append("trade_plan_invalid:stop_not_below_entry")
    except Exception:
        pass

    try:
        if entry_f is not None and t_list:
            if any((t is None) or (float(t) <= float(entry_f)) for t in t_list):
                errors.append("trade_plan_invalid:targets_not_above_entry")
    except Exception:
        pass

    if errors:
        out["trade_plan"] = {"entry": None, "stop": None, "targets": [None, None, None], "gain_pct": None, "risk_reward": None}
        out["errors"] = errors

    # Preserve nulls; do not coerce to 0.0 placeholders.
    if not isinstance(out.get("best_pick"), dict):
        out["best_pick"] = {"symbol": sym, "ai_score_0_100": None, "execution_score_0_100": None, "ai_score_0_10": None, "execution_score_0_10": None, "confidence_0_100": None}
    if not isinstance(out.get("execution_plan"), dict):
        out["execution_plan"] = {}
    if not isinstance(out.get("technicals"), dict):
        out["technicals"] = {"symbol": sym, "technical_analysis": {}, "ai_score": None, "execution_score": None, "system_expectation": ""}
    if not isinstance(out.get("news"), dict):
        out["news"] = {"headlines": [], "sentiment": "Neutral", "items": [], "source": "unavailable"}
    if not isinstance(out.get("reasoning"), dict):
        out["reasoning"] = {"why": [], "confirms": [], "breaks": []}
    if not isinstance(out.get("market_data"), dict):
        out["market_data"] = {"last_price": (float(_round_px(current_px)) if current_px is not None else None), "source": "alpaca"}

    if bool(stream):
        async def _stream():
            yield (json.dumps(out) + "\n").encode("utf-8")
            return
        return StreamingResponse(_stream(), media_type="application/json")

    return out


@app.on_event("startup")
async def _warm_market_cache_task() -> None:
    try:
        if not (os.getenv("OPENAI_API_KEY") or "").strip():
            try:
                log.warning("OPENAI_API_KEY missing; LLM features will run in fallback mode")
            except Exception:
                pass
    except Exception:
        pass
    try:
        _validate_market_env()
    except Exception:
        pass

    # Ensure warmers run once on startup per process and at most every 15 minutes.
    try:
        if globals().get("_MARKET_WARMER_STARTED") is True:
            return
        globals()["_MARKET_WARMER_STARTED"] = True
    except Exception:
        pass

    async def _loop() -> None:
        while True:
            try:
                try:
                    last_ts = float(globals().get("_MARKET_WARMER_LAST_RUN_TS") or 0.0)
                except Exception:
                    last_ts = 0.0
                if last_ts > 0.0 and (time.time() - last_ts) < 900.0:
                    await asyncio.sleep(15)
                    continue
                try:
                    globals()["_MARKET_WARMER_LAST_RUN_TS"] = float(time.time())
                except Exception:
                    pass
                syms = []
                try:
                    syms = [s for s in _SEED_UNIVERSE if s and "." not in s][:200]
                except Exception:
                    syms = []
                if syms:
                    chunks = [syms[i : i + 50] for i in range(0, len(syms), 50)]
                    for ch in chunks:
                        try:
                            await asyncio.to_thread(_alpaca_get_bars_batch, ch, "1Day", 100)
                            try:
                                log.info(f"Market warmer batch size={len(ch)}")
                            except Exception:
                                pass
                        except Exception:
                            pass
            except Exception:
                pass
            await asyncio.sleep(900)

    try:
        asyncio.create_task(_loop())
    except Exception:
        return


@app.get("/technical/{symbol}")
def technical(symbol: str, _user=Depends(_get_current_user)):
    sym = (symbol or "").strip().upper()
    eng = aurexis_engine(sym, allow_llm=False)
    if not isinstance(eng, dict):
        raise HTTPException(status_code=500, detail="ENGINE_FAILED")

    badge = str(eng.get("badge") or "POST_MARKET_CLOSE_DATA")
    t = eng.get("technical_analysis") if isinstance(eng.get("technical_analysis"), dict) else {}

    notes: List[str] = []
    try:
        n = t.get("notes")
        if isinstance(n, list):
            notes = [str(x)[:140] for x in n if isinstance(x, str) and x.strip()]
    except Exception:
        notes = []

    if not notes:
        notes = [
            "Liquidity supports execution",
            "Momentum is developing; confirmation required",
            "Risk is contained above key support",
        ]
    notes = notes[:3]
    while len(notes) < 3:
        notes.append("Signals are developing; recheck next session.")

    def _safe_int_0_100(x: Any, default: int = 50) -> int:
        try:
            v = int(float(x))
        except Exception:
            v = int(default)
        if v < 0:
            v = 0
        if v > 100:
            v = 100
        return v

    momentum = _safe_int_0_100(t.get("momentum"), 50)
    trend = _safe_int_0_100(t.get("trend"), 50)
    volatility = _safe_int_0_100(t.get("volatility"), 50)
    liquidity = _safe_int_0_100(t.get("liquidity"), 50)
    risk = _safe_int_0_100(t.get("risk"), 50)

    # If signals look like a complete failure (all zeros), use neutral values and explain.
    if momentum == 0 and trend == 0 and volatility == 0 and liquidity == 0:
        momentum, trend, volatility, liquidity, risk = 50, 50, 50, 50, 50
        notes = [
            "Indicators unavailable; using neutral technical values.",
            "Last-known bars could not be retrieved reliably; treat as mixed conditions.",
            "Recheck at next market open for confirmation.",
        ]

    return {
        "badge": badge,
        "momentum": momentum,
        "trend": trend,
        "volatility": volatility,
        "liquidity": liquidity,
        "risk": risk,
        "notes": notes,
    }


@app.get("/debug/candles/{symbol}", include_in_schema=False)
def debug_candles(symbol: str, _: None = Depends(_require_debug)):
    sym = (symbol or "").strip().upper()
    df = None
    try:
        df = get_candles(sym, timeframe="1Day", limit=100)
    except Exception as e:
        return {"symbol": sym, "error": str(e)[:200], "count": 0}
    try:
        n = len(df) if df is not None else 0
    except Exception:
        n = 0
    # Return a small sample to keep payload light.
    sample = []
    try:
        to_dict = getattr(df, "to_dict", None)
        if callable(to_dict):
            sample = (df.tail(5)).to_dict(orient="records")  # type: ignore
        elif isinstance(df, list):
            sample = df[-5:]
    except Exception:
        sample = []
    return {"symbol": sym, "count": int(n), "sample": sample}


@app.get("/debug/indicators/{symbol}", include_in_schema=False)
def debug_indicators(symbol: str, _: None = Depends(_require_debug)):
    sym = (symbol or "").strip().upper()
    df = None
    try:
        df = get_candles(sym, timeframe="1Day", limit=100)
    except Exception as e:
        return {"symbol": sym, "error": str(e)[:200], "technical_analysis": {"momentum": 50, "trend": 50, "volatility": 50, "liquidity": 50, "risk": 50}}
    try:
        n = len(df) if df is not None else 0
    except Exception:
        n = 0
    if df is None or n < 20:
        ta = {"momentum": 50, "trend": 50, "volatility": 50, "liquidity": 50, "risk": 50}
        return {"symbol": sym, "candles": int(n), "technical_analysis": ta}
    try:
        ta = {
            "momentum": int(calculate_momentum(df)) if callable(calculate_momentum) else 50,
            "trend": int(calculate_trend(df)) if callable(calculate_trend) else 50,
            "volatility": int(calculate_volatility(df)) if callable(calculate_volatility) else 50,
            "liquidity": int(calculate_liquidity(df)) if callable(calculate_liquidity) else 50,
            "risk": int(calculate_risk(df)) if callable(calculate_risk) else 50,
        }
    except Exception as e:
        return {"symbol": sym, "candles": int(n), "error": str(e)[:200], "technical_analysis": {"momentum": 50, "trend": 50, "volatility": 50, "liquidity": 50, "risk": 50}}
    return {"symbol": sym, "candles": int(n), "technical_analysis": ta}


@app.get("/news/{symbol}")
def news(symbol: str, allow_llm: bool = True, _user=Depends(_get_current_user)):
    sym = (symbol or "").strip().upper()
    eng = aurexis_engine(sym, allow_llm=allow_llm)
    if not isinstance(eng, dict):
        raise HTTPException(status_code=500, detail="ENGINE_FAILED")

    badge = str(eng.get("badge") or "POST_MARKET_CLOSE_DATA")
    sentiment = eng.get("news_sentiment") if isinstance(eng.get("news_sentiment"), dict) else {}
    return _news_contract_from_sentiment(badge, sentiment)


    # ----------------------------
    # Execution time window (UTC) — must never throw
    # ----------------------------
    time_window = {
        "session": "CLOSED",
        "startUTC": None,
        "endUTC": None,
        "exchangeTZ": "America/New_York",
        "reason": "Clock unavailable",
    }
    try:
        c, _ = _get_clock_cached()
        if c is not None:
            is_open = bool(getattr(c, "is_open", False))
            ts = getattr(c, "timestamp", None)
            next_open = getattr(c, "next_open", None)
            next_close = getattr(c, "next_close", None)

            ts_utc = ts.astimezone(timezone.utc) if getattr(ts, "astimezone", None) else None
            # Use NYSE windows in ET converted to UTC for correct DST handling.
            et = None
            if ts_utc is not None:
                et = ts_utc.astimezone(ZoneInfo("America/New_York"))
            else:
                et = datetime.now(ZoneInfo("America/New_York"))

            open_et = datetime(et.year, et.month, et.day, 9, 30, tzinfo=ZoneInfo("America/New_York"))
            close_et = datetime(et.year, et.month, et.day, 16, 0, tzinfo=ZoneInfo("America/New_York"))
            pre_et = datetime(et.year, et.month, et.day, 4, 0, tzinfo=ZoneInfo("America/New_York"))
            post_et = datetime(et.year, et.month, et.day, 20, 0, tzinfo=ZoneInfo("America/New_York"))

            open_utc = open_et.astimezone(timezone.utc)
            close_utc = close_et.astimezone(timezone.utc)
            pre_utc = pre_et.astimezone(timezone.utc)
            post_utc = post_et.astimezone(timezone.utc)

            if is_open:
                # POWER_HOUR if within last 60 minutes to close (prefer Alpaca next_close if present)
                end_utc = close_utc
                if getattr(next_close, "astimezone", None):
                    end_utc = next_close.astimezone(timezone.utc)
                start_utc = open_utc
                session = "OPEN"
                reason = "Market open"
                if ts_utc is not None and end_utc is not None:
                    try:
                        mins_to_close = int(max(0, (end_utc - ts_utc).total_seconds() // 60))
                    except Exception:
                        mins_to_close = 999
                    if mins_to_close <= 60:
                        session = "POWER_HOUR"
                        reason = "Final hour before close"
                        start_utc = end_utc - timedelta(minutes=60)

                time_window = {
                    "session": session,
                    "startUTC": start_utc.isoformat() if start_utc else None,
                    "endUTC": end_utc.isoformat() if end_utc else None,
                    "exchangeTZ": "America/New_York",
                    "reason": reason,
                }
            else:
                # Pre-market if before open on the same trading day; else closed.
                # Prefer next_open from Alpaca when present.
                no = next_open.astimezone(timezone.utc) if getattr(next_open, "astimezone", None) else None
                if ts_utc is not None and open_utc is not None and ts_utc < open_utc and ts_utc >= pre_utc:
                    time_window = {
                        "session": "PRE_MARKET",
                        "startUTC": pre_utc.isoformat(),
                        "endUTC": open_utc.isoformat(),
                        "exchangeTZ": "America/New_York",
                        "reason": "Pre-market window",
                    }
                elif ts_utc is not None and ts_utc >= close_utc and ts_utc <= post_utc:
                    time_window = {
                        "session": "CLOSED",
                        "startUTC": close_utc.isoformat(),
                        "endUTC": post_utc.isoformat(),
                        "exchangeTZ": "America/New_York",
                        "reason": "Post-close window",
                    }
                else:
                    time_window = {
                        "session": "CLOSED",
                        "startUTC": (no.isoformat() if no else None),
                        "endUTC": (no.isoformat() if no else None),
                        "exchangeTZ": "America/New_York",
                        "reason": "Market closed",
                    }
    except Exception:
        pass

    if time_window.get("startUTC") is None or time_window.get("endUTC") is None:
        try:
            et = datetime.now(ZoneInfo("America/New_York"))
            is_weekday = et.weekday() < 5
            mins = et.hour * 60 + et.minute
            open_mins = 9 * 60 + 30
            close_mins = 16 * 60
            if bool(is_weekday and mins >= open_mins and mins < close_mins):
                time_window = {
                    "session": "OPEN",
                    "startUTC": None,
                    "endUTC": None,
                    "exchangeTZ": "America/New_York",
                    "reason": "NYSE hours fallback",
                }
            else:
                time_window = {
                    "session": "CLOSED",
                    "startUTC": None,
                    "endUTC": None,
                    "exchangeTZ": "America/New_York",
                    "reason": "NYSE hours fallback",
                }
        except Exception:
            pass

    if time_window.get("startUTC") is None or time_window.get("endUTC") is None:
        try:
            now_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        except Exception:
            now_utc = now_iso()
        time_window["startUTC"] = time_window.get("startUTC") or now_utc
        time_window["endUTC"] = time_window.get("endUTC") or now_utc
        time_window["exchangeTZ"] = time_window.get("exchangeTZ") or "America/New_York"

    try:
        snap = get_snapshot(sym)
    except Exception as e:
        bars = bars_payload
        series = get_daily_line_series(sym, limit=180) or []
        if not series:
            series = _bars_to_close_line_series(bars, limit=260) or []
        factors = {
            "momentum": 0,
            "liquidity": 0,
            "volatility": 0,
            "trend": 0,
            "risk": 0,
        }
        return {
            "symbol": sym,
            "price": None,
            "change_pct": None,
            "score": 0,
            "confidence": None,
            "recommendation": "AVOID",
            "market_regime": "UNKNOWN",
            "timeWindow": time_window,
            "factors": factors,
            "entry": None,
            "stop": None,
            "target": None,
            "badges": [],
            "risk_badges": ["NO_PRICE_DATA"],
            "catalyst": "Snapshot unavailable",
            "buy_zone": None,
            "buy_zone_pct": None,
            "profit_targets": None,
            "expected_move_pct": None,
            "r_multiple": None,
            "outcome_state": "ACTIVE",
            "session_context": "UNKNOWN",
            "why_it_works": [],
            "what_breaks_it": ["Snapshot unavailable"],
            "why_not_now": "Snapshot unavailable",
            "time_horizon": "1–3 days",
            "ts": now_iso(),
            "chart": {
                "timeframe": "1Day",
                "series": series,
            },
            "score_breakdown": None,
            "bullish_factors": [],
            "bearish_factors": ["Snapshot unavailable"],
            "price_context": {
                "current": None,
                "entry": None,
                "stop": None,
                "target": None,
            },
            "bars": bars,
            "error": str(e),
        }

    pa_bar = snap.get("dailyBar") or {}
    prev = snap.get("prevDailyBar") or {}

    # ----------------------------
    # Hard safety: no price data
    # ----------------------------
    if not pa_bar or not prev:
        bars = bars_payload
        series = get_daily_line_series(sym, limit=180) or []
        if not series:
            series = _bars_to_close_line_series(bars, limit=260) or []
        factors = {
            "momentum": 0,
            "liquidity": 0,
            "volatility": 0,
            "trend": 0,
            "risk": 0,
        }
        return {
            "symbol": sym,
            "price": None,
            "change_pct": None,
            "score": 0,
            "confidence": None,
            "recommendation": "AVOID",
            "market_regime": "UNKNOWN",
            "timeWindow": time_window,
            "factors": factors,
            "entry": None,
            "stop": None,
            "target": None,
            "badges": [],
            "risk_badges": ["NO_PRICE_DATA"],
            "catalyst": "No usable market data",
            "buy_zone": None,
            "buy_zone_pct": None,
            "profit_targets": None,
            "expected_move_pct": None,
            "r_multiple": None,
            "outcome_state": "ACTIVE",
            "session_context": "POST_CLOSE",
            "why_it_works": [],
            "what_breaks_it": ["Missing price data"],
            "why_not_now": "No usable market data",
            "time_horizon": "1–3 days",
            "ts": now_iso(),
            "chart": {
                "timeframe": "1Day",
                "series": series if series else _bars_to_close_line_series(bars, limit=260),
            },
            "score_breakdown": None,
            "bullish_factors": [],
            "bearish_factors": ["Missing price data"],
            "price_context": {
                "current": None,
                "entry": None,
                "stop": None,
                "target": None,
            },
            "bars": bars,
        }

    # ----------------------------
    # Price + volume
    # ----------------------------
    try:
        price = float(pa_bar.get("c"))
        prev_close = float(prev.get("c"))
        if not (math.isfinite(price) and math.isfinite(prev_close) and prev_close != 0.0):
            raise ValueError("Invalid price data")
        change = ((price - prev_close) / prev_close) * 100
        volume = pa_bar.get("v", 0)
        try:
            volume = float(volume) if volume is not None else 0.0
        except Exception:
            volume = 0.0
    except Exception:
        bars = bars_payload
        factors = {
            "momentum": 0,
            "liquidity": 0,
            "volatility": 0,
            "trend": 0,
            "risk": 0,
        }
        series = get_daily_line_series(sym, limit=180) or []
        if not series:
            series = _bars_to_close_line_series(bars, limit=260) or []
        return {
            "symbol": sym,
            "price": None,
            "change_pct": None,
            "score": 0,
            "confidence": None,
            "recommendation": "AVOID",
            "market_regime": "UNKNOWN",
            "timeWindow": time_window,
            "factors": factors,
            "entry": None,
            "stop": None,
            "target": None,
            "badges": [],
            "risk_badges": ["NO_PRICE_DATA"],
            "catalyst": "No usable market data",
            "buy_zone": None,
            "buy_zone_pct": None,
            "profit_targets": None,
            "expected_move_pct": None,
            "r_multiple": None,
            "outcome_state": "ACTIVE",
            "session_context": "POST_CLOSE",
            "why_it_works": [],
            "what_breaks_it": ["Missing price data"],
            "why_not_now": "No usable market data",
            "time_horizon": "1–3 days",
            "ts": now_iso(),
            "chart": {
                "timeframe": "1Day",
                "series": series,
            },
            "score_breakdown": None,
            "bullish_factors": [],
            "bearish_factors": ["Missing price data"],
            "price_context": {
                "current": None,
                "entry": None,
                "stop": None,
                "target": None,
            },
            "bars": bars,
        }

    # recent bars for charting (canonical; always list)
    bars = bars_payload

    # ----------------------------
    # Market regime
    # ----------------------------
    regime = market_regime()
    minutes_from_open = regime.get("minutes_from_open")
    try:
        minutes_from_open = int(minutes_from_open) if minutes_from_open is not None else 999
    except Exception:
        minutes_from_open = 999
    early_session = minutes_from_open <= 45

    # ----------------------------
    # News + LLM (optional; must never break analyze)
    # ----------------------------
    headlines = []
    llm: Dict[str, Any] = {}
    try:
        headlines = get_recent_news(sym)
    except Exception:
        headlines = []
    if allow_llm:
        try:
            llm = llm_safe_analyze(sym, tuple(headlines)) or {}
        except Exception:
            llm = {}

    llm_confirms = (
        not llm.get("uncertain", True)
        and llm.get("sentiment") in ("bullish", "constructive")
    )

    # ----------------------------
    # Score breakdown + factors (derived from existing signals)
    # ----------------------------
    day_high = pa_bar.get("h")
    day_low = pa_bar.get("l")
    day_range_pct = None
    try:
        if day_high is not None and day_low is not None and float(price) > 0:
            day_range_pct = abs(float(day_high) - float(day_low)) / float(price) * 100.0
    except Exception:
        day_range_pct = None

    momentum_score = _clamp100(min(100.0, abs(float(change)) * 20.0) if change is not None else None)
    liquidity_score = _clamp100(min(100.0, (float(volume) / 2_000_000.0) * 100.0) if volume is not None else None)
    volatility_score = _clamp100(
        min(100.0, (abs(float(change)) * 8.0) + ((float(day_range_pct) if day_range_pct is not None else 0.0) * 6.0))
        if change is not None
        else None
    )
    trend_score = _clamp100(
        min(100.0, max(0.0, 50.0 + (float(change) * 10.0))) if change is not None else None
    )

    # risk is higher when volatility high + liquidity low + AI caution
    risk_raw = 0.0
    if volatility_score is not None:
        risk_raw += float(volatility_score) * 0.55
    if liquidity_score is not None:
        risk_raw += float(100 - liquidity_score) * 0.25
    risk_raw += (20.0 if not llm_confirms else 0.0)
    if not regime.get("is_open"):
        risk_raw += 10.0
    risk_score = _clamp100(risk_raw)

    score_breakdown = {
        "momentum": momentum_score,
        "liquidity": liquidity_score,
        "volatility": volatility_score,
        "trend": trend_score,
        "risk": risk_score,
    }

    bullish_factors: List[str] = []
    bearish_factors: List[str] = []

    try:
        if change is not None and float(change) > 2:
            bullish_factors.append(f"Strong momentum: +{round(float(change), 2)}% today")
        if change is not None and float(change) < -2:
            bearish_factors.append(f"Negative momentum: {round(float(change), 2)}% today")
    except Exception:
        pass

    try:
        if volume is not None and float(volume) >= 1_000_000:
            bullish_factors.append(f"High liquidity: {int(volume):,} shares")
        if volume is not None and float(volume) < 500_000:
            bearish_factors.append(f"Low liquidity: {int(volume):,} shares")
    except Exception:
        pass

    if llm_confirms:
        bullish_factors.append("AI news read-through is constructive")
    else:
        bearish_factors.append("AI did not confirm a strong catalyst")

    if not regime.get("is_open"):
        bearish_factors.append("Market closed: wait for open confirmation")

    if early_session and llm_confirms:
        bullish_factors.append("Early-session edge: confirmation during opening window")

    if day_range_pct is not None:
        try:
            if float(day_range_pct) >= 5.0:
                bearish_factors.append(f"Elevated intraday range: {round(float(day_range_pct), 2)}%")
        except Exception:
            pass

    # ----------------------------
    # Scoring (ANTI-BUY-STARVATION)
    # ----------------------------
    score = 50
    badges = []
    risk_badges = []

    # Momentum
    if abs(change) > 2:
        score += 15
        badges.append("MOMENTUM")

    # Liquidity
    if volume > 1_000_000:
        score += 10
        badges.append("HIGH_LIQUIDITY")

    # AI confirmation (boost only, no hard penalty)
    if llm_confirms:
        score += 8
        badges.append("AI_CONFIRMED")
    else:
        risk_badges.append("AI_CAUTION")

    # Early-session edge (AI-only)
    if early_session and llm_confirms:
        score += 5
        badges.append("EARLY_SESSION_EDGE")

    # ----------------------------
    # Caps
    # ----------------------------
    caps = []
    if not regime.get("is_open"):
        caps.append(60)
        risk_badges.append("MARKET_CLOSED")

    score = apply_caps(score, caps)

    # ----------------------------
    # Recommendation (tiered)
    # ----------------------------
    if score >= 70:
        recommendation = "BUY"
        badges.append("STRONG_SETUP")
    elif score >= 62 and llm_confirms:
        recommendation = "BUY"
    elif score < 45:
        recommendation = "AVOID"
    else:
        recommendation = "HOLD"

    # ----------------------------
    # Confidence (momentum-first, AI-boosted)
    # ----------------------------
    confidence = (
        (1 if abs(change) > 2 else 0) * 0.45 +
        (1 if volume > 1_000_000 else 0) * 0.25 +
        (1 if llm_confirms else 0) * 0.30
    )
    confidence = round(min(confidence, 1.0), 2)
    confidence = _clamp01(confidence)

    # ----------------------------
    # Catalyst (never null)
    # ----------------------------
    catalyst = (
        llm.get("summary")
        if llm.get("summary")
        else "No strong news-driven catalyst confirmed by AI"
    )

    # ----------------------------
    # Actionability
    # ----------------------------
    session_context = "POST_CLOSE"
    try:
        c, _ = _get_clock_cached()
        if c is not None and bool(getattr(c, "is_open", False)):
            session_context = "OPEN"
        else:
            ts = getattr(c, "timestamp", None) if c is not None else None
            next_open = getattr(c, "next_open", None) if c is not None else None
            if ts and next_open and hasattr(ts, "date") and hasattr(next_open, "date"):
                session_context = "PRE_MARKET" if ts.date() == next_open.date() else "POST_CLOSE"
    except Exception:
        session_context = "POST_CLOSE"

    if recommendation == "BUY":
        buy_zone, buy_zone_pct, profit_targets, expected_move_pct, r_multiple, outcome_state = _derive_buy_zone_and_targets(
            pa_bar, prev, recommendation
        )
        why_not_now = None

        if not llm_confirms:
            risk_badges.append("NO_AI_CONFIRMATION")
    else:
        buy_zone = None
        buy_zone_pct = None
        profit_targets = None
        expected_move_pct = None
        r_multiple = None
        outcome_state = "ACTIVE"
        why_not_now = (
            "Needs confirmation / better location"
            if recommendation == "HOLD"
            else "Risk outweighs reward under current conditions"
        )

    if not regime.get("is_open"):
        if session_context == "POST_CLOSE" and recommendation == "BUY":
            confidence = _clamp01(round(max((confidence or 0.0) - 0.05, 0.0), 2))
            risk_badges.append("MARKET_CLOSED")
            why_not_now = "Market closed — wait for open confirmation"

    # ----------------------------
    # Price context (derived)
    # ----------------------------
    entry = None
    stop = None
    target = None
    try:
        if buy_zone and isinstance(buy_zone, dict):
            lo = buy_zone.get("low")
            hi = buy_zone.get("high")
            if lo is not None and hi is not None:
                entry = (float(lo) + float(hi)) / 2.0
            stop = float(lo) if lo is not None else None
    except Exception:
        entry = None
        stop = None
    try:
        if profit_targets is not None:
            if isinstance(profit_targets, list) and profit_targets:
                first = profit_targets[0]
                if isinstance(first, dict) and first.get("price") is not None:
                    target = float(first.get("price"))
                elif isinstance(first, (int, float)):
                    target = float(first)
            elif isinstance(profit_targets, (int, float)):
                target = float(profit_targets)
    except Exception:
        target = None

    series = get_daily_line_series(sym, limit=180) or []
    if not series:
        series = _bars_to_close_line_series(bars, limit=260) or []

    # ----------------------------
    # Shared Score + Explain engines (additive)
    # ----------------------------
    try:
        se = score_engine(snapshot, bars, regime, llm if allow_llm else None)
    except Exception:
        se = {
            "final_score": 3.5,
            "confidence_1_to_10": 3.0,
            "tier": "WATCH",
            "factors": {"momentum": 0, "trend": 0, "volatility": 0, "liquidity": 0, "risk": 100},
            "gates_applied": ["SCORE_ENGINE_FAILED"],
        }

    # Reference levels must always exist for WAIT/WATCH too.
    # If we cannot compute a real ATR-like range, keep targets as None and downgrade tier via gates.
    entry_ref = None
    stop_ref = None
    t1 = None
    t2 = None
    try:
        entry_ref = float(price) if price is not None else None
    except Exception:
        entry_ref = None
    try:
        low_day = float(pa_bar.get("l")) if isinstance(pa_bar, dict) and pa_bar.get("l") is not None else None
    except Exception:
        low_day = None
    try:
        stop_ref = low_day if (low_day is not None and entry_ref is not None and float(low_day) < float(entry_ref)) else (float(entry_ref) * 0.97 if entry_ref is not None else None)
    except Exception:
        stop_ref = None
    try:
        h = float(pa_bar.get("h")) if isinstance(pa_bar, dict) and pa_bar.get("h") is not None else None
        l = float(pa_bar.get("l")) if isinstance(pa_bar, dict) and pa_bar.get("l") is not None else None
        pc = float(prev.get("c")) if isinstance(prev, dict) and prev.get("c") is not None else None
        tr = None
        if h is not None and l is not None:
            tr0 = float(h) - float(l)
            if pc is not None:
                tr0 = max(tr0, abs(float(h) - float(pc)), abs(float(l) - float(pc)))
            tr = tr0 if tr0 > 0 else None
        if entry_ref is not None and tr is not None:
            t1 = float(entry_ref) + (1.0 * float(tr))
            t2 = float(entry_ref) + (2.0 * float(tr))
    except Exception:
        t1 = None
        t2 = None

    # Upgrade entry/stop/targets only when missing; never overwrite derived buy-zone based levels.
    if entry is None:
        entry = entry_ref
    if stop is None:
        stop = stop_ref
    if target is None and t1 is not None:
        target = t1

    # Ensure profit_targets has 2 levels for the frontend, without fabricating when TR unknown.
    if profit_targets is None:
        if t1 is not None and t2 is not None:
            profit_targets = [
                {"price": float(t1), "label": "T1"},
                {"price": float(t2), "label": "T2"},
            ]

    pc_for_explain = {
        "current": price,
        "entry": entry,
        "stop": stop,
        "target": target,
    }
    try:
        ee = explain_engine(sym, snapshot, regime, str(se.get("tier")), se.get("factors") if isinstance(se.get("factors"), dict) else {}, pc_for_explain)
    except Exception:
        ee = {
            "system_expectation": "Low edge: insufficient data to form a confident expectation.",
            "why_system_believes": ["", ""],
            "what_confirms": ["", ""],
            "what_breaks": ["", ""],
            "action_now": "WATCH",
            "setup_type": "CHOP_NO_EDGE",
        }

    # Position context (explicit)
    risk_per_share = None
    try:
        if entry is not None and stop is not None:
            rps = abs(float(entry) - float(stop))
            if math.isfinite(rps) and rps > 0:
                risk_per_share = rps
    except Exception:
        risk_per_share = None

    confidence_cap_applied = False
    try:
        # Explicitly communicate caps instead of forcing frontend inference.
        confidence_cap_applied = "MARKET_CLOSED" in list(set(risk_badges))
    except Exception:
        confidence_cap_applied = False

    # Pre-mover explanation comes from bars; score/confidence MUST be derived from 0–100 internal factors.
    is_mover = False
    try:
        is_mover = sym in _movers_set_cached()
    except Exception:
        is_mover = False

    _, why_early, building, what_confirms, what_breaks = _pre_mover_score_from_bars(bars)

    factors = {
        "momentum": int(momentum_score) if momentum_score is not None else 0,
        "liquidity": int(liquidity_score) if liquidity_score is not None else 0,
        "volatility": int(volatility_score) if volatility_score is not None else 0,
        "trend": int(trend_score) if trend_score is not None else 0,
        "risk": int(risk_score) if risk_score is not None else 0,
    }

    score, confidence, weighted_internal_score = _derive_score_confidence_from_factors(
        factors,
        market_regime_str=str(regime.get("regime") or ""),
    )

    # Top mover penalty: decay confidence if already a mover / late spike.
    try:
        if bool(is_mover) or (change is not None and abs(float(change)) >= 10.0):
            confidence = max(0.0, round(float(confidence) - 0.10, 4))
            if confidence > (float(score) / 10.0):
                confidence = round(float(score) / 10.0, 4)
    except Exception:
        pass

    def _tier_from_score(s: float) -> str:
        try:
            x = float(s)
        except Exception:
            x = 1.0
        if x >= 8.0:
            return "PRIMARY"
        if x >= 6.0:
            return "SECONDARY"
        if x >= 4.0:
            return "TERTIARY"
        return "WATCH"

    tier = _tier_from_score(score)

    # 1–2 sentence rationale
    rationale = ""
    try:
        why = ("; ".join(why_early[:2]) if isinstance(why_early, list) and why_early else "")
        conf = ("; ".join(what_confirms[:1]) if isinstance(what_confirms, list) and what_confirms else "")
        if why and conf:
            rationale = f"{why}. {conf}."
        elif why:
            rationale = why
        else:
            rationale = "Pre-mover scan: best available setup with defined risk."
    except Exception:
        rationale = "Pre-mover scan: best available setup with defined risk."

    return aurexis_decision(sym, allow_llm=allow_llm, snapshot=snap, bars=bars_payload)


# ... (rest of the code remains the same)


# ---------------------------
# BEST PICK (FULL)
# ---------------------------

def _market_block_from_regime(regime: str) -> dict:
    """
    Maps market regime to UI block / execution bias.
    Safe fallback if regime logic unavailable.
    """

    if isinstance(regime, dict):
        try:
            regime = str(regime.get("regime") or regime.get("session_context") or "")
        except Exception:
            regime = ""
    else:
        try:
            regime = str(regime or "")
        except Exception:
            regime = ""

    if not regime:
        return {
            "label": "Neutral Market",
            "bias": "Balanced",
            "execution": "Standard risk"
        }

    r = regime.lower()

    if "bull" in r:
        return {
            "label": "Bullish Regime",
            "bias": "Long continuation",
            "execution": "Buy dips / breakouts"
        }

    if "bear" in r:
        return {
            "label": "Bearish Regime",
            "bias": "Short pressure",
            "execution": "Fade pops / breakdowns"
        }

    if "volatile" in r:
        return {
            "label": "High Volatility",
            "bias": "Scalp / fast exits",
            "execution": "Reduced size"
        }

    return {
        "label": "Neutral Market",
        "bias": "Balanced",
        "execution": "Standard risk"
    }

def _best_pick_legacy(max_scan: int = 200):
    import time

    START_TIME = time.time()
    HARD_TIME_LIMIT = 30  # seconds
    MAX_DEEP = 3          # max full LLM analyses

    regime = market_regime()
    symbols = get_alpaca_symbols(max_scan)

    scanned = 0

    # Best Pick thresholds (1–10 scale)
    BUY_SCORE_MIN = 7
    BUY_CONF_MIN = 6
    WAIT_SCORE_MIN = 6
    WAIT_CONF_MIN = 4
    WAIT_CONF_MAX = 5

    def _clamp01(x: float) -> float:
        try:
            return max(0.0, min(1.0, float(x)))
        except Exception:
            return 0.0

    def _derive_setup_levels_from_bars(bars_list: Any):
        # Returns (entry_zone_dict, stop, first_target, support, resistance, atr14)
        if not isinstance(bars_list, list) or len(bars_list) < 30:
            return None, None, None, None, None, None

        bars0 = [b for b in bars_list if isinstance(b, dict)]
        if len(bars0) < 30:
            return None, None, None, None, None, None

        try:
            bars0.sort(key=lambda x: int(x.get("time", 0)))
        except Exception:
            pass

        closes = []
        highs = []
        lows = []
        for b in bars0:
            try:
                c = b.get("c")
                h = b.get("h")
                l = b.get("l")
                if c is None or h is None or l is None:
                    continue
                closes.append(float(c))
                highs.append(float(h))
                lows.append(float(l))
            except Exception:
                continue

        if len(closes) < 30:
            return None, None, None, None, None, None

        price = closes[-1]
        try:
            resistance = max(highs[-20:]) if len(highs) >= 20 else max(highs)
        except Exception:
            resistance = None
        try:
            support = min(lows[-10:]) if len(lows) >= 10 else min(lows)
        except Exception:
            support = None

        # Fallbacks if highs/lows are missing/unreliable
        try:
            if resistance is None and len(closes) >= 20:
                resistance = max(closes[-20:])
        except Exception:
            resistance = resistance
        try:
            if support is None and len(closes) >= 10:
                support = min(closes[-10:])
        except Exception:
            support = support

        # ATR14
        atr14 = None
        try:
            if len(closes) >= 15:
                trs = []
                for i in range(len(closes) - 14, len(closes)):
                    hi = highs[i]
                    lo = lows[i]
                    pc = closes[i - 1]
                    tr = max(hi - lo, abs(hi - pc), abs(lo - pc))
                    trs.append(tr)
                atr14 = (sum(trs) / float(len(trs))) if trs else None
        except Exception:
            atr14 = None

        # Entry zone: just below/through resistance (breakout trigger)
        entry_zone = None
        try:
            anchor = None
            if resistance is not None and math.isfinite(float(resistance)) and float(resistance) > 0:
                anchor = float(resistance)
            elif price is not None and math.isfinite(float(price)) and float(price) > 0:
                anchor = float(price)
            if anchor is not None:
                lo = float(anchor) * 0.995
                hi = float(anchor) * 1.005
                entry_zone = {"low": round(lo, 4), "high": round(hi, 4)}
        except Exception:
            entry_zone = None

        # Stop: below support (or 1.5*ATR below entry)
        stop = None
        try:
            if support is not None and math.isfinite(float(support)):
                stop = float(support)
        except Exception:
            stop = None
        try:
            if stop is None and entry_zone and atr14 is not None and math.isfinite(float(atr14)):
                stop = float(entry_zone.get("low")) - (1.5 * float(atr14))
        except Exception:
            stop = stop
        try:
            if stop is None and entry_zone and entry_zone.get("low") is not None:
                stop = float(entry_zone.get("low")) * 0.97
        except Exception:
            stop = stop

        # First target: entry + 3*ATR (or resistance + 3*ATR)
        first_target = None
        try:
            anchor = None
            if entry_zone and entry_zone.get("high") is not None:
                anchor = float(entry_zone.get("high"))
            elif resistance is not None:
                anchor = float(resistance)
            if anchor is not None and atr14 is not None and math.isfinite(float(atr14)):
                first_target = anchor + (3.0 * float(atr14))
        except Exception:
            first_target = None
        try:
            if first_target is None and entry_zone and entry_zone.get("high") is not None:
                first_target = float(entry_zone.get("high")) * 1.12
        except Exception:
            first_target = first_target

        return entry_zone, stop, first_target, support, resistance, atr14
    fast_candidates = []
    mid_candidates = []
    deep_candidates = []

    best = None
    best_rank = -1
    fallback = None

    # ----------------------------
    # STAGE 1 — FAST PREFILTER
    # ----------------------------
    for sym in symbols:
        if time.time() - START_TIME > HARD_TIME_LIMIT:
            break

        scanned += 1
        try:
            snap = get_snapshot(sym)
            bar = snap.get("dailyBar")
            prev = snap.get("prevDailyBar")
            if not bar or not prev:
                continue

            change = ((bar["c"] - prev["c"]) / prev["c"]) * 100
            volume = bar.get("v", 0)

            if abs(change) < 1.5:
                continue
            if volume < 500_000:
                continue

            fast_candidates.append(sym)

        except Exception:
            continue

        if len(fast_candidates) >= 30:
            break

    if not fast_candidates:
        return {
            "status": "no_edge",
            "market_regime": regime.get("regime"),
            "scanned": scanned,
            "runtime_sec": round(time.time() - START_TIME, 2),
            "message": "No fast-moving liquid stocks detected",
            "updated_at": now_iso()
        }

    # ----------------------------
    # STAGE 2 — MID ANALYSIS (NO LLM)
    # ----------------------------
    for sym in fast_candidates:
        if time.time() - START_TIME > HARD_TIME_LIMIT:
            break

        try:
            res = analyze(sym, allow_llm=False)
        except Exception:
            continue

        if not isinstance(res, dict):
            continue

        score = res.get("score", 0)
        conf = res.get("confidence", 0)
        try:
            conf = float(conf) if conf is not None else 0.0
        except Exception:
            conf = 0.0

        if score >= 55 or conf >= 0.4:
            mid_candidates.append(res)

        if len(mid_candidates) >= 8:
            break

    if not mid_candidates:
        return {
            "status": "no_edge",
            "market_regime": regime.get("regime"),
            "scanned": scanned,
            "runtime_sec": round(time.time() - START_TIME, 2),
            "message": "Momentum detected but no quality setups",
            "updated_at": now_iso()
        }

    # ----------------------------
    # STAGE 3 — DEEP AI ANALYSIS
    # ----------------------------
    for res in sorted(mid_candidates, key=lambda x: x["score"], reverse=True):
        if time.time() - START_TIME > HARD_TIME_LIMIT:
            break
        if len(deep_candidates) >= MAX_DEEP:
            break

        sym = res["symbol"]
        try:
            deep = analyze(sym, allow_llm=True)
            deep_candidates.append(deep)
        except Exception:
            continue

    # ----------------------------
    # FINAL SELECTION
    # ----------------------------
    for res in deep_candidates:
        score = res.get("score", 0)
        conf = res.get("confidence", 0)
        risk = res.get("risk_badges", [])

        ai_penalty = 5 if "AI_CAUTION" in risk else 0
        rank = score + (conf * 10) - ai_penalty

        if res["recommendation"] == "HOLD":
            if not fallback or score > fallback["score"]:
                fallback = res

        if res["recommendation"] != "BUY":
            continue

        if rank > best_rank:
            best = res
            best_rank = rank

    runtime = round(time.time() - START_TIME, 2)

    if best:
        return {
            "status": "ok",
            "pick": best,
            "market_regime": regime.get("regime"),
            "scanned": scanned,
            "runtime_sec": runtime,
            "updated_at": now_iso()
        }

    if fallback:
        fallback = fallback.copy()
        fallback["recommendation"] = "WAIT"
        fallback["why_not_now"] = "Strong setup, but AI confirmation incomplete"

        return {
            "status": "no_buy",
            "pick": fallback,
            "market_regime": regime.get("regime"),
            "scanned": scanned,
            "runtime_sec": runtime,
            "message": "No BUY passed full AI confirmation",
            "updated_at": now_iso()
        }

    return {
        "status": "no_pick",
        "reason": "NO_VALID_SET]UPS",
        "market_regime": regime.get("regime"),
        "scanned": scanned,
        "runtime_sec": runtime,
        "message": "Market conditions did not produce a high-quality setup",
        "updated_at": now_iso()
    }






# ===========================
# ALPACA UNIVERSE + BEST PICK
# (DROP-IN REPLACEMENT BLOCK)
# ===========================

from typing import Any, Dict, List, Optional

def _asset_field(a: Any, name: str, default=None):
    """
    Alpaca Asset objects vary by SDK version.
    This safely reads fields from:
    - attributes (a.symbol)
    - dict-like (_raw)
    - __dict__
    """
    if a is None:
        return default
    if hasattr(a, name):
        return getattr(a, name)
    raw = getattr(a, "_raw", None)
    if isinstance(raw, dict) and name in raw:
        return raw.get(name, default)
    d = getattr(a, "__dict__", None)
    if isinstance(d, dict) and name in d:
        return d.get(name, default)
    return default

def _asset_class(a: Any) -> Optional[str]:
    # Different SDKs: asset_class / class_ / class
    v = _asset_field(a, "asset_class", None)
    if v is None:
        v = _asset_field(a, "class_", None)
    if v is None:
        v = _asset_field(a, "class", None)
    if isinstance(v, str):
        return v.lower().strip()
    return None

def get_alpaca_symbols(max_symbols: int = 0) -> List[str]:
    """
    Returns all ACTIVE, TRADABLE US equities from Alpaca assets.
    max_symbols=0 means no cap.
    """
    api = tradeapi.REST(os.getenv("ALPACA_API_KEY",""), os.getenv("ALPACA_SECRET_KEY",""), base_url="https://paper-api.alpaca.markets")  # uses your existing function
    try:
        assets = api.list_assets(status="active")
    except TypeError:
        # some versions use list_assets() without kwargs
        assets = api.list_assets()

    out: List[str] = []
    for a in assets or []:
        sym = _asset_field(a, "symbol", "")
        if not sym:
            continue

        # Only tradable
        tradable = _asset_field(a, "tradable", True)
        if tradable is False:
            continue

        # Prefer US equities only
        cls = _asset_class(a)
        if cls and cls not in ("us_equity", "equity"):
            continue

        # Avoid OTC if field exists
        exch = _asset_field(a, "exchange", "")
        if isinstance(exch, str) and exch.upper() == "OTC":
            continue

        out.append(str(sym).strip().upper())

        if max_symbols and len(out) >= max_symbols:
            break

    # De-dupe while keeping order
    seen = set()
    deduped = []
    for s in out:
        if s not in seen:
            seen.add(s)
            deduped.append(s)
    return deduped


def score_symbol(technical: Dict[str, Any], sentiment: Dict[str, Any], timing: Dict[str, Any]) -> float:
    try:
        t = float((technical or {}).get("score") or 0.0)
        s = float((sentiment or {}).get("score") or 0.0)
        tm = float((timing or {}).get("score") or 0.0)
    except Exception:
        t, s, tm = 0.0, 0.0, 0.0
    return (t * 0.45) + (s * 0.40) + (tm * 0.15)


def _compute_timing_from_regime_and_factors(regime: Dict[str, Any], factors_0_100: Dict[str, Any]) -> Dict[str, Any]:
    try:
        is_open = bool((regime or {}).get("is_open"))
    except Exception:
        is_open = False
    try:
        mom = float((factors_0_100 or {}).get("momentum") or 0.0)
        tr = float((factors_0_100 or {}).get("trend") or 0.0)
        rk = float((factors_0_100 or {}).get("risk") or 50.0)
    except Exception:
        mom, tr, rk = 0.0, 0.0, 50.0

    base = 50.0
    try:
        if tr >= 55.0 and mom >= 55.0:
            base += 20.0
        elif tr >= 55.0 or mom >= 55.0:
            base += 10.0
        if rk >= 75.0:
            base -= 15.0
        if not is_open:
            base -= 10.0
    except Exception:
        base = 45.0

    try:
        base = max(0.0, min(100.0, float(base)))
    except Exception:
        base = 45.0
    return {"score": float(base), "is_open": bool(is_open)}


def _sentiment_score_from_news_block(news_sentiment: Dict[str, Any]) -> Dict[str, Any]:
    ns = news_sentiment if isinstance(news_sentiment, dict) else {}
    score_100 = None
    try:
        score_100 = ns.get("score_100")
        if score_100 is None:
            score_100 = ns.get("score")
    except Exception:
        score_100 = None
    try:
        s = float(score_100) if score_100 is not None else 50.0
    except Exception:
        s = 50.0
    if s < 0.0:
        s = 0.0
    if s > 100.0:
        s = 100.0
    return {"score": float(s)}


def _technical_score_from_factors(factors_0_100: Dict[str, Any]) -> Dict[str, Any]:
    f = factors_0_100 if isinstance(factors_0_100, dict) else {}
    try:
        score_100 = (
            0.35 * float(f.get("trend") or 0.0)
            + 0.30 * float(f.get("momentum") or 0.0)
            + 0.25 * float(f.get("liquidity") or 0.0)
            + 0.10 * (100.0 - float(f.get("risk") or 0.0))
        )
    except Exception:
        score_100 = 0.0
    try:
        score_100 = max(0.0, min(100.0, float(score_100)))
    except Exception:
        score_100 = 0.0
    return {
        "score": float(score_100),
        "liquidity": float(f.get("liquidity") or 0.0),
        "factors": f,
    }


def _clamp_0_100(x: Any, default: float = 50.0) -> float:
    try:
        v = float(x)
    except Exception:
        v = float(default)
    if not math.isfinite(v):
        v = float(default)
    if v < 0.0:
        v = 0.0
    if v > 100.0:
        v = 100.0
    return float(v)


def _bars_to_ohlcv(bars: Any) -> Dict[str, List[float]]:
    out = {"o": [], "h": [], "l": [], "c": [], "v": []}
    if not isinstance(bars, list):
        return out
    for b in bars:
        if not isinstance(b, dict):
            continue
        try:
            o = float(b.get("o"))
            h = float(b.get("h"))
            l = float(b.get("l"))
            c = float(b.get("c"))
            v = float(b.get("v") or b.get("volume") or 0.0)
        except Exception:
            continue
        if not (math.isfinite(o) and math.isfinite(h) and math.isfinite(l) and math.isfinite(c) and math.isfinite(v)):
            continue
        out["o"].append(o)
        out["h"].append(h)
        out["l"].append(l)
        out["c"].append(c)
        out["v"].append(v)
    return out


def _pct_change(a: Optional[float], b: Optional[float]) -> Optional[float]:
    try:
        if a is None or b is None:
            return None
        if float(b) == 0.0:
            return None
        return (float(a) - float(b)) / float(b) * 100.0
    except Exception:
        return None


def _technical_analysis_from_bars_and_snapshot(symbol: str, bars: Any, snapshot: Any) -> Dict[str, Any]:
    _ = symbol
    ohlcv = _bars_to_ohlcv(bars)
    c = ohlcv.get("c") or []
    h = ohlcv.get("h") or []
    l = ohlcv.get("l") or []
    v = ohlcv.get("v") or []
    if len(c) < 35:
        return {"momentum": 50, "trend": 50, "volatility": 50, "liquidity": 50, "risk": 50}

    price = float(c[-1])
    rsi14 = ta.rsi(c, 14)
    macd_line, macd_sig, macd_hist = ta.macd(c)
    _ = macd_line
    _ = macd_sig
    sma20 = ta.sma(c, 20)
    sma50 = ta.sma(c, 50)
    sma200 = ta.sma(c, 200)
    atr14 = ta.atr(h, l, c, 14)

    vol20 = None
    vol60 = None
    try:
        if len(v) >= 20:
            vol20 = sum(v[-20:]) / 20.0
        if len(v) >= 60:
            vol60 = sum(v[-60:]) / 60.0
    except Exception:
        vol20, vol60 = None, None

    vol_trend = None
    try:
        if vol20 is not None and vol60 is not None and float(vol60) > 0:
            vol_trend = float(vol20) / float(vol60)
    except Exception:
        vol_trend = None

    ret5 = None
    ret20 = None
    try:
        if len(c) >= 6:
            ret5 = _pct_change(c[-1], c[-6])
        if len(c) >= 21:
            ret20 = _pct_change(c[-1], c[-21])
    except Exception:
        ret5, ret20 = None, None

    rsi_score = None
    try:
        if rsi14 is not None:
            rsi_score = max(0.0, min(100.0, (float(rsi14) - 30.0) / 40.0 * 100.0))
    except Exception:
        rsi_score = None

    accel_score = 50.0
    try:
        if ret5 is not None and ret20 is not None:
            accel = float(ret5) - (float(ret20) / 4.0)
            accel_score = max(0.0, min(100.0, 50.0 + (accel / 5.0) * 50.0))
    except Exception:
        accel_score = 50.0

    macd_score = 50.0
    try:
        if macd_hist is not None and price > 0:
            norm = float(macd_hist) / float(price)
            macd_score = max(0.0, min(100.0, 50.0 + (norm / 0.01) * 50.0))
    except Exception:
        macd_score = 50.0

    momentum = 0.0
    try:
        parts = []
        if rsi_score is not None:
            parts.append(float(rsi_score))
        parts.append(float(accel_score))
        parts.append(float(macd_score))
        momentum = sum(parts) / float(len(parts))
    except Exception:
        momentum = 0.0

    ma_align = 0.0
    try:
        align = 0
        if sma20 is not None and price > float(sma20):
            align += 1
        if sma50 is not None and price > float(sma50):
            align += 1
        if sma200 is not None and price > float(sma200):
            align += 1
        if sma20 is not None and sma50 is not None and float(sma20) > float(sma50):
            align += 1
        if sma50 is not None and sma200 is not None and float(sma50) > float(sma200):
            align += 1
        ma_align = (float(align) / 5.0) * 100.0
    except Exception:
        ma_align = 0.0

    hh_hl = 50.0
    try:
        if len(c) >= 60:
            prev_high = max(c[-60:-20])
            recent_high = max(c[-20:])
            if prev_high and recent_high:
                hh_hl = 50.0 + (_pct_change(recent_high, prev_high) or 0.0) * 4.0
        hh_hl = max(0.0, min(100.0, float(hh_hl)))
    except Exception:
        hh_hl = 50.0

    trend = (0.65 * float(ma_align)) + (0.35 * float(hh_hl))
    trend = _clamp_0_100(trend, 0.0)

    atr_pct = None
    try:
        if atr14 is not None and price > 0:
            atr_pct = float(atr14) / float(price) * 100.0
    except Exception:
        atr_pct = None

    atr_score = 0.0
    try:
        if atr_pct is not None:
            atr_score = max(0.0, min(100.0, (float(atr_pct) / 6.0) * 100.0))
    except Exception:
        atr_score = 0.0

    atr_expansion = 50.0
    try:
        if atr_pct is not None and len(c) >= 80:
            atr50 = ta.atr(h[-80:], l[-80:], c[-80:], 50)
            atr50_pct = (float(atr50) / float(price) * 100.0) if (atr50 is not None and price > 0) else None
            if atr50_pct is not None and float(atr50_pct) > 0:
                ratio = float(atr_pct) / float(atr50_pct)
                atr_expansion = max(0.0, min(100.0, 50.0 + (ratio - 1.0) * 60.0))
    except Exception:
        atr_expansion = 50.0

    volatility = _clamp_0_100((0.60 * float(atr_score)) + (0.40 * float(atr_expansion)), 0.0)

    bid = None
    ask = None
    try:
        if isinstance(snapshot, dict):
            q = snapshot.get("latestQuote") if isinstance(snapshot.get("latestQuote"), dict) else {}
            bid = float(q.get("bp")) if q.get("bp") is not None else None
            ask = float(q.get("ap")) if q.get("ap") is not None else None
    except Exception:
        bid, ask = None, None

    spread_pct = None
    try:
        if bid is not None and ask is not None and float(ask) > 0 and float(ask) >= float(bid):
            spread_pct = (float(ask) - float(bid)) / float(ask) * 100.0
    except Exception:
        spread_pct = None

    dv = None
    try:
        vol_last = float(v[-1]) if v else None
        if vol_last is not None:
            dv = float(price) * float(vol_last)
    except Exception:
        dv = None

    liq_vol = 0.0
    try:
        if dv is not None:
            liq_vol = max(0.0, min(100.0, (float(dv) / 30_000_000.0) * 100.0))
    except Exception:
        liq_vol = 0.0

    liq_spread = 50.0
    try:
        if spread_pct is not None:
            liq_spread = max(0.0, min(100.0, 100.0 - (float(spread_pct) / 1.0) * 100.0))
    except Exception:
        liq_spread = 50.0

    liq_vol_trend = 50.0
    try:
        if vol_trend is not None:
            liq_vol_trend = max(0.0, min(100.0, 50.0 + (float(vol_trend) - 1.0) * 50.0))
    except Exception:
        liq_vol_trend = 50.0

    liquidity = _clamp_0_100((0.55 * float(liq_vol)) + (0.25 * float(liq_spread)) + (0.20 * float(liq_vol_trend)), 0.0)

    structure_risk = 50.0
    try:
        if sma50 is not None and price > 0:
            dd = (float(sma50) - float(price)) / float(price) * 100.0
            structure_risk = max(0.0, min(100.0, 50.0 + dd * 6.0))
    except Exception:
        structure_risk = 50.0

    vol_risk = 50.0
    try:
        vol_risk = max(0.0, min(100.0, 30.0 + (float(volatility) * 0.8)))
    except Exception:
        vol_risk = 50.0

    liquidity_risk = 50.0
    try:
        liquidity_risk = max(0.0, min(100.0, 100.0 - float(liquidity)))
    except Exception:
        liquidity_risk = 50.0

    risk = _clamp_0_100((0.45 * float(vol_risk)) + (0.35 * float(structure_risk)) + (0.20 * float(liquidity_risk)), 60.0)

    return {
        "momentum": int(round(momentum)),
        "trend": int(round(trend)),
        "volatility": int(round(volatility)),
        "liquidity": int(round(liquidity)),
        "risk": int(round(risk)),
    }


def _fetch_news_for_symbol(symbol: str, limit: int = 10) -> List[Dict[str, Any]]:
    sym = str(symbol or "").strip().upper()
    if not sym:
        return []
    top_n = max(1, min(int(limit or 10), 20))
    url = f"{ALPACA_DATA_BASE_URL}/v1beta1/news"
    params = {"symbols": sym, "limit": top_n, "sort": "desc"}
    try:
        r = requests.get(url, headers=data_headers(), params=params, timeout=12)
        if r.status_code != 200:
            return []
        data = r.json() or {}
    except Exception:
        return []
    items = data.get("news") or data.get("items") or []
    if not isinstance(items, list):
        return []
    out: List[Dict[str, Any]] = []
    for n in items[:top_n]:
        if not isinstance(n, dict):
            continue
        title = str(n.get("headline") or n.get("title") or "").strip()
        summary = str(n.get("summary") or "").strip()
        if not title:
            continue
        out.append(
            {
                "title": title[:240],
                "summary": summary[:500],
                "url": str(n.get("url") or "").strip()[:400],
                "source": str(n.get("source") or "").strip()[:120],
                "publishedAt": str(n.get("created_at") or n.get("published_at") or "").strip()[:64],
            }
        )
    return out


def _sentiment_from_news(symbol: str, allow_llm: bool = True) -> Dict[str, Any]:
    sym = str(symbol or "").strip().upper()
    if not sym:
        return {"direction": "NEUTRAL", "summary": "unavailable", "headlines": [], "score_100": 50, "news_status": "unavailable"}

    news_items = _fetch_news_for_symbol(sym, limit=10)
    headlines = [str(x.get("title") or "").strip() for x in news_items if isinstance(x, dict) and str(x.get("title") or "").strip()]
    headlines = headlines[:5]
    if not headlines:
        return {"direction": "NEUTRAL", "summary": "No recent headlines available.", "headlines": [], "score_100": 50, "news_status": "unavailable"}

    if not allow_llm:
        return {"direction": "NEUTRAL", "summary": "News sentiment unavailable.", "headlines": headlines, "score_100": 50, "news_status": "ok", "llm_used": False}

    if not os.getenv("OPENAI_API_KEY"):
        return {"direction": "NEUTRAL", "summary": "News sentiment unavailable.", "headlines": headlines, "score_100": 50, "news_status": "ok", "llm_used": False}

    try:
        if bool(_llm_cb_is_open()):
            return {"direction": "NEUTRAL", "summary": "News sentiment unavailable.", "headlines": headlines, "score_100": 50, "news_status": "ok", "llm_used": False}
    except Exception:
        pass

    # Prefer the drop-in Responses API wiring (llm_services) when available.
    if _llm_news_sentiment is not None:
        try:
            data = _llm_news_sentiment(sym, headlines)
            direction = str((data or {}).get("direction") or "NEUTRAL").strip().upper()
            if direction not in ("BULLISH", "BEARISH", "NEUTRAL"):
                direction = "NEUTRAL"
            summary = str((data or {}).get("summary") or "unavailable").strip()[:220]
            hl2 = (data or {}).get("headlines")
            if not isinstance(hl2, list):
                hl2 = headlines
            hl2 = [str(x).strip() for x in hl2 if str(x or "").strip()][:8]

            risk_flags = []
            try:
                rf0 = (data or {}).get("risk_flags")
                if isinstance(rf0, list):
                    risk_flags = [str(x).strip() for x in rf0 if str(x or "").strip()][:6]
            except Exception:
                risk_flags = []

            score_100 = 50
            try:
                if direction == "BULLISH":
                    score_100 = 70
                elif direction == "BEARISH":
                    score_100 = 30
                else:
                    score_100 = 50
            except Exception:
                score_100 = 50

            llm_used = True
            try:
                llm_used = bool("LLM disabled" not in summary) and bool(summary) and (summary != "unavailable")
            except Exception:
                llm_used = False

            return {
                "direction": direction,
                "summary": summary,
                "headlines": hl2,
                "score_100": int(score_100),
                "news_status": "ok",
                "llm_used": bool(llm_used),
                "risk_flags": risk_flags,
            }
        except Exception:
            pass

    return {"direction": "NEUTRAL", "summary": "News sentiment unavailable.", "headlines": headlines, "score_100": 50, "news_status": "ok", "llm_used": False}


def get_market_universe() -> List[str]:
    # Build a large tradable universe from Alpaca assets, then filter by volume using snapshots.
    # This is the main driver to ensure scanning is dynamic (no tiny hardcoded ticker lists).
    movers: List[str] = []
    try:
        for m in (get_top_movers(200) or []):
            if isinstance(m, dict) and m.get("symbol"):
                movers.append(str(m.get("symbol") or "").strip().upper())
    except Exception:
        movers = []

    max_assets = 6000
    try:
        max_assets = int(os.getenv("MARKET_UNIVERSE_MAX_ASSETS", "6000") or 6000)
    except Exception:
        max_assets = 6000
    if max_assets < 500:
        max_assets = 500
    if max_assets > 10000:
        max_assets = 10000

    universe_all: List[str] = []
    try:
        universe_all = get_alpaca_symbols(max_symbols=max_assets) or []
    except Exception:
        universe_all = []

    min_volume = 1_000_000.0
    try:
        min_volume = float(os.getenv("MARKET_UNIVERSE_MIN_DAILY_VOLUME", "1000000") or 1_000_000.0)
    except Exception:
        min_volume = 1_000_000.0
    if min_volume < 0:
        min_volume = 0.0

    liquid: List[str] = []
    # Batched snapshots for volume filter (fast; avoids per-symbol calls).
    try:
        for i in range(0, len(universe_all), 100):
            chunk = [str(s or "").strip().upper() for s in universe_all[i : i + 100] if str(s or "").strip()]
            if not chunk:
                continue
            try:
                url = f"{ALPACA_DATA_BASE_URL}/v2/stocks/snapshots"
                params = {"symbols": ",".join(chunk), "feed": (os.getenv("ALPACA_DATA_FEED") or "iex").strip() or "iex"}
                r = requests.get(url, headers=data_headers(), params=params, timeout=10)
                if r.status_code != 200:
                    continue
                snapmap = (r.json() or {}).get("snapshots") or {}
            except Exception:
                snapmap = {}
            if not isinstance(snapmap, dict):
                continue
            for sym, snap in snapmap.items():
                if not isinstance(sym, str) or not isinstance(snap, dict):
                    continue
                bar = snap.get("dailyBar") if isinstance(snap.get("dailyBar"), dict) else {}
                try:
                    vol = float(bar.get("v")) if bar.get("v") is not None else None
                except Exception:
                    vol = None
                if vol is None or vol < float(min_volume):
                    continue
                sd = _symbol_sanitize(str(sym or ""), allow_extended=False)
                if bool(sd.get("ok")):
                    liquid.append(str(sd.get("symbol") or "").strip().upper())

            if len(liquid) >= 800:
                break
    except Exception:
        liquid = []

    saved: List[str] = []
    try:
        for row in (_saved_picks_list() or []):
            if isinstance(row, dict) and row.get("symbol"):
                saved.append(str(row.get("symbol") or "").strip().upper())
    except Exception:
        saved = []

    watchlist_syms: List[str] = []
    try:
        wl = watchlist_get()
        witems = wl.get("items") if isinstance(wl, dict) else []
        if not isinstance(witems, list):
            witems = []
        for it in witems:
            if not isinstance(it, dict):
                continue
            s = str(it.get("symbol") or "").strip().upper()
            if s:
                watchlist_syms.append(s)
    except Exception:
        watchlist_syms = []

    # Required ETF universe
    etfs = ["SPY", "QQQ", "IWM", "DIA"]
    sector_etfs = ["XLK", "XLF", "XLE", "XLV", "XLY", "XLP"]

    # Extra liquid ETFs / proxies (safe add-ons)
    extra_etfs = [
        "XLU", "XLI", "XLB", "XLC", "XLRE", "SMH", "SOXX", "IBB", "ARKK", "TLT",
        "GLD", "SLV", "USO", "EEM", "EWZ", "FXI",
    ]
    etfs = list(etfs + sector_etfs + extra_etfs)

    out: List[str] = []
    seen = set()
    for s in (etfs + movers + watchlist_syms + saved + liquid):
        ss = str(s or "").strip().upper()
        if not ss or ss in seen:
            continue
        seen.add(ss)
        out.append(ss)
    # Keep the universe reasonably sized for scanning.
    try:
        cap = int(os.getenv("MARKET_UNIVERSE_SYMBOL_CAP", "1200") or 1200)
    except Exception:
        cap = 1200
    if cap < 500:
        cap = 500
    if cap > 3000:
        cap = 3000
    return out[:cap]


_SCAN_UNIVERSE_CACHE: Dict[str, Any] = {"ts": 0.0, "raw": [], "filtered": [], "ranked": []}


def get_alpaca_trading_client():
    if _AlpacaPyTradingClient is None:
        return None
    return _AlpacaPyTradingClient(
        api_key=(os.getenv("ALPACA_API_KEY") or "").strip(),
        secret_key=(os.getenv("ALPACA_SECRET_KEY") or "").strip(),
        paper=str(os.getenv("ALPACA_PAPER", "true") or "true").strip().lower() in ("1", "true", "yes", "y"),
    )


def get_scan_universe(max_scan: int = 0) -> List[str]:
    try:
        # Default 4 hours: the bg scan refreshes every 4h so the full Alpaca fetch
        # only happens via bg thread (never per-request, avoiding Railway 30s timeout).
        cache_ttl_s = float(os.getenv("SCAN_UNIVERSE_CACHE_SECONDS", "14400") or 14400)
    except Exception:
        cache_ttl_s = 14400.0
    if cache_ttl_s < 0:
        cache_ttl_s = 0.0

    try:
        now_ts = time.time()
    except Exception:
        now_ts = 0.0

    try:
        if cache_ttl_s > 0 and (now_ts - float(_SCAN_UNIVERSE_CACHE.get("ts") or 0.0)) < cache_ttl_s:
            ranked_cached = _SCAN_UNIVERSE_CACHE.get("ranked") or []
            if isinstance(ranked_cached, list) and ranked_cached:
                out0 = [str(s or "").strip().upper() for s in ranked_cached if str(s or "").strip()]
                # Don't serve a tiny seed cache when a larger universe was requested —
                # fall through to file cache or full fetch instead.
                req_cap = int(max_scan) if max_scan else 0
                min_acceptable = min(req_cap, 500) if req_cap > 500 else 0
                if min_acceptable and len(out0) < min_acceptable:
                    pass  # fall through
                elif req_cap and req_cap > 0:
                    return out0[:req_cap]
                else:
                    return out0
            filtered_cached = _SCAN_UNIVERSE_CACHE.get("filtered") or []
            if isinstance(filtered_cached, list) and filtered_cached:
                out0 = [str(s or "").strip().upper() for s in filtered_cached if str(s or "").strip()]
                return out0
    except Exception:
        pass

    # Check shared file cache written by whichever worker last did a full fetch.
    try:
        import json as _json
        _uc_path = os.path.join(os.getenv("RAILWAY_VOLUME_MOUNT_PATH", "/app/data"), "scan_universe_cache.json")
        with open(_uc_path) as _f:
            _fc = _json.load(_f)
        _fc_ts = float(_fc.get("ts") or 0.0)
        _fc_ranked = _fc.get("ranked") or []
        if isinstance(_fc_ranked, list) and _fc_ranked and (time.time() - _fc_ts) < cache_ttl_s:
            _SCAN_UNIVERSE_CACHE["ts"] = _fc_ts
            _SCAN_UNIVERSE_CACHE["ranked"] = _fc_ranked
            log.info(f"get_scan_universe: loaded {len(_fc_ranked)} symbols from shared file cache")
            cap = int(max_scan) if max_scan else 0
            return _fc_ranked[:cap] if cap else _fc_ranked
    except Exception:
        pass

    log.info(f"get_scan_universe: cache miss, fetching full Alpaca universe (max_scan={max_scan})")

    try:
        cap0 = int(max_scan) if max_scan is not None else 0
    except Exception:
        cap0 = 0
    if cap0 < 0:
        cap0 = 0

    exclude_leveraged = str(os.getenv("SCAN_UNIVERSE_EXCLUDE_LEVERAGED", "0") or "0").strip().lower() in (
        "1",
        "true",
        "yes",
        "y",
    )

    all_assets: List[Any] = []
    try:
        api_py = get_alpaca_trading_client()
        if api_py is not None and _AlpacaPyGetAssetsRequest is not None and _AlpacaPyAssetStatus is not None and _AlpacaPyAssetClass is not None:
            req = _AlpacaPyGetAssetsRequest(status=_AlpacaPyAssetStatus.ACTIVE, asset_class=_AlpacaPyAssetClass.US_EQUITY)
            all_assets = list(api_py.get_all_assets(req) or [])
        elif api_py is not None:
            try:
                all_assets = list(api_py.get_all_assets(status="active", asset_class="us_equity") or [])
            except Exception:
                all_assets = list(api_py.get_all_assets() or [])
        else:
            api = tradeapi.REST(os.getenv("ALPACA_API_KEY",""), os.getenv("ALPACA_SECRET_KEY",""), base_url="https://paper-api.alpaca.markets")
            try:
                all_assets = api.list_assets(status="active")
            except TypeError:
                all_assets = api.list_assets()
    except Exception as e:
        try:
            log.exception(f"get_scan_universe: Alpaca list_assets failed: {e}")
        except Exception:
            pass
        all_assets = []

    raw_syms: List[str] = []
    for a in all_assets or []:
        sym = _asset_field(a, "symbol", "")
        if not sym:
            continue
        s = str(sym).strip().upper()
        if not s:
            continue

        tradable = _asset_field(a, "tradable", True)
        if tradable is False:
            continue

        # Asset class must be US equity
        cls = _asset_class(a)
        if cls != "us_equity":
            continue

        # Exclude OTC / Pink (exchange missing or OTC flag)
        exch = _asset_field(a, "exchange", None)
        if exch is None:
            continue
        exch_u = str(exch or "").strip().upper()
        if not exch_u:
            continue
        if "OTC" in exch_u:
            continue

        # Exclude halted / blocked if field exists
        if bool(_asset_field(a, "trading_blocked", False)) is True:
            continue

        # Exclusions by symbol shape
        # - Warrants/Units/Rights: end with W/U/R (common pattern)
        # - Preferred shares: contains PFD or PR
        if s.endswith("W") or s.endswith("U") or s.endswith("R"):
            continue
        if "PFD" in s or "PR" in s:
            continue
        if s.startswith("TEST"):
            continue
        if exclude_leveraged and ("2X" in s or "3X" in s):
            continue

        raw_syms.append(s)

    # De-dupe (keep order)
    raw_seen = set()
    raw_deduped: List[str] = []
    for s in raw_syms:
        if s in raw_seen:
            continue
        raw_seen.add(s)
        raw_deduped.append(s)
    raw_syms = raw_deduped

    filtered_syms = list(raw_syms)

    # Include core ETFs alongside equities (system spec), but allow disabling.
    etf_core = ["SPY", "QQQ", "IWM", "DIA", "XLK", "XLF", "XLE", "XLV"]
    include_etfs = str(os.getenv("SCAN_UNIVERSE_INCLUDE_ETFS", "1") or "1").strip().lower() in (
        "1",
        "true",
        "yes",
        "y",
        "on",
    )
    if include_etfs:
        scan_list = list(etf_core) + list(filtered_syms)
    else:
        scan_list = list(filtered_syms)

    # De-dupe while preserving order.
    try:
        seen2 = set()
        scan_list_d: List[str] = []
        for s in scan_list:
            ss = str(s or "").strip().upper()
            if not ss or ss in seen2:
                continue
            seen2.add(ss)
            scan_list_d.append(ss)
        scan_list = scan_list_d
    except Exception:
        pass

    universe_total = len(scan_list)
    liquidity_ranked = 0

    if cap0 and cap0 > 0 and scan_list:
        # Liquidity pre-ranking: dollar_volume = last_price * volume
        snap_chunk_size = 200
        try:
            snap_chunk_size = int(os.getenv("SCAN_UNIVERSE_SNAPSHOT_CHUNK_SIZE", "200") or 200)
        except Exception:
            snap_chunk_size = 200
        snap_chunk_size = max(50, min(300, snap_chunk_size))

        dv_by_sym: Dict[str, float] = {}
        missing_dv: List[str] = []

        for i in range(0, len(scan_list), snap_chunk_size):
            chunk = scan_list[i : i + snap_chunk_size]
            if not chunk:
                continue
            try:
                snapmap = _alpaca_get_snapshots_batch(chunk) or {}
            except Exception:
                snapmap = {}
            if not isinstance(snapmap, dict):
                snapmap = {}
            for sym in chunk:
                s = str(sym or "").strip().upper()
                if not s:
                    continue
                snap = snapmap.get(s)
                if snap is None and isinstance(snapmap, dict):
                    snap = snapmap.get(sym)
                px = None
                vol = None
                try:
                    if isinstance(snap, dict):
                        px = snap.get("last_price")
                        db = snap.get("dailyBar") if isinstance(snap.get("dailyBar"), dict) else {}
                        vol = db.get("v")
                        if px is None:
                            px = db.get("c")
                except Exception:
                    px, vol = None, None
                try:
                    px_f = float(px) if px is not None else None
                except Exception:
                    px_f = None
                try:
                    vol_f = float(vol) if vol is not None else None
                except Exception:
                    vol_f = None
                if px_f is None or vol_f is None or (not math.isfinite(px_f)) or (not math.isfinite(vol_f)):
                    missing_dv.append(s)
                    continue
                dv_by_sym[s] = float(px_f) * float(vol_f)

        # Fallback: daily bars average dollar volume when snapshot DV missing
        if missing_dv:
            bars_chunk_size = 200
            try:
                bars_chunk_size = int(os.getenv("SCAN_UNIVERSE_BARS_CHUNK_SIZE", "200") or 200)
            except Exception:
                bars_chunk_size = 200
            bars_chunk_size = max(50, min(300, bars_chunk_size))

            try:
                lookback = int(os.getenv("SCAN_UNIVERSE_BARS_LOOKBACK_DAYS", "30") or 30)
            except Exception:
                lookback = 30
            lookback = max(10, min(90, lookback))

            for i in range(0, len(missing_dv), bars_chunk_size):
                chunk = missing_dv[i : i + bars_chunk_size]
                if not chunk:
                    continue
                try:
                    bars_map = _alpaca_get_bars_batch(chunk, "1Day", int(lookback)) or {}
                except Exception:
                    bars_map = {}
                if not isinstance(bars_map, dict):
                    bars_map = {}
                for sym, bars in bars_map.items():
                    s = str(sym or "").strip().upper()
                    if not s or s in dv_by_sym:
                        continue
                    if not isinstance(bars, list) or not bars:
                        continue
                    xs: List[float] = []
                    for b in bars[-lookback:]:
                        if not isinstance(b, dict):
                            continue
                        try:
                            c = float(b.get("c")) if b.get("c") is not None else None
                            v = float(b.get("v")) if b.get("v") is not None else None
                        except Exception:
                            c, v = None, None
                        if c is None or v is None or c <= 0 or v < 0:
                            continue
                        xs.append(float(c) * float(v))
                    if xs:
                        dv_by_sym[s] = float(sum(xs) / float(len(xs) or 1))

        ranked_syms = [s for s in scan_list if str(s or "").strip()]
        ranked_syms = [str(s).strip().upper() for s in ranked_syms]
        ranked_syms = list(dict.fromkeys(ranked_syms))
        ranked_syms.sort(key=lambda s: float(dv_by_sym.get(s, 0.0) or 0.0), reverse=True)
        liquidity_ranked = len(ranked_syms)

        ensure = [s for s in etf_core if s]
        ensure_set = set(ensure)

        cap_eff = int(cap0)
        keep_n = max(0, cap_eff - len(ensure))
        top_rest = [s for s in ranked_syms if s not in ensure_set][:keep_n]
        scan_list = ensure + top_rest

    try:
        log.info(
            {
                "universe_total": int(universe_total),
                "liquidity_ranked": int(liquidity_ranked),
                "scan_size_final": int(len(scan_list)),
            }
        )
    except Exception:
        pass

    try:
        _SCAN_UNIVERSE_CACHE["ts"] = float(now_ts)
        _SCAN_UNIVERSE_CACHE["raw"] = list(raw_syms)
        _SCAN_UNIVERSE_CACHE["filtered"] = list(filtered_syms)
        _SCAN_UNIVERSE_CACHE["ranked"] = list(scan_list)
    except Exception:
        pass

    # Write to shared file so all gunicorn workers can read the same universe.
    try:
        import json as _json
        _uc_path = os.path.join(os.getenv("RAILWAY_VOLUME_MOUNT_PATH", "/app/data"), "scan_universe_cache.json")
        with open(_uc_path, "w") as _f:
            _json.dump({"ts": float(now_ts), "ranked": list(scan_list)}, _f)
    except Exception:
        pass

    return scan_list


async def _best_pick_v2_universe(*, max_scan: int = 1200, mode: str = "ranked") -> List[str]:
    etf_core = [
        "SPY",
        "QQQ",
        "IWM",
        "DIA",
        "XLK",
        "XLF",
        "XLE",
        "XLV",
        "XLY",
        "XLP",
        "XLU",
        "XLI",
        "XLB",
        "XLC",
        "XLRE",
        "SMH",
        "SOXX",
        "IBB",
        "ARKK",
        "TLT",
        "GLD",
        "SLV",
        "USO",
        "EEM",
        "EWZ",
        "FXI",
    ]

    try:
        cap = int(max_scan) if max_scan is not None else 1200
    except Exception:
        cap = 1200
    try:
        mode0 = str(mode or "ranked").strip().lower()
    except Exception:
        mode0 = "ranked"
    cap_max = 8000 if mode0 in ("all", "all_assets", "assets") else 3000
    cap = max(200, min(cap_max, cap))

    try:
        max_assets = int(os.getenv("BEST_PICK_V2_MAX_ASSETS", "6000") or 6000)
    except Exception:
        max_assets = 6000
    max_assets = max(500, min(8000, max_assets))

    try:
        liquid_take = int(os.getenv("BEST_PICK_V2_LIQUID_TAKE", "2000") or 2000)
    except Exception:
        liquid_take = 2000
    liquid_take = max(300, min(3000, liquid_take))

    try:
        rs_take = int(os.getenv("BEST_PICK_V2_RS20_TAKE", "300") or 300)
    except Exception:
        rs_take = 300
    rs_take = max(50, min(600, rs_take))

    def _avg_30d_dollar_vol(bars: Any) -> Optional[float]:
        if not isinstance(bars, list) or len(bars) < 30:
            return None
        tail = [b for b in bars[-30:] if isinstance(b, dict)]
        if len(tail) < 20:
            return None
        xs: List[float] = []
        for b in tail:
            try:
                c = float(b.get("c")) if b.get("c") is not None else None
                v = float(b.get("v")) if b.get("v") is not None else None
            except Exception:
                c, v = None, None
            if c is None or v is None or c <= 0 or v < 0:
                continue
            xs.append(float(c) * float(v))
        if len(xs) < 20:
            return None
        return float(sum(xs) / float(len(xs) or 1))

    def _avg_30d_vol(bars: Any) -> Optional[float]:
        if not isinstance(bars, list) or len(bars) < 30:
            return None
        tail = [b for b in bars[-30:] if isinstance(b, dict)]
        if len(tail) < 20:
            return None
        xs: List[float] = []
        for b in tail:
            try:
                v = float(b.get("v")) if b.get("v") is not None else None
            except Exception:
                v = None
            if v is None or v < 0:
                continue
            xs.append(float(v))
        if len(xs) < 20:
            return None
        return float(sum(xs) / float(len(xs) or 1))

    def _last_close(bars: Any) -> Optional[float]:
        if not isinstance(bars, list) or not bars:
            return None
        for b in reversed(bars):
            if not isinstance(b, dict):
                continue
            if b.get("c") is None:
                continue
            try:
                c = float(b.get("c"))
            except Exception:
                continue
            if c > 0:
                return float(c)
        return None

    def _roc20(bars: Any) -> Optional[float]:
        if not isinstance(bars, list) or len(bars) < 22:
            return None
        closes: List[float] = []
        for b in bars[-60:]:
            if not isinstance(b, dict) or b.get("c") is None:
                continue
            try:
                closes.append(float(b.get("c")))
            except Exception:
                continue
        if len(closes) < 22:
            return None
        a = float(closes[-21])
        b = float(closes[-1])
        if a <= 0:
            return None
        return float((b - a) / a)

    equities: List[str] = []
    try:
        equities = await asyncio.to_thread(get_alpaca_symbols, max_assets)
    except Exception as e:
        try:
            log.exception(f"best_pick_v2_universe: get_alpaca_symbols failed: {e}")
        except Exception:
            pass
        equities = []

    equities = [str(s or "").strip().upper() for s in (equities or []) if str(s or "").strip()]
    equities = list(dict.fromkeys(equities))

    if mode0 in ("all", "all_assets", "assets"):
        out_all: List[str] = []
        seen_all = set()
        for s in (etf_core + equities):
            ss = str(s or "").strip().upper()
            if not ss or ss in seen_all:
                continue
            seen_all.add(ss)
            out_all.append(ss)
            if len(out_all) >= int(cap):
                break
        try:
            log.info(f"best_pick_v2_universe(all_assets): etfs={len(etf_core)} equities_assets={len(equities)} final={len(out_all)}")
        except Exception:
            pass
        return out_all

    chunk_size = 200
    try:
        chunk_size = int(os.getenv("BEST_PICK_V2_UNIVERSE_CHUNK_SIZE", "200") or 200)
    except Exception:
        chunk_size = 200
    chunk_size = max(50, min(300, chunk_size))

    bars_by_symbol: Dict[str, Any] = {}
    for i in range(0, len(equities), chunk_size):
        chunk = equities[i : i + chunk_size]
        if not chunk:
            continue
        try:
            d0 = await asyncio.to_thread(_alpaca_get_bars_batch, chunk, "1Day", 80)
        except Exception:
            d0 = {}
        if isinstance(d0, dict):
            bars_by_symbol.update(d0)
        if len(bars_by_symbol) >= max_assets:
            break

    liquid_ranked: List[Tuple[str, float]] = []
    for sym, bars in (bars_by_symbol or {}).items():
        s = str(sym or "").strip().upper()
        if not s:
            continue
        px = _last_close(bars)
        if px is None or float(px) < 5.0:
            continue
        av = _avg_30d_vol(bars)
        if av is None or float(av) < 200_000.0:
            continue
        dv = _avg_30d_dollar_vol(bars)
        if dv is None or float(dv) <= 0:
            continue
        liquid_ranked.append((s, float(dv)))

    liquid_ranked.sort(key=lambda x: float(x[1]), reverse=True)
    liquid_syms = [s for s, _ in liquid_ranked[: int(liquid_take)]]

    spy_roc = None
    try:
        spy_bars = bars_by_symbol.get("SPY")
        if spy_bars is None:
            spy_bars = (await asyncio.to_thread(_alpaca_get_bars_batch, ["SPY"], "1Day", 80)).get("SPY")
        spy_roc = _roc20(spy_bars)
    except Exception:
        spy_roc = None

    rs_ranked: List[Tuple[str, float]] = []
    if spy_roc is not None:
        for sym in liquid_syms[: max(800, int(liquid_take))]:
            bars = bars_by_symbol.get(sym)
            r = _roc20(bars)
            if r is None:
                continue
            rs = float(r) - float(spy_roc)
            rs_ranked.append((sym, rs))
        rs_ranked.sort(key=lambda x: float(x[1]), reverse=True)

    momentum_syms = [s for s, _ in rs_ranked[: int(rs_take)]]

    earnings_syms: List[str] = []

    out: List[str] = []
    seen = set()
    for s in (etf_core + liquid_syms + momentum_syms + earnings_syms):
        ss = str(s or "").strip().upper()
        if not ss or ss in seen:
            continue
        seen.add(ss)
        out.append(ss)
        if len(out) >= int(cap):
            break

    try:
        log.info(
            "best_pick_v2_universe: "
            f"etfs={len(etf_core)} "
            f"equities_assets={len(equities)} "
            f"bars_loaded={len(bars_by_symbol)} "
            f"liquid={len(liquid_syms)} "
            f"momentum={len(momentum_syms)} "
            f"earnings={len(earnings_syms)} "
            f"final={len(out)}"
        )
    except Exception:
        pass

    return out


def _clamp01(v: Any) -> float:
    try:
        x = float(v)
    except Exception:
        x = 0.0
    if not math.isfinite(x):
        x = 0.0
    if x < 0.0:
        x = 0.0
    if x > 1.0:
        x = 1.0
    return float(x)


def _pct_change_from_snapshot(snapshot: Any) -> Optional[float]:
    if not isinstance(snapshot, dict):
        return None
    bar = snapshot.get("dailyBar") if isinstance(snapshot.get("dailyBar"), dict) else {}
    prev = snapshot.get("prevDailyBar") if isinstance(snapshot.get("prevDailyBar"), dict) else {}
    try:
        c = float(bar.get("c")) if bar.get("c") is not None else None
        pc = float(prev.get("c")) if prev.get("c") is not None else None
    except Exception:
        c, pc = None, None
    if c is None or pc is None or pc == 0:
        return None
    try:
        return float(round(((c - pc) / pc) * 100.0, 3))
    except Exception:
        return None


async def _scan_universe_ranked(*, universe: List[str], max_seconds: float = 8.0) -> Dict[str, Any]:
    t0 = time.time()

    syms: List[str] = []
    try:
        for s in (universe or []):
            sd = _symbol_sanitize(str(s or ""), allow_extended=False)
            if bool(sd.get("ok")):
                syms.append(str(sd.get("symbol") or "").strip().upper())
    except Exception:
        syms = [str(s or "").strip().upper() for s in (universe or []) if str(s or "").strip()]
    syms = list(dict.fromkeys([s for s in syms if s]))

    try:
        log.info(f"Scanning universe… ({len(syms)} symbols)")
    except Exception:
        pass

    async def _fetch_chunked() -> tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
        # Alpaca snapshot batch is capped (and bars endpoints can hit URL-length limits), so chunk.
        chunk_size = 200
        try:
            chunk_size = int(os.getenv("BEST_PICK_SCAN_CHUNK_SIZE", "200") or 200)
        except Exception:
            chunk_size = 200
        chunk_size = max(25, min(300, chunk_size))

        sem = asyncio.Semaphore(3)

        async def _run_chunk(chunk: List[str]) -> tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
            async with sem:
                snaps_task = asyncio.to_thread(_alpaca_get_snapshots_batch, chunk)
                daily_task = asyncio.to_thread(_alpaca_get_bars_batch, chunk, "1Day", 100)
                intra_task = asyncio.to_thread(_alpaca_get_bars_batch, chunk, "5Min", 300)
                snaps, daily_map, intra_map = await asyncio.gather(snaps_task, daily_task, intra_task)
                return (
                    snaps if isinstance(snaps, dict) else {},
                    daily_map if isinstance(daily_map, dict) else {},
                    intra_map if isinstance(intra_map, dict) else {},
                )

        tasks: List[asyncio.Task] = []
        for i in range(0, len(syms), chunk_size):
            chunk = syms[i : i + chunk_size]
            if not chunk:
                continue
            tasks.append(asyncio.create_task(_run_chunk(chunk)))

        snaps_out: Dict[str, Any] = {}
        daily_out: Dict[str, Any] = {}
        intra_out: Dict[str, Any] = {}
        for t in tasks:
            if (time.time() - t0) > float(max_seconds):
                break
            try:
                s0, d0, i0 = await t
                snaps_out.update(s0)
                daily_out.update(d0)
                intra_out.update(i0)
            except Exception:
                continue
        return snaps_out, daily_out, intra_out

    try:
        snaps0, daily0, intra0 = await asyncio.wait_for(_fetch_chunked(), timeout=max(3.0, float(max_seconds)))
    except Exception:
        snaps0, daily0, intra0 = {}, {}, {}

    snaps = snaps0 if isinstance(snaps0, dict) else {}
    daily_map = daily0 if isinstance(daily0, dict) else {}
    intra_map = intra0 if isinstance(intra0, dict) else {}

    try:
        log.info(f"Evaluating {len(syms)} symbols…")
    except Exception:
        pass

    ranked: List[Dict[str, Any]] = []

    try:
        max_abs_chg_exclude = float(os.getenv("BEST_PICK_MAX_ABS_PCT_CHANGE", "25") or 25.0)
    except Exception:
        max_abs_chg_exclude = 25.0
    try:
        max_abs_chg_exclude = float(max(0.0, min(250.0, max_abs_chg_exclude)))
    except Exception:
        max_abs_chg_exclude = 25.0

    try:
        max_abs_chg_penalty_start = float(os.getenv("BEST_PICK_PCT_CHANGE_PENALTY_START", "12") or 12.0)
    except Exception:
        max_abs_chg_penalty_start = 12.0
    try:
        max_abs_chg_penalty_start = float(max(0.0, min(max_abs_chg_exclude, max_abs_chg_penalty_start)))
    except Exception:
        max_abs_chg_penalty_start = min(12.0, max_abs_chg_exclude)

    for sym in syms:
        if (time.time() - t0) > float(max_seconds):
            break

        snapshot = snaps.get(sym) if isinstance(snaps, dict) else None
        daily_bars = daily_map.get(sym) if isinstance(daily_map, dict) else None
        intra_bars = intra_map.get(sym) if isinstance(intra_map, dict) else None

        pct_change = None
        try:
            pct_change = _pct_change_from_snapshot(snapshot)
        except Exception:
            pct_change = None

        try:
            if pct_change is not None and max_abs_chg_exclude > 0 and abs(float(pct_change)) >= float(max_abs_chg_exclude):
                continue
        except Exception:
            pass

        if not isinstance(daily_bars, list) or len(daily_bars) < 50:
            continue

        ta0: Dict[str, Any] = {}
        ind0: Dict[str, Any] = {}
        try:
            ta0 = compute_technical_indicators(daily_bars) or {}
        except Exception:
            ta0 = {}

        # For alignment with /analyze, compute indicators via indicator_engine.calculate_indicators.
        # This yields the same indicator schema that feeds _score_composite_0_100 and _score_execution_0_100 in /analyze.
        try:
            ind0 = calculate_indicators(daily_bars) or {}
        except Exception:
            ind0 = {}
        if not isinstance(ind0, dict) or not ind0:
            ind0 = ta0 if isinstance(ta0, dict) else {}

        # 0..1 normalized factors
        momentum = _clamp01(_clamp_0_100(ta0.get("momentum")) / 100.0)
        trend = _clamp01(_clamp_0_100(ta0.get("trend")) / 100.0)
        volatility = _clamp01(_clamp_0_100(ta0.get("volatility")) / 100.0)
        liquidity = 0.0
        try:
            liquidity = _clamp01(float(_score_volume_0_100_from_snapshot(snapshot)) / 100.0)
        except Exception:
            liquidity = 0.0
        risk = _clamp01(_clamp_0_100(ta0.get("risk")) / 100.0)

        # Sentiment proxy: cheap, deterministic; upgraded elsewhere when LLM/news is enabled.
        sentiment_0_1 = 0.5
        try:
            sent = _sentiment_proxy_from_snapshot(snapshot) or {}
            sentiment_0_1 = _clamp01(float((sent or {}).get("score_100") or 50.0) / 100.0)
        except Exception:
            sentiment_0_1 = 0.5

        pick_score = _clamp01(
            (0.30 * momentum)
            + (0.25 * trend)
            + (0.20 * volatility)
            + (0.15 * liquidity)
            + (0.10 * sentiment_0_1)
        )

        # Analyze-style rating (non-LLM, fast) for alignment between Best Pick and /analyze.
        analyze_ai_0_100 = None
        analyze_ex_0_100 = None
        analyze_rating_0_100 = None
        try:
            # Convert the sentiment proxy back to 0..100 to match the scoring engine signature.
            ns100 = float(max(0.0, min(100.0, float(sentiment_0_1) * 100.0)))
            _regime = "neutral"
            try:
                _regime = _get_market_regime()
            except Exception:
                pass
            analyze_ai_0_100 = float(_score_composite_0_100(indicators=ind0, news_sentiment_0_100=ns100, regime=_regime) or 0.0)
            analyze_ex_0_100 = float(_score_execution_0_100(indicators=ind0) or 0.0)
            analyze_rating_0_100 = float((0.65 * analyze_ai_0_100) + (0.35 * analyze_ex_0_100))
        except Exception:
            analyze_ai_0_100 = None
            analyze_ex_0_100 = None
            analyze_rating_0_100 = None

        try:
            if pct_change is not None and abs(float(pct_change)) >= float(max_abs_chg_penalty_start) and float(max_abs_chg_exclude) > float(max_abs_chg_penalty_start):
                span = float(max_abs_chg_exclude) - float(max_abs_chg_penalty_start)
                over = max(0.0, abs(float(pct_change)) - float(max_abs_chg_penalty_start))
                t = max(0.0, min(1.0, over / max(1e-9, span)))
                penalty_mult = 1.0 - (0.55 * t)
                pick_score = _clamp01(float(pick_score) * float(penalty_mult))
        except Exception:
            pass

        ranked.append(
            {
                "symbol": sym,
                "pick_score": float(pick_score),
                "market_cap": None,
                "pct_change": (float(pct_change) if pct_change is not None else None),
                "analyze_ai_score_0_100": (float(round(analyze_ai_0_100, 1)) if analyze_ai_0_100 is not None else None),
                "analyze_execution_score_0_100": (float(round(analyze_ex_0_100, 1)) if analyze_ex_0_100 is not None else None),
                "analyze_rating_0_100": (float(round(analyze_rating_0_100, 1)) if analyze_rating_0_100 is not None else None),
                "factors": {
                    "momentum": float(momentum),
                    "trend": float(trend),
                    "volatility": float(volatility),
                    "liquidity": float(liquidity),
                    "risk": float(risk),
                    "sentiment": float(sentiment_0_1),
                },
                "snapshot": snapshot,
                "daily_bars": daily_bars[-100:] if isinstance(daily_bars, list) else [],
                "intraday_bars": intra_bars[-300:] if isinstance(intra_bars, list) else [],
            }
        )

    def _rank_key(it: Dict[str, Any]) -> float:
        try:
            a = float(it.get("analyze_rating_0_100") or 0.0) / 100.0
        except Exception:
            a = 0.0
        try:
            p = float(it.get("pick_score") or 0.0)
        except Exception:
            p = 0.0
        # Weight analyze rating more heavily to match /analyze philosophy.
        return (0.70 * a) + (0.30 * p)

    ranked.sort(key=_rank_key, reverse=True)

    # Optional Polygon market cap enrichment (top 50 only). Must not block the event loop.
    if ranked and callable(_polygon_get_market_cap):
        for it in ranked[:50]:
            if (time.time() - t0) > float(max_seconds):
                break
            try:
                sym0 = str(it.get("symbol") or "").strip().upper()
                if not sym0:
                    continue
                mc = await asyncio.to_thread(_polygon_get_market_cap, sym0)
                it["market_cap"] = mc
            except Exception:
                continue

    # News + Sentiment enrichment (top K only; cached). This drives 20% of ranking.
    news_k = 25
    try:
        news_k = int(os.getenv("BEST_PICK_NEWS_TOPK", "25") or 25)
    except Exception:
        news_k = 25
    news_k = max(0, min(50, news_k))

    allow_llm_news = True
    try:
        allow_llm_news = str(os.getenv("BEST_PICK_NEWS_ALLOW_LLM", "1") or "1").strip().lower() in ("1", "true", "yes", "on")
    except Exception:
        allow_llm_news = True

    # Re-score using 20% sentiment weight and a scaled-down base model.
    # Old weights (sum=0.90 without sentiment): mom 0.30, trend 0.25, vol 0.20, liq 0.15
    # New: sentiment 0.20; remaining 0.80 allocated proportionally to the old base weights.
    base_scale = 0.80 / 0.90
    w_mom = 0.30 * base_scale
    w_tr = 0.25 * base_scale
    w_vol = 0.20 * base_scale
    w_liq = 0.15 * base_scale
    w_sent = 0.20

    if ranked and news_k > 0:
        for it in ranked[:news_k]:
            if (time.time() - t0) > float(max_seconds):
                break
            try:
                sym0 = str(it.get("symbol") or "").strip().upper()
                if not sym0:
                    continue
                n0 = await asyncio.to_thread(_news_and_sentiment, sym0, allow_llm=bool(allow_llm_news))
                if isinstance(n0, dict):
                    it["news"] = n0
                    # Convert sentiment score to 0..1. Prefer explicit score, else direction.
                    sent01 = 0.5
                    try:
                        if n0.get("score") is not None:
                            sent01 = _clamp01((float(n0.get("score")) + 100.0) / 200.0)
                        else:
                            sent01 = _clamp01(float(_sentiment_score_0_100(n0)) / 100.0)
                    except Exception:
                        sent01 = 0.5

                    f = it.get("factors") if isinstance(it.get("factors"), dict) else {}
                    mom = _clamp01(float(f.get("momentum") or 0.0))
                    tr = _clamp01(float(f.get("trend") or 0.0))
                    vol = _clamp01(float(f.get("volatility") or 0.0))
                    liq = _clamp01(float(f.get("liquidity") or 0.0))
                    # Updated composite
                    it["pick_score"] = float(_clamp01((w_mom * mom) + (w_tr * tr) + (w_vol * vol) + (w_liq * liq) + (w_sent * sent01)))
            except Exception:
                continue

        ranked.sort(key=lambda x: float(x.get("pick_score") or 0.0), reverse=True)

    # Optional Polygon unusual options (top 10 only) and +10% scoring weight.
    top_k = 10
    try:
        top_k = int(os.getenv("POLYGON_OPTIONS_TOPK", "10") or 10)
    except Exception:
        top_k = 10
    top_k = max(0, min(25, top_k))

    if ranked and callable(_polygon_get_unusual_options) and top_k > 0:
        for it in ranked[:top_k]:
            if (time.time() - t0) > float(max_seconds):
                break
            try:
                sym = str(it.get("symbol") or "").strip().upper()
                if not sym:
                    continue
                uo = await asyncio.to_thread(_polygon_get_unusual_options, sym)
                if not isinstance(uo, dict):
                    continue
                u_score = _clamp01(float(uo.get("unusual_options_score") or 0.0) / 100.0)

                base = _clamp01(it.get("pick_score"))
                final = _clamp01((0.90 * base) + (0.10 * u_score))

                # Penalize very small caps (<$500M) by damping the final score.
                try:
                    mc = it.get("market_cap")
                    if mc is not None and float(mc) > 0 and float(mc) < 500_000_000:
                        final = _clamp01(final * 0.80)
                except Exception:
                    pass

                it["pick_score"] = float(final)
                it["unusual_options"] = uo
            except Exception:
                continue

        ranked.sort(key=lambda x: float(x.get("pick_score") or 0.0), reverse=True)

    try:
        log.info("Ranking complete…")
    except Exception:
        pass

    best = ranked[0] if ranked else {"symbol": "SPY", "pick_score": 0.0, "factors": {}}
    try:
        log.info(f"Best pick selected: {best.get('symbol')} ({best.get('pick_score')})")
    except Exception:
        pass

    return {
        "ranked": ranked,
        "best": best,
        "universe": syms,
        "elapsed_s": float(round(time.time() - t0, 3)),
    }


def get_liquid_stocks(limit: int = 300, max_scan_assets: int = 1200) -> List[str]:
    try:
        lim = max(1, int(limit))
    except Exception:
        lim = 300

    try:
        max_scan = max(50, int(max_scan_assets))
    except Exception:
        max_scan = 1200

    try:
        universe = get_alpaca_symbols(max_scan) or []
    except Exception:
        universe = []

    regime = {}
    try:
        regime = market_regime() or {}
    except Exception:
        regime = {}

    ranked: List[Dict[str, Any]] = []
    for sym in universe:
        s = str(sym or "").strip().upper()
        if not s:
            continue
        try:
            snapshot = get_snapshot(s)
        except Exception:
            snapshot = None
        if not snapshot:
            continue
        try:
            bars = get_daily_bars_lookback(s, lookback_days=180, min_bars=30) or []
        except Exception:
            bars = []
        try:
            se = score_engine(snapshot, bars, regime, llm_result=None) or {}
            f = se.get("factors") if isinstance(se.get("factors"), dict) else {}
            liq = float(f.get("liquidity") or 0.0)
        except Exception:
            liq = 0.0
        if liq <= 0:
            continue
        ranked.append({"symbol": s, "liquidity": liq})

    ranked.sort(key=lambda x: float(x.get("liquidity") or 0.0), reverse=True)
    return [str(x.get("symbol") or "").upper() for x in ranked[:lim] if x.get("symbol")]


async def scan_market_for_best_pick(max_scan: int = 200) -> Dict[str, Any]:
    symbols = get_market_universe()
    try:
        cap = int(max_scan) if max_scan is not None else 200
    except Exception:
        cap = 200
    cap = max(300, min(800, cap))
    try:
        symbols = list(symbols)[:cap]
    except Exception:
        symbols = symbols

    clean_universe: List[str] = []
    try:
        for s in symbols:
            sd = _symbol_sanitize(str(s or ""), allow_extended=False)
            if bool(sd.get("ok")):
                clean_universe.append(str(sd.get("symbol") or "").strip().upper())
    except Exception:
        clean_universe = [str(s or "").strip().upper() for s in symbols if str(s or "").strip()]
    symbols = clean_universe

    t0 = time.time()

    regime = {}
    try:
        regime = market_regime() or {}
    except Exception:
        regime = {}

    market = {}
    try:
        market = _market_block_from_regime(regime)
    except Exception:
        market = {"is_open": False, "session": "UNKNOWN", "updated_at": now_iso()}

    async def _get_snapshots_batched(syms: List[str]) -> Dict[str, Any]:
        outm: Dict[str, Any] = {}
        feed = (os.getenv("ALPACA_DATA_FEED") or "iex").strip() or "iex"
        for i in range(0, len(syms), 100):
            chunk = [str(s or "").strip().upper() for s in syms[i : i + 100] if str(s or "").strip()]
            if not chunk:
                continue

            def _do():
                url = f"{ALPACA_DATA_BASE_URL}/v2/stocks/snapshots"
                params = {"symbols": ",".join(chunk), "feed": feed}
                r = requests.get(url, headers=data_headers(), params=params, timeout=10)
                if r.status_code != 200:
                    return {}
                return ((r.json() or {}).get("snapshots") or {})

            try:
                snapmap0 = await asyncio.to_thread(_retry_call, _do, retries=3, base_delay_sec=0.35)
            except Exception:
                snapmap0 = {}
            if isinstance(snapmap0, dict):
                outm.update(snapmap0)
            await asyncio.sleep(0.25)
        return outm

    snapmap = await _get_snapshots_batched(symbols)

    def _tier1(min_volume: float, min_atr_pct: float) -> List[str]:
        out0: List[str] = []
        for sym, snap in (snapmap or {}).items():
            if not isinstance(sym, str) or not isinstance(snap, dict):
                continue
            bar = snap.get("dailyBar") if isinstance(snap.get("dailyBar"), dict) else {}
            lt = snap.get("latestTrade") if isinstance(snap.get("latestTrade"), dict) else {}
            try:
                px = lt.get("p") if lt.get("p") is not None else bar.get("c")
                px = float(px) if px is not None else None
            except Exception:
                px = None
            try:
                vol = float(bar.get("v")) if bar.get("v") is not None else None
            except Exception:
                vol = None
            if px is None or vol is None or px <= 0 or vol <= 0:
                continue
            if float(vol) < float(min_volume):
                continue
            try:
                hi = float(bar.get("h")) if bar.get("h") is not None else None
                lo = float(bar.get("l")) if bar.get("l") is not None else None
            except Exception:
                hi, lo = None, None
            atr_pct = None
            try:
                if hi is not None and lo is not None and float(px) > 0:
                    atr_pct = (float(hi) - float(lo)) / float(px) * 100.0
            except Exception:
                atr_pct = None
            if atr_pct is None or float(atr_pct) < float(min_atr_pct):
                continue
            out0.append(sym.strip().upper())
        return list(dict.fromkeys(out0))

    survivors = _tier1(min_volume=500_000.0, min_atr_pct=1.0)
    if len(survivors) < 5:
        survivors = _tier1(min_volume=500_000.0 * 0.70, min_atr_pct=1.0 * 0.70)
    if not survivors:
        survivors = [str(s or "").strip().upper() for s in symbols if str(s or "").strip()][:120]

    deep_cap = 80
    try:
        deep_cap = max(15, min(80, int(max_scan) if max_scan is not None else 80))
    except Exception:
        deep_cap = 80

    async def _scan_pass(*, rsi_lo: float, rsi_hi: float, trend_strength_min: float, momentum_min: float, volatility_min: float) -> List[Dict[str, Any]]:
        outc: List[Dict[str, Any]] = []
        for symbol in survivors[:deep_cap]:
            sym = str(symbol or "").strip().upper()
            if not sym:
                continue

            snapshot = snapmap.get(sym) if isinstance(snapmap, dict) else None
            if not isinstance(snapshot, dict):
                snapshot = get_snapshot_cached(sym)

            df = get_candles_cached(sym, limit=100)
            candles = _bars_payload_from_candles(df, limit=100)
            if not candles or len(candles) < 50:
                await asyncio.sleep(0.25)
                continue
            if len(candles) > 100:
                candles = candles[-100:]

            if not isinstance(snapshot, dict):
                last_close = None
                try:
                    last_close = float(candles[-1].get("c")) if candles[-1].get("c") is not None else None
                except Exception:
                    last_close = None
                snapshot = {
                    "dailyBar": {"c": last_close},
                    "prevDailyBar": {},
                    "latestQuote": {},
                    "latestTrade": {"p": last_close},
                }

            try:
                ta0 = compute_technical_indicators(candles)
            except Exception:
                await asyncio.sleep(0.25)
                continue

            rsi = _rsi_14_from_candles(candles)
            trend_strength = _trend_strength_from_candles(candles)
            try:
                if not (float(rsi_lo) <= float(rsi) <= float(rsi_hi)):
                    await asyncio.sleep(0.25)
                    continue
            except Exception:
                await asyncio.sleep(0.25)
                continue
            try:
                if float(trend_strength) < float(trend_strength_min):
                    await asyncio.sleep(0.25)
                    continue
            except Exception:
                await asyncio.sleep(0.25)
                continue

            mom = _clamp_0_100(ta0.get("momentum"))
            vol0 = _clamp_0_100(ta0.get("volatility"))
            if not ((mom >= float(momentum_min)) or (vol0 >= float(volatility_min))):
                await asyncio.sleep(0.25)
                continue

            news_sentiment = _sentiment_proxy_from_snapshot(snapshot)
            volume_score = _score_volume_0_100_from_snapshot(snapshot)
            composite_score = _clamp_0_100(
                (0.35 * mom)
                + (0.25 * _clamp_0_100(ta0.get("trend")))
                + (0.15 * _sentiment_score_0_100(news_sentiment))
                + (0.15 * float(volume_score))
                + (0.10 * vol0)
            )

            outc.append(
                {
                    "symbol": sym,
                    "composite_score": float(composite_score),
                    "technical_analysis": ta0,
                    "news_sentiment": news_sentiment,
                    "snapshot": snapshot,
                    "bars": candles,
                    "volume_score": float(volume_score),
                }
            )

            if (time.time() - t0) > 18.0:
                break
            await asyncio.sleep(0.25)
        return outc

    candidates = await _scan_pass(rsi_lo=35.0, rsi_hi=75.0, trend_strength_min=40.0, momentum_min=55.0, volatility_min=60.0)
    if len(candidates) < 5:
        # Auto-relax tier2/tier3 thresholds by 30%
        candidates = await _scan_pass(
            rsi_lo=(35.0 * 0.70),
            rsi_hi=min(90.0, 75.0 * 1.30),
            trend_strength_min=(40.0 * 0.70),
            momentum_min=(55.0 * 0.70),
            volatility_min=(60.0 * 0.70),
        )

    if not candidates:
        return {
            "best": {
                "symbol": "SPY",
                "composite_score": 0.0,
                "bars": [],
                "news_sentiment": {"direction": "NEUTRAL", "summary": "Unavailable", "score_100": 50},
            },
            "alternates": [],
            "universe": symbols,
            "market": market,
        }

    candidates.sort(key=lambda x: float(x.get("composite_score") or 0.0), reverse=True)
    best = candidates[0]
    alternates = candidates[1:3]
    return {"best": best, "alternates": alternates, "universe": symbols, "market": market}


_BEST_PICK_CACHE: Dict[str, Any] = {"ts": 0.0, "resp": None}
_BEST_PICK_PERSIST: Dict[str, Any] = {"ts": 0.0, "resp": None}
_LAST_V2_WATCHLIST: Dict[str, Any] = {"ts": 0.0, "candidates": []}


def _bg_v2_scan_once() -> None:
    """Run _scan_best_pick_v2 in a fresh event loop and persist watchlist_candidates."""
    try:
        import asyncio as _aio

        async def _run() -> None:
            try:
                # Force a fresh full-universe fetch — expire the cache so get_scan_universe
                # pulls all assets from Alpaca instead of returning the startup seed.
                _SCAN_UNIVERSE_CACHE["ts"] = 0.0
                universe = await _aio.to_thread(get_scan_universe, 3000)
            except Exception:
                universe = []
            if not universe:
                universe = ["SPY", "QQQ", "AAPL", "NVDA", "MSFT"]

            def _nf(sym: str) -> Dict[str, Any]:
                try:
                    return _news_and_sentiment(str(sym or "").strip().upper(), allow_llm=False)
                except Exception:
                    return {}

            out = await _scan_best_pick_v2(
                universe=universe,
                news_fetcher=_nf,
                allow_llm_news=True,
                max_seconds=1200.0,
                news_top_k=25,
            )
            if isinstance(out, dict):
                cands = out.get("watchlist_candidates") or []
                if isinstance(cands, list) and cands:
                    _LAST_V2_WATCHLIST["ts"] = float(time.time())
                    _LAST_V2_WATCHLIST["candidates"] = list(cands)

                # Auto-record the top watchlist candidate into performance picks.
                # Use the best pick (out) if it's a trade, otherwise fall back to
                # the #1 watchlist candidate.
                try:
                    from performance_tracker import record_pick as _record_pick
                    if bool(out.get("is_trade")) and str(out.get("symbol") or "").strip():
                        _rp_result = _record_pick(out)
                        # Fire new-pick alert only for fresh inserts (not duplicate suppression)
                        if isinstance(_rp_result, int):
                            try:
                                from alerts import send_new_pick_alert_bg
                                send_new_pick_alert_bg(out)
                            except Exception as _ae:
                                log.warning(f"bg_scan: new_pick alert failed: {_ae}")
                    elif isinstance(cands, list) and cands:
                        top = cands[0]
                        _record_pick({
                            "symbol": top.get("symbol") or "",
                            "trade_plan": {},
                            "edge_signals": top.get("edge_signals") or [],
                            "edge_score_0_10": top.get("premover") or 0.0,
                            "final_score_0_10": top.get("final_score") or 0.0,
                            "confidence_0_10": top.get("confidence") or 0.0,
                            "premover_score_0_10": top.get("premover") or 0.0,
                        })
                except Exception as _rp_err:
                    log.warning(f"bg_scan: record_pick failed: {_rp_err}")

        _aio.run(_run())
    except Exception as _e:
        try:
            log.warning(f"Background v2 scan error: {_e}")
        except Exception:
            pass


def _bg_v2_scan_loop() -> None:
    """Run once, then reschedule every 4 hours."""
    _bg_v2_scan_once()
    _t = threading.Timer(4 * 3600, _bg_v2_scan_loop)
    _t.daemon = True
    _t.start()


# ---------------------------------------------------------------------------
# Pre-mover background scan
# ---------------------------------------------------------------------------

def _bg_premover_scan_once() -> None:
    """Run the small-cap pre-mover scan and persist results to module cache."""
    try:
        from pre_mover_scanner import run_premover_scan, set_cached_premover_results
        from performance_tracker import record_pick as _record_pick

        universe = get_scan_universe(3000)
        result = run_premover_scan(
            scan_universe=universe,
            max_results=25,
            news_top_k=50,
            max_seconds=300.0,
        )
        if isinstance(result, dict) and result.get("results"):
            set_cached_premover_results(result)

            # Auto-record the top pick if score > 75
            top = result["results"][0] if result["results"] else None
            if top and float(top.get("score") or 0.0) >= 75.0:
                try:
                    _record_pick({
                        "symbol": top.get("symbol") or "",
                        "trade_plan": {"entry": top.get("entry_zone"), "stop": top.get("invalidation")},
                        "edge_signals": [k for k in (top.get("signals") or {})],
                        "edge_score_0_10": float(top.get("score") or 0.0) / 10.0,
                        "final_score_0_10": float(top.get("score") or 0.0) / 10.0,
                        "confidence_0_10": float(top.get("score") or 0.0) / 10.0,
                        "premover_score_0_10": float(top.get("score") or 0.0) / 10.0,
                    })
                except Exception as _rp_err:
                    log.warning(f"bg_premover: record_pick failed: {_rp_err}")
    except Exception as _e:
        log.warning(f"bg_premover_scan error: {_e}")


def _bg_premover_scan_loop() -> None:
    """Run once every 2 hours (small-cap setups don't change minute-to-minute)."""
    _bg_premover_scan_once()
    _t = threading.Timer(2 * 3600, _bg_premover_scan_loop)
    _t.daemon = True
    _t.start()


def _bg_brain_outcome_loop() -> None:
    """
    Every 6 hours: fetch current prices for pending picks, record outcomes,
    and trigger weight recalibration so the scanner keeps learning.
    """
    try:
        from brain import run_outcome_checks
        n = run_outcome_checks()
        log.info(f"brain_outcome_loop: checked {n} outcomes")
    except Exception as e:
        log.warning(f"brain_outcome_loop error: {e}")
    try:
        from performance_tracker import evaluate_pending_picks
        n2 = evaluate_pending_picks()
        log.info(f"brain_outcome_loop: evaluated {n2} perf_tracker picks")
    except Exception as e:
        log.warning(f"brain_outcome_loop perf_tracker error: {e}")
    finally:
        _t = threading.Timer(6 * 3600, _bg_brain_outcome_loop)
        _t.daemon = True
        _t.start()


class BestPickResponse(BaseModel):
    status: str = "ok"
    reason: str = ""
    symbol: str = ""
    score: Optional[float] = None
    score_0_100: Optional[float] = None
    ai_score_0_100: Optional[float] = None
    execution_score_0_100: Optional[float] = None
    analyze_ai_score_0_100: Optional[float] = None
    analyze_execution_score_0_100: Optional[float] = None
    analyze_rating_0_100: Optional[float] = None
    confidence_0_100: Optional[int] = None
    direction: str = "neutral"
    last_price: Optional[float] = None
    percent_change: Optional[float] = None
    reasoning_available: bool = False
    classification: str = ""
    confidence: Optional[float] = None
    ai_score: Optional[float] = None
    execution_score: Optional[float] = None
    trade_plan: Dict[str, Any] = Field(default_factory=dict)
    execution_plan: Dict[str, Any] = Field(default_factory=dict)
    technical_analysis: Dict[str, Any] = Field(default_factory=dict)
    news_sentiment: Dict[str, Any] = Field(default_factory=dict)
    why: List[str] = Field(default_factory=list)
    what_confirms: List[str] = Field(default_factory=list)
    what_breaks: List[str] = Field(default_factory=list)
    when_to_recheck: str = ""
    next_check_timing: str = ""


class BestPickV2Response(BaseModel):
    symbol: str = ""
    type: str = ""
    ai_score_0_10: float = 1.0
    execution_score_0_10: float = 1.0
    confidence_0_100: int = 0
    confidence_definition: str = "P(+1.5R before -1R in 7D)"
    high_grade: bool = False
    low_conviction: bool = False
    low_conviction_note: str = ""
    log_llm_enabled: bool = False
    candidates_scored: int = 0
    candidates_passing_threshold: int = 0
    candidates_skipped_data: int = 0
    trade_plan: Dict[str, Any] = Field(default_factory=dict)
    catalysts: List[str] = Field(default_factory=list)
    risk_flags: List[str] = Field(default_factory=list)
    pillar_scores_0_10: Dict[str, float] = Field(default_factory=dict)
    watchlist_candidates: List[Dict[str, Any]] = Field(default_factory=list)
    market_regime: Optional[str] = None
    is_trade: Optional[bool] = None
    trade_decision: Optional[str] = None
    no_trade_reason: Optional[str] = None
    edge_signals: List[str] = Field(default_factory=list)
    position_size_pct: Optional[float] = None
    upgrade_for_levels: Optional[bool] = None


def _best_pick_contract(x: Any) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if isinstance(x, dict):
        out = dict(x)

    out.setdefault("symbol", "")
    out.setdefault("ai_score_0_100", None)
    out.setdefault("execution_score_0_100", None)
    out.setdefault("score_0_100", None)
    out.setdefault("analyze_ai_score_0_100", None)
    out.setdefault("analyze_execution_score_0_100", None)
    out.setdefault("analyze_rating_0_100", None)
    out.setdefault("confidence_0_100", None)
    out.setdefault("trade_plan", {})
    out.setdefault("execution_plan", {})

    # Back-compat with older cached payloads.
    try:
        if out.get("ai_score_0_100") is None and out.get("ai_score") is not None:
            out["ai_score_0_100"] = float(out.get("ai_score"))
    except Exception:
        pass
    try:
        if out.get("execution_score_0_100") is None and out.get("execution_score") is not None:
            out["execution_score_0_100"] = float(out.get("execution_score"))
    except Exception:
        pass
    try:
        if out.get("confidence_0_100") is None and out.get("confidence") is not None:
            v = float(out.get("confidence"))
            if 0.0 <= v <= 1.0:
                out["confidence_0_100"] = int(round(v * 100.0))
    except Exception:
        pass

    if not isinstance(out.get("trade_plan"), dict):
        out["trade_plan"] = {}
    if not isinstance(out.get("execution_plan"), dict):
        out["execution_plan"] = {}
    return out


@app.get("/best_pick", response_model=BestPickResponse)
async def best_pick(
    max_scan: int = 200,
    refresh: bool = False,
    tz: Optional[str] = Query(None),
    stream: bool = Query(False),
    min_score_0_100: int = Query(85, ge=0, le=100),
):
    _ = max_scan
    _ = stream

    # Scan a liquid universe; allow larger scanning via max_scan (capped).
    universe = get_scan_universe() or ["SPY"]
    try:
        cap = int(max_scan) if max_scan is not None else len(universe)
    except Exception:
        cap = len(universe)
    cap = max(50, min(3000, cap))
    try:
        universe = list(universe)[:cap]
    except Exception:
        pass

    # Hard floor: never return a Best Pick below this threshold, regardless of query params.
    try:
        hard_floor = int(os.getenv("BEST_PICK_HARD_MIN_SCORE_0_100", "80") or 80)
    except Exception:
        hard_floor = 80
    hard_floor = max(0, min(100, hard_floor))
    try:
        requested = int(min_score_0_100) if min_score_0_100 is not None else 85
    except Exception:
        requested = 85
    effective_min_score_0_100 = max(int(hard_floor), int(max(0, min(100, requested))))

    # Small TTL cache to meet <3s cached target.
    try:
        if not bool(refresh):
            cached = _BEST_PICK_CACHE.get("resp") if isinstance(_BEST_PICK_CACHE, dict) else None
            ts = float(_BEST_PICK_CACHE.get("ts") or 0.0) if isinstance(_BEST_PICK_CACHE, dict) else 0.0
            if isinstance(cached, dict) and cached.get("symbol") and (time.time() - ts) <= 3.0:
                try:
                    sc = float(cached.get("score_0_100")) if cached.get("score_0_100") is not None else float(cached.get("ai_score_0_100") or 0.0)
                except Exception:
                    sc = 0.0
                if sc >= float(effective_min_score_0_100):
                    return _best_pick_contract(cached)
    except Exception:
        pass

    # Persistent cache: return last scan result even if TTL cache expired.
    try:
        if not bool(refresh):
            cached_p = _BEST_PICK_PERSIST.get("resp") if isinstance(_BEST_PICK_PERSIST, dict) else None
            if isinstance(cached_p, dict) and cached_p.get("symbol"):
                try:
                    scp = float(cached_p.get("score_0_100")) if cached_p.get("score_0_100") is not None else float(cached_p.get("ai_score_0_100") or 0.0)
                except Exception:
                    scp = 0.0
                if scp >= float(effective_min_score_0_100):
                    return _best_pick_contract(cached_p)
    except Exception:
        pass

    async def _run_scan() -> Dict[str, Any]:
        try:
            max_s = float(os.getenv("BEST_PICK_SCAN_MAX_SECONDS", "8.0") or 8.0)
        except Exception:
            max_s = 8.0
        try:
            # Scale time budget slightly with universe size; still bounded.
            max_s = max(6.0, min(18.0, max_s + (0.004 * float(len(universe) or 0))))
        except Exception:
            max_s = max_s
        return await _scan_universe_ranked(universe=universe, max_seconds=float(max_s))

    try:
        scan = await asyncio.wait_for(_run_scan(), timeout=12.0)

        best = scan.get("best") if isinstance(scan, dict) else None
        if not isinstance(best, dict) or not best.get("symbol"):
            best = {"symbol": "SPY", "pick_score": 0.0, "factors": {}}

        sym = str(best.get("symbol") or "SPY").strip().upper()
        snapshot = best.get("snapshot") if isinstance(best.get("snapshot"), dict) else None
        daily_bars = best.get("daily_bars") if isinstance(best.get("daily_bars"), list) else []
        intraday_bars = best.get("intraday_bars") if isinstance(best.get("intraday_bars"), list) else []

        # Market-data derived fields
        last_price = None
        try:
            last_price = float(_last_price_from_snapshot(snapshot)) if snapshot is not None else None
        except Exception:
            last_price = None
        percent_change = _pct_change_from_snapshot(snapshot)

        # Trade plan via existing engine (same as analyze())
        out: Dict[str, Any] = {
            "status": "ok",
            "reason": "",
            "symbol": sym,
            "score": float(round(_clamp01(best.get("pick_score")) * 10.0, 2)),
            "score_0_100": float(round(_clamp01(best.get("pick_score")) * 100.0, 1)),
            "ai_score_0_100": float(round(_clamp01(best.get("pick_score")) * 100.0, 1)),
            "execution_score_0_100": None,
            "analyze_ai_score_0_100": None,
            "analyze_execution_score_0_100": None,
            "analyze_rating_0_100": None,
            "confidence_0_100": int(round(_clamp01(best.get("pick_score")) * 100.0)),
            "direction": "neutral",
            "last_price": (float(last_price) if last_price is not None else None),
            "percent_change": (float(percent_change) if percent_change is not None else None),
            "reasoning_available": bool(os.getenv("OPENAI_API_KEY")),
            "classification": "",
            "confidence": float(round(_clamp01(best.get("pick_score")), 4)),
            "ai_score": float(round(_clamp01(best.get("pick_score")) * 100.0, 1)),
            "execution_score": None,
            "trade_plan": {},
            "execution_plan": {},
            "technical_analysis": {},
            "news_sentiment": {},
            "why": [],
            "what_confirms": [],
            "what_breaks": [],
            "when_to_recheck": "",
            "next_check_timing": "",
        }

        try:
            ta0 = compute_technical_indicators(daily_bars) if isinstance(daily_bars, list) else {}
            out["technical_analysis"] = ta0 if isinstance(ta0, dict) else {}
        except Exception:
            out["technical_analysis"] = {}

        # Separate /analyze-style rating (different from pick_score) so both can be used together.
        try:
            ind = out.get("technical_analysis") if isinstance(out.get("technical_analysis"), dict) else {}
            ns0 = out.get("news_sentiment") if isinstance(out.get("news_sentiment"), dict) else {}
            ns100 = float(_sentiment_score_0_100(ns0) or 50.0)
            _regime = "neutral"
            try:
                _regime = _get_market_regime()
            except Exception:
                pass
            a_ai = float(_score_composite_0_100(indicators=ind, news_sentiment_0_100=ns100, regime=_regime) or 0.0)
            a_ex = float(_score_execution_0_100(indicators=ind) or 0.0)
            out["analyze_ai_score_0_100"] = float(round(a_ai, 1))
            out["analyze_execution_score_0_100"] = float(round(a_ex, 1))
            out["analyze_rating_0_100"] = float(round((0.65 * a_ai) + (0.35 * a_ex), 1))
        except Exception:
            out["analyze_ai_score_0_100"] = None
            out["analyze_execution_score_0_100"] = None
            out["analyze_rating_0_100"] = None

        try:
            out["execution_score_0_100"] = float(round(float(_score_execution_0_100(indicators=(out.get("technical_analysis") if isinstance(out.get("technical_analysis"), dict) else {})) or 0.0), 1))
        except Exception:
            out["execution_score_0_100"] = None
        try:
            if out.get("execution_score_0_100") is not None:
                out["execution_score"] = float(out.get("execution_score_0_100"))
        except Exception:
            pass

        def _clean_lines(xs: Any, *, limit: int = 6) -> List[str]:
            if not isinstance(xs, list):
                return []
            outl: List[str] = []
            for x in xs:
                s = str(x or "").strip()
                if not s:
                    continue
                outl.append(s)
                if len(outl) >= int(limit or 6):
                    break
            return outl

        # Use the same news+sentiment engine as /analyze for consistency with the UI.
        ns_obj: Dict[str, Any] = {}
        try:
            allow_llm_news = str(os.getenv("BEST_PICK_NEWS_ALLOW_LLM", "1") or "1").strip().lower() in ("1", "true", "yes", "on")
        except Exception:
            allow_llm_news = True
        try:
            n0 = await asyncio.to_thread(_news_and_sentiment, sym, allow_llm=bool(allow_llm_news))
            if isinstance(n0, dict):
                try:
                    score_v = int(float(n0.get("score") or 0)) if n0.get("score") is not None else 0
                except Exception:
                    score_v = 0
                try:
                    conf_v = int(float(n0.get("confidence") or 0)) if n0.get("confidence") is not None else (15 if not (n0.get("items") or []) else 35)
                except Exception:
                    conf_v = 15
                ns_obj = {
                    "direction": str(n0.get("direction") or "NEUTRAL").strip().upper(),
                    "summary": str(n0.get("summary") or "Low news volume. Sentiment confidence reduced.").strip()[:420],
                    "score": score_v,
                    "confidence": conf_v,
                    "catalysts": n0.get("catalysts") if isinstance(n0.get("catalysts"), list) else [],
                    "headlines": [
                        str(it.get("title") or it.get("headline") or "").strip()[:240]
                        for it in (n0.get("items") or [])
                        if isinstance(it, dict) and str(it.get("title") or it.get("headline") or "").strip()
                    ][:10],
                }
        except Exception:
            ns_obj = {}

        if not isinstance(ns_obj, dict) or not str(ns_obj.get("summary") or "").strip():
            # Keep a backend-safe fallback; never blank.
            try:
                ns_obj = _sentiment_proxy_from_snapshot(snapshot) if snapshot is not None else {"direction": "NEUTRAL", "summary": "Unavailable", "score_100": 50}
            except Exception:
                ns_obj = {"direction": "NEUTRAL", "summary": "Unavailable", "score_100": 50}
            try:
                if not str(ns_obj.get("summary") or "").strip():
                    ns_obj["summary"] = "Unavailable"
            except Exception:
                pass

        out["news_sentiment"] = ns_obj

        # Now that news_sentiment exists, compute analyze_rating again (it uses sentiment).
        try:
            ind = out.get("technical_analysis") if isinstance(out.get("technical_analysis"), dict) else {}
            ns0 = out.get("news_sentiment") if isinstance(out.get("news_sentiment"), dict) else {}
            ns100 = float(_sentiment_score_0_100(ns0) or 50.0)
            _regime = "neutral"
            try:
                _regime = _get_market_regime()
            except Exception:
                pass
            a_ai = float(_score_composite_0_100(indicators=ind, news_sentiment_0_100=ns100, regime=_regime) or 0.0)
            a_ex = float(_score_execution_0_100(indicators=ind) or 0.0)
            out["analyze_ai_score_0_100"] = float(round(a_ai, 1))
            out["analyze_execution_score_0_100"] = float(round(a_ex, 1))
            out["analyze_rating_0_100"] = float(round((0.65 * a_ai) + (0.35 * a_ex), 1))
        except Exception:
            pass

        # Enforce threshold (with hard floor): only return picks at/above effective_min_score_0_100.
        try:
            sc0 = float(out.get("score_0_100")) if out.get("score_0_100") is not None else float(out.get("ai_score_0_100") or 0.0)
        except Exception:
            sc0 = 0.0
        if sc0 < float(effective_min_score_0_100):
            return {
                "status": "no_pick",
                "reason": "below_threshold",
                "symbol": "",
                "score": float(round(sc0 / 10.0, 2)),
                "score_0_100": float(round(sc0, 1)),
                "ai_score_0_100": float(round(sc0, 1)),
                "execution_score_0_100": out.get("execution_score_0_100"),
                "analyze_ai_score_0_100": out.get("analyze_ai_score_0_100"),
                "analyze_execution_score_0_100": out.get("analyze_execution_score_0_100"),
                "analyze_rating_0_100": out.get("analyze_rating_0_100"),
                "confidence_0_100": out.get("confidence_0_100"),
                "direction": "neutral",
                "last_price": out.get("last_price"),
                "percent_change": out.get("percent_change"),
                "reasoning_available": out.get("reasoning_available"),
                "classification": "",
                "confidence": out.get("confidence"),
                "ai_score": out.get("ai_score"),
                "execution_score": out.get("execution_score"),
                "trade_plan": {},
                "execution_plan": {},
                "technical_analysis": out.get("technical_analysis") if isinstance(out.get("technical_analysis"), dict) else {},
                "news_sentiment": out.get("news_sentiment") if isinstance(out.get("news_sentiment"), dict) else {},
                "why": [],
                "what_confirms": [],
                "what_breaks": [],
                "when_to_recheck": "",
                "next_check_timing": "",
            }

        # Deterministic reasoning fallback (LLM may be disabled). Prevent blank UI lines.
        try:
            ta = out.get("technical_analysis") if isinstance(out.get("technical_analysis"), dict) else {}
            mom = _safe_f(ta.get("momentum"))
            tr = _safe_f(ta.get("trend"))
            vol = _safe_f(ta.get("volatility"))
            liq = _safe_f(ta.get("liquidity"))
        except Exception:
            mom = tr = vol = liq = None

        ns_dir = "NEUTRAL"
        try:
            ns_dir = str((ns_obj or {}).get("direction") or "NEUTRAL").strip().upper()
        except Exception:
            ns_dir = "NEUTRAL"

        why_lines: List[str] = []
        conf_lines: List[str] = []
        break_lines: List[str] = []
        try:
            if mom is not None:
                why_lines.append(f"Momentum score {int(round(float(mom)))} / 100 with volume trend x1.0.")
                if float(mom) >= 65:
                    conf_lines.append("Strong momentum supports continuation.")
                elif float(mom) <= 40:
                    break_lines.append("Weak momentum increases reversal risk.")
            if tr is not None:
                why_lines.append("Trade plan is risk-defined around VWAP/ATR with clear invalidation.")
                if float(tr) >= 60:
                    conf_lines.append("Trend strength supports the setup.")
                elif float(tr) <= 40:
                    break_lines.append("Trend is weak; setup may fail.")
            if liq is not None and float(liq) <= 35:
                break_lines.append("Liquidity is thin; slippage risk elevated.")
            if vol is not None and float(vol) >= 70:
                conf_lines.append("Volatility is elevated; breakout follow-through is possible (manage risk).")
            if ns_dir == "BULLISH":
                why_lines.append("News sentiment currently Bullish.")
            elif ns_dir == "BEARISH":
                why_lines.append("News sentiment currently Bearish.")
            else:
                why_lines.append("News sentiment currently Neutral.")
        except Exception:
            pass

        # Hard defaults so the card never shows blank rows.
        if not why_lines:
            why_lines = ["Selected as the top-ranked symbol based on momentum/trend/liquidity factors."]
        if not conf_lines:
            conf_lines = ["Price reclaim and hold above VWAP supports the setup.", "Volume expansion confirms demand."]
        if not break_lines:
            break_lines = ["Loss of VWAP invalidates the setup.", "Failed breakout / lower-high pattern is a warning."]

        out["why"] = _clean_lines(why_lines, limit=4)
        out["what_confirms"] = _clean_lines(conf_lines, limit=4)
        out["what_breaks"] = _clean_lines(break_lines, limit=4)

        # When to recheck (UI field). Keep timezone-aware and never blank.
        try:
            wtr = _format_recheck_local(hour_et=9, minute_et=45, user_tz=tz, days_ahead=1)
        except Exception:
            wtr = "Next session"
        if not str(wtr or "").strip():
            wtr = "Next session"
        out["when_to_recheck"] = str(wtr)
        out["next_check_timing"] = str(wtr)

        try:
            if daily_bars:
                rec = _deterministic_trade_plan(symbol=sym, daily_bars=daily_bars, intraday_bars=intraday_bars, indicators=(out.get("technical_analysis") if isinstance(out.get("technical_analysis"), dict) else {}))
                resistance = _safe_f(rec.get("recent_high"))
            else:
                resistance = None
        except Exception:
            resistance = None

        try:
            md = _market_data_from_snapshot_and_bars(symbol=sym, snapshot=snapshot, daily_bars=daily_bars, intraday_bars=intraday_bars)
        except Exception:
            md = {}
        try:
            atr14 = _safe_f(md.get("atr14"))
            vwap = _safe_f(md.get("vwap"))
            last_px = _safe_f(md.get("last_price"))
        except Exception:
            atr14, vwap, last_px = None, None, None

        try:
            if last_price is None and last_px is not None:
                out["last_price"] = float(last_px)
        except Exception:
            pass

        if resistance is None and out.get("last_price") is not None:
            try:
                resistance = float(out.get("last_price"))
            except Exception:
                resistance = 0.0

        try:
            out["trade_plan"] = _trade_plan_from_spec(
                last_price=float(out.get("last_price") or 0.0),
                atr14=float(atr14 or 0.0),
                vwap=float(vwap or 0.0),
                resistance=float(resistance or 0.0),
            )
        except Exception:
            out["trade_plan"] = {}

        try:
            out["execution_plan"] = generate_execution_plan(
                sym,
                volatility=float((out.get("technical_analysis") or {}).get("volatility") or 0.0),
                trend_strength=float((out.get("technical_analysis") or {}).get("trend") or 0.0),
            )
        except Exception:
            out["execution_plan"] = {}

        # Cache result for fast subsequent calls.
        try:
            _BEST_PICK_CACHE["ts"] = float(time.time())
            _BEST_PICK_CACHE["resp"] = dict(out)
        except Exception:
            pass

        # Persist last scan result server-side.
        try:
            _BEST_PICK_PERSIST["ts"] = float(time.time())
            _BEST_PICK_PERSIST["resp"] = dict(out)
        except Exception:
            pass

        return out

    except asyncio.TimeoutError:
        try:
            log.warning("best_pick timeout triggered — returning cached pick")
        except Exception:
            pass
        cached = None
        try:
            cached = _BEST_PICK_CACHE.get("resp")
        except Exception:
            cached = None
        if isinstance(cached, dict) and cached.get("symbol"):
            return _best_pick_contract(cached)

        # Degraded fallback (SPY)
        return {
            "status": "degraded",
            "reason": "best_pick_timeout",
            "symbol": "SPY",
            "classification": "",
            "confidence": None,
            "ai_score_0_100": None,
            "execution_score_0_100": None,
            "confidence_0_100": None,
            "ai_score": None,
            "execution_score": None,
            "trade_plan": {},
            "execution_plan": {},
            "technical_analysis": {},
            "news_sentiment": {"direction": "NEUTRAL", "summary": "Unavailable", "score_100": 50},
            "why": [],
            "what_confirms": [],
            "what_breaks": [],
        }
    except Exception:
        try:
            log.warning("best_pick scan failed — returning SPY fallback")
        except Exception:
            pass
        return {
            "status": "degraded",
            "reason": "best_pick_scan_failed",
            "symbol": "SPY",
            "classification": "",
            "confidence": None,
            "ai_score_0_100": None,
            "execution_score_0_100": None,
            "confidence_0_100": None,
            "ai_score": None,
            "execution_score": None,
            "trade_plan": {},
            "execution_plan": {},
            "technical_analysis": {},
            "news_sentiment": {"direction": "NEUTRAL", "summary": "Unavailable", "score_100": 50},
            "why": [],
            "what_confirms": [],
            "what_breaks": [],
        }


@app.get("/best-pick", response_model=BestPickResponse)
async def best_pick_alias(max_scan: int = 200, refresh: bool = False, tz: Optional[str] = Query(None), stream: bool = Query(False)):
    return await best_pick(max_scan=max_scan, refresh=refresh, tz=tz, stream=stream)


def _user_field(user, *keys):
    """Read a field from a sqlite3.Row or dict safely."""
    for k in keys:
        try:
            v = user[k]
            if v is not None:
                return v
        except Exception:
            pass
    return None


def _check_starter_weekly_limit(user) -> None:
    """Raise 429 if a Starter user has used 3 picks this Mon-Sun UTC week."""
    import sqlite3 as _sq3
    from datetime import datetime, timezone, timedelta

    plan = str(_user_field(user, "plan") or "free").lower()
    if plan not in ("starter",):
        return  # Free/Pro/Elite: not subject to starter weekly limit

    user_id = _user_field(user, "id")
    if not user_id:
        return

    today = datetime.now(timezone.utc).date()
    week_start = today - timedelta(days=today.weekday())  # Monday of current UTC week
    week_key = week_start.isoformat()  # e.g. "2026-06-09"
    db_path = os.getenv("AUTH_DB_PATH", "/app/data/auth.db")

    with _sq3.connect(db_path, timeout=10) as conn:
        conn.row_factory = _sq3.Row
        row = conn.execute(
            "SELECT count FROM pick_usage WHERE user_id = ? AND date = ?",
            (user_id, week_key),
        ).fetchone()
        if row and row["count"] >= 3:
            raise HTTPException(
                status_code=429,
                detail={"detail": "daily_limit_reached", "limit": 3, "plan": "starter", "upgrade_to": "pro"},
            )
        conn.execute(
            """INSERT INTO pick_usage (user_id, date, count) VALUES (?, ?, 1)
               ON CONFLICT(user_id, date) DO UPDATE SET count = count + 1""",
            (user_id, week_key),
        )
        conn.commit()


@app.get("/best_pick_v2", response_model=BestPickV2Response)
async def best_pick_v2(
    max_scan: int = 1500,
    refresh: bool = False,
    allow_llm_news: bool = True,
    full_universe: bool = False,
    _user=_dep_starter,
):
    _ = refresh
    _ = full_universe
    _check_starter_weekly_limit(_user)
    try:
        universe = await asyncio.to_thread(get_scan_universe, int(max_scan or 1200))
    except Exception as e:
        try:
            log.exception(f"best_pick_v2: get_scan_universe failed: {e}")
        except Exception:
            pass
        universe = ["SPY"]

    try:
        news_top_k = int(os.getenv("BEST_PICK_V2_NEWS_TOPK", "25") or 25)
    except Exception:
        news_top_k = 25
    news_top_k = max(0, min(50, news_top_k))

    try:
        max_s = float(os.getenv("BEST_PICK_V2_SCAN_MAX_SECONDS", "25.0") or 25.0)
    except Exception:
        max_s = 25.0
    max_s = max(10.0, min(60.0, max_s))

    def _news_fetcher(sym: str) -> Dict[str, Any]:
        try:
            return _news_and_sentiment(str(sym or "").strip().upper(), allow_llm=bool(allow_llm_news))
        except Exception:
            return {}

    try:
        start = time.time()
        out = await _scan_best_pick_v2(
            universe=universe,
            news_fetcher=_news_fetcher,
            allow_llm_news=bool(allow_llm_news),
            max_seconds=float(max_s),
            news_top_k=int(news_top_k),
        )
        try:
            log.info({"best_pick_v2_elapsed": float(time.time() - start), "max_scan": int(max_scan or 0), "max_seconds": float(max_s)})
        except Exception:
            pass
        try:
            wl_cands = out.get("watchlist_candidates") if isinstance(out, dict) else None
            if isinstance(wl_cands, list) and wl_cands:
                _LAST_V2_WATCHLIST["ts"] = float(time.time())
                _LAST_V2_WATCHLIST["candidates"] = list(wl_cands)
        except Exception:
            pass
    except Exception:
        out = {
            "symbol": "AAPL",
            "type": "STOCK",
            "ai_score_0_10": 1.0,
            "execution_score_0_10": 1.0,
            "confidence_0_100": 5,
            "confidence_definition": "P(+1.5R before -1R in 7D)",
            "high_grade": False,
            "low_conviction_note": "Low-conviction environment — defensive positioning preferred.",
            "trade_plan": {},
            "catalysts": [],
            "risk_flags": ["scan_failed"],
            "pillar_scores_0_10": {"technical": 1.0, "catalyst": 1.0, "sentiment": 1.0, "risk_structure": 1.0, "upside": 1.0},
        }

    if not isinstance(out, dict):
        out = {"symbol": "AAPL", "type": "STOCK"}
    out.setdefault("symbol", "AAPL")
    out.setdefault("type", "STOCK")
    out.setdefault("ai_score_0_10", 1.0)
    out.setdefault("execution_score_0_10", 1.0)
    out.setdefault("confidence_0_100", 5)
    out.setdefault("confidence_definition", "P(+1.5R before -1R in 7D)")
    out.setdefault("high_grade", False)
    out.setdefault("low_conviction_note", "")
    out.setdefault("trade_plan", {})
    out.setdefault("catalysts", [])
    out.setdefault("risk_flags", [])
    out.setdefault("pillar_scores_0_10", {"technical": 1.0, "catalyst": 1.0, "sentiment": 1.0, "risk_structure": 1.0, "upside": 1.0})
    out.setdefault("watchlist_candidates", [])

    # --- Dynamic position sizing (Pro/Elite only; stripped for Starter below) ---
    _ai_s100 = float(out.get("ai_score_0_10") or 0.0) * 10.0
    if _ai_s100 < 40:
        _pos_pct = 2.0
    elif _ai_s100 < 55:
        _pos_pct = 4.0
    elif _ai_s100 < 65:
        _pos_pct = 6.0
    elif _ai_s100 < 75:
        _pos_pct = 8.0
    elif _ai_s100 < 85:
        _pos_pct = 10.0
    else:
        _pos_pct = 12.0
    out["position_size_pct"] = _pos_pct

    if _ai_s100 < 55 and not out.get("low_conviction_note"):
        out["low_conviction_note"] = "Reduce size — moderate conviction"

    # --- Tier gating: Starter gets no trade levels ---
    _plan = str(_user_field(_user, "plan") or "free").lower()
    if _plan == "starter":
        out["trade_plan"] = {}
        out["pillar_scores_0_10"] = {}
        out["position_size_pct"] = None
        out["upgrade_for_levels"] = True

    return out


@app.get("/best-pick-v2", response_model=BestPickV2Response)
async def best_pick_v2_alias(max_scan: int = 400, refresh: bool = False, allow_llm_news: bool = True, _user=_dep_starter):
    return await best_pick_v2(max_scan=max_scan, refresh=refresh, allow_llm_news=allow_llm_news)


@app.post("/best_pick_v2/unlock")
async def best_pick_v2_free_unlock(
    max_scan: int = 400,
    _user=Depends(_get_current_user),
):
    """
    One-time monthly free pick for free-plan users.
    Checks server-side if they've already used their pick this month.
    If not, records usage in DB and returns the full pick.
    """
    from datetime import datetime, timezone
    import sqlite3 as _sqlite3

    user_id = _user_field(_user, "id")
    plan = str(_user_field(_user, "plan") or "free").lower()

    if plan != "free":
        raise HTTPException(status_code=400, detail="PAID_USERS_USE_BEST_PICK_V2")

    current_month = datetime.now(timezone.utc).strftime("%Y-%m")

    db_path = os.getenv("AUTH_DB_PATH", "/app/data/auth.db")
    with _sqlite3.connect(db_path, timeout=10) as conn:
        conn.row_factory = _sqlite3.Row
        row = conn.execute(
            "SELECT free_pick_month FROM users WHERE id = ?", (user_id,)
        ).fetchone()

        if not row:
            raise HTTPException(status_code=404, detail="USER_NOT_FOUND")

        if row["free_pick_month"] == current_month:
            raise HTTPException(
                status_code=403,
                detail="FREE_PICK_ALREADY_USED",
            )

        conn.execute(
            "UPDATE users SET free_pick_month = ? WHERE id = ?",
            (current_month, user_id),
        )
        conn.commit()

    # Run the scan directly — can't call best_pick_v2() internally (dependency injection won't fire)
    universe = await asyncio.to_thread(get_scan_universe, int(max_scan or 400))

    def _noop_news(sym: str):
        return {}

    out = await _scan_best_pick_v2(
        universe=universe,
        news_fetcher=_noop_news,
        allow_llm_news=False,
        max_seconds=55,
    )
    out.setdefault("watchlist_candidates", [])
    return out


@app.get("/health", include_in_schema=True)
def health():
    # Must always return quickly and never error.
    try:
        alpaca_keys_present = bool((os.getenv("ALPACA_API_KEY") or "").strip()) and bool((os.getenv("ALPACA_SECRET_KEY") or "").strip())
        openai_present = bool((os.getenv("OPENAI_API_KEY") or "").strip())
        market_data_ok = False
        market_data_reason = ""
        try:
            s = get_snapshot("SPY")
            if isinstance(s, dict) and isinstance(s.get("dailyBar"), dict):
                market_data_ok = True
        except Exception as e:
            market_data_ok = False
            try:
                market_data_reason = str(e)[:160]
            except Exception:
                market_data_reason = "snapshot_failed"

        status = "ok" if (alpaca_keys_present and market_data_ok) else "degraded"
        return {
            "status": status,
            "ts": now_iso(),
            "alpaca": {
                "keys_present": bool(alpaca_keys_present),
                "data_base": (os.getenv("ALPACA_DATA_BASE_URL") or "https://data.alpaca.markets"),
                "feed": (os.getenv("ALPACA_DATA_FEED") or "iex"),
                "market_data_ok": bool(market_data_ok),
                "reason": market_data_reason,
            },
            "openai": {"key_present": bool(openai_present)},
        }
    except Exception:
        return {"status": "ok"}


@app.get("/market_state", include_in_schema=True)
def market_state_alias() -> Dict[str, Any]:
    try:
        r = market_regime() or {}
    except Exception:
        r = {}
    # Frontend/readiness-friendly contract.
    return {
        "is_open": bool((r or {}).get("is_open")),
        "regime": str((r or {}).get("regime") or ""),
        "session": str((r or {}).get("session_context") or (r or {}).get("session") or ""),
        "updated_at": now_iso(),
    }


@app.get("/market-state", include_in_schema=False)
def market_state_alias_dash() -> Dict[str, Any]:
    return market_state_alias()


@app.get("/clock", include_in_schema=True)
def clock():
    # Frontend-friendly market clock. Must never hard-fail.
    try:
        ctx = market_context() or {}
    except Exception:
        ctx = {}
    if not isinstance(ctx, dict):
        ctx = {}
    ctx.setdefault("status", "ok")
    ctx.setdefault("ts", now_iso())
    return ctx


@app.get("/top-movers", include_in_schema=True)
def top_movers(limit: int = Query(12, ge=1, le=50)) -> Dict[str, Any]:
    try:
        raw = get_top_movers(int(limit)) or []
    except Exception:
        raw = []

    movers: List[Dict[str, Any]] = []
    for it in (raw or [])[: int(limit)]:
        if not isinstance(it, dict):
            continue
        sym = str(it.get("symbol") or "").strip().upper()
        if not sym:
            continue

        price = it.get("price")
        if price is None:
            price = it.get("last_price")
        if price is None:
            price = it.get("last")

        cp = it.get("changePercent")
        if cp is None:
            cp = it.get("percent_change")
        if cp is None:
            cp = it.get("pct_change")
        if cp is None:
            cp = it.get("change_percent")

        vol_raw = it.get("volume")
        if vol_raw is None:
            vol_raw = it.get("v")
        try:
            volume_v = int(float(vol_raw)) if vol_raw is not None else 0
        except Exception:
            volume_v = 0

        movers.append(
            {
                "symbol": sym,
                "last": (float(price) if price is not None else None),
                "price": (float(price) if price is not None else None),
                "change": (float(it.get("change")) if it.get("change") is not None else None),
                "pct_change": (float(cp) if cp is not None else None),
                "change_percent": (float(cp) if cp is not None else None),
                "volume": volume_v,
                "updated_at": now_iso(),
            }
        )

    status = "ok" if movers else "unavailable"
    reason = "" if movers else "no_live_data"
    return {"movers": movers, "timestamp": now_iso(), "status": status, "reason": reason}


@app.get("/top_movers", include_in_schema=False)
def top_movers_alias(limit: int = Query(12, ge=1, le=50)) -> Dict[str, Any]:
    return top_movers(limit=limit)


@app.get("/quote/{symbol}")
def quote(symbol: str) -> Dict[str, Any]:
    sd = _symbol_sanitize(symbol, allow_extended=False)
    sym = str(sd.get("symbol") or "").strip().upper()
    if not bool(sd.get("ok")) or not sym:
        return {
            "status": "degraded",
            "reason": "invalid_symbol",
            "symbol": sym,
            "price": None,
            "last": None,
            "prev_close": None,
            "pct_change": None,
            "change_percent": None,
            "volume": None,
            "vwap": None,
            "updated_at": now_iso(),
        }

    snapn = None
    try:
        snapn = _get_snapshot_normalized(sym)
    except Exception:
        snapn = None

    if not isinstance(snapn, dict):
        snapn = {}

    last_px = _safe_f(snapn.get("last_price"))
    prev_close = _safe_f(snapn.get("prev_close"))
    pct_change = _safe_f(snapn.get("percent_change"))
    volume = snapn.get("volume")
    vwap = _safe_f(snapn.get("vwap"))

    # Normalize percent change units: some upstream sources may return fraction (-0.05) vs percent (-5.0).
    try:
        if pct_change is not None and abs(float(pct_change)) <= 1.5:
            pct_change = float(pct_change) * 100.0
    except Exception:
        pass

    ok = bool(snapn.get("snapshot_available")) and (last_px is not None and float(last_px) > 0.0)
    out: Dict[str, Any] = {
        "status": "ok" if ok else "degraded",
        "reason": "" if ok else str(snapn.get("reason") or "snapshot_unavailable"),
        "symbol": sym,
        "price": (float(_round_px(last_px)) if last_px is not None else None),
        "last": (float(_round_px(last_px)) if last_px is not None else None),
        "prev_close": (float(_round_px(prev_close)) if prev_close is not None else None),
        "pct_change": (float(pct_change) if pct_change is not None else None),
        "change_percent": (float(pct_change) if pct_change is not None else None),
        "volume": volume,
        "vwap": (float(_round_px(vwap)) if vwap is not None else None),
        "updated_at": now_iso(),
    }

    if out.get("pct_change") is None and out.get("price") is not None and out.get("prev_close") is not None:
        try:
            p = float(out.get("price"))
            pc = float(out.get("prev_close"))
            if pc != 0:
                out["pct_change"] = float(round(((p - pc) / pc) * 100.0, 3))
                out["change_percent"] = float(out.get("pct_change"))
        except Exception:
            pass

    return out


@app.get("/quotes")
def quotes(symbols: str = ""):
    raw = str(symbols or "").strip()
    if not raw:
        return {"items": [], "updated_at": now_iso()}
    syms = [s.strip().upper() for s in raw.split(",") if s and s.strip()]
    syms = [s for s in syms if s][:50]

    out: List[Dict[str, Any]] = []
    for sym in syms:
        try:
            out.append(quote(sym))
        except HTTPException:
            out.append({"status": "degraded", "reason": "snapshot_unavailable", "symbol": sym, "updated_at": now_iso()})
        except Exception:
            out.append({"status": "degraded", "reason": "snapshot_unavailable", "symbol": sym, "updated_at": now_iso()})
    return {"items": out, "updated_at": now_iso()}


@app.get("/snapshot/{symbol}")
def snapshot(symbol: str):
    sd = _symbol_sanitize(symbol, allow_extended=False)
    sym = str(sd.get("symbol") or "").strip().upper()
    if not bool(sd.get("ok")) or not sym:
        return {
            "symbol": sym,
            "last_price": None,
            "percent_change": None,
            "volume": None,
            "vwap": None,
            "prev_close": None,
            "session": None,
            "market_status": "UNKNOWN",
            "updated_at": now_iso(),
            "snapshot_available": False,
            "reason": "invalid_symbol",
        }

    try:
        out = _get_snapshot_normalized(sym)
    except Exception:
        out = None

    if not isinstance(out, dict):
        out = {}

    out.setdefault("symbol", sym)
    out.setdefault("last_price", None)
    out.setdefault("percent_change", None)
    out.setdefault("volume", None)
    out.setdefault("vwap", None)
    out.setdefault("prev_close", None)
    out.setdefault("session", None)
    try:
        out.setdefault("market_status", market_state_safe())
    except Exception:
        out.setdefault("market_status", "UNKNOWN")
    out.setdefault("updated_at", now_iso())
    out.setdefault("snapshot_available", False)
    out.setdefault("reason", "market_data_unavailable")
    return out


def _ema_last(values: List[float], period: int) -> Optional[float]:
    try:
        if not values or period <= 1:
            return float(values[-1]) if values else None
        k = 2.0 / (float(period) + 1.0)
        ema = float(values[0])
        for v in values[1:]:
            ema = (float(v) * k) + (ema * (1.0 - k))
        return float(ema)
    except Exception:
        return None


@app.get("/trade_plan/{symbol}")
async def trade_plan(symbol: str):
    sd = _symbol_sanitize(symbol, allow_extended=False)
    sym = str(sd.get("symbol") or "").strip().upper()
    if not bool(sd.get("ok")) or not sym:
        return {"symbol": sym, "trade_plan_available": False, "reason": "invalid_symbol", "updated_at": now_iso()}

    snap = None
    try:
        snap = await asyncio.to_thread(_alpaca_get_snapshot, sym)
    except Exception:
        snap = None

    candles: List[Dict[str, Any]] = []
    intraday: List[Dict[str, Any]] = []
    try:
        bars = await asyncio.to_thread(_alpaca_get_bars, sym, "1Day", 100)
        candles = bars.get("candles") if isinstance(bars, dict) else []
    except Exception:
        candles = []
    try:
        bars_i = await asyncio.to_thread(_alpaca_get_bars, sym, "5Min", 300)
        intraday = bars_i.get("candles") if isinstance(bars_i, dict) else []
    except Exception:
        intraday = []

    md = _market_data_from_snapshot_and_bars(symbol=sym, snapshot=snap if isinstance(snap, dict) else None, daily_bars=candles, intraday_bars=intraday)
    if not bool(md.get("snapshot_available")):
        return {"symbol": sym, "trade_plan_available": False, "reason": "snapshot_unavailable", "updated_at": now_iso()}
    last_px = _safe_f(md.get("last_price"))
    if last_px is None or float(last_px) <= 0.0:
        return {"symbol": sym, "trade_plan_available": False, "reason": "missing_last_price", "updated_at": now_iso()}

    atr14 = _safe_f(md.get("atr14"))
    if atr14 is None or float(atr14) <= 0.0:
        atr14 = _atr_14_from_bars(candles)

    if atr14 is None or float(atr14) <= 0.0:
        return {"symbol": sym, "trade_plan_available": False, "reason": "missing_atr", "updated_at": now_iso()}

    entry = float(last_px)
    stop = float(entry) - float(atr14)
    r = float(entry) - float(stop)
    target1 = float(entry) + float(r)
    target2 = float(entry) + float(r) * 2.0
    target3 = float(entry) + float(r) * 3.0

    gain_pct = None
    rr = None
    try:
        if float(entry) > 0:
            gain_pct = ((float(target3) - float(entry)) / float(entry)) * 100.0
    except Exception:
        gain_pct = None
    try:
        if float(r) > 0:
            rr = (float(target2) - float(entry)) / float(r)
    except Exception:
        rr = None

    return {
        "symbol": sym,
        "trade_plan_available": True,
        "reason": "",
        "entry": float(_round_px(entry)),
        "stop": float(_round_px(stop)),
        "target1": float(_round_px(target1)),
        "target2": float(_round_px(target2)),
        "target3": float(_round_px(target3)),
        "gain_pct": float(round(float(gain_pct or 0.0), 2)) if gain_pct is not None else None,
        "risk_reward": float(round(float(rr or 0.0), 2)) if rr is not None else None,
        "updated_at": now_iso(),
    }


@app.get("/execution_plan/{symbol}")
async def execution_plan(symbol: str, timeframe: str = "swing", tz: Optional[str] = Query(None)):
    sd = _symbol_sanitize(symbol, allow_extended=False)
    sym = str(sd.get("symbol") or "").strip().upper()
    if not bool(sd.get("ok")) or not sym:
        return {"symbol": sym, "execution_plan_available": False, "reason": "invalid_symbol", "updated_at": now_iso()}

    candles: List[Dict[str, Any]] = []
    try:
        bars = await asyncio.to_thread(_alpaca_get_bars, sym, "1Day", 120)
        candles = bars.get("candles") if isinstance(bars, dict) else []
    except Exception:
        candles = []

    closes: List[float] = []
    for b in (candles[-200:] if isinstance(candles, list) else []):
        if not isinstance(b, dict):
            continue
        c = _safe_f(b.get("c"))
        if c is None:
            continue
        closes.append(float(c))

    if len(closes) < 50:
        return {"symbol": sym, "execution_plan_available": False, "reason": "insufficient_bars", "updated_at": now_iso()}

    ema20 = _ema_last(closes[-120:], 20)
    atr14 = _atr_14_from_bars(candles)
    if ema20 is None or atr14 is None or float(atr14) <= 0.0:
        return {"symbol": sym, "execution_plan_available": False, "reason": "missing_ema_or_atr", "updated_at": now_iso()}

    tf = str(timeframe or "swing").strip().lower()
    if tf not in ("swing", "intraday"):
        tf = "swing"

    tzinfo = None
    try:
        tzinfo = ZoneInfo(str(tz)) if tz else ZoneInfo("America/New_York")
    except Exception:
        tzinfo = ZoneInfo("America/New_York")

    trade_date = datetime.now(timezone.utc).astimezone(tzinfo).date().isoformat()
    entry_window = "1-3 days" if tf == "swing" else "next session"

    entry_method = "consolidation"
    try:
        last = float(closes[-1])
        if last > float(ema20) + float(atr14) * 0.25:
            entry_method = "breakout"
        elif last < float(ema20):
            entry_method = "pullback"
        else:
            entry_method = "consolidation"
    except Exception:
        entry_method = "consolidation"

    buy_low = float(ema20) - float(atr14)
    buy_high = float(ema20) + float(atr14)
    buy_zone = f"${_round_px(buy_low):.2f} – ${_round_px(buy_high):.2f}"

    # Provide both legacy + required schema keys.
    date_label = ""
    try:
        date_label = _format_exec_date_label(trade_date)
    except Exception:
        date_label = trade_date
    window_label = ""
    try:
        window_label = "9:35 – 10:15 AM" if tf == "intraday" else "1–3 days"
    except Exception:
        window_label = ""

    return {
        "symbol": sym,
        "execution_plan_available": True,
        "reason": "",
        # legacy
        "trade_date": trade_date,
        "entry_window": entry_window,
        "entry_method": entry_method,
        "buy_zone": buy_zone,
        # required
        "date": str(date_label or ""),
        "window": str(window_label or ""),
        "entry_method": str(entry_method or ""),
        "buy_zone": str(buy_zone or ""),
        "timezone": str(getattr(tzinfo, "key", "America/New_York")),
        "updated_at": now_iso(),
    }


@app.get("/watchlist/live")
def watchlist_live():
    wl = watchlist_get()
    items = wl.get("items") if isinstance(wl, dict) else []
    if not isinstance(items, list):
        items = []
    out: List[Dict[str, Any]] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        sym = str(it.get("symbol") or "").strip().upper()
        if not sym:
            continue
        q = None
        try:
            q = quote(sym)
        except Exception:
            q = {"symbol": sym, "price": None, "prev_close": None, "pct_change": None, "updated_at": now_iso()}
        out.append({"symbol": sym, "note": str(it.get("note") or "")[:240], "quote": q})
    return {"items": out, "updated_at": now_iso()}

@app.get("/recommend/top")
def recommend_top(n: int = 10):
    """Compatibility endpoint for the frontend. Never throws; returns {items: []}."""
    try:
        bp = best_pick(max_scan=200)
        items: List[Dict[str, Any]] = []
        if isinstance(bp, dict):
            pick = bp.get("pick") or bp.get("best_pick")
            if isinstance(pick, dict):
                items.append(pick)
            else:
                items.append(bp)
    except Exception:
        items = []
    return {"items": items[: max(0, int(n))], "updated_at": now_iso()}




# ----------------------------
# HARD ALIVE (CANNOT FAIL)
# ----------------------------
@app.get("/__alive__")
def alive():
    return {"status": "alive", "ts": now_iso()}

def llm_optional_explain(context: dict):
    if not os.getenv("OPENAI_API_KEY"):
        return None

    try:
        client = _get_openai_client()

        prompt = f"""
Explain this trading decision clearly and concisely.
Do not speculate.
Use only provided facts.

Context:
{context}
"""

        try:
            r = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2,
                max_tokens=120,
                timeout=_openai_timeout_seconds(),
            )
        except TypeError:
            r = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2,
                max_tokens=120,
            )
        return r.choices[0].message.content.strip()
    except Exception:
        return None

@app.get("/portfolio", include_in_schema=True)
def portfolio():
    try:
        conn = _db_connect()
        cur = conn.cursor()
        cur.execute("SELECT symbol, shares, avg_price, added_at FROM portfolio ORDER BY added_at DESC")
        rows = cur.fetchall() or []
        conn.close()
    except Exception:
        try:
            conn.close()
        except Exception:
            pass
        rows = []

    positions: List[Dict[str, Any]] = []
    for r in rows:
        try:
            sym = str(r["symbol"] or "").strip().upper()
        except Exception:
            sym = ""
        if not sym:
            continue
        try:
            shares = float(r["shares"] or 0.0)
        except Exception:
            shares = 0.0
        try:
            avg_price = float(r["avg_price"] or 0.0)
        except Exception:
            avg_price = 0.0
        try:
            added_at = str(r["added_at"] or "").strip()
        except Exception:
            added_at = ""

        live_px = _latest_price_for_symbol(sym)
        price = float(live_px) if live_px is not None else 0.0
        market_value = float(shares * price)
        cost_basis = float(shares * avg_price)
        pnl = float(market_value - cost_basis)
        pnl_pct = float((pnl / cost_basis) * 100.0) if cost_basis != 0 else 0.0

        positions.append(
            {
                "symbol": sym,
                "shares": float(shares),
                "avg_price": float(avg_price),
                "price": float(price),
                "market_value": float(market_value),
                "pnl": float(pnl),
                "pnl_pct": float(pnl_pct),
                "added_at": added_at or now_iso(),
            }
        )

    cash = 0.0
    try:
        # If Alpaca is available, prefer it; otherwise keep safe zero.
        a = trade_client().get_account()
        cash = float(getattr(a, "cash", 0.0) or 0.0)
    except Exception:
        cash = 0.0

    total_positions_value = 0.0
    total_cost_basis = 0.0
    total_pnl = 0.0
    for p in positions:
        try:
            total_positions_value += float(p.get("market_value") or 0.0)
        except Exception:
            pass
        try:
            cb = float(p.get("shares") or 0.0) * float(p.get("avg_price") or 0.0)
            total_cost_basis += float(cb)
        except Exception:
            pass
        try:
            total_pnl += float(p.get("pnl") or 0.0)
        except Exception:
            pass

    total_value = float(cash) + float(total_positions_value)
    pnl_percent = float((total_pnl / total_cost_basis) * 100.0) if total_cost_basis > 0 else 0.0

    # Allocation % per position (never missing)
    for p in positions:
        try:
            mv = float(p.get("market_value") or 0.0)
        except Exception:
            mv = 0.0
        alloc = float((mv / total_value) * 100.0) if total_value > 0 else 0.0
        p["allocation_percent"] = float(round(alloc, 4))

    out = {
        "positions": positions,
        "total_value": float(round(total_value, 4)),
        "cash": float(round(float(cash), 4)),
        "pnl": float(round(total_pnl, 4)),
        "pnl_percent": float(round(pnl_percent, 4)),
        "updated_at": now_iso(),
    }
    return _no_nulls(out)


def watchlist_get() -> Dict[str, Any]:
    try:
        conn = _db_connect()
        cur = conn.cursor()
        cur.execute("SELECT symbol, added_at FROM watchlist ORDER BY added_at DESC")
        rows = cur.fetchall() or []
        conn.close()
    except Exception:
        try:
            conn.close()
        except Exception:
            pass
        rows = []

    items: List[Dict[str, Any]] = []
    for r in rows:
        try:
            sym = str(r["symbol"] or "").strip().upper()
        except Exception:
            sym = ""
        if not sym:
            continue
        added_at = ""
        try:
            added_at = str(r["added_at"] or "").strip()
        except Exception:
            added_at = ""

        px = _latest_price_for_symbol(sym)
        price = float(px) if px is not None else 0.0

        change_pct = 0.0
        try:
            snap = get_snapshot(sym)
        except Exception:
            snap = None
        if isinstance(snap, dict):
            bar = snap.get("dailyBar") if isinstance(snap.get("dailyBar"), dict) else {}
            prev = snap.get("prevDailyBar") if isinstance(snap.get("prevDailyBar"), dict) else {}
            try:
                c = float(bar.get("c")) if bar.get("c") is not None else None
                pc = float(prev.get("c")) if prev.get("c") is not None else None
                if c is not None and pc is not None and pc > 0:
                    change_pct = float((c - pc) / pc * 100.0)
            except Exception:
                change_pct = 0.0

        items.append(
            {
                "symbol": sym,
                "price": float(price),
                "change_pct": float(change_pct),
                "added_at": added_at or now_iso(),
            }
        )

    return {"status": "ok", "items": items, "updated_at": now_iso()}


def _saved_picks_list(limit: int = 200) -> List[Dict[str, Any]]:
    try:
        lim = int(limit or 200)
    except Exception:
        lim = 200
    lim = max(1, min(500, lim))

    rows = []
    try:
        conn = _db_connect()
        cur = conn.cursor()
        cur.execute(
            "SELECT id, symbol, side, entry, stop_loss, targets_json, opened_at, closed_at, close_price, score, confidence, reason, source, status "
            "FROM saved_picks ORDER BY opened_at DESC LIMIT ?",
            (lim,),
        )
        rows = cur.fetchall() or []
        conn.close()
    except Exception:
        try:
            conn.close()
        except Exception:
            pass
        rows = []

    out: List[Dict[str, Any]] = []
    for r in rows:
        try:
            sym = str(r["symbol"] or "").strip().upper()
        except Exception:
            sym = ""
        if not sym:
            continue
        try:
            entry = float(r["entry"] or 0.0)
        except Exception:
            entry = 0.0
        try:
            side = str(r["side"] or "watch").strip().lower()
        except Exception:
            side = "watch"
        if side not in ("long", "short", "watch"):
            side = "watch"

        targets = []
        try:
            tj = r["targets_json"]
            arr = json.loads(tj) if isinstance(tj, str) and tj.strip() else []
            if isinstance(arr, list):
                targets = [float(x) for x in arr[:6] if _safe_f(x) is not None]
        except Exception:
            targets = []

        out.append(
            {
                "id": str(r["id"] or ""),
                "symbol": sym,
                "side": side,
                "entry": _px2(entry),
                "stop_loss": _px2(r["stop_loss"]),
                "targets": [_px2(x) for x in (targets[:3] if isinstance(targets, list) else [])][:3],
                "opened_at": str(r["opened_at"] or "")[:40],
                "closed_at": str(r["closed_at"] or "")[:40],
                "close_price": _px2(r["close_price"]),
                "score": float(round(float(_safe_f(r["score"], 0.0) or 0.0), 1)),
                "confidence": float(round(float(_safe_f(r["confidence"], 0.0) or 0.0), 1)),
                "reason": str(r["reason"] or "")[:240],
                "source": str(r["source"] or "")[:60],
                "status": str(r["status"] or "").strip().upper() or "OPEN",
            }
        )
    return out


@app.get("/portfolio/picks", include_in_schema=True)
def portfolio_picks():
    items = _saved_picks_list(limit=250)
    out: List[Dict[str, Any]] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        sym = str(it.get("symbol") or "").strip().upper()
        if not sym:
            continue
        entry = _safe_f(it.get("entry"), 0.0) or 0.0
        side = str(it.get("side") or "watch").strip().lower()
        status = str(it.get("status") or "OPEN").strip().upper() or "OPEN"

        current_px = None
        if status == "OPEN":
            current_px = _latest_price_for_symbol(sym)
        else:
            current_px = _safe_f(it.get("close_price"))
        if current_px is None:
            current_px = _safe_f(it.get("close_price"))
        if current_px is None:
            current_px = entry

        pnl_pct = 0.0
        pnl_abs = 0.0
        try:
            if entry > 0 and current_px is not None and float(current_px) > 0:
                if side == "short":
                    pnl_abs = float(entry) - float(current_px)
                elif side == "long":
                    pnl_abs = float(current_px) - float(entry)
                pnl_pct = (float(pnl_abs) / float(entry)) * 100.0
        except Exception:
            pnl_abs = 0.0
            pnl_pct = 0.0

        out.append(
            dict(
                it,
                **{
                    "current_price": _px2(current_px),
                    "pnl": float(round(float(pnl_abs), 2)),
                    "pnl_pct": float(round(float(pnl_pct), 2)),
                },
            )
        )

    return _no_nulls({"items": out, "updated_at": now_iso()})


@app.get("/watchlist", include_in_schema=True)
def watchlist():
    try:
        wl = watchlist_get()
        items = wl.get("items") if isinstance(wl, dict) else []
        if not isinstance(items, list):
            items = []
        symbols: List[str] = []
        seen: set = set()
        for it in items:
            if not isinstance(it, dict):
                continue
            s = str(it.get("symbol") or "").strip().upper()
            if s and s not in seen:
                symbols.append(s)
                seen.add(s)

        # Augment with near-threshold candidates from last scan (stale OK up to 4h)
        try:
            v2_ts = float(_LAST_V2_WATCHLIST.get("ts") or 0.0)
            v2_cands = _LAST_V2_WATCHLIST.get("candidates") or []
            if isinstance(v2_cands, list) and (time.time() - v2_ts) < 4 * 3600:
                for cand in v2_cands:
                    if not isinstance(cand, dict):
                        continue
                    cs = str(cand.get("symbol") or "").strip().upper()
                    if cs and cs not in seen:
                        symbols.append(cs)
                        seen.add(cs)
        except Exception:
            pass

        if not symbols:
            symbols = ["NVDA", "SPY"]
        return _no_nulls({"watchlist": symbols[:50]})
    except Exception:
        return _no_nulls({"watchlist": ["NVDA", "SPY"]})


@app.post("/watchlist/add", include_in_schema=True)
def watchlist_add(payload: Dict[str, Any] = Body(...), _user=_dep_starter):
    sym_raw = str((payload or {}).get("symbol") or "").strip().upper()
    sd = _symbol_sanitize(sym_raw, allow_extended=False)
    sym = str(sd.get("symbol") or "").strip().upper()
    if not bool(sd.get("ok")) or not sym:
        return _no_nulls({"ok": True, "symbol": sym_raw, "added": False})

    try:
        conn = _db_connect()
        cur = conn.cursor()
        cur.execute(
            "INSERT OR REPLACE INTO watchlist(symbol, added_at) VALUES(?, ?)",
            (sym, now_iso()),
        )
        conn.commit()
        conn.close()
    except Exception:
        try:
            conn.close()
        except Exception:
            pass
    return _no_nulls({"ok": True, "symbol": sym, "added": True})


@app.delete("/watchlist/remove/{symbol}", include_in_schema=True)
def watchlist_remove(symbol: str, _user=_dep_starter):
    sym_raw = str(symbol or "").strip().upper()
    sd = _symbol_sanitize(sym_raw, allow_extended=False)
    sym = str(sd.get("symbol") or "").strip().upper()
    if not bool(sd.get("ok")) or not sym:
        return _no_nulls({"ok": True, "symbol": sym_raw, "removed": False})
    try:
        conn = _db_connect()
        cur = conn.cursor()
        cur.execute("DELETE FROM watchlist WHERE symbol = ?", (sym,))
        conn.commit()
        conn.close()
    except Exception:
        try:
            conn.close()
        except Exception:
            pass
    return _no_nulls({"ok": True, "symbol": sym, "removed": True})


@app.post("/portfolio/add", include_in_schema=True)
def portfolio_add(payload: Dict[str, Any] = Body(...), _user=_dep_pro):
    sym_raw = str((payload or {}).get("symbol") or "").strip().upper()
    sd = _symbol_sanitize(sym_raw, allow_extended=False)
    sym = str(sd.get("symbol") or "").strip().upper()
    if not bool(sd.get("ok")) or not sym:
        return _no_nulls({"ok": True, "symbol": sym_raw, "added": False})

    try:
        shares = float((payload or {}).get("shares") or 0.0)
    except Exception:
        shares = 0.0
    try:
        avg_price = float((payload or {}).get("avg_price") or 0.0)
    except Exception:
        avg_price = 0.0
    if shares <= 0:
        shares = 0.0
    if avg_price < 0:
        avg_price = 0.0

    try:
        conn = _db_connect()
        cur = conn.cursor()
        cur.execute(
            "INSERT OR REPLACE INTO portfolio(symbol, shares, avg_price, added_at) VALUES(?, ?, ?, ?)",
            (sym, float(shares), float(avg_price), now_iso()),
        )
        conn.commit()
        conn.close()
    except Exception:
        try:
            conn.close()
        except Exception:
            pass

    return _no_nulls({"ok": True, "symbol": sym, "added": True})


@app.delete("/portfolio/remove/{symbol}", include_in_schema=True)
def portfolio_remove(symbol: str, _user=_dep_pro):
    sym_raw = str(symbol or "").strip().upper()
    sd = _symbol_sanitize(sym_raw, allow_extended=False)
    sym = str(sd.get("symbol") or "").strip().upper()
    if not bool(sd.get("ok")) or not sym:
        return _no_nulls({"ok": True, "symbol": sym_raw, "removed": False})
    try:
        conn = _db_connect()
        cur = conn.cursor()
        cur.execute("DELETE FROM portfolio WHERE symbol = ?", (sym,))
        conn.commit()
        conn.close()
    except Exception:
        try:
            conn.close()
        except Exception:
            pass
    return _no_nulls({"ok": True, "symbol": sym, "removed": True})


@app.post("/portfolio/save_pick")
def portfolio_save_pick(payload: Dict[str, Any] = Body(...), _user=_dep_pro):
    sym_raw = str((payload or {}).get("symbol") or "").strip().upper()
    sd = _symbol_sanitize(sym_raw, allow_extended=False)
    if not bool(sd.get("ok")):
        raise HTTPException(status_code=400, detail="INVALID_SYMBOL")
    sym = str(sd.get("symbol") or "").strip().upper()

    side = str((payload or {}).get("side") or "watch").strip().lower()
    if side not in ("long", "short", "watch"):
        side = "watch"

    entry = (payload or {}).get("entry")
    stop_loss = (payload or {}).get("stop_loss")
    targets = (payload or {}).get("targets")
    ts = (payload or {}).get("timestamp")
    score = (payload or {}).get("score")
    confidence = (payload or {}).get("confidence")
    reason = str((payload or {}).get("reason") or "")[:240]
    source = str((payload or {}).get("source") or "")[:60]

    try:
        entry_f = float(entry) if entry is not None else None
    except Exception:
        entry_f = None
    if entry_f is None or entry_f <= 0:
        px = _latest_price_for_symbol(sym)
        try:
            entry_f = float(px) if px is not None else 0.0
        except Exception:
            entry_f = 0.0

    try:
        stop_f = float(stop_loss) if stop_loss is not None else None
    except Exception:
        stop_f = None
    if stop_f is None:
        stop_f = 0.0

    try:
        sc = float(score) if score is not None else 0.0
    except Exception:
        sc = 0.0
    try:
        cf = float(confidence) if confidence is not None else 0.0
    except Exception:
        cf = 0.0
    sc = float(_clamp_0_to_10(sc))
    cf = float(_clamp_0_to_10(cf))

    tjson = "[]"
    try:
        if isinstance(targets, list):
            tjson = json.dumps(targets[:6])
    except Exception:
        tjson = "[]"

    pid = f"pick_{int(time.time()*1000)}_{hash(sym) & 0xFFFF}"
    opened_at = str(ts).strip()[:40] if isinstance(ts, str) and ts.strip() else now_iso()

    try:
        conn = _db_connect()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO saved_picks (id, symbol, side, entry, stop_loss, targets_json, opened_at, closed_at, close_price, score, confidence, reason, source, status) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?, ?, ?, 'OPEN')",
            (pid, sym, side, float(entry_f), float(stop_f), tjson, opened_at, float(sc), float(cf), reason, source),
        )
        conn.commit()
        conn.close()
    except Exception:
        try:
            conn.close()
        except Exception:
            pass
        raise HTTPException(status_code=500, detail="SAVE_PICK_FAILED")

    return {"ok": True, "id": pid, "symbol": sym}


@app.post("/portfolio/close_pick")
def portfolio_close_pick(payload: Dict[str, Any] = Body(...), _user=_dep_pro):
    pid = str((payload or {}).get("id") or "").strip()
    if not pid:
        raise HTTPException(status_code=400, detail="MISSING_ID")
    try:
        conn = _db_connect()
        cur = conn.cursor()
        cur.execute("SELECT id, symbol, side, entry FROM saved_picks WHERE id = ? AND status = 'OPEN'", (pid,))
        row = cur.fetchone()
        if row is None:
            conn.close()
            raise HTTPException(status_code=404, detail="PICK_NOT_FOUND")
        sym = str(row["symbol"] or "").strip().upper()
        side = str(row["side"] or "watch").strip().lower()
        entry = float(row["entry"] or 0.0)

        px = _latest_price_for_symbol(sym)
        try:
            close_px = float(px) if px is not None else entry
        except Exception:
            close_px = entry

        cur.execute(
            "UPDATE saved_picks SET status = 'CLOSED', closed_at = ?, close_price = ? WHERE id = ?",
            (now_iso(), float(close_px), pid),
        )
        conn.commit()
        conn.close()
    except HTTPException:
        raise
    except Exception:
        try:
            conn.close()
        except Exception:
            pass
        raise HTTPException(status_code=500, detail="CLOSE_PICK_FAILED")

    pnl = 0.0
    try:
        if entry > 0 and close_px > 0:
            if side == "short":
                pnl = float(entry - close_px)
            elif side == "long":
                pnl = float(close_px - entry)
    except Exception:
        pnl = 0.0

    return {"ok": True, "id": pid, "symbol": sym, "close_price": float(close_px), "pnl": float(round(pnl, 2))}


@app.get("/alerts/preferences", include_in_schema=True)
async def get_alerts_prefs(request: Request, _user=Depends(_get_current_user)):
    """Return the authenticated user's alert preferences."""
    try:
        from alerts import get_alert_prefs
        prefs = get_alert_prefs(_user["id"])
        return JSONResponse({"ok": True, **prefs})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


class _AlertPrefsBody(BaseModel):
    phone:            Optional[str]  = None
    alerts_new_pick:  bool           = True
    alerts_outcome:   bool           = True
    alerts_channel:   str            = "email"


@app.post("/alerts/preferences", include_in_schema=True)
async def save_alerts_prefs(body: _AlertPrefsBody, _user=Depends(_get_current_user)):
    """Save alert preferences for the authenticated user."""
    try:
        from alerts import save_alert_prefs
        ok = save_alert_prefs(
            user_id         = _user["id"],
            phone           = body.phone,
            alerts_new_pick = body.alerts_new_pick,
            alerts_outcome  = body.alerts_outcome,
            alerts_channel  = body.alerts_channel,
        )
        return JSONResponse({"ok": ok})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.get("/account", include_in_schema=True)
def account():
    """Frontend-friendly account summary. Must never 404 or hard-500."""
    try:
        client = trade_client()
        a = client.get_account()
        return {
            "mode": "LIVE",
            "cash": float(getattr(a, "cash", 0) or 0),
            "equity": float(getattr(a, "equity", 0) or 0),
            "buying_power": float(getattr(a, "buying_power", 0) or 0),
            "account_value": float(getattr(a, "equity", 0) or 0),
            "updated_at": now_iso(),
        }
    except Exception as e:
        return {
            "mode": "OFFLINE",
            "cash": 0.0,
            "equity": 0.0,
            "buying_power": 0.0,
            "account_value": 0.0,
            "updated_at": now_iso(),
            "error": f"Alpaca error: {str(e)}",
        }


@app.get("/performance", include_in_schema=True)
def performance_summary():
    import sqlite3 as _sq
    db_path = os.environ.get("PERF_TRACKER_DB", os.path.join(
        os.environ.get("DATA_DIR", os.path.dirname(os.path.abspath(__file__))), "perf_tracker.db"))
    try:
        con = _sq.connect(db_path)
        con.row_factory = _sq.Row
        cur = con.cursor()
        cur.execute(
            "SELECT status, max_return_pct FROM picks WHERE status != 'pending'"
        )
        rows = cur.fetchall() or []
        con.close()
    except Exception as e:
        return {"error": str(e), "total_picks": 0, "wins": 0, "losses": 0, "win_rate": 0.0, "avg_return_pct": 0.0}

    total_picks = len(rows)
    wins = sum(1 for r in rows if str(r["status"] or "").startswith("won"))
    losses = sum(1 for r in rows if str(r["status"] or "").startswith("lost") or str(r["status"] or "").startswith("expired"))
    contested = wins + losses
    win_rate = round(wins / contested * 100.0, 1) if contested > 0 else 0.0
    returns = [float(r["max_return_pct"]) for r in rows if r["max_return_pct"] is not None]
    avg_return_pct = round(sum(returns) / len(returns), 2) if returns else 0.0
    return {
        "total_picks": total_picks,
        "wins": wins,
        "losses": losses,
        "win_rate": win_rate,
        "avg_return_pct": avg_return_pct,
    }


@app.get("/performance/picks", include_in_schema=True)
def performance_picks():
    import sqlite3 as _sq, datetime as _dt, json as _json
    db_path = os.environ.get("PERF_TRACKER_DB", os.path.join(
        os.environ.get("DATA_DIR", os.path.dirname(os.path.abspath(__file__))), "perf_tracker.db"))
    out = []
    try:
        con = _sq.connect(db_path)
        con.row_factory = _sq.Row
        cur = con.cursor()
        cur.execute("SELECT symbol, status, entry_price, max_return_pct, edge_signals, recorded_at, hit_target, hit_stop FROM picks ORDER BY recorded_at DESC LIMIT 50")
        for row in cur.fetchall():
            try:
                date_str = _dt.datetime.utcfromtimestamp(row["recorded_at"]).strftime("%Y-%m-%d")
            except:
                date_str = ""
            status = row["status"] or "pending"
            pct = row["max_return_pct"] or 0.0
            if status == "lost" or row["hit_stop"]:
                outcome = "lost"
            elif status == "won" or row["hit_target"]:
                outcome = "won"
            else:
                outcome = status
            try:
                signals = _json.loads(row["edge_signals"] or "[]")
            except:
                signals = []
            out.append({"symbol": row["symbol"], "return_pct": round(pct, 2), "outcome": outcome, "date": date_str, "signals": signals if isinstance(signals, list) else []})
        con.close()
    except Exception as e:
        return {"error": str(e), "picks": []}
    return {"picks": out}


@app.get("/scan/pre_movers", include_in_schema=True)
async def scan_pre_movers(
    refresh: bool = Query(False, description="Force a fresh scan instead of returning cached results"),
    max_results: int = Query(20, ge=1, le=50),
    _user=_dep_starter,
):
    """Return top small-cap pre-mover candidates ($1-$20).

    Results are cached for 1 hour. Pass refresh=true to trigger a fresh scan
    (takes ~2-5 min).
    """
    try:
        from pre_mover_scanner import (
            get_cached_premover_results,
            premover_cache_is_fresh,
            run_premover_scan,
            set_cached_premover_results,
        )

        if not refresh and premover_cache_is_fresh():
            cached = get_cached_premover_results()
            results = cached.get("results") or []
            return {
                "status": "ok",
                "source": "cache",
                "scanned": cached.get("scanned") or 0,
                "results": results[:max_results],
                "ts": cached.get("ts") or 0.0,
            }

        # Run fresh scan in background thread to avoid blocking the event loop
        universe = await asyncio.to_thread(get_scan_universe, 3000)
        result = await asyncio.to_thread(
            run_premover_scan,
            universe,
            max_results,
            50,
            300.0,
        )
        if isinstance(result, dict) and result.get("results"):
            set_cached_premover_results(result)

        return {
            "status": "ok",
            "source": "fresh",
            "scanned": result.get("scanned") or 0,
            "elapsed": result.get("elapsed") or 0.0,
            "results": (result.get("results") or [])[:max_results],
            "ts": result.get("ts") or time.time(),
        }
    except Exception as e:
        log.exception(f"scan_pre_movers error: {e}")
        raise HTTPException(status_code=500, detail="SCAN_FAILED")


@app.post("/scan/brain_reset", include_in_schema=True)
async def scan_brain_reset(_user=_dep_elite):
    """
    Wipe ALL outcomes and signal_stats, then run a full backfill with correct bar data.
    Use this once to clear corrupt outcomes from the old get_snapshots_batch bug.
    """
    import asyncio
    try:
        from brain import _conn, backfill_all_outcomes, recalibrate_weights
        def _reset_and_backfill():
            with _conn() as db:
                db.execute("DELETE FROM outcomes")
                db.execute("DELETE FROM signal_stats")
            result = backfill_all_outcomes()
            return result
        result = await asyncio.to_thread(_reset_and_backfill)
        return {"ok": True, "action": "wiped_all_outcomes_and_backfilled", **result}
    except Exception as e:
        log.warning(f"brain_reset error: {e}")
        raise HTTPException(status_code=500, detail="BRAIN_RESET_FAILED")


@app.post("/scan/brain_backfill", include_in_schema=True)
async def scan_brain_backfill(_user=_dep_elite):
    """
    Retroactively evaluate ALL historical picks that haven't been fully checked.
    Fetches real price bars from after each pick date and records outcomes.
    Runs synchronously — may take 30-60s for large pick histories.
    """
    import asyncio
    try:
        from brain import backfill_all_outcomes
        result = await asyncio.to_thread(backfill_all_outcomes)
        return {"ok": True, **result}
    except Exception as e:
        log.warning(f"brain_backfill error: {e}")
        raise HTTPException(status_code=500, detail="BRAIN_BACKFILL_FAILED")


@app.get("/scan/brain_stats", include_in_schema=True)
async def scan_brain_stats():
    """
    Returns the scanner's self-learning brain stats:
    - Overall win rate across all tracked picks
    - Per-signal win rates and learned multipliers
    - Recent picks with outcomes
    """
    try:
        from brain import get_brain_stats
        return get_brain_stats()
    except Exception as e:
        log.warning(f"brain_stats error: {e}")
        raise HTTPException(status_code=500, detail="BRAIN_STATS_FAILED")


@app.post("/scan/train_nn", include_in_schema=True)
async def scan_train_nn(
    force: bool = Query(False, description="Train even if fewer than 20 resolved picks"),
    _user=_dep_starter,
):
    """
    Trigger a neural network training run from resolved picks in perf_tracker.db.
    Runs in a background thread — returns immediately with a status message.
    The trained model is saved to models/nn_scorer.npz and applied to all
    future scans automatically.
    """
    import threading as _threading

    def _train():
        try:
            from ml.trainer import run_training
            result = run_training(force=force)
            log.info(f"train_nn endpoint: {result}")
        except Exception as e:
            log.warning(f"train_nn endpoint error: {e}")

    t = _threading.Thread(target=_train, daemon=True)
    t.start()
    return {"status": "training_started", "force": force,
            "message": "NN training running in background. Check /scan/nn_status for progress."}


@app.get("/scan/nn_status", include_in_schema=True)
async def scan_nn_status():
    """
    Returns the status of the neural network scorer:
    - Whether a trained model exists
    - How old it is
    - Win probability for a test inference (using neutral inputs)
    """
    try:
        from ml.predictor import model_info, predict_win_prob
        info = model_info()
        # Quick sanity-check inference with neutral inputs
        if info.get("ready"):
            test_closes = [100.0] * 60
            test_prob = predict_win_prob(test_closes, test_closes, test_closes, [1_000_000] * 60)
            info["test_inference_prob"] = round(test_prob, 3)
        return info
    except Exception as e:
        log.warning(f"nn_status error: {e}")
        return {"ready": False, "error": str(e)}


# ---------------------------------------------------------------------------
# Chatbot endpoint
# ---------------------------------------------------------------------------

def _chat_build_context() -> str:
    """Pull live system context to include in the chatbot system prompt."""
    lines = []

    # Recent picks from perf_tracker
    try:
        import sqlite3 as _sq
        _pt = os.getenv("PERF_TRACKER_DB", os.path.join(os.getenv("DATA_DIR", os.path.dirname(os.path.abspath(__file__))), "perf_tracker.db"))
        con = _sq.connect(_pt, timeout=5)
        con.row_factory = _sq.Row
        rows = con.execute(
            "SELECT symbol, status, edge_signals, edge_score, final_score, "
            "max_return_pct, recorded_at FROM picks ORDER BY recorded_at DESC LIMIT 8"
        ).fetchall()
        con.close()
        if rows:
            lines.append("RECENT PICKS (last 8):")
            for r in rows:
                sig = r["edge_signals"] or "[]"
                ret = f"{r['max_return_pct']:+.1f}%" if r["max_return_pct"] is not None else "pending"
                lines.append(
                    f"  {r['symbol']:6s} status={r['status']:15s} score={r['final_score'] or r['edge_score'] or 0:.1f}/10 "
                    f"return={ret} signals={sig}"
                )
    except Exception:
        pass

    # Win/loss summary
    try:
        con2 = _sq.connect(_pt, timeout=5)
        row2 = con2.execute(
            "SELECT COUNT(*) as total, "
            "SUM(CASE WHEN status IN ('won','won_drift') THEN 1 ELSE 0 END) as wins, "
            "SUM(CASE WHEN status IN ('lost','lost_drift') THEN 1 ELSE 0 END) as losses, "
            "SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END) as pending "
            "FROM picks"
        ).fetchone()
        con2.close()
        if row2:
            wr = int(row2["wins"]) / max(int(row2["wins"]) + int(row2["losses"]), 1) * 100
            lines.append(
                f"\nPERFORMANCE SUMMARY: {row2['total']} total | "
                f"{row2['wins']} wins | {row2['losses']} losses | {row2['pending']} pending | "
                f"win rate {wr:.0f}%"
            )
    except Exception:
        pass

    # NN status
    try:
        from ml.predictor import model_info
        info = model_info()
        if info.get("ready"):
            lines.append(f"\nNEURAL NETWORK: trained {info.get('trained_h_ago', '?')}h ago, active")
        else:
            lines.append("\nNEURAL NETWORK: not yet trained")
    except Exception:
        pass

    # Saved portfolio picks
    try:
        conn3 = _db_connect()
        cur3 = conn3.cursor()
        cur3.execute("SELECT symbol, side, status, score FROM saved_picks ORDER BY opened_at DESC LIMIT 5")
        saved = cur3.fetchall() or []
        conn3.close()
        if saved:
            lines.append("\nPORTFOLIO PICKS:")
            for s in saved:
                lines.append(f"  {s['symbol']} {s['side']} status={s['status']} score={s['score']}")
    except Exception:
        pass

    return "\n".join(lines) if lines else "No live context available."


_CHAT_SYSTEM = """You are StackIQ's AI trading assistant — a sharp, concise market analyst built into the StackIQ scanner platform.

You have real-time access to the system's live data shown below. Use it to give concrete, specific answers.

Your personality:
- Direct and confident, like a good trading desk analyst
- Short answers unless asked to elaborate — traders don't want walls of text
- Always ground your answers in the data when it's relevant
- If asked about a specific stock not in the context, say you don't have live data but can discuss it generally
- Never give financial advice — frame everything as analysis and education

You know about:
- The system's recent picks and their win/loss outcomes
- The neural network scorer and what signals it's learning from
- Market regime detection (BULL/BEAR/CHOPPY)
- Technical signals: MOMENTUM_EXPANSION, BREAKOUT_STRUCTURE, RS_LEADER, VOLATILITY_EXPANSION, SUPPORT_RECLAIM
- How the scoring system works (0-10 scale, edge signals, NN probability blend)

Live system context:
{context}
"""


class _ChatMessage(BaseModel):
    role: str   # "user" | "assistant"
    content: str


class _ChatRequest(BaseModel):
    message: str
    history: List[_ChatMessage] = []


@app.post("/api/chat", include_in_schema=True)
async def api_chat(req: _ChatRequest, request: Request, _user=_dep_starter):
    """
    Chatbot endpoint.  Accepts a user message + conversation history,
    returns the AI assistant's reply.  Backed by GPT-4o-mini with live
    system context (recent picks, performance, NN status).
    """
    # Rate limit: 20 messages per minute per IP
    client_ip = (request.headers.get("X-Forwarded-For") or request.client.host or "unknown").split(",")[0].strip()
    if not _rate_limit(f"chat:{client_ip}", max_calls=20, window_s=60):
        raise HTTPException(status_code=429, detail="RATE_LIMIT_EXCEEDED")

    try:
        from llm_client import call_llm_text, llm_available

        if not llm_available():
            return {"reply": "The AI assistant isn't available right now (LLM not configured). "
                             "Check that OPENAI_API_KEY is set.", "ok": False}

        context = await asyncio.to_thread(_chat_build_context)
        system_prompt = _CHAT_SYSTEM.format(context=context)

        history_text = ""
        for msg in (req.history or [])[-10:]:
            role = "You" if msg.role == "assistant" else "User"
            history_text += f"{role}: {msg.content}\n"

        user_prompt = history_text + f"User: {req.message.strip()}\nYou:"

        reply = await asyncio.to_thread(
            call_llm_text,
            system=system_prompt,
            user=user_prompt,
            max_output_tokens=512,
            timeout_s=20.0,
        )

        return {"reply": reply.strip(), "ok": True}

    except HTTPException:
        raise
    except Exception as e:
        log.warning(f"api_chat error: {e}")
        return {"reply": "Sorry, I hit an internal error. Please try again.", "ok": False}


@app.get("/chat", include_in_schema=False)
async def serve_chatbot():
    """Serve the standalone chatbot page."""
    from fastapi.responses import FileResponse as _FR
    widget = os.path.join(os.path.dirname(__file__), "chatbot_widget.html")
    if os.path.isfile(widget):
        return _FR(widget, media_type="text/html")
    raise HTTPException(status_code=404, detail="chatbot_widget.html not found")
