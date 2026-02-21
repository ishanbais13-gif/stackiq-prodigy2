import os
import time
import logging
import random
import asyncio
import threading
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional
from collections.abc import Iterable

import requests

try:
    from alpaca.data.historical import StockHistoricalDataClient  # type: ignore
    from alpaca.data.requests import (  # type: ignore
        StockBarsRequest,
        StockSnapshotRequest,
        StockLatestTradeRequest,
    )
    from alpaca.data.timeframe import TimeFrame  # type: ignore
    _ALPACA_PY_AVAILABLE = True
except Exception:
    StockHistoricalDataClient = None  # type: ignore
    StockBarsRequest = None  # type: ignore
    StockSnapshotRequest = None  # type: ignore
    StockLatestTradeRequest = None  # type: ignore
    TimeFrame = None  # type: ignore
    _ALPACA_PY_AVAILABLE = False

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None  # type: ignore

log = logging.getLogger("stackiq")


try:
    if load_dotenv is not None:
        _dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
        load_dotenv(dotenv_path=_dotenv_path, override=False)
except Exception:
    pass

try:
    if not os.getenv("ALPACA_API_KEY"):
        log.warning("ALPACA_API_KEY missing — market data degraded")
except Exception:
    pass


_REQUIRED_ALPACA_ENV = {
    "ALPACA_API_KEY": None,
    "ALPACA_SECRET_KEY": None,
    "ALPACA_DATA_BASE_URL": "https://data.alpaca.markets",
    "ALPACA_DATA_FEED": "iex",
}


def validate_market_env() -> Dict[str, Any]:
    """Validate required Alpaca/OpenAI env vars.

    Requirement: warn if missing but do not crash.
    """
    missing: List[str] = []
    for k, default in _REQUIRED_ALPACA_ENV.items():
        v = os.getenv(k)
        if (v is None or str(v).strip() == "") and default is None:
            missing.append(k)
    try:
        if missing:
            log.warning(f"Missing Alpaca env vars (market data may degrade): {', '.join(missing)}")
    except Exception:
        pass

    # Ensure defaults are present (do not override explicit env).
    try:
        for k, default in _REQUIRED_ALPACA_ENV.items():
            if default is not None and not (os.getenv(k) or "").strip():
                os.environ[k] = str(default)
    except Exception:
        pass

    return {"missing": missing, "ok": (len(missing) == 0)}


_BARS_CACHE_TTL_SECONDS = 60
_bars_cache: Dict[tuple, Dict[str, Any]] = {}

_ALPACA_AUTH_COOLDOWN_UNTIL = 0.0
_LAST_ALPACA_AUTH_WARN_TS = 0.0

# Separate cooldown for 429 storms (rate limiting). This must not reuse auth cooldown.
_ALPACA_RATE_COOLDOWN_UNTIL = 0.0


class TTLCache:
    def __init__(self, *, maxsize: int, ttl_seconds: float):
        self.maxsize = int(maxsize or 1000)
        self.ttl_seconds = float(ttl_seconds or 60.0)
        self._lock = threading.Lock()
        self._data: Dict[str, Any] = {}

    def get(self, key: str) -> Any:
        if not key:
            return None
        now = time.time()
        with self._lock:
            item = self._data.get(key)
            if not item or not isinstance(item, tuple) or len(item) != 2:
                return None
            ts, val = item
            try:
                if (now - float(ts or 0.0)) > self.ttl_seconds:
                    self._data.pop(key, None)
                    return None
            except Exception:
                self._data.pop(key, None)
                return None
            return val

    def set(self, key: str, value: Any) -> None:
        if not key:
            return
        now = time.time()
        with self._lock:
            self._data[key] = (now, value)
            if len(self._data) > self.maxsize:
                try:
                    items = sorted(self._data.items(), key=lambda kv: float(kv[1][0] or 0.0))
                    over = max(0, len(items) - self.maxsize)
                    for k, _ in items[:over]:
                        self._data.pop(k, None)
                except Exception:
                    pass


class RateLimiter:
    def __init__(self, rate_per_sec: float = 4.0):
        self.rate_per_sec = float(rate_per_sec or 4.0)
        self._lock = threading.Lock()
        self._last_called = 0.0

    def wait_sync(self) -> None:
        if self.rate_per_sec <= 0:
            return
        with self._lock:
            now = time.time()
            elapsed = now - float(self._last_called or 0.0)
            wait_time = max(0.0, (1.0 / self.rate_per_sec) - elapsed)
            if wait_time > 0:
                try:
                    log.info(f"Rate limit governor wait {wait_time:.3f}s")
                except Exception:
                    pass
                time.sleep(wait_time)
            self._last_called = time.time()

    async def wait(self) -> None:
        if self.rate_per_sec <= 0:
            return
        # Use to_thread to share the same timing gate with sync callers.
        await asyncio.to_thread(self.wait_sync)


class TokenBucketGovernor:
    """Simple global governor: max N requests per window.

    This is intentionally conservative and synchronous-safe.
    It queues callers by sleeping until tokens refill, and will time out
    rather than block forever.
    """

    def __init__(self, *, capacity: int, window_seconds: float, max_wait_seconds: float = 12.0):
        self.capacity = max(1, int(capacity))
        self.window_seconds = max(1.0, float(window_seconds))
        self.max_wait_seconds = max(0.5, float(max_wait_seconds))
        self._lock = threading.Lock()
        self._tokens = float(self.capacity)
        self._last_refill = time.time()

    def _refill_locked(self) -> None:
        now = time.time()
        elapsed = now - float(self._last_refill or 0.0)
        if elapsed <= 0:
            return
        refill_rate = float(self.capacity) / float(self.window_seconds)
        self._tokens = min(float(self.capacity), float(self._tokens) + (elapsed * refill_rate))
        self._last_refill = now

    def acquire_sync(self) -> None:
        start = time.time()
        while True:
            with self._lock:
                self._refill_locked()
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                tokens_needed = 1.0 - float(self._tokens)
                refill_rate = float(self.capacity) / float(self.window_seconds)
                wait_s = max(0.05, tokens_needed / max(1e-9, refill_rate))

            if (time.time() - start) > float(self.max_wait_seconds):
                raise AlpacaRateLimitError("Alpaca governor queue overflow (local)")
            time.sleep(min(wait_s, 0.35))

    async def acquire(self) -> None:
        await asyncio.to_thread(self.acquire_sync)


_rate_limiter = RateLimiter(rate_per_sec=float(os.getenv("ALPACA_MAX_REQ_PER_SEC", "3.34") or 3.34))

# Hard governor (200/min) to prevent request storms and rate-limit spam.
_governor = TokenBucketGovernor(
    capacity=int(os.getenv("ALPACA_MAX_REQ_PER_MIN", "200") or 200),
    window_seconds=60.0,
    max_wait_seconds=float(os.getenv("ALPACA_MAX_GOVERNOR_WAIT_SECONDS", "12") or 12.0),
)

_snapshot_cache = TTLCache(maxsize=5000, ttl_seconds=30.0)
_snapshots_batch_cache = TTLCache(maxsize=500, ttl_seconds=60.0)

# Product requirement: cache bars for 60 sec to prevent rate spam.
_bars_cache_daily = TTLCache(maxsize=5000, ttl_seconds=60.0)
_bars_cache_intraday = TTLCache(maxsize=5000, ttl_seconds=60.0)


_HIST_CLIENT_SINGLETON: Optional[Any] = None


def _alpaca_sdk_client() -> Optional[Any]:
    """Preferred authenticated Alpaca client (alpaca-py) when installed."""
    global _HIST_CLIENT_SINGLETON
    if not _ALPACA_PY_AVAILABLE:
        return None
    key = (os.getenv("ALPACA_API_KEY") or "").strip()
    secret = (os.getenv("ALPACA_SECRET_KEY") or "").strip()
    if not key or not secret:
        return None
    if _HIST_CLIENT_SINGLETON is None:
        try:
            _HIST_CLIENT_SINGLETON = StockHistoricalDataClient(api_key=key, secret_key=secret)
        except Exception:
            _HIST_CLIENT_SINGLETON = None
    return _HIST_CLIENT_SINGLETON


def _bars_cache_for_timeframe(tf: str) -> TTLCache:
    s = str(tf or "").strip().lower()
    if s in ("1day", "day", "d", "1d"):
        return _bars_cache_daily
    return _bars_cache_intraday


def _bars_cache_key(sym: str, tf: str, limit: int) -> str:
    return f"bars:{sym}:{tf}:{int(limit or 0)}"


def _snapshot_cache_key(sym: str) -> str:
    return f"snap:{sym}"


def _request_governed(method: str, url: str, params: Optional[dict] = None) -> dict:
    _governor.acquire_sync()
    _rate_limiter.wait_sync()
    return _request(method, url, params=params)


def safe_alpaca_call_sync(func, *args, **kwargs):
    last_err: Optional[Exception] = None
    # Max retry = 2 (3 total attempts)
    for attempt in range(0, 3):
        try:
            return func(*args, **kwargs)
        except AlpacaRateLimitError as e:
            last_err = e
            try:
                _alpaca_set_rate_cooldown(30)
            except Exception:
                pass
            try:
                log.warning(f"Rate limit encountered — retrying (attempt={attempt + 1}/2)")
            except Exception:
                pass
            if attempt >= 2:
                break
            time.sleep(2 ** attempt)
        except Exception as e:
            last_err = e
            break
    return None


class AlpacaRequestError(Exception):
    pass


class AlpacaAuthError(Exception):
    pass


class AlpacaRateLimitError(Exception):
    pass


def _alpaca_auth_in_cooldown() -> bool:
    try:
        return time.time() < float(_ALPACA_AUTH_COOLDOWN_UNTIL or 0.0)
    except Exception:
        return False


def _alpaca_rate_in_cooldown() -> bool:
    try:
        return time.time() < float(_ALPACA_RATE_COOLDOWN_UNTIL or 0.0)
    except Exception:
        return False


def _alpaca_set_rate_cooldown(seconds: int = 30) -> None:
    global _ALPACA_RATE_COOLDOWN_UNTIL
    try:
        _ALPACA_RATE_COOLDOWN_UNTIL = max(float(_ALPACA_RATE_COOLDOWN_UNTIL or 0.0), time.time() + float(seconds))
    except Exception:
        pass


def _alpaca_set_auth_cooldown(seconds: int = 300) -> None:
    global _ALPACA_AUTH_COOLDOWN_UNTIL
    try:
        _ALPACA_AUTH_COOLDOWN_UNTIL = max(float(_ALPACA_AUTH_COOLDOWN_UNTIL or 0.0), time.time() + float(seconds))
    except Exception:
        pass


def _warn_auth_throttled(msg: str) -> None:
    global _LAST_ALPACA_AUTH_WARN_TS
    try:
        now_ts = time.time()
        last = float(_LAST_ALPACA_AUTH_WARN_TS or 0.0)
        if (now_ts - last) >= 180.0:
            _LAST_ALPACA_AUTH_WARN_TS = now_ts
            log.warning(msg)
    except Exception:
        pass


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _headers() -> Dict[str, str]:
    key = os.getenv("ALPACA_API_KEY", "")
    secret = os.getenv("ALPACA_SECRET_KEY", "")
    return {
        "APCA-API-KEY-ID": key,
        "APCA-API-SECRET-KEY": secret,
        "Accept": "application/json",
    }


def _data_base_url() -> str:
    return os.getenv("ALPACA_DATA_BASE_URL", "https://data.alpaca.markets").rstrip("/")


def _feed_candidates() -> List[str]:
    raw = str(os.getenv("ALPACA_DATA_FEED", "iex") or "iex").strip() or "iex"
    cands = [raw]
    if raw.lower() != "iex":
        cands.append("iex")
    # preserve order; unique
    out: List[str] = []
    seen = set()
    for f in cands:
        fx = str(f or "").strip()
        if not fx:
            continue
        k = fx.lower()
        if k in seen:
            continue
        seen.add(k)
        out.append(fx)
    return out or ["iex"]


def _request(method: str, url: str, params: Optional[dict] = None) -> dict:
    if _alpaca_auth_in_cooldown():
        raise AlpacaAuthError("Alpaca auth disabled (cooldown active).")

    if _alpaca_rate_in_cooldown():
        raise AlpacaRateLimitError("Alpaca rate limited (cooldown active).")

    key0 = (os.getenv("ALPACA_API_KEY") or "").strip()
    sec0 = (os.getenv("ALPACA_SECRET_KEY") or "").strip()
    if not key0 or not sec0:
        _alpaca_set_auth_cooldown(300)
        raise AlpacaAuthError("Alpaca auth failed (missing ALPACA_API_KEY / ALPACA_SECRET_KEY).")

    try:
        connect_timeout_s = float(os.getenv("ALPACA_HTTP_CONNECT_TIMEOUT_SECONDS", "3") or 3)
    except Exception:
        connect_timeout_s = 3.0
    try:
        read_timeout_s = float(os.getenv("ALPACA_HTTP_READ_TIMEOUT_SECONDS", "9") or 9)
    except Exception:
        read_timeout_s = 9.0
    if connect_timeout_s <= 0:
        connect_timeout_s = 3.0
    if read_timeout_s <= 0:
        read_timeout_s = 9.0
    if connect_timeout_s > 10.0:
        connect_timeout_s = 10.0
    if read_timeout_s > 20.0:
        read_timeout_s = 20.0
    try:
        r = requests.request(method, url, headers=_headers(), params=params, timeout=(connect_timeout_s, read_timeout_s))
    except requests.RequestException as e:
        raise AlpacaRequestError(f"Network error calling Alpaca: {e}")

    if r.status_code == 401:
        _alpaca_set_auth_cooldown(300)
        raise AlpacaAuthError("Alpaca auth failed (check ALPACA_API_KEY / ALPACA_SECRET_KEY).")
    if r.status_code == 403:
        # 403 can be caused by feed entitlement (e.g. SIP not allowed) and should NOT
        # trigger global auth cooldown, otherwise we cannot fall back to IEX.
        try:
            payload = r.json()
        except Exception:
            payload = {"message": r.text[:200]}
        raise AlpacaRequestError(f"Alpaca forbidden 403: {payload}")
    if r.status_code == 429:
        # Protect the system from retry storms.
        _alpaca_set_rate_cooldown(30)
        raise AlpacaRateLimitError("Alpaca rate limit hit (429). Try again in a bit.")
    if r.status_code >= 400:
        try:
            payload = r.json()
        except Exception:
            payload = {"message": r.text[:200]}
        raise AlpacaRequestError(f"Alpaca request failed ({r.status_code}): {payload}")

    return r.json()


def _normalize_symbol(symbol: str) -> str:
    s = (symbol or "").strip().upper()
    if not s.isalnum():
        raise AlpacaRequestError(f"Invalid symbol: {symbol}")
    return s


def _to_float(x) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None


def get_snapshot(symbol: str) -> Dict[str, Any]:
    sym = _normalize_symbol(symbol)
    ck = _snapshot_cache_key(sym)
    cached = _snapshot_cache.get(ck)
    if isinstance(cached, dict):
        try:
            log.info(f"Snapshot cache hit for {sym}")
        except Exception:
            pass
        return cached

    try:
        log.info(f"Snapshot cache miss for {sym}")
    except Exception:
        pass

    url = f"{_data_base_url()}/v2/stocks/{sym}/snapshot"
    data0 = safe_alpaca_call_sync(_request_governed, "GET", url, None)
    if isinstance(data0, dict):
        _snapshot_cache.set(ck, data0)
        return data0
    raise AlpacaRequestError("Snapshot unavailable")


def get_snapshots_batch(symbols: List[str]) -> Dict[str, Any]:
    """Fetch snapshots for multiple symbols in one request.

    Returns raw Alpaca snapshot payload map: {SYM: snapshot_dict}
    """
    clean: List[str] = []
    for s in symbols or []:
        try:
            clean.append(_normalize_symbol(str(s)))
        except Exception:
            continue
    clean = list(dict.fromkeys(clean))
    if not clean:
        return {}

    ck = "snaps:" + ",".join(clean[:200])
    cached = _snapshots_batch_cache.get(ck)
    if isinstance(cached, dict):
        return cached

    url = f"{_data_base_url()}/v2/stocks/snapshots"
    data0 = None
    for feed in _feed_candidates():
        params = {"symbols": ",".join(clean[:200]), "feed": feed}
        try:
            data0 = safe_alpaca_call_sync(_request_governed, "GET", url, params)
        except Exception:
            data0 = None
        if isinstance(data0, dict):
            break

    if not isinstance(data0, dict):
        return {}

    snaps = data0.get("snapshots")
    if not isinstance(snaps, dict):
        return {}
    try:
        _snapshots_batch_cache.set(ck, snaps)
    except Exception:
        pass
    return snaps


def _get_latest_trade_price(symbol: str) -> Optional[float]:
    sym = _normalize_symbol(symbol)
    # Prefer SDK
    client = _alpaca_sdk_client()
    if client is not None and StockLatestTradeRequest is not None:
        try:
            for feed in _feed_candidates():
                try:
                    req = StockLatestTradeRequest(symbol_or_symbols=sym, feed=feed)
                    resp = client.get_stock_latest_trade(req)
                    trade = resp.get(sym) if isinstance(resp, dict) else None
                    price = getattr(trade, "price", None) if trade is not None else None
                    if price is not None:
                        return float(price)
                except Exception:
                    continue
        except Exception:
            pass

    # REST fallback
    url = f"{_data_base_url()}/v2/stocks/{sym}/trades/latest"
    params = {"feed": os.getenv("ALPACA_DATA_FEED", "iex")}
    data0 = safe_alpaca_call_sync(_request_governed, "GET", url, params)
    try:
        t = data0.get("trade") if isinstance(data0, dict) else None
        if isinstance(t, dict) and t.get("p") is not None:
            return float(t.get("p"))
    except Exception:
        return None
    return None


def get_snapshot_normalized(symbol: str, *, allow_mock_dev: bool = True) -> Dict[str, Any]:
    """Normalized snapshot contract.

    Response schema:
    {
      symbol, last_price, percent_change, volume, vwap, prev_close, session, updated_at,
      snapshot_available, reason
    }

    Fallback priority:
    1) Alpaca snapshot
    2) Alpaca latest trade
    3) Cached snapshot
    4) Mock (dev only)
    """
    validate_market_env()
    sym = _normalize_symbol(symbol)
    out: Dict[str, Any] = {
        "symbol": sym,
        "last_price": None,
        "percent_change": None,
        "volume": None,
        "vwap": None,
        "prev_close": None,
        "session": None,
        "open": None,
        "close": None,
        "updated_at": _iso_now(),
        "snapshot_available": False,
        "reason": "market_data_unavailable",
    }

    snap: Optional[Dict[str, Any]] = None
    last_err: Optional[str] = None

    # (1) Snapshot (SDK preferred)
    client = _alpaca_sdk_client()
    if client is not None and StockSnapshotRequest is not None:
        try:
            for feed in _feed_candidates():
                try:
                    req = StockSnapshotRequest(symbol_or_symbols=sym, feed=feed)
                    resp = client.get_stock_snapshot(req)
                    snap = resp.get(sym) if isinstance(resp, dict) else None
                    if snap is None:
                        continue
                    try:
                        lt = getattr(snap, "latest_trade", None)
                        dq = getattr(snap, "daily_bar", None)
                        pv = getattr(snap, "previous_daily_bar", None)
                        last = getattr(lt, "price", None) if lt is not None else None
                        # Required contract:
                        # last price = snapshot.latestTrade.p (SDK: latest_trade.price)
                        # open/close/volume come from daily_bar
                        o0 = getattr(dq, "open", None) if dq is not None else None
                        c0 = getattr(dq, "close", None) if dq is not None else None
                        prev_close = getattr(pv, "close", None) if pv is not None else None
                        vol = getattr(dq, "volume", None) if dq is not None else None
                        vwap = getattr(dq, "vwap", None) if dq is not None else None
                        out["last_price"] = float(last) if last is not None else None
                        out["open"] = float(o0) if o0 is not None else None
                        out["close"] = float(c0) if c0 is not None else None
                        out["prev_close"] = float(prev_close) if prev_close is not None else None
                        out["volume"] = int(vol) if vol is not None else None
                        out["vwap"] = float(vwap) if vwap is not None else None
                        if out["last_price"] is not None and out["open"] not in (None, 0.0):
                            out["percent_change"] = float((float(out["last_price"]) - float(out["open"])) / float(out["open"]) * 100.0)
                        out["snapshot_available"] = bool(out["last_price"] is not None and float(out["last_price"] or 0.0) > 0.0)
                        if out["snapshot_available"]:
                            out["reason"] = ""
                            return out
                    except Exception:
                        continue
                except Exception:
                    continue
        except Exception as e:
            last_err = str(e)[:160]

    # REST snapshot
    try:
        snap = get_snapshot(sym)
    except Exception as e:
        snap = None
        last_err = str(e)[:160]

    if isinstance(snap, dict):
        try:
            bar = snap.get("dailyBar") if isinstance(snap.get("dailyBar"), dict) else {}
            prev = snap.get("prevDailyBar") if isinstance(snap.get("prevDailyBar"), dict) else {}
            lt = snap.get("latestTrade") if isinstance(snap.get("latestTrade"), dict) else {}
            last = _to_float(lt.get("p"))
            # Snapshot missing latestTrade is treated as unavailable for trade-plan purposes,
            # but snapshot endpoint can still show a fallback close.
            if last is None:
                last = _to_float(bar.get("c"))
            o0 = _to_float(bar.get("o"))
            c0 = _to_float(bar.get("c"))
            prev_close = _to_float(prev.get("c"))
            out["last_price"] = float(last) if last is not None else None
            out["open"] = float(o0) if o0 is not None else None
            out["close"] = float(c0) if c0 is not None else None
            out["prev_close"] = float(prev_close) if prev_close is not None else None
            out["volume"] = int(bar.get("v")) if bar.get("v") is not None else None
            out["vwap"] = _to_float(bar.get("vw"))
            if out["last_price"] is not None and out["open"] not in (None, 0.0):
                out["percent_change"] = float((float(out["last_price"]) - float(out["open"])) / float(out["open"]) * 100.0)
            out["snapshot_available"] = bool(out["last_price"] is not None and float(out["last_price"] or 0.0) > 0.0)
            if out["snapshot_available"]:
                out["reason"] = ""
                return out
        except Exception:
            pass

    # Snapshot missing: fallback to latest bar close for UI purposes.
    # IMPORTANT: this does NOT set snapshot_available=True.
    try:
        bars = get_bars(sym, timeframe="1Day", limit=1)
        candles = bars.get("candles") if isinstance(bars, dict) else []
        if isinstance(candles, list) and candles:
            c0 = _to_float(candles[-1].get("c"))
            if c0 is not None and float(c0) > 0.0:
                out["last_price"] = float(c0)
                out["close"] = float(c0)
                out["reason"] = "snapshot_missing_fallback_close"
    except Exception:
        pass

    # (2) Latest trade
    try:
        last_px = _get_latest_trade_price(sym)
        if last_px is not None and float(last_px) > 0.0:
            out["last_price"] = float(last_px)
            # Latest trade is a price fallback, but still not a full snapshot.
            out["snapshot_available"] = False
            out["reason"] = "latest_trade_fallback"
            return out
    except Exception as e:
        last_err = str(e)[:160]

    # (3) Cached snapshot (stale accepted)
    try:
        cached = _snapshot_cache.get(_snapshot_cache_key(sym))
        if isinstance(cached, dict):
            bar = cached.get("dailyBar") if isinstance(cached.get("dailyBar"), dict) else {}
            lt = cached.get("latestTrade") if isinstance(cached.get("latestTrade"), dict) else {}
            last = _to_float(lt.get("p"))
            if last is None:
                last = _to_float(bar.get("c"))
            if last is not None and float(last) > 0.0:
                out["last_price"] = float(last)
                out["snapshot_available"] = True
                out["reason"] = "cached_snapshot"
                return out
    except Exception:
        pass

    # (4) Mock (dev only)
    dev = str(os.getenv("STACKIQ_DEV_MODE", "0") or "0").strip().lower() in ("1", "true", "yes", "on")
    if allow_mock_dev and dev:
        try:
            base = 100.0 + (random.random() * 50.0)
            out["last_price"] = float(round(base, 2))
            out["prev_close"] = float(round(base * (0.995 + random.random() * 0.01), 2))
            if out["prev_close"] not in (None, 0.0):
                out["percent_change"] = float(round((float(out["last_price"]) - float(out["prev_close"])) / float(out["prev_close"]) * 100.0, 2))
            out["volume"] = int(1_000_000 + random.randint(0, 2_000_000))
            out["vwap"] = float(round(float(out["last_price"]) * (0.998 + random.random() * 0.004), 2))
            out["session"] = "mock"
            out["updated_at"] = _iso_now()
            out["snapshot_available"] = True
            out["reason"] = "mock_data"
            return out
        except Exception:
            pass

    if last_err:
        out["reason"] = "market_data_unavailable"
    return out


def get_bars_normalized(symbol: str, timeframe: str, limit: int) -> Dict[str, Any]:
    """Normalized bars output for analysis engines.

    Returns:
    {symbol, timeframe, bars: [{open,high,low,close,volume,timestamp}], updated_at}
    """
    sym = _normalize_symbol(symbol)
    tf = str(timeframe or "1Day").strip() or "1Day"
    payload = get_bars(sym, timeframe=tf, limit=int(limit or 100))
    candles = payload.get("candles") if isinstance(payload, dict) else []
    out_bars: List[Dict[str, Any]] = []
    if isinstance(candles, list):
        for c in candles:
            if not isinstance(c, dict):
                continue
            out_bars.append(
                {
                    "open": _to_float(c.get("o")),
                    "high": _to_float(c.get("h")),
                    "low": _to_float(c.get("l")),
                    "close": _to_float(c.get("c")),
                    "volume": int(c.get("v") or 0),
                    "timestamp": str(c.get("t") or ""),
                }
            )
    return {"symbol": sym, "timeframe": tf, "bars": out_bars, "updated_at": _iso_now()}


def get_snapshot_simple(symbol: str) -> Dict[str, Any]:
    """Normalized snapshot contract for UI and movers.

    Returns:
    {"symbol": str, "last": float, "prev_close": float, "change_pct": float, "volume": int}
    """
    sym = _normalize_symbol(symbol)
    snap = get_snapshot(sym)
    bar = snap.get("dailyBar") if isinstance(snap.get("dailyBar"), dict) else {}
    prev = snap.get("prevDailyBar") if isinstance(snap.get("prevDailyBar"), dict) else {}
    lt = snap.get("latestTrade") if isinstance(snap.get("latestTrade"), dict) else {}

    last = _to_float(lt.get("p"))
    if last is None:
        last = _to_float(bar.get("c"))
    prev_close = _to_float(prev.get("c"))
    volume_i = 0
    try:
        v0 = bar.get("v")
        if v0 is not None:
            volume_i = int(v0)
    except Exception:
        volume_i = 0

    change_pct = 0.0
    try:
        if last is not None and prev_close is not None and float(prev_close) != 0.0:
            change_pct = (float(last) - float(prev_close)) / float(prev_close) * 100.0
    except Exception:
        change_pct = 0.0

    return {
        "symbol": sym,
        "last": float(last or 0.0),
        "prev_close": float(prev_close or 0.0),
        "change_pct": float(change_pct or 0.0),
        "volume": int(volume_i or 0),
    }


def get_latest_quote(symbol: str) -> Dict[str, Any]:
    sym = _normalize_symbol(symbol)
    bars = get_bars(sym, timeframe="1Day", limit=2)
    candles = bars["candles"]
    if not candles:
        raise AlpacaRequestError(f"No market data available for {sym}")
    last_daily = candles[-1]
    prev_daily = candles[-2] if len(candles) >= 2 else None
    prev_close = _to_float(prev_daily.get("c")) if prev_daily else _to_float(last_daily.get("c"))
    if prev_close is None:
        prev_close = _to_float(last_daily.get("c")) or 0.0
    live_price: Optional[float] = None
    try:
        snap = get_snapshot(sym)
        latest_trade = snap.get("latestTrade") or {}
        latest_quote = snap.get("latestQuote") or {}
        daily_bar = snap.get("dailyBar") or {}
        live_price = _to_float(latest_trade.get("p"))
        if live_price is None:
            bid = _to_float(latest_quote.get("bp"))
            ask = _to_float(latest_quote.get("ap"))
            if bid is not None and ask is not None and ask >= bid and ask > 0:
                live_price = (bid + ask) / 2.0
            else:
                live_price = _to_float(daily_bar.get("c"))
    except Exception as e:
        log.info(f"Snapshot unavailable for {sym}, using daily close: {e}")
    price = live_price if live_price is not None else _to_float(last_daily.get("c"))
    if price is None:
        raise AlpacaRequestError(f"No usable price available for {sym}")
    change = price - prev_close
    change_pct = (change / prev_close * 100.0) if prev_close else 0.0
    return {
        "symbol": sym,
        "price": round(float(price), 2),
        "change": round(float(change), 2),
        "changePercent": round(float(change_pct), 2),
        "timestamp": _iso_now(),
    }


def get_bars(symbol: str, timeframe: str, limit: int) -> Dict[str, Any]:
    sym = _normalize_symbol(symbol)
    tf = (timeframe or "1Day").strip()
    url = f"{_data_base_url()}/v2/stocks/{sym}/bars"
    lim0 = int(limit)
    if lim0 < 1:
        lim0 = 1
    # Alpaca may return only the most recent bar if no explicit range is provided.
    # Provide a reasonable start/end window sized to the requested limit.
    now_utc = datetime.now(timezone.utc)
    lookback_days = 400
    try:
        if str(tf).strip().lower() in ("1day", "day", "d", "1d"):
            lookback_days = max(30, min(900, int(lim0) * 3))
        else:
            lookback_days = 10
    except Exception:
        lookback_days = 400
    start_dt = now_utc - timedelta(days=int(lookback_days))
    start_iso = start_dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    end_iso = now_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    base_params = {
        "timeframe": tf,
        "start": start_iso,
        "end": end_iso,
        "limit": int(lim0),
        "adjustment": "raw",
        "sort": "asc",
    }

    cache = _bars_cache_for_timeframe(tf)
    ck = _bars_cache_key(sym, tf, int(limit))
    cached2 = cache.get(ck)
    min_needed = max(1, min(50, int(lim0)))
    if isinstance(cached2, list) and len(cached2) >= int(min_needed):
        try:
            log.info(f"Bars cache hit for {sym} {tf} ({len(cached2)})")
        except Exception:
            pass
        return {"symbol": sym, "candles": cached2}

    try:
        log.info(f"Bars cache miss for {sym} {tf}")
    except Exception:
        pass

    cache_key = (sym, tf)

    def _get_cached() -> List[Dict[str, Any]]:
        try:
            item = _bars_cache.get(cache_key)
            if not item:
                return []
            ts = float(item.get("timestamp") or 0)
            if ts <= 0:
                return []
            age = time.time() - ts
            if age > _BARS_CACHE_TTL_SECONDS:
                return []
            c = item.get("candles")
            return c if isinstance(c, list) and c else []
        except Exception:
            return []

    def _set_cache(candles: List[Dict[str, Any]]) -> None:
        if not isinstance(candles, list) or not candles:
            return
        try:
            _bars_cache[cache_key] = {
                "candles": candles,
                "timestamp": int(time.time()),
            }
        except Exception:
            pass

    def _request_with_backoff() -> dict:
        # Gentle pacing + retry a couple times.
        # On 429 we retry at most 2 times (3 total attempts), then fall back to cache.
        last_err = None
        feeds = _feed_candidates()
        for feed in feeds:
            params = dict(base_params)
            params["feed"] = feed
            for attempt in range(0, 3):
                try:
                    time.sleep(0.15 + random.random() * 0.10)
                except Exception:
                    pass
                try:
                    return _request_governed("GET", url, params=params)
                except AlpacaRateLimitError as e:
                    last_err = e
                    if attempt >= 2:
                        break
                    continue
                except AlpacaRequestError as e:
                    last_err = e
                    continue
        if last_err is not None:
            raise last_err
        raise AlpacaRequestError("Unknown error calling Alpaca bars")

    try:
        data = _request_with_backoff()
        bars = data.get("bars") or []
        candles: List[Dict[str, Any]] = []
        for b in bars:
            candles.append(
                {
                    "t": b.get("t"),
                    "o": b.get("o"),
                    "h": b.get("h"),
                    "l": b.get("l"),
                    "c": b.get("c"),
                    "v": b.get("v"),
                }
            )

        if candles and len(candles) >= int(min_needed):
            _set_cache(candles)
            try:
                cache.set(ck, candles)
            except Exception:
                pass
            return {"symbol": sym, "candles": candles}

        cached = _get_cached()
        if isinstance(cached, list) and len(cached) >= int(min_needed):
            try:
                if candles and len(candles) < 50:
                    log.warning(f"Alpaca returned short bars for {sym} {tf} ({len(candles)}); using cached candles ({len(cached)})")
                elif not candles:
                    log.warning(f"Alpaca returned empty bars for {sym} {tf}; using cached candles ({len(cached)})")
            except Exception:
                pass
            return {"symbol": sym, "candles": cached}

        cached3 = cache.get(ck)
        if isinstance(cached3, list) and len(cached3) >= int(min_needed):
            try:
                log.warning(f"Alpaca returned short/empty bars for {sym} {tf}; using TTL cache ({len(cached3)})")
            except Exception:
                pass
            return {"symbol": sym, "candles": cached3}

        return {"symbol": sym, "candles": []}
    except (AlpacaRateLimitError, AlpacaAuthError) as e:
        cached = _get_cached()
        if cached:
            # Stay quiet on rate limits; cached bars are expected behavior during 429.
            try:
                if not isinstance(e, AlpacaRateLimitError):
                    _warn_auth_throttled(f"Alpaca bars fallback for {sym} {tf} ({type(e).__name__}); using cached candles ({len(cached)})")
            except Exception:
                pass
            return {"symbol": sym, "candles": cached}
        if isinstance(e, AlpacaAuthError):
            _warn_auth_throttled(f"Alpaca bars failed for {sym} {tf} ({type(e).__name__}); no cache available")
        else:
            log.warning(f"Alpaca bars failed for {sym} {tf} ({type(e).__name__}); no cache available")
        return {"symbol": sym, "candles": []}
    except AlpacaRequestError as e:
        cached = _get_cached()
        if cached:
            log.warning(f"Alpaca bars fallback for {sym} {tf} (request error); using cached candles ({len(cached)}): {e}")
            return {"symbol": sym, "candles": cached}
        raise


def get_bars_batch(symbols: List[str], timeframe: str, limit: int) -> Dict[str, List[Dict[str, Any]]]:
    tf = (timeframe or "1Day").strip() or "1Day"
    lim = int(limit or 30)
    if lim < 1:
        lim = 1

    cache = _bars_cache_for_timeframe(tf)
    out: Dict[str, List[Dict[str, Any]]] = {}

    tf0 = str(tf).strip().lower()
    is_daily = tf0 in ("1day", "day", "d", "1d")
    if is_daily:
        # Product requirement: pull enough daily history for robust scoring.
        lim = max(200, int(lim))
    min_needed = max(1, int(lim))

    def _is_supported_symbol(s: str) -> bool:
        if not s:
            return False
        for ch in s:
            if ch == "." or ch == "-":
                continue
            if "A" <= ch <= "Z":
                continue
            return False
        if s.startswith(".") or s.endswith(".") or s.startswith("-") or s.endswith("-"):
            return False
        if ".." in s or "--" in s or ".-" in s or "-." in s:
            return False
        return True

    clean: List[str] = []
    for s in symbols or []:
        try:
            sym = _normalize_symbol(str(s))
        except Exception:
            continue
        if not _is_supported_symbol(sym):
            continue
        clean.append(sym)

    clean = list(dict.fromkeys(clean))
    if not clean:
        return out

    missing: List[str] = []
    for sym in clean:
        ck = _bars_cache_key(sym, tf, lim)
        cached = cache.get(ck)
        if isinstance(cached, list) and len(cached) >= int(min_needed):
            out[sym] = cached
        else:
            missing.append(sym)

    if not missing:
        return out

    try:
        log.info(f"Bars batch fetch size={len(missing)} timeframe={tf} limit={lim}")
    except Exception:
        pass

    if not is_daily:
        return out

    if not _ALPACA_PY_AVAILABLE or StockBarsRequest is None or TimeFrame is None:
        return out

    key = (os.getenv("ALPACA_API_KEY") or "").strip()
    secret = (os.getenv("ALPACA_SECRET_KEY") or "").strip()
    if not key or not secret:
        return out

    try:
        data_client = StockHistoricalDataClient(api_key=key, secret_key=secret)
    except Exception:
        return out

    end = datetime.now(timezone.utc)
    start = end - timedelta(days=365)

    def _feeds_for(primary: str) -> List[str]:
        p = (primary or "").strip().lower()
        out_f: List[str] = []
        if p:
            out_f.append(p)
        if p != "iex":
            out_f.append("iex")
        if not out_f:
            out_f = ["iex"]
        # preserve order; unique
        seen = set()
        uniq: List[str] = []
        for f in out_f:
            if f in seen:
                continue
            seen.add(f)
            uniq.append(f)
        return uniq

    feed0 = (os.getenv("ALPACA_DATA_FEED") or "").strip().lower()
    feeds_to_try = _feeds_for(feed0)

    def _extract_symbol_map(resp_obj: Any) -> Dict[str, List[Any]]:
        # Alpaca SDK may provide either .data (dict of lists) or .df (pandas dataframe-like).
        data0 = getattr(resp_obj, "data", None)
        if isinstance(data0, dict):
            def _coerce_bars(v: Any) -> List[Any]:
                if v is None:
                    return []
                if isinstance(v, list):
                    return v
                # alpaca-py may return BarSet/Sequence-like values that are iterable but not lists.
                try:
                    if isinstance(v, (str, bytes, dict)):
                        return []
                    if isinstance(v, Iterable):
                        return list(v)
                except Exception:
                    return []
                return []

            out0: Dict[str, List[Any]] = {}
            try:
                for k, v in data0.items():
                    kk = str(k).strip().upper()
                    if not kk:
                        continue
                    out0[kk] = _coerce_bars(v)
                return out0
            except Exception:
                try:
                    for k, v in (data0 or {}).items():
                        kk = str(k).strip().upper()
                        if not kk:
                            continue
                        out0[kk] = _coerce_bars(v)
                except Exception:
                    pass
                return out0

        df = getattr(resp_obj, "df", None)
        if df is None:
            return {}
        try:
            if bool(getattr(df, "empty")):
                return {}
        except Exception:
            pass

        # MultiIndex: (symbol, timestamp). Group by symbol (level=0).
        try:
            import pandas as pd  # type: ignore
        except Exception:
            pd = None  # type: ignore

        try:
            if pd is not None and isinstance(getattr(df, "index", None), pd.MultiIndex):
                symbol_groups = {symbol: group.droplevel(0) for symbol, group in df.groupby(level=0)}
            else:
                symbol_groups = {"": df}
        except Exception:
            return {}

        out_map: Dict[str, List[Any]] = {}
        for sym, g in (symbol_groups or {}).items():
            symu = str(sym or "").strip().upper()
            if not symu:
                continue
            try:
                # g index is timestamp after droplevel(0)
                idx = getattr(g, "index", None)
                if idx is None:
                    out_map[symu] = []
                    continue

                cols = set([str(c) for c in list(getattr(g, "columns", []) or [])])
                def _col(name: str, *alts: str) -> Optional[str]:
                    if name in cols:
                        return name
                    for a in alts:
                        if a in cols:
                            return a
                    return None

                c_open = _col("open", "o")
                c_high = _col("high", "h")
                c_low = _col("low", "l")
                c_close = _col("close", "c")
                c_vol = _col("volume", "v")

                bars: List[Dict[str, Any]] = []
                # Iterate rows; index is timestamp
                for ts, row in g.iterrows():
                    bars.append(
                        {
                            "timestamp": ts,
                            "open": row.get(c_open) if c_open else None,
                            "high": row.get(c_high) if c_high else None,
                            "low": row.get(c_low) if c_low else None,
                            "close": row.get(c_close) if c_close else None,
                            "volume": row.get(c_vol) if c_vol else None,
                        }
                    )
                out_map[symu] = bars
            except Exception:
                out_map[symu] = []

        return out_map

    req_chunk = 200

    def _fetch_with_feeds(feeds: List[str]) -> Dict[str, List[Dict[str, Any]]]:
        bars_dict_local: Dict[str, List[Dict[str, Any]]] = {s: (out.get(s) if isinstance(out.get(s), list) else []) for s in clean}
        for j in range(0, len(missing), req_chunk):
            chunk = missing[j : j + req_chunk]
            if not chunk:
                continue
            # Alpaca batch bars limit applies to the entire response, not per symbol.
            req_limit = int(max(1, int(lim)) * int(len(chunk) or 1))
            best_sym_map: Optional[Dict[str, List[Any]]] = None
            best_feed: Optional[str] = None
            best_any = -1
            best_total = -1

            for feed in feeds:
                try:
                    req = StockBarsRequest(
                        symbol_or_symbols=chunk,
                        timeframe=TimeFrame.Day,
                        start=start,
                        end=end,
                        limit=req_limit,
                        feed=feed,
                    )
                except TypeError:
                    req = StockBarsRequest(
                        symbol_or_symbols=chunk,
                        timeframe=TimeFrame.Day,
                        start=start,
                        end=end,
                        limit=req_limit,
                    )

                resp = safe_alpaca_call_sync(data_client.get_stock_bars, req)
                if resp is None:
                    continue
                sym_map = _extract_symbol_map(resp)
                if not isinstance(sym_map, dict):
                    continue

                any_bars = 0
                total_bars = 0
                try:
                    for _k, _v in sym_map.items():
                        if not isinstance(_v, list):
                            continue
                        n = len(_v)
                        if n > 0:
                            any_bars += 1
                            total_bars += n
                except Exception:
                    any_bars = 0
                    total_bars = 0

                if any_bars > best_any or (any_bars == best_any and total_bars > best_total):
                    best_any = int(any_bars)
                    best_total = int(total_bars)
                    best_sym_map = sym_map
                    best_feed = feed

            if best_sym_map is None:
                try:
                    log.warning({"bars_batch_fetch": "failed", "reason": "sdk_call_returned_none", "chunk_size": len(chunk)})
                except Exception:
                    pass
                continue

            # Only log per-chunk feed decisions when things look very wrong.
            if best_any <= 1:
                try:
                    log.warning(
                        {
                            "bars_batch_chunk_low_coverage": True,
                            "chunk_size": int(len(chunk)),
                            "chosen_feed": str(best_feed),
                            "symbols_with_any_bars": int(best_any),
                            "total_bars_returned": int(best_total),
                        }
                    )
                except Exception:
                    pass

            sym_map = best_sym_map

            for sym in chunk:
                symu = str(sym or "").strip().upper()
                bars_any = sym_map.get(symu, []) if isinstance(sym_map, dict) else []
                if not isinstance(bars_any, list):
                    bars_any = []

                # If bars are actual Bar objects, sort by timestamp.
                try:
                    def _bar_ts_key(b: Any):
                        if isinstance(b, dict):
                            return b.get("timestamp")
                        return getattr(b, "timestamp", None)

                    bars_sorted = sorted(bars_any, key=_bar_ts_key)
                except Exception:
                    bars_sorted = list(bars_any)

                candles: List[Dict[str, Any]] = []
                for b in bars_sorted:
                    try:
                        ts = getattr(b, "timestamp", None)
                        if ts is None:
                            # DataFrame tuple path: try infer timestamp from index tuple.
                            idx = getattr(b, "Index", None)
                            if isinstance(idx, tuple) and len(idx) >= 2:
                                ts = idx[1]
                        if isinstance(b, dict):
                            # Dataframe extraction path (dict rows)
                            ts2 = b.get("timestamp")
                            if ts is None and ts2 is not None:
                                ts = ts2

                        # Robust timestamp stringification: never drop a row just because
                        # the timestamp type isn't a datetime with .replace().
                        ts_s = None
                        if ts is not None:
                            try:
                                if hasattr(ts, "to_pydatetime"):
                                    ts = ts.to_pydatetime()
                            except Exception:
                                pass
                            try:
                                if hasattr(ts, "replace") and hasattr(ts, "isoformat"):
                                    ts_s = ts.replace(microsecond=0).isoformat().replace("+00:00", "Z")
                                else:
                                    ts_s = str(ts)
                            except Exception:
                                ts_s = str(ts)

                        o = getattr(b, "open", None)
                        h = getattr(b, "high", None)
                        l = getattr(b, "low", None)
                        c = getattr(b, "close", None)
                        v = getattr(b, "volume", None)
                        if o is None and isinstance(b, dict):
                            o = b.get("open")
                            h = b.get("high")
                            l = b.get("low")
                            c = b.get("close")
                            v = b.get("volume")

                        candles.append({"t": ts_s, "o": o, "h": h, "l": l, "c": c, "v": v})
                    except Exception:
                        continue

                # Ensure oldest->newest ordering
                try:
                    candles = sorted(candles, key=lambda x: str(x.get("t") or ""))
                except Exception:
                    pass

                bars_dict_local[symu] = candles

                if candles and len(candles) >= int(min_needed):
                    ck = _bars_cache_key(symu, tf, lim)
                    cache.set(ck, candles)
        return bars_dict_local

    bars_dict = _fetch_with_feeds(feeds_to_try)

    def _coverage_stats(bd: Dict[str, List[Dict[str, Any]]]) -> Dict[str, int]:
        requested = int(len(clean))
        any_bars = 0
        total_bars = 0
        for s in clean:
            symu = str(s).strip().upper()
            n = len(bd.get(symu, []) or [])
            if n > 0:
                any_bars += 1
                total_bars += int(n)
        zero_bars = max(0, requested - any_bars)
        return {
            "symbols_requested": requested,
            "symbols_returned_any_bars": int(any_bars),
            "symbols_returned_zero_bars": int(zero_bars),
            "total_bars_returned": int(total_bars),
        }

    stats0 = _coverage_stats(bars_dict)
    try:
        ratio = float(stats0.get("symbols_returned_any_bars", 0)) / float(max(1, stats0.get("symbols_requested", 0)))
    except Exception:
        ratio = 0.0

    if ratio < 0.10:
        # Near-total empties: retry with SIP if possible.
        try:
            log.warning({"bars_batch_low_coverage": True, "coverage_ratio": ratio, "primary_feed": feeds_to_try[:1]})
        except Exception:
            pass
        if "sip" not in feeds_to_try:
            bars_dict_sip = _fetch_with_feeds(["sip", "iex"])
            stats1 = _coverage_stats(bars_dict_sip)
            try:
                ratio1 = float(stats1.get("symbols_returned_any_bars", 0)) / float(max(1, stats1.get("symbols_requested", 0)))
            except Exception:
                ratio1 = 0.0
            if ratio1 > ratio:
                bars_dict = bars_dict_sip
                stats0 = stats1
                ratio = ratio1
        # If still near-total empties, shrink to only symbols that actually returned bars.
        if ratio < 0.10:
            try:
                allowed = [s for s in clean if len(bars_dict.get(str(s).strip().upper(), []) or []) > 0]
                bars_dict = {str(s).strip().upper(): bars_dict.get(str(s).strip().upper(), []) for s in allowed}
            except Exception:
                pass

    # Always log stats for the final map actually returned.
    try:
        stats0 = _coverage_stats(bars_dict)
    except Exception:
        pass

    try:
        log.info(stats0)
    except Exception:
        pass

    log_per_symbol = str(os.getenv("STACKIQ_LOG_BARS_PER_SYMBOL", "0") or "0").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )
    if log_per_symbol:
        for symbol in clean:
            symu = str(symbol or "").strip().upper()
            if not symu:
                continue
            try:
                log.info({"symbol": symu, "bars_count": len(bars_dict.get(symu, []) or [])})
            except Exception:
                pass
    else:
        # Avoid log spam in large scans; still provide visibility into data coverage.
        try:
            zeros = [s for s in clean if len(bars_dict.get(str(s).strip().upper(), []) or []) == 0]
            if zeros:
                log.info({"bars_returned_zero": len(zeros), "zero_symbols_sample": zeros[:25]})
        except Exception:
            pass

    try:
        spy_n = len((bars_dict.get("SPY") or [])) if isinstance(bars_dict, dict) else 0
        log.info({"diagnostic_symbol": "SPY", "bars_returned": int(spy_n)})
    except Exception:
        pass

    return bars_dict


def get_news(limit: int) -> List[Dict[str, Any]]:
    url = f"{_data_base_url()}/v1beta1/news"
    params = {"limit": int(limit), "sort": "desc"}
    data = _request("GET", url, params=params)
    items = data.get("news") or data.get("items", [])
    out: List[Dict[str, Any]] = []
    for n in items[:limit]:
        out.append(
            {
                "title": n.get("headline") or n.get("title") or "",
                "url": n.get("url") or "",
                "source": n.get("source") or "",
                "publishedAt": n.get("created_at") or n.get("published_at") or "",
                "summary": (n.get("summary") or "")[:500],
            }
        )
    return out


_LIQUID_SYMBOLS = [
    "AAPL", "MSFT", "NVDA", "TSLA", "META",
    "AMD", "AMZN", "GOOGL", "SPY", "QQQ",
    "NFLX", "JPM", "XOM"
]


def get_top_movers(limit: int) -> List[Dict[str, Any]]:
    top_n = max(1, min(int(limit or 10), 50))
    try:
        url = f"{_data_base_url()}/v1beta1/screener/stocks/movers"
        data = _request("GET", url, params={"top": top_n})
        out: List[Dict[str, Any]] = []
        def _coerce(x: Dict[str, Any]) -> Optional[Dict[str, Any]]:
            try:
                sym = _normalize_symbol(x.get("symbol", ""))
            except Exception:
                return None
            try:
                price = float(x.get("price", 0))
                change = float(x.get("change", 0))
                cp = float(x.get("change_percent", 0))
            except Exception:
                price, change, cp = 0.0, 0.0, 0.0
            return {
                "symbol": sym,
                "price": round(price, 2),
                "change": round(change, 2),
                "changePercent": round(cp, 2),
                "timestamp": _iso_now(),
            }
        for row in (data.get("gainers") or []):
            if row.get("symbol"):
                item = _coerce(row)
                if item is not None:
                    out.append(item)
        for row in (data.get("losers") or []):
            if row.get("symbol"):
                item = _coerce(row)
                if item is not None:
                    out.append(item)
        out.sort(key=lambda x: abs(x["changePercent"]), reverse=True)
        return out[: int(limit)]
    except Exception as e:
        try:
            # Throttle noisy fallback logs in refresh loops.
            if not hasattr(get_top_movers, "_last_fallback_log_ts"):
                setattr(get_top_movers, "_last_fallback_log_ts", 0.0)
            last = float(getattr(get_top_movers, "_last_fallback_log_ts") or 0.0)
            now_ts = time.time()
            if (now_ts - last) > 180.0:
                setattr(get_top_movers, "_last_fallback_log_ts", now_ts)
                log.info(f"Movers endpoint unavailable, fallback active: {e}")
        except Exception:
            pass
        quotes: List[Dict[str, Any]] = []
        for s in _LIQUID_SYMBOLS:
            try:
                quotes.append(get_latest_quote(s))
            except Exception:
                pass
        quotes.sort(key=lambda x: abs(x["changePercent"]), reverse=True)
        return quotes[: int(limit)]


# ======================================================================
# ADDITIVE: HISTORICAL DATA ACCESS FOR BACKTESTING (NASDAQ / FALLBACK)
# ======================================================================

def get_historical_daily(
    symbol: str,
    start: str,
    end: str,
    source: str = "nasdaq",
) -> List[Dict[str, Any]]:
    """
    Unified historical daily candles for backtesting.
    - Primary intent: NASDAQ (deep history, cached upstream)
    - Fallback: Alpaca daily bars
    Returns list of {date, open, high, low, close, volume}
    """
    sym = _normalize_symbol(symbol)

    # NOTE: NASDAQ integration placeholder.
    # This function is intentionally source-agnostic so backtest logic
    # does NOT care where data came from.

    if source == "alpaca":
        bars = get_bars(sym, timeframe="1Day", limit=10000)
        out: List[Dict[str, Any]] = []
        for c in bars.get("candles", []):
            out.append(
                {
                    "date": c.get("t"),
                    "open": _to_float(c.get("o")),
                    "high": _to_float(c.get("h")),
                    "low": _to_float(c.get("l")),
                    "close": _to_float(c.get("c")),
                    "volume": _to_float(c.get("v")),
                }
            )
        return out

    # NASDAQ path (to be wired to real endpoint + cache)
    raise AlpacaRequestError(
        "NASDAQ historical source not yet configured. "
        "Set source='alpaca' or wire NASDAQ adapter."
    )