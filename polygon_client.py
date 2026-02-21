import os
import time
import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import requests

log = logging.getLogger("stackiq")


_POLYGON_BASE_URL = "https://api.polygon.io"

# In-memory caches
# market cap: 24h
_MARKET_CAP_CACHE: Dict[str, Tuple[float, Optional[int]]] = {}
_MARKET_CAP_TTL_S = 24.0 * 60.0 * 60.0

# unusual options: 15m
_UNUSUAL_OPTS_CACHE: Dict[str, Tuple[float, Dict[str, Any]]] = {}
_UNUSUAL_OPTS_TTL_S = 15.0 * 60.0

# news: 10m
_NEWS_CACHE: Dict[str, Tuple[float, List[Dict[str, Any]]]] = {}
_NEWS_TTL_S = 10.0 * 60.0


def _api_key() -> str:
    return (os.getenv("POLYGON_API_KEY") or "").strip()


def _cache_get(cache: Dict[str, Tuple[float, Any]], key: str, ttl_s: float) -> Any:
    if not key:
        return None
    try:
        item = cache.get(key)
    except Exception:
        return None
    if not item or not isinstance(item, tuple) or len(item) != 2:
        return None
    ts, val = item
    try:
        if (time.time() - float(ts or 0.0)) > float(ttl_s):
            try:
                cache.pop(key, None)
            except Exception:
                pass
            return None
    except Exception:
        return None
    return val


def _cache_set(cache: Dict[str, Tuple[float, Any]], key: str, value: Any) -> None:
    if not key:
        return
    try:
        cache[key] = (float(time.time()), value)
    except Exception:
        return


def _request_json(path: str, params: Optional[Dict[str, Any]] = None, timeout_s: float = 8.0) -> Optional[Dict[str, Any]]:
    key = _api_key()
    if not key:
        return None

    url = f"{_POLYGON_BASE_URL}{path}"
    q = dict(params or {})
    q["apiKey"] = key

    t0 = time.time()
    try:
        log.info(f"polygon request start: {path}")
    except Exception:
        pass

    def _do() -> requests.Response:
        return requests.get(url, params=q, timeout=float(timeout_s))

    r: Optional[requests.Response] = None
    try:
        r = _do()
        if r.status_code == 429:
            # single short backoff retry
            time.sleep(0.6)
            r = _do()
    except Exception as e:
        try:
            log.warning(f"polygon request error: {path} err={str(e)[:160]}")
        except Exception:
            pass
        return None

    dt = time.time() - t0
    try:
        log.info(f"polygon request end: {path} status={getattr(r, 'status_code', None)} elapsed_s={dt:.3f}")
    except Exception:
        pass

    if r is None:
        return None

    if r.status_code != 200:
        try:
            log.warning(f"polygon http error: {path} status={r.status_code} body={r.text[:180]}")
        except Exception:
            pass
        return None

    try:
        data = r.json()
    except Exception:
        return None

    return data if isinstance(data, dict) else None


def get_market_cap(ticker: str) -> Optional[int]:
    sym = str(ticker or "").strip().upper()
    if not sym:
        return None

    ck = f"mcap:{sym}"
    cached = _cache_get(_MARKET_CAP_CACHE, ck, _MARKET_CAP_TTL_S)
    if cached is not None or ck in _MARKET_CAP_CACHE:
        try:
            log.info(f"polygon market cap cache hit: {sym}")
        except Exception:
            pass
        return cached

    if not _api_key():
        _cache_set(_MARKET_CAP_CACHE, ck, None)
        return None

    data = _request_json(f"/v3/reference/tickers/{sym}", params={"market": "stocks"}, timeout_s=8.0)
    cap: Optional[int] = None
    try:
        res = data.get("results") if isinstance(data, dict) else None
        if isinstance(res, dict):
            mc = res.get("market_cap")
            if mc is not None:
                cap = int(float(mc))
    except Exception:
        cap = None

    _cache_set(_MARKET_CAP_CACHE, ck, cap)
    return cap


def _iso_date(d: datetime) -> str:
    return d.date().isoformat()


def _iso_utc(dt: datetime) -> str:
    try:
        return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    except Exception:
        return dt.replace(microsecond=0).isoformat()


def get_ticker_news(ticker: str) -> List[Dict[str, Any]]:
    sym = str(ticker or "").strip().upper()
    if not sym:
        return []

    ck = f"news:{sym}"
    cached = _cache_get(_NEWS_CACHE, ck, _NEWS_TTL_S)
    if isinstance(cached, list):
        try:
            log.info(f"polygon news cache hit: {sym}")
        except Exception:
            pass
        return cached

    if not _api_key():
        _cache_set(_NEWS_CACHE, ck, [])
        return []

    now = datetime.now(timezone.utc)
    gte = _iso_utc(now - timedelta(days=7))

    data = _request_json(
        "/v2/reference/news",
        params={
            # Polygon expects `ticker` for filtering. Keep `tickers` too for backwards
            # compatibility in case the API accepts both.
            "ticker": sym,
            "tickers": sym,
            "published_utc.gte": gte,
            "order": "desc",
            "sort": "published_utc",
            "limit": 20,
        },
        timeout_s=8.0,
    )

    results = data.get("results") if isinstance(data, dict) else None
    if not isinstance(results, list):
        _cache_set(_NEWS_CACHE, ck, [])
        return []

    out: List[Dict[str, Any]] = []
    for it in results[:20]:
        if not isinstance(it, dict):
            continue
        title = str(it.get("title") or "").strip()
        if not title:
            continue
        desc = str(it.get("description") or "").strip()
        url = str(it.get("article_url") or it.get("url") or "").strip()
        published_at = str(it.get("published_utc") or "").strip()
        pub = it.get("publisher") if isinstance(it.get("publisher"), dict) else {}
        source = str(pub.get("name") or it.get("source") or "").strip()

        out.append(
            {
                "title": title[:240],
                "summary": desc[:600],
                "source": source[:120],
                "published_at": published_at[:40],
                "url": url[:500],
                "sentiment_label": None,
            }
        )

    _cache_set(_NEWS_CACHE, ck, out)
    return out


def get_unusual_options(ticker: str) -> Dict[str, Any]:
    sym = str(ticker or "").strip().upper()
    if not sym:
        return {
            "unusual_options_score": 0,
            "call_put_ratio": None,
            "top_contracts": [],
            "notes": "unavailable",
        }

    ck = f"uopts:{sym}"
    cached = _cache_get(_UNUSUAL_OPTS_CACHE, ck, _UNUSUAL_OPTS_TTL_S)
    if isinstance(cached, dict):
        try:
            log.info(f"polygon unusual options cache hit: {sym}")
        except Exception:
            pass
        return cached

    if not _api_key():
        out0 = {
            "unusual_options_score": 0,
            "call_put_ratio": None,
            "top_contracts": [],
            "notes": "unavailable",
        }
        _cache_set(_UNUSUAL_OPTS_CACHE, ck, out0)
        return out0

    # Pull contracts expiring in 7-21 days
    now = datetime.now(timezone.utc)
    exp_gte = _iso_date(now + timedelta(days=7))
    exp_lte = _iso_date(now + timedelta(days=21))

    contracts = _request_json(
        "/v3/reference/options/contracts",
        params={
            "underlying_ticker": sym,
            "expiration_date.gte": exp_gte,
            "expiration_date.lte": exp_lte,
            "limit": 50,
            "sort": "expiration_date",
            "order": "asc",
        },
        timeout_s=8.0,
    )

    results = contracts.get("results") if isinstance(contracts, dict) else None
    if not isinstance(results, list) or not results:
        out1 = {
            "unusual_options_score": 0,
            "call_put_ratio": None,
            "top_contracts": [],
            "notes": "no_contracts",
        }
        _cache_set(_UNUSUAL_OPTS_CACHE, ck, out1)
        return out1

    # Best-effort: try snapshot chain endpoint for underlying
    snap_chain = _request_json(f"/v3/snapshot/options/{sym}", params={}, timeout_s=8.0)
    snap_results = snap_chain.get("results") if isinstance(snap_chain, dict) else None

    by_ticker: Dict[str, Dict[str, Any]] = {}
    if isinstance(snap_results, list):
        for it in snap_results:
            if not isinstance(it, dict):
                continue
            tkr = str(it.get("ticker") or "").strip().upper()
            if tkr:
                by_ticker[tkr] = it

    enriched: List[Dict[str, Any]] = []
    for c in results[:50]:
        if not isinstance(c, dict):
            continue
        ct = str(c.get("ticker") or "").strip().upper()
        if not ct:
            continue

        snap = by_ticker.get(ct, {}) if isinstance(by_ticker.get(ct), dict) else {}
        details = snap.get("details") if isinstance(snap.get("details"), dict) else (c.get("details") if isinstance(c.get("details"), dict) else {})

        contract_type = str(details.get("contract_type") or c.get("contract_type") or "").strip().lower()
        strike = details.get("strike_price") or c.get("strike_price")
        exp = details.get("expiration_date") or c.get("expiration_date")

        oi = None
        vol = None
        try:
            greeks = snap.get("open_interest")
            if greeks is not None:
                oi = float(greeks)
        except Exception:
            oi = None
        try:
            day = snap.get("day") if isinstance(snap.get("day"), dict) else {}
            if day.get("volume") is not None:
                vol = float(day.get("volume"))
        except Exception:
            vol = None

        enriched.append(
            {
                "contract": ct,
                "type": ("call" if contract_type == "call" else "put" if contract_type == "put" else ""),
                "expiration": exp,
                "strike": strike,
                "volume": vol,
                "open_interest": oi,
                "volume_oi_ratio": (float(vol) / float(oi) if (vol is not None and oi is not None and float(oi) > 0) else None),
            }
        )

    # Score unusualness
    call_vol = 0.0
    put_vol = 0.0
    unusual_hits = 0

    for e in enriched:
        v = e.get("volume")
        oi = e.get("open_interest")
        ratio = e.get("volume_oi_ratio")
        typ = str(e.get("type") or "")

        try:
            if typ == "call" and v is not None:
                call_vol += float(v)
            if typ == "put" and v is not None:
                put_vol += float(v)
        except Exception:
            pass

        is_unusual = False
        try:
            if v is not None and float(v) >= 500:
                is_unusual = True
        except Exception:
            pass
        try:
            if ratio is not None and float(ratio) >= 2.0:
                is_unusual = True
        except Exception:
            pass

        if is_unusual:
            unusual_hits += 1

    total_vol = max(0.0, call_vol + put_vol)
    call_put_ratio: Optional[float] = None
    try:
        if put_vol > 0:
            call_put_ratio = float(call_vol) / float(put_vol)
    except Exception:
        call_put_ratio = None

    score = 0.0
    try:
        # Base from count of unusual contracts
        score += min(60.0, float(unusual_hits) * 12.0)
        # Tape activity
        score += min(25.0, total_vol / 5000.0 * 25.0)
        # Imbalance (either direction)
        if call_put_ratio is not None:
            score += min(15.0, abs(call_put_ratio - 1.0) * 10.0)
    except Exception:
        score = 0.0

    score_i = int(max(0, min(100, round(score))))

    enriched.sort(key=lambda x: float(x.get("volume") or 0.0), reverse=True)
    top_contracts = []
    for e in enriched[:5]:
        top_contracts.append(
            {
                "contract": e.get("contract"),
                "type": e.get("type"),
                "expiration": e.get("expiration"),
                "strike": e.get("strike"),
                "volume": e.get("volume"),
                "open_interest": e.get("open_interest"),
                "volume_oi_ratio": e.get("volume_oi_ratio"),
            }
        )

    notes = "unavailable"
    try:
        if score_i <= 10:
            notes = "no_unusual_flow_detected"
        elif call_put_ratio is None:
            notes = f"unusual_contracts={unusual_hits}"
        else:
            tilt = "calls" if call_put_ratio >= 1.15 else "puts" if call_put_ratio <= 0.87 else "balanced"
            notes = f"unusual_contracts={unusual_hits}, flow_tilt={tilt}"
    except Exception:
        notes = "unavailable"

    out = {
        "unusual_options_score": int(score_i),
        "call_put_ratio": call_put_ratio,
        "top_contracts": top_contracts,
        "notes": str(notes)[:240],
    }

    _cache_set(_UNUSUAL_OPTS_CACHE, ck, out)
    return out
