import asyncio
import math
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Awaitable, Callable, Dict, List, Optional, Set, Tuple

import requests

from data_fetcher import get_bars_batch, get_snapshots_batch, get_snapshot_normalized

try:
    from backend.market_regime import detect_market_regime_full as _detect_regime_full
except Exception:
    def _detect_regime_full() -> Dict[str, Any]:  # type: ignore
        try:
            from data_fetcher import get_market_regime
            r = get_market_regime()
            regime_map = {"bull": "BULL", "bear": "BEAR", "neutral": "CHOPPY"}
            return {"regime": regime_map.get(r, "UNKNOWN"), "regime_legacy": r,
                    "regime_strength": "moderate", "vix_proxy": 0.0, "trend_slope_5d": 0.0, "confidence": 0.7}
        except Exception:
            return {"regime": "UNKNOWN", "regime_legacy": "unknown", "regime_strength": "unknown",
                    "vix_proxy": 0.0, "trend_slope_5d": 0.0, "confidence": 0.0}


log = logging.getLogger("stackiq")


def _conviction_label(score_0_10: float) -> str:
    """Map a 0-10 AI score to a conviction tier label."""
    s = float(score_0_10 or 0.0) * 10.0  # convert to 0-100 scale
    if s < 45:
        return "LOW"
    if s < 62:
        return "MODERATE"
    if s < 75:
        return "SOLID"
    if s < 85:
        return "HIGH"
    return "VERY HIGH"


def _clamp(v: Any, lo: float, hi: float) -> float:
    try:
        x = float(v)
    except Exception:
        x = lo
    if not math.isfinite(x):
        x = lo
    if x < lo:
        x = lo
    if x > hi:
        x = hi
    return float(x)


def _clamp01(v: Any) -> float:
    return _clamp(v, 0.0, 1.0)


def _score_1_10_from_01(v01: Any) -> float:
    v = _clamp01(v01)
    return float(round(1.0 + 9.0 * v, 1))


def _score_0_10_from_01(v01: Any) -> float:
    v = _clamp01(v01)
    return float(round(10.0 * v, 2))


def _safe_f(v: Any) -> Optional[float]:
    try:
        x = float(v)
    except Exception:
        return None
    if not math.isfinite(x):
        return None
    return float(x)


def _normalize_llm_reasoning_payload(payload: Any) -> Dict[str, Any]:
    data = payload if isinstance(payload, dict) else {}

    def _as_lines(v: Any) -> List[str]:
        if not isinstance(v, list):
            return []
        return [str(x).strip() for x in v if str(x).strip()]

    return {
        "bullish_factors": _as_lines(data.get("bullish_factors", [])),
        "bearish_factors": _as_lines(data.get("bearish_factors", [])),
        "catalysts": _as_lines(data.get("catalysts", [])),
        "risks": _as_lines(data.get("risks", [])),
        "summary": str(data.get("summary", "") or "").strip(),
    }


_EARNINGS_SAFE_CACHE: Dict[str, Any] = {}
_EARNINGS_SAFE_CACHE_TTL = 3600  # seconds

# ── Weekly pick quota by regime ────────────────────────────────────────────────

def _regime_weekly_limit(regime: str) -> int:
    """Max trade picks to output per calendar week based on market regime."""
    r = regime.upper()
    if "BULL" in r:
        return 3   # active market — fire Mon/Wed/Fri-ish
    elif "BEAR" in r:
        return 1   # defensive — only the clearest setup all week
    elif "CHOP" in r:
        return 1   # choppy — one best shot, then stay cash
    else:          # TRENDING, VOLATILE, SIDEWAYS, UNKNOWN
        return 2   # moderate — two good setups per week


def _weekly_picks_so_far() -> int:
    """Count confirmed trade picks (non-NO_TRADE) recorded this ISO calendar week."""
    import sqlite3 as _sql
    from datetime import date, timedelta
    today = date.today()
    week_start = (today - timedelta(days=today.weekday())).isoformat()  # Monday
    week_end   = (today + timedelta(days=6 - today.weekday())).isoformat()  # Sunday
    _perf_db = os.getenv("PERF_TRACKER_DB", os.path.join(os.path.dirname(os.path.abspath(__file__)), "perf_tracker.db"))
    try:
        con = _sql.connect(_perf_db, timeout=5)
        row = con.execute(
            "SELECT COUNT(*) FROM picks WHERE date >= ? AND date <= ? AND trade_decision IN ('HIGH_CONVICTION','LOW_CONVICTION')",
            (week_start, week_end),
        ).fetchone()
        con.close()
        return int(row[0]) if row else 0
    except Exception:
        return 0


# ── Historical win-pattern boost ──────────────────────────────────────────────

_WIN_PATTERN_CACHE: Dict[str, Any] = {}

def _winning_pattern_boost(candidate_signals: Set[str]) -> float:
    """
    Score boost (0–1.5) when today's candidate matches signal combos from recent big wins.
    Caches for 6 hours so we don't hammer the DB on every candidate.
    """
    import sqlite3 as _sql, json as _json
    now = time.time()
    if _WIN_PATTERN_CACHE.get("ts", 0) + 21600 > now:
        winning_combos = _WIN_PATTERN_CACHE.get("combos", [])
    else:
        _perf_db = os.getenv("PERF_TRACKER_DB", os.path.join(os.path.dirname(os.path.abspath(__file__)), "perf_tracker.db"))
        winning_combos: List[Set[str]] = []
        try:
            con = _sql.connect(_perf_db, timeout=5)
            rows = con.execute(
                """SELECT edge_signals, max_return_pct FROM picks
                   WHERE status IN ('won','won_drift')
                     AND max_return_pct >= 4.0
                   ORDER BY max_return_pct DESC
                   LIMIT 20"""
            ).fetchall()
            con.close()
            for raw_sigs, ret_pct in rows:
                try:
                    sigs = set(_json.loads(raw_sigs or "[]"))
                    if sigs:
                        winning_combos.append((sigs, float(ret_pct or 0)))
                except Exception:
                    pass
        except Exception:
            pass
        _WIN_PATTERN_CACHE["combos"] = winning_combos
        _WIN_PATTERN_CACHE["ts"] = now

    if not candidate_signals or not winning_combos:
        return 0.0

    best_overlap = 0.0
    for win_sigs, win_ret in winning_combos:
        if not win_sigs:
            continue
        overlap = len(candidate_signals & win_sigs) / max(len(win_sigs), 1)
        # Weight by return magnitude — bigger wins count more
        weight = min(win_ret / 10.0, 1.5)
        best_overlap = max(best_overlap, overlap * weight)

    return round(min(best_overlap, 1.5), 3)

# Hardcoded earnings calendar — safety net when Yahoo Finance / news classifier misses
# upcoming reports. Update weekly. Format: "TICKER": "YYYY-MM-DD".
KNOWN_EARNINGS_DATES: Dict[str, str] = {
    # April / May 2026
    "MSFT":  "2026-04-30",
    "META":  "2026-04-30",
    "QCOM":  "2026-04-30",
    "PYPL":  "2026-04-29",
    "F":     "2026-04-29",
    "GM":    "2026-04-29",
    "SNAP":  "2026-04-29",
    "FSLR":  "2026-04-29",
    "AAPL":  "2026-05-01",
    "AMZN":  "2026-05-01",
    "GOOGL": "2026-05-01",
    "ABNB":  "2026-05-01",
    "ROKU":  "2026-05-01",
    "SQ":    "2026-05-02",
    "PLTR":  "2026-05-05",
    "AMD":   "2026-05-06",
    "RIVN":  "2026-05-06",
    "CVS":   "2026-05-06",
    "CLSK":  "2026-05-06",
    "SHOP":  "2026-05-07",
    "UBER":  "2026-05-07",
    "ARM":   "2026-05-07",
    "DIS":   "2026-05-07",
    "DASH":  "2026-05-07",
    "LYFT":  "2026-05-07",
    "RBLX":  "2026-05-07",
    "MARA":  "2026-05-07",
    "COIN":  "2026-05-08",
    "AFRM":  "2026-05-08",
    "NET":   "2026-05-08",
    "PINS":  "2026-05-08",
    "U":     "2026-05-08",
    "DKNG":  "2026-05-08",
    "WULF":  "2026-05-08",
    "CORZ":  "2026-05-08",
    "HUT":   "2026-05-09",
    "RIOT":  "2026-05-12",
    "BMNR":  "2026-05-12",
    "BTBT":  "2026-05-12",
    "CIFR":  "2026-05-12",
    "SLNH":  "2026-05-15",
    "NVDA":  "2026-05-21",
    "TSLA":  "2026-04-23",
}


def _fetch_earnings_date(symbol: str) -> Optional[datetime]:
    """Return the next earnings date for symbol from Yahoo Finance, or None if unavailable."""
    sym = str(symbol or "").strip().upper()
    if not sym:
        return None
    try:
        url = f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{sym}"
        resp = requests.get(
            url,
            params={"modules": "calendarEvents"},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=4,
        )
        if resp.status_code != 200:
            return None
        data = resp.json()
        result = (data.get("quoteSummary") or {}).get("result") or []
        if not result:
            return None
        cal = (result[0].get("calendarEvents") or {}).get("earnings") or {}
        dates = cal.get("earningsDate") or []
        if not isinstance(dates, list) or not dates:
            return None
        raw = dates[0]
        if isinstance(raw, dict):
            ts = raw.get("raw")
        else:
            ts = raw
        if ts is None:
            return None
        return datetime.fromtimestamp(float(ts), tz=timezone.utc)
    except Exception:
        return None


def _is_earnings_season() -> bool:
    """January, April, July, October are peak earnings season months."""
    return datetime.now(tz=timezone.utc).month in (1, 4, 7, 10)


def _classify_earnings_news(symbol: str) -> Dict[str, Any]:
    """
    Classify earnings news as 'upcoming', 'recent', or 'none'.
    Returns {"type": str, "pub_age_days": float|None}.
    Only called when the Yahoo Finance calendar returns no date.
    """
    try:
        from data_fetcher import get_news as _get_news
        items = _get_news(15, symbols=[symbol]) or []
        now_utc = datetime.now(tz=timezone.utc)

        _upcoming_kw = (
            "will report", "scheduled to report", "upcoming earnings",
            "expected to report", "set to report", "ahead of earnings",
            "before earnings", "earnings preview", "earnings call scheduled",
        )
        _recent_kw = (
            "reported earnings", "posted earnings", "announced earnings",
            "q1 results", "q2 results", "q3 results", "q4 results",
            "quarterly results", "fiscal quarter results",
            "beat estimate", "beat expectations", "beats estimates",
            "missed estimate", "miss expectations", "misses estimates",
            "earnings beat", "earnings miss",
        )

        best_type = "none"
        best_age: Optional[float] = None

        for item in items:
            title   = str(item.get("title")   or "").lower()
            summary = str(item.get("summary") or "").lower()
            pub_str = str(item.get("publishedAt") or "")
            text    = title + " " + summary

            pub_age: Optional[float] = None
            try:
                pub_dt  = datetime.fromisoformat(pub_str.replace("Z", "+00:00"))
                pub_age = (now_utc - pub_dt).total_seconds() / 86400.0
            except Exception:
                pass

            # Only consider news within the last 30 days for upcoming, 7 days for recent
            if pub_age is not None and pub_age > 30.0:
                continue

            if any(kw in text for kw in _upcoming_kw):
                # Upcoming always wins; no need to look further
                return {"type": "upcoming", "pub_age_days": pub_age}

            # Recent earnings: only count articles from the last 7 days
            if pub_age is not None and pub_age <= 7.0 and any(kw in text for kw in _recent_kw):
                if best_type != "upcoming":
                    best_type = "recent"
                    best_age  = pub_age

        return {"type": best_type, "pub_age_days": best_age}
    except Exception:
        return {"type": "none", "pub_age_days": None}


async def _check_earnings_safe(symbol: str, window_days: int = 14) -> Dict[str, Any]:
    """
    Returns {"safe": bool, "days_to_earnings": int|None, "_source": str, "_note": str, "_ts": float}.

    Decision hierarchy:
      1. Yahoo Finance calendar → upcoming within window → unsafe; past → safe.
      2. Alpaca news classification:
         - "upcoming" keywords  → unsafe (earnings_upcoming)
         - "recent"   keywords  → fetch price change; >+2% → safe (post_earnings_continuation)
                                                         else → unsafe (post_earnings_drift)
         - "none"               → safe (no_earnings_window)
      3. News fetch failed entirely + earnings season → unsafe (earnings_season_default).
    """
    sym    = str(symbol or "").strip().upper()
    now_ts = time.time()

    cached = _EARNINGS_SAFE_CACHE.get(sym)
    if isinstance(cached, dict) and now_ts - float(cached.get("_ts", 0)) < _EARNINGS_SAFE_CACHE_TTL:
        return cached

    def _save(safe: bool, *, days: Optional[int] = None, source: str = "", note: str = "") -> Dict[str, Any]:
        r: Dict[str, Any] = {
            "safe": bool(safe), "days_to_earnings": days,
            "_source": source, "_note": note, "_ts": now_ts,
        }
        _EARNINGS_SAFE_CACHE[sym] = r
        return r

    # ── 0. Layer 1 safety net: hardcoded earnings calendar ───────────────────
    if sym in KNOWN_EARNINGS_DATES:
        try:
            import datetime as _dt
            _ed = _dt.datetime.strptime(KNOWN_EARNINGS_DATES[sym], "%Y-%m-%d").date()
            _today = _dt.datetime.now().date()
            _days_to = (_ed - _today).days
            if 0 <= _days_to <= window_days:
                log.info(
                    "earnings_filter: %s hardcoded match — %dd to earnings (%s)",
                    sym, _days_to, KNOWN_EARNINGS_DATES[sym],
                )
                return _save(False, days=_days_to, source="hardcoded_earnings_calendar",
                             note=f"Earnings scheduled in {_days_to} days ({KNOWN_EARNINGS_DATES[sym]}) — pre-earnings risk")
        except Exception as _e:
            log.warning("earnings_filter: hardcoded lookup failed for %s: %s", sym, _e)

    # ── 1. Primary: Yahoo Finance calendar ───────────────────────────────────
    try:
        earnings_dt = await asyncio.wait_for(
            asyncio.to_thread(_fetch_earnings_date, sym),
            timeout=5.0,
        )
    except Exception:
        earnings_dt = None

    if earnings_dt is not None:
        now_utc    = datetime.now(tz=timezone.utc)
        days_delta = (earnings_dt - now_utc).days
        if 0 <= days_delta <= window_days:
            return _save(False, days=int(days_delta), source="earnings_upcoming",
                         note=f"Earnings in {days_delta} days — avoiding pre-earnings risk")
        return _save(True,
                     days=int(days_delta) if days_delta >= 0 else None,
                     source="yahoo_calendar", note="")

    # ── 2. Fallback: Alpaca news classification ───────────────────────────────
    news_fetch_ok = False
    news_class: Dict[str, Any] = {"type": "none", "pub_age_days": None}
    try:
        news_class    = await asyncio.wait_for(
            asyncio.to_thread(_classify_earnings_news, sym),
            timeout=3.0,
        )
        news_fetch_ok = True
    except Exception:
        pass

    earnings_type = str(news_class.get("type") or "none")

    if earnings_type == "upcoming":
        return _save(False, source="earnings_upcoming",
                     note="News signals upcoming earnings — avoiding pre-earnings risk")

    if earnings_type == "recent":
        # Determine post-earnings price action over the last 7 trading days
        price_change_pct: Optional[float] = None
        try:
            bars_map = await asyncio.wait_for(
                asyncio.to_thread(get_bars_batch, [sym], "1Day", 10),
                timeout=3.0,
            )
            bars = list((bars_map or {}).get(sym) or [])
            if len(bars) >= 2:
                baseline_idx = max(0, len(bars) - 8)
                baseline_c   = _safe_f(bars[baseline_idx].get("c"))
                latest_c     = _safe_f(bars[-1].get("c"))
                if baseline_c and latest_c and float(baseline_c) > 0:
                    price_change_pct = (float(latest_c) - float(baseline_c)) / float(baseline_c) * 100.0
        except Exception:
            price_change_pct = None

        if price_change_pct is not None and price_change_pct > 2.0:
            return _save(True, source="post_earnings_continuation",
                         note=f"Stock running post-beat ({price_change_pct:+.1f}% since earnings), momentum favorable")
        chg_str = f"{price_change_pct:+.1f}%" if price_change_pct is not None else "unknown"
        return _save(False, source="post_earnings_drift",
                     note=f"Avoiding post-earnings drift ({chg_str} since report)")

    # earnings_type == "none": news fetched successfully, no earnings activity found
    if news_fetch_ok:
        return _save(True, source="no_earnings_window", note="")

    # ── 3. News fetch failed entirely — earnings season conservative default ──
    if _is_earnings_season():
        return _save(False, source="earnings_season_default",
                     note="No earnings data available during peak earnings season")
    return _save(True, source="no_data", note="")


def _sma(values: List[float], period: int) -> Optional[float]:
    if not values or period <= 0 or len(values) < period:
        return None
    tail = values[-period:]
    return float(sum(tail) / float(period))


def _rsi(values: List[float], period: int = 14) -> float:
    if not values or len(values) < (period + 2):
        return 50.0
    gains = 0.0
    losses = 0.0
    start = len(values) - period
    for i in range(start, len(values)):
        if i <= 0:
            continue
        d = float(values[i]) - float(values[i - 1])
        if d >= 0:
            gains += d
        else:
            losses += abs(d)
    if gains <= 0 and losses <= 0:
        return 50.0
    if losses <= 0:
        return 100.0
    rs = (gains / float(period)) / (losses / float(period))
    r = 100.0 - (100.0 / (1.0 + rs))
    return float(_clamp(r, 0.0, 100.0))


def _atr(highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> Optional[float]:
    if not closes or len(closes) < (period + 2) or len(highs) != len(closes) or len(lows) != len(closes):
        return None
    trs: List[float] = []
    for i in range(1, len(closes)):
        h = float(highs[i])
        l = float(lows[i])
        pc = float(closes[i - 1])
        tr = max(h - l, abs(h - pc), abs(l - pc))
        trs.append(tr)
    if not trs:
        return None
    tail = trs[-period:] if len(trs) >= period else trs
    return float(sum(tail) / float(len(tail) or 1))


def _percentile_ranks(values: List[Optional[float]]) -> List[float]:
    """Returns percentile ranks in [0,1]. None -> 0."""
    idx_vals: List[Tuple[int, float]] = []
    for i, v in enumerate(values):
        if v is None:
            continue
        try:
            fv = float(v)
        except Exception:
            continue
        if not math.isfinite(fv):
            continue
        idx_vals.append((i, fv))

    if not idx_vals:
        return [0.0 for _ in values]

    idx_vals.sort(key=lambda x: x[1])
    n = len(idx_vals)
    ranks = [0.0 for _ in values]
    if n == 1:
        ranks[idx_vals[0][0]] = 1.0
        return ranks

    for r, (i, _) in enumerate(idx_vals):
        ranks[i] = float(r) / float(n - 1)
    return ranks


def _quote_from_snapshot(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(snapshot, dict):
        return {}
    q = snapshot.get("latestQuote")
    return q if isinstance(q, dict) else {}


def _last_price_from_snapshot(snapshot: Dict[str, Any]) -> Optional[float]:
    if not isinstance(snapshot, dict):
        return None
    lt = snapshot.get("latestTrade") if isinstance(snapshot.get("latestTrade"), dict) else {}
    if lt.get("p") is not None:
        try:
            return float(lt.get("p"))
        except Exception:
            pass
    bar = snapshot.get("dailyBar") if isinstance(snapshot.get("dailyBar"), dict) else {}
    if bar.get("c") is not None:
        try:
            return float(bar.get("c"))
        except Exception:
            pass
    return None


def _spread_pct(snapshot: Dict[str, Any]) -> Optional[float]:
    q = _quote_from_snapshot(snapshot)
    ap = _safe_f(q.get("ap"))
    bp = _safe_f(q.get("bp"))
    if ap is None or bp is None:
        return None
    mid = (float(ap) + float(bp)) / 2.0
    if mid <= 0:
        return None
    sp = float(ap) - float(bp)
    if sp < 0:
        sp = abs(sp)
    return float(sp / mid * 100.0)


def _dollar_volume_30d(bars: List[Dict[str, Any]]) -> Tuple[Optional[float], Optional[float]]:
    """Returns (avg_share_vol_30d, avg_dollar_vol_30d)."""
    if not isinstance(bars, list) or len(bars) < 25:
        return None, None
    tail = bars[-20:]
    vols: List[float] = []
    dollars: List[float] = []
    for b in tail:
        c = _safe_f(b.get("c"))
        v = _safe_f(b.get("v"))
        if c is None or v is None or c <= 0 or v <= 0:
            continue
        vols.append(float(v))
        dollars.append(float(c) * float(v))
    if len(vols) < 20:
        return None, None
    return float(sum(vols) / float(len(vols) or 1)), float(sum(dollars) / float(len(dollars) or 1))


def _roc(values: List[float], n: int) -> Optional[float]:
    if not values or n <= 0 or len(values) <= n:
        return None
    a = float(values[-n - 1])
    b = float(values[-1])
    if a == 0:
        return None
    return float((b - a) / a * 100.0)


def _slope(values: List[float], n: int) -> Optional[float]:
    if not values or n <= 2 or len(values) < n:
        return None
    window = values[-n:]
    xs = list(range(n))
    x_mean = (n - 1) / 2.0
    y_mean = sum(window) / float(n)
    num = 0.0
    den = 0.0
    for i in range(n):
        dx = float(xs[i]) - x_mean
        num += dx * (float(window[i]) - y_mean)
        den += dx * dx
    if den <= 0:
        return 0.0
    return float(num / den)


def _swing_low(bars: List[Dict[str, Any]], lookback: int = 10) -> Optional[float]:
    if not isinstance(bars, list) or len(bars) < lookback:
        return None
    lows: List[float] = []
    for b in bars[-lookback:]:
        if not isinstance(b, dict):
            continue
        l = _safe_f(b.get("l"))
        if l is None:
            continue
        lows.append(float(l))
    if not lows:
        return None
    return float(min(lows))


@dataclass
class _Candidate:
    symbol: str
    type: str  # "Stock"|"ETF" (best effort)
    snapshot: Dict[str, Any]
    daily_bars: List[Dict[str, Any]]

    last_price: Optional[float] = None
    spread_pct_now: Optional[float] = None
    avg_vol_30d: Optional[float] = None
    avg_dollar_vol_30d: Optional[float] = None

    closes: Optional[List[float]] = None
    highs: Optional[List[float]] = None
    lows: Optional[List[float]] = None

    sma20: Optional[float] = None
    sma50: Optional[float] = None
    rsi14: Optional[float] = None
    roc5: Optional[float] = None
    roc20: Optional[float] = None
    slope20: Optional[float] = None
    atr14: Optional[float] = None
    atr_pct: Optional[float] = None

    stop: Optional[float] = None
    stop_distance_pct: Optional[float] = None
    expected_move_5d: Optional[float] = None
    upside_ratio: Optional[float] = None

    # subscores 1-10
    technical_score: float = 1.0
    catalyst_score: float = 1.0
    sentiment_score: float = 1.0
    risk_structure_score: float = 1.0
    upside_score: float = 1.0

    execution_score: float = 1.0
    ai_score: float = 1.0
    pre_mover_score: Optional[float] = None
    final_rank_score: Optional[float] = None

    news_obj: Optional[Dict[str, Any]] = None
    catalysts: List[str] = None
    risk_flags: List[str] = None
    llm_reasoning: Optional[Dict[str, Any]] = None
    llm_active: bool = False

    # Enhanced component scores (0-10)
    momentum_score: float = 5.0
    volatility_score_0_10: float = 5.0   # tradability: 10=ideal ATR range, not too dead or chaotic
    risk_reward_score: float = 5.0        # quality of R:R setup
    liquidity_score: float = 5.0          # dollar volume / execution quality
    news_score: float = 5.0               # 5=neutral, >5=bullish, <5=bearish
    final_score_0_10: Optional[float] = None
    pick_rationale: Optional[List[str]] = None

    # Elite trading intelligence fields (v3 upgrade)
    market_regime: str = "UNKNOWN"        # BULL | BEAR | CHOPPY | UNKNOWN
    trade_quality: str = "B"              # A+ | A | B | C | AVOID
    position_size_pct: float = 3.0        # recommended position size 1-10%
    risk_level: str = "medium"            # low | medium | high
    news_summary: str = ""                # 1-sentence LLM summary
    key_drivers: Optional[List[str]] = None  # top 3 reasons from LLM
    event_type: str = "unknown"           # earnings | analyst | guidance | macro | product | insider | unknown
    premover_score_0_10: float = 5.0      # early-stage expansion signal 0-10
    overextended_penalty: float = 0.0     # subtracted from final_score when stock already moved
    trade_decision: str = "LOW_CONVICTION"  # HIGH_CONVICTION | LOW_CONVICTION | NO_TRADE
    is_trade: bool = False                  # True only for HIGH_CONVICTION
    edge_signals: List[str] = None         # MOMENTUM_EXPANSION | BREAKOUT_STRUCTURE | RS_LEADER | VOLATILITY_EXPANSION
    edge_score_0_10: float = 0.0           # 0–10 from signal weights
    is_momentum_bypass: bool = False       # True when changePercent > 15% — gets 1.3x final_score boost in ranking
    nn_win_prob: Optional[float] = None    # P(win) from the trained neural network [0, 1]; None = model not ready


def _detect_edge_signals(c: "_Candidate", spy_roc5: Optional[float] = None) -> List[str]:
    """Detect real breakout / momentum edge signals.  Returns list of triggered signal names.
    If the list is empty the stock has NO edge and should be skipped."""
    signals: List[str] = []
    try:
        closes = list(c.closes or [])
        highs  = list(c.highs  or [])
        lows   = list(c.lows   or [])
        bars   = list(c.daily_bars or [])
        price  = _safe_f(c.last_price)
        if price is None and closes:
            price = _safe_f(closes[-1])
        if not price or float(price) <= 0:
            return signals

        # ── 1. MOMENTUM EXPANSION: 5d ROC > 3% AND today's vol > 1.5x avg ──
        try:
            roc5 = _safe_f(c.roc5)
            if roc5 is not None and float(roc5) > 3.0:
                # Current volume
                cur_vol: Optional[float] = None
                try:
                    db = c.snapshot.get("dailyBar") if isinstance(c.snapshot, dict) else {}
                    cur_vol = _safe_f((db or {}).get("v")) if isinstance(db, dict) else None
                except Exception:
                    cur_vol = None
                vols = [_safe_f(b.get("v")) for b in bars[-25:] if isinstance(b, dict)]
                vols = [v for v in vols if v is not None and float(v) > 0]
                if cur_vol is None and vols:
                    cur_vol = float(vols[-1])
                avg_vol = float(sum(vols[-20:]) / len(vols[-20:])) if len(vols) >= 5 else None
                if avg_vol and avg_vol > 0 and cur_vol is not None:
                    if float(cur_vol) >= 1.5 * float(avg_vol):
                        signals.append("MOMENTUM_EXPANSION")
        except Exception:
            pass

        # ── 2. BREAKOUT STRUCTURE: price within 2% of 20-day high AND prior range contraction ──
        try:
            if len(highs) >= 20:
                high20 = max(float(h) for h in highs[-20:])
                dist_pct = (float(high20) - float(price)) / float(price)
                if 0.0 <= dist_pct <= 0.02:
                    # Confirm range contraction: ATR last 5 bars < ATR bars 6-20
                    atr_recent = _atr(highs, lows, closes, 5)
                    atr_prior  = _atr(highs[:-5], lows[:-5], closes[:-5], 14) if len(closes) > 20 else None
                    contraction = (
                        atr_recent is not None
                        and atr_prior is not None
                        and float(atr_recent) < float(atr_prior)
                    )
                    if contraction:
                        signals.append("BREAKOUT_STRUCTURE")
        except Exception:
            pass

        # ── 3. RELATIVE STRENGTH LEADER: outperforms SPY by > 2% over 5 days ──
        try:
            roc5 = _safe_f(c.roc5)
            if roc5 is not None and spy_roc5 is not None:
                outperformance = float(roc5) - float(spy_roc5)
                if outperformance > 2.0:
                    signals.append("RS_LEADER")
        except Exception:
            pass

        # ── 4. VOLATILITY EXPANSION: ATR expanding AND current bar range > 1.2x avg ──
        try:
            if len(highs) >= 15 and len(lows) >= 15 and len(closes) >= 15:
                atr5  = _atr(highs, lows, closes, 5)
                atr14 = _atr(highs, lows, closes, 14)
                # ATR expanding: recent > prior
                atr_expanding = (
                    atr5 is not None and atr14 is not None
                    and float(atr5) > float(atr14)
                )
                # Current bar range vs avg of prior 10 bars
                cur_range = float(highs[-1]) - float(lows[-1]) if highs and lows else None
                prior_ranges = [
                    float(highs[j]) - float(lows[j])
                    for j in range(max(0, len(highs) - 11), len(highs) - 1)
                    if j < len(lows)
                ]
                avg_range = float(sum(prior_ranges) / len(prior_ranges)) if prior_ranges else None
                range_expanding = (
                    cur_range is not None and avg_range is not None and avg_range > 0
                    and float(cur_range) >= 1.2 * float(avg_range)
                )
                if atr_expanding and range_expanding:
                    signals.append("VOLATILITY_EXPANSION")
        except Exception:
            pass

    except Exception:
        pass

    return signals


def _detect_choppy_signals(c: "_Candidate", spy_roc3: Optional[float] = None) -> List[str]:
    """CHOPPY-regime signal detection. Returns list of triggered signal names."""
    signals: List[str] = []
    try:
        closes = list(c.closes or [])
        highs  = list(c.highs  or [])
        lows   = list(c.lows   or [])
        price  = _safe_f(c.last_price)
        if price is None and closes:
            price = _safe_f(closes[-1])
        if not price or float(price) <= 0:
            return signals

        # ── 1. RSI_OVERSOLD_BOUNCE: RSI < 35 and rising over the last 3 bars ──
        try:
            if len(closes) >= 16:
                rsi_now   = _rsi(closes,       period=14)
                rsi_prev  = _rsi(closes[:-1],  period=14)
                rsi_prev2 = _rsi(closes[:-2],  period=14)
                if rsi_now < 35.0 and rsi_now > rsi_prev > rsi_prev2:
                    signals.append("RSI_OVERSOLD_BOUNCE")
        except Exception:
            pass

        # ── 2. SUPPORT_RECLAIM: prior bar touched 20-day low, current close above it ──
        try:
            if len(lows) >= 20 and len(closes) >= 2:
                low20     = min(float(l) for l in lows[-20:])
                prev_low  = float(lows[-2])
                cur_close = float(closes[-1])
                # "touched" = prior low within 0.5% of the 20-day low
                touched   = prev_low <= low20 * 1.005
                recovered = cur_close > low20
                if touched and recovered:
                    signals.append("SUPPORT_RECLAIM")
        except Exception:
            pass

        # ── 3. SECTOR_ROTATION: stock positive over 3 days while SPY is negative ──
        try:
            roc3 = _roc(closes, 3)
            if roc3 is not None and spy_roc3 is not None:
                if float(roc3) > 0.0 and float(spy_roc3) < 0.0:
                    signals.append("SECTOR_ROTATION")
        except Exception:
            pass

    except Exception:
        pass
    return signals


def _vwap_from_bars(bars: List[Dict[str, Any]], period: int = 20) -> Optional[float]:
    if not isinstance(bars, list) or len(bars) < max(1, int(period)):
        return None
    tail = bars[-int(period):]
    numer = 0.0
    denom = 0.0
    for b in tail:
        if not isinstance(b, dict):
            continue
        h = _safe_f(b.get("h"))
        l = _safe_f(b.get("l"))
        c = _safe_f(b.get("c"))
        v = _safe_f(b.get("v"))
        if h is None or l is None or c is None or v is None or float(v) <= 0:
            continue
        typical = (float(h) + float(l) + float(c)) / 3.0
        numer += typical * float(v)
        denom += float(v)
    if denom <= 0:
        return None
    return float(numer / denom)


def _compute_pre_mover_score(c: _Candidate) -> Optional[float]:
    """Additive early-expansion probability score in [0,10]."""
    try:
        closes = list(c.closes or [])
        highs = list(c.highs or [])
        lows = list(c.lows or [])
        bars = list(c.daily_bars or [])
        price = _safe_f(c.last_price)

        if price is None and closes:
            price = _safe_f(closes[-1])
        if price is None or float(price) <= 0:
            return None

        components: List[Tuple[float, float]] = []

        # 1) Range compression: lower ATR_5 / ATR_20 is better.
        try:
            atr5 = _atr(highs, lows, closes, 5)
            atr20 = _atr(highs, lows, closes, 20)
            if atr5 is not None and atr20 is not None and float(atr20) > 0:
                compression = float(atr5) / float(atr20)
                compression01 = _clamp01((1.25 - float(compression)) / 0.75)
                components.append((0.25, _score_0_10_from_01(compression01)))
        except Exception:
            pass

        # 2) Breakout proximity: distance to recent resistance.
        try:
            if len(highs) >= 20:
                resistance = max(float(x) for x in highs[-20:])
                distance = abs(float(resistance) - float(price)) / float(price)
                breakout01 = _clamp01((0.03 - float(distance)) / 0.03)
                components.append((0.25, _score_0_10_from_01(breakout01)))
        except Exception:
            pass

        # 3) Early volume expansion: score ramps from 1.2x -> 2.5x.
        try:
            vols: List[float] = []
            for b in bars[-40:]:
                if not isinstance(b, dict):
                    continue
                v0 = _safe_f(b.get("v"))
                if v0 is None or float(v0) < 0:
                    continue
                vols.append(float(v0))
            avg_vol_20 = None
            if len(vols) >= 20:
                avg_vol_20 = float(sum(vols[-20:]) / 20.0)

            cur_vol = None
            try:
                db = c.snapshot.get("dailyBar") if isinstance(c.snapshot, dict) else {}
                if isinstance(db, dict):
                    cur_vol = _safe_f(db.get("v"))
            except Exception:
                cur_vol = None
            if cur_vol is None and vols:
                cur_vol = float(vols[-1])

            if avg_vol_20 is not None and avg_vol_20 > 0 and cur_vol is not None and float(cur_vol) >= 0:
                volume_ratio = float(cur_vol) / float(avg_vol_20)
                volume01 = _clamp01((float(volume_ratio) - 1.2) / (2.5 - 1.2))
                components.append((0.20, _score_0_10_from_01(volume01)))
        except Exception:
            pass

        # 4) VWAP positioning: slight premium above VWAP favors accumulation.
        try:
            vwap20 = _vwap_from_bars(bars, 20)
            if vwap20 is not None and float(vwap20) > 0:
                delta = (float(price) - float(vwap20)) / float(vwap20)
                vwap01 = 1.0 - _clamp01(abs(float(delta) - 0.008) / 0.03)
                components.append((0.15, _score_0_10_from_01(vwap01)))
        except Exception:
            pass

        # 5) Liquidity acceleration: current dollar-vol vs prior 5-day average.
        try:
            dollar_vols: List[float] = []
            for b in bars[-40:]:
                if not isinstance(b, dict):
                    continue
                c0 = _safe_f(b.get("c"))
                v0 = _safe_f(b.get("v"))
                if c0 is None or v0 is None or float(c0) <= 0 or float(v0) < 0:
                    continue
                dollar_vols.append(float(c0) * float(v0))

            cur_dollar = None
            try:
                db = c.snapshot.get("dailyBar") if isinstance(c.snapshot, dict) else {}
                if isinstance(db, dict):
                    c1 = _safe_f(db.get("c"))
                    v1 = _safe_f(db.get("v"))
                    if c1 is not None and v1 is not None and float(c1) > 0 and float(v1) >= 0:
                        cur_dollar = float(c1) * float(v1)
            except Exception:
                cur_dollar = None
            if cur_dollar is None and dollar_vols:
                cur_dollar = float(dollar_vols[-1])

            avg_dollar_5 = None
            if len(dollar_vols) >= 6:
                prev5 = dollar_vols[-6:-1]
                if prev5:
                    avg_dollar_5 = float(sum(prev5) / float(len(prev5) or 1))
            elif len(dollar_vols) >= 5:
                prev5 = dollar_vols[-5:]
                avg_dollar_5 = float(sum(prev5) / float(len(prev5) or 1))

            if cur_dollar is not None and avg_dollar_5 is not None and float(avg_dollar_5) > 0:
                liq_ratio = float(cur_dollar) / float(avg_dollar_5)
                liq01 = _clamp01((float(liq_ratio) - 1.05) / 0.95)
                components.append((0.15, _score_0_10_from_01(liq01)))
        except Exception:
            pass

        if not components:
            return None

        weight_sum = float(sum(w for w, _ in components) or 0.0)
        if weight_sum <= 0:
            return None
        weighted = float(sum(float(w) * float(s) for w, s in components) / weight_sum)
        return float(round(_clamp(weighted, 0.0, 10.0), 2))
    except Exception:
        return None


def _score_premover_v2(c: "_Candidate") -> float:
    """0–10: probability this stock is in PRE-MOVER phase (about to move, not already moved).

    Uses an additive raw-points system (target max ~100) normalized to 0–10.

    Points budget:
      Core signals   (max ~30):  vol_build(0-10) + price_breakout(0-10) + momentum(0-10)
      Primary signals(max ~90):  volume_coil_breakout(0-30) + flag_consolidation(0-20)
                                 + news_catalyst(0-25) + rsi_momentum(-10 to +15)
                                 + short_squeeze(0-20)

    Design target: tight consolidation + volume surge + news catalyst + RSI 55–75
                   → 85–95 raw pts → score 8.5–9.5 — picked the night before next day's top mover.
    """
    try:
        closes = list(c.closes or [])
        highs  = list(c.highs  or [])
        lows   = list(c.lows   or [])
        bars   = list(c.daily_bars or [])
        price  = _safe_f(c.last_price)
        if price is None and closes:
            price = _safe_f(closes[-1])
        if price is None or float(price) <= 0:
            return 5.0

        raw_pts = 0.0  # accumulates raw points (0–100+, clamped before normalizing)

        # ── Shared: current volume and ratio (re-used across multiple signals) ──
        cur_vol: Optional[float] = None
        avg_vol: Optional[float] = None
        try:
            db = c.snapshot.get("dailyBar") if isinstance(c.snapshot, dict) else {}
            cur_vol = _safe_f((db or {}).get("v")) if isinstance(db, dict) else None
            vols = [_safe_f(b.get("v")) for b in bars[-25:] if isinstance(b, dict)]
            vols = [v for v in vols if v is not None and float(v) > 0]
            if cur_vol is None and vols:
                cur_vol = float(vols[-1])
            avg_vol = float(sum(vols[-20:]) / len(vols[-20:])) if len(vols) >= 5 else None
        except Exception:
            pass
        vol_ratio = (float(cur_vol) / float(avg_vol)) if (cur_vol and avg_vol and float(avg_vol) > 0) else None

        # --- 1. EARLY VOLUME BUILD (0–10 pts): 1.3x–3x sweet spot; >5x = late-stage penalty ---
        try:
            if vol_ratio is not None:
                vr = float(vol_ratio)
                if vr > 5.0:
                    raw_pts += _clamp(10.0 - (vr - 5.0) * 1.5, 1.0, 4.0)
                elif vr >= 1.3:
                    raw_pts += _clamp(5.0 + (vr - 1.3) / (3.0 - 1.3) * 5.0, 5.0, 10.0)
                else:
                    raw_pts += _clamp(1.0 + (vr / 1.3) * 4.0, 1.0, 5.0)
        except Exception:
            pass

        # --- 2. VOLUME COIL BREAKOUT (0–30 pts): ATR contraction then expansion ----
        # Upgraded from weight 0.22 → 30 pts: single most predictive signal for next-day continuation.
        try:
            atr5  = _atr(highs, lows, closes, 5)  if len(closes) >= 7  else None
            atr10 = _atr(highs, lows, closes, 10) if len(closes) >= 12 else None
            atr20 = _atr(highs, lows, closes, 20) if len(closes) >= 22 else None

            if atr5 is not None and atr20 is not None and float(atr20) > 0:
                compression_ratio = float(atr5) / float(atr20)
                if compression_ratio < 0.5:
                    coil_01 = 0.40   # too flat / dead
                elif compression_ratio <= 0.85:
                    # Ideal compression zone: peaks at ratio 0.5
                    coil_01 = _clamp(0.60 + (0.85 - compression_ratio) / 0.35 * 0.40, 0.60, 1.00)
                else:
                    # Expanding already — good if moderate, diminishing above 1.0
                    coil_01 = _clamp(1.00 - (compression_ratio - 0.85) / 0.15 * 0.60, 0.40, 0.90)

                # Bonus: ATR5 just above ATR10 = expansion only beginning (highest conviction)
                if atr10 is not None and float(atr10) > 0:
                    early_expand = float(atr5) / float(atr10)
                    if 1.05 <= early_expand <= 1.40:
                        coil_01 = min(1.00, float(coil_01) + 0.10)

                raw_pts += _clamp(float(coil_01) * 30.0, 0.0, 30.0)
        except Exception:
            pass

        # --- 3. PRICE NEAR BREAKOUT (0–10 pts): within 3% of 20-day high, not already extended ---
        try:
            if len(highs) >= 15:
                resist = max(float(h) for h in highs[-20:] if h is not None)
                dist = (float(resist) - float(price)) / float(price) if float(price) > 0 else 0.1
                ext  = (float(price)  - float(resist)) / float(price) if float(price) > 0 else 0.0

                if ext > 0.05:
                    brk_01 = _clamp(0.40 - ext * 2.0, 0.10, 0.40)
                elif dist <= 0.0:
                    brk_01 = 0.80
                elif dist <= 0.03:
                    brk_01 = 0.80 + (1.0 - dist / 0.03) * 0.20
                elif dist <= 0.06:
                    brk_01 = _clamp(0.60 - (dist - 0.03) / 0.03 * 0.20, 0.40, 0.60)
                else:
                    brk_01 = _clamp(0.50 - dist * 3.0, 0.10, 0.50)

                raw_pts += _clamp(float(brk_01) * 10.0, 0.0, 10.0)
        except Exception:
            pass

        # --- 4. MOMENTUM ACCELERATION (0–10 pts): ROC delta shows acceleration, not just level ---
        try:
            if len(closes) >= 12:
                roc3_now  = (closes[-1] - closes[-4]) / closes[-4] * 100.0 if float(closes[-4]) > 0 else 0.0
                roc3_prev = (closes[-4] - closes[-7]) / closes[-7] * 100.0 if float(closes[-7]) > 0 else 0.0
                delta = float(roc3_now) - float(roc3_prev)

                roc5 = _safe_f(c.roc5)
                if roc5 is not None:
                    if float(roc5) > 5.0:
                        roc_01 = _clamp((7.0 - (float(roc5) - 5.0) * 0.5) / 10.0, 0.20, 0.70)
                    elif float(roc5) >= 0.5:
                        roc_01 = _clamp((5.0 + float(roc5)) / 10.0, 0.55, 1.00)
                    else:
                        roc_01 = _clamp((5.0 + float(roc5) * 2.0) / 10.0, 0.10, 0.55)
                else:
                    roc_01 = 0.50

                accel_bonus = _clamp(float(delta) * 0.15, -0.20, 0.20)
                mom_01 = _clamp(float(roc_01) + float(accel_bonus), 0.0, 1.0)
                raw_pts += _clamp(float(mom_01) * 10.0, 0.0, 10.0)
        except Exception:
            pass

        # --- 5. FLAG / CONSOLIDATION PATTERN (0–20 pts) --------------------------------
        # Tight daily range (H–L < 3% of close) for 3+ consecutive days, then today expands above it.
        # Classic "flag pole + flag" — stock resting before the next leg up.
        try:
            if len(highs) >= 5 and len(lows) >= 5 and len(closes) >= 5:
                tight_days = 0
                for i in range(max(0, len(highs) - 6), len(highs) - 1):
                    ref_c = float(closes[i]) if closes[i] and float(closes[i]) > 0 else None
                    if ref_c is None:
                        tight_days = 0
                        continue
                    day_range_pct = (float(highs[i]) - float(lows[i])) / ref_c
                    if day_range_pct < 0.03:
                        tight_days += 1
                    else:
                        tight_days = 0   # must be consecutive

                if tight_days >= 3:
                    ref_c_today = float(closes[-1]) if closes[-1] and float(closes[-1]) > 0 else float(price)
                    today_range_pct = (float(highs[-1]) - float(lows[-1])) / ref_c_today if ref_c_today > 0 else 0.0
                    if today_range_pct > 0.03:
                        raw_pts += 20.0
        except Exception:
            pass

        # --- 6. NEWS CATALYST CONFIRMED (0–25 pts) -------------------------------------
        # A real catalyst (earnings beat, FDA approval, analyst upgrade) with volume = highest-probability setup.
        # news_score > 6.5 triggers; scales linearly to 25 pts at score 10.
        try:
            ns = float(c.news_score or 5.0)
            if ns > 6.5:
                raw_pts += _clamp((ns - 6.5) / 3.5 * 25.0, 5.0, 25.0)
            elif 5.2 <= ns <= 6.5:
                # Subdued positive (early signal, not yet crowded): 0–5 pts
                raw_pts += _clamp((ns - 5.2) / 1.3 * 5.0, 0.0, 5.0)
            # Below 5.2: no news boost
        except Exception:
            pass

        # --- 7. RSI MOMENTUM (-10 to +15 pts) -----------------------------------------
        # RSI 55–75: strong uptrend momentum, not overbought → +15 pts.
        # RSI > 75: overbought, likely to mean-revert → -10 pts.
        # RSI < 55: no signal in either direction.
        try:
            rsi = _safe_f(c.rsi14)
            if rsi is not None:
                rsi_f = float(rsi)
                if 55.0 <= rsi_f <= 75.0:
                    raw_pts += 15.0
                elif rsi_f > 75.0:
                    raw_pts -= 10.0
        except Exception:
            pass

        # --- 8. SHORT SQUEEZE POTENTIAL (0–20 pts) -------------------------------------
        # Up 15%+ today AND relative volume >= 3x: forced short covering creates the biggest moves.
        try:
            _snap    = c.snapshot if isinstance(c.snapshot, dict) else {}
            _prev_db = _snap.get("prevDailyBar") if isinstance(_snap.get("prevDailyBar"), dict) else {}
            _prev_c_px = _safe_f((_prev_db or {}).get("c"))
            _cur_px    = _safe_f(c.last_price)
            if _prev_c_px and _cur_px and float(_prev_c_px) > 0 and float(_cur_px) > 0:
                today_chg_pct = (float(_cur_px) - float(_prev_c_px)) / float(_prev_c_px) * 100.0
                if today_chg_pct >= 15.0 and vol_ratio is not None and float(vol_ratio) >= 3.0:
                    raw_pts += 20.0
        except Exception:
            pass

        if raw_pts <= 0.0:
            return 5.0

        # Normalize: 100 raw pts → 10.0 final score
        final = _clamp(raw_pts / 10.0, 1.0, 10.0)
        return float(round(final, 1))
    except Exception:
        return 5.0


def _compute_overextension_penalty(c: "_Candidate") -> float:
    """Returns 0.0–2.5 penalty to subtract from final_score when stock is already extended.
    Fires on: >5x vol, price far above 20-day range, extreme ATR%."""
    try:
        penalty = 0.0
        closes = list(c.closes or [])
        highs = list(c.highs or [])
        bars = list(c.daily_bars or [])
        price = _safe_f(c.last_price)
        if price is None and closes:
            price = _safe_f(closes[-1])
        if price is None or float(price) <= 0:
            return 0.0

        # Penalty 1: Volume >5x average (late-stage move)
        try:
            cur_vol: Optional[float] = None
            try:
                db = c.snapshot.get("dailyBar") if isinstance(c.snapshot, dict) else {}
                cur_vol = _safe_f((db or {}).get("v")) if isinstance(db, dict) else None
            except Exception:
                cur_vol = None
            vols = [_safe_f(b.get("v")) for b in bars[-25:] if isinstance(b, dict)]
            vols = [v for v in vols if v is not None and float(v) > 0]
            if cur_vol is None and vols:
                cur_vol = float(vols[-1])
            avg_vol = float(sum(vols[-20:]) / len(vols[-20:])) if len(vols) >= 5 else None
            if avg_vol and avg_vol > 0 and cur_vol is not None:
                vr = float(cur_vol) / float(avg_vol)
                if vr > 5.0:
                    penalty += _clamp((vr - 5.0) * 0.25, 0.0, 1.0)
        except Exception:
            pass

        # Penalty 2: Price >7% above 20-day high (already exploded)
        try:
            if len(highs) >= 15:
                resist = max(float(h) for h in highs[-20:] if h is not None)
                ext = (float(price) - float(resist)) / float(price)
                if ext > 0.07:
                    penalty += _clamp((ext - 0.07) * 10.0, 0.0, 1.0)
        except Exception:
            pass

        # Penalty 3: ATR% >5% (chaotic, late-stage volatility spike)
        try:
            atrp = _safe_f(c.atr_pct)
            if atrp is not None and float(atrp) > 5.0:
                penalty += _clamp((float(atrp) - 5.0) * 0.10, 0.0, 0.5)
        except Exception:
            pass

        return float(round(_clamp(penalty, 0.0, 2.5), 2))
    except Exception:
        return 0.0


def _infer_type(symbol: str) -> str:
    # Best effort. Alpaca assets typing is not passed into this engine.
    # Treat common ETF tickers as ETF; else Stock.
    etf_core = {
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
        # Inverse / leveraged ETFs
        "SQQQ",
        "SOXS",
        "SDOW",
        "RWM",
        "SPXS",
        "SPXU",
        "QID",
        "SH",
        "PSQ",
        "UVXY",
        "VIXY",
        "EMB",
    }
    return "ETF" if str(symbol or "").strip().upper() in etf_core else "STOCK"


def _inject_core_etfs(universe: List[str]) -> List[str]:
    core = ["SPY", "QQQ", "IWM", "XLK", "XLE", "XLF"]
    out: List[str] = []
    seen = set()
    for s in (core + (universe or [])):
        sym = str(s or "").strip().upper()
        if not sym or sym in seen:
            continue
        seen.add(sym)
        out.append(sym)
    return out


def _is_tradeable_equity(symbol: str, price, volume) -> bool:
    try:
        px = float(price) if price is not None else 0.0
        vol = int(volume) if volume is not None else 0
    except Exception:
        return False
    if px < 2.00:
        return False
    if vol < 100000:
        return False
    sym = str(symbol).upper().strip() if symbol else ""
    if not sym:
        return False
    # Reject anything longer than 5 characters
    if len(sym) > 5:
        return False
    # Reject dots: covers DSX.WS style OTC warrants and preferred share classes (BRK.B)
    if "." in sym:
        return False
    # Reject warrants (W, WS), rights (R), units (U) by suffix — no length guard needed after len>5 check
    if sym.endswith("WS"):
        return False
    if sym[-1] in ("W", "R", "U"):
        return False
    return True


def _passes_universe_gates(
    *,
    symbol: str,
    last_price: Optional[float],
    avg_vol_30d: Optional[float],
    avg_dollar_vol_30d: Optional[float],
    spread_pct_now: Optional[float],
) -> Tuple[bool, List[str]]:
    flags: List[str] = []

    if last_price is None or last_price <= 0:
        return False, ["no_price"]
    if float(last_price) < 5.0:
        return False, ["penny_stock"]
    try:
        if float(last_price) > float(_max_pick_price()):
            return False, ["above_max_pick_price"]
    except Exception:
        return False, ["invalid_price"]

    # Liquidity gating is handled upstream (universe pre-ranking). Do NOT eliminate candidates
    # here; scoring will handle execution penalties.
    _ = avg_vol_30d
    _ = avg_dollar_vol_30d

    # Spread data can be unavailable depending on feed/entitlements.
    # Do NOT fail the candidate in that case; treat it as an execution penalty later.
    if spread_pct_now is not None and float(spread_pct_now) > 0.35:
        flags.append("wide_spread")

    return True, flags


def _compute_placeholder_raw_prob(*, tech: float, risk: float, exec_score: float, catalyst: float) -> float:
    base = 50.0
    base += (float(tech) - 5.0) * 4.0
    base += (float(risk) - 5.0) * 4.0
    base += (float(exec_score) - 5.0) * 3.0
    base += max(0.0, float(catalyst) - 5.0) * 2.0
    conf_0_100 = float(_clamp(base, 5.0, 85.0))
    return float(_clamp01(conf_0_100 / 100.0))


def _confidence_0_10_from_raw_prob(*, raw_prob: float, low_conviction: bool) -> float:
    compressed_prob = float(_clamp01(raw_prob)) ** 1.35
    if bool(low_conviction):
        compressed_prob *= 0.75
    return float(round(max(0.0, min(10.0, compressed_prob * 10.0)), 1))


def _high_grade(ai_score: float, execution_score: float, risk_score: float) -> bool:
    return bool((ai_score >= 6.2) and (execution_score >= 6.0) and (risk_score >= 5.5))


# ---------------------------------------------------------------------------
# Enhanced component scoring functions (v2 upgrade)
# ---------------------------------------------------------------------------

def _score_momentum(c: "_Candidate", roc5_rank: float, roc20_rank: float, slope_rank: float) -> float:
    """0–10: short-term return, medium-term return, MA stack quality, slope trend.
    Uses cross-sectional percentile ranks for ROC so relative strength is rewarded."""
    comps: List[Tuple[float, float]] = []

    # Short-term relative strength (ROC5 percentile)
    comps.append((0.20, float(roc5_rank)))

    # Medium-term relative strength (ROC20 percentile)
    comps.append((0.30, float(roc20_rank)))

    # MA stack: price vs SMA20 vs SMA50
    try:
        p = float(c.last_price or 0.0)
        m20 = float(c.sma20 or 0.0)
        m50 = float(c.sma50 or 0.0)
        if p > 0 and m20 > 0 and m50 > 0:
            if p >= m20 and m20 >= m50:
                # Full bull stack — bonus for how far above SMA20 (max 5%)
                dist = _clamp01((p - m20) / m20 / 0.05)
                ma01 = _clamp01(0.65 + 0.35 * dist)
            elif p >= m20:
                ma01 = 0.60
            elif p >= m50:
                ma01 = 0.40
            else:
                ma01 = 0.15
        elif p > 0 and m20 > 0:
            ma01 = 0.60 if p >= m20 else 0.30
        else:
            ma01 = 0.50
    except Exception:
        ma01 = 0.50
    comps.append((0.30, float(ma01)))

    # Slope consistency (cross-sectional rank)
    comps.append((0.20, float(slope_rank)))

    w = sum(wt for wt, _ in comps) or 1.0
    score01 = sum(wt * v for wt, v in comps) / w
    return float(round(_clamp(score01 * 10.0, 1.0, 10.0), 1))


def _score_volatility_tradability(c: "_Candidate") -> float:
    """0–10: ATR% sweet spot (1.5–4% ideal), daily range consistency.
    Too dead (<0.5%) or too chaotic (>10%) both score low."""
    try:
        atrp = float(c.atr_pct or 0.0)
        if atrp <= 0:
            return 5.0
        # Sweet-spot curve: peak at ~2.5%
        if atrp < 0.5:
            base01 = 0.20      # dead / no movement
        elif atrp < 1.0:
            base01 = _clamp01(0.20 + (atrp - 0.5) / 0.5 * 0.35)   # 0.20→0.55
        elif atrp < 1.5:
            base01 = _clamp01(0.55 + (atrp - 1.0) / 0.5 * 0.25)   # 0.55→0.80
        elif atrp <= 4.0:
            # Peak zone
            mid = 2.5
            dist = abs(atrp - mid) / 1.5
            base01 = _clamp01(1.0 - 0.15 * dist)                    # 0.85→1.0→0.85
        elif atrp <= 6.0:
            base01 = _clamp01(0.85 - (atrp - 4.0) / 2.0 * 0.35)   # 0.85→0.50
        elif atrp <= 9.0:
            base01 = _clamp01(0.50 - (atrp - 6.0) / 3.0 * 0.30)   # 0.50→0.20
        else:
            base01 = 0.15      # chaotic

        # Consistency bonus: low std-dev of daily returns = more predictable
        try:
            closes = list(c.closes or [])
            if len(closes) >= 10:
                rets = [float(closes[j]) / float(closes[j - 1]) - 1.0 for j in range(max(1, len(closes) - 14), len(closes)) if float(closes[j - 1]) > 0]
                if len(rets) >= 5:
                    mean_r = sum(rets) / len(rets)
                    var_r = sum((r - mean_r) ** 2 for r in rets) / len(rets)
                    std_r = math.sqrt(var_r) * 100.0  # in pct
                    # Low std = consistent; high std = chaotic
                    consist01 = _clamp01((4.0 - std_r) / 4.0)
                    base01 = _clamp01(0.80 * base01 + 0.20 * consist01)
        except Exception:
            pass

        return float(round(_clamp(base01 * 10.0, 1.0, 10.0), 1))
    except Exception:
        return 5.0


def _score_risk_reward(c: "_Candidate") -> float:
    """0–10: quality of the risk/reward setup.
    Computed from stop_distance_pct and upside_ratio (expected_move / risk).
    Clean tight stop + solid upside → high score."""
    try:
        sd = float(c.stop_distance_pct or 3.0)
        if sd <= 0:
            return 3.0

        # R:R via upside_ratio (expected ATR-move / stop distance)
        ur = float(c.upside_ratio or 0.0)

        # Ideal stop distance: 1–3.5%
        if sd < 0.5:
            stop_01 = 0.20   # too tight (unrealistic)
        elif sd <= 1.0:
            stop_01 = 0.55
        elif sd <= 3.5:
            stop_01 = 1.00   # sweet spot
        elif sd <= 5.0:
            stop_01 = 0.70
        elif sd <= 7.0:
            stop_01 = 0.40
        else:
            stop_01 = 0.15

        # Upside ratio quality
        if ur <= 0:
            rr_01 = 0.30
        elif ur < 0.8:
            rr_01 = 0.30
        elif ur < 1.0:
            rr_01 = 0.50
        elif ur < 1.5:
            rr_01 = 0.70
        elif ur < 2.0:
            rr_01 = 0.85
        else:
            rr_01 = 1.00   # outstanding upside

        score01 = 0.55 * stop_01 + 0.45 * rr_01
        return float(round(_clamp(score01 * 10.0, 1.0, 10.0), 1))
    except Exception:
        return 5.0


def _score_liquidity(c: "_Candidate", dollar_rank: float, spread_rank: float) -> float:
    """0–10: dollar volume quality + spread efficiency.
    Absolute dollar vol thresholds + cross-sectional ranks."""
    try:
        dv = float(c.avg_dollar_vol_30d or 0.0)

        # Absolute dollar volume tier (0–1)
        if dv >= 200_000_000:
            dv_abs = 1.00
        elif dv >= 100_000_000:
            dv_abs = 0.90
        elif dv >= 50_000_000:
            dv_abs = 0.78
        elif dv >= 20_000_000:
            dv_abs = 0.65
        elif dv >= 10_000_000:
            dv_abs = 0.52
        elif dv >= 5_000_000:
            dv_abs = 0.40
        else:
            dv_abs = 0.20

        # Blend absolute tier + cross-sectional rank
        dv_score01 = _clamp01(0.60 * dv_abs + 0.40 * float(dollar_rank))

        # Spread penalty (tight spread = good execution)
        sp = float(c.spread_pct_now or 0.0) if c.spread_pct_now is not None else None
        if sp is None:
            sp_01 = 0.60    # unknown → neutral
        elif sp <= 0.05:
            sp_01 = 1.00
        elif sp <= 0.10:
            sp_01 = 0.90
        elif sp <= 0.20:
            sp_01 = 0.75
        elif sp <= 0.35:
            sp_01 = 0.55
        else:
            sp_01 = 0.25

        score01 = 0.70 * dv_score01 + 0.30 * _clamp01(0.50 * sp_01 + 0.50 * float(spread_rank))
        return float(round(_clamp(score01 * 10.0, 1.0, 10.0), 1))
    except Exception:
        return 5.0


def _score_news(c: "_Candidate") -> float:
    """0–10: 5.0 = neutral (no data). Derived from existing sentiment_score
    which is already populated from news/LLM overlay. Never blocks pick."""
    try:
        return float(_clamp(float(c.sentiment_score or 5.0), 1.0, 10.0))
    except Exception:
        return 5.0


def _compute_enhanced_confidence(
    *,
    momentum: float,
    volatility: float,
    risk_reward: float,
    liquidity: float,
    news: float,
    high_grade: bool,
) -> float:
    """Replace static proxy formula with component-derived confidence."""
    # Weighted combination (all on 0–10 scale)
    raw = (
        0.28 * momentum
        + 0.22 * risk_reward
        + 0.20 * liquidity
        + 0.15 * volatility
        + 0.15 * news
    )
    # Compress toward middle (avoid false confidence)
    conf01 = _clamp01(raw / 10.0) ** 1.25
    # Low-grade penalty
    if not high_grade:
        conf01 *= 0.82
    return float(round(max(0.0, min(10.0, conf01 * 10.0)), 1))


def _build_pick_rationale(c: "_Candidate") -> List[str]:
    """Generate 1–3 deterministic reason strings for the pick."""
    reasons: List[str] = []

    # Momentum
    try:
        ms = float(c.momentum_score or 5.0)
        if ms >= 7.5:
            roc20_str = f"+{c.roc20:.1f}%" if c.roc20 and c.roc20 > 0 else ""
            stack = "above SMA20 & SMA50" if (c.last_price and c.sma20 and c.sma50 and float(c.last_price) >= float(c.sma20) >= float(c.sma50)) else "trending"
            reasons.append(f"Strong momentum: {roc20_str + ' ' if roc20_str else ''}{stack}".strip())
        elif ms >= 5.5:
            reasons.append("Moderate uptrend momentum.")
    except Exception:
        pass

    # Risk/Reward
    try:
        rrs = float(c.risk_reward_score or 5.0)
        ur = float(c.upside_ratio or 0.0)
        sd = float(c.stop_distance_pct or 3.0)
        if rrs >= 7.5:
            reasons.append(f"Clean R:R setup: {ur:.1f}x expected upside vs {sd:.1f}% risk.")
        elif rrs >= 5.5:
            reasons.append(f"Acceptable risk structure ({sd:.1f}% to stop).")
    except Exception:
        pass

    # Liquidity
    try:
        ls = float(c.liquidity_score or 5.0)
        dv = float(c.avg_dollar_vol_30d or 0.0)
        if ls >= 7.5 and dv > 0:
            dv_m = dv / 1_000_000
            reasons.append(f"High liquidity: ${dv_m:.0f}M avg daily dollar vol.")
        elif ls < 4.0:
            reasons.append("Lower liquidity — size positions conservatively.")
    except Exception:
        pass

    # Volatility tradability
    try:
        vs = float(c.volatility_score_0_10 or 5.0)
        atrp = float(c.atr_pct or 0.0)
        if vs >= 7.5:
            reasons.append(f"Good tradability: ATR {atrp:.1f}% (ideal range).")
        elif vs < 3.5:
            reasons.append(f"Elevated volatility (ATR {atrp:.1f}%) — use smaller size.")
    except Exception:
        pass

    # News overlay (only if active)
    try:
        if bool(c.llm_active) and float(c.news_score or 5.0) > 6.5:
            reasons.append("Constructive news/LLM sentiment overlay.")
        elif bool(c.llm_active) and float(c.news_score or 5.0) < 4.0:
            reasons.append("Cautious news sentiment — monitor catalyst closely.")
    except Exception:
        pass

    # Pre-mover signals (v4 upgrade)
    try:
        pm = float(c.premover_score_0_10 or 5.0)
        ext = float(c.overextended_penalty or 0.0)
        if ext >= 0.8:
            reasons.append("Caution: already extended — late-stage setup.")
        elif pm >= 7.5:
            reasons.append("Early volume build + pre-breakout structure detected.")
        elif pm >= 6.5:
            reasons.append("Volume building; approaching key resistance.")
    except Exception:
        pass

    # Fallback
    if not reasons:
        reasons.append("Best available setup under current market conditions.")

    return reasons[:3]


# ---------------------------------------------------------------------------
# Elite trading intelligence helpers (v3 upgrade)
# ---------------------------------------------------------------------------

def _classify_trade_quality(final_score: float, high_grade: bool, confidence: float) -> str:
    """Classify setup quality as A+/A/B/C/AVOID based on component alignment."""
    try:
        s = float(final_score or 0.0)
        c = float(confidence or 0.0)
        if not bool(high_grade):
            if s < 4.5:
                return "AVOID"
            return "C"
        if s >= 8.0 and c >= 7.5:
            return "A+"
        if s >= 7.0 and c >= 6.5:
            return "A"
        if s >= 6.0 and c >= 5.5:
            return "B"
        if s >= 5.0:
            return "C"
        return "AVOID"
    except Exception:
        return "B"


def _compute_position_size(
    stop_dist_pct: float,
    confidence: float,
    atr_pct: float,
    regime: str = "UNKNOWN",
    regime_strength: str = "unknown",
) -> Dict[str, Any]:
    """Return recommended position size % and risk level.
    Smaller stop → bigger size; higher confidence → bigger size; high ATR → smaller.
    Regime caps: BEAR strong ≤ 3%, BEAR moderate/CHOPPY ≤ 5%, BULL ≤ 10%."""
    try:
        sd = max(0.1, float(stop_dist_pct or 3.0))
        cf = _clamp(float(confidence or 5.0), 0.0, 10.0)
        at = max(0.1, float(atr_pct or 2.0))

        # Base size: 2% risk / stop_distance (Kelly-lite)
        base = 2.0 / sd * 100.0  # e.g. stop=2% → base=100%, cap below

        # Scale by confidence: cf=5 → 1.0x, cf=10 → 1.3x, cf=1 → 0.7x
        cf_mult = 0.7 + (cf / 10.0) * 0.6

        # Penalize high ATR: at=1% → 1.0x, at=4% → 0.7x
        atr_mult = _clamp(1.2 - (at - 1.0) * 0.10, 0.5, 1.2)

        raw_pct = base * cf_mult * atr_mult

        # Regime-based hard cap: in uncertain/hostile regimes keep position small
        _r = str(regime or "UNKNOWN").strip().upper()
        _rs = str(regime_strength or "").strip().lower()
        if _r == "BEAR" and _rs == "strong":
            regime_cap = 3.0
        elif _r == "BEAR":
            regime_cap = 5.0
        elif _r == "CHOPPY":
            regime_cap = 5.0
        elif _r == "UNKNOWN":
            regime_cap = 4.0
        else:  # BULL
            regime_cap = 10.0

        size_pct = round(_clamp(raw_pct, 1.0, regime_cap), 1)

        if size_pct >= 7.0:
            risk_level = "low"
        elif size_pct >= 4.0:
            risk_level = "medium"
        else:
            risk_level = "high"

        return {"position_size_pct": float(size_pct), "risk_level": risk_level}
    except Exception:
        return {"position_size_pct": 3.0, "risk_level": "medium"}


def _apply_regime_boost(c: "_Candidate", regime: str, regime_strength: str) -> None:
    """Adjust final_score_0_10 in-place based on market regime. Non-destructive cap at 1-10."""
    try:
        if c.final_score_0_10 is None:
            return
        fs = float(c.final_score_0_10)
        if regime == "BEAR":
            mult = 0.88 if regime_strength == "strong" else 0.93
            c.final_score_0_10 = float(round(_clamp(fs * mult, 1.0, 10.0), 1))
        elif regime == "CHOPPY":
            mult = 0.93 if regime_strength == "strong" else 0.96
            c.final_score_0_10 = float(round(_clamp(fs * mult, 1.0, 10.0), 1))
        elif regime == "BULL":
            # Reward momentum in bull — bonus proportional to momentum score
            mom_bonus = _clamp01((float(c.momentum_score or 5.0) - 5.0) / 5.0)
            mult = 1.0 + 0.05 * mom_bonus  # up to +5% for top momentum in bull
            c.final_score_0_10 = float(round(_clamp(fs * mult, 1.0, 10.0), 1))
    except Exception:
        pass


def _max_pick_price() -> float:
    try:
        v = float(os.getenv("BEST_PICK_MAX_PRICE", "300") or 300.0)
    except Exception:
        v = 300.0
    return float(max(5.0, min(100000.0, v)))


def _trade_plan_from_levels(*, direction: str, last_price: Optional[float], stop: Optional[float], atr14: Optional[float]) -> Dict[str, Any]:
    entry = last_price
    if entry is not None:
        entry = float(round(entry, 4))
    if stop is not None:
        stop = float(round(stop, 4))

    targets: List[Optional[float]] = [None, None, None]
    _MAX_TGT_PCT = 0.30  # swing-trade target cap: 30% above entry
    try:
        if entry is not None and stop is not None and entry > 0 and float(stop) < float(entry):
            r = float(entry) - float(stop)  # risk per share (stop guaranteed < entry)
            if r > 0:
                raw_tgts = [
                    float(round(entry + (1.5 * r), 4)),
                    float(round(entry + (2.5 * r), 4)),
                    float(round(entry + (4.0 * r), 4)),
                ]
                # Cap targets at 30% above entry; replace oversized targets with % steps
                capped = [min(t, entry * (1.0 + _MAX_TGT_PCT)) for t in raw_tgts]
                targets = capped
    except Exception:
        targets = [None, None, None]

    entry_method = "breakout continuation"
    if direction == "short":
        entry_method = "breakdown continuation"

    if atr14 is not None and entry is not None and float(entry) > 0:
        atr_pct = float(atr14) / float(entry) * 100.0
        if atr_pct >= 4.5:
            entry_method = "tight trigger only (high ATR)"

    return {
        "direction": direction,
        "entry": entry,
        "entry_method": entry_method,
        "stop": stop,
        "targets": targets,
        "time_stop": "Exit if no meaningful progress in 5 days",
    }


async def scan_best_pick_v2(
    *,
    universe: List[str],
    news_fetcher: Callable[[str], Dict[str, Any]],
    allow_llm_news: bool = True,
    max_seconds: float = 90.0,
    news_top_k: int = 25,
    prior_symbol: Optional[str] = None,
    repeat_min_edge: float = 0.15,
    scan_all: bool = False,
    top_movers_set: Optional[Set[str]] = None,
    momentum_bypass_map: Optional[Dict[str, float]] = None,
) -> Dict[str, Any]:
    start = time.time()
    t0 = start

    log_llm_enabled = bool(allow_llm_news)

    MIN_SYMBOLS_BEFORE_TIMEOUT = 5
    timeout_reached = False

    universe = list(universe or [])

    # Normalize symbols
    syms: List[str] = []
    seen = set()
    for s in universe or []:
        sym = str(s or "").strip().upper()
        if not sym or sym in seen:
            continue
        seen.add(sym)
        syms.append(sym)

    syms_before = len(syms)
    syms = [s for s in syms if _is_tradeable_equity(s, price=999.0, volume=999_999)]
    try:
        log.info(f"best_pick_v2: symbol_shape_filter removed={syms_before - len(syms)} remaining={len(syms)}")
    except Exception:
        pass

    try:
        log.info(f"best_pick_v2: universe_size={len(syms)}")
    except Exception:
        pass

    if not syms:
        return {
            "symbol": "",
            "type": "STOCK",
            "ai_score_0_10": 0.0,
            "execution_score_0_10": 0.0,
            "confidence_0_10": 0.0,
            "confidence_definition": "P(+1.5R before -1R in 7D)",
            "high_grade": False,
            "low_conviction": True,
            "low_conviction_note": "No universe provided — scan skipped.",
            "log_llm_enabled": bool(log_llm_enabled),
            "trade_plan": _trade_plan_from_levels(direction="long", last_price=None, stop=None, atr14=None),
            "catalysts": [],
            "risk_flags": ["empty_universe"],
            "error": "empty_universe",
            "scan_completed": False,
            "symbols_scanned": 0,
            "fallback_used": True,
            "pillar_scores_0_10": {"technical": 0.0, "catalyst": 0.0, "sentiment": 0.0, "risk_structure": 0.0, "upside": 0.0},
            "llm_reasoning": _normalize_llm_reasoning_payload({}),
            "llm_active": False,
        }

    # Fetch in chunks to avoid request limits.
    chunk_size = 200
    sem = asyncio.Semaphore(3)

    async def _fetch_chunk(chunk: List[str]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        async with sem:
            snaps_task = asyncio.to_thread(get_snapshots_batch, chunk)
            daily_task = asyncio.to_thread(get_bars_batch, chunk, "1Day", 50)
            snaps, daily_map = await asyncio.gather(snaps_task, daily_task)
            snaps0 = snaps if isinstance(snaps, dict) else {}
            daily0 = daily_map if isinstance(daily_map, dict) else {}

            # Normalize map keys to uppercase to avoid symbol-case mismatches.
            try:
                snaps0 = {str(k).strip().upper(): v for k, v in snaps0.items() if str(k).strip()}
            except Exception:
                snaps0 = snaps0 if isinstance(snaps0, dict) else {}
            try:
                daily0 = {str(k).strip().upper(): v for k, v in daily0.items() if str(k).strip()}
            except Exception:
                daily0 = daily0 if isinstance(daily0, dict) else {}

            return (snaps0, daily0)

    snaps_all: Dict[str, Any] = {}
    daily_all: Dict[str, Any] = {}

    tasks: List[asyncio.Task] = []
    for i in range(0, len(syms), chunk_size):
        chunk = syms[i : i + chunk_size]
        if not chunk:
            continue
        tasks.append(asyncio.create_task(_fetch_chunk(chunk)))

    for t in tasks:
        # Data fetch should be best-effort. If we run out of time budget, stop fetching more
        # but allow scoring to proceed with partial maps.
        if (not bool(scan_all)) and (time.time() - t0) > float(max_seconds):
            timeout_reached = True
            break
        try:
            s0, d0 = await t
            if isinstance(s0, dict):
                snaps_all.update({str(k).strip().upper(): v for k, v in s0.items() if str(k).strip()})
            if isinstance(d0, dict):
                daily_all.update({str(k).strip().upper(): v for k, v in d0.items() if str(k).strip()})
        except Exception:
            continue

    try:
        log.info({"bars_keys_sample": list(daily_all.keys())[:10], "symbols_sample": list(syms)[:10]})
    except Exception:
        pass

    # --- Market regime detection (5-min cached, non-blocking) ---
    regime_info: Dict[str, Any] = {}
    try:
        regime_info = await asyncio.wait_for(asyncio.to_thread(_detect_regime_full), timeout=8.0)
    except Exception:
        regime_info = {}
    if not isinstance(regime_info, dict):
        regime_info = {}
    regime_str = str(regime_info.get("regime") or "UNKNOWN").strip().upper()
    regime_strength = str(regime_info.get("regime_strength") or "unknown").strip().lower()
    try:
        log.info({"market_regime": regime_str, "regime_strength": regime_strength,
                  "vix_proxy": regime_info.get("vix_proxy"), "slope_5d": regime_info.get("trend_slope_5d")})
    except Exception:
        pass

    total_scanned = int(len(syms))
    try:
        bars_available = int(sum(1 for _k, v in (daily_all or {}).items() if isinstance(v, list) and len(v) >= 30))
    except Exception:
        bars_available = int(len(daily_all))

    cands: List[_Candidate] = []

    candidates_skipped_data = 0
    scored_count = 0
    skipped_count = 0

    # Gate-stage counters
    stage_price = 0
    stage_vol = 0
    stage_dollar = 0
    stage_spread = 0
    bar_fallback_count = 0  # symbols where bar data substituted for missing snapshot

    # Pre-compute SPY 5d and 3d ROC for relative-strength comparisons
    _spy_roc5: Optional[float] = None
    _spy_roc3: Optional[float] = None
    try:
        _spy_bars = daily_all.get("SPY")
        if isinstance(_spy_bars, list):
            _spy_closes = [_safe_f(b.get("c")) for b in _spy_bars if isinstance(b, dict)]
            _spy_closes = [v for v in _spy_closes if v is not None and float(v) > 0]
            _spy_roc5 = _roc([float(v) for v in _spy_closes], 5)
            _spy_roc3 = _roc([float(v) for v in _spy_closes], 3)
    except Exception:
        _spy_roc5 = None
        _spy_roc3 = None

    # First pass: compute raw features + apply hard universe gates
    for sym in syms:
        _elapsed_now = time.time() - t0
        # Hard wall: if we've used more than max_seconds, stop regardless of scored count
        if (not bool(scan_all)) and _elapsed_now > float(max_seconds):
            timeout_reached = True
            break
        if (not bool(scan_all)) and _elapsed_now > float(max_seconds) * 0.8 and int(scored_count) >= int(MIN_SYMBOLS_BEFORE_TIMEOUT):
            timeout_reached = True
            break

        snapshot = snaps_all.get(sym)
        if not isinstance(snapshot, dict):
            snapshot = {}

        daily_bars = daily_all.get(sym)
        if not isinstance(daily_bars, list):
            daily_bars = []

        # --- Price: snapshot first, then bar close fallback (works on weekends/after-hours) ---
        using_bar_fallback = False
        last_px = _last_price_from_snapshot(snapshot)
        if last_px is None and daily_bars:
            try:
                last_px = _safe_f((daily_bars[-1] or {}).get("c"))
                if last_px is not None:
                    using_bar_fallback = True
            except Exception:
                last_px = None

        # Price is required for any meaningful scoring.
        if last_px is None:
            skipped_count += 1
            continue
        try:
            if float(last_px) <= 0:
                skipped_count += 1
                continue
        except Exception:
            skipped_count += 1
            continue

        spread_pct_now = _spread_pct(snapshot) if snapshot else None

        MIN_BARS_REQUIRED = 15

        avg_vol_30d, avg_dollar_vol_30d = (None, None)
        try:
            if len(daily_bars) >= 15:
                avg_vol_30d, avg_dollar_vol_30d = _dollar_volume_30d(daily_bars)
        except Exception:
            avg_vol_30d, avg_dollar_vol_30d = (None, None)

        # --- Volume / dollar-volume bar fallback ---
        # _dollar_volume_30d requires 25+ bars; when unavailable (weekend, new listing, short history)
        # compute a simpler estimate from the last 5 bars so liquid stocks are not falsely rejected.
        if avg_vol_30d is None and daily_bars:
            try:
                tail5 = [b for b in daily_bars[-5:] if isinstance(b, dict)]
                vols5 = [_safe_f(b.get("v")) for b in tail5]
                vols5 = [v for v in vols5 if v is not None and v > 0]
                if vols5:
                    avg_vol_30d = float(sum(vols5) / len(vols5))
                    using_bar_fallback = True
            except Exception:
                pass

        if avg_dollar_vol_30d is None and daily_bars and last_px is not None:
            try:
                tail5 = [b for b in daily_bars[-5:] if isinstance(b, dict)]
                dvols5 = []
                for b in tail5:
                    c = _safe_f(b.get("c"))
                    v = _safe_f(b.get("v"))
                    if c is not None and v is not None and c > 0 and v > 0:
                        dvols5.append(float(c) * float(v))
                if dvols5:
                    avg_dollar_vol_30d = float(sum(dvols5) / len(dvols5))
                    using_bar_fallback = True
            except Exception:
                pass

        if using_bar_fallback:
            bar_fallback_count += 1

        try:
            log.debug(
                {
                    "symbol": sym,
                    "price": (float(last_px) if last_px is not None else None),
                    "avg_dollar_volume": (float(avg_dollar_vol_30d) if avg_dollar_vol_30d is not None else None),
                    "bars_len": int(len(daily_bars) if isinstance(daily_bars, list) else 0),
                    "bar_fallback": using_bar_fallback,
                    "passed_price_gate": (last_px is not None and float(last_px) >= 5.0),
                    "passed_liquidity_gate": (avg_dollar_vol_30d is not None and float(avg_dollar_vol_30d) >= 100_000.0),
                    "passed_bar_gate": (len(daily_bars) >= int(MIN_BARS_REQUIRED)),
                }
            )
        except Exception:
            pass

        # HARD universe gates BEFORE scoring: warrants, SPAC units, penny stocks, illiquid names.
        _px_gate = float(last_px) if last_px is not None else None
        # Use avg_vol_30d (real or bar-estimated); if still None default to a passable value
        # so symbol shape/name check in _is_tradeable_equity can still run.
        if avg_vol_30d is not None:
            _vol_gate = float(avg_vol_30d)
        else:
            _vol_gate = 150_000.0  # assume tradeable when no volume data at all
        if not _is_tradeable_equity(sym, _px_gate, _vol_gate):
            skipped_count += 1
            continue

        try:
            if float(last_px) < 5.0:
                skipped_count += 1
                continue
        except Exception:
            skipped_count += 1
            continue

        # Dollar-volume gate: hard-reject only when data is present and clearly too low.
        # When avg_dollar_vol_30d is None it means the feed didn't provide volume data (e.g. IEX
        # returning v=0 for all daily bars). In that case pass through with a penalty flag rather
        # than blindly rejecting every stock in the universe.
        # Symbols confirmed as today's top movers bypass this gate — their intraday liquidity is
        # already proven by today's volume and a 30-day average would unfairly exclude them.
        _is_top_mover = bool(top_movers_set) and sym in top_movers_set
        gate_flags: List[str] = []
        try:
            if not _is_top_mover and avg_dollar_vol_30d is not None and float(avg_dollar_vol_30d) < 100_000.0:
                skipped_count += 1
                continue
            if avg_dollar_vol_30d is None:
                gate_flags.append("no_vol_data")
        except Exception:
            gate_flags.append("no_vol_data")
        try:
            if spread_pct_now is not None and float(spread_pct_now) > 0.35:
                gate_flags.append("wide_spread")
        except Exception:
            pass

        if last_px is not None and float(last_px) >= 5.0:
            stage_price += 1
        if avg_vol_30d is not None and float(avg_vol_30d) >= 300_000.0:
            stage_vol += 1
        if avg_dollar_vol_30d is not None and float(avg_dollar_vol_30d) >= 5_000_000.0:
            stage_dollar += 1
        if spread_pct_now is None or (spread_pct_now is not None and float(spread_pct_now) <= 0.35):
            stage_spread += 1

        closes: List[float] = []
        highs: List[float] = []
        lows: List[float] = []
        for b in daily_bars[-220:]:
            if not isinstance(b, dict):
                continue
            c = _safe_f(b.get("c"))
            h = _safe_f(b.get("h"))
            l = _safe_f(b.get("l"))
            if c is None or h is None or l is None:
                continue
            if c <= 0:
                continue
            closes.append(float(c))
            highs.append(float(h))
            lows.append(float(l))

        limited_history = bool(len(closes) < 20)

        # Hard data requirement: need at least MIN_BARS_REQUIRED valid daily bars.
        if len(closes) < int(MIN_BARS_REQUIRED):
            candidates_skipped_data += 1
            skipped_count += 1
            continue

        # Risk flags are informational only; structural disqualifiers are enforced via hard gates above.

        sma20 = _sma(closes, 20)
        sma50 = _sma(closes, 50)
        rsi14 = _rsi(closes, 14)
        roc5 = _roc(closes, 5)
        roc20 = _roc(closes, 20)
        slope20 = _slope(closes, 20)
        atr14 = _atr(highs, lows, closes, 14)

        # ATR fallback proxy: avg(high-low) over available bars
        if atr14 is None:
            try:
                spans: List[float] = []
                for j in range(max(0, len(highs) - 20), len(highs)):
                    spans.append(abs(float(highs[j]) - float(lows[j])))
                if spans:
                    atr14 = float(sum(spans) / float(len(spans) or 1))
            except Exception:
                atr14 = None

        # Hard requirement: ATR must be available (real or proxy). If missing, skip.
        if atr14 is None:
            candidates_skipped_data += 1
            skipped_count += 1
            continue

        atr_pct = None
        if atr14 is not None and last_px is not None and float(last_px) > 0:
            atr_pct = float(atr14) / float(last_px) * 100.0

        swing_low_10 = _swing_low(daily_bars, 10)
        stop = None
        # stop: below swing low or SMA20 (whichever lower), small buffer.
        # BUG FIX (2026-05-03): only use a level as a stop candidate when it is
        # STRICTLY BELOW the current price. SMA20 can sit above price for stocks
        # in a downtrend (e.g. CLSK post-sell-off), which caused stop > entry.
        try:
            _px_ref = float(last_px) if last_px is not None and float(last_px) > 0 else None
            candidates_stop = [
                x for x in [swing_low_10, sma20]
                if x is not None and float(x) > 0
                and (_px_ref is None or float(x) < _px_ref)
            ]
            if candidates_stop:
                stop = float(min(candidates_stop)) * 0.995
        except Exception:
            stop = None

        # Stop fallback: ATR-based, then 3% baseline — always relative to current price
        if stop is None and last_px is not None and float(last_px) > 0:
            try:
                if atr14 is not None and float(atr14) > 0:
                    stop = max(float(last_px) * 0.95, float(last_px) - 1.5 * float(atr14))
                else:
                    stop = float(last_px) * 0.97
            except Exception:
                stop = None

        # Hard guarantee: stop must be strictly below current price
        if stop is not None and last_px is not None and float(stop) >= float(last_px):
            stop = float(last_px) * 0.95

        stop_dist_pct = None
        if stop is not None and last_px is not None and float(last_px) > 0:
            stop_dist_pct = (float(last_px) - float(stop)) / float(last_px) * 100.0

        if stop_dist_pct is None:
            stop_dist_pct = 3.0

        expected_move_5d = None
        if atr14 is not None:
            expected_move_5d = float(atr14) * math.sqrt(5.0)

        upside_ratio = None
        try:
            if expected_move_5d is not None and last_px is not None and stop is not None:
                stop_dist_abs = abs(float(last_px) - float(stop))
                if stop_dist_abs > 0:
                    upside_ratio = float(expected_move_5d) / float(stop_dist_abs)
        except Exception:
            upside_ratio = None

        cands.append(
            _Candidate(
                symbol=sym,
                type=_infer_type(sym),
                snapshot=snapshot,
                daily_bars=daily_bars,
                last_price=last_px,
                spread_pct_now=spread_pct_now,
                avg_vol_30d=avg_vol_30d,
                avg_dollar_vol_30d=avg_dollar_vol_30d,
                closes=closes,
                highs=highs,
                lows=lows,
                sma20=sma20,
                sma50=sma50,
                rsi14=rsi14,
                roc5=roc5,
                roc20=roc20,
                slope20=slope20,
                atr14=atr14,
                atr_pct=atr_pct,
                stop=stop,
                stop_distance_pct=stop_dist_pct,
                expected_move_5d=expected_move_5d,
                upside_ratio=upside_ratio,
                catalysts=[],
                risk_flags=(list(gate_flags or []) + (["limited_history"] if limited_history else [])),
                is_momentum_bypass=bool(momentum_bypass_map and sym in momentum_bypass_map),
            )
        )
        scored_count += 1

    try:
        log.info(
            f"best_pick_v2: funnel "
            f"symbols_requested={total_scanned} "
            f"symbols_scored={scored_count} "
            f"candidates_passed_gates={len(cands)} "
            f"skipped_no_data={candidates_skipped_data} "
            f"bar_fallback={bar_fallback_count} | "
            f"gate_stages: price={stage_price} vol={stage_vol} dvol={stage_dollar} spread={stage_spread}"
        )
    except Exception:
        pass

    try:
        rejected = int(skipped_count)
        remaining = int(len(cands))
        log.info(f"Filtered {rejected} non-tradeable symbols, {remaining} candidates remaining")
    except Exception:
        pass

    if not cands:
        return {"error": "no_symbols_passed_universe_gates", "candidates_scanned": int(total_scanned)}

    # Cross-sectional normalization (percentile ranks)
    roc5_r  = _percentile_ranks([c.roc5  for c in cands])
    roc20_r = _percentile_ranks([c.roc20 for c in cands])
    slope_r = _percentile_ranks([c.slope20 for c in cands])
    dollar_r = _percentile_ranks([c.avg_dollar_vol_30d for c in cands])
    # If spread is missing, treat it as neutral (0.5) rather than a hard failure.
    spread_r = _percentile_ranks([(-1.0 * float(c.spread_pct_now)) if c.spread_pct_now is not None else 0.0 for c in cands])
    atrp_r = _percentile_ranks([(-1.0 * float(c.atr_pct)) if c.atr_pct is not None else None for c in cands])
    upside_r = _percentile_ranks([c.upside_ratio for c in cands])

    for i, c in enumerate(cands):
        # Technical: MA alignment + ROC + slope + RSI sweet spot
        if "limited_history" in (c.risk_flags or []):
            c.technical_score = 5.0
        else:
            ma01 = 0.0
            try:
                if c.last_price is not None and c.sma20 is not None and c.sma50 is not None:
                    if float(c.last_price) >= float(c.sma20) >= float(c.sma50):
                        ma01 = 1.0
                    elif float(c.last_price) >= float(c.sma20):
                        ma01 = 0.7
                    elif float(c.last_price) < float(c.sma20) <= float(c.sma50):
                        ma01 = 0.2
                    else:
                        ma01 = 0.4
            except Exception:
                ma01 = 0.4

            rsi01 = 0.5
            try:
                r = float(c.rsi14 or 50.0)
                if 55.0 <= r <= 70.0:
                    rsi01 = 1.0
                elif r >= 80.0:
                    rsi01 = 0.35
                elif r >= 70.0:
                    rsi01 = 0.7
                elif r <= 35.0:
                    rsi01 = 0.25
                else:
                    rsi01 = 0.55
            except Exception:
                rsi01 = 0.5

            tech01 = _clamp01((0.35 * ma01) + (0.35 * roc20_r[i]) + (0.20 * slope_r[i]) + (0.10 * rsi01))
            c.technical_score = _score_1_10_from_01(tech01)

        # Risk structure: stop distance band
        # Bars fallback rule: if we have limited history / data quality, keep risk neutral.
        if ("limited_history" in (c.risk_flags or [])) or ("data_quality" in (c.risk_flags or [])):
            c.risk_structure_score = 5.0
        else:
            rs01 = 0.3
            try:
                sd = float(c.stop_distance_pct) if c.stop_distance_pct is not None else None
                if sd is None or sd <= 0:
                    rs01 = 0.2
                elif 1.0 <= sd <= 3.5:
                    rs01 = 1.0
                elif sd < 0.6:
                    rs01 = 0.25
                elif sd <= 5.0:
                    rs01 = 0.7
                elif sd <= 6.0:
                    rs01 = 0.45
                else:
                    rs01 = 0.15
            except Exception:
                rs01 = 0.3

            # Penalize extreme ATR% for low-risk mode
            try:
                if c.atr_pct is not None and float(c.atr_pct) >= 7.0:
                    rs01 = min(rs01, 0.35)
            except Exception:
                pass

            c.risk_structure_score = _score_1_10_from_01(rs01)

        # Upside (bounded)
        up01 = upside_r[i]
        try:
            if c.atr_pct is not None and float(c.atr_pct) >= 7.0:
                up01 = min(float(up01), 0.55)
        except Exception:
            pass
        c.upside_score = _score_1_10_from_01(up01)

        # Execution score: dollar vol + spread + ATR% (lower is better) + stability placeholder
        exec01 = _clamp01((0.55 * dollar_r[i]) + (0.25 * atrp_r[i]) + (0.15 * spread_r[i]) + (0.05 * 0.6))
        c.execution_score = _score_1_10_from_01(exec01)

        # --- Enhanced component scores (v2 upgrade) ---
        c.momentum_score       = _score_momentum(c, roc5_r[i], roc20_r[i], slope_r[i])
        c.volatility_score_0_10 = _score_volatility_tradability(c)
        c.risk_reward_score    = _score_risk_reward(c)
        c.liquidity_score      = _score_liquidity(c, dollar_r[i], spread_r[i])
        # news_score defaults to 5.0 (neutral); upgraded after news/LLM loop below.

        # Catalyst: placeholder before earnings calendar is fully wired.
        # Start neutral; later upgraded from news/earnings signals.
        c.catalyst_score = 5.0
        c.sentiment_score = 5.0

    # Pre-rank without news sentiment to limit news calls
    def _pre_rank_key(x: _Candidate) -> float:
        # favor technical + execution + risk (no sentiment yet)
        return (0.45 * x.technical_score) + (0.30 * x.execution_score) + (0.25 * x.risk_structure_score)

    cands.sort(key=_pre_rank_key, reverse=True)

    # News sentiment enrichment (top K). Must NEVER filter/remove candidates.
    # Only runs when allow_llm_news is enabled; otherwise sentiment remains neutral baseline.
    k = max(0, min(50, int(news_top_k or 25)))
    if bool(log_llm_enabled) and k > 0:
        for c in cands[:k]:
            if (not bool(scan_all)) and (time.time() - t0) > float(max_seconds) and int(scored_count) >= int(MIN_SYMBOLS_BEFORE_TIMEOUT):
                timeout_reached = True
                break

            # Default baseline: never penalize when news/LLM is missing.
            c.sentiment_score = 5.0

            try:
                n0 = await asyncio.to_thread(news_fetcher, c.symbol)
            except Exception as e:
                try:
                    log.exception(f"best_pick_v2: news_fetcher failed for {c.symbol}: {e}")
                except Exception:
                    pass
                n0 = {}

            if not isinstance(n0, dict):
                n0 = {}
            c.news_obj = n0
            c.llm_reasoning = _normalize_llm_reasoning_payload({})
            c.llm_active = False

            # Only compute sentiment if a score is explicitly present.
            try:
                sc = n0.get("score")
                if sc is None:
                    sc = n0.get("sentiment_score")
                if sc is not None:
                    scf = float(sc)
                    scf = _clamp(scf, -100.0, 100.0)
                    sent01 = (scf + 100.0) / 200.0
                    c.sentiment_score = _score_1_10_from_01(sent01)
            except Exception:
                c.sentiment_score = 5.0

            # Catalyst score: use catalysts array length + unusual options signals if present
            cat01 = 0.5
            try:
                cats = n0.get("catalysts") if isinstance(n0.get("catalysts"), list) else n0.get("key_catalysts")
                if not isinstance(cats, list):
                    cats = []
                c.catalysts = [str(x).strip() for x in cats if str(x).strip()][:6]
                cat01 = _clamp01(0.35 + 0.10 * float(len(c.catalysts)))
            except Exception:
                c.catalysts = []
                cat01 = 0.5

            # Earnings proximity (basic): if LLM risk flags mention earnings, boost catalyst but add risk flag.
            rf: List[str] = []
            try:
                rfs = n0.get("risk_flags") if isinstance(n0.get("risk_flags"), list) else []
                rf = [str(x).strip() for x in rfs if str(x).strip()][:8]
            except Exception:
                rf = []

            # Never overwrite existing risk flags from the core scoring pipeline.
            # LLM/news risk flags are additive.
            try:
                c.risk_flags = list(dict.fromkeys(list(c.risk_flags or []) + list(rf or [])))
            except Exception:
                c.risk_flags = list(dict.fromkeys(list(c.risk_flags or [])))

            try:
                joined = " ".join([str(x).lower() for x in (c.risk_flags or [])])
                if "earnings" in joined:
                    cat01 = min(1.0, float(cat01) + 0.15)
            except Exception:
                pass

            c.catalyst_score = _score_1_10_from_01(cat01)

            try:
                direction = str(n0.get("direction") or "NEUTRAL").strip().upper()
                sentiment_source = str(n0.get("sentiment_source") or "").strip().lower()
                llm_error = str(n0.get("llm_error") or "").strip()
                llm_reasoning_raw: Dict[str, Any] = {
                    "bullish_factors": [],
                    "bearish_factors": [],
                    "catalysts": list(c.catalysts or []),
                    "risks": list(rf or []),
                    "summary": str(n0.get("summary") or "").strip(),
                }
                if direction == "BULLISH":
                    llm_reasoning_raw["bullish_factors"] = ["LLM/news sentiment is constructive."]
                elif direction == "BEARISH":
                    llm_reasoning_raw["bearish_factors"] = ["LLM/news sentiment is cautious."]
                c.llm_reasoning = _normalize_llm_reasoning_payload(llm_reasoning_raw)
                c.llm_active = bool(sentiment_source == "llm" and not llm_error)
            except Exception:
                c.llm_reasoning = _normalize_llm_reasoning_payload({})
                c.llm_active = False

    # Populate news_score from sentiment_score (set by LLM/news loop above, or still 5.0 neutral)
    for c in cands:
        c.news_score = _score_news(c)

    # Final AI score (with bounded upside)
    for c in cands:
        # Earnings proximity: do not eliminate/cap; apply a risk penalty.
        try:
            joined = " ".join([str(x).lower() for x in (c.risk_flags or [])])
            if "earnings" in joined:
                c.risk_structure_score = float(max(1.0, float(c.risk_structure_score) - 1.0))
        except Exception:
            pass

        ai = (
            (0.30 * c.technical_score)
            + (0.20 * c.catalyst_score)
            + (0.15 * c.sentiment_score)
            + (0.20 * c.risk_structure_score)
            + (0.15 * c.upside_score)
        )

        try:
            lr = c.llm_reasoning if isinstance(c.llm_reasoning, dict) else _normalize_llm_reasoning_payload({})
            c.llm_reasoning = lr
            ai_boost = 0.0
            if bool(c.llm_active):
                if lr.get("bullish_factors"):
                    ai_boost += 0.2
                if lr.get("catalysts"):
                    ai_boost += 0.1
            ai += float(ai_boost)
        except Exception:
            pass

        c.ai_score = float(round(_clamp(ai, 1.0, 10.0), 1))

        # Compute final_score_0_10: weighted blend of all enhanced components
        try:
            c.final_score_0_10 = float(round(_clamp(
                0.28 * c.momentum_score
                + 0.22 * c.risk_reward_score
                + 0.20 * c.liquidity_score
                + 0.15 * c.volatility_score_0_10
                + 0.10 * c.technical_score   # existing tech as supporting pillar
                + 0.05 * c.news_score,
                1.0, 10.0), 1))
        except Exception:
            c.final_score_0_10 = float(c.ai_score)

        # Pre-mover score + overextension penalty
        try:
            c.premover_score_0_10 = _score_premover_v2(c)
            c.overextended_penalty = _compute_overextension_penalty(c)
        except Exception:
            c.premover_score_0_10 = 5.0
            c.overextended_penalty = 0.0

        # Intraday momentum boost: stocks up >15% today are proven same-day movers.
        # _score_premover_v2 uses 5-day patterns and misses these — override its floor.
        try:
            _snap = c.snapshot if isinstance(c.snapshot, dict) else {}
            _prev_db = _snap.get("prevDailyBar") if isinstance(_snap.get("prevDailyBar"), dict) else {}
            _prev_c = float(_prev_db.get("c") or 0.0)
            _cur_px = float(c.last_price or 0.0)
            if _prev_c > 0 and _cur_px > 0 and (_cur_px - _prev_c) / _prev_c * 100.0 > 15.0:
                c.premover_score_0_10 = max(c.premover_score_0_10, 6.0)
        except Exception:
            pass

        # Edge signal detection + scoring
        try:
            c.edge_signals = _detect_edge_signals(c, spy_roc5=_spy_roc5)
            _edge_pts = (
                (3 if "MOMENTUM_EXPANSION"  in c.edge_signals else 0)
                + (3 if "BREAKOUT_STRUCTURE"  in c.edge_signals else 0)
                + (2 if "RS_LEADER"           in c.edge_signals else 0)
                + (2 if "VOLATILITY_EXPANSION" in c.edge_signals else 0)
            )
            c.edge_score_0_10 = float(_clamp(_edge_pts * 10.0 / 10.0, 0.0, 10.0))
        except Exception:
            c.edge_signals = []
            c.edge_score_0_10 = 0.0

        # ── Edge signal boosts ────────────────────────────────────────────────
        # Every confirmed signal adds to final_score; combos get extra credit.
        # Premover floor is lifted when signals confirm but price hasn't moved yet
        # (that's the early-stage "coiling" pattern — our best historical setups).
        try:
            _sigs   = c.edge_signals or []
            _pm_now = float(c.premover_score_0_10 or 5.0)

            _has_mom  = "MOMENTUM_EXPANSION"  in _sigs
            _has_vola = "VOLATILITY_EXPANSION" in _sigs
            _has_brk  = "BREAKOUT_STRUCTURE"  in _sigs
            _has_rs   = "RS_LEADER"           in _sigs

            _n_sigs = sum([_has_mom, _has_vola, _has_brk, _has_rs])

            # Per-signal base boost (applies in all regimes)
            _base_boost = _n_sigs * 0.5
            c.final_score_0_10 = float(round(_clamp(float(c.final_score_0_10 or 1.0) + _base_boost, 1.0, 10.0), 1))

            # Combo bonus: MOMENTUM + VOLATILITY together = confirmed expansion
            if _has_mom and _has_vola:
                c.final_score_0_10 = float(round(_clamp(float(c.final_score_0_10 or 1.0) + 0.6, 1.0, 10.0), 1))

            # Combo bonus: MOMENTUM + BREAKOUT = price breaking structure with acceleration
            if _has_mom and _has_brk:
                c.final_score_0_10 = float(round(_clamp(float(c.final_score_0_10 or 1.0) + 0.5, 1.0, 10.0), 1))

            # Strong setup (3+ signals): lift premover floor so conviction gate can clear
            if _n_sigs >= 3 and _pm_now < 6.0:
                c.premover_score_0_10 = float(round(_clamp(max(_pm_now, 6.0), 1.0, 10.0), 1))
                _pm_now = float(c.premover_score_0_10)

            # ── GOLDEN PATTERN: MOMENTUM + VOLATILITY at early stage ─────────────
            # From 33 resolved picks: MOMENTUM+VOLATILITY with pm<6.0 → 4/4 big wins
            # (NOK +17%, CLSK +14%, SE +9%, ROIV +7.9%). These stocks are coiling
            # before the move — premover function penalises them unfairly.
            # Expanded from pm<5.0 to pm<6.0 to catch more early-stage setups.
            if _has_mom and _has_vola and _pm_now < 6.0:
                _lift = max(6.0 - _pm_now, 0.0) + 0.8   # always lift to at least 6.0+0.8=6.8
                c.premover_score_0_10 = float(round(_clamp(_pm_now + _lift, 1.0, 10.0), 1))
                c.final_score_0_10    = float(round(_clamp(float(c.final_score_0_10 or 1.0) + 1.2, 1.0, 10.0), 1))

            # RS_LEADER alone in BULL regime: confirmed relative strength is tradeable
            if _has_rs and regime_str == "BULL" and _pm_now < 6.0:
                c.premover_score_0_10 = float(round(_clamp(max(_pm_now, 5.8), 1.0, 10.0), 1))

        except Exception:
            pass

        # CHOPPY-specific signals: RSI_OVERSOLD_BOUNCE, SUPPORT_RECLAIM, SECTOR_ROTATION
        # Each fired signal adds 0.8 to final_score and is appended to edge_signals.
        if regime_str == "CHOPPY":
            try:
                _choppy_sigs = _detect_choppy_signals(c, spy_roc3=_spy_roc3)
                for _sig in _choppy_sigs:
                    c.edge_signals = list(c.edge_signals or [])
                    if _sig not in c.edge_signals:
                        c.edge_signals.append(_sig)
                    c.final_score_0_10 = float(round(_clamp(
                        float(c.final_score_0_10 or 1.0) + 0.8,
                        1.0, 10.0), 1))
            except Exception:
                pass

        # Integrate premover into final_score: +0.35 * premover - overextended_penalty
        try:
            _pm_center = 4.0 if regime_str == "CHOPPY" else 5.0
            c.final_score_0_10 = float(round(_clamp(
                float(c.final_score_0_10 or 1.0)
                + 0.35 * (float(c.premover_score_0_10) - _pm_center)  # centered: threshold=neutral, >threshold=boost, <threshold=drag
                - float(c.overextended_penalty),
                1.0, 10.0), 1))
        except Exception:
            pass

        # Neural network win-probability adjustment
        # The NN predicts P(win) from bar patterns + current subscores.
        # We blend ±0.75 pts into final_score: confident win → +0.75, confident loss → -0.75.
        # When the model isn't trained yet this is a no-op (nn_win_prob stays None).
        try:
            from ml.predictor import predict_win_prob_from_candidate, model_is_ready
            if model_is_ready():
                _nn_p = predict_win_prob_from_candidate(c)
                c.nn_win_prob = round(float(_nn_p), 3)
                # centre at 0.5; max ±1.5 pts adjustment (was ±0.75)
                _nn_delta = (_nn_p - 0.5) * 3.0
                c.final_score_0_10 = float(round(_clamp(
                    float(c.final_score_0_10 or 1.0) + _nn_delta,
                    1.0, 10.0), 1))
        except Exception:
            pass

        # Apply market regime adjustment to final_score (in-place, capped 1-10)
        _apply_regime_boost(c, regime_str, regime_strength)
        c.market_regime = regime_str

        # Caps
        # spread% > 0.35 -> execution penalty (cap)
        try:
            if c.spread_pct_now is not None and float(c.spread_pct_now) > 0.35:
                c.execution_score = float(min(c.execution_score, 6.0))
                if "wide_spread" not in (c.risk_flags or []):
                    c.risk_flags = list(c.risk_flags or []) + ["wide_spread"]
        except Exception:
            pass

        # stop_distance% > 6% -> ai <= 6.5
        try:
            if c.stop_distance_pct is not None and float(c.stop_distance_pct) > 6.0:
                c.ai_score = float(min(c.ai_score, 6.5))
        except Exception:
            pass

        _ = c  # placeholder

    # Rank: combined_score = 0.7*final_score + 0.3*edge_score, execution+ai as tiebreakers
    # Momentum bypass symbols (changePercent > 15%) get a 1.3x boost to final_score so they
    # rank above technically stable but low-momentum names like T or VZ.
    def _rank_key_final(x: _Candidate) -> Tuple[float, float, float]:
        _fs = float(x.final_score_0_10 if x.final_score_0_10 is not None else x.ai_score or 0.0)
        if x.is_momentum_bypass:
            _fs = _fs * 1.3
        _es = float(x.edge_score_0_10 or 0.0)
        combined = 0.7 * _fs + 0.3 * _es
        return (combined, float(x.execution_score or 0.0), float(x.ai_score or 0.0))

    cands.sort(key=_rank_key_final, reverse=True)

    # Final validation: never allow a disqualified symbol to win.
    def _passes_final_validation(cand: _Candidate) -> bool:
        try:
            if cand.last_price is None or float(cand.last_price) < 5.0:
                return False
        except Exception:
            return False
        try:
            if float(cand.last_price) > float(_max_pick_price()):
                return False
        except Exception:
            return False
        try:
            if cand.avg_dollar_vol_30d is None or float(cand.avg_dollar_vol_30d) < 5_000_000.0:
                return False
        except Exception:
            return False
        return True

    ordered_valid: List[_Candidate] = []
    try:
        stock_valid = [
            c
            for c in cands
            if _passes_final_validation(c)
            and str(c.type or "").strip().upper() != "ETF"
        ]
    except Exception:
        stock_valid = []
    try:
        etf_valid = [
            c
            for c in cands
            if _passes_final_validation(c)
            and str(c.type or "").strip().upper() == "ETF"
        ]
    except Exception:
        etf_valid = []
    ordered_valid = list(stock_valid) + list(etf_valid)

    if not ordered_valid:
        return {"error": "no_symbols_passed_universe_gates", "candidates_scanned": int(total_scanned)}

    best = ordered_valid[0]
    runner_up = ordered_valid[1] if len(ordered_valid) > 1 else None

    # Rotation rule: avoid repeating the prior symbol unless materially stronger.
    try:
        prior = str(prior_symbol or "").strip().upper()
    except Exception:
        prior = ""
    try:
        edge = float(repeat_min_edge)
    except Exception:
        edge = 0.15
    edge = max(0.0, min(0.50, float(edge)))

    try:
        if prior and runner_up is not None and str(best.symbol or "").strip().upper() == prior:
            top_s = float(_safe_f(best.ai_score) or 0.0)
            alt_s = float(_safe_f(runner_up.ai_score) or 0.0)
            score_diff = float(top_s) - float(alt_s)
            if score_diff < (float(top_s) * float(edge)):
                best = runner_up
    except Exception:
        pass

    candidates_scored = int(len(cands))
    try:
        candidates_passing = int(
            sum(1 for x in cands if _high_grade(float(x.ai_score), float(x.execution_score), float(x.risk_structure_score)))
        )
    except Exception:
        candidates_passing = 0

    if bool(timeout_reached):
        try:
            log.warning(
                {
                    "best_pick_v2_timeout": True,
                    "elapsed": float(time.time() - t0),
                    "max_seconds": float(max_seconds),
                    "scored_count": int(scored_count),
                    "candidates_scored": int(candidates_scored),
                }
            )
        except Exception:
            pass

    # Low conviction note
    high_grade = _high_grade(best.ai_score, best.execution_score, best.risk_structure_score)
    low_note = ""
    if len(cands) < 5 or not high_grade:
        low_note = "Low-conviction environment — defensive positioning preferred."

    confidence_0_10 = _compute_enhanced_confidence(
        momentum=best.momentum_score,
        volatility=best.volatility_score_0_10,
        risk_reward=best.risk_reward_score,
        liquidity=best.liquidity_score,
        news=best.news_score,
        high_grade=bool(high_grade),
    )

    direction = "long"
    try:
        if best.last_price is not None and best.sma20 is not None and float(best.last_price) < float(best.sma20):
            direction = "short"
    except Exception:
        direction = "long"

    # ── Live-price level anchoring ────────────────────────────────────────────
    # Fetch live snapshot BEFORE building trade levels so entry/stop/targets
    # reflect the current price, not yesterday's close. Scoring stays on daily bars.
    _live_px: Optional[float] = None
    _live_snap_ok = False
    _live_fallback_reason = "not_attempted"
    try:
        _snap_raw = await asyncio.wait_for(
            asyncio.to_thread(
                get_snapshot_normalized,
                str(best.symbol or "").strip().upper(),
            ),
            timeout=2.5,
        )
        if isinstance(_snap_raw, dict) and _snap_raw.get("snapshot_available"):
            _cand_px = _safe_f(_snap_raw.get("last_price"))
            if _cand_px is not None and float(_cand_px) > 0:
                _live_px      = float(_cand_px)
                _live_snap_ok = True
            else:
                _live_fallback_reason = "null_price"
        else:
            _live_fallback_reason = "snapshot_unavailable"
    except asyncio.TimeoutError:
        _live_fallback_reason = "timeout"
    except Exception:
        _live_fallback_reason = "error"

    _best_atr14 = _safe_f(best.atr14)

    if _live_snap_ok and _live_px is not None and _best_atr14 is not None and float(_best_atr14) > 0:
        _atr_value = float(_best_atr14)
        # 0.1% breakout buffer above current price
        _live_entry = float(round(_live_px * 1.001, 4))
        _MAX_TGT_PCT = 0.30  # targets capped at 30% from entry
        if direction == "long":
            _live_stop = float(round(_live_px - _atr_value * 1.5, 4))
            # Cap targets at 30% above entry; fall back to % steps if ATR is too large
            _t1 = float(round(_live_entry + _atr_value * 2.0, 4))
            _t2 = float(round(_live_entry + _atr_value * 3.0, 4))
            _t3 = float(round(_live_entry + _atr_value * 4.0, 4))
            if _t1 <= _live_entry * (1.0 + _MAX_TGT_PCT):
                _live_targets = [_t1, min(_t2, _live_entry * (1.0 + _MAX_TGT_PCT)), min(_t3, _live_entry * (1.0 + _MAX_TGT_PCT))]
            else:
                _live_targets = [
                    float(round(_live_entry * 1.10, 4)),
                    float(round(_live_entry * 1.20, 4)),
                    float(round(_live_entry * 1.28, 4)),
                ]
        else:  # short
            _live_stop = float(round(_live_px + _atr_value * 1.5, 4))
            _live_targets = [
                float(round(_live_entry - _atr_value * 2.0, 4)),
                float(round(_live_entry - _atr_value * 3.0, 4)),
                float(round(_live_entry - _atr_value * 4.0, 4)),
            ]
        trade_plan = _trade_plan_from_levels(
            direction=direction,
            last_price=_live_entry,
            stop=_live_stop,
            atr14=_best_atr14,
        )
        trade_plan["targets"]         = _live_targets
        trade_plan["stale_data"]      = False
        trade_plan["live_price_used"] = True
        log.info(
            f"best_pick_v2: live_levels {best.symbol} "
            f"live_px={_live_px:.2f} entry={_live_entry:.2f} "
            f"stop={_live_stop:.2f} atr={_atr_value:.2f}"
        )
    else:
        # Fallback: daily-bar close as anchor (yesterday's price)
        trade_plan = _trade_plan_from_levels(
            direction=direction,
            last_price=best.last_price,
            stop=best.stop,
            atr14=best.atr14,
        )
        trade_plan["stale_data"]      = True
        trade_plan["live_price_used"] = False
        log.info(
            f"best_pick_v2: live_levels_fallback {best.symbol} "
            f"reason={_live_fallback_reason} daily_close={best.last_price}"
        )

    if int(candidates_passing) <= 0:
        low_note = "No A-grade setups passed all filters — returning strongest low-conviction mover."

    # Build pick rationale after all scores are finalized
    try:
        best.pick_rationale = _build_pick_rationale(best)
    except Exception:
        best.pick_rationale = ["Best available setup under current market conditions."]

    # Trade quality + position sizing (elite intelligence layer)
    try:
        best.trade_quality = _classify_trade_quality(
            float(best.final_score_0_10 or best.ai_score or 5.0),
            bool(high_grade),
            float(confidence_0_10),
        )
    except Exception:
        best.trade_quality = "B"

    try:
        ps = _compute_position_size(
            float(best.stop_distance_pct or 3.0),
            float(confidence_0_10),
            float(best.atr_pct or 2.0),
            regime=regime_str,
            regime_strength=regime_strength,
        )
        best.position_size_pct = float(ps.get("position_size_pct") or 3.0)
        best.risk_level = str(ps.get("risk_level") or "medium")
    except Exception:
        best.position_size_pct = 3.0
        best.risk_level = "medium"

    # Intraday momentum override: recheck best symbol before the NO_TRADE gate.
    # Stocks up >15% today should never be gated out by 5-day pre-mover patterns.
    # Also derives signal_count for the output (picked up by app.py pre_mover_context).
    # Seed with the already-detected edge signal count so the NO_TRADE gate sees
    # the real number of edge signals (not just the intraday-boost path).
    _intraday_signal_count: int = len(best.edge_signals or [])
    try:
        _b_snap = best.snapshot if isinstance(best.snapshot, dict) else {}
        _b_prev_db = _b_snap.get("prevDailyBar") if isinstance(_b_snap.get("prevDailyBar"), dict) else {}
        _b_prev_c = float(_b_prev_db.get("c") or 0.0)
        _b_px = float(best.last_price or 0.0)
        if _b_prev_c > 0 and _b_px > 0:
            _b_chg = (_b_px - _b_prev_c) / _b_prev_c * 100.0
            if _b_chg > 15.0:
                best.premover_score_0_10 = max(best.premover_score_0_10, 6.0)
                _intraday_signal_count = max(_intraday_signal_count, 2)
                log.info(
                    f"best_pick_v2: intraday_momentum_boost {best.symbol} "
                    f"chg={_b_chg:.1f}% premover_score→{best.premover_score_0_10:.1f} "
                    f"signal_count→{_intraday_signal_count}"
                )
    except Exception:
        pass

    # Trade decision: HIGH_CONVICTION / LOW_CONVICTION / NO_TRADE
    _no_trade_reason = ""
    try:
        _pm  = float(best.premover_score_0_10 or 5.0)
        _fs  = float(best.final_score_0_10 or best.ai_score or 5.0)
        _cf  = float(confidence_0_10)

        # ── Winning-pattern boost ──────────────────────────────────────────
        # Analyse recent big wins and reward candidates matching their signal
        # fingerprint. Only boosts — never penalises.
        _win_boost = _winning_pattern_boost(set(best.edge_signals or []))
        _pm = min(_pm + _win_boost * 0.5, 10.0)
        _fs = min(_fs + _win_boost * 0.4, 10.0)
        if _win_boost > 0.1:
            log.info("best_pick: winning-pattern boost +%.2f (pm→%.1f fs→%.1f)", _win_boost, _pm, _fs)

        _week_limit = _regime_weekly_limit(regime_str)
        _week_so_far = _weekly_picks_so_far()
        _week_full = _week_so_far >= _week_limit

        # Golden pattern: MOMENTUM_EXPANSION + VOLATILITY_EXPANSION = early-stage coiling.
        # NOK +17%, CLSK +14%, HPQ +11.5%, SE +9.1% all had this combo at low premover.
        # These historically fail the fs >= 5.8 gate despite being the best picks — lower
        # the fs threshold specifically for this combo.
        _golden_pattern = (
            "MOMENTUM_EXPANSION" in (best.edge_signals or []) and
            "VOLATILITY_EXPANSION" in (best.edge_signals or [])
        )

        # Minimum floor gate
        _pm_floor = 2.5 if regime_str == "CHOPPY" else 4.0
        _fs_floor = 4.5 if regime_str == "CHOPPY" else 5.2
        _no_trade_reason = ""

        if _pm < _pm_floor or _fs < _fs_floor:
            _parts = []
            if _fs < _fs_floor:
                _parts.append(f"final_score={_fs:.1f}<{_fs_floor}")
            if _pm < _pm_floor:
                _parts.append(f"premover={_pm:.1f}<{_pm_floor}")
            _no_trade_reason = "No quality setups found: " + "; ".join(_parts)
            best.trade_decision = "NO_TRADE"
            best.is_trade = False
        elif _week_full:
            # Weekly quota used — preserve as watchlist candidate, not a trade
            _no_trade_reason = f"Weekly quota met ({_week_so_far}/{_week_limit} picks in {regime_str} regime)"
            log.info("best_pick: %s", _no_trade_reason)
            best.trade_decision = "NO_TRADE"
            best.is_trade = False
        elif _pm >= 6.0 and _fs >= (6.0 if _golden_pattern else 6.5):
            best.trade_decision = "HIGH_CONVICTION"
            best.is_trade = True
        elif _pm >= 5.0 and _fs >= (5.0 if _golden_pattern else 5.8):
            best.trade_decision = "LOW_CONVICTION"
            best.is_trade = True
        elif _pm >= 4.0 and _fs >= 5.2:
            best.trade_decision = "NO_TRADE"
            best.is_trade = False
        else:
            best.trade_decision = "NO_TRADE"
            best.is_trade = False
    except Exception:
        best.trade_decision = "LOW_CONVICTION"
        best.is_trade = False

    # Append decision label to pick_rationale (allowed up to 4 items)
    try:
        rationale = list(best.pick_rationale or [])
        if best.trade_decision == "HIGH_CONVICTION":
            rationale.append("Strong pre-mover setup — early-stage breakout potential.")
        elif best.trade_decision == "LOW_CONVICTION":
            rationale.append("Moderate setup — lacks full confirmation.")
        else:
            rationale.append("No high-quality setups detected — market conditions not ideal.")
        best.pick_rationale = rationale[:4]
    except Exception:
        pass

    # Extract news intelligence from news_obj if available
    try:
        if isinstance(best.news_obj, dict):
            ns = str(best.news_obj.get("summary") or "").strip()
            if ns:
                best.news_summary = ns[:200]
            kd = best.news_obj.get("key_drivers")
            if isinstance(kd, list) and kd:
                best.key_drivers = [str(d).strip() for d in kd if str(d).strip()][:3]
            et = str(best.news_obj.get("event_type") or "").strip().lower()
            if et:
                best.event_type = et
    except Exception:
        pass

    # Earnings calendar filter: reject symbols with earnings within 14 days.
    _earnings_safe = True
    _days_to_earnings: Optional[int] = None
    try:
        _ec = await _check_earnings_safe(str(best.symbol or "").strip().upper(), window_days=14)
        _earnings_safe = bool(_ec.get("safe", True))
        _days_to_earnings = _ec.get("days_to_earnings")
        if not _earnings_safe:
            _dte = int(_days_to_earnings) if _days_to_earnings is not None else 0
            log.warning(f"{best.symbol} rejected: earnings in {_dte} days")
            best.trade_decision = "NO_TRADE"
            best.is_trade = False
    except Exception:
        _earnings_safe = True

    # Validate stop is strictly below entry — never surface a pick with broken levels.
    try:
        _tp_entry = _safe_f((trade_plan or {}).get("entry"))
        _tp_stop  = _safe_f((trade_plan or {}).get("stop"))
        if _tp_entry and _tp_stop and float(_tp_stop) >= float(_tp_entry):
            _no_trade_reason = (
                f"invalid_stop_above_entry — stop {_tp_stop:.2f} >= entry {_tp_entry:.2f}"
            )
            best.trade_decision = "NO_TRADE"
            best.is_trade = False
            log.warning(
                f"best_pick_v2: invalid_stop_above_entry {best.symbol} "
                f"stop={_tp_stop:.2f} entry={_tp_entry:.2f}"
            )
    except Exception:
        pass

    # Build watchlist_candidates: top 5 valid candidates (excluding best) that didn't clear the NO_TRADE gate
    try:
        _wl_pm_floor = 2.5 if regime_str == "CHOPPY" else 5.0
        _wl_fs_floor = 5.2 if regime_str == "CHOPPY" else 5.7
        _best_sym = str(best.symbol or "").strip().upper()
        _watchlist_candidates: List[Dict[str, Any]] = []
        for _wc in ordered_valid:
            if str(_wc.symbol or "").strip().upper() == _best_sym:
                continue
            _wc_pm = float(_wc.premover_score_0_10 or 5.0)
            _wc_fs = float(_wc.final_score_0_10 or _wc.ai_score or 5.0)
            if _wc_pm < _wl_pm_floor or _wc_fs < _wl_fs_floor:
                _wc_no_trade = True
            elif _wc_pm >= 7.0 and _wc_fs >= 7.5:
                _wc_no_trade = False
            elif _wc_pm >= 6.0 and _wc_fs >= 6.5:
                _wc_no_trade = False
            else:
                _wc_no_trade = True
            if not _wc_no_trade:
                continue
            _wc_high_grade = _high_grade(float(_wc.ai_score), float(_wc.execution_score), float(_wc.risk_structure_score))
            _wc_conf = _compute_enhanced_confidence(
                momentum=_wc.momentum_score,
                volatility=_wc.volatility_score_0_10,
                risk_reward=_wc.risk_reward_score,
                liquidity=_wc.liquidity_score,
                news=_wc.news_score,
                high_grade=bool(_wc_high_grade),
            )
            import datetime as _dt_wl
            _wc_sym = str(_wc.symbol or "").strip().upper()
            _wc_earnings_risk = False
            _wc_earnings_note = ""
            _wc_days_to_earnings = None
            if _wc_sym in KNOWN_EARNINGS_DATES:
                try:
                    _wc_ed = _dt_wl.datetime.strptime(KNOWN_EARNINGS_DATES[_wc_sym], "%Y-%m-%d").date()
                    _wc_today = _dt_wl.datetime.now().date()
                    _wc_days_to_earnings = (_wc_ed - _wc_today).days
                    if 0 <= _wc_days_to_earnings <= 14:
                        _wc_earnings_risk = True
                        _wc_earnings_note = f"Earnings in {_wc_days_to_earnings}d ({KNOWN_EARNINGS_DATES[_wc_sym]})"
                        log.info(
                            "watchlist: %s flagged earnings_risk — %dd to earnings",
                            _wc_sym, _wc_days_to_earnings,
                        )
                except Exception as _wc_e:
                    log.warning("watchlist: earnings lookup failed for %s: %s", _wc_sym, _wc_e)
            _watchlist_candidates.append({
                "symbol": str(_wc.symbol or ""),
                "final_score": float(round(_wc_fs, 4)),
                "confidence": float(round(float(_wc_conf), 4)),
                "premover": float(round(_wc_pm, 4)),
                "edge_signals": list(_wc.edge_signals or []),
                "earnings_risk": bool(_wc_earnings_risk),
                "earnings_note": _wc_earnings_note,
                "days_to_earnings": _wc_days_to_earnings,
            })
            if len(_watchlist_candidates) >= 5:
                break
    except Exception:
        _watchlist_candidates = []

    # Final-line symbol shape guard: warrants/rights/units must never appear in output
    _final_sym = str(best.symbol or "").strip().upper()
    if _final_sym.endswith(("WS", "W", "R", "U")) and len(_final_sym) >= 4:
        log.warning(f"best_pick_v2: final_guard rejected warrant/right/unit symbol={_final_sym}")
        best.trade_decision = "NO_TRADE"
        best.is_trade = False

    out = {
        "symbol": best.symbol,
        "type": best.type,
        "ai_score": float(best.ai_score),
        "ai_score_0_10": float(best.ai_score),
        "execution_score": float(best.execution_score),
        "execution_score_0_10": float(best.execution_score),
        "confidence_0_10": float(confidence_0_10),
        "confidence_definition": "P(+1.5R before -1R in 7D)",
        "high_grade": bool(high_grade),
        "low_conviction": bool(not high_grade),
        "low_conviction_note": str(low_note),
        "conviction": _conviction_label(float(best.ai_score)),
        "log_llm_enabled": bool(log_llm_enabled),
        "total_scanned": int(total_scanned),
        "symbols_scanned": int(total_scanned),   # explicit alias — total symbols fed into scanner
        "bars_available": int(bars_available),
        "scored_count": int(scored_count),
        "skipped_count": int(skipped_count),
        "candidates_scored": int(candidates_scored),
        "candidates_passing_threshold": int(candidates_passing),
        "candidates_skipped_data": int(candidates_skipped_data),
        "trade_plan": trade_plan,
        "catalysts": list(best.catalysts or []),
        "risk_flags": list(best.risk_flags or []),
        "llm_reasoning": (
            _normalize_llm_reasoning_payload(best.llm_reasoning)
            if isinstance(best.llm_reasoning, dict)
            else _normalize_llm_reasoning_payload({})
        ),
        "llm_active": bool(best.llm_active),
        "pre_mover_context": {},
        "_pre_mover_input": {},
        "pillar_scores_0_10": {
            "technical": float(best.technical_score),
            "catalyst": float(best.catalyst_score),
            "sentiment": float(best.sentiment_score),
            "risk_structure": float(best.risk_structure_score),
            "upside": float(best.upside_score),
        },
        # Enhanced v2 component scores
        "momentum_score_0_10": float(best.momentum_score),
        "volatility_score_0_10": float(best.volatility_score_0_10),
        "risk_reward_score_0_10": float(best.risk_reward_score),
        "liquidity_score_0_10": float(best.liquidity_score),
        "news_score_0_10": float(best.news_score),
        "final_score_0_10": float(best.final_score_0_10 if best.final_score_0_10 is not None else best.ai_score),
        "pick_rationale": list(best.pick_rationale or []),
        # Pre-mover intelligence (v4)
        "premover_score_0_10": float(best.premover_score_0_10),
        "overextended_penalty": float(best.overextended_penalty),
        "signal_count": int(_intraday_signal_count),
        "trade_decision": str(best.trade_decision or "LOW_CONVICTION"),
        "no_trade_reason": str(_no_trade_reason),
        "is_trade": bool(best.is_trade),
        "earnings_safe": bool(_earnings_safe),
        "days_to_earnings": _days_to_earnings,
        "trailing_stop_plan": [
            {
                "trigger_pct": 8.0,
                "new_stop_pct_from_entry": 0.0,
                "action": "Move stop to breakeven (entry price)",
            },
            {
                "trigger_pct": 12.0,
                "new_stop_pct_from_entry": 6.0,
                "action": "Lock in 6% gain",
            },
            {
                "trigger_pct": 20.0,
                "new_stop_pct_from_entry": 15.0,
                "action": "Lock in 15% gain",
            },
        ],
        # Edge detection (v5)
        "edge_signals": list(best.edge_signals or []),
        "edge_score_0_10": float(best.edge_score_0_10 or 0.0),
        # Neural network win probability (None until model is trained)
        "nn_win_prob": best.nn_win_prob,
        # Elite trading intelligence fields (v3)
        "market_regime": str(best.market_regime or regime_str),
        "trade_quality": str(best.trade_quality or "B"),
        "position_size_pct": float(best.position_size_pct or 3.0),
        "risk_level": str(best.risk_level or "medium"),
        "news_summary": str(best.news_summary or ""),
        "key_drivers": list(best.key_drivers or []),
        "event_type": str(best.event_type or "unknown"),
        "_timing_inputs": {
            "atr14": (_safe_f(best.atr14) if best.atr14 is not None else None),
            "volatility_score": (
                float(_clamp(float(best.atr_pct) * 10.0, 0.0, 100.0))
                if best.atr_pct is not None
                else None
            ),
            "trend_strength": float(_clamp(float(best.technical_score) * 10.0, 0.0, 100.0)),
        },
        "watchlist_candidates": _watchlist_candidates,
    }

    try:
        spy_bars = daily_all.get("SPY") if isinstance(daily_all, dict) else []
        if not isinstance(spy_bars, list):
            spy_bars = []
        intraday_bars: List[Dict[str, Any]] = []
        try:
            intraday_map = await asyncio.wait_for(
                asyncio.to_thread(get_bars_batch, [str(best.symbol or "").strip().upper()], "5Min", 300),
                timeout=2.0,
            )
            if isinstance(intraday_map, dict):
                b0 = intraday_map.get(str(best.symbol or "").strip().upper())
                if isinstance(b0, list):
                    intraday_bars = list(b0)
        except Exception:
            intraday_bars = []
        out["_pre_mover_input"] = {
            "symbol": str(best.symbol or "").strip().upper(),
            "snapshot": dict(best.snapshot) if isinstance(best.snapshot, dict) else {},
            "bars": list(best.daily_bars or []),
            "spy_bars": list(spy_bars or []),
            "intraday_bars": list(intraday_bars or []),
        }
    except Exception:
        out["_pre_mover_input"] = {}

    try:
        log.info({"elapsed_total": float(round(time.time() - start, 3)), "symbols_scored": int(scored_count)})
    except Exception:
        pass

    # Missed-entry check: fetch live price and compare to calculated entry.
    # Runs only when a real entry exists and the pick is not already a NO_TRADE.
    if out.get("trade_decision") != "NO_TRADE":
        try:
            _me_sym   = str(out.get("symbol") or "").strip().upper()
            _me_entry = _safe_f((out.get("trade_plan") or {}).get("entry"))
            if _me_sym and _me_entry and float(_me_entry) > 0:
                _live_snap = await asyncio.wait_for(
                    asyncio.to_thread(get_snapshot_normalized, _me_sym),
                    timeout=2.5,
                )
                _live_px = _safe_f((_live_snap or {}).get("last_price"))
                if _live_px is not None and float(_live_px) > float(_me_entry) * 1.015:
                    _reason = (
                        f"Entry already exceeded — current price ${float(_live_px):.2f} "
                        f"is more than 1.5% above calculated entry ${float(_me_entry):.2f}"
                    )
                    out["trade_decision"] = "MISSED_ENTRY"
                    out["no_trade_reason"] = _reason
                    out["is_trade"]        = False
                    out["badge"]           = "MISSED"
                    _gap_pct = (float(_live_px) - float(_me_entry)) / float(_me_entry) * 100.0
                    log.info(
                        f"best_pick_v2: MISSED_ENTRY {_me_sym} "
                        f"entry={_me_entry:.2f} current={_live_px:.2f} gap={_gap_pct:+.2f}%"
                    )
                    try:
                        _audit = (
                            f"{datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')} "
                            f"MISSED_ENTRY symbol={_me_sym} "
                            f"calculated_entry={_me_entry:.2f} "
                            f"current_price={_live_px:.2f} "
                            f"gap_pct={_gap_pct:+.2f}%\n"
                        )
                        with open("scan_log.txt", "a") as _slf:
                            _slf.write(_audit)
                    except Exception:
                        pass
        except Exception:
            pass

    # ── Evolution engine: apply learned weights + log pick ────────────────────
    try:
        from learning import (
            get_weights, get_kelly_position_size, get_fingerprint_similarity,
            get_calibration_multiplier, get_sector_bias, get_macro_conviction_penalty,
            get_dynamic_thresholds, multi_agent_score, log_pick as _log_pick,
            compute_second_deriv_momentum, compute_rsi_divergence,
            compute_consolidation_tightness, compute_gap_fill_probability,
        )

        _regime_str = str(out.get("market_regime") or "ALL").upper()
        _best_bars  = list(best.daily_bars or [])

        # Compute additional evolved signals
        _second_deriv = compute_second_deriv_momentum(_best_bars)
        _rsi_div      = compute_rsi_divergence(_best_bars, rsi_now=None)
        _consolidation = compute_consolidation_tightness(_best_bars)
        _gap_fill     = compute_gap_fill_probability(_best_bars)

        # Build full signal dict for this pick
        _signals = {
            "momentum":               float(best.momentum_score or 5),
            "volume":                 float(best.liquidity_score or 5),
            "technical":              float(best.technical_score or 5),
            "catalyst":               float(best.catalyst_score or 5),
            "sentiment":              float(best.sentiment_score or 5),
            "risk_structure":         float(best.risk_structure_score or 5),
            "upside":                 float(best.upside_score or 5),
            "premover":               float(best.premover_score_0_10 or 5),
            "edge_score":             float(best.edge_score_0_10 or 0),
            "news_score":             float(best.news_score or 5),
            "liquidity":              float(best.liquidity_score or 5),
            "volatility":             float(best.volatility_score_0_10 or 5),
            "second_deriv_momentum":  _second_deriv,
            "rsi_divergence":         _rsi_div,
            "consolidation_tightness": _consolidation,
            "gap_fill_prob":          _gap_fill,
        }

        # Apply learned weights to final score
        _weights        = get_weights(_regime_str)
        _weighted_score = sum(
            _signals.get(k, 5.0) * _weights.get(k, 1.0)
            for k in _signals
        ) / max(len(_signals), 1)
        # Blend learned score with original (60/40) — prevents wild swings early on
        _orig_score = float(out.get("ai_score_0_10") or best.ai_score or 5.0)
        _evolved_score = 0.6 * _orig_score + 0.4 * _weighted_score
        _evolved_score = max(0.0, min(10.0, _evolved_score))

        # Calibration multiplier (score bucket reliability)
        _cal_mult = get_calibration_multiplier(_evolved_score)
        _evolved_score = max(0.0, min(10.0, _evolved_score * _cal_mult))

        # Macro event penalty
        _macro_pen = get_macro_conviction_penalty()
        _evolved_score = max(0.0, _evolved_score - _macro_pen)

        # Winner fingerprint similarity boost/penalty
        _fp_sim = get_fingerprint_similarity(_signals)
        _fp_boost = (_fp_sim - 0.5) * 1.0  # -0.5 to +0.5 on 0-10 scale
        _evolved_score = max(0.0, min(10.0, _evolved_score + _fp_boost))

        # Multi-agent debate
        _debate = multi_agent_score(_signals)
        # If agents strongly disagree, slightly reduce confidence
        if _debate["disagreement"] > 2.0:
            _evolved_score = max(0.0, _evolved_score - 0.3)

        # Dynamic conviction thresholds
        _thresholds = get_dynamic_thresholds()

        # Kelly position sizing (overrides fixed tiers)
        _kelly_size = get_kelly_position_size(_regime_str)

        # Update out dict with evolved values
        out["ai_score_evolved"]       = round(_evolved_score, 3)
        out["ai_score_0_10_evolved"]  = round(_evolved_score, 3)
        out["fingerprint_similarity"] = round(_fp_sim, 3)
        out["multi_agent_debate"]     = _debate
        out["conviction_thresholds"]  = _thresholds
        out["kelly_position_size"]    = round(_kelly_size, 1)
        out["macro_penalty"]          = _macro_pen
        out["signal_weights_applied"] = True

        # Update conviction label using dynamic thresholds + evolved score
        _s100 = _evolved_score * 10
        if _s100 < _thresholds["low"]:       _evo_conv = "LOW"
        elif _s100 < _thresholds["moderate"]: _evo_conv = "MODERATE"
        elif _s100 < _thresholds["solid"]:    _evo_conv = "SOLID"
        elif _s100 < _thresholds["high"]:     _evo_conv = "HIGH"
        else:                                  _evo_conv = "VERY HIGH"
        out["conviction"] = _evo_conv

        # Log this pick for future learning (non-blocking)
        import threading
        _sym_to_log = str(out.get("symbol") or "")
        _score_to_log = _evolved_score
        threading.Thread(
            target=_log_pick,
            args=(_sym_to_log, _signals, _regime_str, "", _score_to_log),
            daemon=True,
        ).start()

    except Exception as _learn_err:
        log.warning(f"best_pick_v2: evolution engine error (non-fatal): {_learn_err}")
    # ──────────────────────────────────────────────────────────────────────────

    result = out

    # --- normalize output ---
    tp = result.get("trade_plan", {})
    _entry   = tp.get("entry")
    _stop    = tp.get("stop")
    _targets = tp.get("targets") or []
    _t1      = _targets[0] if len(_targets) > 0 else None
    _t2      = _targets[1] if len(_targets) > 1 else None
    _t3      = _targets[2] if len(_targets) > 2 else None

    # Risk:Reward ratio  (reward to first target / risk to stop)
    _rr_ratio = None
    try:
        _risk   = abs(float(_entry) - float(_stop))
        _reward = abs(float(_t1)    - float(_entry))
        if _risk > 0:
            _rr_ratio = round(_reward / _risk, 2)
    except Exception:
        pass

    # Stop-loss % from entry
    _stop_pct = None
    try:
        _stop_pct = round(abs(float(_entry) - float(_stop)) / float(_entry) * 100, 2)
    except Exception:
        pass

    # T1 / T2 / T3 gain %
    def _gain_pct(tgt):
        try:
            return round((float(tgt) - float(_entry)) / float(_entry) * 100, 2)
        except Exception:
            return None

    # Time horizon: rough estimate from regime + conviction
    _td  = result.get("trade_decision", "")
    _reg = str(result.get("market_regime") or "").upper()
    if "HIGH" in _td:
        _horizon_days = 3 if "BULL" in _reg else 5
        _horizon_label = "3–5 days"
    elif "LOW" in _td:
        _horizon_days = 5
        _horizon_label = "5–7 days"
    else:
        _horizon_days = 7
        _horizon_label = "7–10 days"

    # Entry method based on regime
    _entry_note = (
        "Buy on open or limit at entry price" if "BULL" in _reg
        else "Limit order at entry — do not chase" if "CHOP" in _reg
        else "Limit order preferred"
    )

    normalized = {
        "symbol": result.get("symbol"),
        "entry": _entry,
        "stop": _stop,
        "take_profit": _t1,
        "target_1": _t1,
        "target_2": _t2,
        "target_3": _t3,
        "target_1_gain_pct": _gain_pct(_t1),
        "target_2_gain_pct": _gain_pct(_t2),
        "target_3_gain_pct": _gain_pct(_t3),
        "stop_loss_pct": _stop_pct,
        "risk_reward_ratio": _rr_ratio,
        "time_horizon_days": _horizon_days,
        "time_horizon_label": _horizon_label,
        "entry_note": _entry_note,
        "confidence": result.get("confidence_0_10"),
        "final_score_0_10": result.get("final_score_0_10"),
    }

    # merge original + normalized
    return {
        **result,
        **normalized,
    }
