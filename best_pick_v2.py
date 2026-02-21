import asyncio
import math
import logging
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

from data_fetcher import get_bars_batch, get_snapshots_batch, get_snapshot_normalized


log = logging.getLogger("stackiq")


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


def _safe_f(v: Any) -> Optional[float]:
    try:
        x = float(v)
    except Exception:
        return None
    if not math.isfinite(x):
        return None
    return float(x)


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
    tail = bars[-30:] if len(bars) >= 30 else bars[-25:]
    vols: List[float] = []
    dollars: List[float] = []
    for b in tail:
        c = _safe_f(b.get("c"))
        v = _safe_f(b.get("v"))
        if c is None or v is None or c <= 0 or v < 0:
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

    news_obj: Optional[Dict[str, Any]] = None
    catalysts: List[str] = None
    risk_flags: List[str] = None


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

    # Liquidity gating is handled upstream (universe pre-ranking). Do NOT eliminate candidates
    # here; scoring will handle execution penalties.
    _ = avg_vol_30d
    _ = avg_dollar_vol_30d

    # Spread data can be unavailable depending on feed/entitlements.
    # Do NOT fail the candidate in that case; treat it as an execution penalty later.
    if spread_pct_now is not None and float(spread_pct_now) > 0.35:
        flags.append("wide_spread")

    return True, flags


def _compute_placeholder_confidence_0_100(*, tech: float, risk: float, exec_score: float, catalyst: float) -> int:
    base = 50.0
    base += (float(tech) - 5.0) * 4.0
    base += (float(risk) - 5.0) * 4.0
    base += (float(exec_score) - 5.0) * 3.0
    base += max(0.0, float(catalyst) - 5.0) * 2.0
    return int(round(_clamp(base, 5.0, 85.0)))


def _high_grade(ai_score: float, execution_score: float, risk_score: float) -> bool:
    return bool((ai_score >= 6.2) and (execution_score >= 6.0) and (risk_score >= 5.5))


def _trade_plan_from_levels(*, direction: str, last_price: Optional[float], stop: Optional[float], atr14: Optional[float]) -> Dict[str, Any]:
    entry = last_price
    if entry is not None:
        entry = float(round(entry, 4))
    if stop is not None:
        stop = float(round(stop, 4))

    targets: List[Optional[float]] = [None, None, None]
    try:
        if entry is not None and stop is not None and entry > 0:
            r = abs(float(entry) - float(stop))
            if r > 0:
                targets = [
                    float(round(entry + (1.0 * r), 4)),
                    float(round(entry + (1.5 * r), 4)),
                    float(round(entry + (2.0 * r), 4)),
                ]
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
    max_seconds: float = 10.0,
    news_top_k: int = 25,
) -> Dict[str, Any]:
    start = time.time()
    t0 = start

    log_llm_enabled = bool(allow_llm_news)

    MIN_SYMBOLS_BEFORE_TIMEOUT = 300
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

    try:
        log.info(f"best_pick_v2: universe_size={len(syms)}")
    except Exception:
        pass

    if not syms:
        return {
            "symbol": "AAPL",
            "type": "STOCK",
            "ai_score_0_10": 1.0,
            "execution_score_0_10": 1.0,
            "confidence_0_100": 5,
            "confidence_definition": "P(+1.5R before -1R in 7D)",
            "high_grade": False,
            "low_conviction_note": "Low-conviction environment — defensive positioning preferred.",
            "log_llm_enabled": bool(log_llm_enabled),
            "trade_plan": _trade_plan_from_levels(direction="long", last_price=None, stop=None, atr14=None),
            "catalysts": [],
            "risk_flags": ["empty_universe"],
            "pillar_scores_0_10": {"technical": 1.0, "catalyst": 1.0, "sentiment": 1.0, "risk_structure": 1.0, "upside": 1.0},
        }

    # Fetch in chunks to avoid request limits.
    chunk_size = 200
    sem = asyncio.Semaphore(3)

    async def _fetch_chunk(chunk: List[str]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        async with sem:
            snaps_task = asyncio.to_thread(get_snapshots_batch, chunk)
            daily_task = asyncio.to_thread(get_bars_batch, chunk, "1Day", 30)
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
        if (time.time() - t0) > float(max_seconds):
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

    # First pass: compute raw features + apply hard universe gates
    for sym in syms:
        if (time.time() - t0) > float(max_seconds) and int(scored_count) >= int(MIN_SYMBOLS_BEFORE_TIMEOUT):
            timeout_reached = True
            break

        snapshot = snaps_all.get(sym)
        if not isinstance(snapshot, dict):
            snapshot = {}

        daily_bars = daily_all.get(sym)
        if not isinstance(daily_bars, list):
            daily_bars = []

        last_px = _last_price_from_snapshot(snapshot)
        if last_px is None and daily_bars:
            try:
                last_px = _safe_f((daily_bars[-1] or {}).get("c"))
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

        avg_vol_30d, avg_dollar_vol_30d = (None, None)
        try:
            if len(daily_bars) >= 25:
                avg_vol_30d, avg_dollar_vol_30d = _dollar_volume_30d(daily_bars)
        except Exception:
            avg_vol_30d, avg_dollar_vol_30d = (None, None)

        # HARD universe gates BEFORE scoring: penny stocks and illiquid names never score/rank.
        try:
            if float(last_px) < 5.0:
                skipped_count += 1
                continue
        except Exception:
            skipped_count += 1
            continue

        try:
            if avg_dollar_vol_30d is None or float(avg_dollar_vol_30d) < 10_000_000.0:
                skipped_count += 1
                continue
        except Exception:
            skipped_count += 1
            continue

        gate_flags: List[str] = []
        try:
            if spread_pct_now is not None and float(spread_pct_now) > 0.35:
                gate_flags.append("wide_spread")
        except Exception:
            pass

        if last_px is not None and float(last_px) >= 5.0:
            stage_price += 1
        if avg_vol_30d is not None and float(avg_vol_30d) >= 300_000.0:
            stage_vol += 1
        if avg_dollar_vol_30d is not None and float(avg_dollar_vol_30d) >= 10_000_000.0:
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

        MIN_BARS_REQUIRED = 25

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
        # stop: below swing low or SMA20 (whichever lower), small buffer
        try:
            candidates_stop = [x for x in [swing_low_10, sma20] if x is not None and float(x) > 0]
            if candidates_stop:
                stop = float(min(candidates_stop)) * 0.995
        except Exception:
            stop = None

        # Stop fallback: default 3% baseline
        if stop is None and last_px is not None and float(last_px) > 0:
            try:
                stop = float(last_px) * 0.97
            except Exception:
                stop = None

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
            )
        )
        scored_count += 1

    try:
        log.info(
            "best_pick_v2: gates "
            f"after_price={stage_price} "
            f"after_volume={stage_vol} "
            f"after_dollar_volume={stage_dollar} "
            f"after_spread={stage_spread} "
            f"final_candidates={len(cands)}"
        )
    except Exception:
        pass

    if not cands:
        snap_norm = None
        try:
            snap_norm = await asyncio.to_thread(get_snapshot_normalized, "AAPL")
        except Exception:
            snap_norm = None
        last = _safe_f((snap_norm or {}).get("last_price")) if isinstance(snap_norm, dict) else None
        return {
            "symbol": "AAPL",
            "type": "STOCK",
            "ai_score_0_10": 1.0,
            "execution_score_0_10": 1.0,
            "confidence_0_100": 10 if last is not None else 5,
            "confidence_definition": "P(+1.5R before -1R in 7D)",
            "high_grade": False,
            "low_conviction": True,
            "low_conviction_note": "Low-conviction environment — defensive positioning preferred.",
            "log_llm_enabled": bool(log_llm_enabled),
            "scored_count": int(scored_count),
            "skipped_count": int(skipped_count),
            "candidates_scored": 0,
            "candidates_passing_threshold": 0,
            "candidates_skipped_data": int(candidates_skipped_data),
            "trade_plan": _trade_plan_from_levels(direction="long", last_price=last, stop=None, atr14=None),
            "catalysts": [],
            "risk_flags": ["no_candidates_after_gates"],
            "pillar_scores_0_10": {"technical": 1.0, "catalyst": 1.0, "sentiment": 1.0, "risk_structure": 1.0, "upside": 1.0},
        }

    # Cross-sectional normalization (percentile ranks)
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
            if (time.time() - t0) > float(max_seconds) and int(scored_count) >= int(MIN_SYMBOLS_BEFORE_TIMEOUT):
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
        c.ai_score = float(round(_clamp(ai, 1.0, 10.0), 1))

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

    # Rank: AI then Execution
    def _rank_key_final(x: _Candidate) -> Tuple[float, float, int]:
        return (float(x.ai_score), float(x.execution_score), 0)

    cands.sort(key=_rank_key_final, reverse=True)

    # Final validation: never allow a disqualified symbol to win.
    def _passes_final_validation(cand: _Candidate) -> bool:
        try:
            if cand.last_price is None or float(cand.last_price) < 5.0:
                return False
        except Exception:
            return False
        try:
            if cand.avg_dollar_vol_30d is None or float(cand.avg_dollar_vol_30d) < 10_000_000.0:
                return False
        except Exception:
            return False
        return True

    best = None

    # Prefer stocks first.
    for cand in cands:
        try:
            if str(cand.type or "").strip().upper() == "ETF":
                continue
        except Exception:
            pass
        if not _passes_final_validation(cand):
            continue
        best = cand
        break

    # Fallback to ETFs only if no stock qualifies.
    if best is None:
        for cand in cands:
            if not _passes_final_validation(cand):
                continue
            best = cand
            break

    if best is None:
        snap_norm = None
        try:
            snap_norm = await asyncio.to_thread(get_snapshot_normalized, "AAPL")
        except Exception:
            snap_norm = None
        last = _safe_f((snap_norm or {}).get("last_price")) if isinstance(snap_norm, dict) else None
        return {
            "symbol": "AAPL",
            "type": "STOCK",
            "ai_score_0_10": 1.0,
            "execution_score_0_10": 1.0,
            "confidence_0_100": 10 if last is not None else 5,
            "confidence_definition": "P(+1.5R before -1R in 7D)",
            "high_grade": False,
            "low_conviction": True,
            "low_conviction_note": "Low-conviction environment — defensive positioning preferred.",
            "log_llm_enabled": bool(log_llm_enabled),
            "scored_count": int(scored_count),
            "skipped_count": int(skipped_count),
            "candidates_scored": 0,
            "candidates_passing_threshold": 0,
            "candidates_skipped_data": int(candidates_skipped_data),
            "trade_plan": _trade_plan_from_levels(direction="long", last_price=last, stop=None, atr14=None),
            "catalysts": [],
            "risk_flags": ["no_candidates_after_gates"],
            "pillar_scores_0_10": {"technical": 1.0, "catalyst": 1.0, "sentiment": 1.0, "risk_structure": 1.0, "upside": 1.0},
        }

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

    conf = _compute_placeholder_confidence_0_100(
        tech=best.technical_score,
        risk=best.risk_structure_score,
        exec_score=best.execution_score,
        catalyst=best.catalyst_score,
    )

    direction = "long"
    try:
        if best.last_price is not None and best.sma20 is not None and float(best.last_price) < float(best.sma20):
            direction = "short"
    except Exception:
        direction = "long"

    trade_plan = _trade_plan_from_levels(direction=direction, last_price=best.last_price, stop=best.stop, atr14=best.atr14)

    if int(candidates_passing) <= 0:
        out0 = {
            "status": "NO_HIGH_QUALITY_SETUP",
            "market_mode": "LOW_CONVICTION",
            "scanned": int(total_scanned),
            "message": "No A-grade setups found. Defensive positioning recommended.",
            "log_llm_enabled": bool(log_llm_enabled),
            "total_scanned": int(total_scanned),
            "bars_available": int(bars_available),
            "scored_count": int(scored_count),
            "skipped_count": int(skipped_count),
            "candidates_scored": int(candidates_scored),
            "candidates_passing_threshold": int(candidates_passing),
            "candidates_skipped_data": int(candidates_skipped_data),
        }

        try:
            log.info({"elapsed_total": float(round(time.time() - start, 3)), "symbols_scored": int(scored_count)})
        except Exception:
            pass

        return out0

    out = {
        "symbol": best.symbol,
        "type": best.type,
        "ai_score_0_10": float(best.ai_score),
        "execution_score_0_10": float(best.execution_score),
        "confidence_0_100": int(conf),
        "confidence_definition": "P(+1.5R before -1R in 7D)",
        "high_grade": bool(high_grade),
        "low_conviction": bool(not high_grade),
        "low_conviction_note": str(low_note),
        "log_llm_enabled": bool(log_llm_enabled),
        "total_scanned": int(total_scanned),
        "bars_available": int(bars_available),
        "scored_count": int(scored_count),
        "skipped_count": int(skipped_count),
        "candidates_scored": int(candidates_scored),
        "candidates_passing_threshold": int(candidates_passing),
        "candidates_skipped_data": int(candidates_skipped_data),
        "trade_plan": trade_plan,
        "catalysts": list(best.catalysts or []),
        "risk_flags": list(best.risk_flags or []),
        "pillar_scores_0_10": {
            "technical": float(best.technical_score),
            "catalyst": float(best.catalyst_score),
            "sentiment": float(best.sentiment_score),
            "risk_structure": float(best.risk_structure_score),
            "upside": float(best.upside_score),
        },
    }

    try:
        log.info({"elapsed_total": float(round(time.time() - start, 3)), "symbols_scored": int(scored_count)})
    except Exception:
        pass

    return out
