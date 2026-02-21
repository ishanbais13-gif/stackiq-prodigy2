import asyncio
import json
import time
from typing import Any, Dict, List, Optional

from data_fetcher import get_bars, get_snapshot
from data_fetcher import get_bars_batch, get_snapshots_batch
from data_fetcher import get_snapshot_normalized
from indicator_engine import calculate_indicators
from scoring_engine import score_composite_0_100, score_execution_0_100
from execution_engine import build_execution_plan
from llm_client import call_llm_text, llm_available


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


def _news_sentiment_from_snapshot(snap: Dict[str, Any]) -> Dict[str, Any]:
    try:
        bar = snap.get("dailyBar") if isinstance(snap.get("dailyBar"), dict) else {}
        prev = snap.get("prevDailyBar") if isinstance(snap.get("prevDailyBar"), dict) else {}
        lt = snap.get("latestTrade") if isinstance(snap.get("latestTrade"), dict) else {}
        px = lt.get("p") if lt.get("p") is not None else bar.get("c")
        px = float(px) if px is not None else None
        pc = float(prev.get("c")) if prev.get("c") is not None else None
        if px is None or pc is None or pc <= 0:
            return {"direction": "NEUTRAL", "summary": "No sentiment signal.", "score_100": 50}
        chg = (px - pc) / pc * 100.0
        if chg >= 1.0:
            return {"direction": "BULLISH", "summary": "Bullish tape proxy.", "score_100": _clamp_0_100(55.0 + min(25.0, chg * 5.0))}
        if chg <= -1.0:
            return {"direction": "BEARISH", "summary": "Bearish tape proxy.", "score_100": _clamp_0_100(45.0 - min(25.0, abs(chg) * 5.0))}
        return {"direction": "NEUTRAL", "summary": "Neutral tape proxy.", "score_100": 50}
    except Exception:
        return {"direction": "NEUTRAL", "summary": "No sentiment signal.", "score_100": 50}


def _safe_symbol_list(universe: List[str], cap: int = 50) -> List[str]:
    out: List[str] = []
    seen = set()
    for s in (universe or []):
        sym = str(s or "").strip().upper()
        if not sym:
            continue
        if sym in seen:
            continue
        seen.add(sym)
        out.append(sym)
        if len(out) >= int(cap):
            break
    return out


def _passes_fast_filter(ind: Dict[str, Any]) -> bool:
    # NEW SAFE FILTER
    try:
        if float(ind.get("momentum") or 0.0) < 40.0:
            return False
    except Exception:
        return False
    try:
        if float(ind.get("trend") or 0.0) < 50.0:
            return False
    except Exception:
        return False
    try:
        if float(ind.get("liquidity") or 0.0) < 45.0:
            return False
    except Exception:
        return False
    return True


def _build_why_blocks(ind: Dict[str, Any], ns: Dict[str, Any]) -> Dict[str, List[str]]:
    mom = int(_clamp_0_100(ind.get("momentum")))
    tr = int(_clamp_0_100(ind.get("trend")))
    vol = int(_clamp_0_100(ind.get("volatility")))
    liq = int(_clamp_0_100(ind.get("liquidity")))

    try:
        d = str(ns.get("direction") or "NEUTRAL").strip().upper()
    except Exception:
        d = "NEUTRAL"

    why = [
        f"Momentum {mom}/100 and trend {tr}/100 based on RSI/MACD + EMA alignment.",
        f"Volatility {vol}/100 with liquidity {liq}/100 supporting execution.",
        f"News bias: {d}.",
    ]

    confirms = [
        "Break above prior day high with expanding volume.",
    ]

    breaks = [
        "Close below planned stop invalidates setup.",
    ]

    return {"why": why[:3], "what_confirms": confirms[:1], "what_breaks": breaks[:1]}


def _llm_enrich_once(*, symbol: str, indicators: Dict[str, Any], news_sentiment: Dict[str, Any], execution_plan: Dict[str, Any], timeout_fallback: Dict[str, List[str]]) -> Dict[str, List[str]]:
    if not llm_available():
        return timeout_fallback
    ctx = {
        "symbol": symbol,
        "indicators": indicators,
        "news_sentiment": news_sentiment,
        "execution_plan": execution_plan,
        "schema": {"why": "list[str](3)", "what_confirms": "list[str](1)", "what_breaks": "list[str](1)"},
    }
    system = "Return ONLY valid JSON with keys: why (array of 3 short bullets), what_confirms (array of 1 short bullet), what_breaks (array of 1 short bullet). No extra keys."
    user = json.dumps(ctx)
    try:
        raw = call_llm_text(system=system, user=user)
    except Exception:
        return timeout_fallback
    try:
        obj = json.loads(raw)
    except Exception:
        return timeout_fallback
    if not isinstance(obj, dict):
        return timeout_fallback
    out = {
        "why": obj.get("why") if isinstance(obj.get("why"), list) else timeout_fallback["why"],
        "what_confirms": obj.get("what_confirms") if isinstance(obj.get("what_confirms"), list) else timeout_fallback["what_confirms"],
        "what_breaks": obj.get("what_breaks") if isinstance(obj.get("what_breaks"), list) else timeout_fallback["what_breaks"],
    }
    out["why"] = [str(x)[:180] for x in out["why"] if isinstance(x, str) and x.strip()][:3]
    out["what_confirms"] = [str(x)[:180] for x in out["what_confirms"] if isinstance(x, str) and x.strip()][:1]
    out["what_breaks"] = [str(x)[:180] for x in out["what_breaks"] if isinstance(x, str) and x.strip()][:1]
    while len(out["why"]) < 3:
        out["why"].append(timeout_fallback["why"][0])
    if not out["what_confirms"]:
        out["what_confirms"] = timeout_fallback["what_confirms"]
    if not out["what_breaks"]:
        out["what_breaks"] = timeout_fallback["what_breaks"]
    return out


def _candle_payload_from_fetch(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    c = payload.get("candles") if isinstance(payload, dict) else None
    return c if isinstance(c, list) else []


def _fetch_symbol_data(symbol: str) -> Optional[Dict[str, Any]]:
    snap = None
    try:
        snap = get_snapshot(symbol)
    except Exception:
        snap = None

    bars = get_bars(symbol, timeframe="1Day", limit=100)
    candles = _candle_payload_from_fetch(bars)

    # Retry if we got too few bars (2 retries max)
    if len(candles) < 50:
        bars = get_bars(symbol, timeframe="1Day", limit=100)
        candles = _candle_payload_from_fetch(bars)
    if len(candles) < 50:
        return None

    if not isinstance(snap, dict):
        try:
            last_close = float(candles[-1].get("c")) if candles[-1].get("c") is not None else None
        except Exception:
            last_close = None
        snap = {
            "dailyBar": {"c": last_close},
            "prevDailyBar": {},
            "latestQuote": {},
            "latestTrade": {"p": last_close},
        }

    return {"symbol": symbol, "snapshot": snap, "candles": candles}


def _direction_from_indicators(indicators: Dict[str, Any]) -> str:
    try:
        mom = float((indicators or {}).get("momentum") or 0.0)
        tr = float((indicators or {}).get("trend") or 0.0)
    except Exception:
        mom, tr = 0.0, 0.0
    bias = (0.55 * tr) + (0.45 * mom)
    if bias >= 60.0:
        return "bullish"
    if bias <= 40.0:
        return "bearish"
    return "neutral"


def pick_best_sync(*, universe: List[str], tz: Optional[str] = None, allow_llm: bool = True, max_seconds: float = 8.0) -> Dict[str, Any]:
    return asyncio.run(pick_best_async(universe=universe, tz=tz, allow_llm=allow_llm, max_seconds=max_seconds))


def _confidence_0_100(ai_score: float, execution_score: float) -> float:
    return _clamp_0_100((0.60 * float(ai_score)) + (0.40 * float(execution_score)))


def _confidence_adjusted_0_100(*, ai_score: float, execution_score: float, data_completeness_0_1: float, news_sentiment_0_100: float) -> float:
    base = _confidence_0_100(float(ai_score), float(execution_score))
    try:
        dc = float(data_completeness_0_1)
    except Exception:
        dc = 0.0
    if dc < 0.0:
        dc = 0.0
    if dc > 1.0:
        dc = 1.0
    # Keep some non-zero confidence when we have at least snapshot price.
    dc = 0.50 + (0.50 * dc)

    try:
        ns = float(news_sentiment_0_100)
    except Exception:
        ns = 50.0
    if ns < 0.0:
        ns = 0.0
    if ns > 100.0:
        ns = 100.0
    # Small nudge: +-5 points around neutral.
    sentiment_adj = (ns - 50.0) * 0.10
    return _clamp_0_100((base * dc) + sentiment_adj)


def _confidence_from_factors_0_100(
    *,
    indicators: Dict[str, Any],
    candles: List[Dict[str, Any]],
    snapshot: Dict[str, Any],
    news_sentiment: Dict[str, Any],
) -> float:
    # Required confidence drivers:
    # - Volume confirmation
    # - News sentiment alignment
    # - Trend strength
    # - Breakout proximity
    ind = indicators if isinstance(indicators, dict) else {}
    snap = snapshot if isinstance(snapshot, dict) else {}

    # Trend strength: use trend score directly.
    trend_strength = _clamp_0_100(ind.get("trend"))

    # Volume confirmation: compare snapshot daily volume to 20-day average volume.
    vol_conf = 50.0
    try:
        bar = snap.get("dailyBar") if isinstance(snap.get("dailyBar"), dict) else {}
        v_today = bar.get("v")
        v_today_f = float(v_today) if v_today is not None else None
    except Exception:
        v_today_f = None

    avg20 = None
    try:
        vols = [float(b.get("v") or 0.0) for b in (candles[-21:-1] if isinstance(candles, list) else []) if isinstance(b, dict)]
        vols = [v for v in vols if v and v > 0]
        if vols:
            avg20 = float(sum(vols)) / float(len(vols))
    except Exception:
        avg20 = None

    try:
        if v_today_f is not None and avg20 is not None and avg20 > 0:
            rvol = float(v_today_f) / float(avg20)
            # 1.0 => 50, 2.0+ => 100, 0.5 => 25
            vol_conf = _clamp_0_100(50.0 + (min(2.0, max(0.0, rvol)) - 1.0) * 50.0)
    except Exception:
        vol_conf = 50.0

    # Breakout proximity: distance to recent high, closer is higher confidence.
    breakout = 50.0
    last = None
    try:
        lt = snap.get("latestTrade") if isinstance(snap.get("latestTrade"), dict) else {}
        if lt.get("p") is not None:
            last = float(lt.get("p"))
    except Exception:
        last = None
    if last is None:
        try:
            bar = snap.get("dailyBar") if isinstance(snap.get("dailyBar"), dict) else {}
            if bar.get("c") is not None:
                last = float(bar.get("c"))
        except Exception:
            last = None

    recent_high = None
    try:
        hs = [float(b.get("h")) for b in (candles[-21:] if isinstance(candles, list) else []) if isinstance(b, dict) and b.get("h") is not None]
        if hs:
            recent_high = float(max(hs))
    except Exception:
        recent_high = None

    try:
        if last is not None and recent_high is not None and float(last) > 0 and float(recent_high) > 0:
            dist = abs(float(recent_high) - float(last)) / float(last)
            # 0% away => 100, 2% away => 70, 5% away => 40, 10%+ => 20
            breakout = _clamp_0_100(100.0 - min(80.0, dist * 1200.0))
    except Exception:
        breakout = 50.0

    # News alignment: sentiment direction vs technical direction.
    news_align = 50.0
    try:
        d_news = str((news_sentiment or {}).get("direction") or "NEUTRAL").strip().upper()
    except Exception:
        d_news = "NEUTRAL"
    try:
        d_tech = _direction_from_indicators(ind)
        d_tech_u = str(d_tech or "neutral").strip().upper()
    except Exception:
        d_tech_u = "NEUTRAL"
    try:
        if d_news == "NEUTRAL":
            news_align = 50.0
        elif d_news == d_tech_u:
            news_align = 80.0
        else:
            news_align = 30.0
    except Exception:
        news_align = 50.0

    # Final blend (equal-weighted, then lightly nudged by trend strength).
    base = (0.25 * vol_conf) + (0.25 * news_align) + (0.25 * trend_strength) + (0.25 * breakout)
    return _clamp_0_100(base)


def _indicators_from_snapshot_fallback(*, snap: Dict[str, Any]) -> Dict[str, Any]:
    # Minimal snapshot-only scoring so Best Pick never breaks under degraded conditions.
    try:
        prev = snap.get("prevDailyBar") if isinstance(snap, dict) else None
        prev_close = float(prev.get("c")) if isinstance(prev, dict) and prev.get("c") is not None else None
    except Exception:
        prev_close = None
    last = None
    try:
        lt = snap.get("latestTrade") if isinstance(snap, dict) else None
        if isinstance(lt, dict) and lt.get("p") is not None:
            last = float(lt.get("p"))
    except Exception:
        last = None
    if last is None:
        try:
            bar = snap.get("dailyBar") if isinstance(snap, dict) else None
            if isinstance(bar, dict) and bar.get("c") is not None:
                last = float(bar.get("c"))
        except Exception:
            last = None

    chg_pct = 0.0
    try:
        if last is not None and prev_close is not None and float(prev_close) > 0.0:
            chg_pct = ((float(last) - float(prev_close)) / float(prev_close)) * 100.0
    except Exception:
        chg_pct = 0.0

    # Heuristic mapping: price change becomes momentum/trend proxy; everything else neutral.
    mom = _clamp_0_100(50.0 + (chg_pct * 3.0))
    tr = _clamp_0_100(50.0 + (chg_pct * 2.0))
    vol = 50.0
    liq = 50.0
    rk = 50.0
    return {
        "momentum": float(mom),
        "trend": float(tr),
        "volatility": float(vol),
        "liquidity": float(liq),
        "risk": float(rk),
    }


def _classify(ai_score: float, execution_score: float) -> str:
    a = float(ai_score)
    e = float(execution_score)
    if a >= 75.0 and e >= 65.0:
        return "ACTIONABLE"
    if a >= 60.0:
        return "SETUP"
    if a >= 45.0:
        return "WATCH"
    return "IGNORE"


async def pick_best_async(*, universe: List[str], tz: Optional[str] = None, allow_llm: bool = True, max_seconds: float = 8.0) -> Dict[str, Any]:
    t0 = time.time()
    syms = _safe_symbol_list(universe, cap=50)

    # BATCHING: fetch all daily bars + snapshots with minimal requests.
    # This prevents timeouts and rate-limit spam.
    bars_by_symbol: Dict[str, List[Dict[str, Any]]] = {}
    snaps_by_symbol: Dict[str, Any] = {}
    try:
        bars_by_symbol = await asyncio.to_thread(get_bars_batch, syms, "1Day", 100)
    except Exception:
        bars_by_symbol = {}
    try:
        snaps_by_symbol = await asyncio.to_thread(get_snapshots_batch, syms)
    except Exception:
        snaps_by_symbol = {}

    raw_results: List[Optional[Dict[str, Any]]] = []
    for s in syms:
        if (time.time() - t0) > float(max_seconds):
            break
        candles = bars_by_symbol.get(s) if isinstance(bars_by_symbol, dict) else None
        snap = snaps_by_symbol.get(s) if isinstance(snaps_by_symbol, dict) else None
        if not isinstance(candles, list):
            candles = []
        if not isinstance(snap, dict):
            # fallback snapshot shell
            last_close = None
            try:
                last_close = float(candles[-1].get("c")) if candles and candles[-1].get("c") is not None else None
            except Exception:
                last_close = None
            snap = {
                "dailyBar": {"c": last_close},
                "prevDailyBar": {},
                "latestQuote": {},
                "latestTrade": {"p": last_close},
            }
        raw_results.append({"symbol": s, "snapshot": snap, "candles": candles})

    candidates: List[Dict[str, Any]] = []

    for res in raw_results:
        if not res:
            continue
        sym = str(res.get("symbol") or "").strip().upper()
        candles = res.get("candles") if isinstance(res.get("candles"), list) else []
        snap = res.get("snapshot") if isinstance(res.get("snapshot"), dict) else {}
        if not sym:
            continue

        ind = None
        data_completeness = 0.0
        if isinstance(candles, list) and len(candles) >= 50:
            try:
                ind = calculate_indicators(candles)
                data_completeness = 1.0
            except Exception:
                ind = None
                data_completeness = 0.0
        if ind is None and isinstance(snap, dict) and snap:
            ind = _indicators_from_snapshot_fallback(snap=snap)
            data_completeness = 0.35
        if not isinstance(ind, dict) or not ind:
            continue

        news_sentiment = _news_sentiment_from_snapshot(snap)
        ns100 = _sentiment_score_0_100(news_sentiment)

        if not _passes_fast_filter(ind):
            continue

        ai_score = score_composite_0_100(indicators=ind, news_sentiment_0_100=ns100)
        execution_score = score_execution_0_100(indicators=ind)

        candidates.append(
            {
                "symbol": sym,
                "ai_score": float(ai_score),
                "execution_score": float(execution_score),
                "data_completeness": float(data_completeness),
                "technical_analysis": ind,
                "news_sentiment": news_sentiment,
                "candles": candles,
                "snapshot": snap,
            }
        )

    if len(candidates) < 5:
        # Auto fallback to scoring full universe (no filter)
        candidates = []
        for res in raw_results:
            if not res:
                continue
            sym = str(res.get("symbol") or "").strip().upper()
            candles = res.get("candles") if isinstance(res.get("candles"), list) else []
            snap = res.get("snapshot") if isinstance(res.get("snapshot"), dict) else {}
            if not sym or len(candles) < 50:
                continue
            try:
                ind = calculate_indicators(candles)
            except Exception:
                continue
            news_sentiment = _news_sentiment_from_snapshot(snap)
            ns100 = _sentiment_score_0_100(news_sentiment)
            ai_score = score_composite_0_100(indicators=ind, news_sentiment_0_100=ns100)
            execution_score = score_execution_0_100(indicators=ind)
            candidates.append(
                {
                    "symbol": sym,
                    "ai_score": float(ai_score),
                    "execution_score": float(execution_score),
                    "technical_analysis": ind,
                    "news_sentiment": news_sentiment,
                    "candles": candles,
                    "snapshot": snap,
                }
            )

    if not candidates:
        # Deterministic fallback. Still try to provide last price and % change.
        snap_norm = None
        try:
            snap_norm = await asyncio.to_thread(get_snapshot_normalized, "SPY")
        except Exception:
            snap_norm = None
        try:
            lp0 = float(snap_norm.get("last_price")) if isinstance(snap_norm, dict) and snap_norm.get("last_price") is not None else None
        except Exception:
            lp0 = None
        conf0 = 0.0
        if lp0 is not None and float(lp0) > 0.0:
            conf0 = 25.0
        return {
            "symbol": "SPY",
            "classification": "IGNORE",
            "confidence": float(conf0),
            "ai_score": 0.0,
            "execution_score": 0.0,
            "technical_analysis": {},
            "news_sentiment": {"direction": "NEUTRAL", "summary": "Unavailable", "score_100": 50},
            "execution_plan": {"strategy": "AVOID", "time_window": "Next session", "session": "Pre-market", "playbook": "Avoid / Next session"},
            "trade_plan": {},
            "why": [],
            "what_confirms": [],
            "what_breaks": [],
            "direction": "neutral",
            "last_price": (snap_norm.get("last_price") if isinstance(snap_norm, dict) else None),
            "percent_change": (snap_norm.get("percent_change") if isinstance(snap_norm, dict) else None),
        }

    candidates.sort(key=lambda x: float(x.get("ai_score") or 0.0), reverse=True)
    best = candidates[0]

    ind = best.get("technical_analysis") if isinstance(best.get("technical_analysis"), dict) else {}
    ns = best.get("news_sentiment") if isinstance(best.get("news_sentiment"), dict) else {"direction": "NEUTRAL", "summary": "Unavailable", "score_100": 50}

    plan = build_execution_plan(indicators=ind, tz=tz)

    ai_score = float(best.get("ai_score") or 0.0)
    execution_score = float(best.get("execution_score") or 0.0)

    classification = _classify(ai_score, execution_score)
    try:
        ns100_best = _sentiment_score_0_100(ns)
    except Exception:
        ns100_best = 50.0
    confidence = _confidence_adjusted_0_100(
        ai_score=float(ai_score),
        execution_score=float(execution_score),
        data_completeness_0_1=float(best.get("data_completeness") or 0.0),
        news_sentiment_0_100=float(ns100_best),
    )

    try:
        confidence = _confidence_from_factors_0_100(
            indicators=ind,
            candles=(best.get("candles") if isinstance(best.get("candles"), list) else []),
            snapshot=(best.get("snapshot") if isinstance(best.get("snapshot"), dict) else {}),
            news_sentiment=ns,
        )
    except Exception:
        # keep adjusted composite confidence as fallback
        confidence = float(confidence)

    blocks = _build_why_blocks(ind, ns)
    if allow_llm:
        blocks = await asyncio.to_thread(
            _llm_enrich_once,
            symbol=str(best.get("symbol") or "").strip().upper(),
            indicators=ind,
            news_sentiment=ns,
            execution_plan=plan,
            timeout_fallback=blocks,
        )

    snap_norm = None
    try:
        snap_norm = await asyncio.to_thread(get_snapshot_normalized, str(best.get("symbol") or "").strip().upper())
    except Exception:
        snap_norm = None

    return {
        "symbol": str(best.get("symbol") or "").strip().upper(),
        "classification": classification,
        "confidence": float(confidence),
        "ai_score": float(ai_score),
        "execution_score": float(execution_score),
        "technical_analysis": ind,
        "news_sentiment": ns,
        "execution_plan": plan,
        "trade_plan": {},
        "why": blocks.get("why") if isinstance(blocks.get("why"), list) else [],
        "what_confirms": blocks.get("what_confirms") if isinstance(blocks.get("what_confirms"), list) else [],
        "what_breaks": blocks.get("what_breaks") if isinstance(blocks.get("what_breaks"), list) else [],
        "direction": _direction_from_indicators(ind),
        "last_price": (snap_norm.get("last_price") if isinstance(snap_norm, dict) else None),
        "percent_change": (snap_norm.get("percent_change") if isinstance(snap_norm, dict) else None),
    }
