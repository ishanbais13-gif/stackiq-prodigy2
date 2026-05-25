from __future__ import annotations

import time as _time
from typing import Any, Dict, List

from data_fetcher import get_bars


def _safe_f(x: Any) -> float | None:
    try:
        v = float(x)
        if v != v:
            return None
        return v
    except Exception:
        return None


def _sma(values: List[float], period: int) -> float | None:
    if len(values) < int(period or 0) or period <= 0:
        return None
    window = values[-period:]
    if not window:
        return None
    return float(sum(window) / float(period))


def _true_ranges(bars: List[Dict[str, Any]]) -> List[float]:
    out: List[float] = []
    prev_close: float | None = None
    for bar in bars:
        h = _safe_f((bar or {}).get("h"))
        l = _safe_f((bar or {}).get("l"))
        c = _safe_f((bar or {}).get("c"))
        if h is None or l is None:
            continue
        if prev_close is None:
            tr = h - l
        else:
            tr = max(h - l, abs(h - prev_close), abs(l - prev_close))
        out.append(float(max(0.0, tr)))
        if c is not None:
            prev_close = c
    return out


# ---------------------------------------------------------------------------
# 5-minute module-level cache for regime detection
# ---------------------------------------------------------------------------
_REGIME_CACHE: Dict[str, Any] = {"ts": 0.0, "result": None}
_REGIME_TTL_S: float = 0.0  # no cache — always recompute fresh


def _unknown_regime() -> Dict[str, Any]:
    return {
        "regime": "UNKNOWN",
        "regime_legacy": "unknown",
        "regime_strength": "unknown",
        "spy_above_sma50": False,
        "spy_above_sma200": False,
        "trend_slope_5d": 0.0,
        "vix_proxy": 0.0,
        "confidence": 0.0,
        "note": "insufficient_data",
    }


def _compute_regime_internal() -> Dict[str, Any]:
    """Fetch SPY bars and classify market regime as BULL / BEAR / CHOPPY."""
    bars_obj = get_bars("SPY", "1Day", 220)
    bars = bars_obj.get("candles") if isinstance(bars_obj, dict) else []
    if not isinstance(bars, list) or len(bars) < 50:
        return _unknown_regime()

    closes = [x for x in (_safe_f((b or {}).get("c")) for b in bars) if x is not None]
    if len(closes) < 50:
        return _unknown_regime()

    price = closes[-1]
    sma20 = _sma(closes, 20)
    sma50 = _sma(closes, min(50, len(closes)))
    sma200 = _sma(closes, min(200, len(closes))) if len(closes) >= 100 else None

    # 5-day rate of change as slope indicator
    slope_5d = 0.0
    try:
        if len(closes) >= 6 and closes[-6] and closes[-6] > 0:
            slope_5d = (closes[-1] - closes[-6]) / closes[-6] * 100.0
    except Exception:
        slope_5d = 0.0

    # VIX proxy: 14-day ATR as % of price
    trs = _true_ranges([b for b in bars if isinstance(b, dict)])
    atr14 = sum(trs[-14:]) / 14.0 if len(trs) >= 14 else 0.0
    vix_proxy = round(atr14 / price * 100.0, 2) if price and price > 0 else 0.0

    # ATR momentum: recent 5 bars vs prior 5 bars
    atr_rising = False
    if len(trs) >= 10:
        atr_recent = sum(trs[-5:]) / 5.0
        atr_prev = sum(trs[-10:-5]) / 5.0
        atr_rising = bool(atr_prior := atr_prev) and atr_recent > atr_prior * 1.15

    spy_above_50 = bool(price and sma50 and float(price) > float(sma50))
    spy_above_200 = bool(price and sma200 and float(price) > float(sma200))
    sma50_above_200 = bool(sma50 and sma200 and float(sma50) > float(sma200))

    # -----------------------------------------------------------------------
    # Regime classification
    # -----------------------------------------------------------------------
    if spy_above_50 and sma50_above_200 and slope_5d > -1.5:
        regime = "BULL"
        regime_legacy = "trend_up"
        if slope_5d > 1.5 and vix_proxy < 1.5:
            strength = "strong"
        elif slope_5d >= 0.0 or vix_proxy < 2.0:
            strength = "moderate"
        else:
            strength = "weak"

    elif not spy_above_50 and not spy_above_200:
        regime = "BEAR"
        regime_legacy = "risk_off"
        if slope_5d < -2.0 or vix_proxy > 2.5:
            strength = "strong"
        elif slope_5d < -1.0:
            strength = "moderate"
        else:
            strength = "weak"

    elif atr_rising or vix_proxy > 2.0:
        regime = "CHOPPY"
        regime_legacy = "range_bound"
        strength = "strong" if vix_proxy > 2.5 else "moderate"

    else:
        # Price above SMA50 but SMA50 below SMA200 (recovery attempt), or other mixed
        regime = "CHOPPY"
        regime_legacy = "range_bound"
        strength = "weak"

    return {
        "regime": regime,
        "regime_legacy": regime_legacy,
        "regime_strength": strength,
        "spy_above_sma50": spy_above_50,
        "spy_above_sma200": spy_above_200,
        "sma50_above_sma200": sma50_above_200,
        "trend_slope_5d": round(slope_5d, 2),
        "vix_proxy": vix_proxy,
        "confidence": round(
            0.75 if strength == "strong" else (0.50 if strength == "moderate" else 0.30), 2
        ),
        "note": f"SPY@{round(price, 2)} SMA50@{round(float(sma50 or 0), 2)} SMA200@{round(float(sma200 or 0), 2) if sma200 else 'N/A'}",
    }


def detect_market_regime_full() -> Dict[str, Any]:
    """Full BULL/BEAR/CHOPPY regime detection — 5-minute cached, fail-safe."""
    global _REGIME_CACHE
    now = _time.time()
    try:
        cached_ts = float(_REGIME_CACHE.get("ts") or 0.0)
        if (now - cached_ts) < _REGIME_TTL_S:
            cached_result = _REGIME_CACHE.get("result")
            if isinstance(cached_result, dict) and cached_result.get("regime"):
                return dict(cached_result)
    except Exception:
        pass

    try:
        result = _compute_regime_internal()
    except Exception:
        result = _unknown_regime()

    try:
        _REGIME_CACHE["ts"] = now
        _REGIME_CACHE["result"] = dict(result)
    except Exception:
        pass

    return result


def detect_market_regime(snapshot_data: dict) -> str:
    """Legacy string-only interface (backwards compatible)."""
    _ = snapshot_data
    try:
        return str(detect_market_regime_full().get("regime_legacy") or "unknown")
    except Exception:
        return "unknown"
