from typing import Any, Dict, List, Optional


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


def score_composite_0_100(*, indicators: Dict[str, Any], news_sentiment_0_100: float) -> float:
    mom = _clamp_0_100((indicators or {}).get("momentum"))
    tr = _clamp_0_100((indicators or {}).get("trend"))
    vol = _clamp_0_100((indicators or {}).get("volatility"))
    liq = _clamp_0_100((indicators or {}).get("liquidity"))
    rk = _clamp_0_100((indicators or {}).get("risk"))

    # Launch scoring weights:
    # 0.30 momentum + 0.25 trend + 0.15 volatility + 0.15 liquidity + 0.15 risk_inverse
    # (sentiment is handled as a confidence adjustment elsewhere; keep arg for compatibility)
    risk_positive = _clamp_0_100(100.0 - rk)
    base = (
        (0.30 * mom)
        + (0.25 * tr)
        + (0.15 * vol)
        + (0.15 * liq)
        + (0.15 * risk_positive)
    )
    if mom > 70.0:
        base *= 1.2
    if tr > 60.0:
        base *= 1.1
    base = max(45.0, base)
    return float(min(95.0, max(0.0, base)))


def direction_from_indicators(indicators: Dict[str, Any]) -> str:
    mom = _clamp_0_100((indicators or {}).get("momentum"))
    tr = _clamp_0_100((indicators or {}).get("trend"))
    bias = (0.55 * tr) + (0.45 * mom)
    if bias >= 60.0:
        return "bullish"
    if bias <= 40.0:
        return "bearish"
    return "neutral"


def conviction_from_score(score_0_100: Any) -> str:
    s = _clamp_0_100(score_0_100)
    if s >= 75.0:
        return "high"
    if s >= 55.0:
        return "medium"
    return "low"


def choppy_signal_boost(signals: List[str]) -> float:
    """Returns the cumulative final_score boost for CHOPPY-regime signals (0.8 per signal fired)."""
    choppy_signals = {"RSI_OVERSOLD_BOUNCE", "SUPPORT_RECLAIM", "SECTOR_ROTATION"}
    return float(sum(0.8 for s in (signals or []) if s in choppy_signals))


def score_execution_0_100(*, indicators: Dict[str, Any], execution_factors: Optional[Dict[str, Any]] = None) -> float:
    ef = execution_factors if isinstance(execution_factors, dict) else None
    if ef is not None:
        breakout = _clamp_0_100(ef.get("breakout_proximity"))
        vwap = _clamp_0_100(ef.get("vwap_alignment"))
        volexp = _clamp_0_100(ef.get("volume_expansion"))
        overhead = _clamp_0_100(ef.get("resistance_overhead"))
        return _clamp_0_100((0.30 * breakout) + (0.25 * vwap) + (0.25 * volexp) + (0.20 * overhead))

    tr = _clamp_0_100((indicators or {}).get("trend"))
    mom = _clamp_0_100((indicators or {}).get("momentum"))
    liq = _clamp_0_100((indicators or {}).get("liquidity"))
    return _clamp_0_100((0.40 * tr) + (0.30 * mom) + (0.30 * liq))
