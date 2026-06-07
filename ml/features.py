"""
ml/features.py — Feature engineering for the NN scorer.

Turns raw OHLCV bars + candidate subscores into an 18-dim float vector.
All values are clipped to a safe range and normalized to roughly [-1, 1] or [0, 1]
so the network trains stably.

Call either:
  vector_from_bars(closes, highs, lows, volumes)            ← training (raw bars)
  vector_from_candidate(c)                                  ← inference (live scan)
"""

from __future__ import annotations
from typing import List, Optional, Any

FEATURE_DIM = 18


def _safe(v: Any, default: float = 0.0) -> float:
    try:
        f = float(v)
        return f if (f == f) else default   # NaN check
    except Exception:
        return default


def _clip(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _rsi(closes: List[float], period: int = 14) -> float:
    if len(closes) < period + 1:
        return 50.0
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    avg_g = sum(gains[-period:]) / period
    avg_l = sum(losses[-period:]) / period
    if avg_l == 0:
        return 100.0
    rs = avg_g / avg_l
    return 100.0 - 100.0 / (1.0 + rs)


def _sma(closes: List[float], period: int) -> Optional[float]:
    if len(closes) < period:
        return None
    return sum(closes[-period:]) / period


def _atr(closes: List[float], highs: List[float], lows: List[float], period: int = 14) -> Optional[float]:
    if len(closes) < period + 1 or len(highs) != len(closes) or len(lows) != len(closes):
        return None
    trs = []
    for i in range(1, len(closes)):
        tr = max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
        trs.append(tr)
    return sum(trs[-period:]) / period


def vector_from_bars(
    closes: List[float],
    highs: List[float],
    lows: List[float],
    volumes: List[float],
    edge_score_0_10: float = 5.0,
    momentum_score: float = 5.0,
    volatility_score_0_10: float = 5.0,
) -> List[float]:
    """Compute the 18-feature vector from raw OHLCV lists."""
    c = [_safe(x) for x in closes]
    h = [_safe(x) for x in highs]
    l = [_safe(x) for x in lows]
    v = [_safe(x) for x in volumes]

    price = c[-1] if c else 1.0
    if price <= 0:
        price = 1.0

    # ── Technical indicators ──────────────────────────────────────────────────

    # 1. RSI-14 normalised to [0, 1]
    rsi = _rsi(c, 14) / 100.0

    # 2. 5-day return clipped ±20%
    roc5 = _clip((c[-1] / c[-6] - 1.0) if len(c) >= 6 else 0.0, -0.20, 0.20) / 0.20

    # 3. 20-day return clipped ±30%
    roc20 = _clip((c[-1] / c[-21] - 1.0) if len(c) >= 21 else 0.0, -0.30, 0.30) / 0.30

    # 4. close vs SMA-20 (±15%)
    sma20 = _sma(c, 20)
    close_vs_sma20 = _clip((price / sma20 - 1.0) if sma20 else 0.0, -0.15, 0.15) / 0.15

    # 5. close vs SMA-50 (±20%)
    sma50 = _sma(c, 50)
    close_vs_sma50 = _clip((price / sma50 - 1.0) if sma50 else 0.0, -0.20, 0.20) / 0.20

    # 6. SMA-20 slope over last 5 bars (normalised by price)
    sma20_slope = 0.0
    if sma20 and len(c) >= 25:
        old_sma20 = _sma(c[:-5], 20)
        if old_sma20 and old_sma20 > 0:
            sma20_slope = _clip((sma20 - old_sma20) / old_sma20, -0.05, 0.05) / 0.05

    # 7. ATR% normalised (typical ATR% is 1-5%)
    atr = _atr(c, h, l, 14)
    atr_pct = _clip((atr / price) if atr else 0.02, 0.0, 0.10) / 0.10

    # 8. Volume ratio: 5d avg vs 20d avg  (>1 = expanding volume)
    avg5 = sum(v[-5:]) / 5.0 if len(v) >= 5 else 0.0
    avg20 = sum(v[-20:]) / 20.0 if len(v) >= 20 else avg5
    vol_ratio_5_20 = _clip((avg5 / avg20 - 1.0) if avg20 > 0 else 0.0, -1.0, 2.0) / 2.0

    # 9. Today's volume vs 5d avg
    today_vol = v[-1] if v else 0.0
    vol_ratio_1_5 = _clip((today_vol / avg5 - 1.0) if avg5 > 0 else 0.0, -1.0, 4.0) / 4.0

    # 10. Bollinger Band width (normalised by price)
    if len(c) >= 20 and sma20:
        std20 = (sum((x - sma20) ** 2 for x in c[-20:]) / 20.0) ** 0.5
        bb_width = _clip(4.0 * std20 / price, 0.0, 0.20) / 0.20
    else:
        bb_width = 0.5

    # 11. Average intraday close-position over last 5 bars  [0=bottom, 1=top]
    close_pos_vals = []
    for i in range(min(5, len(c))):
        hi_i = h[-(i + 1)]
        lo_i = l[-(i + 1)]
        cl_i = c[-(i + 1)]
        span = hi_i - lo_i
        close_pos_vals.append((cl_i - lo_i) / span if span > 0 else 0.5)
    close_pos = sum(close_pos_vals) / len(close_pos_vals) if close_pos_vals else 0.5

    # 12. Distance below 20-day high  [0 = at high, -1 = 20% below]
    high20 = max(h[-20:]) if len(h) >= 20 else (max(h) if h else price)
    vs_high20 = _clip((price / high20 - 1.0) if high20 > 0 else 0.0, -0.20, 0.0) / 0.20  # range [-1,0] → flip to [0,1]
    near_high = 1.0 + vs_high20   # 1 = at high, 0 = 20% below

    # 13. Up-day fraction in last 5 bars
    up5 = sum(1 for i in range(1, min(6, len(c))) if c[-i] > c[-i - 1]) / 5.0 if len(c) >= 6 else 0.5

    # 14. Up-day fraction in last 20 bars
    up20 = sum(1 for i in range(1, min(21, len(c))) if c[-i] > c[-i - 1]) / 20.0 if len(c) >= 21 else 0.5

    # 15. Consecutive up days (positive) or down days (negative), normalised ±5
    streak = 0
    for i in range(1, min(11, len(c))):
        if c[-i] > c[-i - 1]:
            if streak >= 0:
                streak += 1
            else:
                break
        else:
            if streak <= 0:
                streak -= 1
            else:
                break
    streak_norm = _clip(streak / 5.0, -1.0, 1.0)

    # ── Candidate subscores (already computed by heuristic scorer) ──────────

    # 16. Edge score 0-10 → [0, 1]
    edge_01 = _clip(_safe(edge_score_0_10) / 10.0, 0.0, 1.0)

    # 17. Momentum score 0-10 → [0, 1]
    momentum_01 = _clip(_safe(momentum_score) / 10.0, 0.0, 1.0)

    # 18. Volatility score 0-10 → [0, 1]
    volatility_01 = _clip(_safe(volatility_score_0_10) / 10.0, 0.0, 1.0)

    return [
        rsi, roc5, roc20, close_vs_sma20, close_vs_sma50,
        sma20_slope, atr_pct, vol_ratio_5_20, vol_ratio_1_5, bb_width,
        close_pos, near_high, up5, up20, streak_norm,
        edge_01, momentum_01, volatility_01,
    ]


def vector_from_candidate(c: Any) -> List[float]:
    """Compute the 18-feature vector from a live _Candidate object."""
    closes = list(c.closes or [])
    highs  = list(c.highs  or [])
    lows   = list(c.lows   or [])

    # Extract volumes from daily_bars since _Candidate doesn't store them separately
    volumes: List[float] = []
    for bar in (c.daily_bars or []):
        try:
            volumes.append(float(bar.get("v") or bar.get("volume") or 0.0))
        except Exception:
            volumes.append(0.0)

    return vector_from_bars(
        closes, highs, lows, volumes,
        edge_score_0_10=_safe(getattr(c, "edge_score_0_10", 5.0), 5.0),
        momentum_score=_safe(getattr(c, "momentum_score", 5.0), 5.0),
        volatility_score_0_10=_safe(getattr(c, "volatility_score_0_10", 5.0), 5.0),
    )
