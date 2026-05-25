from __future__ import annotations

import math
from typing import Any, Dict, List, Optional, Tuple


def _safe_float(value: Any) -> Optional[float]:
    try:
        x = float(value)
    except Exception:
        return None
    if not math.isfinite(x):
        return None
    return float(x)


def _clamp(value: Any, lo: float, hi: float) -> float:
    x = _safe_float(value)
    if x is None:
        x = float(lo)
    if x < lo:
        x = float(lo)
    if x > hi:
        x = float(hi)
    return float(x)


def _clamp01(value: Any) -> float:
    return _clamp(value, 0.0, 1.0)


def _mean(values: List[float]) -> Optional[float]:
    if not values:
        return None
    return float(sum(values) / float(len(values) or 1))


def _extract_series(bars: List[Dict[str, Any]], key: str) -> List[float]:
    out: List[float] = []
    for bar in bars:
        if not isinstance(bar, dict):
            continue
        v = _safe_float(bar.get(key))
        if v is None:
            continue
        out.append(float(v))
    return out


def _atr(highs: List[float], lows: List[float], closes: List[float], period: int) -> Optional[float]:
    if period <= 0:
        return None
    if len(highs) != len(lows) or len(lows) != len(closes):
        return None
    if len(closes) < period + 2:
        return None

    trs: List[float] = []
    for i in range(1, len(closes)):
        h = float(highs[i])
        l = float(lows[i])
        prev_close = float(closes[i - 1])
        tr = max(h - l, abs(h - prev_close), abs(l - prev_close))
        trs.append(float(tr))

    if not trs:
        return None
    tail = trs[-period:] if len(trs) >= period else trs
    return _mean(tail)


def _ret_3d(closes: List[float]) -> Optional[float]:
    if len(closes) < 4:
        return None
    start = _safe_float(closes[-4])
    end = _safe_float(closes[-1])
    if start is None or end is None or float(start) <= 0.0:
        return None
    return float((float(end) - float(start)) / float(start))


def _score_from_01(v01: float) -> float:
    return float(round(_clamp01(v01) * 10.0, 2))


def _interpretation(score: float) -> str:
    s = _clamp(score, 0.0, 10.0)
    if s >= 7.0:
        return "early momentum buildup detected"
    if s <= 4.0:
        return "late-stage move risk or weak pre-move structure"
    return "neutral pre-mover setup"


def compute_pre_mover_signal(snapshot: dict) -> dict:
    """Compute a deterministic pre-mover signal in [0,10].

    Expected input payload (best-effort):
      {
        "symbol": str,
        "snapshot": {...raw market snapshot...},
        "bars": [{"o","h","l","c","v",...}, ...],
        "spy_bars": [{"o","h","l","c","v",...}, ...],
      }
    """
    out: Dict[str, Any] = {
        "pre_mover_score": 5.0,
        "signals": {},
        "components_used": 0,
        "interpretation": "insufficient_data",
    }

    try:
        payload = snapshot if isinstance(snapshot, dict) else {}
        snap = payload.get("snapshot") if isinstance(payload.get("snapshot"), dict) else {}
        bars = payload.get("bars") if isinstance(payload.get("bars"), list) else []
        spy_bars = payload.get("spy_bars") if isinstance(payload.get("spy_bars"), list) else []

        highs = _extract_series(bars, "h")
        lows = _extract_series(bars, "l")
        closes = _extract_series(bars, "c")
        vols = _extract_series(bars, "v")

        spy_closes = _extract_series(spy_bars, "c")

        components: List[Tuple[float, float]] = []
        signals: Dict[str, Any] = {}

        # 1) Volume acceleration
        try:
            cur_vol = None
            db = snap.get("dailyBar") if isinstance(snap, dict) else {}
            if isinstance(db, dict):
                cur_vol = _safe_float(db.get("v"))
            if cur_vol is None and vols:
                cur_vol = float(vols[-1])

            avg_prev_20 = None
            if len(vols) >= 21:
                avg_prev_20 = _mean([float(v) for v in vols[-21:-1] if _safe_float(v) is not None])
            elif len(vols) >= 2:
                avg_prev_20 = _mean([float(v) for v in vols[:-1] if _safe_float(v) is not None])

            if cur_vol is not None and avg_prev_20 is not None and float(avg_prev_20) > 0:
                ratio = float(cur_vol) / float(avg_prev_20)
                v01 = _clamp01((float(ratio) - 1.0) / 1.5)
                score = _score_from_01(v01)
                components.append((0.30, score))
                signals["volume_acceleration"] = {
                    "score": score,
                    "volume_ratio": float(round(ratio, 3)),
                }
        except Exception:
            pass

        # 2) Range compression (ATR5 / ATR20)
        atr5 = None
        atr20 = None
        atr_ratio = None
        try:
            atr5 = _atr(highs, lows, closes, 5)
            atr20 = _atr(highs, lows, closes, 20)
            if atr5 is not None and atr20 is not None and float(atr20) > 0:
                atr_ratio = float(atr5) / float(atr20)
                rc01 = _clamp01((1.20 - float(atr_ratio)) / 0.70)
                score = _score_from_01(rc01)
                components.append((0.25, score))
                signals["range_compression"] = {
                    "score": score,
                    "atr5_atr20_ratio": float(round(atr_ratio, 4)),
                }
        except Exception:
            pass

        # 3) Relative strength vs SPY (3-day return spread)
        try:
            stock_r3 = _ret_3d(closes)
            spy_r3 = _ret_3d(spy_closes)
            if stock_r3 is not None and spy_r3 is not None:
                alpha = float(stock_r3) - float(spy_r3)
                rs01 = _clamp01((float(alpha) + 0.02) / 0.06)
                score = _score_from_01(rs01)
                components.append((0.25, score))
                signals["relative_strength"] = {
                    "score": score,
                    "stock_3d_return_pct": float(round(float(stock_r3) * 100.0, 2)),
                    "spy_3d_return_pct": float(round(float(spy_r3) * 100.0, 2)),
                    "alpha_3d_pct": float(round(float(alpha) * 100.0, 2)),
                }
            elif stock_r3 is not None:
                rs01 = _clamp01((float(stock_r3) + 0.01) / 0.05)
                score = _score_from_01(rs01)
                components.append((0.25, score))
                signals["relative_strength"] = {
                    "score": score,
                    "stock_3d_return_pct": float(round(float(stock_r3) * 100.0, 2)),
                    "spy_3d_return_pct": None,
                    "alpha_3d_pct": None,
                }
        except Exception:
            pass

        # 4) Liquidity expansion (dollar volume expansion + volatility stability)
        try:
            dollars: List[float] = []
            for bar in bars:
                if not isinstance(bar, dict):
                    continue
                c = _safe_float(bar.get("c"))
                v = _safe_float(bar.get("v"))
                if c is None or v is None or float(c) <= 0 or float(v) < 0:
                    continue
                dollars.append(float(c) * float(v))

            cur_dollar = None
            db = snap.get("dailyBar") if isinstance(snap, dict) else {}
            if isinstance(db, dict):
                c_now = _safe_float(db.get("c"))
                v_now = _safe_float(db.get("v"))
                if c_now is not None and v_now is not None and float(c_now) > 0 and float(v_now) >= 0:
                    cur_dollar = float(c_now) * float(v_now)
            if cur_dollar is None and dollars:
                cur_dollar = float(dollars[-1])

            avg_prev_5 = None
            if len(dollars) >= 6:
                avg_prev_5 = _mean(dollars[-6:-1])
            elif len(dollars) >= 5:
                avg_prev_5 = _mean(dollars[-5:])

            if cur_dollar is not None and avg_prev_5 is not None and float(avg_prev_5) > 0:
                dollar_ratio = float(cur_dollar) / float(avg_prev_5)
                liq01 = _clamp01((float(dollar_ratio) - 1.0) / 1.0)

                stability01 = None
                if atr_ratio is not None:
                    stability01 = 1.0 - _clamp01(abs(float(atr_ratio) - 1.0) / 0.50)

                if stability01 is None:
                    combined01 = float(liq01)
                else:
                    combined01 = (0.65 * float(liq01)) + (0.35 * float(stability01))

                score = _score_from_01(combined01)
                components.append((0.20, score))
                signals["liquidity_expansion"] = {
                    "score": score,
                    "dollar_volume_ratio": float(round(dollar_ratio, 3)),
                    "volatility_stability": (float(round(stability01, 3)) if stability01 is not None else None),
                }
        except Exception:
            pass

        if components:
            w_sum = float(sum(w for w, _ in components) or 0.0)
            if w_sum > 0:
                weighted = float(sum(float(w) * float(s) for w, s in components) / w_sum)
                final_score = float(round(_clamp(weighted, 0.0, 10.0), 2))
                out["pre_mover_score"] = final_score
                out["components_used"] = int(len(components))
                out["signals"] = signals
                out["interpretation"] = _interpretation(final_score)
                return out

        out["signals"] = signals
        out["interpretation"] = "insufficient_data"
        return out
    except Exception:
        return out
