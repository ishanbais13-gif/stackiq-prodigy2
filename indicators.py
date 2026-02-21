"""Technical indicators for StackIQ.

This module computes real multi-bar indicators from OHLCV candle data (as
returned by Alpaca bars endpoints). It is intentionally lightweight (no pandas)
and defensive against empty/partial data.

Key outputs are normalized to 0-100. If candle data is unavailable/empty, this
module returns mid-value fallbacks (50) rather than zeros.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple


MID_VALUE_FALLBACK: int = 50


def _clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    try:
        v = float(value)
    except Exception:
        return float(MID_VALUE_FALLBACK)
    if v != v:  # NaN
        return float(MID_VALUE_FALLBACK)
    return max(low, min(high, v))


def _clamp_score_nonzero(value: float) -> int:
    """Clamp to 0-100 and avoid returning 0 when data is present.

    We still allow 0 internally for computations, but final normalized scores
    are never 0 unless the caller intentionally uses a fallback.
    """
    v = int(round(_clamp(value)))
    return 1 if v == 0 else v


def _safe_div(n: float, d: float, default: float = 0.0) -> float:
    try:
        dn = float(d)
        if dn == 0.0:
            return default
        return float(n) / dn
    except Exception:
        return default


def _to_bars(candles: Any) -> List[Dict[str, Any]]:
    """Accepts Alpaca-style bars, returns list[dict] with o/h/l/c/v."""
    if candles is None:
        return []
    # Pandas DataFrame support: expected columns open/high/low/close/volume.
    # We keep this optional so the backend doesn't hard-require pandas.
    try:
        cols = getattr(candles, "columns", None)
        to_dict = getattr(candles, "to_dict", None)
        if cols is not None and callable(to_dict):
            cols_l = [str(x).strip().lower() for x in list(cols)]
            if all(k in cols_l for k in ("open", "high", "low", "close", "volume")):
                records = candles.to_dict(orient="records")  # type: ignore
                out: List[Dict[str, Any]] = []
                for r in records:
                    if not isinstance(r, dict):
                        continue
                    out.append(
                        {
                            "o": r.get("open"),
                            "h": r.get("high"),
                            "l": r.get("low"),
                            "c": r.get("close"),
                            "v": r.get("volume"),
                        }
                    )
                return out
    except Exception:
        pass
    if isinstance(candles, list):
        return [c for c in candles if isinstance(c, dict)]
    if hasattr(candles, "__iter__") and not isinstance(candles, (str, bytes, dict)):
        out: List[Dict[str, Any]] = []
        for x in candles:
            if isinstance(x, dict):
                out.append(x)
        return out
    if isinstance(candles, dict):
        # Sometimes callers pass {"bars": [...]}.
        b = candles.get("bars")
        if isinstance(b, list):
            return [c for c in b if isinstance(c, dict)]
    return []


def _extract_ohlcv(
    bars: List[Dict[str, Any]],
) -> Tuple[List[float], List[float], List[float], List[float], List[float]]:
    o: List[float] = []
    h: List[float] = []
    l: List[float] = []
    c: List[float] = []
    v: List[float] = []

    for b in bars:
        try:
            oo = float(b.get("o"))
            hh = float(b.get("h"))
            ll = float(b.get("l"))
            cc = float(b.get("c"))
            vv = float(b.get("v")) if b.get("v") is not None else 0.0
        except Exception:
            continue
        if cc <= 0 or hh <= 0 or ll <= 0:
            continue
        o.append(oo)
        h.append(hh)
        l.append(ll)
        c.append(cc)
        v.append(max(0.0, vv))
    return o, h, l, c, v


def _ema(values: List[float], period: int) -> List[float]:
    if period <= 1 or not values:
        return values[:]
    k = 2.0 / (period + 1.0)
    out: List[float] = []
    ema_v: Optional[float] = None
    for x in values:
        if ema_v is None:
            ema_v = float(x)
        else:
            ema_v = float(x) * k + ema_v * (1.0 - k)
        out.append(ema_v)
    return out


def _rsi(closes: List[float], period: int = 14) -> Optional[float]:
    if len(closes) < period + 1:
        return None
    gains: float = 0.0
    losses: float = 0.0
    for i in range(1, period + 1):
        ch = closes[i] - closes[i - 1]
        if ch >= 0:
            gains += ch
        else:
            losses += -ch
    avg_gain = gains / period
    avg_loss = losses / period
    for i in range(period + 1, len(closes)):
        ch = closes[i] - closes[i - 1]
        g = ch if ch > 0 else 0.0
        ls = -ch if ch < 0 else 0.0
        avg_gain = (avg_gain * (period - 1) + g) / period
        avg_loss = (avg_loss * (period - 1) + ls) / period

    if avg_loss == 0.0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def _macd(
    closes: List[float], fast: int = 12, slow: int = 26, signal: int = 9
) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    if len(closes) < slow + signal:
        return None, None, None
    ema_fast = _ema(closes, fast)
    ema_slow = _ema(closes, slow)
    macd_line = [a - b for a, b in zip(ema_fast, ema_slow)]
    sig = _ema(macd_line, signal)
    hist = macd_line[-1] - sig[-1]
    return macd_line[-1], sig[-1], hist


def _atr(
    highs: List[float], lows: List[float], closes: List[float], period: int = 14
) -> Optional[float]:
    if len(closes) < period + 1:
        return None
    trs: List[float] = []
    for i in range(1, len(closes)):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1]),
        )
        trs.append(tr)
    if len(trs) < period:
        return None
    atr_v = sum(trs[:period]) / period
    for i in range(period, len(trs)):
        atr_v = (atr_v * (period - 1) + trs[i]) / period
    return atr_v


def _stddev(values: List[float]) -> Optional[float]:
    if not values:
        return None
    m = sum(values) / len(values)
    var = sum((x - m) ** 2 for x in values) / max(1, (len(values) - 1))
    return var**0.5


def _fallback_mid() -> int:
    return MID_VALUE_FALLBACK


def calculate_momentum(candles: Any) -> int:
    bars = _to_bars(candles)
    if not bars:
        return _fallback_mid()
    _o, _h, _l, closes, vols = _extract_ohlcv(bars)
    if len(closes) < 20:
        return _fallback_mid()

    rsi_v = _rsi(closes, 14)
    _macd_line, _macd_sig, macd_hist = _macd(closes)
    ema10 = _ema(closes, 10)

    rsi_score = 50.0
    if rsi_v is not None:
        rsi_score = _clamp(rsi_v)

    macd_score = 50.0
    if macd_hist is not None:
        denom = max(1e-9, closes[-1])
        macd_pct = (macd_hist / denom) * 100.0
        macd_score = _clamp(50.0 + macd_pct * 250.0)

    ema_slope_score = 50.0
    try:
        ema_slope = _safe_div((ema10[-1] - ema10[-6]), ema10[-6], 0.0) * 100.0
        ema_slope_score = _clamp(50.0 + ema_slope * 8.0)
    except Exception:
        ema_slope_score = 50.0

    vol_score = 50.0
    if len(vols) >= 20:
        avg20 = sum(vols[-20:]) / 20.0
        vol_ratio = _safe_div(vols[-1], avg20, 1.0)
        vol_score = _clamp(50.0 + (vol_ratio - 1.0) * 30.0)

    momentum = (
        0.45 * rsi_score
        + 0.35 * macd_score
        + 0.15 * ema_slope_score
        + 0.05 * vol_score
    )
    return _clamp_score_nonzero(momentum)


def calculate_trend(candles: Any) -> int:
    bars = _to_bars(candles)
    if not bars:
        return _fallback_mid()
    _o, _h, _l, closes, _v = _extract_ohlcv(bars)
    if len(closes) < 60:
        return _fallback_mid()

    ema20 = _ema(closes, 20)
    ema50 = _ema(closes, 50)
    ema200 = _ema(closes, 200) if len(closes) >= 200 else None

    price = closes[-1]

    align = 0.0
    if ema20[-1] > ema50[-1]:
        align += 1.0
    if ema200 is not None:
        if ema50[-1] > ema200[-1]:
            align += 1.0
        if price > ema200[-1]:
            align += 0.5
    if price > ema20[-1]:
        align += 0.5

    slope20 = _safe_div((ema20[-1] - ema20[-11]), ema20[-11], 0.0) * 100.0
    slope50 = _safe_div((ema50[-1] - ema50[-21]), ema50[-21], 0.0) * 100.0

    trend = 50.0 + align * 12.0 + slope20 * 6.0 + slope50 * 4.0
    return _clamp_score_nonzero(trend)


def calculate_volatility(candles: Any) -> int:
    bars = _to_bars(candles)
    if not bars:
        return _fallback_mid()
    _o, highs, lows, closes, _v = _extract_ohlcv(bars)
    if len(closes) < 20:
        return _fallback_mid()

    atr_v = _atr(highs, lows, closes, 14)
    atr_pct = 0.0
    if atr_v is not None:
        atr_pct = _safe_div(atr_v, closes[-1], 0.0) * 100.0

    rets: List[float] = []
    for i in range(1, min(len(closes), 31)):
        prev = closes[-i - 1]
        cur = closes[-i]
        if prev <= 0:
            continue
        rets.append((cur - prev) / prev)
    sd = _stddev(rets)
    sd_pct = float(sd) * 100.0 if sd is not None else 0.0

    vol = 50.0 + atr_pct * 6.0 + sd_pct * 10.0
    return _clamp_score_nonzero(vol)


def calculate_liquidity(candles: Any) -> int:
    bars = _to_bars(candles)
    if not bars:
        return _fallback_mid()
    _o, _h, _l, closes, vols = _extract_ohlcv(bars)
    if len(closes) < 20 or len(vols) < 20:
        return _fallback_mid()

    last_price = closes[-1]
    avg_vol20 = sum(vols[-20:]) / 20.0
    dollar_vol = avg_vol20 * last_price

    dv = max(0.0, float(dollar_vol))
    if dv <= 0.0:
        return _fallback_mid()

    import math

    liq = 15.0 + 20.0 * math.log10(dv / 1_000_000.0 + 1.0)

    avg_vol5 = sum(vols[-5:]) / 5.0
    vol_trend = _safe_div(avg_vol5, avg_vol20, 1.0)
    liq += (vol_trend - 1.0) * 20.0

    return _clamp_score_nonzero(liq)


def calculate_risk(candles: Any) -> int:
    bars = _to_bars(candles)
    if not bars:
        return _fallback_mid()
    _o, highs, lows, closes, _v = _extract_ohlcv(bars)
    if len(closes) < 30:
        return _fallback_mid()

    atr_v = _atr(highs, lows, closes, 14)
    atr_pct = 0.0
    if atr_v is not None:
        atr_pct = _safe_div(atr_v, closes[-1], 0.0) * 100.0

    lookback = closes[-60:] if len(closes) >= 60 else closes[:]
    peak = lookback[0]
    max_dd = 0.0
    for x in lookback:
        if x > peak:
            peak = x
        dd = _safe_div((peak - x), peak, 0.0)
        if dd > max_dd:
            max_dd = dd
    max_dd_pct = max_dd * 100.0

    risk = 50.0 + atr_pct * 7.0 + max_dd_pct * 1.2
    return _clamp_score_nonzero(risk)


def technical_analysis_from_candles(candles: Any) -> Dict[str, int]:
    """Primary interface used by scoring engines."""
    return {
        "momentum": calculate_momentum(candles),
        "trend": calculate_trend(candles),
        "volatility": calculate_volatility(candles),
        "liquidity": calculate_liquidity(candles),
        "risk": calculate_risk(candles),
    }


def compute_indicators(
    price: float,
    change_pct: Optional[float],
    quote: Dict[str, Any],
    is_market_open: Optional[bool] = None,
    candles: Any = None,
) -> Dict[str, Any]:
    """Backwards compatible wrapper.

    Existing call sites used quote-only pseudo-indicators. New usage should pass
    `candles` (Alpaca bars) to compute real indicators.
    """
    bars = _to_bars(candles)
    if bars:
        _o, h, l, c, _v = _extract_ohlcv(bars)
        rsi_v = _rsi(c, 14)
        _macd_line, _macd_sig, macd_hist = _macd(c)
        ema_fast = _ema(c, 12)[-1] if len(c) >= 12 else None
        ema_slow = _ema(c, 26)[-1] if len(c) >= 26 else None
        atr_v = _atr(h, l, c, 14)
        atr_pct = (
            _safe_div(float(atr_v), c[-1], 0.0) * 100.0 if atr_v is not None else None
        )

        ta = technical_analysis_from_candles(bars)
        return {
            "rsi": float(rsi_v) if rsi_v is not None else float(MID_VALUE_FALLBACK),
            "ema_fast": float(ema_fast) if ema_fast is not None else float(price),
            "ema_slow": float(ema_slow) if ema_slow is not None else float(price),
            "macd_hist": float(macd_hist) if macd_hist is not None else 0.0,
            "atr": float(atr_v) if atr_v is not None else 0.0,
            "atr_pct": float(atr_pct) if atr_pct is not None else 0.0,
            "momentum": int(ta["momentum"]),
            "trend": int(ta["trend"]),
            "volatility": int(ta["volatility"]),
            "liquidity": int(ta["liquidity"]),
            "risk": int(ta["risk"]),
            "used_session_reference": False,
        }

    return {
        "rsi": float(MID_VALUE_FALLBACK),
        "ema_fast": float(price),
        "ema_slow": float(price),
        "macd_hist": 0.0,
        "atr": 0.0,
        "atr_pct": 0.0,
        "momentum": MID_VALUE_FALLBACK,
        "trend": MID_VALUE_FALLBACK,
        "volatility": MID_VALUE_FALLBACK,
        "liquidity": MID_VALUE_FALLBACK,
        "risk": MID_VALUE_FALLBACK,
        "used_session_reference": (is_market_open is False),
    }


# ======================================================================
# ADDITIVE: BACKTEST + UI NORMALIZATION HELPERS (SAFE)
# ======================================================================

def compute_indicators_historical(
    price: float,
    change_pct: Optional[float],
    quote: Dict[str, Any],
) -> Dict[str, Any]:
    return compute_indicators(
        price=price,
        change_pct=change_pct,
        quote=quote,
        is_market_open=False,
    )


def normalize_indicator_score_ui(score_0_100: float) -> float:
    try:
        s = float(score_0_100)
    except Exception:
        return 1.0
    ui = 1.0 + (s / 100.0) * 9.0
    return round(_clamp(ui, 1.0, 10.0), 1)


# ======================================================================
# END ADDITIVE
# ======================================================================