import math
from typing import Any, Dict, List


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


def _ema(values: List[float], period: int) -> List[float]:
    if not values or period <= 1:
        return list(values)
    k = 2.0 / (float(period) + 1.0)
    out: List[float] = []
    ema = float(values[0])
    out.append(ema)
    for v in values[1:]:
        ema = (float(v) * k) + (ema * (1.0 - k))
        out.append(ema)
    return out


def _rsi(values: List[float], period: int = 14) -> float:
    try:
        if len(values) < period + 2:
            return 50.0
        gains = 0.0
        losses = 0.0
        for i in range(len(values) - period, len(values)):
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
        rsi = 100.0 - (100.0 / (1.0 + rs))
        return _clamp_0_100(rsi)
    except Exception:
        return 50.0


def _atr_pct(highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> float:
    try:
        if len(closes) < period + 2:
            return 1.0
        trs: List[float] = []
        for i in range(1, len(closes)):
            h = float(highs[i])
            l = float(lows[i])
            pc = float(closes[i - 1])
            tr = max(h - l, abs(h - pc), abs(l - pc))
            trs.append(tr)
        tail = trs[-period:] if len(trs) >= period else trs
        atr = sum(tail) / float(len(tail) or 1)
        last = float(closes[-1])
        if last <= 0:
            return 1.0
        return float(atr / last * 100.0)
    except Exception:
        return 1.0


def calculate_indicators(candles: List[Dict[str, Any]]) -> Dict[str, int]:
    if not isinstance(candles, list) or len(candles) < 50:
        raise ValueError("INSUFFICIENT_CANDLES")

    closes: List[float] = []
    highs: List[float] = []
    lows: List[float] = []
    vols: List[float] = []

    for b in candles[-200:]:
        if not isinstance(b, dict):
            continue
        c = b.get("c")
        h = b.get("h")
        l = b.get("l")
        v = b.get("v")
        if c is None or h is None or l is None:
            continue
        try:
            closes.append(float(c))
            highs.append(float(h))
            lows.append(float(l))
            vols.append(float(v or 0.0))
        except Exception:
            continue

    if len(closes) < 50:
        raise ValueError("INSUFFICIENT_CANDLES")

    rsi14 = _rsi(closes, 14)

    ema12 = _ema(closes, 12)
    ema26 = _ema(closes, 26)
    macd_line = [a - b for a, b in zip(ema12[-len(ema26) :], ema26)]
    if not macd_line:
        macd = 0.0
    else:
        macd = float(macd_line[-1])
    macd_norm = 50.0
    try:
        last = float(closes[-1])
        if last > 0:
            macd_norm = _clamp_0_100(50.0 + (macd / last * 10000.0))
    except Exception:
        macd_norm = 50.0

    momentum = _clamp_0_100((0.60 * rsi14) + (0.40 * macd_norm))

    ema20 = _ema(closes, 20)
    ema50 = _ema(closes, 50)
    ema_align = 50.0
    try:
        last = float(closes[-1])
        e20 = float(ema20[-1])
        e50 = float(ema50[-1])
        if last > 0:
            score = 50.0
            if last >= e20 >= e50:
                score = 85.0
            elif last >= e20:
                score = 70.0
            elif last < e20 <= e50:
                score = 35.0
            elif last < e20:
                score = 45.0
            ema_align = score
    except Exception:
        ema_align = 50.0

    slope = 0.0
    try:
        window = closes[-20:]
        if len(window) >= 10:
            n = len(window)
            xs = list(range(n))
            x_mean = (n - 1) / 2.0
            y_mean = sum(window) / float(n)
            num = 0.0
            den = 0.0
            for i in range(n):
                dx = float(xs[i]) - x_mean
                num += dx * (float(window[i]) - y_mean)
                den += dx * dx
            slope = (num / den) if den > 0 else 0.0
    except Exception:
        slope = 0.0

    slope_norm = 50.0
    try:
        last = float(closes[-1])
        if last > 0:
            slope_norm = _clamp_0_100(50.0 + (slope / last * 10000.0))
    except Exception:
        slope_norm = 50.0

    trend = _clamp_0_100((0.60 * ema_align) + (0.40 * slope_norm))

    atrp = _atr_pct(highs, lows, closes, 14)
    volatility = _clamp_0_100(min(100.0, atrp * 8.0))

    vol_avg = 0.0
    try:
        tail = vols[-20:] if len(vols) >= 20 else vols
        vol_avg = sum(tail) / float(len(tail) or 1)
    except Exception:
        vol_avg = 0.0

    vol_ratio = 1.0
    try:
        if vol_avg > 0:
            vol_ratio = float(vols[-1]) / float(vol_avg)
    except Exception:
        vol_ratio = 1.0

    liquidity = _clamp_0_100(50.0 + (min(3.0, max(0.0, vol_ratio)) - 1.0) * 25.0)

    risk = _clamp_0_100(100.0 - ((0.35 * momentum) + (0.35 * trend) + (0.15 * volatility) + (0.15 * liquidity)))

    return {
        "momentum": int(round(momentum)),
        "trend": int(round(trend)),
        "volatility": int(round(volatility)),
        "liquidity": int(round(liquidity)),
        "risk": int(round(risk)),
    }
