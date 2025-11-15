# indicators.py
from typing import List, Optional, Tuple

Number = float


def sma(values: List[Number], period: int) -> List[Optional[Number]]:
    """Simple moving average."""
    result: List[Optional[Number]] = []
    window_sum = 0.0
    window: List[Number] = []

    for v in values:
        window.append(v)
        window_sum += v
        if len(window) > period:
            window_sum -= window.pop(0)

        if len(window) < period:
            result.append(None)
        else:
            result.append(window_sum / period)
    return result


def _ema(values: List[Number], period: int) -> List[Optional[Number]]:
    """Exponential moving average helper."""
    result: List[Optional[Number]] = []
    if not values:
        return [None] * 0

    k = 2.0 / (period + 1.0)
    ema_val: Optional[Number] = None
    for v in values:
        if ema_val is None:
            ema_val = v
        else:
            ema_val = v * k + ema_val * (1.0 - k)
        result.append(ema_val)
    return result


def rsi(values: List[Number], period: int = 14) -> List[Optional[Number]]:
    """Relative Strength Index (simplified Wilder-style)."""
    if len(values) < period + 1:
        return [None] * len(values)

    gains: List[Number] = [0.0]
    losses: List[Number] = [0.0]
    for i in range(1, len(values)):
        diff = values[i] - values[i - 1]
        if diff >= 0:
            gains.append(diff)
            losses.append(0.0)
        else:
            gains.append(0.0)
            losses.append(-diff)

    rsis: List[Optional[Number]] = [None] * len(values)

    # First average
    avg_gain = sum(gains[1 : period + 1]) / period
    avg_loss = sum(losses[1 : period + 1]) / period

    if avg_loss == 0:
        rsis[period] = 100.0
    else:
        rs = avg_gain / avg_loss
        rsis[period] = 100.0 - (100.0 / (1.0 + rs))

    # Wilder smoothing
    for i in range(period + 1, len(values)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

        if avg_loss == 0:
            rsis[i] = 100.0
        else:
            rs = avg_gain / avg_loss
            rsis[i] = 100.0 - (100.0 / (1.0 + rs))

    return rsis


def atr(high: List[Number], low: List[Number], close: List[Number],
        period: int = 14) -> List[Optional[Number]]:
    """Average True Range."""
    n = min(len(high), len(low), len(close))
    if n == 0:
        return []

    trs: List[Number] = []
    for i in range(n):
        if i == 0:
            tr = high[i] - low[i]
        else:
            hl = high[i] - low[i]
            hc = abs(high[i] - close[i - 1])
            lc = abs(low[i] - close[i - 1])
            tr = max(hl, hc, lc)
        trs.append(tr)

    # Simple moving average of TR as ATR
    result: List[Optional[Number]] = []
    window: List[Number] = []
    window_sum = 0.0
    for v in trs:
        window.append(v)
        window_sum += v
        if len(window) > period:
            window_sum -= window.pop(0)
        if len(window) < period:
            result.append(None)
        else:
            result.append(window_sum / period)
    return result


def macd(values: List[Number],
         fast: int = 12,
         slow: int = 26,
         signal: int = 9) -> Tuple[List[Optional[Number]],
                                   List[Optional[Number]],
                                   List[Optional[Number]]]:
    """
    MACD line, signal line, and histogram.
    Uses EMA-based MACD implementation.
    """
    if not values:
        return [], [], []

    ema_fast = _ema(values, fast)
    ema_slow = _ema(values, slow)

    macd_line: List[Optional[Number]] = []
    for f, s in zip(ema_fast, ema_slow):
        if f is None or s is None:
            macd_line.append(None)
        else:
            macd_line.append(f - s)

    # Signal line is EMA of MACD line (ignoring initial None values)
    clean_macd: List[Number] = []
    for v in macd_line:
        if v is not None:
            clean_macd.append(v)

    signal_line_partial = _ema(clean_macd, signal)
    signal_line: List[Optional[Number]] = []
    idx = 0
    for v in macd_line:
        if v is None:
            signal_line.append(None)
        else:
            signal_line.append(signal_line_partial[idx])
            idx += 1

    hist: List[Optional[Number]] = []
    for m, s in zip(macd_line, signal_line):
        if m is None or s is None:
            hist.append(None)
        else:
            hist.append(m - s)

    return macd_line, signal_line, hist


def bollinger_percent(values: List[Number],
                      period: int = 20,
                      num_std_up: float = 2.0,
                      num_std_down: float = 2.0) -> List[Optional[Number]]:
    """
    Bollinger %B: (price - lower_band) / (upper_band - lower_band)
    Returns values in [0, 1] when within the bands.
    """
    import math

    result: List[Optional[Number]] = []
    window: List[Number] = []

    for v in values:
        window.append(v)
        if len(window) > period:
            window.pop(0)

        if len(window) < period:
            result.append(None)
            continue

        mean = sum(window) / period
        var = sum((x - mean) ** 2 for x in window) / period
        std = math.sqrt(var)

        upper = mean + num_std_up * std
        lower = mean - num_std_down * std
        rng = upper - lower
        if rng == 0:
            result.append(None)
        else:
            result.append((v - lower) / rng)
    return result


def dmi_adx(high: List[Number], low: List[Number], close: List[Number],
            period: int = 14) -> Tuple[List[Optional[Number]],
                                       List[Optional[Number]],
                                       List[Optional[Number]]]:
    """
    Very simplified DMI / ADX implementation.
    Returns +DI, -DI, ADX.
    """
    import math

    n = min(len(high), len(low), len(close))
    if n == 0:
        return [], [], []

    plus_dm: List[Number] = [0.0]
    minus_dm: List[Number] = [0.0]
    tr: List[Number] = [0.0]

    for i in range(1, n):
        up_move = high[i] - high[i - 1]
        down_move = low[i - 1] - low[i]

        if up_move > down_move and up_move > 0:
            plus_dm.append(up_move)
        else:
            plus_dm.append(0.0)

        if down_move > up_move and down_move > 0:
            minus_dm.append(down_move)
        else:
            minus_dm.append(0.0)

        tr_i = max(
            high[i] - low[i],
            abs(high[i] - close[i - 1]),
            abs(low[i] - close[i - 1]),
        )
        tr.append(tr_i)

    def smooth(series: List[Number]) -> List[Optional[Number]]:
        out: List[Optional[Number]] = []
        window_sum = 0.0
        window: List[Number] = []
        for v in series:
            window.append(v)
            window_sum += v
            if len(window) > period:
                window_sum -= window.pop(0)
            if len(window) < period:
                out.append(None)
            else:
                out.append(window_sum)
        return out

    tr_s = smooth(tr)
    pdm_s = smooth(plus_dm)
    mdm_s = smooth(minus_dm)

    plus_di: List[Optional[Number]] = []
    minus_di: List[Optional[Number]] = []
    dx: List[Optional[Number]] = []

    for t, p, m in zip(tr_s, pdm_s, mdm_s):
        if t is None or t == 0:
            plus_di.append(None)
            minus_di.append(None)
            dx.append(None)
            continue

        pdi = 100.0 * (p / t) if p is not None else None
        mdi = 100.0 * (m / t) if m is not None else None

        plus_di.append(pdi)
        minus_di.append(mdi)

        if pdi is None or mdi is None or (pdi + mdi) == 0:
            dx.append(None)
        else:
            dx.append(100.0 * abs(pdi - mdi) / (pdi + mdi))

    # ADX is SMA of DX
    adx: List[Optional[Number]] = []
    window: List[Number] = []
    for v in dx:
        if v is None:
            adx.append(None)
            continue
        window.append(v)
        if len(window) > period:
            window.pop(0)
        if len(window) < period:
            adx.append(None)
        else:
            adx.append(sum(window) / len(window))

    return plus_di, minus_di, adx
