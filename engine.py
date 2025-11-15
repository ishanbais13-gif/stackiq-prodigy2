# engine.py
from __future__ import annotations

from typing import Dict, Any, List, Optional, Tuple
from math import floor, tanh
from statistics import mean
from datetime import datetime, timezone

import data_fetcher as df


# -----------------------------
# Small helpers
# -----------------------------


def _last(series: List[Optional[float]]) -> Optional[float]:
    """Return last non-None value in a sequence."""
    for v in reversed(series):
        if v is not None:
            return v
    return None


def _safe_div(a: float, b: float) -> Optional[float]:
    if b is None or b == 0:
        return None
    return a / b


# -----------------------------
# Technical indicators (no external deps)
# -----------------------------


def sma(values: List[float], window: int) -> Optional[float]:
    if len(values) < window:
        return None
    return mean(values[-window:])


def ema(values: List[float], window: int) -> Optional[float]:
    if len(values) < window or window <= 0:
        return None
    k = 2 / (window + 1.0)
    ema_val = values[0]
    for v in values[1:]:
        ema_val = v * k + ema_val * (1 - k)
    return ema_val


def rsi(values: List[float], window: int = 14) -> Optional[float]:
    if len(values) <= window:
        return None

    gains: List[float] = []
    losses: List[float] = []

    for i in range(1, len(values)):
        diff = values[i] - values[i - 1]
        if diff > 0:
            gains.append(diff)
            losses.append(0.0)
        else:
            gains.append(0.0)
            losses.append(-diff)

    if len(gains) < window:
        return None

    avg_gain = mean(gains[-window:])
    avg_loss = mean(losses[-window:])

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1 + rs))


def atr(high: List[float], low: List[float], close: List[float], window: int = 14) -> Optional[float]:
    if len(close) < window + 1:
        return None

    trs: List[float] = []
    for i in range(1, len(close)):
        h = high[i]
        l = low[i]
        prev_close = close[i - 1]
        tr = max(
            h - l,
            abs(h - prev_close),
            abs(l - prev_close),
        )
        trs.append(tr)

    if len(trs) < window:
        return None

    return mean(trs[-window:])


def macd(values: List[float], fast: int = 12, slow: int = 26, signal: int = 9) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    if len(values) < slow + signal:
        return None, None, None

    ema_fast = []
    ema_slow = []

    # build EMA series
    k_fast = 2 / (fast + 1.0)
    k_slow = 2 / (slow + 1.0)
    e_fast = values[0]
    e_slow = values[0]
    for v in values:
        e_fast = v * k_fast + e_fast * (1 - k_fast)
        e_slow = v * k_slow + e_slow * (1 - k_slow)
        ema_fast.append(e_fast)
        ema_slow.append(e_slow)

    macd_line = [f - s for f, s in zip(ema_fast, ema_slow)]
    sig_line = []

    k_sig = 2 / (signal + 1.0)
    e_sig = macd_line[0]
    for v in macd_line:
        e_sig = v * k_sig + e_sig * (1 - k_sig)
        sig_line.append(e_sig)

    hist = macd_line[-1] - sig_line[-1]
    return macd_line[-1], sig_line[-1], hist


def bollinger_percent(values: List[float], window: int = 20, num_std: float = 2.0) -> Optional[float]:
    if len(values) < window:
        return None
    window_vals = values[-window:]
    m = mean(window_vals)
    if window <= 1:
        return None
    variance = sum((x - m) ** 2 for x in window_vals) / (window - 1)
    std = variance ** 0.5
    if std == 0:
        return None
    upper = m + num_std * std
    lower = m - num_std * std
    price = values[-1]
    return (price - lower) / (upper - lower)  # 0..1 roughly


def dmi_adx(high: List[float], low: List[float], close: List[float], window: int = 14) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """
    Very lightweight DMI/ADX implementation.
    Returns +DI, -DI, ADX (all 0-100).
    """
    if len(close) <= window + 1:
        return None, None, None

    plus_dm: List[float] = []
    minus_dm: List[float] = []
    tr_list: List[float] = []

    for i in range(1, len(close)):
        up_move = high[i] - high[i - 1]
        down_move = low[i - 1] - low[i]

        plus = max(up_move, 0.0) if up_move > down_move else 0.0
        minus = max(down_move, 0.0) if down_move > up_move else 0.0

        prev_close = close[i - 1]
        tr = max(
            high[i] - low[i],
            abs(high[i] - prev_close),
            abs(low[i] - prev_close),
        )

        plus_dm.append(plus)
        minus_dm.append(minus)
        tr_list.append(tr)

    if len(tr_list) < window:
        return None, None, None

    avg_tr = mean(tr_list[-window:])
    if avg_tr == 0:
        return None, None, None

    plus_di = 100.0 * mean(plus_dm[-window:]) / avg_tr
    minus_di = 100.0 * mean(minus_dm[-window:]) / avg_tr

    dx_list: List[float] = []
    for p, m in zip(plus_dm[-window:], minus_dm[-window:]):
        if p + m == 0:
            dx_list.append(0.0)
        else:
            dx_list.append(100.0 * abs(p - m) / (p + m))

    adx = mean(dx_list) if dx_list else 0.0
    return plus_di, minus_di, adx


# -----------------------------
# Feature builder
# -----------------------------


def build_features(symbol: str) -> Dict[str, Any]:
    """
    Build a feature dictionary for a single symbol using Finnhub candles,
    recommendations, news sentiment and earnings.
    """
    raw = df.candles(symbol, days=260)
    c = raw["c"]
    h = raw["h"]
    l = raw["l"]

    price = c[-1]

    sma20 = sma(c, 20)
    sma50 = sma(c, 50)
    sma200 = sma(c, 200)
    rsi14 = rsi(c, 14)
    atr14 = atr(h, l, c, 14)
    macd_val, macd_sig, macd_hist = macd(c)
    bbp = bollinger_percent(c, 20, 2.0)
    pdi, mdi, adx_val = dmi_adx(h, l, c, 14)

    # Momentum windows (% returns)
    def window_ret(n: int) -> Optional[float]:
        if len(c) < n + 1:
            return None
        past = c[-n - 1]
        if past == 0:
            return None
        return (c[-1] / past) - 1.0

    r5 = window_ret(5)
    r20 = window_ret(20)
    r60 = window_ret(60)

    # Finnhub extras
    rec = df.recommendation_trends(symbol) or {}
    total = (rec.get("strongBuy") or 0) + (rec.get("buy") or 0) + (rec.get("hold") or 0) + (rec.get("sell") or 0) + (
        rec.get("strongSell") or 0
    )
    if total:
        buy_score = (rec.get("strongBuy") or 0) * 2 + (rec.get("buy") or 0)
        sell_score = (rec.get("strongSell") or 0) * 2 + (rec.get("sell") or 0)
        rec_bias = (buy_score - sell_score) / max(total, 1)
    else:
        rec_bias = None

    news = df.news_sentiment(symbol) or {}
    bullish = news.get("bullishPercent") or 0.0
    news_bias = (bullish / 100.0 - 0.5) * 2.0  # map [0,100] -> [-1,1]

    earn = df.earnings_calendar(symbol) or {}
    upcoming_earn = False
    try:
        dt_str = earn.get("date")
        if dt_str:
            dt = datetime.strptime(dt_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            days = (dt - datetime.now(timezone.utc)).days
            upcoming_earn = days >= 0 and days <= 7
    except Exception:
        pass

    feats: Dict[str, Any] = {
        "price": price,
        "sma20": sma20,
        "sma50": sma50,
        "sma200": sma200,
        "rsi14": rsi14,
        "atr14": atr14,
        "macd": macd_val,
        "macd_sig": macd_sig,
        "macd_hist": macd_hist,
        "bbp": bbp,
        "pdi": pdi,
        "mdi": mdi,
        "adx": adx_val,
        "r5": r5,
        "r20": r20,
        "r60": r60,
        "rec_bias": rec_bias,
        "news_bias": news_bias,
        "upcoming_earnings": upcoming_earn,
    }

    return feats


# -----------------------------
# Signal construction
# -----------------------------


def _score_from_features(f: Dict[str, Any]) -> Tuple[float, List[str]]:
    """
    Combine features into a 0-100 confidence score and human-readable
    bullet points.
    """
    bullets: List[str] = []
    score = 0.0

    price = f["price"]
    sma20 = f.get("sma20")
    sma50 = f.get("sma50")
    sma200 = f.get("sma200")
    rsi14 = f.get("rsi14")
    bbp = f.get("bbp")
    macd_hist = f.get("macd_hist")
    rec_bias = f.get("rec_bias")
    news_bias = f.get("news_bias")
    adx_val = f.get("adx")
    r5 = f.get("r5")
    r20 = f.get("r20")
    r60 = f.get("r60")

    # Trend vs SMA
    if sma20 and price > sma20:
        score += 0.8
        bullets.append(f"Price above 20-day SMA ({price:.2f} > {sma20:.2f}).")
    elif sma20 and price < sma20:
        score -= 0.8
        bullets.append(f"Price below 20-day SMA ({price:.2f} < {sma20:.2f}).")

    if sma50 and sma20 and sma20 > sma50:
        score += 0.6
        bullets.append("Short-term trend stronger than 50-day trend.")
    if sma200 and price > sma200:
        score += 0.7
        bullets.append("Trading above 200-day long-term trend.")
    elif sma200 and price < sma200:
        score -= 0.7
        bullets.append("Trading below 200-day long-term trend.")

    # RSI
    if rsi14 is not None:
        if 30 <= rsi14 <= 70:
            score += 0.4
            bullets.append(f"RSI neutral ({rsi14:.1f}), no extreme overbought/oversold.")
        elif rsi14 < 30:
            score += 0.6
            bullets.append(f"RSI oversold ({rsi14:.1f}) — potential bounce zone.")
        elif rsi14 > 70:
            score -= 0.6
            bullets.append(f"RSI overbought ({rsi14:.1f}) — potential pullback zone.")

    # MACD histogram
    if macd_hist is not None:
        if macd_hist > 0:
            score += 0.5
            bullets.append("MACD histogram positive — upside momentum.")
        else:
            score -= 0.5
            bullets.append("MACD histogram negative — downside momentum.")

    # Bollinger Bands %B
    if bbp is not None:
        if bbp < 0.1:
            score += 0.4
            bullets.append("Price near lower Bollinger Band — mean-reversion upside.")
        elif bbp > 0.9:
            score -= 0.4
            bullets.append("Price near upper Bollinger Band — mean-reversion downside.")

    # Short/mid/long momentum
    for label, r in [("5-day", r5), ("20-day", r20), ("60-day", r60)]:
        if r is None:
            continue
        if r > 0:
            score += 0.3
            bullets.append(f"{label} momentum positive ({r*100:.1f}%).")
        else:
            score -= 0.3
            bullets.append(f"{label} momentum negative ({r*100:.1f}%).")

    # Recommendations & news
    if rec_bias is not None:
        score += rec_bias * 0.8
        if rec_bias > 0:
            bullets.append("Analyst recommendations skew bullish.")
        elif rec_bias < 0:
            bullets.append("Analyst recommendations skew bearish.")

    if news_bias is not None:
        score += news_bias * 0.5
        if news_bias > 0:
            bullets.append("Recent news sentiment bullish.")
        elif news_bias < 0:
            bullets.append("Recent news sentiment bearish.")

    # Trend strength filter
    if adx_val is not None and adx_val < 15:
        score *= 0.7
        bullets.append("Trend strength (ADX) is weak; conviction reduced.")

    # Normalize score into 0-100
    raw = tanh(score)  # -1..1
    conf = (raw + 1.0) * 50.0
    return conf, bullets


def _position_plan(f: Dict[str, Any], budget: float) -> Dict[str, Any]:
    """Turn features into entry/target/stop/shares."""
    price = f["price"]
    atr_val = f.get("atr14")

    # Basic price levels using ATR if available
    if atr_val is not None:
        entry = price
        target = price + 2.0 * atr_val
        stop = price - 1.5 * atr_val
    else:
        entry = price
        target = price * 1.08
        stop = price * 0.94

    risk_per_share = max(entry - stop, 0.01)
    max_risk_dollars = budget * 0.02  # risk 2% of budget per trade
    shares_by_risk = floor(max_risk_dollars / risk_per_share)
    shares_by_budget = floor(budget / entry)
    shares = max(0, min(shares_by_budget, shares_by_risk))

    return {
        "entry": entry,
        "target": target,
        "stop": stop,
        "atr": atr_val,
        "shares": shares,
    }


# -----------------------------
# Public API
# -----------------------------


def predict(symbol: str, budget: float) -> Dict[str, Any]:
    """
    Main single-symbol prediction entry point.
    Returns price, signal, confidence, levels and rationale bullets.
    """
    symbol = symbol.upper()
    feats = build_features(symbol)
    conf, bullets = _score_from_features(feats)
    plan = _position_plan(feats, budget)

    # simple signal based on confidence
    if conf >= 65:
        signal = "BUY"
    elif conf <= 35:
        signal = "SELL"
    else:
        signal = "HOLD"

    entry = plan["entry"]
    target = plan["target"]
    stop = plan["stop"]
    atr_val = plan["atr"]
    shares = plan["shares"]
    budget_used = round(shares * entry, 2)

    return {
        "symbol": symbol,
        "price": round(feats["price"], 4),
        "signal": signal,
        "confidence": round(conf, 1),
        "entry": round(entry, 4),
        "target": round(target, 4),
        "stop": round(stop, 4),
        "atr": round(atr_val, 4) if atr_val is not None else None,
        "shares": shares,
        "budget_used": budget_used,
        "rationale": bullets[:8],
        "features": feats,
    }


def predict_batch(symbols: List[str], budget: float) -> List[Dict[str, Any]]:
    """
    Convenience helper for batch predictions.
    """
    return [predict(sym, budget) for sym in symbols]

