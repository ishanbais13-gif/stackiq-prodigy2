"""
Lightweight technical indicator engine for StackIQ.

IMPORTANT:
Because of Finnhub plan limits (403 on /stock/candle), this module is designed
to work ONLY with the real-time quote data you already have:

    c  = current price
    pc = previous close
    dp = percent change today
    d  = dollar change today
    h  = intraday high
    l  = intraday low
    o  = intraday open

These are NOT "true" multi-day RSI/EMA/MACD values. They are pseudo-indicators
built from single-day behavior that are:

- deterministic
- cheap
- always available on your plan
- good enough to rank stocks relative to each other

You can replace this module with real multi-day indicators later when you have
historical data access.
"""

from typing import Any, Dict, Optional


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def compute_indicators(
    price: float,
    change_pct: Optional[float],
    quote: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Compute pseudo-technical indicators based on the current quote.

    Returns a dict with:
      - rsi
      - ema_fast
      - ema_slow
      - macd_hist
      - volatility_score_numeric
      - volatility_label
      - volume_spike (bool)
      - indicator_score (0–100)
      - indicator_trend_label
    """
    # ---------- Base inputs ----------
    cp = change_pct if change_pct is not None else 0.0

    high = quote.get("h") or price
    low = quote.get("l") or price
    prev_close = quote.get("pc") or price

    # Day range in %
    day_range_pct = 0.0
    if price > 0 and high is not None and low is not None:
        day_range_pct = abs(high - low) / price * 100.0

    # ---------- Pseudo RSI ----------
    # Map today's percent change into a 0–100 scale.
    # +10% -> ~100, -10% -> ~0, small moves -> around 50.
    rsi = 50.0 + cp * 5.0
    rsi = _clamp(rsi, 0.0, 100.0)

    # ---------- Pseudo EMAs ----------
    # These are NOT real EMAs. They are "tilted" prices that respond
    # more or less strongly to today's move.
    # This is enough to create a trend-like signal for ranking.
    ema_fast = price * (1.0 + cp / 200.0)  # more sensitive
    ema_slow = price * (1.0 + cp / 400.0)  # slower
    macd_hist = ema_fast - ema_slow

    # ---------- Volatility score ----------
    # Combine absolute percent change and intraday range.
    raw_vol = abs(cp) * 3.0 + day_range_pct  # simple linear combo
    raw_vol = _clamp(raw_vol, 0.0, 100.0)

    if raw_vol < 5:
        vol_label = "low"
    elif raw_vol < 15:
        vol_label = "medium"
    else:
        vol_label = "high"

    # ---------- Volume spike (approximate) ----------
    # We don't have real volume on the quote endpoint, so we approximate:
    # big move or wide intraday range => likely high activity.
    volume_spike = abs(cp) > 4.0 or day_range_pct > 8.0

    # ---------- Indicator score ----------
    # Start neutral and adjust based on RSI + direction + volatility
    score = 50.0

    # RSI contribution
    if 45 <= rsi <= 55:
        score += 5  # healthy, balanced
    elif 55 < rsi <= 70:
        score += 10  # bullish momentum
    elif rsi > 70:
        score -= 5  # overbought zone
    elif 30 <= rsi < 45:
        score -= 5  # mild weakness
    else:  # rsi < 30
        score -= 10  # oversold / weak

    # Direction contribution
    if cp > 1.0:
        score += 5
    elif cp < -1.0:
        score -= 5

    # Volatility contribution: very high vol is risky
    if vol_label == "high":
        score -= 5
    elif vol_label == "low":
        score += 2

    score = _clamp(score, 0.0, 100.0)

    # ---------- Trend label ----------
    if score >= 80:
        trend_label = "strong_bullish"
    elif score >= 65:
        trend_label = "bullish"
    elif score >= 50:
        trend_label = "slightly_bullish"
    elif score >= 35:
        trend_label = "neutral_or_choppy"
    elif score >= 20:
        trend_label = "bearish"
    else:
        trend_label = "strong_bearish"

    return {
        "rsi": rsi,
        "ema_fast": ema_fast,
        "ema_slow": ema_slow,
        "macd_hist": macd_hist,
        "volatility_score_numeric": raw_vol,
        "volatility_label": vol_label,
        "volume_spike": volume_spike,
        "indicator_score": score,
        "indicator_trend_label": trend_label,
        "day_range_pct": day_range_pct,
        "base_change_pct": cp,
        "prev_close": prev_close,
    }

