# engine.py
from typing import Dict, Any, List, Optional, Tuple
from math import floor, tanh
import data_fetcher as df
import indicators as ta

def _last(series: List[Optional[float]]) -> Optional[float]:
    for v in reversed(series):
        if v is not None: return v
    return None

def build_features(symbol: str) -> Dict[str, Any]:
    raw = df.candles(symbol, days=260)
    c = raw["c"]; h = raw["h"]; l = raw["l"]
    price = c[-1]

    sma20 = ta.sma(c, 20); sma50 = ta.sma(c, 50); sma200 = ta.sma(c, 200)
    rsi14 = ta.rsi(c, 14)
    atr14  = ta.atr(h, l, c, 14)
    macd_l, macd_s, macd_h = ta.macd(c)
    bbp = ta.bollinger_percent(c, 20, 2.0)
    pdi, mdi, adx = ta.dmi_adx(h,l,c,14)

    # Momentum windows
    def window_ret(n: int):
        if len(c) <= n: return None
        return (c[-1] - c[-1-n]) / c[-1-n]

    r5  = window_ret(5); r20 = window_ret(20); r60 = window_ret(60)

    # Finnhub extras (current snapshot proxies)
    rec = df.recommendation_trends(symbol) or {}
    buy = (rec.get("strongBuy") or 0) + (rec.get("buy") or 0)
    sell = (rec.get("strongSell") or 0) + (rec.get("sell") or 0)
    hold = (rec.get("hold") or 0)
    total = buy + sell + hold
    rec_bias = None if total == 0 else (buy - sell) / max(total, 1)

    news = df.news_sentiment(symbol) or {}
    bullish = news.get("bullishPercent") or news.get("sentiment", {}).get("bullishPercent")
    news_bias = (bullish/100.0 - 0.5)*2.0 if bullish is not None else None  # [-1..1]

    # Next 7d earnings risk
    earn = df.earnings_calendar(symbol) or {}
    upcoming_earn = False
    if earn.get("date"):
        from datetime import datetime, timezone
        try:
            dt = datetime.strptime(earn["date"], "%Y-%m-%d").replace(tzinfo=timezone.utc)
            upcoming_earn = (dt - datetime.now(timezone.utc)).days <= 7
        except Exception:
            pass

    feats = {
        "price": price,
        "sma20": _last(sma20), "sma50": _last(sma50), "sma200": _last(sma200),
        "rsi14": _last(rsi14),
        "atr": _last(atr14),
        "macd": _last(macd_l), "macd_sig": _last(macd_s), "macd_hist": _last(macd_h),
        "bbp": _last(bbp),
        "pdi": _last(pdi), "mdi": _last(mdi), "adx": _last(adx),
        "r5": r5, "r20": r20, "r60": r60,
        "rec_bias": rec_bias, "news_bias": news_bias,
        "upcoming_earnings": upcoming_earn
    }
    # Distances & ATR%
    for k in ("sma20","sma50","sma200"):
        feats[f"dist_{k}"] = ((feats["price"] - feats[k]) / feats[k]) if feats.get(k) else None
    feats["atrp"] = (feats["atr"] / feats["price"]) if feats.get("atr") else None
    return feats

# ----- SIGNAL COMPONENTS ------------------------------------------------------

def s_momentum(f: Dict[str, Any]) -> Tuple[float, List[str]]:
    w5, w20, w60 = 0.5, 0.35, 0.15
    mom = 0.0
    for w, key in [(w5,"r5"),(w20,"r20"),(w60,"r60")]:
        if f.get(key) is not None:
            mom += w * f[key]
    txt = [f"Momentum 5/20/60 = { (f.get('r5') or 0)*100:.1f}% / {(f.get('r20') or 0)*100:.1f}% / {(f.get('r60') or 0)*100:.1f}%"]
    mom = max(-0.25, min(0.25, mom))
    return mom, txt

def s_trend(f: Dict[str, Any]) -> Tuple[float, List[str]]:
    up = 0.0
    for k,w in [("dist_sma20",0.3),("dist_sma50",0.35),("dist_sma200",0.35)]:
        x = f.get(k)
        if x is not None: up += w * (-x)
    if f.get("macd") and f.get("macd_sig"):
        up += 0.15 * (1.0 if f["macd"]>f["macd_sig"] else -1.0)
    t = [f"Trend dist to SMAs: 20:{(f.get('dist_sma20') or 0)*100:.1f}% 50:{(f.get('dist_sma50') or 0)*100:.1f}% 200:{(f.get('dist_sma200') or 0)*100:.1f}%"]
    return max(-1, min(1, up)), t

def s_mean_reversion(f: Dict[str, Any]) -> Tuple[float, List[str]]:
    s = 0.0; t=[]
    rsi = f.get("rsi14"); bbp = f.get("bbp")
    if rsi is not None:
        if rsi < 30: s += 0.6
        elif rsi > 70: s -= 0.6
        t.append(f"RSI={rsi:.0f}")
    if bbp is not None:
        s += 0.4 * (0.5 - bbp)
        t.append(f"BB%={bbp:.2f}")
    return max(-1, min(1, s)), t

def s_breakout(f: Dict[str, Any]) -> Tuple[float, List[str]]:
    s = 0.0; t=[]
    adx = f.get("adx") or 0
    hist = f.get("macd_hist") or 0
    if adx >= 20: s += 0.2
    if hist > 0: s += 0.2
    t.append(f"ADX={adx:.1f} MACD_hist={hist:.3f}")
    return max(-1,min(1,s)), t

def s_altdata(f: Dict[str, Any]) -> Tuple[float, List[str]]:
    s = 0.0; t=[]
    if f.get("news_bias") is not None:
        s += 0.25 * f["news_bias"]
        t.append(f"News bias={f['news_bias']:+.2f}")
    if f.get("rec_bias") is not None:
        s += 0.25 * f["rec_bias"]
        t.append(f"Recs bias={f['rec_bias']:+.2f}")
    return max(-1,min(1,s)), t

def s_volatility_guard(f: Dict[str, Any]) -> Tuple[float, List[str]]:
    atrp = f.get("atrp") or 0
    if 0.01 <= atrp <= 0.05: bonus = 0.15
    elif atrp > 0.08: bonus = -0.25
    else: bonus = 0.0
    return bonus, [f"ATR%={atrp*100:.1f}%"]

def s_event_risk(f: Dict[str, Any]) -> Tuple[float, List[str]]:
    return (-0.35 if f.get("upcoming_earnings") else 0.0), (["Earnings < 7 days"] if f.get("upcoming_earnings") else [])

COMP_WEIGHTS = {
    "momentum": 0.27,
    "trend": 0.18,
    "meanrev": 0.20,
    "breakout": 0.12,
    "altdata": 0.15,
    "vol": 0.05,
    "event": 0.03
}

def ensemble_score(f: Dict[str, Any]) -> Tuple[float, List[str]]:
    parts = []
    s1,t1 = s_momentum(f); parts.append(("momentum", s1, t1))
    s2,t2 = s_trend(f); parts.append(("trend", s2, t2))
    s3,t3 = s_mean_reversion(f); parts.append(("meanrev", s3, t3))
    s4,t4 = s_breakout(f); parts.append(("breakout", s4, t4))
    s5,t5 = s_altdata(f); parts.append(("altdata", s5, t5))
    s6,t6 = s_volatility_guard(f); parts.append(("vol", s6, t6))
    s7,t7 = s_event_risk(f); parts.append(("event", s7, t7))

    raw = 0.0; bullets=[]
    for name, val, notes in parts:
        w = COMP_WEIGHTS[name]
        raw += w * val
        bullets.append(f"{name}: {val:+.3f} (w={w}) | " + " ; ".join(notes))
    conf = (tanh(raw*1.5)+1)/2 * 100.0
    return conf, bullets

def position_plan(f: Dict[str, Any], budget: float, buy: float = 67.0, sell: float = 33.0) -> Dict[str, Any]:
    price = f["price"]; atr = f.get("atr") or 0.0; atrp = f.get("atrp") or 0.0
    conf, notes = ensemble_score(f)
    if conf >= buy: signal = "BUY"
    elif conf <= sell: signal = "SELL"
    else: signal = "HOLD"
    if atrp <= 0.01: R = 0.75
    elif atrp <= 0.03: R = 1.0
    elif atrp <= 0.06: R = 1.2
    else: R = 1.5
    entry = price
    target = price + R*atr if signal != "SELL" else price - R*atr
    stop   = price - 1.0*atr if signal != "SELL" else price + 1.0*atr

    edge = max(-0.25, min(0.25, (conf - 50.0)/100.0))
    kelly_f = max(0.0, min(0.1, 2*edge))
    risk_cap = max(0.01 * budget, 10.0)
    stop_dist = abs(entry - stop) or (0.02*price)
    shares_kelly = int((kelly_f * budget) // price) if price>0 else 0
    shares_risk  = int(risk_cap // stop_dist) if stop_dist>0 else 0
    shares_budget= int(budget // price) if price>0 else 0
    shares = max(0, min(shares_kelly, shares_risk, shares_budget))

    return {
        "signal": signal,
        "confidence": round(conf,1),
        "entry": round(entry,4),
        "target": round(target,4),
        "stop": round(stop,4),
        "atr": round(atr,4) if atr else None,
        "shares": shares,
        "budget_used": round(shares * entry, 2),
        "rationale": notes[:8]
    }

def predict(symbol: str, budget: float) -> Dict[str, Any]:
    f = build_features(symbol)
    plan = position_plan(f, budget)
    return {"symbol": symbol.upper(), "price": round(f["price"],4), "features": f, **plan}
