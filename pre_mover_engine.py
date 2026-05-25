from __future__ import annotations

import math
from typing import Any, Dict, List, Optional, Tuple


def _safe_f(v: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        x = float(v)
    except Exception:
        return default
    if not math.isfinite(x):
        return default
    return float(x)


def _clamp(v: Any, lo: float, hi: float) -> float:
    x = _safe_f(v, lo)
    if x is None:
        x = lo
    if x < lo:
        x = lo
    if x > hi:
        x = hi
    return float(x)


def _clamp01(v: Any) -> float:
    return _clamp(v, 0.0, 1.0)


def _series(bars: Any, key: str, limit: int = 220) -> List[float]:
    if not isinstance(bars, list):
        return []
    out: List[float] = []
    for b in bars[-max(1, int(limit)):]:
        if not isinstance(b, dict):
            continue
        v = _safe_f(b.get(key))
        if v is None:
            continue
        out.append(float(v))
    return out


def _atr_from_ohlc(highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> Optional[float]:
    if not highs or not lows or not closes:
        return None
    if len(highs) != len(lows) or len(lows) != len(closes):
        return None
    if len(closes) < 3:
        return None
    trs: List[float] = []
    for i in range(1, len(closes)):
        h = float(highs[i])
        l = float(lows[i])
        pc = float(closes[i - 1])
        tr = max(h - l, abs(h - pc), abs(l - pc))
        trs.append(float(tr))
    if not trs:
        return None
    n = max(1, int(period))
    tail = trs[-n:] if len(trs) >= n else trs
    if not tail:
        return None
    return float(sum(tail) / float(len(tail)))


def _roc(closes: List[float], lookback: int) -> Optional[float]:
    if not closes or len(closes) <= int(lookback):
        return None
    try:
        a = float(closes[-(int(lookback) + 1)])
        b = float(closes[-1])
        if a <= 0:
            return None
        return float((b - a) / a)
    except Exception:
        return None


def _daily_pct_change(snapshot: Dict[str, Any], closes: List[float]) -> float:
    try:
        db = snapshot.get("dailyBar") if isinstance(snapshot.get("dailyBar"), dict) else {}
        pb = snapshot.get("prevDailyBar") if isinstance(snapshot.get("prevDailyBar"), dict) else {}
        c = _safe_f(db.get("c"))
        pc = _safe_f(pb.get("c"))
        if c is not None and pc is not None and float(pc) > 0:
            return float((float(c) - float(pc)) / float(pc))
    except Exception:
        pass
    try:
        if len(closes) >= 2 and float(closes[-2]) > 0:
            return float((float(closes[-1]) - float(closes[-2])) / float(closes[-2]))
    except Exception:
        pass
    return 0.0


def _intraday_vwap_reclaim(intraday_bars: List[Dict[str, Any]]) -> Dict[str, Any]:
    out = {
        "hit": False,
        "vwap": None,
        "close_above_vwap": False,
        "reclaim_vol_ratio": 0.0,
        "held_after_reclaim": False,
    }
    if not isinstance(intraday_bars, list) or len(intraday_bars) < 30:
        return out

    closes: List[float] = []
    vols: List[float] = []
    vwaps: List[float] = []

    num = 0.0
    den = 0.0
    for b in intraday_bars[-390:]:
        if not isinstance(b, dict):
            continue
        c = _safe_f(b.get("c"))
        v = _safe_f(b.get("v"), 0.0)
        if c is None or v is None or float(v) <= 0:
            continue
        num += float(c) * float(v)
        den += float(v)
        closes.append(float(c))
        vols.append(float(v))
        vwaps.append(float(num / den) if den > 0 else float(c))

    if len(closes) < 30 or len(closes) != len(vwaps):
        return out

    reclaim_idx = None
    was_below = False
    for i in range(1, len(closes)):
        if closes[i - 1] < vwaps[i - 1]:
            was_below = True
        crossed = closes[i - 1] <= vwaps[i - 1] and closes[i] > vwaps[i]
        if was_below and crossed:
            reclaim_idx = i
            break

    close_above = bool(closes[-1] > vwaps[-1])
    out["vwap"] = float(vwaps[-1])
    out["close_above_vwap"] = bool(close_above)

    if reclaim_idx is None:
        return out

    try:
        start = max(0, reclaim_idx - 20)
        avg_vol = sum(vols[start:reclaim_idx]) / float(max(1, reclaim_idx - start))
        reclaim_vol = float(vols[reclaim_idx])
        vol_ratio = float(reclaim_vol / avg_vol) if avg_vol > 0 else 0.0
    except Exception:
        vol_ratio = 0.0
    out["reclaim_vol_ratio"] = float(round(vol_ratio, 3))

    held = True
    try:
        for j in range(reclaim_idx, len(closes)):
            if closes[j] < (vwaps[j] * 0.997):
                held = False
                break
    except Exception:
        held = False
    out["held_after_reclaim"] = bool(held)

    out["hit"] = bool(close_above and held and vol_ratio >= 1.1)
    return out


def _approx_last_price(snapshot: Dict[str, Any], closes: List[float]) -> Optional[float]:
    try:
        lt = snapshot.get("latestTrade") if isinstance(snapshot.get("latestTrade"), dict) else {}
        p = _safe_f(lt.get("p"))
        if p is not None and p > 0:
            return float(p)
    except Exception:
        pass
    try:
        db = snapshot.get("dailyBar") if isinstance(snapshot.get("dailyBar"), dict) else {}
        p = _safe_f(db.get("c"))
        if p is not None and p > 0:
            return float(p)
    except Exception:
        pass
    try:
        if closes:
            p = float(closes[-1])
            if p > 0:
                return p
    except Exception:
        pass
    return None


def _extract_catalyst_text(news: Dict[str, Any]) -> str:
    parts: List[str] = []
    try:
        cats = news.get("catalysts") if isinstance(news.get("catalysts"), list) else []
        parts.extend([str(x) for x in cats if str(x).strip()])
    except Exception:
        pass
    try:
        kc = news.get("key_catalysts") if isinstance(news.get("key_catalysts"), list) else []
        parts.extend([str(x) for x in kc if str(x).strip()])
    except Exception:
        pass
    try:
        parts.append(str(news.get("summary") or ""))
    except Exception:
        pass
    return " ".join(parts).lower()


def compute_pre_mover_score(
    symbol: str,
    daily_bars: List[Dict[str, Any]],
    intraday_bars: List[Dict[str, Any]],
    snapshot: Optional[Dict[str, Any]],
    news: Optional[Dict[str, Any]],
    spy_bars: Optional[List[Dict[str, Any]]],
) -> Dict[str, Any]:
    """High-accuracy next-session pre-mover score in [0,100]."""
    sym = str(symbol or "").strip().upper()
    out: Dict[str, Any] = {
        "symbol": sym,
        "score": 0,
        "confidence": 0.0,
        "signals": {},
        "top_signals": [],
        "signal_count": 0,
        "interpretation": "insufficient_data",
        "rs_vs_spy_pct": 0.0,
        "predicted_move_pct": 0.0,
        "entry_zone": {},
        "invalidation": None,
        "time_to_watch": "9:35-10:15 AM ET tomorrow",
    }

    try:
        bars = daily_bars if isinstance(daily_bars, list) else []
        intra = intraday_bars if isinstance(intraday_bars, list) else []
        snap = snapshot if isinstance(snapshot, dict) else {}
        news0 = news if isinstance(news, dict) else {}
        spy = spy_bars if isinstance(spy_bars, list) else []

        closes = _series(bars, "c", 240)
        highs = _series(bars, "h", 240)
        lows = _series(bars, "l", 240)
        vols = _series(bars, "v", 240)
        spy_closes = _series(spy, "c", 120)

        if len(closes) < 20 or len(highs) < 20 or len(lows) < 20:
            return out

        px = _approx_last_price(snap, closes)
        if px is None or px <= 0:
            return out

        weights = {
            "volume_coil_breakout": 22.0,
            "rs_vs_spy": 18.0,
            "vwap_reclaim_hold": 15.0,
            "close_top_15pct": 13.0,
            "news_catalyst_unpriced": 12.0,
            "base_breakout_setup": 10.0,
            "institutional_footprint": 6.0,
            "gap_reclaim_pattern": 4.0,
        }

        signal_rows: Dict[str, Dict[str, Any]] = {}

        # 1) Volume coil + breakout setup (22%)
        vc_hit = False
        vc_strength = 0.0
        try:
            prev5 = vols[-6:-1] if len(vols) >= 6 else []
            today_vol = float(vols[-1]) if vols else 0.0
            avg5 = (sum(prev5) / float(len(prev5))) if prev5 else 0.0
            contraction = False
            if len(prev5) >= 4:
                contraction = bool(prev5[-1] <= prev5[0] * 0.90)
            expansion = bool(avg5 > 0 and today_vol > (avg5 * 1.5))

            tr_all: List[float] = []
            for i in range(1, len(closes)):
                tr_all.append(
                    max(
                        float(highs[i]) - float(lows[i]),
                        abs(float(highs[i]) - float(closes[i - 1])),
                        abs(float(lows[i]) - float(closes[i - 1])),
                    )
                )
            atr_shrinking = False
            if len(tr_all) >= 10:
                atr_prev = sum(tr_all[-10:-5]) / 5.0
                atr_now = sum(tr_all[-5:]) / 5.0
                atr_shrinking = bool(atr_now <= atr_prev * 0.92)

            vc_hit = bool(contraction and expansion and atr_shrinking)
            if vc_hit:
                exp_ratio = float(today_vol / avg5) if avg5 > 0 else 1.0
                vc_strength = _clamp01((exp_ratio - 1.5) / 1.5)
            signal_rows["volume_coil_breakout"] = {
                "hit": vc_hit,
                "weight": weights["volume_coil_breakout"],
                "strength": float(round(vc_strength, 4)),
                "details": {
                    "contraction": bool(contraction),
                    "expansion": bool(expansion),
                    "atr_shrinking": bool(atr_shrinking),
                    "today_vs_5d_vol": float(round((today_vol / avg5), 3)) if avg5 > 0 else None,
                },
            }
        except Exception:
            signal_rows["volume_coil_breakout"] = {
                "hit": False,
                "weight": weights["volume_coil_breakout"],
                "strength": 0.0,
                "details": {},
            }

        # 2) Relative strength vs SPY over 5 days (18%)
        rs_hit = False
        rs_strength = 0.0
        rs_spread = 0.0
        stock_roc5 = _roc(closes, 5)
        spy_roc5 = _roc(spy_closes, 5)
        try:
            sr = float(stock_roc5 or 0.0)
            pr = float(spy_roc5 or 0.0)
            rs_spread = float(sr - pr)
            rs_hit = bool(rs_spread > 0.03)
            base = _clamp01((rs_spread - 0.03) / 0.07)
            bonus = 0.15 if (sr > 0 and pr <= 0) else 0.0
            rs_strength = _clamp01(base + bonus)
        except Exception:
            rs_hit = False
            rs_strength = 0.0
            rs_spread = 0.0
        signal_rows["rs_vs_spy"] = {
            "hit": rs_hit,
            "weight": weights["rs_vs_spy"],
            "strength": float(round(rs_strength, 4)),
            "details": {
                "stock_roc_5d_pct": float(round(float(stock_roc5 or 0.0) * 100.0, 2)),
                "spy_roc_5d_pct": float(round(float(spy_roc5 or 0.0) * 100.0, 2)),
                "spread_pct": float(round(rs_spread * 100.0, 2)),
            },
        }

        # 3) VWAP reclaim + hold (15%)
        vwap_hit = False
        vwap_strength = 0.0
        try:
            vwap_obj = _intraday_vwap_reclaim(intra)
            vwap_hit = bool(vwap_obj.get("hit"))
            reclaim_ratio = float(vwap_obj.get("reclaim_vol_ratio") or 0.0)
            vwap_strength = _clamp01((reclaim_ratio - 1.1) / 1.2) if vwap_hit else 0.0
            signal_rows["vwap_reclaim_hold"] = {
                "hit": vwap_hit,
                "weight": weights["vwap_reclaim_hold"],
                "strength": float(round(vwap_strength, 4)),
                "details": {
                    "close_above_vwap": bool(vwap_obj.get("close_above_vwap")),
                    "held_after_reclaim": bool(vwap_obj.get("held_after_reclaim")),
                    "reclaim_vol_ratio": float(round(reclaim_ratio, 3)),
                },
            }
        except Exception:
            signal_rows["vwap_reclaim_hold"] = {
                "hit": False,
                "weight": weights["vwap_reclaim_hold"],
                "strength": 0.0,
                "details": {},
            }

        # 4) Close in top 15% of daily range (13%)
        close_hit = False
        close_strength = 0.0
        try:
            hi = float(highs[-1])
            lo = float(lows[-1])
            rng = float(hi - lo)
            pos = ((float(closes[-1]) - lo) / rng) if rng > 0 else 0.5
            close_hit = bool(pos >= 0.85)
            close_strength = _clamp01((pos - 0.85) / 0.15) if close_hit else 0.0
            signal_rows["close_top_15pct"] = {
                "hit": close_hit,
                "weight": weights["close_top_15pct"],
                "strength": float(round(close_strength, 4)),
                "details": {"close_position_pct": float(round(pos * 100.0, 2))},
            }
        except Exception:
            signal_rows["close_top_15pct"] = {
                "hit": False,
                "weight": weights["close_top_15pct"],
                "strength": 0.0,
                "details": {},
            }

        # 5) News catalyst present + unpriced (12%)
        news_hit = False
        news_strength = 0.0
        try:
            direction = str(news0.get("direction") or "NEUTRAL").strip().upper()
            conf = _safe_f(news0.get("confidence"), 0.0) or 0.0
            move_today = abs(_daily_pct_change(snap, closes))
            catalyst_text = _extract_catalyst_text(news0)
            catalyst_match = (
                ("earnings beat" in catalyst_text)
                or ("guidance raise" in catalyst_text)
                or ("fda" in catalyst_text and "approval" in catalyst_text)
                or ("contract" in catalyst_text and "win" in catalyst_text)
            )
            unpriced = bool(move_today < 0.04)
            news_hit = bool(direction == "BULLISH" and conf >= 55.0 and unpriced and catalyst_match)
            if news_hit:
                conf_strength = _clamp01((float(conf) - 55.0) / 35.0)
                move_strength = _clamp01((0.04 - float(move_today)) / 0.04)
                news_strength = _clamp01((0.65 * conf_strength) + (0.35 * move_strength))
            signal_rows["news_catalyst_unpriced"] = {
                "hit": news_hit,
                "weight": weights["news_catalyst_unpriced"],
                "strength": float(round(news_strength, 4)),
                "details": {
                    "direction": direction,
                    "confidence": float(round(conf, 2)),
                    "move_today_pct": float(round(move_today * 100.0, 2)),
                    "unpriced": bool(unpriced),
                    "catalyst_match": bool(catalyst_match),
                },
            }
        except Exception:
            signal_rows["news_catalyst_unpriced"] = {
                "hit": False,
                "weight": weights["news_catalyst_unpriced"],
                "strength": 0.0,
                "details": {},
            }

        # 6) Multi-day base breakout setup (10%)
        base_hit = False
        base_strength = 0.0
        try:
            win = closes[-21:-1] if len(closes) >= 21 else closes[:-1]
            highs_win = highs[-21:-1] if len(highs) >= 21 else highs[:-1]
            lows_win = lows[-21:-1] if len(lows) >= 21 else lows[:-1]
            vols_win = vols[-21:-1] if len(vols) >= 21 else vols[:-1]

            if win and highs_win and lows_win:
                base_high = max(float(x) for x in highs_win)
                base_low = min(float(x) for x in lows_win)
                base_range_pct = (float(base_high) - float(base_low)) / max(1e-9, float(base_high))
                near_high = abs(float(closes[-1]) - float(base_high)) / max(1e-9, float(base_high)) <= 0.005
                compact = bool(base_range_pct <= 0.08)

                vol_decl = False
                if len(vols_win) >= 10:
                    early = sum(vols_win[:5]) / 5.0
                    late = sum(vols_win[-5:]) / 5.0
                    vol_decl = bool(late <= early * 0.92)

                base_hit = bool(near_high and compact and vol_decl)
                if base_hit:
                    dist = abs(float(closes[-1]) - float(base_high)) / max(1e-9, float(base_high))
                    base_strength = _clamp01((0.005 - dist) / 0.005)

                signal_rows["base_breakout_setup"] = {
                    "hit": base_hit,
                    "weight": weights["base_breakout_setup"],
                    "strength": float(round(base_strength, 4)),
                    "details": {
                        "near_high": bool(near_high),
                        "base_range_pct": float(round(base_range_pct * 100.0, 2)),
                        "volume_declining": bool(vol_decl),
                    },
                }
            else:
                signal_rows["base_breakout_setup"] = {
                    "hit": False,
                    "weight": weights["base_breakout_setup"],
                    "strength": 0.0,
                    "details": {},
                }
        except Exception:
            signal_rows["base_breakout_setup"] = {
                "hit": False,
                "weight": weights["base_breakout_setup"],
                "strength": 0.0,
                "details": {},
            }

        # 7) Institutional footprint (6%)
        inst_hit = False
        inst_strength = 0.0
        try:
            uo_score = _safe_f(snap.get("unusual_options_score"), None)
            if uo_score is None:
                uo_score = _safe_f(snap.get("unusualOptionsScore"), None)
            if uo_score is None:
                uo_score = _safe_f(news0.get("unusual_options_score"), 0.0) or 0.0

            dv_hit = False
            dv_ratio = 0.0
            if isinstance(intra, list) and len(intra) >= 72:
                dv: List[float] = []
                for b in intra[-390:]:
                    if not isinstance(b, dict):
                        continue
                    c = _safe_f(b.get("c"))
                    v = _safe_f(b.get("v"), 0.0)
                    if c is None or v is None or c <= 0 or v < 0:
                        continue
                    dv.append(float(c) * float(v))
                if len(dv) >= 72:
                    cur_2h = sum(dv[-24:])
                    prev_chunks: List[float] = []
                    idx = len(dv) - 48
                    while idx >= 24 and len(prev_chunks) < 6:
                        prev_chunks.append(sum(dv[idx - 24:idx]))
                        idx -= 24
                    base_2h = (sum(prev_chunks) / float(len(prev_chunks))) if prev_chunks else 0.0
                    if base_2h > 0:
                        dv_ratio = float(cur_2h / base_2h)
                        dv_hit = bool(dv_ratio >= 2.0)

            uo_hit = bool(uo_score is not None and float(uo_score) >= 60.0)
            inst_hit = bool(dv_hit or uo_hit)

            dv_strength = _clamp01((dv_ratio - 2.0) / 2.0) if dv_hit else 0.0
            uo_strength = _clamp01((float(uo_score or 0.0) - 60.0) / 40.0) if uo_hit else 0.0
            inst_strength = max(float(dv_strength), float(uo_strength))

            signal_rows["institutional_footprint"] = {
                "hit": inst_hit,
                "weight": weights["institutional_footprint"],
                "strength": float(round(inst_strength, 4)),
                "details": {
                    "dollar_volume_2h_ratio": float(round(dv_ratio, 3)),
                    "unusual_options_score": (float(round(float(uo_score), 2)) if uo_score is not None else None),
                },
            }
        except Exception:
            signal_rows["institutional_footprint"] = {
                "hit": False,
                "weight": weights["institutional_footprint"],
                "strength": 0.0,
                "details": {},
            }

        # 8) Gap + reclaim pattern (4%)
        gap_hit = False
        gap_strength = 0.0
        try:
            atr14 = _atr_from_ohlc(highs, lows, closes, 14)
            if atr14 is None:
                atr14 = float(px) * 0.02
            gap_levels: List[float] = []
            for i in range(1, min(len(bars), 30)):
                cur = bars[-i] if i <= len(bars) else None
                prev = bars[-(i + 1)] if (i + 1) <= len(bars) else None
                if not isinstance(cur, dict) or not isinstance(prev, dict):
                    continue
                o = _safe_f(cur.get("o"))
                pc = _safe_f(prev.get("c"))
                if o is None or pc is None:
                    continue
                if abs(float(o) - float(pc)) > float(atr14) * 0.5:
                    gap_levels.append(float(pc))
            near = False
            reclaimed = False
            nearest_dist = None
            avg20 = (sum(vols[-21:-1]) / 20.0) if len(vols) >= 21 else 0.0
            tv = float(vols[-1]) if vols else 0.0
            for gl in gap_levels[:8]:
                dist = abs(float(px) - float(gl))
                if (nearest_dist is None) or (dist < nearest_dist):
                    nearest_dist = dist
                if dist <= float(atr14):
                    near = True
                    if float(px) >= float(gl) and avg20 > 0 and tv > avg20 * 1.1:
                        reclaimed = True
            gap_hit = bool(near and reclaimed)
            if gap_hit and nearest_dist is not None and atr14 > 0:
                gap_strength = _clamp01((float(atr14) - float(nearest_dist)) / float(atr14))
            signal_rows["gap_reclaim_pattern"] = {
                "hit": gap_hit,
                "weight": weights["gap_reclaim_pattern"],
                "strength": float(round(gap_strength, 4)),
                "details": {
                    "near_gap": bool(near),
                    "reclaimed": bool(reclaimed),
                    "atr14": float(round(float(atr14), 4)),
                },
            }
        except Exception:
            signal_rows["gap_reclaim_pattern"] = {
                "hit": False,
                "weight": weights["gap_reclaim_pattern"],
                "strength": 0.0,
                "details": {},
            }

        total_score = 0.0
        signal_count = 0
        hit_rows: List[Tuple[str, float]] = []
        for name, row in signal_rows.items():
            w = float(row.get("weight") or 0.0)
            st = _clamp01(row.get("strength"))
            hit = bool(row.get("hit"))
            if hit:
                signal_count += 1
                hit_rows.append((name, float(w * st)))
            total_score += float(w * st)

        score = int(max(0, min(100, round(total_score))))

        hit_rows.sort(key=lambda x: x[1], reverse=True)
        top_signals = [n for n, _ in hit_rows[:4]]

        confidence = _clamp01((0.72 * (float(score) / 100.0)) + (0.28 * (float(signal_count) / 8.0)))
        if signal_count <= 2:
            confidence = _clamp01(float(confidence) * 0.82)

        if score >= 80:
            interpretation = "high_probability_next_session_breakout"
        elif score >= 70:
            interpretation = "strong_next_session_breakout_candidate"
        elif score >= 60:
            interpretation = "watchlist_pre_breakout_setup"
        elif score >= 45:
            interpretation = "early_accumulation_setup"
        else:
            interpretation = "insufficient_signal"

        atr14_out = _atr_from_ohlc(highs, lows, closes, 14)
        if atr14_out is None:
            atr14_out = float(px) * 0.02
        z_low = float(px) - (0.20 * float(atr14_out))
        z_high = float(px) + (0.10 * float(atr14_out))
        invalidation = float(px) - (1.10 * float(atr14_out))

        pred_move = _clamp(0.9 + (float(score) * 0.045) + (max(0.0, float(rs_spread)) * 12.0), 0.8, 12.0)

        out.update(
            {
                "score": int(score),
                "confidence": float(round(confidence, 4)),
                "signals": signal_rows,
                "top_signals": top_signals,
                "signal_count": int(signal_count),
                "interpretation": interpretation,
                "rs_vs_spy_pct": float(round(float(rs_spread) * 100.0, 2)),
                "predicted_move_pct": float(round(float(pred_move), 2)),
                "entry_zone": {
                    "low": float(round(z_low, 4)),
                    "high": float(round(z_high, 4)),
                },
                "invalidation": float(round(invalidation, 4)),
                "time_to_watch": "9:35-10:15 AM ET tomorrow",
            }
        )
        return out
    except Exception:
        return out


def backtest_signal_accuracy(bars_history: Any) -> float:
    """Best-effort helper for historical hit-rate estimation in percent [0,100]."""
    if not isinstance(bars_history, list) or not bars_history:
        return 0.0

    positives = 0
    true_positives = 0

    for row in bars_history:
        if not isinstance(row, dict):
            continue

        score = _safe_f(row.get("pre_mover_score"), None)
        if score is None:
            score = _safe_f(row.get("score"), None)
        if score is None:
            continue

        predicted_positive = bool(float(score) >= 70.0)
        if not predicted_positive:
            continue

        positives += 1

        actual_positive = False
        try:
            if bool(row.get("next_day_top20")):
                actual_positive = True
        except Exception:
            pass

        if not actual_positive:
            try:
                rank = int(row.get("next_day_rank")) if row.get("next_day_rank") is not None else None
                if rank is not None and rank > 0 and rank <= 20:
                    actual_positive = True
            except Exception:
                pass

        if not actual_positive:
            try:
                mv = _safe_f(row.get("next_day_move_pct"), None)
                if mv is not None and float(mv) >= 3.0:
                    actual_positive = True
            except Exception:
                pass

        if actual_positive:
            true_positives += 1

    if positives <= 0:
        return 0.0
    return float(round((float(true_positives) / float(positives)) * 100.0, 2))
