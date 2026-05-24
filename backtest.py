#!/usr/bin/env python3
"""
backtest.py — Run best_pick_v2 scoring against 2024 historical data.

For every trading day Jan 2 – Dec 31 2024:
  1. Pull grouped daily bars from Polygon (all US equities, one call per day)
  2. Build a rolling 220-bar per-symbol history with no look-ahead
  3. Replicate the scan_best_pick_v2 scoring pipeline (no LLM, no live feeds)
  4. Pick the top-ranked symbol after the final-validation gate
  5. Measure close-to-close return 7 trading days later

Output: backtest_results_2024.csv
  date, symbol, rank, final_score, confidence, edge_score, edge_signals,
  momentum_score, premover_score,
  entry_price, price_7d_later, return_pct_7d, would_have_won_7d,
  price_14d_later, return_pct_14d, would_have_won_14d,
  price_21d_later, return_pct_21d, would_have_won_21d

Usage:
  python backtest.py                        # full year (252 days)
  python backtest.py --start 2024-06-01     # from a specific date
  python backtest.py --end   2024-03-31     # up to a specific date
  python backtest.py --top 5                # record top-5 picks per day, not just #1
  python backtest.py --out my_results.csv   # custom output filename

Notes:
  - Market-regime boost (_apply_regime_boost) is NOT applied; historical VIX/slope
    data would be needed to reproduce it faithfully without look-ahead.
  - News/sentiment is held at neutral (5.0) — same as the live system when no LLM
    is available.
  - Entry price = that day's closing price (end-of-day scan).
  - Exit price  = close on the 7th TRADING day later (not 7 calendar days).
  - Alpaca IEX historical data; symbols batched in chunks of 200 per request.
"""

import argparse
import csv
import math
import os
import requests
import sys
import time
import logging
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ── Bootstrap path so imports from this directory work ───────────────────────
sys.path.insert(0, str(Path(__file__).parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("backtest")

# ── Import signal logic from the live codebase ────────────────────────────────
# All of these functions are pure (no network calls once _Candidate is built).
from best_pick_v2 import (
    _Candidate,
    _percentile_ranks,
    _score_momentum,
    _score_volatility_tradability,
    _score_risk_reward,
    _score_liquidity,
    _score_news,
    _score_premover_v2,
    _compute_overextension_penalty,
    _detect_edge_signals,
    _compute_enhanced_confidence,
    _high_grade,
    _is_tradeable_equity,
    _infer_type,
    _dollar_volume_30d,
    _sma, _rsi, _atr, _roc, _slope, _swing_low,
    _clamp, _clamp01, _safe_f,
    _score_1_10_from_01,
)
# ── Trading calendar ──────────────────────────────────────────────────────────

# US market holidays observed in 2024
_HOLIDAYS_2024 = {
    date(2024,  1,  1),  # New Year's Day
    date(2024,  1, 15),  # MLK Day
    date(2024,  2, 19),  # Presidents' Day
    date(2024,  3, 29),  # Good Friday
    date(2024,  5, 27),  # Memorial Day
    date(2024,  6, 19),  # Juneteenth
    date(2024,  7,  4),  # Independence Day
    date(2024,  9,  2),  # Labor Day
    date(2024, 11, 28),  # Thanksgiving
    date(2024, 12, 25),  # Christmas
}


def _trading_days(start: date, end: date) -> List[date]:
    days: List[date] = []
    d = start
    while d <= end:
        if d.weekday() < 5 and d not in _HOLIDAYS_2024:
            days.append(d)
        d += timedelta(days=1)
    return days


# ── Alpaca data layer ─────────────────────────────────────────────────────────

# 100 liquid symbols used as the backtest universe.
# Covers high-beta tech, semis, biotech, crypto-adjacent, and a few ETFs for SPY ROC.
_BACKTEST_UNIVERSE: List[str] = [
    # Broad market anchors (needed for SPY ROC / RS_LEADER signal)
    "SPY", "QQQ", "IWM",
    # Mega-cap tech
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "AVGO",
    # Semiconductors
    "AMD", "INTC", "MU", "QCOM", "AMAT", "MRVL", "ON", "WOLF", "SMCI",
    # Software / cloud
    "NOW", "PANW", "CRWD", "SNOW", "DDOG", "ZS", "NET", "PLTR", "PATH",
    "HUBS", "MDB", "TWLO", "OKTA", "BILL", "GTLB",
    # Internet / consumer tech
    "SHOP", "UBER", "LYFT", "DASH", "ABNB", "RBLX", "SNAP", "PINS", "EBAY",
    # Fintech
    "SQ", "PYPL", "AFRM", "SOFI", "HOOD", "UPST", "COIN",
    # AI / quantum
    "IONQ", "QUBT", "RGTI", "BBAI", "SOUN", "AI",
    # Biotech
    "MRNA", "BNTX", "CRSP", "BEAM", "EDIT", "ARWR", "SRPT", "ACAD",
    "VRTX", "REGN", "ALNY",
    # EV / clean energy
    "RIVN", "LCID", "CHPT", "PLUG", "BLNK", "NKLA",
    # Crypto-adjacent
    "MARA", "RIOT", "MSTR", "CLSK", "WULF",
    # Space / defense
    "RKLB", "ASTS", "JOBY", "ACHR", "KTOS", "AXON",
    # Consumer / retail
    "NKE", "SBUX", "DKNG", "PENN", "GME", "AMC",
    # Financials
    "SCHW", "HOOD",
    # Materials / commodities
    "FCX", "NEM", "CLF",
    # Oil & gas
    "RIG", "SM", "MTDR",
]
# Deduplicate while preserving order
_BACKTEST_UNIVERSE = list(dict.fromkeys(_BACKTEST_UNIVERSE))


def _fetch_grouped_daily(dt: date) -> Dict[str, Dict[str, float]]:
    """
    Fetch one day's OHLCV bars from Alpaca /v2/stocks/bars for _BACKTEST_UNIVERSE.

    Sends all symbols in one request (Alpaca accepts a comma-separated list).
    Follows next_page_token pagination to collect every returned bar.
    Returns {SYMBOL: {o, h, l, c, v}}. Returns empty dict on auth error or if
    Alpaca has no data for that date (holiday / market closed).
    """
    key    = os.getenv("ALPACA_API_KEY",    "").strip()
    secret = os.getenv("ALPACA_SECRET_KEY", "").strip()
    if not key or not secret:
        log.error("ALPACA_API_KEY / ALPACA_SECRET_KEY not set in environment")
        return {}

    base_url = os.getenv("ALPACA_DATA_BASE_URL", "https://data.alpaca.markets").rstrip("/")
    date_str = dt.strftime("%Y-%m-%d")
    headers  = {
        "APCA-API-KEY-ID":     key,
        "APCA-API-SECRET-KEY": secret,
        "Accept":              "application/json",
    }

    out: Dict[str, Dict[str, float]] = {}

    # Alpaca accepts up to ~1 000 symbols per request; chunk at 100 to stay well inside limits
    chunk_size = 100
    for i in range(0, len(_BACKTEST_UNIVERSE), chunk_size):
        chunk = _BACKTEST_UNIVERSE[i : i + chunk_size]

        params: Dict[str, Any] = {
            "symbols":    ",".join(chunk),
            "timeframe":  "1Day",
            "start":      date_str,
            "end":        date_str,
            "feed":       "iex",
            "adjustment": "raw",
            "limit":      1000,
            "sort":       "asc",
        }

        # Follow pagination within this chunk
        while True:
            try:
                r = requests.get(
                    f"{base_url}/v2/stocks/bars",
                    headers=headers,
                    params=params,
                    timeout=(5, 20),
                )
            except requests.RequestException as e:
                log.warning(f"Alpaca bars network error {date_str}: {e}")
                break

            if r.status_code == 429:
                log.warning(f"Alpaca 429 on {date_str} — sleeping 30s")
                time.sleep(30)
                continue   # retry same page
            if r.status_code == 401:
                log.error("Alpaca 401 — check ALPACA_API_KEY / ALPACA_SECRET_KEY")
                return {}
            if r.status_code != 200:
                log.warning(f"Alpaca bars {date_str}: HTTP {r.status_code} — {r.text[:120]}")
                break

            try:
                data = r.json()
            except Exception as e:
                log.warning(f"Alpaca bars JSON parse error {date_str}: {e}")
                break

            for sym, bar_list in (data.get("bars") or {}).items():
                sym = sym.strip().upper()
                if not isinstance(bar_list, list) or not bar_list:
                    continue
                bar = bar_list[0]   # one 1Day bar per symbol for this date
                c = _safe_f(bar.get("c"))
                if c is None or c <= 0:
                    continue
                out[sym] = {
                    "o": _safe_f(bar.get("o")) or c,
                    "h": _safe_f(bar.get("h")) or c,
                    "l": _safe_f(bar.get("l")) or c,
                    "c": c,
                    "v": _safe_f(bar.get("v")) or 0.0,
                }

            page_token = data.get("next_page_token")
            if not page_token:
                break
            params["page_token"] = page_token

    return out


# ── Snapshot builder ──────────────────────────────────────────────────────────

def _build_snapshot(today_bar: Dict, prev_bar: Optional[Dict]) -> Dict[str, Any]:
    """
    Construct the Alpaca-compatible snapshot dict that _Candidate scoring reads.
    Keys match exactly what app.py and best_pick_v2.py expect.
    """
    return {
        "dailyBar": {
            "o": today_bar.get("o"),
            "h": today_bar.get("h"),
            "l": today_bar.get("l"),
            "c": today_bar.get("c"),
            "v": today_bar.get("v"),
        },
        "prevDailyBar": {
            "c": prev_bar["c"] if prev_bar else None,
            "v": prev_bar["v"] if prev_bar else None,
        } if prev_bar else {},
        # Use day's close as the latest trade price
        "latestTrade": {"p": today_bar.get("c")},
        # No intraday or quote data in daily bars
        "minuteBar":   {},
        "latestQuote": {},
    }


# ── Per-day scoring pipeline ──────────────────────────────────────────────────
# This mirrors scan_best_pick_v2 exactly, minus LLM/live-data calls.

def _score_day(
    today_data: Dict[str, Dict],
    symbol_history: Dict[str, List[Dict]],
    spy_roc5: Optional[float],
) -> List[Dict[str, Any]]:
    """
    Build _Candidate objects for every symbol that has data today and enough
    bar history, then run the full cross-sectional scoring pipeline.
    Returns all valid candidates sorted best-first.
    """
    MIN_BARS = 15
    cands: List[_Candidate] = []

    for sym, today_bar in today_data.items():
        # symbol_history[sym] already includes today as bars[-1]
        bars = symbol_history.get(sym, [])
        if len(bars) < MIN_BARS:
            continue

        last_px = _safe_f(today_bar.get("c"))
        if last_px is None or last_px <= 0:
            continue

        # Liquidity metrics from bar history
        avg_vol_30d, avg_dollar_vol_30d = None, None
        try:
            if len(bars) >= 15:
                avg_vol_30d, avg_dollar_vol_30d = _dollar_volume_30d(bars)
        except Exception:
            pass

        # Fallback from last 5 bars when history is thin
        if avg_vol_30d is None:
            vols = [_safe_f(b.get("v")) for b in bars[-5:]]
            vols = [v for v in vols if v and v > 0]
            if vols:
                avg_vol_30d = sum(vols) / len(vols)
        if avg_dollar_vol_30d is None and last_px:
            dvols = []
            for b in bars[-5:]:
                c_v = _safe_f(b.get("c"))
                v_v = _safe_f(b.get("v"))
                if c_v and v_v and c_v > 0 and v_v > 0:
                    dvols.append(c_v * v_v)
            if dvols:
                avg_dollar_vol_30d = sum(dvols) / len(dvols)

        # Hard universe gates (same thresholds as scan_best_pick_v2)
        if not _is_tradeable_equity(sym, last_px, avg_vol_30d or 150_000):
            continue
        if last_px < 5.0:
            continue
        if avg_dollar_vol_30d is not None and avg_dollar_vol_30d < 100_000:
            continue

        # Build OHLC arrays (last 220 bars; today is bars[-1] — no look-ahead)
        closes: List[float] = []
        highs:  List[float] = []
        lows:   List[float] = []
        for b in bars[-220:]:
            c = _safe_f(b.get("c"))
            h = _safe_f(b.get("h"))
            l = _safe_f(b.get("l"))
            if c is None or h is None or l is None or c <= 0:
                continue
            closes.append(c)
            highs.append(h)
            lows.append(l)

        if len(closes) < MIN_BARS:
            continue

        limited_history = len(closes) < 20

        # Technical indicators
        sma20   = _sma(closes, 20)
        sma50   = _sma(closes, 50)
        rsi14   = _rsi(closes, 14)
        roc5    = _roc(closes,  5)
        roc20   = _roc(closes, 20)
        slope20 = _slope(closes, 20)
        atr14   = _atr(highs, lows, closes, 14)

        # ATR fallback: avg(high-low) over last 20 available bars
        if atr14 is None:
            spans = [abs(highs[i] - lows[i]) for i in range(max(0, len(highs) - 20), len(highs))]
            atr14 = sum(spans) / len(spans) if spans else None
        if atr14 is None:
            continue

        atr_pct = (atr14 / last_px * 100.0) if last_px > 0 else None

        # Stop level: below swing-low or SMA20, with 0.5% buffer
        swing_lo = _swing_low(bars, 10)
        stop_candidates = [x for x in [swing_lo, sma20] if x is not None and x > 0]
        stop = min(stop_candidates) * 0.995 if stop_candidates else last_px * 0.97
        stop_dist_pct = (last_px - stop) / last_px * 100.0 if last_px > 0 else 3.0

        expected_move_5d = atr14 * math.sqrt(5.0)
        upside_ratio = None
        stop_dist_abs = abs(last_px - stop)
        if expected_move_5d and stop_dist_abs > 0:
            upside_ratio = expected_move_5d / stop_dist_abs

        # Snapshot: today = bars[-1], prev = bars[-2]
        prev_bar = bars[-2] if len(bars) >= 2 else None
        snapshot = _build_snapshot(today_bar, prev_bar)

        cands.append(_Candidate(
            symbol=sym,
            type=_infer_type(sym),
            snapshot=snapshot,
            daily_bars=list(bars),
            last_price=last_px,
            spread_pct_now=None,          # no bid/ask in daily bars
            avg_vol_30d=avg_vol_30d,
            avg_dollar_vol_30d=avg_dollar_vol_30d,
            closes=closes,
            highs=highs,
            lows=lows,
            sma20=sma20,
            sma50=sma50,
            rsi14=rsi14,
            roc5=roc5,
            roc20=roc20,
            slope20=slope20,
            atr14=atr14,
            atr_pct=atr_pct,
            stop=stop,
            stop_distance_pct=stop_dist_pct,
            expected_move_5d=expected_move_5d,
            upside_ratio=upside_ratio,
            catalysts=[],
            risk_flags=(["limited_history"] if limited_history else []),
            news_score=5.0,   # neutral — no live news in backtest
        ))

    if not cands:
        return []

    # ── Cross-sectional percentile ranks (identical to scan_best_pick_v2) ────
    roc5_r   = _percentile_ranks([c.roc5  for c in cands])
    roc20_r  = _percentile_ranks([c.roc20 for c in cands])
    slope_r  = _percentile_ranks([c.slope20 for c in cands])
    dollar_r = _percentile_ranks([c.avg_dollar_vol_30d for c in cands])
    atrp_r   = _percentile_ranks([(-float(c.atr_pct)) if c.atr_pct is not None else None for c in cands])
    upside_r = _percentile_ranks([c.upside_ratio for c in cands])
    # No bid/ask spread in historical data — hold neutral for all symbols
    spread_r = [0.5] * len(cands)

    for i, c in enumerate(cands):

        # Technical score: MA alignment + ROC + slope + RSI sweet spot
        if "limited_history" in (c.risk_flags or []):
            c.technical_score = 5.0
        else:
            ma01 = 0.4
            try:
                if c.last_price and c.sma20 and c.sma50:
                    if   c.last_price >= c.sma20 >= c.sma50: ma01 = 1.0
                    elif c.last_price >= c.sma20:             ma01 = 0.7
                    elif c.last_price < c.sma20 <= c.sma50:  ma01 = 0.2
            except Exception:
                pass

            rsi01 = 0.5
            try:
                r = float(c.rsi14 or 50.0)
                if   55 <= r <= 70: rsi01 = 1.0
                elif r >= 80:       rsi01 = 0.35
                elif r >= 70:       rsi01 = 0.7
                elif r <= 35:       rsi01 = 0.25
                else:               rsi01 = 0.55
            except Exception:
                pass

            tech01 = _clamp01(0.35*ma01 + 0.35*roc20_r[i] + 0.20*slope_r[i] + 0.10*rsi01)
            c.technical_score = _score_1_10_from_01(tech01)

        # Risk structure score: stop-distance band
        if "limited_history" in (c.risk_flags or []):
            c.risk_structure_score = 5.0
        else:
            rs01 = 0.3
            try:
                sd = c.stop_distance_pct or 0.0
                if   1.0 <= sd <= 3.5: rs01 = 1.0
                elif sd < 0.6:          rs01 = 0.25
                elif sd <= 5.0:         rs01 = 0.7
                elif sd <= 6.0:         rs01 = 0.45
                else:                   rs01 = 0.15
                if c.atr_pct and c.atr_pct >= 7.0:
                    rs01 = min(rs01, 0.35)
            except Exception:
                pass
            c.risk_structure_score = _score_1_10_from_01(rs01)

        # Upside score
        up01 = upside_r[i]
        if c.atr_pct and c.atr_pct >= 7.0:
            up01 = min(float(up01), 0.55)
        c.upside_score = _score_1_10_from_01(up01)

        # Execution score
        exec01 = _clamp01(0.55*dollar_r[i] + 0.25*atrp_r[i] + 0.15*spread_r[i] + 0.05*0.6)
        c.execution_score = _score_1_10_from_01(exec01)

        c.catalyst_score  = 5.0   # neutral — no earnings calendar wired
        c.sentiment_score = 5.0   # neutral — no LLM

        # ── Enhanced component scores (same formulas as scan_best_pick_v2) ──
        c.momentum_score        = _score_momentum(c, roc5_r[i], roc20_r[i], slope_r[i])
        c.volatility_score_0_10 = _score_volatility_tradability(c)
        c.risk_reward_score     = _score_risk_reward(c)
        c.liquidity_score       = _score_liquidity(c, dollar_r[i], spread_r[i])

        # AI score (pillar blend)
        c.ai_score = float(round(_clamp(
            0.30*c.technical_score  + 0.20*c.catalyst_score
            + 0.15*c.sentiment_score + 0.20*c.risk_structure_score
            + 0.15*c.upside_score,
            1.0, 10.0), 1))

        # Final score (component blend)
        c.final_score_0_10 = float(round(_clamp(
            0.28*c.momentum_score    + 0.22*c.risk_reward_score
            + 0.20*c.liquidity_score + 0.15*c.volatility_score_0_10
            + 0.10*c.technical_score + 0.05*c.news_score,
            1.0, 10.0), 1))

        # Pre-mover score + overextension penalty
        try:
            c.premover_score_0_10  = _score_premover_v2(c)
            c.overextended_penalty = _compute_overextension_penalty(c)
        except Exception:
            c.premover_score_0_10  = 5.0
            c.overextended_penalty = 0.0

        # Integrate premover into final score (same formula as scan_best_pick_v2)
        c.final_score_0_10 = float(round(_clamp(
            float(c.final_score_0_10)
            + 0.35 * (c.premover_score_0_10 - 5.0)
            - c.overextended_penalty,
            1.0, 10.0), 1))

        # Edge signals
        try:
            c.edge_signals = _detect_edge_signals(c, spy_roc5=spy_roc5)
            edge_pts = (
                (3 if "MOMENTUM_EXPANSION"    in c.edge_signals else 0)
                + (3 if "BREAKOUT_STRUCTURE"  in c.edge_signals else 0)
                + (2 if "RS_LEADER"           in c.edge_signals else 0)
                + (2 if "VOLATILITY_EXPANSION" in c.edge_signals else 0)
            )
            c.edge_score_0_10 = float(_clamp(edge_pts * 10.0 / 10.0, 0.0, 10.0))
        except Exception:
            c.edge_signals    = []
            c.edge_score_0_10 = 0.0

    # ── Rank: 0.7 × final_score + 0.3 × edge_score ─────────────────────────
    cands.sort(
        key=lambda x: (
            0.7 * float(x.final_score_0_10 or 0)
            + 0.3 * float(x.edge_score_0_10 or 0)
        ),
        reverse=True,
    )

    # ── Final validation gate ─────────────────────────────────────────────────
    def _passes(c: _Candidate) -> bool:
        if c.last_price is None or c.last_price < 5.0 or c.last_price > 500.0:
            return False
        if c.avg_dollar_vol_30d is None or c.avg_dollar_vol_30d < 5_000_000:
            return False
        return True

    valid = [c for c in cands if _passes(c) and c.type != "ETF"]

    # ── Build output records ──────────────────────────────────────────────────
    out: List[Dict[str, Any]] = []
    for c in valid:
        hg   = _high_grade(c.ai_score, c.execution_score, c.risk_structure_score)
        conf = _compute_enhanced_confidence(
            momentum=c.momentum_score,
            volatility=c.volatility_score_0_10,
            risk_reward=c.risk_reward_score,
            liquidity=c.liquidity_score,
            news=c.news_score,
            high_grade=bool(hg),
        )
        out.append({
            "symbol":          c.symbol,
            "final_score":     c.final_score_0_10,
            "confidence":      conf,
            "edge_score":      c.edge_score_0_10,
            "edge_signals":    ",".join(c.edge_signals or []),
            "momentum_score":  round(c.momentum_score, 2),
            "premover_score":  round(c.premover_score_0_10, 2),
            "entry_price":     round(c.last_price, 2),
        })
    return out


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="best_pick_v2 historical backtest (2024)")
    parser.add_argument("--start", default="2024-01-02", help="Start date YYYY-MM-DD")
    parser.add_argument("--end",   default="2024-12-31", help="End date YYYY-MM-DD")
    parser.add_argument("--top",   type=int, default=1,
                        help="Top-N picks to record per day (default 1)")
    parser.add_argument("--out",   default="backtest_results_2024.csv",
                        help="Output CSV path")
    parser.add_argument("--no_lookahead", action="store_true",
                        help=(
                            "Strict no-lookahead mode: signals computed from bars up to D-1 "
                            "(today's close excluded), entry price = D+1 open. "
                            "Eliminates same-day price information from all signals."
                        ))
    args = parser.parse_args()

    start_date = date.fromisoformat(args.start)
    end_date   = date.fromisoformat(args.end)
    top_n      = max(1, args.top)
    out_path   = Path(__file__).parent / args.out

    trading_days = _trading_days(start_date, end_date)
    log.info(
        f"Backtest: {start_date} → {end_date}  "
        f"({len(trading_days)} trading days)  top_n={top_n}"
    )

    # ── Phase 1: Fetch all grouped daily data ─────────────────────────────────
    log.info("Phase 1: fetching grouped daily bars from Polygon …")
    all_days: Dict[str, Dict[str, Dict]] = {}  # {date_str: {SYM: ohlcv}}

    for i, d in enumerate(trading_days):
        date_str = d.strftime("%Y-%m-%d")
        log.info(f"  [{i+1:3d}/{len(trading_days)}] {date_str}")
        bars = _fetch_grouped_daily(d)
        if bars:
            all_days[date_str] = bars
        else:
            log.warning(f"  {date_str}: no data (holiday / market closed?)")
        time.sleep(0.35)   # ~2.9 req/sec — within Polygon rate limits

    sorted_dates = sorted(all_days.keys())
    log.info(
        f"Phase 1 done: {len(sorted_dates)} days with data  "
        f"({len(trading_days) - len(sorted_dates)} skipped)"
    )

    # ── Phase 2: Score each day, measure 7-day returns ───────────────────────
    log.info("Phase 2: scoring candidates …")

    no_lookahead = args.no_lookahead
    if no_lookahead:
        log.info("  *** no-lookahead mode: signals use D-1 bars, entry = D open ***")

    symbol_history: Dict[str, List[Dict]] = {}  # rolling 220-bar window per symbol
    prev_data: Dict[str, Dict] = {}             # D-1 bars (no_lookahead mode only)
    rows: List[Dict[str, Any]] = []

    fieldnames = [
        "date", "symbol", "rank",
        "final_score", "confidence", "edge_score", "edge_signals",
        "momentum_score", "premover_score",
        "entry_price",
        "price_7d_later",  "return_pct_7d",  "would_have_won_7d",  "spy_return_7d",  "alpha_7d",
        "price_14d_later", "return_pct_14d", "would_have_won_14d", "spy_return_14d", "alpha_14d",
        "price_21d_later", "return_pct_21d", "would_have_won_21d", "spy_return_21d", "alpha_21d",
    ]

    for day_idx, date_str in enumerate(sorted_dates):
        today_data = all_days[date_str]

        if no_lookahead:
            # ── No-lookahead: bootstrap on day 0 ─────────────────────────────
            # We need D-1's bars to score; skip the very first day since there
            # is no previous day yet.
            if not prev_data:
                for sym, bar in today_data.items():
                    hist = symbol_history.setdefault(sym, [])
                    hist.append(bar)
                    if len(hist) > 220:
                        symbol_history[sym] = hist[-220:]
                prev_data = today_data
                continue   # nothing to score on day 0

            # Score using D-1 data (prev_data); history was last updated with
            # prev_data's bars — today's bars are NOT yet in history.
            score_data = prev_data
        else:
            # ── Normal mode: update history with today's bar first ────────────
            for sym, bar in today_data.items():
                hist = symbol_history.setdefault(sym, [])
                hist.append(bar)
                if len(hist) > 220:
                    symbol_history[sym] = hist[-220:]
            score_data = today_data

        # ── SPY 5-day ROC for the RS_LEADER edge signal ───────────────────────
        spy_roc5: Optional[float] = None
        try:
            spy_bars   = symbol_history.get("SPY", [])
            spy_closes = [float(v) for b in spy_bars if (v := _safe_f(b.get("c"))) and v > 0]
            spy_roc5   = _roc(spy_closes, 5)
        except Exception:
            pass

        # ── Score ──────────────────────────────────────────────────────────────
        scored = _score_day(score_data, symbol_history, spy_roc5)

        if no_lookahead:
            # Update history with today's bar AFTER scoring (so today's close
            # never contaminates the signal).
            for sym, bar in today_data.items():
                hist = symbol_history.setdefault(sym, [])
                hist.append(bar)
                if len(hist) > 220:
                    symbol_history[sym] = hist[-220:]
            prev_data = today_data

        if not scored:
            log.info(f"  {date_str}: no valid candidates after gates")
            continue

        # ── Find the 7th / 14th / 21st trading day ahead ─────────────────────
        # Buffer of 30 future entries comfortably covers 21 trading days + gaps.
        future_dates   = sorted_dates[day_idx + 1 : day_idx + 30]
        target_date_7  = future_dates[6]  if len(future_dates) >= 7  else None
        target_date_14 = future_dates[13] if len(future_dates) >= 14 else None
        target_date_21 = future_dates[20] if len(future_dates) >= 21 else None

        def _exit_price(target_date: Optional[str], sym: str) -> Optional[float]:
            if not target_date:
                return None
            bar = all_days.get(target_date, {}).get(sym)
            return round(bar["c"], 2) if bar else None

        def _ret(exit_px: Optional[float], entry: float):
            if exit_px is None or entry <= 0:
                return None, None
            pct = round((exit_px - entry) / entry * 100.0, 2)
            return pct, pct > 0

        def _alpha(pick_ret: Optional[float], spy_ret: Optional[float]) -> Optional[float]:
            if pick_ret is None or spy_ret is None:
                return None
            return round(pick_ret - spy_ret, 2)

        if no_lookahead:
            # Entry = today's open (D's open, signal was from D-1's close)
            spy_entry = _safe_f((today_data.get("SPY") or {}).get("o"))
        else:
            # Entry = today's close (end-of-day scan, same-day close)
            spy_entry = _safe_f((today_data.get("SPY") or {}).get("c"))

        spy_p7  = _exit_price(target_date_7,  "SPY")
        spy_p14 = _exit_price(target_date_14, "SPY")
        spy_p21 = _exit_price(target_date_21, "SPY")

        spy_r7,  _ = _ret(spy_p7,  spy_entry or 0)
        spy_r14, _ = _ret(spy_p14, spy_entry or 0)
        spy_r21, _ = _ret(spy_p21, spy_entry or 0)

        # ── Record top_n picks ────────────────────────────────────────────────
        for rank, pick in enumerate(scored[:top_n], start=1):
            sym = pick["symbol"]
            if no_lookahead:
                # Override entry to today's open (next-day fill after D-1 signal)
                raw_open = _safe_f((today_data.get(sym) or {}).get("o"))
                entry_price = round(raw_open, 2) if raw_open and raw_open > 0 else pick["entry_price"]
            else:
                entry_price = pick["entry_price"]

            p7  = _exit_price(target_date_7,  sym)
            p14 = _exit_price(target_date_14, sym)
            p21 = _exit_price(target_date_21, sym)

            r7,  w7  = _ret(p7,  entry_price)
            r14, w14 = _ret(p14, entry_price)
            r21, w21 = _ret(p21, entry_price)

            rows.append({
                "date":             date_str,
                "symbol":           sym,
                "rank":             rank,
                "final_score":      pick["final_score"],
                "confidence":       pick["confidence"],
                "edge_score":       pick["edge_score"],
                "edge_signals":     pick["edge_signals"],
                "momentum_score":   pick["momentum_score"],
                "premover_score":   pick["premover_score"],
                "entry_price":      entry_price,
                "price_7d_later":   p7,  "return_pct_7d":  r7,  "would_have_won_7d":  w7,  "spy_return_7d":  spy_r7,  "alpha_7d":  _alpha(r7,  spy_r7),
                "price_14d_later":  p14, "return_pct_14d": r14, "would_have_won_14d": w14, "spy_return_14d": spy_r14, "alpha_14d": _alpha(r14, spy_r14),
                "price_21d_later":  p21, "return_pct_21d": r21, "would_have_won_21d": w21, "spy_return_21d": spy_r21, "alpha_21d": _alpha(r21, spy_r21),
            })

        top = scored[0]
        log.info(
            f"  {date_str}: top={top['symbol']:6s}  "
            f"score={top['final_score']}  conf={top['confidence']}  "
            f"signals=[{top['edge_signals'] or '—'}]  "
            f"entry=${top['entry_price']}"
        )

    # ── Write CSV ─────────────────────────────────────────────────────────────
    with open(out_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    log.info(f"\nWrote {len(rows)} rows → {out_path}")

    # ── Summary stats ─────────────────────────────────────────────────────────
    rank1 = [r for r in rows if r["rank"] == 1]

    def _horizon_stats(label: str, ret_key: str, win_key: str, alpha_key: str) -> None:
        res  = [r for r in rank1 if r[ret_key] is not None]
        if not res:
            log.info(f"  {label}: no data")
            return
        w    = [r for r in res if r[win_key]]
        l    = [r for r in res if not r[win_key]]
        wr   = len(w) / len(res) * 100
        ar   = sum(r[ret_key] for r in res) / len(res)
        aw   = sum(r[ret_key] for r in w) / len(w)   if w else 0.0
        al   = sum(r[ret_key] for r in l) / len(l)   if l else 0.0
        exp  = (wr/100 * aw) + ((1 - wr/100) * al)
        alpha_res = [r for r in res if r[alpha_key] is not None]
        avg_alpha = sum(r[alpha_key] for r in alpha_res) / len(alpha_res) if alpha_res else None
        alpha_str = f"  avg_alpha={avg_alpha:+.2f}%" if avg_alpha is not None else ""
        log.info(f"  {label}:  n={len(res)}  wr={wr:.1f}%  avg={ar:+.2f}%  "
                 f"win={aw:+.2f}%  loss={al:+.2f}%  exp={exp:+.2f}%{alpha_str}")

    log.info(f"\n{'='*64}")
    log.info("SUMMARY  (rank-1 picks only)")
    log.info(f"  Trading days scored : {len(rank1)}")
    _horizon_stats(" 7-day", "return_pct_7d",  "would_have_won_7d",  "alpha_7d")
    _horizon_stats("14-day", "return_pct_14d", "would_have_won_14d", "alpha_14d")
    _horizon_stats("21-day", "return_pct_21d", "would_have_won_21d", "alpha_21d")

    # Per-edge-signal breakdown (7-day horizon as primary)
    resolved = [r for r in rank1 if r["return_pct_7d"] is not None]
    if resolved:
        by_signal: Dict[str, Dict] = {}
        for r in resolved:
            sigs = [s.strip() for s in (r["edge_signals"] or "").split(",") if s.strip()]
            if not sigs:
                sigs = ["NO_SIGNAL"]
            for sig in sigs:
                if sig not in by_signal:
                    by_signal[sig] = {"n": 0, "wins": 0, "returns": []}
                by_signal[sig]["n"] += 1
                if r["would_have_won_7d"]:
                    by_signal[sig]["wins"] += 1
                by_signal[sig]["returns"].append(r["return_pct_7d"])

        log.info(f"\n  By edge signal (7-day):")
        for sig, d in sorted(by_signal.items(), key=lambda kv: kv[1]["n"], reverse=True):
            n  = d["n"]
            wr = d["wins"] / n * 100
            ar = sum(d["returns"]) / n
            log.info(f"    {sig:<30s}  n={n:3d}  wr={wr:5.1f}%  avg_ret={ar:+.2f}%")

    log.info(f"{'='*64}\n")


if __name__ == "__main__":
    main()
