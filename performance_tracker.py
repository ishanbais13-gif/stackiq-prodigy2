"""
Performance tracker for best_pick_v2 picks.

Records every OK pick, then evaluates outcomes (1–7 days later) using bar data.
All writes and evaluations run in background threads — zero impact on latency.

DB: SQLite at PERF_TRACKER_DB env var (default: perf_tracker.db)
"""

import json
import logging
import math
import os
import sqlite3
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

log = logging.getLogger("stackiq")

_DB_PATH = os.getenv("PERF_TRACKER_DB", os.path.join(
    os.getenv("DATA_DIR", os.path.dirname(os.path.abspath(__file__))),
    "perf_tracker.db"
))

_SCHEMA = """
CREATE TABLE IF NOT EXISTS picks (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol              TEXT    NOT NULL,
    direction           TEXT    DEFAULT 'long',
    entry_price         REAL,
    stop                REAL,
    target1             REAL,
    target2             REAL,
    target3             REAL,
    edge_signals        TEXT,           -- JSON list
    edge_score          REAL,
    final_score         REAL,
    confidence          REAL,
    premover_score      REAL,
    recorded_at         REAL    NOT NULL,  -- unix timestamp
    status              TEXT    DEFAULT 'pending',  -- pending|won|won_drift|lost|lost_drift|expired_neutral|expired
    evaluated_at        REAL,
    max_return_pct      REAL,
    max_drawdown_pct    REAL,
    hit_target          INTEGER DEFAULT 0,
    hit_stop            INTEGER DEFAULT 0,
    days_to_outcome     INTEGER
);
CREATE INDEX IF NOT EXISTS idx_pt_status   ON picks(status);
CREATE INDEX IF NOT EXISTS idx_pt_symbol   ON picks(symbol);
CREATE INDEX IF NOT EXISTS idx_pt_recorded ON picks(recorded_at);
"""

# ──────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ──────────────────────────────────────────────────────────────────────────────

def _sf(v: Any) -> Optional[float]:
    try:
        x = float(v)
        return x if math.isfinite(x) else None
    except Exception:
        return None


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(_DB_PATH, timeout=15, check_same_thread=False)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    return c


def _ensure_schema() -> None:
    try:
        with _conn() as db:
            db.executescript(_SCHEMA)
    except Exception as e:
        log.warning(f"perf_tracker: schema init failed: {e}")


_ensure_schema()


def _bar_ts(bar: Dict[str, Any]) -> float:
    """Extract unix timestamp from Alpaca bar dict."""
    t = bar.get("t")
    if t is None:
        return 0.0
    if isinstance(t, (int, float)):
        return float(t)
    try:
        dt = datetime.fromisoformat(str(t).replace("Z", "+00:00"))
        return dt.timestamp()
    except Exception:
        return 0.0


# ──────────────────────────────────────────────────────────────────────────────
# Public: record a pick
# ──────────────────────────────────────────────────────────────────────────────

def record_pick(pick: Dict[str, Any]):
    """
    Insert a new pick record.
    Safe to call from asyncio.to_thread — never raises.
    Only records picks where status='OK' and symbol is non-empty.

    Returns:
        int  — new row id on successful insert
        dict — {"status": "duplicate_suppressed", "existing_id": int, "age_hours": float}
        None — skipped (NO_TRADE, empty symbol) or error
    """
    try:
        symbol_upper = str(pick.get("symbol") or "").strip().upper()
        if not symbol_upper:
            return None

        # Dedup guard: reject if same symbol recorded within the last 24 hours
        _dedup_window_seconds = 24 * 3600
        _now_ts  = time.time()
        _cutoff_ts = _now_ts - _dedup_window_seconds
        with _conn() as db:
            _existing = db.execute(
                "SELECT id, recorded_at FROM picks WHERE symbol = ? AND recorded_at >= ? ORDER BY recorded_at DESC LIMIT 1",
                (symbol_upper, _cutoff_ts),
            ).fetchone()
        if _existing:
            _existing_id = int(_existing[0])
            _existing_ts = float(_existing[1])
            _age_hours   = (_now_ts - _existing_ts) / 3600
            log.info(
                "perf_tracker: duplicate suppressed symbol=%s existing_id=%d age=%.1fh (within 24h window)",
                symbol_upper, _existing_id, _age_hours,
            )
            return {"status": "duplicate_suppressed", "existing_id": _existing_id, "age_hours": _age_hours}

        sym = symbol_upper  # alias used in the rest of the function

        # Only track real picks, not NO_TRADE responses
        if str(pick.get("status") or "OK") == "NO_TRADE":
            return None

        tp        = pick.get("trade_plan") or {}
        entry     = _sf(tp.get("entry") or pick.get("entry"))
        stop      = _sf(tp.get("stop")  or pick.get("stop"))
        direction = str(tp.get("direction") or "long").lower()
        targets   = tp.get("targets") or []
        t1 = _sf(targets[0]) if len(targets) > 0 else None
        t2 = _sf(targets[1]) if len(targets) > 1 else None
        t3 = _sf(targets[2]) if len(targets) > 2 else None

        edge_signals  = json.dumps(list(pick.get("edge_signals")  or []))
        edge_score    = _sf(pick.get("edge_score_0_10"))
        final_score   = _sf(pick.get("final_score_0_10"))
        confidence    = _sf(pick.get("confidence_0_10"))
        premover      = _sf(pick.get("premover_score_0_10"))

        with _conn() as db:
            cur = db.execute(
                """INSERT INTO picks
                   (symbol, direction, entry_price, stop, target1, target2, target3,
                    edge_signals, edge_score, final_score, confidence, premover_score,
                    recorded_at, status)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (sym, direction, entry, stop, t1, t2, t3,
                 edge_signals, edge_score, final_score, confidence, premover,
                 time.time(), "pending"),
            )
            row_id = int(cur.lastrowid)

        log.info(f"perf_tracker: recorded id={row_id} {sym} entry={entry} "
                 f"stop={stop} t1={t1} edge={edge_signals}")
        return row_id

    except Exception as e:
        log.warning(f"perf_tracker: record_pick failed: {e}")
        return None


# ──────────────────────────────────────────────────────────────────────────────
# Public: evaluate pending picks
# ──────────────────────────────────────────────────────────────────────────────

def evaluate_pending_picks(
    min_age_hours: float = 24.0,
    max_age_days:  float = 7.0,
    batch_size:    int   = 40,
) -> int:
    """
    Fetch bar data for pending picks and resolve win/loss/expired.
    Should be called from a background thread.
    Returns number of picks updated.
    """
    try:
        from data_fetcher import get_bars_batch as _gbatch
    except Exception:
        log.warning("perf_tracker: data_fetcher unavailable, skipping evaluation")
        return 0

    now       = time.time()
    min_age_s = min_age_hours * 3600.0
    max_age_s = max_age_days  * 86400.0

    try:
        with _conn() as db:
            rows = db.execute(
                """SELECT * FROM picks
                   WHERE status IN ('pending', 'expired_neutral')
                     AND recorded_at < ?
                   ORDER BY recorded_at ASC
                   LIMIT ?""",
                (now - min_age_s, batch_size),
            ).fetchall()
    except Exception as e:
        log.warning(f"perf_tracker: query failed: {e}")
        return 0

    if not rows:
        return 0

    symbols = list({row["symbol"] for row in rows})
    try:
        # Fetch 30 days to cover picks going back ~6 weeks
        bars_map: Dict[str, List] = _gbatch(symbols, "1Day", 30) or {}
        bars_map = {str(k).upper(): v for k, v in bars_map.items()}
    except Exception as e:
        log.warning(f"perf_tracker: bar fetch failed: {e}")
        return 0

    updated = 0
    for row in rows:
        try:
            sym         = row["symbol"]
            recorded_at = float(row["recorded_at"])
            age_s       = now - recorded_at
            age_days    = age_s / 86400.0

            all_bars    = bars_map.get(sym) or []
            future_bars = [
                b for b in all_bars
                if isinstance(b, dict) and _bar_ts(b) > recorded_at
            ]

            # If no future bars yet and still within evaluation window, skip
            if not future_bars and age_s < max_age_s:
                continue

            outcome = _resolve_outcome(
                future_bars = future_bars,
                entry       = _sf(row["entry_price"]),
                stop        = _sf(row["stop"]),
                target1     = _sf(row["target1"]),
                direction   = str(row["direction"] or "long"),
                age_days    = age_days,
                max_age_days= max_age_days,
            )

            # If still pending (too early), don't write
            if outcome["status"] == "pending":
                continue

            # Don't downgrade a definitive result back to expired_neutral
            current_status = str(row["status"])
            if current_status in ("won", "won_drift", "lost", "lost_drift"):
                continue
            # Only upgrade expired_neutral to a decisive result — never re-expire
            if current_status == "expired_neutral" and outcome["status"] == "expired_neutral":
                continue

            with _conn() as db:
                db.execute(
                    """UPDATE picks
                       SET status=?, evaluated_at=?,
                           max_return_pct=?, max_drawdown_pct=?,
                           hit_target=?, hit_stop=?, days_to_outcome=?
                       WHERE id=?""",
                    (
                        outcome["status"],
                        now,
                        outcome["max_return_pct"],
                        outcome["max_drawdown_pct"],
                        int(bool(outcome["hit_target"])),
                        int(bool(outcome["hit_stop"])),
                        outcome["days_to_outcome"],
                        row["id"],
                    ),
                )
            updated += 1

            # Feed outcome into evolution engine
            if outcome["status"] in ("won", "won_drift", "lost", "lost_drift"):
                try:
                    from learning import settle_outcome as _settle
                    _ret   = float(outcome["max_return_pct"] or 0)
                    _dd    = float(outcome["max_drawdown_pct"] or 0)
                    _days  = int(outcome["days_to_outcome"] or 1)
                    _tgt_n = 3 if outcome["hit_target"] and _ret > 15 else \
                             2 if outcome["hit_target"] and _ret > 7  else \
                             1 if outcome["hit_target"] else 0
                    _settle(
                        symbol=sym,
                        return_pct=_ret,
                        hit_target_n=_tgt_n,
                        max_drawdown=_dd,
                        days_held=_days,
                        perf_id=int(row["id"]),
                    )
                except Exception as _le:
                    log.warning(f"perf_tracker: learning.settle_outcome failed: {_le}")

            # Fire outcome alert in background (won/lost only, not expired)
            if outcome["status"] in ("won", "won_drift", "lost", "lost_drift"):
                try:
                    from alerts import send_outcome_alert_bg
                    send_outcome_alert_bg(
                        symbol     = sym,
                        status     = outcome["status"],
                        return_pct = outcome["max_return_pct"],
                        entry      = _sf(row["entry_price"]),
                    )
                except Exception as _ae:
                    log.warning(f"perf_tracker: outcome alert failed: {_ae}")

            log.info(
                f"perf_tracker: evaluated id={row['id']} {sym} "
                f"→ {outcome['status']} ret={outcome['max_return_pct']}% "
                f"dd={outcome['max_drawdown_pct']}%"
            )
        except Exception as e:
            log.warning(f"perf_tracker: eval error for {dict(row).get('symbol')}: {e}")

    return updated


def _resolve_outcome(
    *,
    future_bars:  List[Dict[str, Any]],
    entry:        Optional[float],
    stop:         Optional[float],
    target1:      Optional[float],
    direction:    str,
    age_days:     float,
    max_age_days: float,
) -> Dict[str, Any]:
    """Walk forward through bars and determine win/loss/expired."""
    _null = {"status": "pending", "max_return_pct": None, "max_drawdown_pct": None,
             "hit_target": False, "hit_stop": False, "days_to_outcome": None}

    if not entry or not math.isfinite(float(entry)) or float(entry) <= 0:
        # No formal entry price — use first future bar's open as synthetic entry.
        # This lets us track return % for picks that lacked a trade plan.
        synthetic_entry = None
        for b in future_bars:
            v = _sf(b.get("o")) or _sf(b.get("c"))
            if v and v > 0:
                synthetic_entry = v
                break
        if not synthetic_entry:
            return {**_null, "status": "expired_neutral", "days_to_outcome": int(age_days)}
        entry = synthetic_entry
        stop = None    # no stop without a real trade plan
        target1 = None  # no target without a real trade plan

    entry   = float(entry)
    stop    = float(stop)   if stop   is not None and math.isfinite(float(stop))   else None
    target1 = float(target1) if target1 is not None and math.isfinite(float(target1)) else None
    is_long = direction.lower() != "short"

    max_ret:  Optional[float] = None
    max_dd:   Optional[float] = None
    hit_tgt   = False
    hit_stop  = False
    days_out: Optional[int]  = None

    for i, bar in enumerate(future_bars):
        h = _sf(bar.get("h"))
        l = _sf(bar.get("l"))
        if h is None or l is None:
            continue
        day_num = i + 1

        if is_long:
            bar_ret = (h - entry) / entry * 100.0
            bar_dd  = (l - entry) / entry * 100.0
            if target1 is not None and h >= target1 and not hit_tgt:
                hit_tgt  = True
                days_out = day_num
            if stop is not None and l <= stop and not hit_stop:
                hit_stop  = True
                if days_out is None:
                    days_out = day_num
        else:
            bar_ret = (entry - l) / entry * 100.0
            bar_dd  = (entry - h) / entry * 100.0
            if target1 is not None and l <= target1 and not hit_tgt:
                hit_tgt  = True
                days_out = day_num
            if stop is not None and h >= stop and not hit_stop:
                hit_stop  = True
                if days_out is None:
                    days_out = day_num

        max_ret = max(max_ret, bar_ret) if max_ret is not None else bar_ret
        max_dd  = min(max_dd,  bar_dd)  if max_dd  is not None else bar_dd

        # Stop exits first within same bar if both triggered
        if hit_stop and not hit_tgt:
            break
        if hit_tgt:
            break

    # Determine final status
    if hit_tgt:
        status = "won"
    elif hit_stop:
        status = "lost"
    elif age_days >= max_age_days:
        # Time stop reached without hitting a hard target or stop.
        # Classify by final price vs entry using ±2% drift threshold.
        if future_bars:
            last_c = _sf(future_bars[-1].get("c"))
            if last_c is not None and entry > 0:
                final_ret = ((last_c - entry) / entry * 100.0 if is_long
                             else (entry - last_c) / entry * 100.0)
                if final_ret > 2.0:
                    status = "won_drift"
                elif final_ret < -2.0:
                    status = "lost_drift"
                else:
                    status = "expired_neutral"
                if max_ret is None:
                    max_ret = final_ret
            else:
                status = "expired_neutral"
        else:
            status = "expired_neutral"
        days_out = int(age_days)
    else:
        status = "pending"

    return {
        "status":          status,
        "max_return_pct":  round(max_ret, 2) if max_ret is not None else None,
        "max_drawdown_pct": round(max_dd, 2) if max_dd  is not None else None,
        "hit_target":      hit_tgt,
        "hit_stop":        hit_stop,
        "days_to_outcome": days_out,
    }


# ──────────────────────────────────────────────────────────────────────────────
# Public: aggregated stats
# ──────────────────────────────────────────────────────────────────────────────

def get_performance_stats(lookback_days: int = 14) -> Dict[str, Any]:
    """
    Return win rate, avg return, per-edge-signal breakdown, best/worst picks.
    Reads from SQLite — safe to call any time.
    """
    cutoff = time.time() - lookback_days * 86400.0
    try:
        with _conn() as db:
            rows = db.execute(
                """SELECT * FROM picks
                   WHERE recorded_at >= ?
                     AND status IN ('won', 'won_drift', 'lost', 'lost_drift',
                                    'expired_neutral', 'expired')
                   ORDER BY recorded_at DESC""",
                (cutoff,),
            ).fetchall()
            pending_count = db.execute(
                "SELECT COUNT(*) FROM picks WHERE status='pending'",
            ).fetchone()[0]
    except Exception as e:
        log.warning(f"perf_tracker: stats query failed: {e}")
        return {"error": str(e)}

    total    = len(rows)
    wins     = sum(1 for r in rows if r["status"] in ("won", "won_drift"))
    losses   = sum(1 for r in rows if r["status"] in ("lost", "lost_drift"))
    neutral  = sum(1 for r in rows if r["status"] in ("expired_neutral", "expired"))
    decisive = wins + losses  # neutral excluded from win-rate denominator

    if total == 0:
        return {
            "lookback_days": lookback_days,
            "total_evaluated": 0,
            "pending": int(pending_count),
            "win_rate": None,
            "avg_return_pct": None,
            "avg_drawdown_pct": None,
            "by_edge_signal": {},
            "best_picks": [],
            "worst_picks": [],
        }

    rets  = [r["max_return_pct"]   for r in rows if r["max_return_pct"]   is not None]
    dds   = [r["max_drawdown_pct"] for r in rows if r["max_drawdown_pct"] is not None]

    # Per-edge-signal breakdown
    sig_stats: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        try:
            sigs = json.loads(r["edge_signals"] or "[]")
        except Exception:
            sigs = []
        for sig in sigs:
            if sig not in sig_stats:
                sig_stats[sig] = {"picks": 0, "wins": 0, "returns": []}
            sig_stats[sig]["picks"] += 1
            if r["status"] in ("won", "won_drift"):
                sig_stats[sig]["wins"] += 1
            if r["max_return_pct"] is not None:
                sig_stats[sig]["returns"].append(r["max_return_pct"])

    by_edge = {}
    for sig, d in sig_stats.items():
        n = d["picks"]
        by_edge[sig] = {
            "picks":          n,
            "win_rate":       round(d["wins"] / n * 100, 1) if n else None,
            "avg_return_pct": round(sum(d["returns"]) / len(d["returns"]), 2) if d["returns"] else None,
        }

    # Sort by_edge by win_rate desc for readability
    by_edge = dict(sorted(by_edge.items(),
                          key=lambda kv: kv[1]["win_rate"] or 0.0, reverse=True))

    def _summary(r) -> Dict[str, Any]:
        try:
            sigs = json.loads(r["edge_signals"] or "[]")
        except Exception:
            sigs = []
        return {
            "symbol":           r["symbol"],
            "recorded_at":      datetime.fromtimestamp(r["recorded_at"], tz=timezone.utc).strftime("%Y-%m-%d"),
            "status":           r["status"],
            "entry_price":      r["entry_price"],
            "max_return_pct":   r["max_return_pct"],
            "max_drawdown_pct": r["max_drawdown_pct"],
            "days_to_outcome":  r["days_to_outcome"],
            "edge_signals":     sigs,
            "edge_score":       r["edge_score"],
            "final_score":      r["final_score"],
        }

    sorted_rows = sorted(rows, key=lambda r: r["max_return_pct"] or 0.0, reverse=True)

    _win_rate   = round(wins / decisive * 100, 1) if decisive > 0 else None
    _avg_ret    = round(sum(rets) / len(rets), 2) if rets else None
    _avg_dd     = round(sum(dds)  / len(dds),  2) if dds  else None

    # Human-readable one-liner
    _ret_str     = f", avg return {'+' if (_avg_ret or 0) >= 0 else ''}{_avg_ret}%" if _avg_ret is not None else ""
    _dd_str      = f", avg drawdown {_avg_dd}%" if _avg_dd is not None else ""
    _pending_str = f" ({pending_count} pending)" if pending_count else ""
    _neutral_str = f" ({neutral} neutral)" if neutral else ""
    _wr_str      = f"{_win_rate}%" if _win_rate is not None else "n/a"
    _summary_text = (
        f"{total} picks in {lookback_days}d — "
        f"{wins} wins / {losses} losses ({_wr_str} win rate)"
        f"{_neutral_str}{_ret_str}{_dd_str}{_pending_str}"
    )

    return {
        "lookback_days":    lookback_days,
        "total_evaluated":  total,
        "pending":          int(pending_count),
        "wins":             wins,
        "losses":           losses,
        "neutral":          neutral,
        "decisive":         decisive,
        "win_rate":         _win_rate,
        "avg_return_pct":   _avg_ret,
        "avg_drawdown_pct": _avg_dd,
        "summary_text":     _summary_text,
        "by_edge_signal":   by_edge,
        "best_picks":       [_summary(r) for r in sorted_rows[:5]],
        "worst_picks":      [_summary(r) for r in sorted_rows[-5:][::-1]],
    }


def reclassify_expired_picks() -> int:
    """
    Retroactively re-evaluate all legacy 'expired' rows using the drift classification.
    Fetches the current live price for each pick, then assigns:
      won_drift       — final price >+2% above entry
      lost_drift      — final price >-2% below entry
      expired_neutral — within ±2% either direction
    Safe to call from asyncio.to_thread. Returns count of rows updated.
    """
    try:
        from data_fetcher import get_snapshot_normalized as _snap_norm
    except Exception:
        log.warning("perf_tracker: reclassify_expired: data_fetcher unavailable")
        return 0

    try:
        with _conn() as db:
            rows = db.execute(
                """SELECT id, symbol, entry_price, direction
                   FROM picks
                   WHERE status = 'expired'
                     AND entry_price IS NOT NULL
                     AND entry_price > 0""",
            ).fetchall()
    except Exception as e:
        log.warning(f"perf_tracker: reclassify_expired query failed: {e}")
        return 0

    if not rows:
        return 0

    updated = 0
    now     = time.time()
    for row in rows:
        try:
            sym     = row["symbol"]
            entry   = float(row["entry_price"])
            is_long = str(row["direction"] or "long").lower() != "short"

            snap    = _snap_norm(sym)
            live_px = _sf((snap or {}).get("last_price"))
            if live_px is None or float(live_px) <= 0:
                continue

            final_ret = ((float(live_px) - entry) / entry * 100.0 if is_long
                         else (entry - float(live_px)) / entry * 100.0)

            if final_ret > 2.0:
                new_status = "won_drift"
            elif final_ret < -2.0:
                new_status = "lost_drift"
            else:
                new_status = "expired_neutral"

            with _conn() as db:
                db.execute(
                    "UPDATE picks SET status=?, evaluated_at=? WHERE id=? AND status='expired'",
                    (new_status, now, row["id"]),
                )
            updated += 1
            log.info(
                f"perf_tracker: reclassified id={row['id']} {sym} "
                f"expired→{new_status} ret={final_ret:+.1f}%"
            )
        except Exception as e:
            log.warning(f"perf_tracker: reclassify error id={dict(row).get('id')}: {e}")

    log.info(f"perf_tracker: reclassify_expired complete: {updated}/{len(rows)} updated")
    return updated


def get_recent_picks(limit: int = 20) -> List[Dict[str, Any]]:
    """Return the N most recent picks (any status) for the dashboard."""
    try:
        with _conn() as db:
            rows = db.execute(
                "SELECT * FROM picks ORDER BY recorded_at DESC LIMIT ?", (limit,)
            ).fetchall()
    except Exception as e:
        log.warning(f"perf_tracker: recent_picks failed: {e}")
        return []

    out = []
    for r in rows:
        try:
            sigs = json.loads(r["edge_signals"] or "[]")
        except Exception:
            sigs = []
        out.append({
            "id":               r["id"],
            "symbol":           r["symbol"],
            "recorded_at":      datetime.fromtimestamp(r["recorded_at"], tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
            "entry_price":      r["entry_price"],
            "stop":             r["stop"],
            "target1":          r["target1"],
            "status":           r["status"],
            "max_return_pct":   r["max_return_pct"],
            "max_drawdown_pct": r["max_drawdown_pct"],
            "days_to_outcome":  r["days_to_outcome"],
            "edge_signals":     sigs,
            "edge_score":       r["edge_score"],
            "final_score":      r["final_score"],
            "confidence":       r["confidence"],
        })
    return out
