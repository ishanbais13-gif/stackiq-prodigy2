"""
brain.py — Self-learning signal weight engine.

Every pre-mover pick gets recorded. Every day the brain checks what happened
to each pick (1-day and 3-day price change). Over time it figures out which
signals actually predict big moves and boosts their weight — quietly, automatically.

Architecture:
  record_premover_pick()   → called when scanner produces a pick
  run_outcome_checks()     → called daily, fetches current prices, marks wins/losses
  recalibrate_weights()    → called after outcomes recorded, updates multipliers
  get_learned_weights()    → called by scorer before each scan run
  get_brain_stats()        → powers /scan/brain_stats endpoint
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

log = logging.getLogger("stackiq")

_BRAIN_LOCK = threading.Lock()

# Win thresholds: what counts as a "successful" pick
WIN_THRESHOLD_1D = 0.02   # +2% within 1 trading day  (realistic for large-caps)
WIN_THRESHOLD_3D = 0.04   # +4% within 3 trading days
MIN_SAMPLES = 8            # don't adjust a signal's weight until this many picks have fired it


def _db_path() -> str:
    base = os.getenv("DATA_DIR", "/app/data")
    if not os.path.isdir(base):
        base = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base, "brain.db")


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(_db_path(), timeout=15, check_same_thread=False)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    return c


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

def init_brain_db() -> None:
    with _conn() as db:
        db.executescript("""
        CREATE TABLE IF NOT EXISTS premover_picks (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol       TEXT    NOT NULL,
            picked_at    TEXT    NOT NULL,   -- ISO-8601 UTC
            score        REAL,
            price_at_pick REAL,
            signals_json TEXT,               -- {signal_name: {pts, ...}, ...}
            tags_json    TEXT,               -- ["penny","pre_surge",...]
            vol_ratio    REAL,
            float_m      REAL,
            short_pct    REAL
        );

        CREATE TABLE IF NOT EXISTS outcomes (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            pick_id      INTEGER NOT NULL REFERENCES premover_picks(id),
            checked_at   TEXT    NOT NULL,
            days_elapsed REAL,
            price_then   REAL,
            change_pct   REAL,
            is_win       INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS signal_stats (
            signal_name       TEXT PRIMARY KEY,
            appearances       INTEGER DEFAULT 0,
            wins              INTEGER DEFAULT 0,
            win_rate          REAL    DEFAULT 0.5,
            learned_multiplier REAL   DEFAULT 1.0,
            last_updated      TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_pp_symbol    ON premover_picks(symbol);
        CREATE INDEX IF NOT EXISTS idx_pp_picked_at ON premover_picks(picked_at);
        CREATE INDEX IF NOT EXISTS idx_out_pick_id  ON outcomes(pick_id);
        """)
    log.info(f"brain: DB ready at {_db_path()}")
    # One-time: clear corrupt outcomes where price_then = price_at_pick (change_pct always 0)
    try:
        wiped = db.execute("""
            DELETE FROM outcomes WHERE id IN (
                SELECT o.id FROM outcomes o
                JOIN premover_picks p ON p.id = o.pick_id
                WHERE ABS(o.price_then - p.price_at_pick) < 0.0001
            )
        """).rowcount
        if wiped:
            db.execute("DELETE FROM signal_stats")
            log.info(f"brain: wiped {wiped} corrupt zero-change outcomes and reset signal_stats")
    except Exception as _e:
        log.warning(f"brain: corrupt outcome cleanup failed: {_e}")


# ---------------------------------------------------------------------------
# Recording picks
# ---------------------------------------------------------------------------

def record_premover_pick(result: Dict[str, Any]) -> Optional[int]:
    """Persist a scanner result. Returns the new pick_id (or None on error)."""
    try:
        signals = result.get("signals") or {}
        # Pull vol_ratio out of whichever signal recorded it
        vol_ratio = None
        for key in ("quiet_accumulation", "vol_surge"):
            v = (signals.get(key) or {}).get("ratio")
            if v is not None:
                vol_ratio = v
                break

        with _BRAIN_LOCK:
            with _conn() as db:
                cur = db.execute(
                    """INSERT INTO premover_picks
                       (symbol, picked_at, score, price_at_pick,
                        signals_json, tags_json, vol_ratio, float_m, short_pct)
                       VALUES (?,?,?,?,?,?,?,?,?)""",
                    (
                        result.get("symbol", ""),
                        datetime.now(timezone.utc).isoformat(),
                        result.get("score"),
                        result.get("price"),
                        json.dumps(signals),
                        json.dumps(result.get("tags") or []),
                        vol_ratio,
                        result.get("float_m"),
                        result.get("short_pct"),
                    ),
                )
                return cur.lastrowid
    except Exception as e:
        log.warning(f"brain.record_pick: {e}")
        return None


# ---------------------------------------------------------------------------
# Outcome checking
# ---------------------------------------------------------------------------

def get_pending_checks() -> List[Dict[str, Any]]:
    """
    Picks that need a 1-day or 3-day price check.
    A pick gets checked twice: ~1 day after and ~3 days after.
    """
    now = datetime.now(timezone.utc)
    one_day_ago  = (now - timedelta(hours=20)).isoformat()
    six_days_ago = (now - timedelta(days=6)).isoformat()

    try:
        with _conn() as db:
            rows = db.execute("""
                SELECT p.id, p.symbol, p.picked_at, p.price_at_pick,
                       COUNT(o.id) AS checks_done
                FROM premover_picks p
                LEFT JOIN outcomes o ON o.pick_id = p.id
                WHERE p.picked_at <= ?
                  AND p.picked_at >= ?
                  AND p.price_at_pick IS NOT NULL
                GROUP BY p.id
                HAVING checks_done < 2
                ORDER BY p.picked_at ASC
            """, (one_day_ago, six_days_ago)).fetchall()
            return [dict(r) for r in rows]
    except Exception as e:
        log.warning(f"brain.pending_checks: {e}")
        return []


def record_outcome(pick_id: int, price_now: float) -> None:
    """Record what happened to pick `pick_id` at the current price."""
    try:
        with _BRAIN_LOCK:
            with _conn() as db:
                pick = db.execute(
                    "SELECT price_at_pick, picked_at FROM premover_picks WHERE id=?",
                    (pick_id,),
                ).fetchone()
                if not pick or not pick["price_at_pick"]:
                    return

                entry = float(pick["price_at_pick"])
                picked_at = datetime.fromisoformat(pick["picked_at"])
                days = (datetime.now(timezone.utc) - picked_at).total_seconds() / 86400

                change = (price_now - entry) / entry
                threshold = WIN_THRESHOLD_1D if days <= 1.5 else WIN_THRESHOLD_3D
                is_win = 1 if change >= threshold else 0

                db.execute(
                    """INSERT INTO outcomes
                       (pick_id, checked_at, days_elapsed, price_then, change_pct, is_win)
                       VALUES (?,?,?,?,?,?)""",
                    (
                        pick_id,
                        datetime.now(timezone.utc).isoformat(),
                        round(days, 2),
                        price_now,
                        round(change, 4),
                        is_win,
                    ),
                )
    except Exception as e:
        log.warning(f"brain.record_outcome: {e}")


def _get_all_unchecked_picks() -> List[Dict[str, Any]]:
    """All picks with < 2 outcomes, no date window — for backfill."""
    try:
        with _conn() as db:
            rows = db.execute("""
                SELECT p.id, p.symbol, p.picked_at, p.price_at_pick,
                       COUNT(o.id) AS checks_done
                FROM premover_picks p
                LEFT JOIN outcomes o ON o.pick_id = p.id
                WHERE p.price_at_pick IS NOT NULL
                  AND p.price_at_pick > 0
                GROUP BY p.id
                HAVING checks_done < 2
                ORDER BY p.picked_at ASC
            """).fetchall()
            return [dict(r) for r in rows]
    except Exception as e:
        log.warning(f"brain.all_unchecked: {e}")
        return []


def backfill_all_outcomes() -> Dict[str, int]:
    """
    Process ALL historical picks with < 2 outcome checks.
    Uses get_bars_batch (last ~200 trading days = ~10 months of history).
    Runs in symbol batches of 25 to stay within Alpaca limits.
    """
    all_picks = _get_all_unchecked_picks()
    if not all_picks:
        return {"processed": 0, "recorded": 0, "skipped_no_bars": 0}

    try:
        from data_fetcher import get_bars_batch
    except ImportError:
        log.warning("brain.backfill: data_fetcher not available")
        return {"processed": 0, "recorded": 0, "skipped_no_bars": 0}

    BATCH = 25
    now = datetime.now(timezone.utc)
    recorded = 0
    skipped = 0

    all_syms = list({p["symbol"] for p in all_picks})
    bars_by_sym: Dict[str, List] = {}

    for i in range(0, len(all_syms), BATCH):
        batch_syms = all_syms[i:i + BATCH]
        try:
            chunk = get_bars_batch(batch_syms, "1Day", 200) or {}
            bars_by_sym.update(chunk)
        except Exception as e:
            log.warning(f"brain.backfill: bars fetch error for batch {i}: {e}")

    for pick in all_picks:
        sym = pick["symbol"]
        entry = float(pick.get("price_at_pick") or 0)
        if not entry:
            continue

        picked_at = datetime.fromisoformat(str(pick["picked_at"]))
        if picked_at.tzinfo is None:
            picked_at = picked_at.replace(tzinfo=timezone.utc)

        days_since = (now - picked_at).total_seconds() / 86400
        if days_since < 1.0:
            continue

        bars = bars_by_sym.get(sym) or []
        after_bars = []
        for b in bars:
            try:
                bar_t = b.get("t") or b.get("time") or b.get("timestamp") or ""
                bar_dt = datetime.fromisoformat(str(bar_t).replace("Z", "+00:00"))
                if bar_dt.tzinfo is None:
                    bar_dt = bar_dt.replace(tzinfo=timezone.utc)
                if bar_dt > picked_at:
                    after_bars.append((bar_dt, b))
            except Exception:
                continue

        if not after_bars:
            skipped += 1
            continue

        after_bars.sort(key=lambda x: x[0])
        checks_done = pick.get("checks_done", 0)

        if checks_done == 0:
            _, bar = after_bars[0]
            price = float(bar.get("h") or bar.get("c") or 0)
        else:
            window = after_bars[:3]
            price = max(float(b.get("h") or b.get("c") or 0) for _, b in window)

        if not price:
            continue

        record_outcome(pick["id"], price)
        recorded += 1

    log.info(f"brain.backfill: recorded={recorded} skipped={skipped} total_picks={len(all_picks)}")
    if recorded > 0:
        recalibrate_weights()

    return {"processed": len(all_picks), "recorded": recorded, "skipped_no_bars": skipped}


def run_outcome_checks() -> int:
    """
    Evaluate pending picks using historical daily bars from AFTER the pick date.
    Uses the highest close in the 1-5 days following the pick so we measure
    actual next-day movement, not the same closing price the pick was recorded at.
    """
    pending = get_pending_checks()
    if not pending:
        return 0

    try:
        from data_fetcher import get_bars_batch
    except ImportError:
        log.warning("brain: data_fetcher not available, skipping outcome checks")
        return 0

    syms = list({p["symbol"] for p in pending})
    try:
        # Pull last 10 daily bars — enough to cover 1-day and 3-day windows
        bars_by_sym = get_bars_batch(syms, "1Day", 10) or {}
    except Exception as e:
        log.warning(f"brain.outcome_checks: bars fetch error: {e}")
        return 0

    now = datetime.now(timezone.utc)
    recorded = 0

    for pick in pending:
        sym = pick["symbol"]
        entry = float(pick.get("price_at_pick") or 0)
        if not entry:
            continue

        picked_at = datetime.fromisoformat(str(pick["picked_at"]))
        if picked_at.tzinfo is None:
            picked_at = picked_at.replace(tzinfo=timezone.utc)

        days_since = (now - picked_at).total_seconds() / 86400

        # Need at least 1 full trading day before evaluating
        if days_since < 1.0:
            continue

        bars = bars_by_sym.get(sym) or []
        # Filter to bars strictly AFTER the pick date
        after_bars = []
        for b in bars:
            try:
                bar_t = b.get("t") or b.get("time") or b.get("timestamp") or ""
                bar_dt = datetime.fromisoformat(str(bar_t).replace("Z", "+00:00"))
                if bar_dt.tzinfo is None:
                    bar_dt = bar_dt.replace(tzinfo=timezone.utc)
                if bar_dt > picked_at:
                    after_bars.append((bar_dt, b))
            except Exception:
                continue

        if not after_bars:
            continue

        after_bars.sort(key=lambda x: x[0])

        # For 1-day check: use next day's close (high if available to capture intraday peak)
        # For 3-day check: use best close in first 3 bars
        checks_done = pick.get("checks_done", 0)

        if checks_done == 0:
            # First check: next trading day's close/high
            _, bar = after_bars[0]
            price = float(bar.get("h") or bar.get("c") or 0)
        else:
            # Second check: best close across first 3 days after pick
            window = after_bars[:3]
            price = max(float(b.get("h") or b.get("c") or 0) for _, b in window)

        if not price:
            continue

        record_outcome(pick["id"], price)
        recorded += 1

    log.info(f"brain: recorded {recorded} outcomes for {len(pending)} pending picks")
    if recorded > 0:
        recalibrate_weights()
    return recorded


# ---------------------------------------------------------------------------
# Weight learning
# ---------------------------------------------------------------------------

def recalibrate_weights() -> Dict[str, float]:
    """
    Compute win rates per signal from all resolved picks.
    Update learned_multiplier: signals that predict wins get boosted (up to 2.5x),
    signals that predict losses get shrunk (down to 0.4x).
    """
    try:
        with _BRAIN_LOCK:
            with _conn() as db:
                rows = db.execute("""
                    SELECT p.signals_json, o.is_win
                    FROM premover_picks p
                    JOIN outcomes o ON o.pick_id = p.id
                    WHERE o.days_elapsed BETWEEN 0.5 AND 4.5
                    ORDER BY p.picked_at DESC
                    LIMIT 500
                """).fetchall()

                if len(rows) < MIN_SAMPLES:
                    log.info(f"brain: {len(rows)} resolved picks — need {MIN_SAMPLES} to recalibrate")
                    return get_learned_weights()

                total = len(rows)
                total_wins = sum(r["is_win"] for r in rows)
                baseline = total_wins / total if total else 0.5

                appearances: Dict[str, int] = {}
                wins_by_sig: Dict[str, int] = {}

                for row in rows:
                    try:
                        sigs = json.loads(row["signals_json"] or "{}")
                    except Exception:
                        sigs = {}
                    for sig in sigs:
                        appearances[sig] = appearances.get(sig, 0) + 1
                        if row["is_win"]:
                            wins_by_sig[sig] = wins_by_sig.get(sig, 0) + 1

                now_str = datetime.now(timezone.utc).isoformat()
                multipliers: Dict[str, float] = {}

                for sig, count in appearances.items():
                    sig_wins = wins_by_sig.get(sig, 0)
                    win_rate = sig_wins / count

                    if count < MIN_SAMPLES:
                        mult = 1.0
                    else:
                        raw = win_rate / max(baseline, 0.05)
                        mult = max(0.4, min(2.5, raw))

                    multipliers[sig] = mult

                    db.execute("""
                        INSERT INTO signal_stats
                            (signal_name, appearances, wins, win_rate, learned_multiplier, last_updated)
                        VALUES (?,?,?,?,?,?)
                        ON CONFLICT(signal_name) DO UPDATE SET
                            appearances=excluded.appearances,
                            wins=excluded.wins,
                            win_rate=excluded.win_rate,
                            learned_multiplier=excluded.learned_multiplier,
                            last_updated=excluded.last_updated
                    """, (sig, count, sig_wins, win_rate, mult, now_str))

                log.info(
                    f"brain: recalibrated | {total} picks | baseline win_rate={baseline:.1%} | "
                    + " ".join(f"{k}={v:.2f}x" for k, v in sorted(multipliers.items(), key=lambda x: -x[1])[:6])
                )

                # Also pull in main scanner signals (MOMENTUM_EXPANSION, VOLATILITY_EXPANSION,
                # BREAKOUT_STRUCTURE) from perf_tracker.db — these were never tracked before.
                try:
                    import sqlite3 as _sql2
                    _pt_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "perf_tracker.db")
                    pt = _sql2.connect(_pt_path, timeout=5)
                    pt.row_factory = _sql2.Row
                    # entry_price IS NOT NULL excludes no-plan "watchlist"
                    # picks (NO_TRADE candidates auto-recorded with no real
                    # entry/stop/target, resolved via a synthetic entry) --
                    # without this filter, untaken trades were diluting
                    # signal win-rate math with noise from picks that were
                    # never actually traded.
                    pt_rows = pt.execute("""
                        SELECT edge_signals, status, max_return_pct
                        FROM picks
                        WHERE status IN ('won','won_drift','lost','lost_drift','expired_neutral')
                          AND entry_price IS NOT NULL AND entry_price > 0
                    """).fetchall()
                    pt.close()

                    for pr in pt_rows:
                        try:
                            sigs = json.loads(pr["edge_signals"] or "[]")
                        except Exception:
                            continue
                        is_win = pr["status"] in ("won", "won_drift")
                        for sig in sigs:
                            appearances[sig] = appearances.get(sig, 0) + 1
                            if is_win:
                                wins_by_sig[sig] = wins_by_sig.get(sig, 0) + 1

                    # Re-run multiplier computation with merged data
                    for sig, count in appearances.items():
                        sig_wins = wins_by_sig.get(sig, 0)
                        win_rate = sig_wins / count
                        if count < MIN_SAMPLES:
                            mult = 1.0
                        else:
                            raw = win_rate / max(baseline, 0.05)
                            mult = max(0.4, min(2.5, raw))
                        multipliers[sig] = mult
                        db.execute("""
                            INSERT INTO signal_stats
                                (signal_name, appearances, wins, win_rate, learned_multiplier, last_updated)
                            VALUES (?,?,?,?,?,?)
                            ON CONFLICT(signal_name) DO UPDATE SET
                                appearances=excluded.appearances,
                                wins=excluded.wins,
                                win_rate=excluded.win_rate,
                                learned_multiplier=excluded.learned_multiplier,
                                last_updated=excluded.last_updated
                        """, (sig, count, sig_wins, win_rate, mult, now_str))
                    log.info(f"brain: merged perf_tracker signals — {len(pt_rows)} additional picks")
                except Exception as _pt_err:
                    log.warning(f"brain: perf_tracker signal merge failed: {_pt_err}")

                # Kick off NN retraining in a background thread after signal recalibration
                _maybe_trigger_nn_training()

                return multipliers

    except Exception as e:
        log.warning(f"brain.recalibrate: {e}")
        return {}


_NN_MIN_SAMPLES = 20   # don't bother training until this many resolved picks


def _maybe_trigger_nn_training() -> None:
    """Fire-and-forget: retrain the NN scorer in a background thread if enough data."""
    import threading as _threading
    import sqlite3 as _sqlite3

    perf_db = os.getenv("PERF_TRACKER_DB", os.path.join(os.path.dirname(os.path.abspath(__file__)), "perf_tracker.db"))

    try:
        con = _sqlite3.connect(perf_db, timeout=5)
        row = con.execute(
            "SELECT COUNT(*) FROM picks WHERE status IN ('won','won_drift','lost','lost_drift')"
        ).fetchone()
        con.close()
        resolved = int(row[0]) if row else 0
    except Exception:
        resolved = 0

    if resolved < _NN_MIN_SAMPLES:
        log.info(f"brain: NN training skipped — only {resolved} resolved picks (need {_NN_MIN_SAMPLES})")
        return

    def _train():
        try:
            from ml.trainer import run_training
            result = run_training()
            log.info(f"brain: NN retrain complete → {result}")
        except Exception as e:
            log.warning(f"brain: NN retrain failed: {e}")

    t = _threading.Thread(target=_train, daemon=True)
    t.start()
    log.info(f"brain: NN retrain triggered ({resolved} resolved picks)")


def get_learned_weights() -> Dict[str, float]:
    """Return current multipliers. Unknown signals default to 1.0 (neutral)."""
    try:
        with _conn() as db:
            rows = db.execute(
                "SELECT signal_name, learned_multiplier FROM signal_stats"
            ).fetchall()
            return {r["signal_name"]: float(r["learned_multiplier"]) for r in rows}
    except Exception:
        return {}


# ---------------------------------------------------------------------------
# Stats endpoint
# ---------------------------------------------------------------------------

def get_brain_stats() -> Dict[str, Any]:
    try:
        with _conn() as db:
            total_picks    = db.execute("SELECT COUNT(*) FROM premover_picks").fetchone()[0]
            total_outcomes = db.execute("SELECT COUNT(*) FROM outcomes").fetchone()[0]
            wins           = db.execute("SELECT COUNT(*) FROM outcomes WHERE is_win=1").fetchone()[0]
            win_rate       = wins / total_outcomes if total_outcomes else None

            best_pick = db.execute("""
                SELECT p.symbol, p.score, MAX(o.change_pct) AS best_chg
                FROM premover_picks p JOIN outcomes o ON o.pick_id = p.id
                GROUP BY p.id ORDER BY best_chg DESC LIMIT 1
            """).fetchone()

            sig_rows = db.execute("""
                SELECT signal_name, appearances, wins, win_rate, learned_multiplier
                FROM signal_stats ORDER BY learned_multiplier DESC
            """).fetchall()

            recent = db.execute("""
                SELECT p.symbol, p.score, p.picked_at, p.price_at_pick,
                       o.change_pct, o.is_win, o.days_elapsed
                FROM premover_picks p
                LEFT JOIN outcomes o ON o.pick_id = p.id
                ORDER BY p.picked_at DESC LIMIT 30
            """).fetchall()

            return {
                "total_picks_recorded": total_picks,
                "outcomes_checked": total_outcomes,
                "overall_win_rate_pct": round(win_rate * 100, 1) if win_rate is not None else None,
                "best_ever": {
                    "symbol": best_pick["symbol"],
                    "change_pct": round(best_pick["best_chg"] * 100, 1),
                } if best_pick else None,
                "signal_weights": [
                    {
                        "signal": r["signal_name"],
                        "appearances": r["appearances"],
                        "win_rate_pct": round(r["win_rate"] * 100, 1),
                        "multiplier": round(r["learned_multiplier"], 2),
                        "status": (
                            "boosted" if r["learned_multiplier"] > 1.15
                            else "penalized" if r["learned_multiplier"] < 0.85
                            else "neutral"
                        ),
                    }
                    for r in sig_rows
                ],
                "recent_picks": [
                    {
                        "symbol": r["symbol"],
                        "score": r["score"],
                        "picked_at": r["picked_at"][:10] if r["picked_at"] else None,
                        "entry": r["price_at_pick"],
                        "change_pct": round(r["change_pct"] * 100, 1) if r["change_pct"] is not None else None,
                        "days": r["days_elapsed"],
                        "won": bool(r["is_win"]) if r["is_win"] is not None else None,
                    }
                    for r in recent
                ],
            }
    except Exception as e:
        log.warning(f"brain.get_stats: {e}")
        return {"error": str(e)}
