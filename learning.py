"""
Aurexis Evolution Engine — self-learning signal weight system.

Every pick logs its signals. Every outcome feeds back into signal weights.
The scanner applies learned weights each run, getting smarter over time.

Features:
  - Regime-aware weights (BULL / BEAR / NEUTRAL / CHOPPY)
  - Recency decay (recent outcomes count more)
  - Failure autopsy (double penalty for "lying" signals)
  - Winner fingerprinting (DNA of T2/T3 winners)
  - Confidence calibration (score bucket reliability)
  - Kelly criterion position sizing
  - Alpha decay tracking (how long each signal stays valid)
  - Multi-agent debate (momentum / value / contrarian sub-models)
  - Sector rotation bias
  - Macro event calendar (reduce conviction near FOMC/CPI)
  - Self-adjusting conviction thresholds
  - Full weight history log
"""

import json
import logging
import math
import os
import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

log = logging.getLogger("stackiq")

_DB_PATH = os.getenv("LEARNING_DB", os.path.join(
    os.getenv("DATA_DIR", os.path.dirname(os.path.abspath(__file__))),
    "learning.db"
))

_REGIMES = ("BULL", "BEAR", "NEUTRAL", "CHOPPY", "ALL")

_DEFAULT_SIGNALS = [
    "momentum", "volume", "technical", "catalyst", "sentiment",
    "risk_structure", "upside", "premover", "edge_score",
    "options_flow", "social_sentiment", "borrow_rate", "insider_buying",
    "news_score", "liquidity", "volatility", "rsi_divergence",
    "consolidation_tightness", "float_rotation", "second_deriv_momentum",
    "gap_fill_prob", "earnings_surprise_momentum", "short_interest_change",
    "sector_etf_flow", "volume_profile_strength",
]

_WEIGHT_MIN = 0.25
_WEIGHT_MAX = 4.0
_LEARNING_RATE = 0.08
_FAILURE_MULTIPLIER = 1.6   # failure penalized harder than success rewarded
_RECENCY_HALF_LIFE_DAYS = 30.0

_SCHEMA = """
CREATE TABLE IF NOT EXISTS signal_weights (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_name TEXT    NOT NULL,
    regime      TEXT    NOT NULL DEFAULT 'ALL',
    weight      REAL    NOT NULL DEFAULT 1.0,
    win_count   INTEGER NOT NULL DEFAULT 0,
    loss_count  INTEGER NOT NULL DEFAULT 0,
    total_return REAL   NOT NULL DEFAULT 0.0,
    avg_drawdown REAL   NOT NULL DEFAULT 0.0,
    alpha_decay_days REAL NOT NULL DEFAULT 5.0,
    sample_size INTEGER NOT NULL DEFAULT 0,
    updated_at  TEXT,
    UNIQUE(signal_name, regime)
);

CREATE TABLE IF NOT EXISTS pick_signal_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol      TEXT    NOT NULL,
    picked_at   REAL    NOT NULL,
    regime      TEXT,
    sector      TEXT,
    ai_score    REAL,
    signals_json TEXT,
    outcome     TEXT    DEFAULT 'pending',
    return_pct  REAL,
    days_held   INTEGER,
    hit_target_n INTEGER DEFAULT 0,
    max_drawdown REAL,
    settled_at  REAL,
    perf_id     INTEGER
);

CREATE TABLE IF NOT EXISTS weight_history (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_name TEXT    NOT NULL,
    regime      TEXT    NOT NULL,
    old_weight  REAL,
    new_weight  REAL,
    reason      TEXT,
    win_rate    REAL,
    sample_size INTEGER,
    timestamp   TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS sector_performance (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    sector      TEXT    NOT NULL,
    win_count   INTEGER NOT NULL DEFAULT 0,
    loss_count  INTEGER NOT NULL DEFAULT 0,
    total_return REAL   NOT NULL DEFAULT 0.0,
    updated_at  TEXT,
    UNIQUE(sector)
);

CREATE TABLE IF NOT EXISTS conviction_calibration (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    score_bucket TEXT   NOT NULL,
    win_count   INTEGER NOT NULL DEFAULT 0,
    total_count INTEGER NOT NULL DEFAULT 0,
    avg_return  REAL    NOT NULL DEFAULT 0.0,
    updated_at  TEXT,
    UNIQUE(score_bucket)
);

CREATE TABLE IF NOT EXISTS winner_fingerprint (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    signals_json TEXT   NOT NULL,
    return_pct  REAL,
    regime      TEXT,
    recorded_at TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS macro_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    event_name  TEXT    NOT NULL,
    event_date  TEXT    NOT NULL,
    impact      INTEGER NOT NULL DEFAULT 2,
    created_at  TEXT
);

CREATE INDEX IF NOT EXISTS idx_psl_symbol  ON pick_signal_log(symbol);
CREATE INDEX IF NOT EXISTS idx_psl_outcome ON pick_signal_log(outcome);
CREATE INDEX IF NOT EXISTS idx_psl_picked  ON pick_signal_log(picked_at);
CREATE INDEX IF NOT EXISTS idx_sw_name     ON signal_weights(signal_name, regime);
"""


# ── DB helpers ─────────────────────────────────────────────────────────────────

@contextmanager
def _conn():
    c = sqlite3.connect(_DB_PATH, timeout=10)
    c.row_factory = sqlite3.Row
    try:
        yield c
    finally:
        c.close()


def init_learning_db() -> None:
    try:
        with _conn() as c:
            c.executescript(_SCHEMA)
            c.commit()
        _seed_default_weights()
        _seed_macro_events()
        log.info("learning: DB initialized")
    except Exception as e:
        log.warning(f"learning: init failed: {e}")


def _seed_default_weights() -> None:
    try:
        with _conn() as c:
            for sig in _DEFAULT_SIGNALS:
                for regime in _REGIMES:
                    c.execute(
                        "INSERT OR IGNORE INTO signal_weights (signal_name, regime, weight, updated_at) VALUES (?,?,1.0,?)",
                        (sig, regime, datetime.now(tz=timezone.utc).isoformat())
                    )
            c.commit()
    except Exception as e:
        log.warning(f"learning: seed_weights failed: {e}")


def _seed_macro_events() -> None:
    # Seed known 2025-2026 macro dates (FOMC, CPI). Model avoids picks day-before.
    events = [
        ("FOMC", "2026-01-29", 3), ("FOMC", "2026-03-19", 3),
        ("FOMC", "2026-05-07", 3), ("FOMC", "2026-06-18", 3),
        ("FOMC", "2026-07-30", 3), ("FOMC", "2026-09-17", 3),
        ("FOMC", "2026-11-05", 3), ("FOMC", "2026-12-17", 3),
        ("CPI",  "2026-01-15", 2), ("CPI",  "2026-02-12", 2),
        ("CPI",  "2026-03-12", 2), ("CPI",  "2026-04-10", 2),
        ("CPI",  "2026-05-13", 2), ("CPI",  "2026-06-11", 2),
        ("CPI",  "2026-07-14", 2), ("CPI",  "2026-08-13", 2),
        ("CPI",  "2026-09-11", 2), ("CPI",  "2026-10-14", 2),
        ("CPI",  "2026-11-12", 2), ("CPI",  "2026-12-10", 2),
        ("JOBS", "2026-01-09", 2), ("JOBS", "2026-02-06", 2),
        ("JOBS", "2026-03-06", 2), ("JOBS", "2026-04-03", 2),
        ("JOBS", "2026-05-08", 2), ("JOBS", "2026-06-05", 2),
        ("JOBS", "2026-07-02", 2), ("JOBS", "2026-08-07", 2),
        ("JOBS", "2026-09-04", 2), ("JOBS", "2026-10-02", 2),
        ("JOBS", "2026-11-06", 2), ("JOBS", "2026-12-04", 2),
    ]
    try:
        with _conn() as c:
            for ev, dt, impact in events:
                c.execute(
                    "INSERT OR IGNORE INTO macro_events (event_name, event_date, impact, created_at) VALUES (?,?,?,?)",
                    (ev, dt, impact, datetime.now(tz=timezone.utc).isoformat())
                )
            c.commit()
    except Exception:
        log.debug("_seed_macro_events: failed, using fallback default", exc_info=True)
        pass


# ── Public: log a pick ─────────────────────────────────────────────────────────

def log_pick(
    symbol: str,
    signals: Dict[str, float],
    regime: str = "ALL",
    sector: str = "",
    ai_score: float = 5.0,
    perf_id: Optional[int] = None,
) -> Optional[int]:
    """Call immediately after a pick is selected. Returns row id."""
    try:
        with _conn() as c:
            cur = c.execute(
                """INSERT INTO pick_signal_log
                   (symbol, picked_at, regime, sector, ai_score, signals_json, outcome, perf_id)
                   VALUES (?,?,?,?,?,?,?,?)""",
                (symbol, time.time(), regime.upper(), sector or "",
                 float(ai_score), json.dumps(signals), "pending", perf_id)
            )
            row_id = int(cur.lastrowid)
            c.commit()
        return row_id
    except Exception as e:
        log.warning(f"learning: log_pick failed: {e}")
        return None


# ── Public: settle an outcome ──────────────────────────────────────────────────

def settle_outcome(
    symbol: str,
    return_pct: float,
    hit_target_n: int = 0,
    max_drawdown: float = 0.0,
    days_held: int = 1,
    perf_id: Optional[int] = None,
) -> None:
    """
    Call when evaluate_pending_picks resolves a pick.
    hit_target_n: 0=stop, 1=T1, 2=T2, 3=T3
    Triggers weight recalc if enough new outcomes.
    """
    try:
        outcome = "WIN" if return_pct > 0 else "LOSS"
        now = time.time()
        with _conn() as c:
            if perf_id:
                c.execute(
                    """UPDATE pick_signal_log SET outcome=?, return_pct=?, days_held=?,
                       hit_target_n=?, max_drawdown=?, settled_at=?
                       WHERE perf_id=? AND outcome='pending'""",
                    (outcome, return_pct, days_held, hit_target_n, max_drawdown, now, perf_id)
                )
            else:
                c.execute(
                    """UPDATE pick_signal_log SET outcome=?, return_pct=?, days_held=?,
                       hit_target_n=?, max_drawdown=?, settled_at=?
                       WHERE symbol=? AND outcome='pending'
                       ORDER BY picked_at DESC LIMIT 1""",
                    (outcome, return_pct, days_held, hit_target_n, max_drawdown, now, symbol)
                )
            c.commit()

        # Log T2/T3 winner fingerprint
        if hit_target_n >= 2:
            _record_winner_fingerprint(symbol, return_pct)

        # Update sector stats
        _update_sector_performance(symbol, return_pct)

        # Update conviction calibration bucket
        _update_conviction_calibration(symbol, return_pct)

        # Trigger weight recalculation if we have enough new outcomes
        _maybe_recalculate()

    except Exception as e:
        log.warning(f"learning: settle_outcome failed: {e}")


# ── Core learning: recalculate weights ────────────────────────────────────────

def _maybe_recalculate() -> None:
    try:
        with _conn() as c:
            row = c.execute(
                "SELECT COUNT(*) as n FROM pick_signal_log WHERE outcome != 'pending' AND settled_at > ?",
                (time.time() - 7 * 86400,)
            ).fetchone()
            recent_settled = row["n"] if row else 0
        # Recalculate every 5 new outcomes
        if recent_settled > 0 and recent_settled % 5 == 0:
            recalculate_weights()
    except Exception:
        log.debug("_maybe_recalculate: failed, using fallback default", exc_info=True)
        pass


def recalculate_weights() -> Dict[str, Any]:
    """
    Main learning function. Analyzes all settled outcomes and updates signal weights.
    Uses recency decay, failure autopsy, and regime-awareness.
    Returns summary of changes.
    """
    try:
        with _conn() as c:
            rows = c.execute(
                """SELECT symbol, picked_at, regime, signals_json, outcome,
                          return_pct, hit_target_n, max_drawdown, settled_at
                   FROM pick_signal_log
                   WHERE outcome IN ('WIN','LOSS') AND signals_json IS NOT NULL
                   ORDER BY picked_at DESC LIMIT 200"""
            ).fetchall()

        if len(rows) < 3:
            log.info("learning: not enough outcomes to recalculate weights yet")
            return {"status": "insufficient_data", "count": len(rows)}

        now = time.time()
        # Group by (signal, regime)
        signal_stats: Dict[Tuple[str, str], Dict] = {}

        for row in rows:
            try:
                signals = json.loads(row["signals_json"] or "{}")
                regime = str(row["regime"] or "ALL").upper()
                outcome = row["outcome"]
                ret = float(row["return_pct"] or 0.0)
                days_ago = (now - float(row["picked_at"] or now)) / 86400.0
                recency = math.exp(-days_ago * math.log(2) / _RECENCY_HALF_LIFE_DAYS)

                for sig_name, sig_score in signals.items():
                    sig_score = float(sig_score or 0.0)
                    if sig_score <= 0:
                        continue

                    for r in (regime, "ALL"):
                        key = (sig_name, r)
                        if key not in signal_stats:
                            signal_stats[key] = {
                                "wins": 0, "losses": 0,
                                "weighted_wins": 0.0, "weighted_losses": 0.0,
                                "total_return": 0.0, "drawdowns": [],
                                "alpha_days": [],
                            }
                        s = signal_stats[key]
                        weighted_impact = (sig_score / 10.0) * recency

                        if outcome == "WIN":
                            s["wins"] += 1
                            s["weighted_wins"] += weighted_impact
                            s["total_return"] += ret
                            s["alpha_days"].append(float(row["days_held"] or 1))
                        else:
                            s["losses"] += 1
                            s["weighted_losses"] += weighted_impact * _FAILURE_MULTIPLIER
                            s["total_return"] += ret
                            if row["max_drawdown"]:
                                s["drawdowns"].append(float(row["max_drawdown"]))

            except Exception:
                continue

        # Compute new weights and write
        changes = []
        with _conn() as c:
            for (sig_name, regime), s in signal_stats.items():
                total_w = s["weighted_wins"] + s["weighted_losses"]
                if total_w < 0.5:
                    continue

                sample = s["wins"] + s["losses"]
                win_rate = s["wins"] / max(sample, 1)
                avg_return = s["total_return"] / max(sample, 1)
                avg_drawdown = (sum(s["drawdowns"]) / len(s["drawdowns"])) if s["drawdowns"] else 0.0
                avg_alpha_days = (sum(s["alpha_days"]) / len(s["alpha_days"])) if s["alpha_days"] else 5.0

                # New weight: ratio of weighted wins to total weighted activity
                raw_weight = s["weighted_wins"] / total_w * 2.0  # scale: 0-2
                # Blend with current weight (70% old, 30% new) for stability
                old_row = c.execute(
                    "SELECT weight FROM signal_weights WHERE signal_name=? AND regime=?",
                    (sig_name, regime)
                ).fetchone()
                old_weight = float(old_row["weight"]) if old_row else 1.0
                new_weight = 0.7 * old_weight + 0.3 * raw_weight
                new_weight = max(_WEIGHT_MIN, min(_WEIGHT_MAX, new_weight))

                c.execute(
                    """INSERT INTO signal_weights
                       (signal_name, regime, weight, win_count, loss_count,
                        total_return, avg_drawdown, alpha_decay_days, sample_size, updated_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?)
                       ON CONFLICT(signal_name, regime) DO UPDATE SET
                         weight=excluded.weight, win_count=excluded.win_count,
                         loss_count=excluded.loss_count, total_return=excluded.total_return,
                         avg_drawdown=excluded.avg_drawdown, alpha_decay_days=excluded.alpha_decay_days,
                         sample_size=excluded.sample_size, updated_at=excluded.updated_at""",
                    (sig_name, regime, new_weight, s["wins"], s["losses"],
                     avg_return, avg_drawdown, avg_alpha_days, sample,
                     datetime.now(tz=timezone.utc).isoformat())
                )

                delta = new_weight - old_weight
                if abs(delta) > 0.02:
                    c.execute(
                        """INSERT INTO weight_history
                           (signal_name, regime, old_weight, new_weight, reason, win_rate, sample_size, timestamp)
                           VALUES (?,?,?,?,?,?,?,?)""",
                        (sig_name, regime, old_weight, new_weight,
                         f"wr={win_rate:.2f} ret={avg_return:+.1f}% n={sample}",
                         win_rate, sample,
                         datetime.now(tz=timezone.utc).isoformat())
                    )
                    changes.append({
                        "signal": sig_name, "regime": regime,
                        "old": round(old_weight, 3), "new": round(new_weight, 3),
                        "win_rate": round(win_rate, 3), "samples": sample,
                    })

            c.commit()

        log.info(f"learning: recalculate_weights complete — {len(changes)} signals updated")
        return {"status": "ok", "changes": len(changes), "outcomes_used": len(rows)}

    except Exception as e:
        log.warning(f"learning: recalculate_weights failed: {e}")
        return {"status": "error", "error": str(e)}


# ── Backfill from historical perf_tracker picks ───────────────────────────────

# Maps edge signal names → which learning signals they represent and at what strength
_EDGE_SIGNAL_MAP: Dict[str, Dict[str, float]] = {
    "MOMENTUM_EXPANSION":  {"momentum": 8.5, "second_deriv_momentum": 7.5, "volume": 7.0},
    "BREAKOUT_STRUCTURE":  {"technical": 8.5, "consolidation_tightness": 8.0, "momentum": 7.0},
    "VOLATILITY_EXPANSION":{"volatility": 8.0, "momentum": 6.5, "volume": 7.0},
    "RS_LEADER":           {"technical": 8.5, "momentum": 8.0, "sentiment": 7.0},
    "RS_VS_SPY":           {"technical": 8.0, "momentum": 7.5},
    "rs_vs_spy":           {"technical": 8.0, "momentum": 7.5},
    "SUPPORT_RECLAIM":     {"rsi_divergence": 8.0, "technical": 7.5},
    "RSI_OVERSOLD_BOUNCE": {"rsi_divergence": 9.0, "technical": 7.0},
    "SECTOR_ROTATION":     {"sentiment": 8.5, "catalyst": 7.5},
    "vol_surge":           {"volume": 9.0, "momentum": 7.5},
    "float_rotation":      {"float_rotation": 8.5, "volume": 8.0},
    "squeeze_potential":   {"short_interest_change": 8.5, "momentum": 7.5},
    "atr_compression":     {"consolidation_tightness": 9.0, "volatility": 7.0},
    "close_strength":      {"momentum": 8.0, "technical": 7.5},
    "near_high":           {"technical": 8.0, "momentum": 7.0},
    "catalyst":            {"catalyst": 9.0, "news_score": 8.5},
    "already_running":     {"momentum": 9.0, "second_deriv_momentum": 8.0},
    "gap_up":              {"gap_fill_prob": 8.0, "momentum": 7.5},
    "CHOPPY_BOUNCE":       {"rsi_divergence": 7.5, "technical": 7.0},
}


def backfill_from_perf_tracker() -> Dict[str, Any]:
    """
    Read all settled picks from perf_tracker.db and inject them into
    the learning engine. Reconstructs signal scores from edge_signals
    and aggregate scores. Runs weight recalculation after.
    """
    try:
        pt_path = os.getenv("PERF_TRACKER_DB", os.path.join(
            os.getenv("DATA_DIR", os.path.dirname(os.path.abspath(__file__))),
            "perf_tracker.db"
        ))
        pt_conn = sqlite3.connect(pt_path, timeout=10)
        pt_conn.row_factory = sqlite3.Row

        # entry_price IS NOT NULL excludes no-plan "watchlist" picks
        # (NO_TRADE candidates auto-recorded with no real entry/stop/target,
        # resolved via a synthetic entry) -- without this filter, untaken
        # trades were being injected into the learning engine as if they
        # were real trading outcomes, diluting signal weights with noise.
        rows = pt_conn.execute(
            """SELECT symbol, status, max_return_pct, max_drawdown_pct,
                      edge_signals, final_score, confidence, premover_score,
                      edge_score, hit_target, hit_stop, days_to_outcome, recorded_at, id
               FROM picks
               WHERE status NOT IN ('pending')
                 AND entry_price IS NOT NULL AND entry_price > 0
               ORDER BY recorded_at ASC"""
        ).fetchall()
        pt_conn.close()

        injected = 0
        skipped  = 0

        with _conn() as c:
            for row in rows:
                try:
                    sym    = str(row["symbol"] or "").upper()
                    status = str(row["status"] or "")
                    ret    = row["max_return_pct"]
                    dd     = row["max_drawdown_pct"] or 0.0
                    days   = int(row["days_to_outcome"] or 7)
                    ts     = float(row["recorded_at"] or 0)
                    perf_id = int(row["id"])

                    # Determine outcome
                    if status in ("won", "won_drift"):
                        outcome = "WIN"
                        if ret is None: ret = 3.0   # drift wins get small positive
                    elif status in ("lost", "lost_drift"):
                        outcome = "LOSS"
                        if ret is None: ret = -3.0
                    elif status == "expired_neutral":
                        outcome = "LOSS"            # treat expired as slight negative for learning
                        if ret is None: ret = -1.0
                    else:
                        skipped += 1
                        continue

                    # Skip if already backfilled
                    existing = c.execute(
                        "SELECT id FROM pick_signal_log WHERE perf_id=?", (perf_id,)
                    ).fetchone()
                    if existing:
                        skipped += 1
                        continue

                    # Build signal dict from edge_signals + aggregate scores
                    edge_sigs = []
                    try:
                        edge_sigs = json.loads(row["edge_signals"] or "[]")
                    except Exception:
                        pass

                    # Start with base scores from aggregate fields
                    fs   = float(row["final_score"]    or 5.0)
                    conf = float(row["confidence"]     or 5.0)
                    pm   = float(row["premover_score"] or 5.0)
                    es   = float(row["edge_score"]     or 3.0)

                    signals: Dict[str, float] = {
                        "technical":    fs,
                        "momentum":     pm,
                        "catalyst":     (fs + conf) / 2,
                        "sentiment":    conf,
                        "risk_structure": conf,
                        "premover":     pm,
                        "edge_score":   es,
                        "news_score":   fs,
                        "liquidity":    5.0,
                        "volatility":   5.0,
                    }

                    # Overlay signal-specific values from edge signal names
                    for sig_name in edge_sigs:
                        sig_name_up = str(sig_name).upper()
                        # Try exact match first, then uppercase
                        mapping = _EDGE_SIGNAL_MAP.get(sig_name) or _EDGE_SIGNAL_MAP.get(sig_name_up)
                        if mapping:
                            for k, v in mapping.items():
                                signals[k] = max(signals.get(k, 5.0), v)

                    # Signals NOT in edge list get slightly below-average score
                    for k in signals:
                        if signals[k] == 5.0:
                            signals[k] = 4.5

                    # Insert into pick_signal_log with outcome already known
                    hit_tgt_n = 3 if row["hit_target"] and float(ret) > 15 else \
                                2 if row["hit_target"] and float(ret) > 7  else \
                                1 if row["hit_target"] else 0

                    c.execute(
                        """INSERT INTO pick_signal_log
                           (symbol, picked_at, regime, sector, ai_score, signals_json,
                            outcome, return_pct, days_held, hit_target_n, max_drawdown, settled_at, perf_id)
                           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (sym, ts, "ALL", "", fs, json.dumps(signals),
                         outcome, float(ret), days, hit_tgt_n, float(dd),
                         ts + days * 86400, perf_id)
                    )
                    injected += 1

                except Exception as row_err:
                    log.warning(f"learning: backfill row error {dict(row).get('symbol')}: {row_err}")
                    skipped += 1
                    continue

            c.commit()

        log.info(f"learning: backfill complete — injected={injected} skipped={skipped}")

        # Now recalculate weights with full history
        result = recalculate_weights()
        return {
            "status": "ok",
            "injected": injected,
            "skipped": skipped,
            "total_historical": len(rows),
            "weight_recalc": result,
        }

    except Exception as e:
        log.warning(f"learning: backfill_from_perf_tracker failed: {e}")
        return {"status": "error", "error": str(e)}


# ── Public: get weights to apply in scanner ────────────────────────────────────

def get_weights(regime: str = "ALL") -> Dict[str, float]:
    """Returns dict of signal_name → weight for the given regime."""
    try:
        regime = regime.upper()
        with _conn() as c:
            rows = c.execute(
                """SELECT signal_name, weight FROM signal_weights
                   WHERE regime=? OR regime='ALL'
                   ORDER BY regime DESC""",  # regime-specific rows take priority
                (regime,)
            ).fetchall()

        weights: Dict[str, float] = {}
        for row in rows:
            name = row["signal_name"]
            if name not in weights:  # first row wins (regime-specific before ALL)
                weights[name] = float(row["weight"])
        return weights
    except Exception:
        log.debug("get_weights: failed, using fallback default", exc_info=True)
        return {}


def apply_weights(scores: Dict[str, float], regime: str = "ALL") -> Dict[str, float]:
    """Multiply each signal score by its learned weight. Returns new dict."""
    weights = get_weights(regime)
    return {k: v * weights.get(k, 1.0) for k, v in scores.items()}


# ── Kelly criterion position sizing ───────────────────────────────────────────

def get_kelly_position_size(regime: str = "ALL") -> float:
    """
    Calculate optimal position size using Kelly criterion based on
    actual win rate and avg win/loss from recent outcomes.
    Returns pct (2.0–12.0).
    """
    try:
        with _conn() as c:
            rows = c.execute(
                """SELECT outcome, return_pct FROM pick_signal_log
                   WHERE outcome IN ('WIN','LOSS') AND return_pct IS NOT NULL
                   AND picked_at > ?
                   ORDER BY picked_at DESC LIMIT 50""",
                (time.time() - 90 * 86400,)
            ).fetchall()

        if len(rows) < 10:
            return 6.0  # default before enough data

        # Cap at read-time: performance_tracker.py now rejects implausible
        # synthetic-entry returns going forward (>75% treated as bad data,
        # not a real outcome), but that fix doesn't retroactively clean rows
        # already written before it existed. A single uncapped legacy
        # outlier (seen in production: +5,910%, +5,697%) would otherwise
        # dominate avg_win here and push Kelly sizing straight to its max.
        _RETURN_SANITY_CAP = 75.0
        wins  = [min(abs(r["return_pct"]), _RETURN_SANITY_CAP) for r in rows if r["outcome"] == "WIN"]
        losses = [min(abs(r["return_pct"]), _RETURN_SANITY_CAP) for r in rows if r["outcome"] == "LOSS"]

        if not wins or not losses:
            return 6.0

        win_rate = len(wins) / len(rows)
        avg_win  = sum(wins)  / len(wins)
        avg_loss = sum(losses) / len(losses)

        if avg_loss < 0.1:
            return 6.0

        b = avg_win / avg_loss
        q = 1 - win_rate
        kelly = (b * win_rate - q) / b
        half_kelly = kelly / 2.0  # half-Kelly for safety
        size_pct = half_kelly * 100.0
        return float(max(2.0, min(12.0, size_pct)))

    except Exception:
        log.debug("get_kelly_position_size: failed, using fallback default", exc_info=True)
        return 6.0


# ── Winner fingerprinting ──────────────────────────────────────────────────────

def _record_winner_fingerprint(symbol: str, return_pct: float) -> None:
    try:
        with _conn() as c:
            row = c.execute(
                "SELECT signals_json, regime FROM pick_signal_log WHERE symbol=? AND outcome='WIN' ORDER BY picked_at DESC LIMIT 1",
                (symbol,)
            ).fetchone()
            if row and row["signals_json"]:
                c.execute(
                    "INSERT INTO winner_fingerprint (signals_json, return_pct, regime, recorded_at) VALUES (?,?,?,?)",
                    (row["signals_json"], return_pct, row["regime"],
                     datetime.now(tz=timezone.utc).isoformat())
                )
                c.commit()
    except Exception:
        log.debug("_record_winner_fingerprint: failed, using fallback default", exc_info=True)
        pass


def get_fingerprint_similarity(signals: Dict[str, float]) -> float:
    """
    Compare current pick's signals to average winner fingerprint.
    Returns 0.0–1.0 similarity score.
    """
    try:
        with _conn() as c:
            rows = c.execute(
                "SELECT signals_json FROM winner_fingerprint WHERE return_pct > 5 ORDER BY recorded_at DESC LIMIT 30"
            ).fetchall()

        if not rows:
            return 0.5  # neutral before data

        # Average winner vector
        avg_winner: Dict[str, float] = {}
        counts: Dict[str, int] = {}
        for row in rows:
            try:
                fp = json.loads(row["signals_json"])
                for k, v in fp.items():
                    avg_winner[k] = avg_winner.get(k, 0.0) + float(v or 0)
                    counts[k] = counts.get(k, 0) + 1
            except Exception:
                continue

        avg_winner = {k: v / counts[k] for k, v in avg_winner.items()}

        # Cosine similarity
        all_keys = set(signals.keys()) | set(avg_winner.keys())
        dot = sum(signals.get(k, 0) * avg_winner.get(k, 0) for k in all_keys)
        mag_a = math.sqrt(sum(v**2 for v in signals.values()))
        mag_b = math.sqrt(sum(v**2 for v in avg_winner.values()))

        if mag_a < 0.001 or mag_b < 0.001:
            return 0.5
        return float(max(0.0, min(1.0, dot / (mag_a * mag_b))))

    except Exception:
        log.debug("get_fingerprint_similarity: failed, using fallback default", exc_info=True)
        return 0.5


# ── Confidence calibration ─────────────────────────────────────────────────────

def _score_bucket(ai_score_0_10: float) -> str:
    s = ai_score_0_10 * 10
    if s < 45: return "0-45"
    if s < 55: return "45-55"
    if s < 62: return "55-62"
    if s < 70: return "62-70"
    if s < 75: return "70-75"
    if s < 80: return "75-80"
    if s < 85: return "80-85"
    return "85+"


def _update_conviction_calibration(symbol: str, return_pct: float) -> None:
    try:
        with _conn() as c:
            row = c.execute(
                "SELECT ai_score FROM pick_signal_log WHERE symbol=? ORDER BY picked_at DESC LIMIT 1",
                (symbol,)
            ).fetchone()
            if not row:
                return
            bucket = _score_bucket(float(row["ai_score"] or 5))
            is_win = 1 if return_pct > 0 else 0
            c.execute(
                """INSERT INTO conviction_calibration (score_bucket, win_count, total_count, avg_return, updated_at)
                   VALUES (?,?,1,?,?)
                   ON CONFLICT(score_bucket) DO UPDATE SET
                     win_count=win_count+excluded.win_count,
                     total_count=total_count+1,
                     avg_return=(avg_return*(total_count-1)+excluded.avg_return)/total_count,
                     updated_at=excluded.updated_at""",
                (bucket, is_win, return_pct, datetime.now(tz=timezone.utc).isoformat())
            )
            c.commit()
    except Exception:
        log.debug("_update_conviction_calibration: failed, using fallback default", exc_info=True)
        pass


def get_calibration_multiplier(ai_score_0_10: float) -> float:
    """
    Returns a multiplier (0.7–1.3) based on how reliable this score range has been.
    Score bucket with 70%+ win rate → boost. Below 40% → reduce.
    """
    try:
        bucket = _score_bucket(ai_score_0_10)
        with _conn() as c:
            row = c.execute(
                "SELECT win_count, total_count FROM conviction_calibration WHERE score_bucket=?",
                (bucket,)
            ).fetchone()
        if not row or row["total_count"] < 5:
            return 1.0
        win_rate = row["win_count"] / row["total_count"]
        # Linear map: 40% → 0.8, 50% → 1.0, 70% → 1.3
        mult = 0.8 + (win_rate - 0.4) * (0.5 / 0.3)
        return float(max(0.7, min(1.3, mult)))
    except Exception:
        log.debug("get_calibration_multiplier: failed, using fallback default", exc_info=True)
        return 1.0


# ── Multi-agent debate ─────────────────────────────────────────────────────────

def multi_agent_score(signals: Dict[str, float]) -> Dict[str, Any]:
    """
    Run 3 sub-model agents with different signal weightings.
    Returns consensus score and disagreement penalty.
    """
    def _weighted_avg(weights: Dict[str, float]) -> float:
        total_w = sum(weights.values())
        if total_w < 0.001:
            return 5.0
        return sum(signals.get(k, 5.0) * w for k, w in weights.items()) / total_w

    momentum_agent = {"momentum": 3.0, "volume": 2.5, "second_deriv_momentum": 2.0,
                      "premover": 1.5, "edge_score": 1.5, "technical": 1.0}
    value_agent    = {"risk_structure": 2.5, "upside": 2.0, "liquidity": 1.5,
                      "catalyst": 2.0, "earnings_surprise_momentum": 1.5, "technical": 1.0}
    contrarian_agent = {"rsi_divergence": 3.0, "short_interest_change": 2.0,
                        "consolidation_tightness": 2.0, "borrow_rate": 1.5,
                        "float_rotation": 1.5, "gap_fill_prob": 1.0}

    scores = [
        _weighted_avg(momentum_agent),
        _weighted_avg(value_agent),
        _weighted_avg(contrarian_agent),
    ]
    consensus = sum(scores) / 3.0
    std_dev = math.sqrt(sum((s - consensus)**2 for s in scores) / 3.0)
    # Penalize high disagreement
    disagreement_penalty = min(1.5, std_dev / 2.0)
    final = consensus - disagreement_penalty

    return {
        "momentum_agent": round(scores[0], 2),
        "value_agent": round(scores[1], 2),
        "contrarian_agent": round(scores[2], 2),
        "consensus": round(consensus, 2),
        "disagreement": round(std_dev, 2),
        "disagreement_penalty": round(disagreement_penalty, 2),
        "final": round(max(0, min(10, final)), 2),
    }


# ── Sector rotation ────────────────────────────────────────────────────────────

def _update_sector_performance(symbol: str, return_pct: float) -> None:
    try:
        with _conn() as c:
            row = c.execute(
                "SELECT sector FROM pick_signal_log WHERE symbol=? ORDER BY picked_at DESC LIMIT 1",
                (symbol,)
            ).fetchone()
            sector = (row["sector"] if row else None) or "UNKNOWN"
            is_win = 1 if return_pct > 0 else 0
            c.execute(
                """INSERT INTO sector_performance (sector, win_count, loss_count, total_return, updated_at)
                   VALUES (?,?,?,?,?)
                   ON CONFLICT(sector) DO UPDATE SET
                     win_count=win_count+excluded.win_count,
                     loss_count=loss_count+excluded.loss_count,
                     total_return=total_return+excluded.total_return,
                     updated_at=excluded.updated_at""",
                (sector, is_win, 1 - is_win, return_pct,
                 datetime.now(tz=timezone.utc).isoformat())
            )
            c.commit()
    except Exception:
        log.debug("_update_sector_performance: failed, using fallback default", exc_info=True)
        pass


def get_sector_bias(sector: str) -> float:
    """
    Returns a multiplier (0.8–1.2) based on recent sector performance.
    Hot sector → boost. Cold sector → reduce.
    """
    try:
        with _conn() as c:
            row = c.execute(
                "SELECT win_count, loss_count, total_return FROM sector_performance WHERE sector=?",
                (sector.upper(),)
            ).fetchone()
        if not row:
            return 1.0
        total = (row["win_count"] or 0) + (row["loss_count"] or 0)
        if total < 3:
            return 1.0
        win_rate = row["win_count"] / total
        mult = 0.8 + (win_rate - 0.4) * (0.4 / 0.3)
        return float(max(0.8, min(1.2, mult)))
    except Exception:
        log.debug("get_sector_bias: failed, using fallback default", exc_info=True)
        return 1.0


# ── Macro event awareness ──────────────────────────────────────────────────────

def get_macro_conviction_penalty() -> float:
    """
    Returns penalty to subtract from conviction (0.0–2.0) if a high-impact
    macro event is within the next 1 day.
    """
    try:
        today = datetime.now(tz=timezone.utc).date()
        tomorrow = today + timedelta(days=1)
        with _conn() as c:
            rows = c.execute(
                "SELECT event_name, event_date, impact FROM macro_events WHERE event_date BETWEEN ? AND ?",
                (today.isoformat(), tomorrow.isoformat())
            ).fetchall()
        if not rows:
            return 0.0
        max_impact = max(row["impact"] for row in rows)
        penalty = max_impact * 0.5  # impact 3 → 1.5 point penalty on 0-10 scale
        log.info(f"learning: macro penalty={penalty} events={[r['event_name'] for r in rows]}")
        return float(penalty)
    except Exception:
        log.debug("get_macro_conviction_penalty: failed, using fallback default", exc_info=True)
        return 0.0


# ── Self-adjusting conviction thresholds ──────────────────────────────────────

def get_dynamic_thresholds() -> Dict[str, float]:
    """
    Returns conviction thresholds adjusted based on recent model accuracy.
    If model has been wrong at 70+, raises thresholds. If crushing it, lowers slightly.
    Defaults: LOW<45, MODERATE<62, SOLID<75, HIGH<85, VERY HIGH≥85
    """
    try:
        with _conn() as c:
            rows = c.execute(
                """SELECT score_bucket, win_count, total_count FROM conviction_calibration
                   WHERE total_count >= 5"""
            ).fetchall()

        if not rows:
            return {"low": 45, "moderate": 62, "solid": 75, "high": 85}

        # Find where win rate dips below 50%
        adjustments = {}
        for row in rows:
            wr = row["win_count"] / row["total_count"]
            adjustments[row["score_bucket"]] = wr

        # Raise thresholds if high score buckets underperforming
        solid_thresh = 62
        if adjustments.get("62-70", 0.5) < 0.45:
            solid_thresh = 68  # raise bar for SOLID

        high_thresh = 75
        if adjustments.get("70-75", 0.5) < 0.50:
            high_thresh = 78

        vh_thresh = 85
        if adjustments.get("80-85", 0.5) < 0.55:
            vh_thresh = 87

        return {
            "low": 45,
            "moderate": solid_thresh,
            "solid": high_thresh,
            "high": vh_thresh,
        }

    except Exception:
        log.debug("get_dynamic_thresholds: failed, using fallback default", exc_info=True)
        return {"low": 45, "moderate": 62, "solid": 75, "high": 85}


# ── Status / UI data ───────────────────────────────────────────────────────────

def get_learning_status() -> Dict[str, Any]:
    """Full learning engine status for the UI panel."""
    try:
        with _conn() as c:
            total_picks = c.execute("SELECT COUNT(*) as n FROM pick_signal_log").fetchone()["n"]
            settled     = c.execute("SELECT COUNT(*) as n FROM pick_signal_log WHERE outcome!='pending'").fetchone()["n"]
            wins        = c.execute("SELECT COUNT(*) as n FROM pick_signal_log WHERE outcome='WIN'").fetchone()["n"]
            losses      = c.execute("SELECT COUNT(*) as n FROM pick_signal_log WHERE outcome='LOSS'").fetchone()["n"]

            # Clamp each row to +/-75% before averaging -- same rationale as
            # get_kelly_position_size: legacy corrupted synthetic-entry
            # returns (already in the DB, predating the sanity cap added to
            # performance_tracker.py) would otherwise dominate this all-time
            # average via a single outlier.
            avg_ret_row = c.execute(
                "SELECT AVG(MAX(-75.0, MIN(75.0, return_pct))) as v FROM pick_signal_log WHERE outcome IN ('WIN','LOSS')"
            ).fetchone()
            avg_return = float(avg_ret_row["v"] or 0) if avg_ret_row["v"] is not None else None

            # Top 5 strongest signals (by weight, ALL regime)
            top_signals = c.execute(
                """SELECT signal_name, weight, win_count, loss_count, sample_size
                   FROM signal_weights WHERE regime='ALL' AND sample_size > 0
                   ORDER BY weight DESC LIMIT 5"""
            ).fetchall()

            # Bottom 5 weakest
            weak_signals = c.execute(
                """SELECT signal_name, weight, win_count, loss_count, sample_size
                   FROM signal_weights WHERE regime='ALL' AND sample_size > 0
                   ORDER BY weight ASC LIMIT 5"""
            ).fetchall()

            # Recent weight changes
            recent_changes = c.execute(
                """SELECT signal_name, regime, old_weight, new_weight, win_rate, timestamp
                   FROM weight_history ORDER BY timestamp DESC LIMIT 10"""
            ).fetchall()

            # Sector performance
            sectors = c.execute(
                """SELECT sector, win_count, loss_count, total_return
                   FROM sector_performance WHERE win_count+loss_count >= 2
                   ORDER BY total_return DESC"""
            ).fetchall()

            # Score calibration
            calibration = c.execute(
                "SELECT score_bucket, win_count, total_count, avg_return FROM conviction_calibration ORDER BY score_bucket"
            ).fetchall()

        win_rate = wins / max(settled, 1)

        return {
            "total_picks_logged":   total_picks,
            "outcomes_settled":     settled,
            "wins":                 wins,
            "losses":               losses,
            "win_rate":             round(win_rate, 3),
            "avg_return_pct":       round(avg_return, 2) if avg_return is not None else None,
            "kelly_position_size":  round(get_kelly_position_size(), 1),
            "macro_penalty_today":  get_macro_conviction_penalty(),
            "dynamic_thresholds":   get_dynamic_thresholds(),
            "strongest_signals":    [
                {"signal": r["signal_name"], "weight": round(r["weight"], 3),
                 "wins": r["win_count"], "losses": r["loss_count"]}
                for r in top_signals
            ],
            "weakest_signals":      [
                {"signal": r["signal_name"], "weight": round(r["weight"], 3),
                 "wins": r["win_count"], "losses": r["loss_count"]}
                for r in weak_signals
            ],
            "recent_weight_changes": [
                {"signal": r["signal_name"], "regime": r["regime"],
                 "from": round(r["old_weight"], 3), "to": round(r["new_weight"], 3),
                 "win_rate": round(r["win_rate"] or 0, 2), "at": r["timestamp"]}
                for r in recent_changes
            ],
            "sector_performance":   [
                {"sector": r["sector"],
                 "wins": r["win_count"], "losses": r["loss_count"],
                 "total_return": round(r["total_return"], 1)}
                for r in sectors
            ],
            "score_calibration":    [
                {"bucket": r["score_bucket"], "wins": r["win_count"],
                 "total": r["total_count"],
                 "win_rate": round(r["win_count"] / max(r["total_count"], 1), 2),
                 "avg_return": round(r["avg_return"], 1)}
                for r in calibration
            ],
        }

    except Exception as e:
        log.warning(f"learning: get_learning_status failed: {e}")
        return {"error": str(e)}


# ── Additional signal computations ────────────────────────────────────────────

def compute_second_deriv_momentum(bars: List[Dict]) -> float:
    """
    Rate of change of momentum (0-10).
    Accelerating momentum scores high; decelerating scores low.
    """
    try:
        if len(bars) < 10:
            return 5.0
        closes = [float(b.get("c") or b.get("close") or 0) for b in bars[-10:]]
        closes = [c for c in closes if c > 0]
        if len(closes) < 6:
            return 5.0
        momentum_now = (closes[-1] - closes[-3]) / closes[-3] * 100
        momentum_prev = (closes[-4] - closes[-6]) / closes[-6] * 100
        accel = momentum_now - momentum_prev
        # Map -5 to +5 acceleration onto 0-10
        score = 5.0 + accel
        return float(max(0.0, min(10.0, score)))
    except Exception:
        log.debug("compute_second_deriv_momentum: failed, using fallback default", exc_info=True)
        return 5.0


def compute_rsi_divergence(bars: List[Dict], rsi_now: Optional[float] = None) -> float:
    """
    Bullish divergence (price lower, RSI higher) → high score.
    Bearish divergence (price higher, RSI lower) → low score.
    No divergence → neutral 5.0.
    """
    try:
        if len(bars) < 10 or rsi_now is None:
            return 5.0
        closes = [float(b.get("c") or b.get("close") or 0) for b in bars]
        closes = [c for c in closes if c > 0]
        if len(closes) < 10:
            return 5.0
        price_chg = (closes[-1] - closes[-5]) / closes[-5]
        # Estimate RSI 5 bars ago using EMA approximation
        # If we only have current RSI, we can't compute this precisely
        # Use price momentum direction as proxy
        if price_chg < -0.02 and rsi_now > 50:
            return 8.0  # bullish divergence
        if price_chg > 0.02 and rsi_now < 45:
            return 2.0  # bearish divergence
        return 5.0
    except Exception:
        log.debug("compute_rsi_divergence: failed, using fallback default", exc_info=True)
        return 5.0


def compute_consolidation_tightness(bars: List[Dict]) -> float:
    """
    How tight is the range before a potential breakout?
    Tighter consolidation → higher score (0-10).
    """
    try:
        if len(bars) < 10:
            return 5.0
        recent = bars[-10:]
        highs = [float(b.get("h") or b.get("high") or 0) for b in recent]
        lows  = [float(b.get("l") or b.get("low")  or 0) for b in recent]
        highs = [h for h in highs if h > 0]
        lows  = [l for l in lows  if l > 0]
        if not highs or not lows:
            return 5.0
        rng = (max(highs) - min(lows)) / min(lows)
        # Tight range (< 3%) → high score. Wide range (> 10%) → low score.
        score = 10 - (rng / 0.10) * 8
        return float(max(0.0, min(10.0, score)))
    except Exception:
        log.debug("compute_consolidation_tightness: failed, using fallback default", exc_info=True)
        return 5.0


def compute_float_rotation(
    volume_today: Optional[float],
    avg_volume: Optional[float],
    float_shares: Optional[float],
) -> float:
    """
    Volume as % of float. High float rotation → institutional interest → high score.
    """
    try:
        if not volume_today or not float_shares or float_shares <= 0:
            return 5.0
        rotation = volume_today / float_shares
        # 1% daily float rotation is high. Map 0-5% → 0-10
        score = rotation / 0.05 * 10
        return float(max(0.0, min(10.0, score)))
    except Exception:
        log.debug("compute_float_rotation: failed, using fallback default", exc_info=True)
        return 5.0


def compute_gap_fill_probability(bars: List[Dict]) -> float:
    """
    Assess if a recent gap is likely to fill (mean-reverting) vs expand.
    Returns 0-10: high = gap likely to continue (momentum gap), low = fill risk.
    """
    try:
        if len(bars) < 3:
            return 5.0
        prev_close = float(bars[-2].get("c") or bars[-2].get("close") or 0)
        today_open = float(bars[-1].get("o") or bars[-1].get("open") or 0)
        if prev_close <= 0 or today_open <= 0:
            return 5.0
        gap_pct = (today_open - prev_close) / prev_close
        # Positive gap with high volume = continuation. Small gap = fill risk.
        if gap_pct > 0.03:
            return 7.5   # momentum gap, likely to continue
        elif gap_pct > 0.01:
            return 5.5
        elif gap_pct < -0.03:
            return 2.5   # down gap, high fill risk
        return 5.0
    except Exception:
        log.debug("compute_gap_fill_probability: failed, using fallback default", exc_info=True)
        return 5.0


# Initialise on import
try:
    init_learning_db()
except Exception as _e:
    log.warning(f"learning: startup init failed: {_e}")
