"""
Shadow mode — log what an experimental scoring model WOULD pick, never show it.

Every background v2 scan hands its final candidate pool here. The shadow model
(models/shadow_model_v1.json, a small logistic regression trained on resolved
perf_tracker picks) ranks that same pool, and its top choice is written to the
shadow_picks table next to what the real system picked at that moment. Shadow
picks are filled and resolved by performance_tracker's own resolver
(capture_fill at scan time, resolve_rows after), with the same entry, exit,
time-stop and SPY rules, so the two records compare like for like.

Nothing here feeds back into the real pick or alerts. Readers: the admin
comparison endpoint, and /experimental_pick, which shows the latest shadow
pick to users as a clearly labeled experimental second pick.

Feature definitions must stay identical to the training script: every
price-derived feature uses COMPLETED daily bars only (session ended before the
scan), so the model never sees the scan day's unfinished bar.
"""

import json
import logging
import math
import os
import sqlite3
import statistics
import threading
import time
from datetime import datetime
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

log = logging.getLogger("stackiq")

_MODEL_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "models", "shadow_model_v1.json")
_COOLDOWN_SECONDS = 7 * 24 * 3600  # a symbol can't be the shadow pick again for ~5 trading days
# Below this win probability the shadow model "passes" for the scan, the way
# the real model logs NO_TRADE: the row keeps the candidate + probability
# (what it would have picked) with status 'no_trade', is never evaluated,
# never counts in win/loss stats, and doesn't put the symbol on cooldown.
_MIN_TRADE_PROB = 0.50
# When the 50% rule went live (deploy 10790762, 2026-10-16 19:16 ET). The
# 40-unique-closed comparison counts only picks from here on, for both
# sides; earlier shadow picks (including sub-50% ones) stay as history.
_RULE_SINCE = 1792192585.0
_NY = ZoneInfo("America/New_York")
_lock = threading.Lock()
_model_cache: Dict[str, Any] = {}

_SCHEMA = """
CREATE TABLE IF NOT EXISTS shadow_picks (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    model_version    TEXT    NOT NULL,
    recorded_at      REAL    NOT NULL,
    symbol           TEXT    NOT NULL,
    direction        TEXT    DEFAULT 'long',
    entry_price      REAL,
    stop             REAL,
    target1          REAL,
    win_prob         REAL,
    features         TEXT,            -- JSON
    pool_size        INTEGER,
    pool_top5        TEXT,            -- JSON [[symbol, prob], ...]
    real_symbol      TEXT,            -- what the real scan picked at this moment
    real_is_trade    INTEGER,         -- 1 if the real pick was recorded as a trade
    same_as_real     INTEGER,
    status           TEXT    DEFAULT 'pending',
    evaluated_at     REAL,
    exit_return_pct  REAL,
    max_return_pct   REAL,
    max_drawdown_pct REAL,
    days_to_outcome  INTEGER
);
CREATE INDEX IF NOT EXISTS idx_shadow_recorded ON shadow_picks(recorded_at);
CREATE INDEX IF NOT EXISTS idx_shadow_status   ON shadow_picks(status);
"""


def _db_path() -> str:
    return os.getenv("PERF_TRACKER_DB", os.path.join(
        os.getenv("DATA_DIR", os.path.dirname(os.path.abspath(__file__))), "perf_tracker.db"))


_migrated = False


def _conn() -> sqlite3.Connection:
    global _migrated
    con = sqlite3.connect(_db_path(), timeout=10)
    con.row_factory = sqlite3.Row
    con.executescript(_SCHEMA)
    if not _migrated:
        from performance_tracker import add_outcome_columns
        add_outcome_columns(con, "shadow_picks")
        _migrated = True
    return con


def _model() -> Optional[Dict[str, Any]]:
    if "m" not in _model_cache:
        try:
            with open(_MODEL_PATH) as f:
                _model_cache["m"] = json.load(f)
        except Exception as e:
            log.warning(f"shadow_mode: model unavailable: {e}")
            _model_cache["m"] = None
    return _model_cache["m"]


def _f(v: Any) -> Optional[float]:
    try:
        x = float(v)
        return x if math.isfinite(x) else None
    except (TypeError, ValueError):
        return None


def _bar_ts(b: Dict[str, Any]) -> float:
    t = b.get("t")
    if isinstance(t, (int, float)):
        return float(t)
    try:
        return datetime.fromisoformat(str(t).replace("Z", "+00:00")).timestamp()
    except Exception:
        return 0.0


def completed_bars(bars: List[Dict[str, Any]], now_ts: float) -> List[Dict[str, Any]]:
    """Daily bars whose session ended (16:00 ET ~ bar t + 20h) before now_ts."""
    return [b for b in bars or [] if isinstance(b, dict) and _bar_ts(b) + 20 * 3600 <= now_ts]


def features(cand: Dict[str, Any], now_ts: float) -> Optional[Dict[str, float]]:
    entry, stop = _f(cand.get("last_price")), _f(cand.get("stop"))
    pb = completed_bars(cand.get("daily_bars") or [], now_ts)[-21:]
    if not entry or entry <= 0 or not stop or stop >= entry or len(pb) < 15:
        return None
    closes = [_f(b.get("c")) for b in pb]
    vols = [_f(b.get("v")) or 0.0 for b in pb]
    if any(c is None or c <= 0 for c in closes):
        return None
    t = datetime.fromtimestamp(now_ts, _NY)
    return {
        "final_score": _f(cand.get("final_score")) or 0.0,
        "edge_score": _f(cand.get("edge_score")) or 0.0,
        "stop_pct": (entry - stop) / entry * 100.0,
        "ext_20d_avg": (entry / statistics.mean(closes[-20:]) - 1.0) * 100.0,
        "ret_5d": (closes[-1] / closes[-6] - 1.0) * 100.0,
        "ret_1d": (closes[-1] / closes[-2] - 1.0) * 100.0,
        "range_pct_avg": statistics.mean(((_f(b.get("h")) or 0) - (_f(b.get("l")) or 0)) / c * 100.0
                                         for b, c in zip(pb[-10:], closes[-10:])),
        "vol_ratio": vols[-1] / max(1.0, statistics.mean(vols[-20:-1])),
        "log_price": math.log(entry),
        "weekend": float(t.weekday() >= 5 or (t.weekday() == 4 and t.hour >= 16)),
    }


def in_training_range(feats: Dict[str, float], m: Dict[str, Any]) -> bool:
    """Only score trade plans shaped like the ones the model learned from. v1 put
    an 88% win probability on a 17%-stop plan -- pure extrapolation, since every
    training pick had a stop under ~10%."""
    if "max" not in m:
        return True
    hi = dict(zip(m["features"], m["max"]))["stop_pct"]
    return 0.5 <= feats["stop_pct"] <= hi


def win_prob(feats: Dict[str, float], m: Dict[str, Any]) -> float:
    lo = m.get("min") or [-math.inf] * len(m["features"])
    hi = m.get("max") or [math.inf] * len(m["features"])
    # Clip to the training range so an outlier can't produce an extreme score.
    z = m["bias"] + sum(w * (min(max(feats[n], a), b) - mu) / sd for n, w, mu, sd, a, b in
                        zip(m["features"], m["weights"], m["mean"], m["std"], lo, hi))
    return 1.0 / (1.0 + math.exp(-z))


def record(pool: List[Dict[str, Any]], real_out: Optional[Dict[str, Any]]) -> Optional[int]:
    """Rank the real scan's final candidate pool with the shadow model and log its top pick."""
    m = _model()
    if not m or not pool:
        return None
    now = time.time()
    scored = []
    for cand in pool:
        fe = features(cand, now)
        if fe is not None and in_training_range(fe, m):
            scored.append((win_prob(fe, m), cand, fe))
    if not scored:
        return None
    scored.sort(key=lambda x: -x[0])
    # Cooldown: skip symbols the shadow model already picked in the last 7
    # days and take the next-best candidate, so one stock can't count four
    # times (EBAY did, before this).
    with _conn() as con:
        recent = {r[0] for r in con.execute(
            "SELECT symbol FROM shadow_picks WHERE recorded_at > ? AND status != 'no_trade'",
            (now - _COOLDOWN_SECONDS,)).fetchall()}
    fresh = [x for x in scored if str(x[1].get("symbol") or "").upper() not in recent]
    if not fresh:
        return None
    prob, best, fe = fresh[0]
    entry, stop = float(best["last_price"]), float(best["stop"])
    target1 = round(min(entry + 1.5 * (entry - stop), entry * 1.30), 4)  # same rule as _trade_plan_from_levels
    real_sym = str((real_out or {}).get("symbol") or "").strip().upper()
    sym = str(best.get("symbol") or "").upper()
    status = "pending" if prob >= _MIN_TRADE_PROB else "no_trade"
    # entry_price/stop/target1 are the plan, built on the scan's (possibly
    # prior-close) price; the fill is captured live, the same way record_pick does.
    from performance_tracker import capture_fill
    fill = capture_fill(sym)
    with _lock, _conn() as con:
        cur = con.execute(
            """INSERT INTO shadow_picks (model_version, recorded_at, symbol, entry_price, stop, target1,
               win_prob, features, pool_size, pool_top5, real_symbol, real_is_trade, same_as_real, status,
               entry_basis, fill_price, fill_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (m.get("version", "shadow"), now, sym, round(entry, 4), round(stop, 4), target1, round(prob, 4),
             json.dumps({k: round(v, 4) for k, v in fe.items()}), len(scored),
             json.dumps([[str(c.get("symbol")), round(p, 4)] for p, c, _ in scored[:5]]),
             real_sym or None, int(bool((real_out or {}).get("is_trade"))), int(bool(real_sym) and real_sym == sym),
             status, fill["entry_basis"], fill["fill_price"], fill["fill_at"]))
        return cur.lastrowid


def evaluate_pending(batch_size: int = 80) -> int:
    """Resolve pending shadow picks with performance_tracker's resolver.
    Resolved rows are final; no_trade rows are never evaluated."""
    from performance_tracker import resolve_rows, write_outcome
    now = time.time()
    with _conn() as con:
        rows = con.execute("SELECT * FROM shadow_picks WHERE status = 'pending' "
                           "AND recorded_at < ? ORDER BY recorded_at LIMIT ?",
                           (now - 24 * 3600, batch_size)).fetchall()
    if not rows:
        return 0
    updated = 0
    for row_id, outcome in resolve_rows(rows, now=now).items():
        if outcome is None:
            continue
        with _lock, _conn() as con:
            write_outcome(con, "shadow_picks", row_id, outcome, now)
        updated += 1
    return updated


def _positions(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """One row per position: a repeat pick of the same symbol within 7 days of
    the last counted pick of that symbol is the same position, not a new
    result (the same cooldown record_pick and record() now apply to new
    picks). Keeps the earliest pick of each position."""
    last: Dict[str, float] = {}
    kept = []
    for r in sorted(rows, key=lambda r: r["recorded_at"]):
        sym = str(r.get("symbol") or "").upper()
        if sym in last and r["recorded_at"] - last[sym] < _COOLDOWN_SECONDS:
            continue
        last[sym] = r["recorded_at"]
        kept.append(r)
    return kept


def _add_spy(rows: List[Dict[str, Any]]) -> None:
    """excess_pct = pick minus SPY over the pick's own fill -> exit window
    (spy_return_pct, set by performance_tracker's resolver)."""
    for r in rows:
        r["excess_pct"] = None
        if r.get("spy_return_pct") is not None and r.get("exit_return_pct") is not None:
            r["excess_pct"] = round(max(-75.0, min(75.0, r["exit_return_pct"])) - r["spy_return_pct"], 2)


def _summary(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Stats over unique positions (see _positions); raw_* are the counts
    before repeats were collapsed."""
    no_trade = sum(r.get("status") == "no_trade" for r in rows)
    voided = sum(r.get("status") == "void" for r in rows)  # gapped past stop/target before entry
    rows = [r for r in rows if r.get("status") not in ("no_trade", "void")]
    uniq = _positions(rows)
    closed = [r for r in uniq if r["status"] not in ("pending",)]
    wins = sum(r["status"] in ("won", "won_drift") for r in closed)
    losses = sum(r["status"] in ("lost", "lost_drift") for r in closed)
    rets = [max(-75.0, min(75.0, r["exit_return_pct"])) for r in closed if r.get("exit_return_pct") is not None]
    spy = [r["spy_return_pct"] for r in closed if r.get("excess_pct") is not None]
    excess = [r["excess_pct"] for r in closed if r.get("excess_pct") is not None]
    return {"picks": len(uniq), "closed": len(closed), "pending": len(uniq) - len(closed),
            "raw_picks": len(rows), "raw_closed": sum(r["status"] != "pending" for r in rows),
            "no_trade_scans": no_trade, "voided": voided,
            "win_rate": round(100.0 * wins / (wins + losses), 1) if wins + losses else None,
            "avg_return_pct": round(statistics.mean(rets), 2) if rets else None,
            "total_return_pct": round(sum(rets), 1) if rets else None,
            # Same-window SPY benchmark over the closed positions above.
            "avg_spy_pct": round(statistics.mean(spy), 2) if spy else None,
            "avg_excess_pct": round(statistics.mean(excess), 2) if excess else None,
            "beat_spy": sum(e > 0 for e in excess), "benchmarked": len(excess)}


def _real_all_time(con: sqlite3.Connection) -> Dict[str, Any]:
    """The real system's full record (every pick with a full trade plan), so
    picks resolved before shadow logging began still show up somewhere."""
    rows = [dict(r) for r in con.execute(
        "SELECT symbol, recorded_at, status, exit_return_pct, evaluated_at, days_to_outcome, spy_return_pct FROM picks "
        "WHERE entry_price IS NOT NULL AND target1 IS NOT NULL ORDER BY recorded_at").fetchall()]
    _add_spy(rows)
    recent = sorted((r for r in rows if r["status"] != "pending" and r["evaluated_at"]),
                    key=lambda r: -r["evaluated_at"])[:10]
    return {**_summary(rows), "since": rows[0]["recorded_at"] if rows else None, "recent_closed": recent}


def compare(limit: int = 200) -> Dict[str, Any]:
    """Shadow vs real picks over the same period (since shadow logging began),
    plus the real system's all-time record for context."""
    version = (_model() or {}).get("version")
    with _conn() as con:
        real_all = _real_all_time(con)
        # Only the current model version -- earlier versions' rows stay in the
        # table but don't mix into the comparison.
        shadow = [dict(r) for r in con.execute(
            "SELECT * FROM shadow_picks WHERE model_version = ? ORDER BY recorded_at DESC", (version,)).fetchall()]
        if not shadow:
            return {"model": _model(), "shadow": _summary([]), "real": _summary([]),
                    "real_all_time": real_all, "rows": []}
        since = min(r["recorded_at"] for r in shadow)
        real = [dict(r) for r in con.execute(
            "SELECT symbol, recorded_at, status, exit_return_pct, days_to_outcome, spy_return_pct FROM picks WHERE recorded_at >= ? "
            "AND entry_price IS NOT NULL AND target1 IS NOT NULL ORDER BY recorded_at", (since,)).fetchall()]
    _add_spy(shadow)
    _add_spy(real)
    counted = {r["id"] for r in _positions([r for r in shadow if r["status"] != "no_trade"])}
    for r in shadow:
        r["repeat"] = r["status"] != "no_trade" and r["id"] not in counted  # same position as an earlier shadow pick
        # Outcome of the real pick logged closest to this scan (same symbol, within 2h).
        match = [p for p in real if p["symbol"] == r["real_symbol"] and abs(p["recorded_at"] - r["recorded_at"]) < 7200]
        r["real_status"] = match[0]["status"] if match else None
        r["real_exit_return_pct"] = match[0]["exit_return_pct"] if match else None
        r["real_spy_return_pct"] = match[0]["spy_return_pct"] if match else None
        r["features"] = json.loads(r["features"] or "{}")
        r["pool_top5"] = json.loads(r["pool_top5"] or "[]")
    m = _model() or {}
    return {
        "model": {k: m.get(k) for k in ("version", "trained_on", "trained_through", "features", "note")},
        "since": since,
        "shadow": _summary(shadow),
        "real": _summary(real),
        "real_all_time": real_all,
        "agreement_pct": round(100.0 * sum(r["same_as_real"] for r in shadow) / len(shadow), 1),
        "target_unique_closed": 40,
        # The comparison that decides it: shadow picks >= 50% vs real picks,
        # both counted from the moment the 50% rule went live.
        "rule": {
            "since": _RULE_SINCE,
            "shadow": _summary([r for r in shadow if r["recorded_at"] >= _RULE_SINCE and r["status"] != "no_trade"
                                and r["win_prob"] >= _MIN_TRADE_PROB]),
            "real": _summary([r for r in real if r["recorded_at"] >= _RULE_SINCE]),
        },
        "min_trade_prob": _MIN_TRADE_PROB,
        "rows": shadow[:limit],
    }


def latest_for_display() -> Dict[str, Any]:
    """The current model's most recent scan result plus its track record, for
    the user-facing "experimental pick" card. Read-only: same rows and stats
    as compare(); win_prob is left out because it isn't calibrated yet."""
    version = (_model() or {}).get("version")
    with _conn() as con:
        rows = [dict(r) for r in con.execute(
            "SELECT * FROM shadow_picks WHERE model_version = ? ORDER BY recorded_at DESC", (version,)).fetchall()]
    if not rows:
        return {"available": False}
    _add_spy(rows)
    r = rows[0]
    passed = r["status"] == "no_trade"
    return {
        "available": True,
        "model_version": version,
        "recorded_at": r["recorded_at"],
        "passed": passed,
        "symbol": None if passed else r["symbol"],
        "direction": r.get("direction") or "long",
        "entry": None if passed else r["entry_price"],
        "stop": None if passed else r["stop"],
        "target": None if passed else r["target1"],
        "fill_price": None if passed else r.get("fill_price"),
        # Scanned outside market hours: scored from the next session's open,
        # so entry_price above is only the last close.
        "entry_at_next_open": not passed and r.get("entry_basis") == "next_open",
        "status": r["status"],
        "track_record": {**_summary(rows), "since": min(x["recorded_at"] for x in rows)},
    }
