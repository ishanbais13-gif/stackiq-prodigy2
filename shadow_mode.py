"""
Shadow mode — log what an experimental scoring model WOULD pick, never show it.

Every background v2 scan hands its final candidate pool here. The shadow model
(models/shadow_model_v1.json, a small logistic regression trained on resolved
perf_tracker picks) ranks that same pool, and its top choice is written to the
shadow_picks table next to what the real system picked at that moment. Shadow
picks are resolved with performance_tracker's own outcome rules (7-day window,
same won/lost/drift thresholds), so the two records compare like for like.

Nothing here feeds back into the real pick, alerts, or any user-facing
endpoint. The only reader is the admin comparison endpoint.

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
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

log = logging.getLogger("stackiq")

_MODEL_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "models", "shadow_model_v1.json")
_DEDUPE_SECONDS = 20 * 3600   # one row per shadow symbol per ~trading day
_MAX_AGE_DAYS = 7.0           # same time stop as performance_tracker
_ET = timezone(timedelta(hours=-4))
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


def _conn() -> sqlite3.Connection:
    con = sqlite3.connect(_db_path(), timeout=10)
    con.row_factory = sqlite3.Row
    con.executescript(_SCHEMA)
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
    t = datetime.fromtimestamp(now_ts, _ET)
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
    prob, best, fe = scored[0]
    entry, stop = float(best["last_price"]), float(best["stop"])
    target1 = round(min(entry + 1.5 * (entry - stop), entry * 1.30), 4)  # same rule as _trade_plan_from_levels
    real_sym = str((real_out or {}).get("symbol") or "").strip().upper()
    sym = str(best.get("symbol") or "").upper()
    with _lock, _conn() as con:
        dup = con.execute("SELECT 1 FROM shadow_picks WHERE symbol=? AND recorded_at>?",
                          (sym, now - _DEDUPE_SECONDS)).fetchone()
        if dup:
            return None
        cur = con.execute(
            """INSERT INTO shadow_picks (model_version, recorded_at, symbol, entry_price, stop, target1,
               win_prob, features, pool_size, pool_top5, real_symbol, real_is_trade, same_as_real)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (m.get("version", "shadow"), now, sym, round(entry, 4), round(stop, 4), target1, round(prob, 4),
             json.dumps({k: round(v, 4) for k, v in fe.items()}), len(scored),
             json.dumps([[str(c.get("symbol")), round(p, 4)] for p, c, _ in scored[:5]]),
             real_sym or None, int(bool((real_out or {}).get("is_trade"))), int(bool(real_sym) and real_sym == sym)))
        return cur.lastrowid


def evaluate_pending(batch_size: int = 80) -> int:
    """Resolve pending shadow picks with performance_tracker's own outcome rules."""
    from performance_tracker import _resolve_outcome
    from data_fetcher import get_bars_batch
    now = time.time()
    with _conn() as con:
        rows = con.execute("SELECT * FROM shadow_picks WHERE status IN ('pending','expired_neutral') "
                           "AND recorded_at < ? ORDER BY recorded_at LIMIT ?",
                           (now - 24 * 3600, batch_size)).fetchall()
    if not rows:
        return 0
    bars_map = {str(k).upper(): v for k, v in (get_bars_batch(list({r["symbol"] for r in rows}), "1Day", 30) or {}).items()}
    updated = 0
    for r in rows:
        age_days = (now - r["recorded_at"]) / 86400.0
        future = [b for b in bars_map.get(r["symbol"]) or [] if isinstance(b, dict) and _bar_ts(b) > r["recorded_at"]]
        if not future and age_days < _MAX_AGE_DAYS:
            continue
        o = _resolve_outcome(future_bars=future, entry=r["entry_price"], stop=r["stop"], target1=r["target1"],
                             direction=r["direction"] or "long", age_days=age_days,
                             max_age_days=_MAX_AGE_DAYS, symbol=r["symbol"])
        if o["status"] == "pending" or (r["status"] == "expired_neutral" and o["status"] == "expired_neutral"):
            continue
        with _lock, _conn() as con:
            con.execute("""UPDATE shadow_picks SET status=?, evaluated_at=?, exit_return_pct=?, max_return_pct=?,
                           max_drawdown_pct=?, days_to_outcome=? WHERE id=?""",
                        (o["status"], now, o["exit_return_pct"], o["max_return_pct"], o["max_drawdown_pct"],
                         o["days_to_outcome"], r["id"]))
        updated += 1
    return updated


def _summary(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    closed = [r for r in rows if r["status"] not in ("pending",)]
    wins = sum(r["status"] in ("won", "won_drift") for r in closed)
    losses = sum(r["status"] in ("lost", "lost_drift") for r in closed)
    rets = [max(-75.0, min(75.0, r["exit_return_pct"])) for r in closed if r.get("exit_return_pct") is not None]
    return {"picks": len(rows), "closed": len(closed), "pending": len(rows) - len(closed),
            "win_rate": round(100.0 * wins / (wins + losses), 1) if wins + losses else None,
            "avg_return_pct": round(statistics.mean(rets), 2) if rets else None,
            "total_return_pct": round(sum(rets), 1) if rets else None}


def _real_all_time(con: sqlite3.Connection) -> Dict[str, Any]:
    """The real system's full record (every pick with a full trade plan), so
    picks resolved before shadow logging began still show up somewhere."""
    rows = [dict(r) for r in con.execute(
        "SELECT symbol, recorded_at, status, exit_return_pct, evaluated_at FROM picks "
        "WHERE entry_price IS NOT NULL AND target1 IS NOT NULL ORDER BY recorded_at").fetchall()]
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
            "SELECT symbol, recorded_at, status, exit_return_pct FROM picks WHERE recorded_at >= ? "
            "AND entry_price IS NOT NULL AND target1 IS NOT NULL ORDER BY recorded_at", (since,)).fetchall()]
    for r in shadow:
        # Outcome of the real pick logged closest to this scan (same symbol, within 2h).
        match = [p for p in real if p["symbol"] == r["real_symbol"] and abs(p["recorded_at"] - r["recorded_at"]) < 7200]
        r["real_status"] = match[0]["status"] if match else None
        r["real_exit_return_pct"] = match[0]["exit_return_pct"] if match else None
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
        "rows": shadow[:limit],
    }
