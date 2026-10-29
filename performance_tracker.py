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
from typing import Any, Callable, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

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
    days_to_outcome     INTEGER,
    alert_sent_at       REAL,   -- unix timestamp; NULL until a new-pick alert has actually fired for this row
    exit_return_pct     REAL    -- the return actually realized at close-out, from fill_price (see the
                                 -- resolver section): target/stop price, or the open when a bar gapped past it,
                                 -- for won/lost; the time-stop close for the drift statuses. Distinct from
                                 -- max_return_pct, which is the best point reached at any time during the hold
                                 -- and can meaningfully overstate what a drift-classified pick actually banked.
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
            # Non-destructive migration for DBs that predate alert_sent_at.
            try:
                db.execute("ALTER TABLE picks ADD COLUMN alert_sent_at REAL")
                db.commit()
            except Exception:
                pass  # column already exists
            # Non-destructive migration for DBs that predate exit_return_pct.
            try:
                db.execute("ALTER TABLE picks ADD COLUMN exit_return_pct REAL")
                db.commit()
            except Exception:
                pass  # column already exists
            add_outcome_columns(db, "picks")
    except Exception as e:
        log.warning(f"perf_tracker: schema init failed: {e}")




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

        sym = symbol_upper  # alias used in the rest of the function

        # Only track real picks, not NO_TRADE responses
        if str(pick.get("status") or "OK") == "NO_TRADE":
            return None

        tp        = pick.get("trade_plan") or {}
        entry     = _sf(tp.get("entry") or pick.get("entry"))
        stop      = _sf(tp.get("stop")  or pick.get("stop"))
        direction = str(tp.get("direction") or "long").lower()

        # Dedup guard.
        # BUG (fixed): this used to be a flat 24-hour rolling window keyed on
        # symbol alone. The scanner runs once/day at a slightly drifting
        # time, so two consecutive trading-day scans of the SAME unchanged
        # setup routinely landed 24.1-27h apart -- just past the cutoff --
        # and got re-inserted as a "new" pick. Confirmed in production: ~50
        # pairs (~14.5% of all rows) with identical entry/stop/target
        # recorded 24-27h apart, plus weekend cases (Fri->Mon scans) landing
        # 40+ hours apart. A pure time window can't distinguish "same setup
        # still sitting there" from "genuinely new setup after a real price
        # move" -- so this now widens the lookback to 5 days (covers
        # weekends + scan-time drift) but only treats it as a duplicate when
        # entry AND stop are still within 1% of a recent row for this
        # symbol. A symbol that gets a real new setup a day later (price
        # actually moved) is NOT suppressed; the same unchanged level
        # lingering across scans IS.
        #
        # Cooldown (2026-10-12): the 1% price match still let the same symbol
        # in twice when its price moved between scans -- NOK and PSX were
        # each recorded twice on the same day, counting double in the
        # stats. Now any pick of the same symbol within 7 calendar days
        # (~5 trading days) is treated as the same position.
        _dedup_window_seconds = 7 * 24 * 3600
        _now_ts  = time.time()
        _cutoff_ts = _now_ts - _dedup_window_seconds

        with _conn() as db:
            _recent = db.execute(
                "SELECT id, recorded_at, alert_sent_at, entry_price, stop FROM picks "
                "WHERE symbol = ? AND recorded_at >= ? ORDER BY recorded_at DESC",
                (symbol_upper, _cutoff_ts),
            ).fetchall()

        _dupe = _recent[0] if _recent else None  # most recent pick of this symbol in the window

        if _dupe:
            _existing_id = int(_dupe[0])
            _existing_ts = float(_dupe[1])
            _age_hours   = (_now_ts - _existing_ts) / 3600
            # BUG (fixed 2026-07-17): alert-firing used to be gated purely on
            # "did this call insert a fresh row" -- so a pick that got
            # deduped here NEVER got an alert, even if its original insert
            # (for whatever reason -- a code path with no alert wiring, a
            # transient failure, anything) never actually resulted in one
            # either. needs_alert lets a duplicate still get its one alert
            # if the existing row was never marked as alerted, while a
            # true repeat (already alerted) stays suppressed as before.
            _needs_alert = _dupe[2] is None
            log.info(
                "perf_tracker: duplicate suppressed symbol=%s existing_id=%d age=%.1fh "
                "(symbol cooldown, %dd window) needs_alert=%s",
                symbol_upper, _existing_id, _age_hours, _dedup_window_seconds // 86400, _needs_alert,
            )
            return {
                "status": "duplicate_suppressed",
                "existing_id": _existing_id,
                "age_hours": _age_hours,
                "needs_alert": _needs_alert,
            }
        targets   = tp.get("targets") or []
        t1 = _sf(targets[0]) if len(targets) > 0 else None
        t2 = _sf(targets[1]) if len(targets) > 1 else None
        t3 = _sf(targets[2]) if len(targets) > 2 else None

        fill = capture_fill(sym)

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
                    recorded_at, status, entry_basis, fill_price, fill_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (sym, direction, entry, stop, t1, t2, t3,
                 edge_signals, edge_score, final_score, confidence, premover,
                 time.time(), "pending", fill["entry_basis"], fill["fill_price"], fill["fill_at"]),
            )
            row_id = int(cur.lastrowid)

        log.info(f"perf_tracker: recorded id={row_id} {sym} entry={entry} "
                 f"stop={stop} t1={t1} edge={edge_signals}")
        return row_id

    except Exception as e:
        log.warning(f"perf_tracker: record_pick failed: {e}")
        return None


def mark_alert_sent(pick_id: int) -> None:
    """
    Call after actually firing a new-pick alert for a row (fresh insert or
    a needs_alert duplicate) so record_pick's dedup guard can tell "already
    alerted" apart from "already exists" -- see needs_alert above.
    Safe to call from asyncio.to_thread; never raises.
    """
    try:
        with _conn() as db:
            db.execute("UPDATE picks SET alert_sent_at = ? WHERE id = ?", (time.time(), int(pick_id)))
            db.commit()
    except Exception as e:
        log.warning(f"perf_tracker: mark_alert_sent failed for id={pick_id}: {e}")


# ──────────────────────────────────────────────────────────────────────────────
# Public: evaluate pending picks
# ──────────────────────────────────────────────────────────────────────────────

def evaluate_pending_picks(
    min_age_hours: float = 24.0,
    batch_size:    int   = 40,
) -> int:
    """
    Resolve pending picks with resolve_rows() and write the outcome.
    Only 'pending' rows are read: once a pick resolves (including
    expired_neutral at the time stop) it is final and never re-scored.
    Should be called from a background thread.
    Returns number of picks updated.
    """
    now = time.time()
    try:
        with _conn() as db:
            rows = db.execute(
                """SELECT * FROM picks
                   WHERE status = 'pending'
                     AND recorded_at < ?
                   ORDER BY recorded_at ASC
                   LIMIT ?""",
                (now - min_age_hours * 3600.0, batch_size),
            ).fetchall()
    except Exception as e:
        log.warning(f"perf_tracker: query failed: {e}")
        return 0

    if not rows:
        return 0

    outcomes = resolve_rows(rows, now=now)
    updated = 0
    for row in rows:
        try:
            outcome = outcomes.get(row["id"])
            if outcome is None:
                continue  # still open, or its bars aren't in yet
            with _conn() as db:
                write_outcome(db, "picks", row["id"], outcome, now)
            updated += 1

            sym = row["symbol"]
            # Feed outcome into evolution engine. Use the realized exit
            # return, not the peak -- a drift win/loss that reversed before
            # close-out shouldn't teach the model a stronger signal than it
            # actually delivered.
            if outcome["status"] in ("won", "won_drift", "lost", "lost_drift"):
                try:
                    from learning import settle_outcome as _settle
                    _ret   = float(outcome["exit_return_pct"])
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

            # Fire outcome alert in background (won/lost only, not expired) --
            # but only for picks that actually got a new-pick alert in the
            # first place. Without this check, any pick that was recorded
            # but never alerted (a paused PRE_MOVER pick, or any pick whose
            # new-pick send simply failed) would still email/push users an
            # outcome for a pick they were never told about. alert_sent_at
            # is NULL until mark_alert_sent() runs after a confirmed
            # new-pick send, so it's the exact signal to gate on here.
            # outcome_alerted_at makes it once per pick, so a pick re-scored
            # by rescore_all() can never email users a second outcome.
            if (outcome["status"] in ("won", "won_drift", "lost", "lost_drift")
                    and row["alert_sent_at"] is not None and row["outcome_alerted_at"] is None):
                try:
                    from alerts import send_outcome_alert_bg
                    send_outcome_alert_bg(
                        symbol     = sym,
                        status     = outcome["status"],
                        return_pct = outcome["exit_return_pct"],
                        entry      = outcome["fill_price"],
                    )
                    with _conn() as db:
                        db.execute("UPDATE picks SET outcome_alerted_at = ? WHERE id = ?", (time.time(), row["id"]))
                except Exception as _ae:
                    log.warning(f"perf_tracker: outcome alert failed: {_ae}")
            elif outcome["status"] in ("won", "won_drift", "lost", "lost_drift") and row["alert_sent_at"] is None:
                log.info(f"perf_tracker: outcome alert skipped for id={row['id']} {sym} -- no new-pick alert was ever sent for this pick")

            log.info(
                f"perf_tracker: evaluated id={row['id']} {sym} "
                f"→ {outcome['status']} fill={outcome['fill_price']} exit={outcome['exit_price']} "
                f"ret={outcome['exit_return_pct']}% spy={outcome['spy_return_pct']}%"
            )
        except Exception as e:
            log.warning(f"perf_tracker: eval error for {dict(row).get('symbol')}: {e}")

    return updated


# ──────────────────────────────────────────────────────────────────────────────
# Outcome resolver (shared by real picks and shadow_mode)
#
#   fill    first tradable price after the scan: the live trade price when the
#           scan ran during a regular session (capture_fill), otherwise the
#           next session's open. Never the prior close.
#   exits   stop checked before target in every bar; a bar that opens beyond
#           a level fills at the open, otherwise the level price fills. Never
#           the day's high or low.
#   time    close of the MAX_SESSIONS-th session, counting the fill session.
#           Resolved rows are final.
#   SPY     SPY's price at the same fill and exit moments.
# ──────────────────────────────────────────────────────────────────────────────

NY = ZoneInfo("America/New_York")
MAX_SESSIONS = 5            # ~7 calendar days, the old time stop
RESOLVER_VERSION = 2
_LIVE_TRADE_MAX_AGE_S = 15 * 60
_INTRADAY_GIVE_UP_S = 10 * 86400  # no intraday bars for a live fill by then -> score it from the next open

# A pick with no real trade plan gets its return tracked off the next open
# with no stop or target. That price has no sanity-check behind it,
# so a bad print / stock split / bad ticker match can produce a nonsensical
# "return" (seen in production: +5,910% and +5,697% on single picks). Cap
# how far a synthetic-entry outcome is trusted before treating it as a data
# anomaly rather than a real win/loss.
_SYNTHETIC_RETURN_SANITY_CAP_PCT = 75.0

# Columns the resolver owns, on both picks and shadow_picks.
OUTCOME_COLUMNS = {
    "entry_basis":        "TEXT",     # 'live' | 'next_open' (NULL on rows from before fills were captured = next_open)
    "fill_price":         "REAL",
    "fill_at":            "REAL",
    "exit_price":         "REAL",
    "exit_at":            "REAL",
    "hit_target":         "INTEGER DEFAULT 0",
    "hit_stop":           "INTEGER DEFAULT 0",
    "spy_return_pct":     "REAL",     # SPY from fill_at to exit_at
    "spy_basis":          "TEXT",     # 'exact', or 'approx' when intraday bars were missing
    "resolver_version":   "INTEGER",
    "outcome_alerted_at": "REAL",
    "void_reason":        "TEXT",     # status 'void': 'gap_past_stop' | 'gap_past_target'
}


def add_outcome_columns(db: sqlite3.Connection, table: str) -> None:
    have = {r[1] for r in db.execute(f"PRAGMA table_info({table})").fetchall()}
    for col, typ in OUTCOME_COLUMNS.items():
        if col not in have:
            db.execute(f"ALTER TABLE {table} ADD COLUMN {col} {typ}")
    db.commit()


def capture_fill(symbol: str, now: Optional[float] = None) -> Dict[str, Any]:
    """Fill convention for a pick, decided at scan time. During a regular
    session it's the live trade price, proven live by its own timestamp
    (which also rules out holidays and early closes); otherwise the resolver
    fills it later with the next session's open."""
    now = now or time.time()
    t = datetime.fromtimestamp(now, NY)
    if t.weekday() < 5 and (9, 30) <= (t.hour, t.minute) < (16, 0):
        try:
            from data_fetcher import get_latest_trade
            lt = get_latest_trade(symbol)
        except Exception:
            lt = None
        if lt and 0 <= now - lt[1] <= _LIVE_TRADE_MAX_AGE_S:
            return {"entry_basis": "live", "fill_price": round(lt[0], 4), "fill_at": now}
    return {"entry_basis": "next_open", "fill_price": None, "fill_at": None}


def sessions_from_bars(bars: List[Dict[str, Any]], now: Optional[float] = None) -> List[Dict[str, Any]]:
    """Completed regular sessions from daily bars, oldest first. Alpaca stamps
    a daily bar 00:00 ET of its session date, so the bar's ET date is the
    session date (a scan at 1:55 AM is BEFORE that day's session)."""
    out: Dict[Any, Dict[str, Any]] = {}
    for b in bars or []:
        if not isinstance(b, dict):
            continue
        o, h, l, c = (_sf(b.get(k)) for k in ("o", "h", "l", "c"))
        ts = _bar_ts(b)
        if not ts or None in (o, h, l, c):
            continue
        d = datetime.fromtimestamp(ts, NY).date()
        open_ts = datetime(d.year, d.month, d.day, 9, 30, tzinfo=NY).timestamp()
        close_ts = open_ts + 6.5 * 3600
        if now is not None and close_ts > now:
            continue
        out[d] = {"date": d, "open_ts": open_ts, "close_ts": close_ts, "o": o, "h": h, "l": l, "c": c}
    return [out[d] for d in sorted(out)]


def resolve_trade(
    *,
    recorded_at:   float,
    entry_basis:   Optional[str],
    fill_price:    Optional[float],
    stop:          Optional[float],
    target1:       Optional[float],
    direction:     str,
    sessions:      List[Dict[str, Any]],
    scan_day_bars: Optional[List[Dict[str, Any]]] = None,
    max_sessions:  int = MAX_SESSIONS,
) -> Dict[str, Any]:
    """Walk completed sessions from the fill and decide the outcome. Pure:
    all prices come in as arguments. scan_day_bars are the intraday bars of
    the scan day, needed only for a live fill (the scan-day daily bar also
    holds the prices from before the scan). Returns {"status": "pending"}
    while the trade is still open or its data isn't in yet."""
    is_long = str(direction or "long").lower() != "short"
    stop, target1 = _sf(stop), _sf(target1)
    live = entry_basis == "live" and bool(_sf(fill_price))
    day = datetime.fromtimestamp(recorded_at, NY).date()

    if live:
        scan = next((s for s in sessions if s["date"] == day), None)
        if scan is None or not scan["open_ts"] <= recorded_at < scan["close_ts"]:
            if not any(s["date"] > day for s in sessions):
                return {"status": "pending"}  # scan session not complete yet
            live = False                      # no session that day after all
        elif scan_day_bars is None:
            return {"status": "pending", "waiting_for": "scan_day_bars"}
        else:
            fill = float(fill_price)
            rest = [b for b in scan_day_bars if _bar_ts(b) >= recorded_at]
            first = dict(scan, o=fill,
                         h=max([fill] + [_sf(b.get("h")) for b in rest if _sf(b.get("h"))]),
                         l=min([fill] + [_sf(b.get("l")) for b in rest if _sf(b.get("l"))]))
            walk, fill_at = [first] + [s for s in sessions if s["date"] > day], recorded_at
    if not live:
        walk = [s for s in sessions if s["open_ts"] >= recorded_at]
        if not walk:
            return {"status": "pending"}
        fill, fill_at = walk[0]["o"], walk[0]["open_ts"]

    def ret(px: float) -> float:
        return ((px - fill) if is_long else (fill - px)) / fill * 100.0

    best = worst = 0.0
    exit_ = None
    for i, b in enumerate(walk[:max_sessions]):
        o, h, l = b["o"], b["h"], b["l"]
        if is_long:
            if stop is not None and o <= stop:
                exit_ = ("lost", o, "gap")
            elif target1 is not None and o >= target1:
                exit_ = ("won", o, "gap")
            elif stop is not None and l <= stop:      # stop first when a bar spans both
                exit_ = ("lost", stop, "level")
            elif target1 is not None and h >= target1:
                exit_ = ("won", target1, "level")
        else:
            if stop is not None and o >= stop:
                exit_ = ("lost", o, "gap")
            elif target1 is not None and o <= target1:
                exit_ = ("won", o, "gap")
            elif stop is not None and h >= stop:
                exit_ = ("lost", stop, "level")
            elif target1 is not None and l <= target1:
                exit_ = ("won", target1, "level")
        if exit_:
            break
        best, worst = max(best, ret(h if is_long else l)), min(worst, ret(l if is_long else h))

    if exit_ and exit_[2] == "gap" and i == 0:
        # The fill itself is already past a level: the plan was broken before
        # anyone could enter, so it's no trade either way, not a win or loss.
        return {"status": "void", "void_reason": "gap_past_stop" if exit_[0] == "lost" else "gap_past_target",
                "entry_basis": "live" if live else "next_open", "fill_price": round(fill, 4), "fill_at": fill_at}

    if exit_:
        status, px, kind = exit_
        # a gap fills at the bar's open: the session open, or the fill itself on a live first bar
        exit_at = (fill_at if live and i == 0 else b["open_ts"]) if kind == "gap" else None
    elif len(walk) >= max_sessions:
        i, b = max_sessions - 1, walk[max_sessions - 1]
        px, kind, exit_at = b["c"], "time", b["close_ts"]
        r = ret(px)
        status = "won_drift" if r > 2.0 else "lost_drift" if r < -2.0 else "expired_neutral"
    else:
        return {"status": "pending"}

    r = ret(px)
    out = {
        "status":           status,
        "entry_basis":      "live" if live else "next_open",
        "fill_price":       round(fill, 4),
        "fill_at":          fill_at,
        "exit_price":       round(px, 4),
        "exit_at":          exit_at,      # None for a level exit: pinpointed from intraday bars by the caller
        "exit_kind":        kind,
        "exit_session":     b,
        "exit_on_scan_day": live and i == 0,
        "exit_return_pct":  round(r, 2),
        "max_return_pct":   round(max(best, r), 2),
        "max_drawdown_pct": round(min(worst, r), 2),
        "hit_target":       status == "won",
        "hit_stop":         status == "lost",
        "days_to_outcome":  i + 1,
    }
    # No trade plan: discard a bad print as a data anomaly rather than
    # recording a fake blowout number.
    if stop is None and target1 is None and (
            abs(r) > _SYNTHETIC_RETURN_SANITY_CAP_PCT or abs(best) > _SYNTHETIC_RETURN_SANITY_CAP_PCT
            or abs(worst) > _SYNTHETIC_RETURN_SANITY_CAP_PCT):
        log.warning("perf_tracker: discarding implausible synthetic-entry outcome fill=%.4f ret=%.1f", fill, r)
        out.update(status="expired_neutral", exit_return_pct=None, max_return_pct=None, max_drawdown_pct=None)
    return out


def _level_cross_ts(bars: List[Dict[str, Any]], stop: Optional[float], target1: Optional[float],
                    is_long: bool) -> Optional[float]:
    """End of the first intraday bar that touched the stop or target (stop first)."""
    for b in bars:
        h, l = _sf(b.get("h")), _sf(b.get("l"))
        if h is None or l is None:
            continue
        hit = ((stop is not None and l <= stop) or (target1 is not None and h >= target1)) if is_long else \
              ((stop is not None and h >= stop) or (target1 is not None and l <= target1))
        if hit:
            return _bar_ts(b) + 300.0  # 5-minute bars: the exit is certain by the bar's end
    return None


class _Intraday:
    """Cached intraday fetches for one resolve pass. Returns None when the
    fetch failed, so callers can tell 'no data' from 'no trades'."""

    def __init__(self, fetch: Optional[Callable] = None):
        if fetch is None:
            from data_fetcher import get_intraday_bars as fetch
        self._fetch, self._cache = fetch, {}

    def session(self, symbol: str, s: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
        key = (symbol, s["date"])
        if key not in self._cache:
            try:
                self._cache[key] = self._fetch(symbol, s["open_ts"], s["close_ts"])
            except Exception:
                self._cache[key] = None
        return self._cache[key]


def _price_at(sessions: List[Dict[str, Any]], intraday: "_Intraday", symbol: str,
              ts: float) -> Tuple[Optional[float], bool]:
    """First tradable price at/after ts, and whether it's exact. Session
    open/close timestamps use the daily bar; anything in between uses the
    open of the first intraday bar starting at/after ts."""
    s = next((x for x in sessions if x["open_ts"] <= ts <= x["close_ts"]), None)
    if s is None:
        return None, False
    if ts == s["open_ts"]:
        return s["o"], True
    if ts == s["close_ts"]:
        return s["c"], True
    bars = intraday.session(symbol, s) or []
    nxt = [b for b in bars if _bar_ts(b) >= ts and _sf(b.get("o"))]
    if nxt:
        return _sf(nxt[0].get("o")), True
    if bars and _bar_ts(bars[-1]) + 300 >= ts:
        return _sf(bars[-1].get("c")), True  # ts falls in the session's last bar
    # no intraday data: nearest session boundary
    return (s["o"] if ts - s["open_ts"] < s["close_ts"] - ts else s["c"]), False


def _resolve_row(row: Dict[str, Any], sessions: List[Dict[str, Any]], spy_sessions: List[Dict[str, Any]],
                 intraday: "_Intraday", now: float) -> Optional[Dict[str, Any]]:
    """Outcome columns for one picks/shadow_picks row, or None while it's open."""
    sym = str(row["symbol"]).upper()
    is_long = str(row["direction"] or "long").lower() != "short"
    has_plan = bool(_sf(row["entry_price"]))
    stop, target1 = (row["stop"], row["target1"]) if has_plan else (None, None)
    basis, fill_price = row["entry_basis"] or "next_open", row["fill_price"]
    rec = float(row["recorded_at"])

    # Scanned during a session with no live fill captured (picks from before
    # fills were captured, or a stale last trade): the first tradable price
    # is the scan-time intraday bar, not the next open.
    scan_bar = False
    if basis != "live" and any(s["open_ts"] <= rec < s["close_ts"] for s in sessions):
        px, exact = _price_at(sessions, intraday, sym, rec)
        if exact and px:
            basis, fill_price, scan_bar = "live", px, True
        elif now - rec <= _INTRADAY_GIVE_UP_S:
            return None  # intraday bars not in yet

    kw = dict(recorded_at=rec, fill_price=fill_price, stop=stop,
              target1=target1, direction=row["direction"] or "long", sessions=sessions)
    res = resolve_trade(entry_basis=basis, **kw)
    if res.get("waiting_for") == "scan_day_bars":
        scan = next(s for s in sessions if s["date"] == datetime.fromtimestamp(row["recorded_at"], NY).date())
        bars = intraday.session(sym, scan)
        if bars is not None:
            res = resolve_trade(entry_basis=basis, scan_day_bars=bars, **kw)
        elif now - float(row["recorded_at"]) > _INTRADAY_GIVE_UP_S:
            log.warning(f"perf_tracker: no intraday bars for {sym} live fill; scoring from the next open")
            res = resolve_trade(entry_basis="next_open", **kw)
    if res["status"] == "pending":
        return None
    if scan_bar and res["entry_basis"] == "live":
        res["entry_basis"] = "scan_bar"
    if res["status"] == "void":
        return dict(_RESET, status="void", void_reason=res["void_reason"], entry_basis=res["entry_basis"],
                    fill_price=res["fill_price"], fill_at=res["fill_at"], hit_target=0, hit_stop=0,
                    resolver_version=RESOLVER_VERSION)

    exact = True
    exit_at = res["exit_at"]
    if exit_at is None:  # stop/target touched inside a session: find when
        s = res["exit_session"]
        after = res["fill_at"] if res["exit_on_scan_day"] else s["open_ts"]
        bars = [b for b in intraday.session(sym, s) or [] if _bar_ts(b) >= after]
        exit_at = _level_cross_ts(bars, stop, target1, is_long)
        if exit_at is None:
            exit_at, exact = s["close_ts"], False
        exit_at = min(exit_at, s["close_ts"])

    spy_in, ok_in = _price_at(spy_sessions, intraday, "SPY", res["fill_at"])
    spy_out, ok_out = _price_at(spy_sessions, intraday, "SPY", exit_at)
    spy = round((spy_out / spy_in - 1.0) * 100.0, 2) if spy_in and spy_out else None

    return {
        "status":           res["status"],
        "entry_basis":      res["entry_basis"],
        "fill_price":       res["fill_price"],
        "fill_at":          res["fill_at"],
        "exit_price":       res["exit_price"],
        "exit_at":          exit_at,
        "exit_return_pct":  res["exit_return_pct"],
        "max_return_pct":   res["max_return_pct"],
        "max_drawdown_pct": res["max_drawdown_pct"],
        "hit_target":       int(res["hit_target"]),
        "hit_stop":         int(res["hit_stop"]),
        "days_to_outcome":  res["days_to_outcome"],
        "spy_return_pct":   spy,
        "spy_basis":        "exact" if exact and ok_in and ok_out else "approx",
        "resolver_version": RESOLVER_VERSION,
        "void_reason":      None,
    }


def _daily_bars(rows: List[Dict[str, Any]], now: float) -> Dict[str, List[Dict[str, Any]]]:
    """Raw daily bars per symbol (plus SPY) from just before its oldest pick
    to now. Not get_bars_batch: its total-bar cap keeps the oldest bars, so
    the last symbols in a batch lose their recent sessions and their picks
    look open forever."""
    from data_fetcher import get_intraday_bars
    since: Dict[str, float] = {}
    for r in rows:
        sym = str(r["symbol"]).upper()
        since[sym] = min(since.get(sym, now), float(r["recorded_at"]))
    if since:
        since["SPY"] = min(since.values())
    return {sym: get_intraday_bars(sym, ts - 5 * 86400, now, timeframe="1Day") or [] for sym, ts in since.items()}


def resolve_rows(rows: List[Any], now: Optional[float] = None,
                 bars_map: Optional[Dict[str, List[Dict[str, Any]]]] = None,
                 intraday_fetch: Optional[Callable] = None) -> Dict[int, Optional[Dict[str, Any]]]:
    """Resolve picks/shadow_picks rows. Returns {row id: outcome columns, or
    None while open}. bars_map / intraday_fetch default to live Alpaca data;
    passing them in lets an audit replay the exact same logic offline."""
    now = now or time.time()
    rows = [dict(r) for r in rows]
    if bars_map is None:
        bars_map = _daily_bars(rows, now)
    bars_map = {str(k).upper(): v for k, v in bars_map.items()}
    spy_sessions = sessions_from_bars(bars_map.get("SPY") or [], now)
    intraday = _Intraday(intraday_fetch)
    out: Dict[int, Optional[Dict[str, Any]]] = {}
    for r in rows:
        for col in OUTCOME_COLUMNS:
            r.setdefault(col, None)
        try:
            out[r["id"]] = _resolve_row(r, sessions_from_bars(bars_map.get(str(r["symbol"]).upper()) or [], now),
                                        spy_sessions, intraday, now)
        except Exception as e:
            log.warning(f"perf_tracker: resolve failed for id={r.get('id')} {r.get('symbol')}: {e}")
            out[r["id"]] = None
    return out


def write_outcome(db: sqlite3.Connection, table: str, row_id: int, outcome: Dict[str, Any],
                  evaluated_at: Optional[float]) -> None:
    cols = list(outcome)
    db.execute(f"UPDATE {table} SET {', '.join(c + '=?' for c in cols)}, evaluated_at=? WHERE id=?",
               [outcome[c] for c in cols] + [evaluated_at, row_id])


_RESET = {k: None for k in ("exit_price", "exit_at", "exit_return_pct", "max_return_pct", "max_drawdown_pct",
                            "days_to_outcome", "spy_return_pct", "spy_basis", "void_reason")}


def rescore_all(apply: bool = False, now: Optional[float] = None,
                bars_map: Optional[Dict[str, List[Dict[str, Any]]]] = None,
                intraday_fetch: Optional[Callable] = None) -> Dict[str, Any]:
    """Re-resolve every pick in picks and shadow_picks with the current
    resolver and report what changes. apply=False (default) writes nothing.
    apply=True first copies the DB to perf_tracker.db.bak-<unix time>, then
    writes every row. No alerts and no learning updates fire from here, and
    already-alerted picks are marked so a later resolution can't re-alert."""
    now = now or time.time()
    report: Dict[str, Any] = {"applied": False, "resolver_version": RESOLVER_VERSION, "tables": {}}
    with _conn() as db:
        add_outcome_columns(db, "picks")
        tables = {"picks": [dict(r) for r in db.execute("SELECT * FROM picks ORDER BY recorded_at")]}
        if db.execute("SELECT 1 FROM sqlite_master WHERE name='shadow_picks'").fetchone():
            add_outcome_columns(db, "shadow_picks")
            tables["shadow_picks"] = [dict(r) for r in db.execute(
                "SELECT * FROM shadow_picks WHERE status != 'no_trade' ORDER BY recorded_at")]
    every = [r for rows in tables.values() for r in rows]
    if bars_map is None and every:
        bars_map = _daily_bars(every, now)

    new: Dict[str, Dict[int, Optional[Dict[str, Any]]]] = {}
    for table, rows in tables.items():
        new[table] = resolve_rows(rows, now=now, bars_map=bars_map, intraday_fetch=intraday_fetch)
        report["tables"][table] = _rescore_diff(rows, new[table])

    if apply:
        path = f"{_DB_PATH}.bak-{int(now)}"
        with _conn() as src, sqlite3.connect(path) as dst:
            src.backup(dst)
        with _conn() as db:
            for table, rows in tables.items():
                for r in rows:
                    o = new[table].get(r["id"])
                    if o is None:  # open under the fixed rules
                        o = dict(_RESET, status="pending", resolver_version=RESOLVER_VERSION)
                        evaluated = None
                    else:
                        evaluated = now
                    write_outcome(db, table, r["id"], o, evaluated)
                    if table == "picks" and r.get("alert_sent_at") and r["status"] not in ("pending",):
                        db.execute("UPDATE picks SET outcome_alerted_at = COALESCE(outcome_alerted_at, ?) WHERE id = ?",
                                   (r.get("evaluated_at") or now, r["id"]))
            db.commit()
        report.update(applied=True, backup=path)
    return report


def _rescore_diff(rows: List[Dict[str, Any]], new: Dict[int, Optional[Dict[str, Any]]]) -> Dict[str, Any]:
    def stats(pairs: List[Tuple[str, Optional[float], Optional[float]]]) -> Dict[str, Any]:
        done = [p for p in pairs if p[0] not in ("pending", "no_trade", "void")]
        w = sum(p[0] in ("won", "won_drift") for p in done)
        l = sum(p[0] in ("lost", "lost_drift") for p in done)
        rets = [max(-75.0, min(75.0, p[1])) for p in done if p[1] is not None]
        ex = [max(-75.0, min(75.0, p[1])) - p[2] for p in done if p[1] is not None and p[2] is not None]
        return {"resolved": len(done), "wins": w, "losses": l,
                "win_rate": round(100.0 * w / (w + l), 1) if w + l else None,
                "avg_return_pct": round(sum(rets) / len(rets), 2) if rets else None,
                "avg_excess_vs_spy_pct": round(sum(ex) / len(ex), 2) if ex else None}

    before = [(r["status"], r.get("exit_return_pct"), None) for r in rows]
    after = [((new.get(r["id"]) or {}).get("status", "pending"), (new.get(r["id"]) or {}).get("exit_return_pct"),
              (new.get(r["id"]) or {}).get("spy_return_pct")) for r in rows]
    moves: Dict[str, int] = {}
    changed = []
    for r, (s, x, _) in zip(rows, after):
        moves[f"{r['status']} -> {s}"] = moves.get(f"{r['status']} -> {s}", 0) + 1
        old = r.get("exit_return_pct")
        if s != r["status"] or (old is None) != (x is None) or (old is not None and abs(old - x) >= 0.01):
            changed.append({"id": r["id"], "symbol": r["symbol"], "status": f"{r['status']} -> {s}",
                            "exit_return_pct": [old, x]})
    outs = [new.get(r["id"]) or {} for r in rows]
    count = lambda key: {k: sum(o.get(key) == k for o in outs) for k in sorted({o.get(key) for o in outs} - {None})}
    return {"rows": len(rows), "changed": len(changed), "before": stats(before), "after": stats(after),
            "voided": count("void_reason"), "entry_basis": count("entry_basis"),
            "resolved_to_pending": [{"id": r["id"], "symbol": r["symbol"], "was": r["status"]}
                                    for r, (s, _, _) in zip(rows, after)
                                    if s == "pending" and r["status"] not in ("pending", "no_trade")],
            "transitions": dict(sorted(moves.items(), key=lambda kv: -kv[1])),
            "biggest_changes": sorted(changed, key=lambda c: -abs((c["exit_return_pct"][0] or 0)
                                                                    - (c["exit_return_pct"][1] or 0)))[:25]}


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
                    "UPDATE picks SET status=?, evaluated_at=?, exit_return_pct=? WHERE id=? AND status='expired'",
                    (new_status, now, round(final_ret, 2), row["id"]),
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


_ensure_schema()
