from __future__ import annotations

import json
import os
import threading
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple


_LOCK = threading.Lock()
_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
_DATA_DIR = os.path.join(_BASE_DIR, "data")
_MEMORY_PATH = os.path.join(_DATA_DIR, "strategy_memory.json")

_DEFAULT_STORE: Dict[str, Any] = {
    "trades": [],
    "patterns": {},
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_f(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        if v != v:
            return float(default)
        return float(v)
    except Exception:
        return float(default)


def _safe_opt_f(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        v = float(x)
        if v != v:
            return None
        return float(v)
    except Exception:
        return None


def _parse_iso_dt(x: Any) -> Optional[datetime]:
    try:
        s = str(x or "").strip()
        if not s:
            return None
        s = s.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _bucket_score(x: Any) -> str:
    v = _safe_f(x, 0.0)
    if v >= 8.0:
        return ">=8"
    if v >= 6.0:
        return "6-8"
    return "<6"


def _pattern_key(*, regime: str, technical: Any, risk_structure: Any) -> str:
    return "|".join([
        str(regime or "unknown").strip().lower() or "unknown",
        _bucket_score(technical),
        _bucket_score(risk_structure),
    ])


def _ensure_store() -> None:
    os.makedirs(_DATA_DIR, exist_ok=True)
    if not os.path.exists(_MEMORY_PATH):
        with open(_MEMORY_PATH, "w", encoding="utf-8") as f:
            json.dump(_DEFAULT_STORE, f, ensure_ascii=True)


def _load_store() -> Dict[str, Any]:
    try:
        _ensure_store()
        with open(_MEMORY_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            data = {}
    except Exception:
        data = {}

    trades = data.get("trades") if isinstance(data.get("trades"), list) else []
    patterns = data.get("patterns") if isinstance(data.get("patterns"), dict) else {}
    return {"trades": trades, "patterns": patterns}


def _save_store(store: Dict[str, Any]) -> None:
    _ensure_store()
    with open(_MEMORY_PATH, "w", encoding="utf-8") as f:
        json.dump(store, f, ensure_ascii=True)


def _build_patterns(trades: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    agg: Dict[str, Dict[str, Any]] = {}
    for t in trades:
        if not isinstance(t, dict):
            continue
        status = str(t.get("status") or "").strip().lower()
        key = _pattern_key(
            regime=t.get("market_regime"),
            technical=t.get("technical_score"),
            risk_structure=t.get("risk_structure_score"),
        )
        row = agg.get(key)
        if not isinstance(row, dict):
            row = {"wins": 0, "losses": 0, "win_rate": 0.0}
            agg[key] = row

        if status == "win":
            row["wins"] = int(row.get("wins") or 0) + 1
        elif status == "loss":
            row["losses"] = int(row.get("losses") or 0) + 1

    for key, row in list(agg.items()):
        wins = int(row.get("wins") or 0)
        losses = int(row.get("losses") or 0)
        n = wins + losses
        row["win_rate"] = float(round((wins / n) if n > 0 else 0.0, 4))
        agg[key] = row

    return agg


def _infer_direction(trade: Dict[str, Any]) -> str:
    try:
        d = str(trade.get("direction") or "").strip().lower()
        if d in ("long", "short"):
            return d
    except Exception:
        pass

    entry = _safe_opt_f((trade or {}).get("entry"))
    stop = _safe_opt_f((trade or {}).get("stop"))
    if entry is not None and stop is not None:
        if float(stop) > float(entry):
            return "short"
        if float(stop) < float(entry):
            return "long"
    return "long"


def _first_target(trade: Dict[str, Any]) -> Optional[float]:
    try:
        ts = trade.get("targets") if isinstance(trade.get("targets"), list) else []
    except Exception:
        ts = []
    for t in ts:
        tv = _safe_opt_f(t)
        if tv is not None:
            return float(tv)
    return None


def _outcome_from_price(trade: Dict[str, Any], last_price: Optional[float]) -> Optional[str]:
    lp = _safe_opt_f(last_price)
    if lp is None:
        return None

    direction = _infer_direction(trade)
    stop = _safe_opt_f((trade or {}).get("stop"))
    t1 = _first_target(trade)

    if direction == "short":
        try:
            if stop is not None and float(lp) >= float(stop):
                return "loss"
        except Exception:
            pass
        try:
            if t1 is not None and float(lp) <= float(t1):
                return "win"
        except Exception:
            pass
        return None

    # Default long.
    try:
        if stop is not None and float(lp) <= float(stop):
            return "loss"
    except Exception:
        pass
    try:
        if t1 is not None and float(lp) >= float(t1):
            return "win"
    except Exception:
        pass
    return None


def record_pick(result: dict) -> None:
    if not isinstance(result, dict):
        return

    system_ctx = result.get("system_context") if isinstance(result.get("system_context"), dict) else {}
    conf_ctx = system_ctx.get("confidence_context") if isinstance(system_ctx.get("confidence_context"), dict) else {}
    trade_plan = result.get("trade_plan") if isinstance(result.get("trade_plan"), dict) else {}
    pillars = result.get("pillar_scores_0_10") if isinstance(result.get("pillar_scores_0_10"), dict) else {}

    rec = {
        "symbol": str(result.get("symbol") or "").strip().upper(),
        "timestamp": _now_iso(),
        "ai_score": _safe_f(result.get("ai_score_0_10"), 0.0),
        "execution_score": _safe_f(result.get("execution_score_0_10"), 0.0),
        "pillar_scores": dict(pillars),
        "technical_score": _safe_f(pillars.get("technical"), 0.0),
        "risk_structure_score": _safe_f(pillars.get("risk_structure"), 0.0),
        "market_regime": str(conf_ctx.get("market_regime") or "unknown").strip().lower() or "unknown",
        "entry": trade_plan.get("entry"),
        "stop": trade_plan.get("stop"),
        "targets": trade_plan.get("targets") if isinstance(trade_plan.get("targets"), list) else [],
        "direction": str(trade_plan.get("direction") or "long").strip().lower() or "long",
        "status": "pending",
    }

    if not rec["symbol"]:
        return

    with _LOCK:
        store = _load_store()
        trades = store.get("trades") if isinstance(store.get("trades"), list) else []
        trades.append(rec)
        if len(trades) > 5000:
            trades = trades[-5000:]
        store["trades"] = trades
        if not isinstance(store.get("patterns"), dict):
            store["patterns"] = {}
        _save_store(store)


def update_trade_outcome(symbol: str, outcome: str) -> bool:
    sym = str(symbol or "").strip().upper()
    outc = str(outcome or "").strip().lower()
    if not sym or outc not in ("win", "loss", "timeout"):
        return False

    with _LOCK:
        store = _load_store()
        trades = store.get("trades") if isinstance(store.get("trades"), list) else []

        updated = False
        for i in range(len(trades) - 1, -1, -1):
            t = trades[i]
            if not isinstance(t, dict):
                continue
            if str(t.get("symbol") or "").strip().upper() != sym:
                continue
            if str(t.get("status") or "").strip().lower() != "pending":
                continue
            t["status"] = outc
            t["resolved_at"] = _now_iso()
            trades[i] = t
            updated = True
            break

        if updated:
            store["trades"] = trades
            store["patterns"] = _build_patterns(trades)
            _save_store(store)
        return updated


def get_pattern_multiplier(result: dict) -> Tuple[float, int]:
    if not isinstance(result, dict):
        return 1.0, 0

    try:
        store = _load_store()
        patterns = store.get("patterns") if isinstance(store.get("patterns"), dict) else {}

        system_ctx = result.get("system_context") if isinstance(result.get("system_context"), dict) else {}
        conf_ctx = system_ctx.get("confidence_context") if isinstance(system_ctx.get("confidence_context"), dict) else {}
        regime = str(conf_ctx.get("market_regime") or "unknown").strip().lower() or "unknown"

        pillars = result.get("pillar_scores_0_10") if isinstance(result.get("pillar_scores_0_10"), dict) else {}
        key = _pattern_key(
            regime=regime,
            technical=pillars.get("technical"),
            risk_structure=pillars.get("risk_structure"),
        )

        row = patterns.get(key) if isinstance(patterns.get(key), dict) else {}
        wins = int(row.get("wins") or 0)
        losses = int(row.get("losses") or 0)
        sample_size = int(max(0, wins + losses))

        if sample_size < 20:
            return 1.0, sample_size

        win_rate = _safe_f(row.get("win_rate"), 0.5)
        raw = 1.0 + ((float(win_rate) - 0.5) * 0.4)
        multiplier = max(0.9, min(1.1, float(raw)))
        return float(round(multiplier, 4)), sample_size
    except Exception:
        return 1.0, 0


def auto_resolve_pending_outcomes(
    *,
    price_fetcher: Callable[[str], Optional[float]],
    max_pending: int = 25,
    timeout_hours: float = 168.0,
) -> Dict[str, int]:
    if not callable(price_fetcher):
        return {"checked": 0, "wins": 0, "losses": 0, "timeouts": 0, "updated": 0}

    try:
        cap = int(max_pending)
    except Exception:
        cap = 25
    cap = max(1, min(200, cap))

    try:
        timeout_h = float(timeout_hours)
    except Exception:
        timeout_h = 168.0
    timeout_h = max(1.0, min(24.0 * 30.0, timeout_h))

    with _LOCK:
        store = _load_store()
        trades = store.get("trades") if isinstance(store.get("trades"), list) else []

    pending: List[Tuple[int, Dict[str, Any]]] = []
    for i in range(len(trades) - 1, -1, -1):
        t = trades[i]
        if not isinstance(t, dict):
            continue
        if str(t.get("status") or "").strip().lower() != "pending":
            continue
        pending.append((i, t))
        if len(pending) >= int(cap):
            break

    now = datetime.now(timezone.utc)
    updates: Dict[int, str] = {}
    checked = 0

    for i, t in pending:
        checked += 1
        sym = str(t.get("symbol") or "").strip().upper()
        if not sym:
            continue

        # Time-stop unresolved picks.
        ts = _parse_iso_dt(t.get("timestamp"))
        if isinstance(ts, datetime):
            try:
                if (now - ts).total_seconds() >= (float(timeout_h) * 3600.0):
                    updates[i] = "timeout"
                    continue
            except Exception:
                pass

        last_price = None
        try:
            last_price = price_fetcher(sym)
        except Exception:
            last_price = None

        outc = _outcome_from_price(t, last_price)
        if outc in ("win", "loss"):
            updates[i] = str(outc)

    wins = 0
    losses = 0
    timeouts = 0
    applied = 0

    if updates:
        with _LOCK:
            store2 = _load_store()
            trades2 = store2.get("trades") if isinstance(store2.get("trades"), list) else []

            for i, outc in updates.items():
                if i < 0 or i >= len(trades2):
                    continue
                t = trades2[i]
                if not isinstance(t, dict):
                    continue
                if str(t.get("status") or "").strip().lower() != "pending":
                    continue
                t["status"] = str(outc)
                t["resolved_at"] = _now_iso()
                trades2[i] = t
                applied += 1
                if outc == "win":
                    wins += 1
                elif outc == "loss":
                    losses += 1
                elif outc == "timeout":
                    timeouts += 1

            if applied > 0:
                store2["trades"] = trades2
                store2["patterns"] = _build_patterns(trades2)
                _save_store(store2)

    return {
        "checked": int(checked),
        "wins": int(wins),
        "losses": int(losses),
        "timeouts": int(timeouts),
        "updated": int(applied),
    }
