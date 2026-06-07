"""
ml/trainer.py — Training pipeline for the NN scorer.

Workflow:
  1. Load resolved picks from perf_tracker.db (wins + losses, skip pending)
  2. For each pick, fetch ~60 daily bars from Alpaca around the pick date
  3. Compute 18-dim feature vectors
  4. Train NNScorer via mini-batch Adam
  5. Save weights to models/nn_scorer.npz

Called automatically by brain.py after recalibrate_weights() when >= 20 resolved picks exist.
Can also be triggered manually via POST /scan/train_nn.
"""

from __future__ import annotations

import logging
import os
import sqlite3
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

log = logging.getLogger("stackiq")

_PERF_DB = os.getenv("PERF_TRACKER_DB", os.path.join(os.path.dirname(os.path.dirname(__file__)), "perf_tracker.db"))
_MIN_SAMPLES = 20   # don't train until we have this many labelled picks


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(_PERF_DB, timeout=15, check_same_thread=False)
    c.row_factory = sqlite3.Row
    return c


def _load_resolved_picks() -> List[Dict[str, Any]]:
    """Return picks that have a definitive win or loss label."""
    try:
        with _conn() as db:
            rows = db.execute("""
                SELECT symbol, status, edge_signals, edge_score, final_score,
                       confidence, recorded_at, hit_target, hit_stop,
                       max_return_pct, max_drawdown_pct
                FROM picks
                WHERE status IN ('won','won_drift','lost','lost_drift')
                ORDER BY recorded_at DESC
                LIMIT 300
            """).fetchall()
            return [dict(r) for r in rows]
    except Exception as e:
        log.warning(f"nn_trainer: load_picks failed: {e}")
        return []


def _fetch_bars_for_pick(symbol: str, pick_ts: float) -> Optional[Dict[str, Any]]:
    """
    Fetch ~60 daily bars ending on the pick date from Alpaca.
    Returns dict with keys: closes, highs, lows, volumes (all lists of floats).
    """
    try:
        from data_fetcher import get_bars
        result = get_bars(symbol, "1Day", 60)
        candles = result.get("candles") or []
        if len(candles) < 10:
            return None
        closes  = [float(b.get("c") or b.get("close") or 0.0) for b in candles]
        highs   = [float(b.get("h") or b.get("high") or 0.0)  for b in candles]
        lows    = [float(b.get("l") or b.get("low")  or 0.0)  for b in candles]
        volumes = [float(b.get("v") or b.get("volume") or 0.0) for b in candles]
        return {"closes": closes, "highs": highs, "lows": lows, "volumes": volumes}
    except Exception as e:
        log.warning(f"nn_trainer: bars fetch {symbol}: {e}")
        return None


def _label(row: Dict[str, Any]) -> int:
    """1 = win, 0 = loss."""
    status = str(row.get("status") or "").lower()
    if "won" in status:
        return 1
    return 0


def _extra_scores(row: Dict[str, Any]) -> Dict[str, float]:
    """Return feature kwargs extracted from stored pick row."""
    import json
    edge  = float(row.get("edge_score")  or 5.0)
    final = float(row.get("final_score") or 5.0)
    conf  = float(row.get("confidence")  or 5.0)

    # Parse edge_signals JSON string → set of signal names
    raw_sigs = row.get("edge_signals") or "[]"
    try:
        sigs = set(json.loads(raw_sigs) if isinstance(raw_sigs, str) else raw_sigs)
    except Exception:
        sigs = set()

    # premover_score may be stored in a separate column or not at all; default 5.0
    premover = float(row.get("premover_score") or row.get("premover") or 5.0)

    return {
        "edge_score_0_10":          edge,
        "momentum_score":           final,
        "volatility_score_0_10":    conf,
        "has_momentum_expansion":   1.0 if "MOMENTUM_EXPANSION"  in sigs else 0.0,
        "has_volatility_expansion": 1.0 if "VOLATILITY_EXPANSION" in sigs else 0.0,
        "has_breakout_structure":   1.0 if "BREAKOUT_STRUCTURE"   in sigs else 0.0,
        "has_rs_leader":            1.0 if "RS_LEADER"            in sigs else 0.0,
        "premover_score_0_10":      premover,
    }


def run_training(force: bool = False) -> Dict[str, Any]:
    """
    Full training run. Returns a summary dict.
    Safe to call from a background thread.
    """
    try:
        import numpy as np
        from ml.features import vector_from_bars, FEATURE_DIM
        from ml.nn_model import NNScorer
    except ImportError as e:
        return {"error": f"numpy not available: {e}", "trained": False}

    picks = _load_resolved_picks()
    if len(picks) < _MIN_SAMPLES and not force:
        return {
            "trained": False,
            "reason": f"only {len(picks)} resolved picks; need {_MIN_SAMPLES}",
            "resolved_picks": len(picks),
        }

    log.info(f"nn_trainer: building dataset from {len(picks)} resolved picks …")
    X_rows, y_rows = [], []
    skipped = 0

    for pick in picks:
        sym = str(pick.get("symbol") or "").strip().upper()
        if not sym:
            skipped += 1
            continue

        ts = float(pick.get("recorded_at") or 0.0)
        bars = _fetch_bars_for_pick(sym, ts)
        if bars is None:
            skipped += 1
            continue

        feat_kwargs = _extra_scores(pick)
        try:
            vec = vector_from_bars(
                bars["closes"], bars["highs"], bars["lows"], bars["volumes"],
                **feat_kwargs,
            )
        except Exception as fe:
            log.warning(f"nn_trainer: feature error {sym}: {fe}")
            skipped += 1
            continue

        if len(vec) != FEATURE_DIM:
            skipped += 1
            continue

        X_rows.append(vec)
        y_rows.append(float(_label(pick)))

    if len(X_rows) < _MIN_SAMPLES:
        return {
            "trained": False,
            "reason": f"only {len(X_rows)} usable samples after bar fetch (skipped {skipped})",
            "resolved_picks": len(picks),
        }

    X = np.array(X_rows, dtype=float)
    y = np.array(y_rows, dtype=float)

    # Normalise X column-wise to zero-mean / unit-std (robust to outliers)
    mean = X.mean(axis=0)
    std  = X.std(axis=0)
    std[std < 1e-8] = 1.0
    X = (X - mean) / std

    # Train
    model = NNScorer(input_dim=FEATURE_DIM)
    t0 = time.time()
    losses = model.fit(
        X, y,
        epochs=400, lr=1e-3,
        batch_size=min(16, len(X_rows)),
        dropout_p=0.5, l2=1e-3,
        val_split=0.2, patience=40,
    )
    elapsed = time.time() - t0

    # Save model + normalisation stats together
    from ml.nn_model import _MODEL_PATH
    import numpy as np2
    os.makedirs(os.path.dirname(_MODEL_PATH), exist_ok=True)
    model.save()
    # Save normalisation parameters alongside
    np2.savez(
        _MODEL_PATH.replace(".npz", "_norm.npz"),
        mean=mean, std=std,
    )

    win_rate = float(y.mean()) if len(y) > 0 else 0.0
    log.info(
        f"nn_trainer: done in {elapsed:.1f}s  samples={len(X_rows)}  "
        f"win_rate={win_rate:.1%}  final_loss={losses[-1]:.4f}"
    )

    return {
        "trained": True,
        "samples": len(X_rows),
        "skipped": skipped,
        "win_rate_pct": round(win_rate * 100, 1),
        "final_loss": round(losses[-1], 4),
        "elapsed_s": round(elapsed, 1),
        "epochs": 300,
    }
