"""
ml/predictor.py — Fast inference for the live scanner.

Call predict_win_prob(closes, highs, lows, volumes, edge_score, momentum_score, volatility_score)
to get a P(win) float in [0, 1].  Returns 0.5 (neutral) if the model isn't trained yet
or if numpy is unavailable — the heuristic score remains unchanged.

The model and its normalisation stats are loaded once and cached in module-level
globals.  They are reloaded automatically if the .npz file is newer than the cache.
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any, List, Optional

log = logging.getLogger("stackiq")

try:
    import numpy as np
    _NP = True
except ImportError:
    _NP = False

from ml.nn_model import _MODEL_PATH
_NORM_PATH = _MODEL_PATH.replace(".npz", "_norm.npz")

# Module-level cache
_model: Optional[Any] = None
_model_mtime: float = 0.0
_norm_mean: Optional[Any] = None
_norm_std: Optional[Any] = None


def _maybe_reload() -> bool:
    """Reload model from disk if the .npz changed. Returns True if model is available."""
    global _model, _model_mtime, _norm_mean, _norm_std

    if not _NP:
        return False
    if not os.path.isfile(_MODEL_PATH):
        return False

    try:
        mtime = os.path.getmtime(_MODEL_PATH)
    except OSError:
        return False

    if _model is not None and mtime == _model_mtime:
        return True   # already up-to-date

    try:
        from ml.nn_model import NNScorer
        _model = NNScorer.load(_MODEL_PATH)
        _model_mtime = mtime

        if os.path.isfile(_NORM_PATH):
            d = np.load(_NORM_PATH)
            _norm_mean = d["mean"]
            _norm_std  = d["std"]
            _norm_std[_norm_std < 1e-8] = 1.0
        else:
            _norm_mean = None
            _norm_std  = None

        return True
    except Exception as e:
        log.warning(f"nn_predictor: reload failed: {e}")
        _model = None
        return False


def predict_win_prob(
    closes: List[float],
    highs: List[float],
    lows: List[float],
    volumes: List[float],
    edge_score_0_10: float = 5.0,
    momentum_score: float = 5.0,
    volatility_score_0_10: float = 5.0,
) -> float:
    """
    Returns P(win) in [0, 1].
    0.5 = neutral / model not ready.
    >0.5 = NN thinks this is a likely winner.
    <0.5 = NN thinks this is a likely loser.
    """
    if not _maybe_reload():
        return 0.5

    try:
        from ml.features import vector_from_bars
        vec = vector_from_bars(
            closes, highs, lows, volumes,
            edge_score_0_10=edge_score_0_10,
            momentum_score=momentum_score,
            volatility_score_0_10=volatility_score_0_10,
        )
        X = np.array(vec, dtype=float).reshape(1, -1)
        if _norm_mean is not None:
            X = (X - _norm_mean) / _norm_std
        return float(np.clip(_model.predict_proba(X)[0], 0.0, 1.0))
    except Exception as e:
        log.debug(f"nn_predictor: inference error: {e}")
        return 0.5


def predict_win_prob_from_candidate(c: Any) -> float:
    """Convenience wrapper that pulls data directly from a _Candidate object."""
    if not _maybe_reload():
        return 0.5

    try:
        from ml.features import vector_from_candidate
        from ml.nn_model import FEATURE_DIM  # noqa: F401 — ensures import works
        vec = vector_from_candidate(c)
        X = np.array(vec, dtype=float).reshape(1, -1)
        if _norm_mean is not None:
            X = (X - _norm_mean) / _norm_std
        return float(np.clip(_model.predict_proba(X)[0], 0.0, 1.0))
    except Exception as e:
        log.debug(f"nn_predictor: candidate inference error: {e}")
        return 0.5


def model_is_ready() -> bool:
    """True if a trained model exists on disk."""
    return os.path.isfile(_MODEL_PATH)


def model_info() -> dict:
    """Return metadata about the currently loaded model."""
    if not os.path.isfile(_MODEL_PATH):
        return {"ready": False}
    try:
        mtime = os.path.getmtime(_MODEL_PATH)
        age_h = (time.time() - mtime) / 3600.0
        _maybe_reload()
        return {
            "ready": True,
            "trained_h_ago": round(age_h, 1),
            "has_norm": _norm_mean is not None,
            "input_dim": _model.input_dim if _model else None,
        }
    except Exception as e:
        return {"ready": False, "error": str(e)}
