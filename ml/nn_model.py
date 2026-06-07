"""
ml/nn_model.py — 3-layer MLP neural network, pure numpy.

Architecture:
    Input(18) → Dense(32, ReLU) → Dense(16, ReLU) → Dense(1, Sigmoid)

Trained with Adam optimiser + mini-batch gradient descent.
Weights saved/loaded as a single .npz file — no framework needed.
"""

from __future__ import annotations

import os
import time
import logging
from typing import List, Optional, Tuple

log = logging.getLogger("stackiq")

try:
    import numpy as np
    _NP = True
except ImportError:
    _NP = False

_MODEL_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "models", "nn_scorer.npz",
)


# ─────────────────────────────────────────────────────────────────────────────
# Activations
# ─────────────────────────────────────────────────────────────────────────────

def _relu(x):
    return np.maximum(0.0, x)


def _relu_grad(x):
    return (x > 0).astype(float)


def _sigmoid(x):
    return 1.0 / (1.0 + np.exp(-np.clip(x, -30, 30)))


# ─────────────────────────────────────────────────────────────────────────────
# Model class
# ─────────────────────────────────────────────────────────────────────────────

class NNScorer:
    """
    Lightweight 3-layer binary-classification MLP.
    Predicts P(win) for a stock pick given an 18-dim feature vector.
    """

    def __init__(self, input_dim: int = 18, h1: int = 32, h2: int = 16):
        if not _NP:
            raise RuntimeError("numpy is required for NNScorer")

        self.input_dim = input_dim
        rng = np.random.default_rng(42)

        # He initialisation (good for ReLU)
        self.W1 = rng.normal(0, (2.0 / input_dim) ** 0.5, (input_dim, h1))
        self.b1 = np.zeros((1, h1))
        self.W2 = rng.normal(0, (2.0 / h1) ** 0.5, (h1, h2))
        self.b2 = np.zeros((1, h2))
        self.W3 = rng.normal(0, (2.0 / h2) ** 0.5, (h2, 1))
        self.b3 = np.zeros((1, 1))

        # Adam state
        self._params = ["W1", "b1", "W2", "b2", "W3", "b3"]
        self._m  = {k: np.zeros_like(getattr(self, k)) for k in self._params}
        self._v  = {k: np.zeros_like(getattr(self, k)) for k in self._params}
        self._t  = 0   # Adam step counter

    # ── Forward pass ─────────────────────────────────────────────────────────

    def _forward(self, X, training: bool = False, dropout_p: float = 0.3):
        self._X   = X
        self._z1  = X @ self.W1 + self.b1
        self._a1  = _relu(self._z1)
        if training:
            self._d1  = (np.random.rand(*self._a1.shape) > dropout_p).astype(float)
            self._a1  = self._a1 * self._d1 / (1.0 - dropout_p)
        else:
            self._d1  = None

        self._z2  = self._a1 @ self.W2 + self.b2
        self._a2  = _relu(self._z2)
        if training:
            self._d2  = (np.random.rand(*self._a2.shape) > dropout_p).astype(float)
            self._a2  = self._a2 * self._d2 / (1.0 - dropout_p)
        else:
            self._d2  = None

        self._z3  = self._a2 @ self.W3 + self.b3
        self._out = _sigmoid(self._z3)
        return self._out

    # ── Backward pass (binary cross-entropy loss) ─────────────────────────────

    def _backward(self, y: "np.ndarray") -> float:
        n = y.shape[0]
        eps = 1e-9
        loss = -np.mean(y * np.log(self._out + eps) + (1 - y) * np.log(1 - self._out + eps))

        dout = (self._out - y) / n              # d(BCE)/d(z3)

        dW3 = self._a2.T @ dout
        db3 = dout.sum(axis=0, keepdims=True)

        da2 = dout @ self.W3.T
        if self._d2 is not None:
            da2 = da2 * self._d2 / (1.0 - 0.3)
        dz2 = da2 * _relu_grad(self._z2)

        dW2 = self._a1.T @ dz2
        db2 = dz2.sum(axis=0, keepdims=True)

        da1 = dz2 @ self.W2.T
        if self._d1 is not None:
            da1 = da1 * self._d1 / (1.0 - 0.3)
        dz1 = da1 * _relu_grad(self._z1)

        dW1 = self._X.T @ dz1
        db1 = dz1.sum(axis=0, keepdims=True)

        self._grads = {"W1": dW1, "b1": db1, "W2": dW2, "b2": db2, "W3": dW3, "b3": db3}
        return float(loss)

    def _adam_step(self, lr: float, beta1: float = 0.9, beta2: float = 0.999, eps: float = 1e-8, l2: float = 0.0):
        self._t += 1
        for k in self._params:
            g = self._grads[k]
            if l2 > 0 and k.startswith("W"):     # L2 only on weight matrices, not biases
                g = g + l2 * getattr(self, k)
            self._m[k] = beta1 * self._m[k] + (1 - beta1) * g
            self._v[k] = beta2 * self._v[k] + (1 - beta2) * g ** 2
            m_hat = self._m[k] / (1 - beta1 ** self._t)
            v_hat = self._v[k] / (1 - beta2 ** self._t)
            setattr(self, k, getattr(self, k) - lr * m_hat / (v_hat ** 0.5 + eps))

    # ── Training ──────────────────────────────────────────────────────────────

    def fit(
        self,
        X: "np.ndarray",
        y: "np.ndarray",
        epochs: int = 400,
        lr: float = 1e-3,
        batch_size: int = 16,
        dropout_p: float = 0.5,
        l2: float = 1e-3,
        val_split: float = 0.2,
        patience: int = 40,
    ) -> List[float]:
        """
        Train with validation-based early stopping and L2 regularisation.
        Returns training loss history.
        """
        n = X.shape[0]

        # Validation split
        n_val = max(1, int(n * val_split))
        idx_all = np.random.permutation(n)
        val_idx   = idx_all[:n_val]
        train_idx = idx_all[n_val:]

        X_tr, y_tr = X[train_idx], y[train_idx]
        X_val, y_val = X[val_idx], y[val_idx]

        best_val_loss = float("inf")
        best_weights: dict = {}
        no_improve = 0
        losses = []
        eps_bce = 1e-9

        for epoch in range(epochs):
            # Shuffle training set each epoch
            perm = np.random.permutation(len(X_tr))
            epoch_loss = 0.0
            batches = 0
            for start in range(0, len(X_tr), batch_size):
                bi = perm[start: start + batch_size]
                Xb = X_tr[bi]
                yb = y_tr[bi].reshape(-1, 1)
                self._forward(Xb, training=True, dropout_p=dropout_p)
                loss = self._backward(yb)
                self._adam_step(lr, l2=l2)
                epoch_loss += loss
                batches += 1

            avg = epoch_loss / max(batches, 1)
            losses.append(avg)

            # Validation loss (no dropout)
            val_out = self._forward(X_val, training=False).flatten()
            val_loss = -np.mean(
                y_val * np.log(val_out + eps_bce) + (1 - y_val) * np.log(1 - val_out + eps_bce)
            )

            if (epoch + 1) % 50 == 0:
                log.info(f"nn_scorer: epoch {epoch+1}/{epochs}  train={avg:.4f}  val={val_loss:.4f}")

            if val_loss < best_val_loss - 1e-5:
                best_val_loss = val_loss
                best_weights = {k: getattr(self, k).copy() for k in self._params}
                no_improve = 0
            else:
                no_improve += 1
                if no_improve >= patience:
                    log.info(f"nn_scorer: early stop at epoch {epoch+1}  best_val={best_val_loss:.4f}")
                    break

        # Restore best weights
        if best_weights:
            for k, v in best_weights.items():
                setattr(self, k, v)

        return losses

    # ── Inference ─────────────────────────────────────────────────────────────

    def predict_proba(self, X: "np.ndarray") -> "np.ndarray":
        """Return win probability for each row in X."""
        return self._forward(X, training=False).flatten()

    def predict_one(self, features: List[float]) -> float:
        """Return a single win probability."""
        X = np.array(features, dtype=float).reshape(1, -1)
        return float(self.predict_proba(X)[0])

    # ── Persistence ───────────────────────────────────────────────────────────

    def save(self, path: Optional[str] = None) -> None:
        path = path or _MODEL_PATH
        os.makedirs(os.path.dirname(path), exist_ok=True)
        np.savez(
            path,
            W1=self.W1, b1=self.b1,
            W2=self.W2, b2=self.b2,
            W3=self.W3, b3=self.b3,
            trained_at=np.array([time.time()]),
        )
        log.info(f"nn_scorer: model saved → {path}")

    @classmethod
    def load(cls, path: Optional[str] = None) -> "NNScorer":
        path = path or _MODEL_PATH
        d = np.load(path)
        model = cls.__new__(cls)
        model.W1 = d["W1"]; model.b1 = d["b1"]
        model.W2 = d["W2"]; model.b2 = d["b2"]
        model.W3 = d["W3"]; model.b3 = d["b3"]
        model.input_dim = model.W1.shape[0]
        model._params = ["W1", "b1", "W2", "b2", "W3", "b3"]
        model._m  = {k: np.zeros_like(getattr(model, k)) for k in model._params}
        model._v  = {k: np.zeros_like(getattr(model, k)) for k in model._params}
        model._t  = 0
        log.info(f"nn_scorer: model loaded ← {path}  (trained {(time.time() - float(d['trained_at'][0]))/3600:.1f}h ago)")
        return model

    @staticmethod
    def exists(path: Optional[str] = None) -> bool:
        return os.path.isfile(path or _MODEL_PATH)
