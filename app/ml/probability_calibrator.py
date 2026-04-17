"""
Deal Probability Engine — Calibrator (SPEC 048, PLAN_059 Phase 4).

Fits raw composite → probability calibration from labeled outcomes. Two methods:

  - Platt scaling: logistic fit, 2 params (k, x0), smooth
  - Isotonic regression (Pool Adjacent Violators): non-parametric step function

Also provides evaluation via Brier score. Deliberately implemented with
scipy.optimize + numpy only (no sklearn dependency) so the engine stays
self-contained.

Persists fit results to `txn_prob_calibrations`. When `is_active=True`,
the engine picks them up via `get_active_calibration(scope)` and applies
them in preference to the default sigmoid.
"""

from __future__ import annotations

import logging
from typing import Dict, List, Optional, Tuple

import numpy as np
from scipy.optimize import minimize
from sqlalchemy.orm import Session

from app.core.probability_models import TxnProbCalibration

logger = logging.getLogger(__name__)


MIN_SAMPLES_FOR_FIT = 20  # Refuse to fit below this (too noisy)


# ---------------------------------------------------------------------------
# Platt scaling
# ---------------------------------------------------------------------------


def fit_platt(
    raw_scores: np.ndarray, outcomes: np.ndarray
) -> Dict[str, float]:
    """
    Fit logistic regression P = sigmoid(k*(raw - x0)) to raw/outcome pairs.
    Returns {k, x0} via scipy.optimize.minimize on negative log-likelihood.
    """
    raw_scores = np.asarray(raw_scores, dtype=float)
    outcomes = np.asarray(outcomes, dtype=float)

    def neg_log_lik(params):
        k, x0 = params
        # Numerically-stable sigmoid
        z = k * (raw_scores - x0)
        # log(1+exp(x)) with overflow protection
        p = 1.0 / (1.0 + np.exp(-np.clip(z, -500, 500)))
        p = np.clip(p, 1e-9, 1 - 1e-9)
        return -np.mean(outcomes * np.log(p) + (1 - outcomes) * np.log(1 - p))

    result = minimize(
        neg_log_lik,
        x0=np.array([0.08, 55.0]),
        method="Nelder-Mead",
        options={"xatol": 1e-4, "fatol": 1e-6, "maxiter": 500},
    )
    k, x0 = float(result.x[0]), float(result.x[1])
    return {"k": k, "x0": x0, "converged": bool(result.success)}


def apply_platt(raw: float, k: float, x0: float) -> float:
    """Apply a fitted Platt calibration."""
    try:
        return 1.0 / (1.0 + np.exp(-k * (raw - x0)))
    except OverflowError:
        return 0.0 if raw < x0 else 1.0


# ---------------------------------------------------------------------------
# Isotonic regression via Pool Adjacent Violators (PAV)
# ---------------------------------------------------------------------------


def fit_isotonic(
    raw_scores: np.ndarray, outcomes: np.ndarray
) -> Dict[str, List]:
    """
    Non-parametric monotonic-increasing calibration via PAV.

    Returns `{breakpoints: [[x, y], ...]}` — a non-decreasing step function.
    Prediction for a new raw score: pick the largest breakpoint x ≤ raw and
    return its y.
    """
    raw_scores = np.asarray(raw_scores, dtype=float)
    outcomes = np.asarray(outcomes, dtype=float)
    order = np.argsort(raw_scores)
    x = raw_scores[order]
    y = outcomes[order].copy()

    # Pool Adjacent Violators
    # After each merge we keep parallel arrays of (x, y, weight)
    xs = x.tolist()
    ys = y.tolist()
    ws = [1.0] * len(xs)

    i = 0
    while i < len(ys) - 1:
        if ys[i] > ys[i + 1]:
            total_w = ws[i] + ws[i + 1]
            merged_y = (ys[i] * ws[i] + ys[i + 1] * ws[i + 1]) / total_w
            # Merge the next point into the previous one
            xs[i + 1] = xs[i + 1]  # keep the larger x as the step boundary
            ys[i + 1] = merged_y
            ws[i + 1] = total_w
            del xs[i]
            del ys[i]
            del ws[i]
            if i > 0:
                i -= 1
        else:
            i += 1

    breakpoints = [[float(xs[i]), float(ys[i])] for i in range(len(xs))]
    return {"breakpoints": breakpoints}


def apply_isotonic(raw: float, breakpoints: List[List[float]]) -> float:
    """Apply a fitted isotonic step function to a raw score."""
    if not breakpoints:
        return 0.5
    # Find largest x ≤ raw
    last_y = breakpoints[0][1]
    for x, y in breakpoints:
        if x <= raw:
            last_y = y
        else:
            break
    return max(0.0, min(1.0, float(last_y)))


# ---------------------------------------------------------------------------
# Evaluation
# ---------------------------------------------------------------------------


def brier_score(predictions: np.ndarray, outcomes: np.ndarray) -> float:
    """Mean squared error of probability predictions vs 0/1 outcomes."""
    predictions = np.asarray(predictions, dtype=float)
    outcomes = np.asarray(outcomes, dtype=float)
    if predictions.size == 0:
        return 0.0
    return float(np.mean((predictions - outcomes) ** 2))


def reliability_bins(
    predictions: np.ndarray, outcomes: np.ndarray, n_bins: int = 10
) -> List[Dict]:
    """
    Return per-bin summaries (for reliability diagram plotting).

    Each bin: {bin_start, bin_end, n, mean_predicted, actual_rate}
    """
    predictions = np.asarray(predictions, dtype=float)
    outcomes = np.asarray(outcomes, dtype=float)
    bin_edges = np.linspace(0, 1, n_bins + 1)
    bins: List[Dict] = []
    for i in range(n_bins):
        lo, hi = bin_edges[i], bin_edges[i + 1]
        mask = (predictions >= lo) & (predictions < hi if i < n_bins - 1 else predictions <= hi)
        n = int(mask.sum())
        if n == 0:
            bins.append(
                {
                    "bin_start": float(lo),
                    "bin_end": float(hi),
                    "n": 0,
                    "mean_predicted": None,
                    "actual_rate": None,
                }
            )
            continue
        bins.append(
            {
                "bin_start": float(lo),
                "bin_end": float(hi),
                "n": n,
                "mean_predicted": float(predictions[mask].mean()),
                "actual_rate": float(outcomes[mask].mean()),
            }
        )
    return bins


# ---------------------------------------------------------------------------
# Persistence / engine integration
# ---------------------------------------------------------------------------


class ProbabilityCalibrator:
    """
    High-level API: fit + persist + activate calibrations per scope.
    """

    def __init__(self, db: Session):
        self.db = db

    def fit_and_persist(
        self,
        raw_scores: List[float],
        outcomes: List[int],
        scope: str = "global",
        method: str = "platt",
    ) -> Dict:
        """Fit calibration, persist row, deactivate prior rows at same scope."""
        if len(raw_scores) < MIN_SAMPLES_FOR_FIT:
            return {
                "ok": False,
                "reason": f"need ≥ {MIN_SAMPLES_FOR_FIT} samples, got {len(raw_scores)}",
            }

        raw_arr = np.asarray(raw_scores, dtype=float)
        out_arr = np.asarray(outcomes, dtype=float)

        if method == "platt":
            params = fit_platt(raw_arr, out_arr)
            preds = np.array([apply_platt(r, params["k"], params["x0"]) for r in raw_arr])
        elif method == "isotonic":
            params = fit_isotonic(raw_arr, out_arr)
            preds = np.array([apply_isotonic(r, params["breakpoints"]) for r in raw_arr])
        else:
            return {"ok": False, "reason": f"unknown method {method!r}"}

        brier = brier_score(preds, out_arr)

        # Deactivate prior calibrations at this scope
        self.db.query(TxnProbCalibration).filter_by(
            scope=scope, is_active=True
        ).update({"is_active": False})

        row = TxnProbCalibration(
            scope=scope,
            method=method,
            params=params,
            n_samples=len(raw_scores),
            brier_score=brier,
            is_active=True,
        )
        self.db.add(row)
        self.db.commit()
        return {
            "ok": True,
            "scope": scope,
            "method": method,
            "params": params,
            "n_samples": len(raw_scores),
            "brier_score": brier,
        }

    def list_calibrations(self, scope: Optional[str] = None) -> List[Dict]:
        q = self.db.query(TxnProbCalibration)
        if scope:
            q = q.filter_by(scope=scope)
        rows = q.order_by(TxnProbCalibration.fitted_at.desc()).all()
        return [
            {
                "id": r.id,
                "scope": r.scope,
                "method": r.method,
                "params": r.params,
                "n_samples": r.n_samples,
                "brier_score": r.brier_score,
                "is_active": r.is_active,
                "fitted_at": r.fitted_at.isoformat() if r.fitted_at else None,
            }
            for r in rows
        ]


def get_active_calibration(db: Session, scope: str = "global") -> Optional[TxnProbCalibration]:
    """Return the active calibration row for a scope (or None)."""
    return (
        db.query(TxnProbCalibration)
        .filter_by(scope=scope, is_active=True)
        .order_by(TxnProbCalibration.fitted_at.desc())
        .first()
    )


def calibrate_with_active(
    raw: float, calibration: Optional[TxnProbCalibration]
) -> Optional[float]:
    """Apply an active calibration if present; return None to signal fallback."""
    if calibration is None:
        return None
    if calibration.method == "platt":
        params = calibration.params or {}
        return apply_platt(raw, params.get("k", 0.08), params.get("x0", 55.0))
    if calibration.method == "isotonic":
        bp = (calibration.params or {}).get("breakpoints", [])
        return apply_isotonic(raw, bp)
    return None
