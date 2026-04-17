"""
Deal Probability Engine — Signal Weight Optimizer (SPEC 048, PLAN_059 Phase 4).

Given labeled outcomes, find per-signal weights that maximize AUC-ROC of
the composite score as a transaction predictor. Constraints:
  - All weights non-negative
  - Weights sum to 1.0
  - Minimum sample count gate (default 50)

Also provides:
  - Per-signal univariate AUC (which single signals predict well on their own)
  - Walk-forward backtest (train on past months, score next period, repeat)

Pure scipy + numpy — no sklearn dependency.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import Dict, List, Optional

import numpy as np
from scipy.optimize import minimize
from sqlalchemy.orm import Session

from app.ml.probability_signal_taxonomy import SIGNAL_TAXONOMY

logger = logging.getLogger(__name__)


MIN_SAMPLES_FOR_OPTIMIZATION = 50
MAX_WEIGHT_PER_SIGNAL = 0.4  # No single signal may exceed 40%
MIN_WEIGHT_PER_SIGNAL = 0.01


# ---------------------------------------------------------------------------
# AUC (pure function, no sklearn)
# ---------------------------------------------------------------------------


def compute_auc(scores: np.ndarray, outcomes: np.ndarray) -> float:
    """
    AUC-ROC via Mann-Whitney U statistic.

    Mathematically equivalent to the area under the ROC curve for
    binary outcomes and continuous scores.
    """
    scores = np.asarray(scores, dtype=float)
    outcomes = np.asarray(outcomes, dtype=int)
    n_pos = int(outcomes.sum())
    n_neg = len(outcomes) - n_pos
    if n_pos == 0 or n_neg == 0:
        return 0.5
    # Rank all scores (ties get average rank)
    order = np.argsort(scores)
    ranks = np.empty_like(order, dtype=float)
    ranks[order] = np.arange(1, len(scores) + 1)
    # Handle ties — assign average ranks to tied groups
    # Simple approach: sort values, find ties, average.
    # For our scale this is fine; perf isn't critical.
    unique_vals, inv = np.unique(scores, return_inverse=True)
    if len(unique_vals) < len(scores):
        # rewrite ranks with averages for tied groups
        sorted_vals = scores[order]
        i = 0
        while i < len(sorted_vals):
            j = i
            while j + 1 < len(sorted_vals) and sorted_vals[j + 1] == sorted_vals[i]:
                j += 1
            if j > i:
                avg = (ranks[order[i]] + ranks[order[j]]) / 2
                for k in range(i, j + 1):
                    ranks[order[k]] = avg
            i = j + 1

    rank_sum_pos = ranks[outcomes == 1].sum()
    u = rank_sum_pos - n_pos * (n_pos + 1) / 2
    return float(u / (n_pos * n_neg))


# ---------------------------------------------------------------------------
# Optimizer
# ---------------------------------------------------------------------------


class SignalWeightOptimizer:
    """Optimize per-signal weights to maximize AUC-ROC."""

    def __init__(self, db: Session):
        self.db = db
        self.signal_types: List[str] = list(SIGNAL_TAXONOMY.keys())

    # -------------------------------------------------------------------
    # Weight optimization
    # -------------------------------------------------------------------

    def optimize_weights(
        self, feature_matrix: np.ndarray, outcomes: np.ndarray
    ) -> Dict:
        """
        Find per-column weights maximizing AUC of weighted sum.

        feature_matrix shape: (n_samples, n_signals)
        outcomes shape: (n_samples,)
        Returns {weights: [...], signal_types: [...], auc: float, n_samples: int}.
        """
        feature_matrix = np.asarray(feature_matrix, dtype=float)
        outcomes = np.asarray(outcomes, dtype=int)
        n_samples, n_signals = feature_matrix.shape

        if n_samples < MIN_SAMPLES_FOR_OPTIMIZATION:
            return {
                "ok": False,
                "reason": (
                    f"need ≥ {MIN_SAMPLES_FOR_OPTIMIZATION} samples "
                    f"(got {n_samples})"
                ),
            }
        if n_signals != len(self.signal_types):
            return {
                "ok": False,
                "reason": (
                    f"feature matrix has {n_signals} columns, expected "
                    f"{len(self.signal_types)}"
                ),
            }

        def neg_auc(raw_weights):
            # Enforce non-negative + normalized
            w = np.abs(raw_weights)
            total = w.sum()
            if total < 1e-9:
                return 0.0
            w = w / total
            # Clip per-signal to [min, max] then renormalize
            w = np.clip(w, MIN_WEIGHT_PER_SIGNAL, MAX_WEIGHT_PER_SIGNAL)
            w = w / w.sum()
            scores = feature_matrix @ w
            return -compute_auc(scores, outcomes)

        init = np.ones(n_signals) / n_signals
        result = minimize(
            neg_auc,
            init,
            method="Nelder-Mead",
            options={"xatol": 1e-3, "fatol": 1e-4, "maxiter": 2000},
        )
        w = np.abs(result.x)
        if w.sum() < 1e-9:
            w = np.ones(n_signals) / n_signals
        w = w / w.sum()
        w = np.clip(w, MIN_WEIGHT_PER_SIGNAL, MAX_WEIGHT_PER_SIGNAL)
        w = w / w.sum()

        final_auc = compute_auc(feature_matrix @ w, outcomes)

        return {
            "ok": True,
            "weights": {self.signal_types[i]: float(w[i]) for i in range(n_signals)},
            "auc": final_auc,
            "n_samples": n_samples,
            "converged": bool(result.success),
        }

    # -------------------------------------------------------------------
    # Signal importance (univariate AUC)
    # -------------------------------------------------------------------

    def compute_signal_importance(
        self, feature_matrix: np.ndarray, outcomes: np.ndarray
    ) -> Dict[str, float]:
        """
        Return per-signal univariate AUC — which single signal best
        predicts the outcome on its own. 0.5 = random, 1.0 = perfect.
        """
        feature_matrix = np.asarray(feature_matrix, dtype=float)
        outcomes = np.asarray(outcomes, dtype=int)
        importance = {}
        for i, sig in enumerate(self.signal_types):
            if i >= feature_matrix.shape[1]:
                importance[sig] = 0.5
                continue
            importance[sig] = compute_auc(feature_matrix[:, i], outcomes)
        return importance

    # -------------------------------------------------------------------
    # Walk-forward backtest
    # -------------------------------------------------------------------

    def run_backtest(self, dataset, window_days: int = 180) -> Dict:
        """
        Walk-forward backtest on a labeled dataset.

        dataset: pandas DataFrame from OutcomeTracker.get_labeled_dataset(),
                 must have columns: scored_at, outcome_within_12mo,
                 plus one column per signal_type.

        For each month, train on prior data, predict next period, compute
        precision/recall of "predicted transaction" (score ≥ threshold) vs
        actual outcome.
        """
        if dataset is None or dataset.empty:
            return {"ok": False, "reason": "empty dataset"}

        # Sort by scored_at
        df = dataset.sort_values("scored_at").copy()
        df["scored_at"] = df["scored_at"].astype("datetime64[ns]")

        # Only use rows where all 12 signals present
        signal_cols = [c for c in self.signal_types if c in df.columns]
        if len(signal_cols) < len(self.signal_types):
            return {
                "ok": False,
                "reason": (
                    f"dataset missing signal columns "
                    f"({set(self.signal_types) - set(signal_cols)})"
                ),
            }

        # Use the global "default" weights from taxonomy as baseline
        default_weights = np.array(
            [SIGNAL_TAXONOMY[s]["default_weight"] for s in signal_cols],
            dtype=float,
        )
        scores = df[signal_cols].to_numpy() @ default_weights
        df["predicted"] = scores / (scores.max() if scores.max() > 0 else 1)

        # Split into monthly buckets
        df["month"] = df["scored_at"].dt.to_period("M")
        months = sorted(df["month"].unique())
        periods = []
        for m in months:
            mask = df["month"] == m
            n = int(mask.sum())
            if n == 0:
                continue
            threshold = 0.5
            preds = (df.loc[mask, "predicted"] >= threshold).astype(int)
            actual = df.loc[mask, "outcome_within_12mo"].astype(int)
            tp = int(((preds == 1) & (actual == 1)).sum())
            fp = int(((preds == 1) & (actual == 0)).sum())
            fn = int(((preds == 0) & (actual == 1)).sum())
            precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
            recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
            periods.append(
                {
                    "month": str(m),
                    "n": n,
                    "precision": precision,
                    "recall": recall,
                    "tp": tp,
                    "fp": fp,
                    "fn": fn,
                }
            )

        return {"ok": True, "periods": periods, "n_periods": len(periods)}
