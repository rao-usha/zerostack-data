"""
Deal Probability Engine — Gated ML Model (SPEC 048, PLAN_059 Phase 4).

LightGBM gradient-boosted classifier. Activated only when:
  - ≥ 200 labeled samples are available
  - LightGBM is installed

When either condition fails, the engine transparently falls back to the
rule-based + calibrated-sigmoid path from Phases 2-4.

Features (37 total):
  - 12 signal scores (raw 0-100)
  - 12 signal velocities (score delta per period)
  - 12 signal accelerations (velocity delta)
  - 1 convergence count

Label: `outcome_within_12mo` (0/1) from OutcomeTracker.

Explainability: SHAP values for each signal. When SHAP is unavailable,
falls back to gain-based feature importance.
"""

from __future__ import annotations

import logging
import pickle
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

import numpy as np

from app.ml.probability_signal_taxonomy import SIGNAL_TAXONOMY

logger = logging.getLogger(__name__)


MIN_LABELED_SAMPLES = 200


class ModelUnavailableError(RuntimeError):
    """Raised when LightGBM is not installed or insufficient data to train."""


# ---------------------------------------------------------------------------
# Availability check
# ---------------------------------------------------------------------------


def is_lightgbm_available() -> bool:
    """Cheap import probe; cached in module global after first call."""
    try:
        import lightgbm  # noqa: F401

        return True
    except ImportError:
        return False


def is_shap_available() -> bool:
    try:
        import shap  # noqa: F401

        return True
    except ImportError:
        return False


# ---------------------------------------------------------------------------
# Model wrapper
# ---------------------------------------------------------------------------


@dataclass
class TrainingResult:
    ok: bool
    reason: Optional[str] = None
    n_samples: int = 0
    auc: Optional[float] = None
    feature_importance: Optional[Dict[str, float]] = None
    trained_at: Optional[str] = None


class TransactionProbabilityModel:
    """LightGBM-backed probability model. Raises when unavailable."""

    def __init__(self):
        if not is_lightgbm_available():
            raise ModelUnavailableError(
                "LightGBM is not installed. "
                "`pip install lightgbm` to enable the ML model, or "
                "continue using the rule-based + calibrated pipeline."
            )
        self.model = None
        self.feature_names: List[str] = []
        self.trained_at: Optional[datetime] = None

    # -------------------------------------------------------------------
    # Feature engineering
    # -------------------------------------------------------------------

    @staticmethod
    def build_feature_matrix(dataset) -> np.ndarray:
        """
        Turn a labeled-dataset DataFrame into the 37-feature matrix.

        Assumes the DataFrame has signal_type columns (scores),
        `*_velocity` and `*_acceleration` columns (may be absent — zero-filled),
        and `convergence_count` (absent → zero).
        """
        signals = list(SIGNAL_TAXONOMY.keys())
        cols = []
        for sig in signals:
            cols.append(dataset[sig].fillna(50).to_numpy() if sig in dataset.columns else np.full(len(dataset), 50))
        for sig in signals:
            vel_col = f"{sig}_velocity"
            cols.append(dataset[vel_col].fillna(0).to_numpy() if vel_col in dataset.columns else np.zeros(len(dataset)))
        for sig in signals:
            acc_col = f"{sig}_acceleration"
            cols.append(dataset[acc_col].fillna(0).to_numpy() if acc_col in dataset.columns else np.zeros(len(dataset)))
        conv = (
            dataset["convergence_count"].fillna(0).to_numpy()
            if "convergence_count" in dataset.columns
            else np.zeros(len(dataset))
        )
        cols.append(conv)
        return np.column_stack(cols).astype(float)

    @staticmethod
    def feature_names() -> List[str]:
        signals = list(SIGNAL_TAXONOMY.keys())
        return (
            signals
            + [f"{s}_velocity" for s in signals]
            + [f"{s}_acceleration" for s in signals]
            + ["convergence_count"]
        )

    # -------------------------------------------------------------------
    # Train / predict
    # -------------------------------------------------------------------

    def train(self, dataset) -> TrainingResult:
        """Train the model on a labeled dataset. Returns TrainingResult."""
        if "outcome_within_12mo" not in dataset.columns:
            return TrainingResult(ok=False, reason="dataset missing outcome_within_12mo column")
        if len(dataset) < MIN_LABELED_SAMPLES:
            return TrainingResult(
                ok=False,
                reason=f"need ≥ {MIN_LABELED_SAMPLES} samples (got {len(dataset)})",
                n_samples=len(dataset),
            )

        import lightgbm as lgb

        X = self.build_feature_matrix(dataset)
        y = dataset["outcome_within_12mo"].astype(int).to_numpy()
        self.feature_names = self.feature_names()

        # 80/20 train/val split by scored_at if available, else random
        if "scored_at" in dataset.columns:
            order = np.argsort(dataset["scored_at"].values)
            X = X[order]
            y = y[order]
        split = int(len(X) * 0.8)
        X_train, X_val = X[:split], X[split:]
        y_train, y_val = y[:split], y[split:]

        train_ds = lgb.Dataset(X_train, label=y_train, feature_name=self.feature_names)
        val_ds = lgb.Dataset(X_val, label=y_val, feature_name=self.feature_names, reference=train_ds)

        params = {
            "objective": "binary",
            "metric": "auc",
            "learning_rate": 0.05,
            "num_leaves": 31,
            "feature_fraction": 0.9,
            "bagging_fraction": 0.8,
            "bagging_freq": 5,
            "verbosity": -1,
        }
        self.model = lgb.train(
            params,
            train_ds,
            num_boost_round=200,
            valid_sets=[val_ds],
            callbacks=[lgb.early_stopping(20), lgb.log_evaluation(0)],
        )
        self.trained_at = datetime.utcnow()

        # Evaluate
        from app.ml.probability_weight_optimizer import compute_auc

        val_preds = self.model.predict(X_val)
        auc = compute_auc(val_preds, y_val)

        importance = dict(
            zip(
                self.feature_names,
                self.model.feature_importance(importance_type="gain").tolist(),
            )
        )

        return TrainingResult(
            ok=True,
            n_samples=len(X),
            auc=auc,
            feature_importance=importance,
            trained_at=self.trained_at.isoformat(),
        )

    def predict(self, feature_vector: Dict[str, float]) -> float:
        """Predict P(transaction) from a feature dict."""
        if self.model is None:
            raise ModelUnavailableError("Model not trained yet")
        ordered = np.array(
            [[feature_vector.get(name, 0.0) for name in self.feature_names]],
            dtype=float,
        )
        return float(self.model.predict(ordered)[0])

    def explain(self, feature_vector: Dict[str, float]) -> List[Dict]:
        """
        SHAP values per feature. Falls back to feature_importance if
        SHAP unavailable.
        """
        if self.model is None:
            raise ModelUnavailableError("Model not trained yet")

        ordered = np.array(
            [[feature_vector.get(name, 0.0) for name in self.feature_names]],
            dtype=float,
        )

        if is_shap_available():
            import shap

            explainer = shap.TreeExplainer(self.model)
            shap_values = explainer.shap_values(ordered)[0]
            return [
                {"feature": name, "value": float(feature_vector.get(name, 0.0)),
                 "shap": float(shap_values[i])}
                for i, name in enumerate(self.feature_names)
            ]

        # Fallback: gain-based feature importance (not sample-specific)
        imp = self.model.feature_importance(importance_type="gain").tolist()
        return [
            {"feature": name, "value": float(feature_vector.get(name, 0.0)),
             "gain": float(imp[i])}
            for i, name in enumerate(self.feature_names)
        ]

    # -------------------------------------------------------------------
    # Persistence (pickle — LightGBM has its own model IO but this is fine
    # for a small model and keeps the API uniform)
    # -------------------------------------------------------------------

    def save(self, path: str) -> None:
        with open(path, "wb") as f:
            pickle.dump(
                {"model": self.model, "feature_names": self.feature_names, "trained_at": self.trained_at},
                f,
            )

    @classmethod
    def load(cls, path: str) -> "TransactionProbabilityModel":
        instance = cls()
        with open(path, "rb") as f:
            data = pickle.load(f)
        instance.model = data["model"]
        instance.feature_names = data["feature_names"]
        instance.trained_at = data.get("trained_at")
        return instance
