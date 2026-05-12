"""
LightGBM distillation for the synthetic consumer crowd
(SPEC_050 / PLAN_062 addendum 1, Phase 2).

Trains one LightGBM regressor per outcome variable on the 2,000-row teacher
dataset produced by dataset_builder.py. The trained models are the inference
surface for Phase 3's API endpoint — they replace the expensive GPT-4o teacher
with microsecond-latency tabular predictions.

For churn_probability, training is restricted to rows where the scenario
defines churn as applicable (no existing customer relationship → null). The
predict-time wrapper similarly returns null when the scenario doesn't apply.

CLI:
  python -m app.services.synthetic.consumer_crowd.distiller \\
    --input /app/data/reports/synthetic_consumer/v1.jsonl \\
    --output-dir /app/data/reports/synthetic_consumer/distilled_v1/
"""

from __future__ import annotations

import argparse
import json
import logging
import math
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, List, Optional

import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split

logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------
# Feature spec
# -----------------------------------------------------------------------

CATEGORICAL_FEATURES = [
    # Persona
    "age_bracket",
    "income_bracket",
    "geography",
    "price_sensitivity",
    "brand_loyalty",
    "novelty_seeking",
    # Scenario
    "scenario_category",
    "scenario_event_type",
]

NUMERIC_FEATURES = [
    "scenario_magnitude",
]

ALL_FEATURES = CATEGORICAL_FEATURES + NUMERIC_FEATURES

OUTCOMES = [
    "purchase_intent",
    "wtp_delta",
    "sentiment",
    "wom_amplitude",
    "churn_probability",
]


@dataclass
class OutcomeMetrics:
    outcome: str
    n_train: int
    n_test: int
    train_rmse: float
    test_rmse: float
    baseline_rmse: float  # predict-mean baseline
    rmse_ratio: float     # test_rmse / baseline_rmse — must be ≤ 1.5 to pass
    overfit_ratio: float  # test_rmse / train_rmse — must be ≤ 2.0 to pass
    feature_importance: Dict[str, float]

    def passed(self) -> bool:
        return self.rmse_ratio <= 1.5 and self.overfit_ratio <= 2.0


@dataclass
class DistillationReport:
    n_total_rows: int
    outcomes: List[OutcomeMetrics]
    all_passed: bool
    output_dir: str

    def to_dict(self) -> dict:
        return {
            "n_total_rows": self.n_total_rows,
            "outcomes": [asdict(o) for o in self.outcomes],
            "all_passed": self.all_passed,
            "output_dir": self.output_dir,
        }


# -----------------------------------------------------------------------
# Training
# -----------------------------------------------------------------------

def _prepare_features(df: pd.DataFrame) -> pd.DataFrame:
    """Convert categorical columns to pandas Categorical so LightGBM uses native categorical handling."""
    X = df[ALL_FEATURES].copy()
    for col in CATEGORICAL_FEATURES:
        X[col] = X[col].astype("category")
    return X


def _train_one_outcome(
    df: pd.DataFrame,
    outcome: str,
    output_dir: Path,
    test_size: float = 0.2,
    random_state: int = 42,
) -> OutcomeMetrics:
    """Train a LightGBM regressor for a single outcome variable."""

    # Filter to rows where this outcome has a non-null label (applicable to scenario)
    sub = df[df[outcome].notna()].copy()

    X = _prepare_features(sub)
    y = sub[outcome].astype(float).values

    # Stratify on scenario category so train/test cover the category space
    X_train, X_test, y_train, y_test = train_test_split(
        X, y,
        test_size=test_size,
        random_state=random_state,
        stratify=sub["scenario_category"].values,
    )

    train_data = lgb.Dataset(
        X_train, label=y_train,
        categorical_feature=CATEGORICAL_FEATURES,
    )
    valid_data = lgb.Dataset(
        X_test, label=y_test,
        reference=train_data,
        categorical_feature=CATEGORICAL_FEATURES,
    )

    params = {
        "objective": "regression",
        "metric": "rmse",
        "num_leaves": 31,
        "learning_rate": 0.05,
        "feature_fraction": 0.9,
        "bagging_fraction": 0.8,
        "bagging_freq": 5,
        "verbose": -1,
        "min_data_in_leaf": 5,  # Smaller leaves OK at our N=1600 train
    }

    model = lgb.train(
        params,
        train_data,
        num_boost_round=500,
        valid_sets=[train_data, valid_data],
        valid_names=["train", "valid"],
        callbacks=[
            lgb.early_stopping(stopping_rounds=30, verbose=False),
            lgb.log_evaluation(period=0),  # silent
        ],
    )

    # Metrics
    train_pred = model.predict(X_train)
    test_pred = model.predict(X_test)
    train_rmse = float(math.sqrt(np.mean((train_pred - y_train) ** 2)))
    test_rmse = float(math.sqrt(np.mean((test_pred - y_test) ** 2)))

    # Baseline: predict training mean
    baseline_pred = np.full_like(y_test, fill_value=np.mean(y_train), dtype=float)
    baseline_rmse = float(math.sqrt(np.mean((baseline_pred - y_test) ** 2)))

    # Avoid divide-by-zero when baseline is perfect
    rmse_ratio = test_rmse / baseline_rmse if baseline_rmse > 1e-6 else 0.0
    overfit_ratio = test_rmse / train_rmse if train_rmse > 1e-6 else float("inf")

    # Feature importance (split count)
    importance_values = model.feature_importance(importance_type="split")
    feature_importance = {
        name: float(val) for name, val in zip(ALL_FEATURES, importance_values)
    }

    # Persist model + metadata
    output_dir.mkdir(parents=True, exist_ok=True)
    model_path = output_dir / f"{outcome}.txt"
    model.save_model(str(model_path))

    metrics = OutcomeMetrics(
        outcome=outcome,
        n_train=len(X_train),
        n_test=len(X_test),
        train_rmse=train_rmse,
        test_rmse=test_rmse,
        baseline_rmse=baseline_rmse,
        rmse_ratio=rmse_ratio,
        overfit_ratio=overfit_ratio,
        feature_importance=feature_importance,
    )

    status = "PASS" if metrics.passed() else "FAIL"
    logger.info(
        "[%s] %s — train_n=%d test_n=%d | train_rmse=%.4f test_rmse=%.4f baseline=%.4f | "
        "rmse_ratio=%.3f (≤1.5) overfit_ratio=%.3f (≤2.0)",
        status, outcome, metrics.n_train, metrics.n_test,
        metrics.train_rmse, metrics.test_rmse, metrics.baseline_rmse,
        metrics.rmse_ratio, metrics.overfit_ratio,
    )

    return metrics


def train_all_outcomes(
    input_jsonl: Path,
    output_dir: Path,
    random_state: int = 42,
) -> DistillationReport:
    """Train one LightGBM per outcome variable on the teacher dataset."""
    logger.info("Loading teacher dataset from %s", input_jsonl)
    df = pd.read_json(input_jsonl, lines=True)
    logger.info("Loaded %d rows × %d cols", len(df), len(df.columns))

    output_dir.mkdir(parents=True, exist_ok=True)

    metrics_list = []
    for outcome in OUTCOMES:
        metrics_list.append(_train_one_outcome(df, outcome, output_dir, random_state=random_state))

    all_passed = all(m.passed() for m in metrics_list)

    report = DistillationReport(
        n_total_rows=len(df),
        outcomes=metrics_list,
        all_passed=all_passed,
        output_dir=str(output_dir),
    )

    # Persist report
    report_path = output_dir / "distillation_report.json"
    report_path.write_text(json.dumps(report.to_dict(), indent=2))
    logger.info("Wrote distillation report to %s", report_path)

    # Persist feature spec so model.py can read it
    spec_path = output_dir / "feature_spec.json"
    spec_path.write_text(json.dumps({
        "categorical_features": CATEGORICAL_FEATURES,
        "numeric_features": NUMERIC_FEATURES,
        "all_features": ALL_FEATURES,
        "outcomes": OUTCOMES,
    }, indent=2))

    return report


# -----------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Distill the consumer-crowd teacher dataset into LightGBM models.")
    parser.add_argument("--input", required=True, help="Path to teacher dataset JSONL")
    parser.add_argument("--output-dir", required=True, help="Directory to write distilled models + report")
    parser.add_argument("--random-state", type=int, default=42)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    report = train_all_outcomes(Path(args.input), Path(args.output_dir), random_state=args.random_state)

    print()
    print("=" * 80)
    print(f"DISTILLATION REPORT — {report.n_total_rows} rows → {report.output_dir}")
    print("=" * 80)
    for o in report.outcomes:
        status = "✓ PASS" if o.passed() else "✗ FAIL"
        print(f"  {status}  {o.outcome:24s}  train_rmse={o.train_rmse:.4f}  "
              f"test_rmse={o.test_rmse:.4f}  baseline={o.baseline_rmse:.4f}  "
              f"rmse_ratio={o.rmse_ratio:.3f}  overfit_ratio={o.overfit_ratio:.3f}")
    print(f"\nALL PASSED: {report.all_passed}")


if __name__ == "__main__":
    main()
