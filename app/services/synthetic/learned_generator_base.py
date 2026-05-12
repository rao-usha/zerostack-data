"""
LearnedSyntheticGenerator — unified runtime contract for v2 synthetic generators
(PLAN_062 W2.1).

All learned-model generators implement this ABC so they share:
- Training entry point (consume a DB session, produce a ModelArtifact on disk)
- Loading entry point (from artifact path)
- Sampling entry point (from a loaded generator)
- Validation entry point (against held-out real data)

Phase 2 of PLAN_062 addendum 1 (DistilledCrowdModel) is the first concrete
implementation. Future implementations from PLAN_062:
- TabDDPMGenerator (Phase A1 — private financials)
- DiffusionTSGenerator (Phase A2 — macro scenarios)
- (Later) RotatEGraphCompleter (Phase B — LP→GP graph)

The contract is deliberately thin — it standardizes the surfaces a generator
exposes to its callers (API endpoints, validation harness, downstream scorers)
without prescribing implementation details (torch vs lightgbm vs pykeen).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd


# -----------------------------------------------------------------------
# Common data structures
# -----------------------------------------------------------------------

@dataclass
class TrainingConfig:
    """Inputs to a training run. Implementations may extend with extra fields."""
    input_path: Path                      # Where training data lives (parquet, jsonl, DB query, etc.)
    output_dir: Path                       # Where to write the model artifact + reports
    random_state: int = 42
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ModelArtifact:
    """Pointer to a trained model on disk plus its metadata."""
    path: Path                             # Directory or file containing the model
    model_name: str                        # e.g. "tabddpm_v1", "distilled_consumer_v1"
    schema_version: str                    # e.g. "v1", for cross-version compat checks
    training_run_id: str                   # Unique id (UUID or timestamp-based) for audit
    metrics: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ValidationReport:
    """Standard shape for validation output across all generator types."""
    model_name: str
    all_passed: bool
    metric_results: Dict[str, Dict[str, Any]]  # e.g. {"sentiment": {"rmse": 0.18, "passed": True}}
    summary: str = ""


# -----------------------------------------------------------------------
# Abstract base class
# -----------------------------------------------------------------------

class LearnedSyntheticGenerator(ABC):
    """
    Contract that every v2 learned generator implements.

    Implementations are expected to:
    - Be **stateful** after load() — singleton-friendly, no per-call I/O
    - Be **deterministic** for the same input given the same model artifact
    - Report **provenance** in sample() outputs (model_name + training_run_id)
    """

    # Each implementation sets this; used by provenance / API surface.
    model_name: str = "UNDEFINED"
    schema_version: str = "v1"

    # ------------------------------------------------------------------
    # Train
    # ------------------------------------------------------------------

    @classmethod
    @abstractmethod
    def train(cls, config: TrainingConfig) -> ModelArtifact:
        """
        Train a fresh model from data described by `config`. Persists the
        model and any artifacts (e.g., feature spec, validation report) to
        `config.output_dir`. Returns a ModelArtifact pointing to the result.

        Implementations are responsible for:
        - Splitting / validating training data
        - Persisting all files needed by load()
        - Writing a training report (recommended path: output_dir/training_report.json)
        """
        ...

    # ------------------------------------------------------------------
    # Load
    # ------------------------------------------------------------------

    @classmethod
    @abstractmethod
    def load(cls, artifact_path: Path) -> "LearnedSyntheticGenerator":
        """Load a previously-trained model from disk."""
        ...

    # ------------------------------------------------------------------
    # Sample
    # ------------------------------------------------------------------

    @abstractmethod
    def sample(self, n: int, **conditions) -> pd.DataFrame:
        """
        Generate `n` synthetic rows conditioned on the kwargs.

        Implementations should:
        - Honor the `n` row count
        - Use kwargs to condition generation (e.g., NAICS, revenue_bucket for
          TabDDPM; persona+scenario for DistilledCrowdModel; series_id for
          Diffusion-TS)
        - Include provenance columns: `synthetic_model_name`, `synthetic_run_id`
        """
        ...

    # ------------------------------------------------------------------
    # Validate
    # ------------------------------------------------------------------

    @abstractmethod
    def validate(self, held_out: pd.DataFrame) -> ValidationReport:
        """
        Validate the loaded model against held-out real data. Returns a
        ValidationReport with per-metric results.
        """
        ...

    # ------------------------------------------------------------------
    # Provenance helpers (concrete — shared across implementations)
    # ------------------------------------------------------------------

    def provenance_dict(self, run_id: Optional[str] = None) -> Dict[str, Any]:
        return {
            "synthetic_model_name": self.model_name,
            "synthetic_schema_version": self.schema_version,
            "synthetic_run_id": run_id,
        }
