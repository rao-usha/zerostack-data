"""
Unified Data Quality base classes.

Every dataset-specific DQ provider inherits BaseQualityProvider and returns
QualityReport objects scored with the same 4-dimension formula.

Score formula:
    quality_score = 0.35 × completeness + 0.25 × freshness
                  + 0.25 × validity    + 0.15 × consistency
    → clamped 0–100
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Literal

from sqlalchemy.orm import Session


@dataclass
class QualityIssue:
    """Single DQ finding returned by one check function."""

    check: str
    severity: Literal["ERROR", "WARNING", "INFO"]
    message: str
    count: int = 0
    dimension: Literal["completeness", "freshness", "validity", "consistency"] = "validity"


@dataclass
class QualityReport:
    """Full DQ report for one entity (company, deal, site, 3PL company, …)."""

    entity_id: int
    entity_name: str
    dataset: str        # "people" | "pe" | "site_intel" | "three_pl"
    quality_score: int  # 0–100 composite (4-dimension formula)
    completeness: int   # 0–100 sub-score
    freshness: int      # 0–100 sub-score
    validity: int       # 0–100 sub-score
    consistency: int    # 0–100 sub-score
    issues: List[QualityIssue] = field(default_factory=list)
    checked_at: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> dict:
        return {
            "entity_id": self.entity_id,
            "entity_name": self.entity_name,
            "dataset": self.dataset,
            "quality_score": self.quality_score,
            "dimensions": {
                "completeness": self.completeness,
                "freshness": self.freshness,
                "validity": self.validity,
                "consistency": self.consistency,
            },
            "issues": [
                {
                    "check": i.check,
                    "severity": i.severity,
                    "message": i.message,
                    "count": i.count,
                    "dimension": i.dimension,
                }
                for i in self.issues
            ],
            "checked_at": self.checked_at.isoformat(),
        }


def compute_quality_score(
    completeness: int,
    freshness: int,
    validity: int,
    consistency: int,
) -> int:
    """
    Unified 4-dimension formula shared by all BaseQualityProvider subclasses.

    Weights
    -------
    Completeness  35%  required fields present, headcount non-trivial
    Freshness     25%  data age vs per-dataset threshold
    Validity      25%  structural/rule checks pass
    Consistency   15%  cross-field coherence, dedup backlog

    Returns
    -------
    int  0–100
    """
    raw = (
        0.35 * max(0, min(100, completeness))
        + 0.25 * max(0, min(100, freshness))
        + 0.25 * max(0, min(100, validity))
        + 0.15 * max(0, min(100, consistency))
    )
    return max(0, min(100, round(raw)))


def _penalty(issues: list[QualityIssue], dimension: str) -> int:
    """
    Convert a list of issues for one dimension into a 0–100 score.

    ERROR  → -30 pts each
    WARNING → -15 pts each
    INFO    →  -5 pts each
    """
    score = 100
    for issue in issues:
        if issue.dimension != dimension:
            continue
        if issue.severity == "ERROR":
            score -= 30
        elif issue.severity == "WARNING":
            score -= 15
        else:
            score -= 5
    return max(0, score)


class BaseQualityProvider(ABC):
    """Abstract base for all dataset-specific DQ providers.

    Subclasses must:
    1. Set ``dataset`` class attribute (e.g. ``dataset = "pe"``).
    2. Implement ``run(entity_id, db) -> QualityReport``.
    3. Implement ``run_all(db, limit) -> list[QualityReport]`` sorted worst-first.
    """

    dataset: str  # must be declared by subclass

    @abstractmethod
    def run(self, entity_id: int, db: Session) -> QualityReport:
        """Run all checks for one entity."""

    @abstractmethod
    def run_all(self, db: Session, limit: int | None = None) -> list[QualityReport]:
        """Run checks across all entities, sorted worst quality_score first."""
