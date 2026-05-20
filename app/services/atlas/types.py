"""
Atlas typed payload models — SPEC_064.

Plain dataclasses, JSON-serializable via `to_dict()`. No pydantic here — the
API layer (app/api/v1/atlas.py) owns request/response validation; these are
the internal shapes the resolver / cards / graph / service modules pass
around and persist as JSONB.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


# Confidence levels for a card — drives UI emphasis + future ranking.
CONFIDENCE_HIGH = "high"
CONFIDENCE_MEDIUM = "medium"
CONFIDENCE_LOW = "low"


@dataclass
class Provenance:
    """One source-trail entry — a dataset table + how much of it was used."""
    dataset: str
    description: str
    rows_used: int

    def to_dict(self) -> Dict[str, Any]:
        return {"dataset": self.dataset, "description": self.description,
                "rows_used": self.rows_used}


@dataclass
class Metric:
    """A single labeled number/string on a card."""
    label: str
    value: str
    detail: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        d = {"label": self.label, "value": self.value}
        if self.detail:
            d["detail"] = self.detail
        return d


@dataclass
class Card:
    """One insight card. The atomic unit of an exploration.

    Every card carries the full SPEC_064 contract: title, summary, metrics,
    why_it_matters, datasets_used, confidence, coverage, provenance, links.
    """
    id: str
    title: str
    summary: str
    metrics: List[Metric] = field(default_factory=list)
    why_it_matters: str = ""
    datasets_used: List[str] = field(default_factory=list)
    confidence: str = CONFIDENCE_MEDIUM
    coverage: Dict[str, Any] = field(default_factory=dict)
    provenance: List[Provenance] = field(default_factory=list)
    links: List[Dict[str, str]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "title": self.title,
            "summary": self.summary,
            "metrics": [m.to_dict() for m in self.metrics],
            "why_it_matters": self.why_it_matters,
            "datasets_used": list(self.datasets_used),
            "confidence": self.confidence,
            "coverage": self.coverage,
            "provenance": [p.to_dict() for p in self.provenance],
            "links": list(self.links),
        }


@dataclass
class Connection:
    """A relationship edge between two cards."""
    source_card: str
    target_card: str
    relationship: str
    strength: float = 0.5

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source_card": self.source_card,
            "target_card": self.target_card,
            "relationship": self.relationship,
            "strength": self.strength,
        }


@dataclass
class ResolvedEntities:
    """What the resolver made of the query."""
    msa: Optional[Dict[str, str]] = None      # {"code","title"}
    naics: Optional[Dict[str, str]] = None    # {"code","label"}
    geographies: List[str] = field(default_factory=list)   # county FIPS
    datasets: List[str] = field(default_factory=list)      # tables with data

    def to_dict(self) -> Dict[str, Any]:
        return {
            "msa": self.msa,
            "naics": self.naics,
            "geographies": list(self.geographies),
            "datasets": list(self.datasets),
        }


@dataclass
class Exploration:
    """The full object returned by POST /atlas/explore."""
    slug: str
    query: str
    resolved_entities: ResolvedEntities
    summary: Dict[str, Any] = field(default_factory=dict)
    cards: List[Card] = field(default_factory=list)
    connections: List[Connection] = field(default_factory=list)
    related_queries: List[Dict[str, str]] = field(default_factory=list)
    id: Optional[int] = None
    share_url: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "slug": self.slug,
            "query": self.query,
            "resolved_entities": self.resolved_entities.to_dict(),
            "summary": self.summary,
            "cards": [c.to_dict() for c in self.cards],
            "connections": [c.to_dict() for c in self.connections],
            "related_queries": list(self.related_queries),
            "share_url": self.share_url or f"/atlas/{self.slug}",
        }
