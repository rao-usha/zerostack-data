"""
Deal Radar — Convergence Intelligence Database Models.

Three tables for geographic signal convergence tracking:
- convergence_regions: 13 US macro-regions with per-signal and composite scores
- convergence_signals: Individual signal events for the live feed
- convergence_clusters: Persisted cluster events when regions cross thresholds
"""

from sqlalchemy import (
    Column,
    Integer,
    String,
    Text,
    DateTime,
    Float,
    JSON,
    Index,
)
from sqlalchemy.sql import func
from app.core.models import Base


class ConvergenceRegion(Base):
    """
    13 US macro-regions with computed convergence scores.

    Each region maps to a set of US states. Signal scorers query
    source tables (EPA, IRS, Trade, Water, Macro) filtered by
    region states and produce 0-100 scores. The composite
    convergence_score determines cluster_status (HOT/ACTIVE/WATCH/LOW).
    """

    __tablename__ = "convergence_regions"

    id = Column(Integer, primary_key=True)
    region_id = Column(String(30), unique=True, nullable=False, index=True)
    label = Column(String(50), nullable=False)
    states = Column(JSON, nullable=False)  # ["AL", "GA", "SC", ...]
    center_lat = Column(Float)
    center_lon = Column(Float)

    # Per-signal scores (0-100)
    epa_score = Column(Float, default=0)
    irs_migration_score = Column(Float, default=0)
    trade_score = Column(Float, default=0)
    water_score = Column(Float, default=0)
    macro_score = Column(Float, default=0)

    # Composite
    convergence_score = Column(Float, default=0)
    convergence_grade = Column(String(2))  # A/B/C/D/F
    cluster_status = Column(String(10))  # HOT/ACTIVE/WATCH/LOW
    active_signals = Column(JSON)  # ["EPA", "IRS", "Water"]
    signal_count = Column(Integer, default=0)

    scored_at = Column(DateTime, server_default=func.now())
    created_at = Column(DateTime, server_default=func.now())

    __table_args__ = (
        Index("ix_convergence_regions_score", "convergence_score"),
        Index("ix_convergence_regions_status", "cluster_status"),
    )

    def to_dict(self):
        return {
            "region_id": self.region_id,
            "label": self.label,
            "states": self.states,
            "center_lat": self.center_lat,
            "center_lon": self.center_lon,
            "epa_score": self.epa_score,
            "irs_migration_score": self.irs_migration_score,
            "trade_score": self.trade_score,
            "water_score": self.water_score,
            "macro_score": self.macro_score,
            "convergence_score": self.convergence_score,
            "convergence_grade": self.convergence_grade,
            "cluster_status": self.cluster_status,
            "active_signals": self.active_signals,
            "signal_count": self.signal_count,
            "scored_at": self.scored_at.isoformat() if self.scored_at else None,
        }


class ConvergenceSignal(Base):
    """
    Individual signal events for the live feed.

    Each scan produces multiple signals — one per region per signal type.
    These populate the "Live signal feed" sidebar in the Deal Radar UI.
    """

    __tablename__ = "convergence_signals"

    id = Column(Integer, primary_key=True)
    region_id = Column(String(30), nullable=False, index=True)
    signal_type = Column(String(20), nullable=False)  # epa|irs|trade|water|macro
    score = Column(Float, nullable=False)
    description = Column(Text)
    raw_data = Column(JSON)
    detected_at = Column(DateTime, server_default=func.now())
    batch_id = Column(String(60), index=True)

    __table_args__ = (
        Index("ix_convergence_signals_detected", "detected_at"),
        Index("ix_convergence_signals_type_region", "signal_type", "region_id"),
    )

    def to_dict(self):
        return {
            "id": self.id,
            "region_id": self.region_id,
            "signal_type": self.signal_type,
            "score": self.score,
            "description": self.description,
            "detected_at": self.detected_at.isoformat() if self.detected_at else None,
            "batch_id": self.batch_id,
        }


class ConvergenceCluster(Base):
    """
    Persisted cluster events — created when a region crosses threshold.

    Stores the AI-generated investment thesis and sub-scores
    (opportunity, urgency, risk) for regions with convergence_score >= 44.
    """

    __tablename__ = "convergence_clusters"

    id = Column(Integer, primary_key=True)
    region_id = Column(String(30), nullable=False, index=True)
    convergence_score = Column(Float, nullable=False)
    signal_count = Column(Integer, default=0)
    active_signals = Column(JSON)  # ["EPA", "IRS", ...]
    cluster_status = Column(String(10))  # HOT/ACTIVE/WATCH

    # AI-generated thesis (cached)
    thesis_text = Column(Text)
    opportunity_score = Column(Float)
    urgency_score = Column(Float)
    risk_score = Column(Float)

    detected_at = Column(DateTime, server_default=func.now())

    __table_args__ = (
        Index("ix_convergence_clusters_score", "convergence_score"),
        Index("ix_convergence_clusters_detected", "detected_at"),
    )

    def to_dict(self):
        return {
            "id": self.id,
            "region_id": self.region_id,
            "convergence_score": self.convergence_score,
            "signal_count": self.signal_count,
            "active_signals": self.active_signals,
            "cluster_status": self.cluster_status,
            "thesis_text": self.thesis_text,
            "opportunity_score": self.opportunity_score,
            "urgency_score": self.urgency_score,
            "risk_score": self.risk_score,
            "detected_at": self.detected_at.isoformat() if self.detected_at else None,
        }
