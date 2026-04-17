"""
Deal Probability Engine — Database Models (SPEC 045, PLAN_059 Phase 1).

Six tables that power the P(transaction within 6-12 months) scoring system:
- txn_prob_companies: Universe of scored companies (sourced from PE portfolio, industrial, Form D filers)
- txn_prob_signals: Per-company per-signal time-series snapshots with velocity/acceleration
- txn_prob_scores: Composite probability per company per run, with full signal chain
- txn_prob_outcomes: Ground truth labels for the learning loop
- txn_prob_alerts: Threshold-crossing alerts (probability spike, convergence, grade change)
- sector_signal_weights: Sector-specific weight overrides with version tracking
"""

from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    JSON,
    Numeric,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.sql import func

from app.core.models import Base


class TxnProbCompany(Base):
    """
    Universe of companies scored by the Deal Probability Engine.

    Populated from pe_portfolio_companies, industrial_companies, and recent
    Form D filers via CompanyUniverseBuilder. A company's canonical_company_id
    links back to pe_portfolio_companies when a match exists.
    """

    __tablename__ = "txn_prob_companies"

    id = Column(Integer, primary_key=True, autoincrement=True)
    company_name = Column(String(500), nullable=False)
    normalized_name = Column(String(500), nullable=False, index=True)
    canonical_company_id = Column(
        Integer, ForeignKey("pe_portfolio_companies.id", ondelete="SET NULL"), nullable=True
    )

    # Classification
    sector = Column(String(200), index=True)
    industry = Column(String(200))
    naics_code = Column(String(10))

    # Location
    hq_state = Column(String(100))
    hq_city = Column(String(200))

    # Size
    employee_count_est = Column(Integer)
    revenue_est_usd = Column(Numeric(18, 2))
    founded_year = Column(Integer)

    # Status
    ownership_status = Column(String(50))  # PE-Backed, VC-Backed, Private, Public
    universe_source = Column(String(50), nullable=False)  # pe_portfolio|industrial|form_d|manual
    is_active = Column(Boolean, default=True, index=True)

    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        UniqueConstraint("normalized_name", "sector", name="uq_txn_prob_company_name_sector"),
        Index("ix_txn_prob_companies_sector_active", "sector", "is_active"),
    )

    def to_dict(self):
        return {
            "id": self.id,
            "company_name": self.company_name,
            "canonical_company_id": self.canonical_company_id,
            "sector": self.sector,
            "industry": self.industry,
            "naics_code": self.naics_code,
            "hq_state": self.hq_state,
            "employee_count_est": self.employee_count_est,
            "revenue_est_usd": float(self.revenue_est_usd) if self.revenue_est_usd else None,
            "founded_year": self.founded_year,
            "ownership_status": self.ownership_status,
            "universe_source": self.universe_source,
            "is_active": self.is_active,
        }


class TxnProbSignal(Base):
    """
    Time-series snapshots of individual signals per company.

    One row per (company, signal_type, scored_at). Enables velocity
    (score change per period) and acceleration (velocity change) tracking —
    critical for detecting multi-signal convergence events.
    """

    __tablename__ = "txn_prob_signals"

    id = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(
        Integer, ForeignKey("txn_prob_companies.id", ondelete="CASCADE"), nullable=False
    )
    signal_type = Column(String(50), nullable=False)

    score = Column(Float, nullable=False)  # 0-100
    previous_score = Column(Float)
    velocity = Column(Float)  # Score change per period
    acceleration = Column(Float)  # Velocity change per period

    signal_details = Column(JSON)  # Raw breakdown of how the score was computed
    data_sources = Column(JSON)  # Which source tables contributed
    confidence = Column(Float, default=1.0)  # 0-1

    scored_at = Column(DateTime, server_default=func.now(), nullable=False)
    batch_id = Column(String(60), index=True)

    __table_args__ = (
        UniqueConstraint(
            "company_id", "signal_type", "scored_at", name="uq_txn_prob_signal_snapshot"
        ),
        Index("ix_txn_prob_signals_company_type", "company_id", "signal_type"),
        Index("ix_txn_prob_signals_type_score", "signal_type", "score"),
    )

    def to_dict(self):
        return {
            "id": self.id,
            "company_id": self.company_id,
            "signal_type": self.signal_type,
            "score": self.score,
            "previous_score": self.previous_score,
            "velocity": self.velocity,
            "acceleration": self.acceleration,
            "signal_details": self.signal_details,
            "data_sources": self.data_sources,
            "confidence": self.confidence,
            "scored_at": self.scored_at.isoformat() if self.scored_at else None,
            "batch_id": self.batch_id,
        }


class TxnProbScore(Base):
    """
    Composite transaction probability per company per scoring run.

    probability is the calibrated 0-1 output (P(transaction in 6-12 months)).
    signal_chain decomposes the composite into its contributing signals
    with weights — the explainability layer.
    """

    __tablename__ = "txn_prob_scores"

    id = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(
        Integer, ForeignKey("txn_prob_companies.id", ondelete="CASCADE"), nullable=False
    )

    probability = Column(Float, nullable=False)  # 0-1 calibrated
    raw_composite_score = Column(Float, nullable=False)  # 0-100
    grade = Column(String(2))  # A/B/C/D/F
    confidence = Column(Float, default=1.0)

    sector_weights_version = Column(String(20))
    signal_count = Column(Integer, default=0)
    active_signal_count = Column(Integer, default=0)  # Signals above threshold
    convergence_factor = Column(Float, default=1.0)  # Multi-signal convergence bonus

    top_signals = Column(JSON)  # Top 5 contributing signals
    signal_chain = Column(JSON)  # Full decomposition
    narrative_summary = Column(Text)  # LLM-generated (Phase 3)

    model_version = Column(String(20))
    scored_at = Column(DateTime, server_default=func.now(), nullable=False)
    batch_id = Column(String(60), index=True)

    __table_args__ = (
        Index("ix_txn_prob_scores_probability", "probability"),
        Index("ix_txn_prob_scores_company_date", "company_id", "scored_at"),
        Index("ix_txn_prob_scores_grade", "grade"),
    )

    def to_dict(self):
        return {
            "id": self.id,
            "company_id": self.company_id,
            "probability": self.probability,
            "raw_composite_score": self.raw_composite_score,
            "grade": self.grade,
            "confidence": self.confidence,
            "signal_count": self.signal_count,
            "active_signal_count": self.active_signal_count,
            "convergence_factor": self.convergence_factor,
            "top_signals": self.top_signals,
            "signal_chain": self.signal_chain,
            "narrative_summary": self.narrative_summary,
            "model_version": self.model_version,
            "scored_at": self.scored_at.isoformat() if self.scored_at else None,
            "batch_id": self.batch_id,
        }


class TxnProbOutcome(Base):
    """
    Ground truth labels for the learning loop.

    Populated by the OutcomeTracker (Phase 4) scanning pe_deals, SEC filings,
    and news for actual transactions. Records what we predicted at 3 snapshots
    prior to the event for calibration analysis.
    """

    __tablename__ = "txn_prob_outcomes"

    id = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(
        Integer, ForeignKey("txn_prob_companies.id", ondelete="CASCADE"), nullable=False
    )

    outcome_type = Column(
        String(50), nullable=False
    )  # acquired|ipo|secondary_sale|recap|spac_merger|no_transaction
    announced_date = Column(Date)
    closed_date = Column(Date)
    deal_value_usd = Column(Numeric(18, 2))
    buyer_name = Column(String(500))

    deal_source = Column(String(50))  # pe_deals|sec_filing|news|manual
    pe_deal_id = Column(Integer, nullable=True)  # FK to pe_deals when available

    # What we predicted at various horizons (for calibration)
    prediction_at_announcement = Column(Float)
    prediction_6mo_prior = Column(Float)
    prediction_12mo_prior = Column(Float)

    created_at = Column(DateTime, server_default=func.now())

    __table_args__ = (
        Index("ix_txn_prob_outcomes_company", "company_id"),
        Index("ix_txn_prob_outcomes_type", "outcome_type"),
        Index("ix_txn_prob_outcomes_announced", "announced_date"),
    )


class TxnProbAlert(Base):
    """
    Threshold-crossing alerts generated by the scoring engine.

    Alert types:
    - probability_spike: probability delta > 15%
    - grade_change: grade upgraded (e.g., C -> B)
    - new_convergence: new named convergence pattern detected
    - signal_acceleration: signal acceleration crossed threshold
    - new_universe_entry: company newly added to universe
    """

    __tablename__ = "txn_prob_alerts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(
        Integer, ForeignKey("txn_prob_companies.id", ondelete="CASCADE"), nullable=False
    )

    alert_type = Column(String(50), nullable=False)
    severity = Column(String(10), nullable=False)  # high|medium|low
    title = Column(String(500), nullable=False)
    description = Column(Text)

    probability_before = Column(Float)
    probability_after = Column(Float)
    triggering_signals = Column(JSON)

    is_read = Column(Boolean, default=False)
    created_at = Column(DateTime, server_default=func.now())

    __table_args__ = (
        Index("ix_txn_prob_alerts_unread", "is_read", "created_at"),
        Index("ix_txn_prob_alerts_company", "company_id"),
        Index("ix_txn_prob_alerts_severity", "severity"),
    )


class TxnProbCalibration(Base):
    """
    Persisted calibration models for raw composite → probability mapping.

    Phase 4: when enough labeled outcomes accumulate, we fit Platt or
    isotonic calibration per scope (global or sector) and flip `is_active`
    to let the engine prefer it over the default sigmoid.
    """

    __tablename__ = "txn_prob_calibrations"

    id = Column(Integer, primary_key=True, autoincrement=True)
    scope = Column(String(100), nullable=False, index=True)  # "global" or sector name
    method = Column(String(20), nullable=False)  # sigmoid|platt|isotonic
    params = Column(JSON, nullable=False)  # {k, x0} or {breakpoints}
    n_samples = Column(Integer, default=0)
    brier_score = Column(Float)
    auc_roc = Column(Float)
    is_active = Column(Boolean, default=False, index=True)
    fitted_at = Column(DateTime, server_default=func.now())

    __table_args__ = (
        Index("ix_txn_prob_calibrations_active", "scope", "is_active"),
    )


class SectorSignalWeight(Base):
    """
    Sector-specific weight overrides for the 12 probability signals.

    Stored in DB (not just code) so weights can be tuned from the optimizer
    (Phase 4) without code deploys. Each (sector, signal_type, version) is
    unique; bumping version lets us roll out new calibrations safely.
    """

    __tablename__ = "sector_signal_weights"

    id = Column(Integer, primary_key=True, autoincrement=True)
    sector = Column(String(200), nullable=False, index=True)
    signal_type = Column(String(50), nullable=False)
    weight = Column(Float, nullable=False)
    rationale = Column(Text)

    effective_date = Column(Date, server_default=func.current_date())
    version = Column(String(20), nullable=False, default="v1.0")
    created_at = Column(DateTime, server_default=func.now())

    __table_args__ = (
        UniqueConstraint(
            "sector", "signal_type", "version", name="uq_sector_signal_weights_key"
        ),
    )
