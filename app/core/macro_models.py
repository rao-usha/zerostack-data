"""
Macro Causal Graph Models — PLAN_035

Tables:
  macro_nodes              — nodes in the causal graph (FRED series, BLS series, sectors, companies)
  causal_edges             — directed causal relationships between nodes
  cascade_scenarios        — named what-if scenarios
  cascade_results          — computed cascade impacts per scenario
  company_macro_linkages   — how portfolio companies link to macro nodes
"""

from sqlalchemy import (
    Column,
    Integer,
    String,
    Text,
    Date,
    DateTime,
    Boolean,
    Float,
    JSON,
    ForeignKey,
    Index,
    UniqueConstraint,
)
from sqlalchemy.sql import func
from app.core.models import Base


# =============================================================================
# MACRO CAUSAL GRAPH TABLES (5)
# =============================================================================


class MacroNode(Base):
    """
    A node in the macro causal graph.

    Each node represents a trackable economic variable — a FRED/BLS data
    series, a sector aggregate, or a specific company metric.  Nodes are
    connected via CausalEdge records to form a directed causal graph that
    the cascade engine traverses during what-if simulation.
    """

    __tablename__ = "macro_nodes"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Classification
    node_type = Column(String(50), nullable=False)  # 'fred_series','bls_series','sector','company','custom'
    name = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)

    # Measurement
    unit = Column(String(100), nullable=True)        # 'percent','index','thousands of units','USD billions'
    series_id = Column(String(50), nullable=True)    # FRED/BLS series ID e.g. "DFF", "WPU0613"
    ticker = Column(String(20), nullable=True)       # for company nodes e.g. "SHW"
    frequency = Column(String(20), nullable=True)    # 'daily','weekly','monthly','quarterly','annual'

    # Current value cache (refreshed by data collection)
    current_value = Column(Float, nullable=True)
    current_value_date = Column(Date, nullable=True)

    # Indicator type flags
    is_leading_indicator = Column(Boolean, default=False, nullable=False)
    is_coincident = Column(Boolean, default=False, nullable=False)
    is_lagging = Column(Boolean, default=False, nullable=False)

    # Sector classification
    sector_tag = Column(String(50), nullable=True)   # 'housing','consumer','credit','energy','industrial','labor'
    display_order = Column(Integer, default=0, nullable=False)  # for UI ordering within sector

    # Timestamps
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    __table_args__ = (
        Index("ix_macro_nodes_series_id", "series_id"),
        Index("ix_macro_nodes_ticker", "ticker"),
        Index("ix_macro_nodes_sector_tag", "sector_tag"),
        Index("ix_macro_nodes_node_type", "node_type"),
    )

    def __repr__(self):
        return f"<MacroNode id={self.id} name={self.name!r} type={self.node_type}>"


class CausalEdge(Base):
    """
    A directed causal relationship between two MacroNodes.

    Encodes the economic relationship: a 1% change in the source node
    causes an `elasticity`% change in the target node, with the given
    directional sign, after `typical_lag_months`.

    The cascade engine multiplies elasticities along paths and applies a
    per-hop damping factor to represent signal attenuation.
    """

    __tablename__ = "causal_edges"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Graph structure
    source_node_id = Column(Integer, ForeignKey("macro_nodes.id", ondelete="CASCADE"), nullable=False)
    target_node_id = Column(Integer, ForeignKey("macro_nodes.id", ondelete="CASCADE"), nullable=False)

    # Causal parameters
    relationship_direction = Column(String(20), nullable=False)  # 'positive' or 'negative'
    elasticity = Column(Float, nullable=False)          # signed: 1% Δ source → elasticity% Δ target
    typical_lag_months = Column(Integer, default=1, nullable=False)
    lag_min_months = Column(Integer, default=0, nullable=False)
    lag_max_months = Column(Integer, default=3, nullable=False)
    confidence = Column(Float, default=0.7, nullable=False)       # 0-1

    # Evidence / metadata
    mechanism_description = Column(Text, nullable=True)           # plain English explanation
    empirical_correlation = Column(Float, nullable=True)          # measured historical correlation
    data_source_refs = Column(JSON, nullable=True)                # list of study/paper references

    is_active = Column(Boolean, default=True, nullable=False)

    # Timestamps
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    __table_args__ = (
        UniqueConstraint("source_node_id", "target_node_id", name="uq_causal_edge_src_tgt"),
        Index("ix_causal_edges_source", "source_node_id"),
        Index("ix_causal_edges_target", "target_node_id"),
    )

    def __repr__(self):
        return (
            f"<CausalEdge id={self.id} "
            f"src={self.source_node_id} → tgt={self.target_node_id} "
            f"elasticity={self.elasticity}>"
        )


class CascadeScenario(Base):
    """
    A named what-if scenario for cascade simulation.

    Defines a shock to a single input node (e.g. "Federal Funds Rate +200 bps")
    and the time horizon over which downstream impacts are evaluated.
    """

    __tablename__ = "cascade_scenarios"

    id = Column(Integer, primary_key=True, autoincrement=True)

    name = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)

    # Shock definition
    input_node_id = Column(Integer, ForeignKey("macro_nodes.id"), nullable=False)
    input_change_pct = Column(Float, nullable=False)    # +1.0 = +1%, -20.0 = -20%
    horizon_months = Column(Integer, default=24, nullable=False)
    as_of_date = Column(Date, nullable=True)            # date context for the scenario

    # Timestamp
    created_at = Column(DateTime, server_default=func.now(), nullable=False)

    __table_args__ = (
        Index("ix_cascade_scenarios_node", "input_node_id"),
    )

    def __repr__(self):
        return (
            f"<CascadeScenario id={self.id} name={self.name!r} "
            f"change={self.input_change_pct:+.1f}%>"
        )


class CascadeResult(Base):
    """
    Computed cascade impact on a single node for a given scenario.

    Generated by MacroCascadeEngine.simulate() and persisted by
    MacroCascadeEngine.persist_results().  One record per (scenario, node).
    """

    __tablename__ = "cascade_results"

    id = Column(Integer, primary_key=True, autoincrement=True)

    scenario_id = Column(Integer, ForeignKey("cascade_scenarios.id", ondelete="CASCADE"), nullable=False)
    node_id = Column(Integer, ForeignKey("macro_nodes.id", ondelete="CASCADE"), nullable=False)

    estimated_impact_pct = Column(Float, nullable=False)        # % change vs baseline
    peak_impact_month = Column(Integer, nullable=True)          # month 1-24 when largest impact occurs
    confidence = Column(Float, nullable=True)                   # product of edge confidences along path
    impact_path = Column(JSON, nullable=True)                   # list of node names in causal chain
    distance_from_input = Column(Integer, default=0, nullable=False)  # hop count from input node

    computed_at = Column(DateTime, server_default=func.now(), nullable=False)

    __table_args__ = (
        UniqueConstraint("scenario_id", "node_id", name="uq_cascade_result_scenario_node"),
        Index("ix_cascade_results_scenario", "scenario_id"),
        Index("ix_cascade_results_node", "node_id"),
    )

    def __repr__(self):
        return (
            f"<CascadeResult id={self.id} "
            f"scenario={self.scenario_id} node={self.node_id} "
            f"impact={self.estimated_impact_pct:+.2f}%>"
        )


class CompanyMacroLinkage(Base):
    """
    Maps a portfolio company to the macro nodes it is sensitive to.

    Populated by MacroSensitivityAgent from SEC 10-K Risk Factor sections.
    Enables the dashboard to show which macro shocks affect which holdings.
    """

    __tablename__ = "company_macro_linkages"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Company identifier — public companies use ticker; private use company_name
    ticker = Column(String(20), nullable=True)
    company_name = Column(String(255), nullable=True)

    node_id = Column(Integer, ForeignKey("macro_nodes.id", ondelete="CASCADE"), nullable=False)

    # Linkage characterization
    linkage_type = Column(String(50), nullable=False)            # 'revenue_driver','cost_driver','demand_driver','competitor_proxy','risk_factor'
    linkage_strength = Column(Float, default=0.5, nullable=False)  # 0-1
    direction = Column(String(20), nullable=False)               # 'positive' or 'negative'

    # Evidence provenance
    evidence_source = Column(String(50), nullable=False)         # 'sec_10k_risk_factors','empirical','manual'
    evidence_text = Column(Text, nullable=True)

    created_at = Column(DateTime, server_default=func.now(), nullable=False)

    __table_args__ = (
        Index("ix_company_macro_linkages_ticker", "ticker"),
        Index("ix_company_macro_linkages_node", "node_id"),
    )

    def __repr__(self):
        return (
            f"<CompanyMacroLinkage id={self.id} "
            f"ticker={self.ticker!r} node={self.node_id} "
            f"direction={self.direction}>"
        )


class MacroChatMessage(Base):
    """Conversation history for LLM-powered cascade chat (PLAN_058 Phase 3)."""

    __tablename__ = "macro_chat_messages"

    id = Column(Integer, primary_key=True, autoincrement=True)
    conversation_id = Column(String(50), nullable=False, index=True)
    role = Column(String(20), nullable=False)  # 'user', 'assistant', 'system', 'tool'
    content = Column(Text, nullable=False)
    tool_calls = Column(Text, nullable=True)   # JSON array of tool calls made
    tool_results = Column(Text, nullable=True)  # JSON array of tool results
    created_at = Column(DateTime, server_default=func.now())

    __table_args__ = (
        Index("ix_chat_conversation_time", "conversation_id", "created_at"),
    )
