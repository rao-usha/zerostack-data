"""
Analytics view for LP quarterly strategy data.

This module provides SQL view definitions and helper functions for
querying LP strategy data in a "gold layer" analytics-ready format.

The main view: lp_strategy_quarterly_view

Provides one row per (LP, program, fiscal_year, fiscal_quarter) with:
- Basic strategy info
- Pivoted asset class allocations (target and current)
- Key forward-looking metrics (3-year commitment plans)
- Thematic flags
"""
import logging
from typing import Dict, Any, Optional, List
from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


# =============================================================================
# SQL VIEW DEFINITION
# =============================================================================

CREATE_QUARTERLY_VIEW_SQL = """
-- Drop view if exists
DROP VIEW IF EXISTS lp_strategy_quarterly_view;

-- Create analytics view for LP quarterly strategies
CREATE VIEW lp_strategy_quarterly_view AS
SELECT
    -- Core strategy identifiers
    s.id AS strategy_id,
    s.lp_id,
    f.name AS lp_name,
    f.formal_name AS lp_formal_name,
    s.program,
    s.fiscal_year,
    s.fiscal_quarter,
    s.strategy_date,
    s.primary_document_id,
    
    -- Strategy summary fields
    s.summary_text,
    s.risk_positioning,
    s.liquidity_profile,
    s.tilt_description,
    
    -- Public Equity allocations
    MAX(CASE WHEN a.asset_class = 'public_equity' THEN a.target_weight_pct END) AS target_public_equity_pct,
    MAX(CASE WHEN a.asset_class = 'public_equity' THEN a.current_weight_pct END) AS current_public_equity_pct,
    
    -- Private Equity allocations
    MAX(CASE WHEN a.asset_class = 'private_equity' THEN a.target_weight_pct END) AS target_private_equity_pct,
    MAX(CASE WHEN a.asset_class = 'private_equity' THEN a.current_weight_pct END) AS current_private_equity_pct,
    
    -- Real Estate allocations
    MAX(CASE WHEN a.asset_class = 'real_estate' THEN a.target_weight_pct END) AS target_real_estate_pct,
    MAX(CASE WHEN a.asset_class = 'real_estate' THEN a.current_weight_pct END) AS current_real_estate_pct,
    
    -- Fixed Income allocations
    MAX(CASE WHEN a.asset_class = 'fixed_income' THEN a.target_weight_pct END) AS target_fixed_income_pct,
    MAX(CASE WHEN a.asset_class = 'fixed_income' THEN a.current_weight_pct END) AS current_fixed_income_pct,
    
    -- Infrastructure allocations
    MAX(CASE WHEN a.asset_class = 'infrastructure' THEN a.target_weight_pct END) AS target_infrastructure_pct,
    MAX(CASE WHEN a.asset_class = 'infrastructure' THEN a.current_weight_pct END) AS current_infrastructure_pct,
    
    -- Cash allocations
    MAX(CASE WHEN a.asset_class = 'cash' THEN a.target_weight_pct END) AS target_cash_pct,
    MAX(CASE WHEN a.asset_class = 'cash' THEN a.current_weight_pct END) AS current_cash_pct,
    
    -- Hedge Funds allocations
    MAX(CASE WHEN a.asset_class = 'hedge_funds' THEN a.target_weight_pct END) AS target_hedge_funds_pct,
    MAX(CASE WHEN a.asset_class = 'hedge_funds' THEN a.current_weight_pct END) AS current_hedge_funds_pct,
    
    -- Forward-looking metrics (3-year horizon)
    MAX(CASE WHEN p.asset_class = 'private_equity' AND p.projection_horizon = '3_year' 
        THEN p.commitment_plan_amount END) AS pe_commitment_plan_3y_amount,
    MAX(CASE WHEN p.asset_class = 'infrastructure' AND p.projection_horizon = '3_year' 
        THEN p.commitment_plan_amount END) AS infra_commitment_plan_3y_amount,
    MAX(CASE WHEN p.asset_class = 'real_estate' AND p.projection_horizon = '3_year' 
        THEN p.commitment_plan_amount END) AS re_commitment_plan_3y_amount,
    
    -- Thematic flags
    MAX(CASE WHEN t.theme = 'ai' THEN 1 ELSE 0 END) AS theme_ai,
    MAX(CASE WHEN t.theme = 'energy_transition' THEN 1 ELSE 0 END) AS theme_energy_transition,
    MAX(CASE WHEN t.theme = 'climate_resilience' THEN 1 ELSE 0 END) AS theme_climate_resilience,
    MAX(CASE WHEN t.theme = 'reshoring' THEN 1 ELSE 0 END) AS theme_reshoring,
    MAX(CASE WHEN t.theme = 'healthcare' THEN 1 ELSE 0 END) AS theme_healthcare,
    MAX(CASE WHEN t.theme = 'technology' THEN 1 ELSE 0 END) AS theme_technology,
    
    -- Metadata
    s.created_at,
    f.lp_type,
    f.jurisdiction

FROM lp_strategy_snapshot s
INNER JOIN lp_fund f ON s.lp_id = f.id
LEFT JOIN lp_asset_class_target_allocation a ON s.id = a.strategy_id
LEFT JOIN lp_asset_class_projection p ON s.id = p.strategy_id
LEFT JOIN lp_strategy_thematic_tag t ON s.id = t.strategy_id

GROUP BY
    s.id,
    s.lp_id,
    f.name,
    f.formal_name,
    s.program,
    s.fiscal_year,
    s.fiscal_quarter,
    s.strategy_date,
    s.primary_document_id,
    s.summary_text,
    s.risk_positioning,
    s.liquidity_profile,
    s.tilt_description,
    s.created_at,
    f.lp_type,
    f.jurisdiction;
"""


# =============================================================================
# VIEW MANAGEMENT FUNCTIONS
# =============================================================================


def create_analytics_view(db: Session) -> None:
    """
    Create the lp_strategy_quarterly_view analytics view.
    
    Idempotent: Drops and recreates the view if it exists.
    
    Args:
        db: Database session
    """
    logger.info("Creating lp_strategy_quarterly_view...")
    
    try:
        db.execute(text(CREATE_QUARTERLY_VIEW_SQL))
        db.commit()
        logger.info("Successfully created lp_strategy_quarterly_view")
    except Exception as e:
        logger.error(f"Error creating analytics view: {e}")
        db.rollback()
        raise


# =============================================================================
# QUERY HELPER FUNCTIONS
# =============================================================================


def query_strategy_by_lp_program_quarter(
    db: Session,
    lp_name: str,
    program: str,
    fiscal_year: int,
    fiscal_quarter: str
) -> Optional[Dict[str, Any]]:
    """
    Query the quarterly strategy view for a specific LP/program/quarter.
    
    Example:
        query_strategy_by_lp_program_quarter(
            db, "CalPERS", "private_equity", 2025, "Q3"
        )
    
    Args:
        db: Database session
        lp_name: LP fund name (e.g., "CalPERS")
        program: Program name (e.g., "private_equity")
        fiscal_year: Fiscal year (e.g., 2025)
        fiscal_quarter: Fiscal quarter (e.g., "Q3")
        
    Returns:
        Dictionary with strategy data or None if not found
    """
    query_sql = """
    SELECT * FROM lp_strategy_quarterly_view
    WHERE lp_name = :lp_name
      AND program = :program
      AND fiscal_year = :fiscal_year
      AND fiscal_quarter = :fiscal_quarter
    """
    
    result = db.execute(
        text(query_sql),
        {
            "lp_name": lp_name,
            "program": program,
            "fiscal_year": fiscal_year,
            "fiscal_quarter": fiscal_quarter,
        }
    ).fetchone()
    
    if result:
        return dict(result._mapping)
    return None


def query_strategies_by_lp_quarter(
    db: Session,
    lp_name: str,
    fiscal_year: int,
    fiscal_quarter: str
) -> List[Dict[str, Any]]:
    """
    Query all programs for a given LP and quarter.
    
    Example:
        query_strategies_by_lp_quarter(db, "CalPERS", 2025, "Q3")
        # Returns strategies for all programs (total_fund, private_equity, etc.)
    
    Args:
        db: Database session
        lp_name: LP fund name
        fiscal_year: Fiscal year
        fiscal_quarter: Fiscal quarter
        
    Returns:
        List of dictionaries with strategy data
    """
    query_sql = """
    SELECT * FROM lp_strategy_quarterly_view
    WHERE lp_name = :lp_name
      AND fiscal_year = :fiscal_year
      AND fiscal_quarter = :fiscal_quarter
    ORDER BY program
    """
    
    results = db.execute(
        text(query_sql),
        {
            "lp_name": lp_name,
            "fiscal_year": fiscal_year,
            "fiscal_quarter": fiscal_quarter,
        }
    ).fetchall()
    
    return [dict(row._mapping) for row in results]


def query_strategies_by_program_quarter(
    db: Session,
    program: str,
    fiscal_year: int,
    fiscal_quarter: str,
    limit: int = 100
) -> List[Dict[str, Any]]:
    """
    Query all LPs for a given program and quarter.
    
    Example:
        query_strategies_by_program_quarter(db, "private_equity", 2025, "Q3")
        # Returns private equity strategies for all LPs in Q3 2025
    
    Args:
        db: Database session
        program: Program name
        fiscal_year: Fiscal year
        fiscal_quarter: Fiscal quarter
        limit: Maximum results to return
        
    Returns:
        List of dictionaries with strategy data
    """
    query_sql = """
    SELECT * FROM lp_strategy_quarterly_view
    WHERE program = :program
      AND fiscal_year = :fiscal_year
      AND fiscal_quarter = :fiscal_quarter
    ORDER BY lp_name
    LIMIT :limit
    """
    
    results = db.execute(
        text(query_sql),
        {
            "program": program,
            "fiscal_year": fiscal_year,
            "fiscal_quarter": fiscal_quarter,
            "limit": limit,
        }
    ).fetchall()
    
    return [dict(row._mapping) for row in results]


def query_strategies_with_theme(
    db: Session,
    theme: str,
    fiscal_year: Optional[int] = None,
    fiscal_quarter: Optional[str] = None,
    limit: int = 100
) -> List[Dict[str, Any]]:
    """
    Query strategies that have a specific thematic tag.
    
    Example:
        query_strategies_with_theme(db, "ai", fiscal_year=2025)
        # Returns all strategies in 2025 with AI theme
    
    Args:
        db: Database session
        theme: Theme name (e.g., "ai", "energy_transition")
        fiscal_year: Optional fiscal year filter
        fiscal_quarter: Optional fiscal quarter filter
        limit: Maximum results to return
        
    Returns:
        List of dictionaries with strategy data
    """
    # Build dynamic WHERE clause
    where_clauses = [f"theme_{theme} = 1"]
    params = {"limit": limit}
    
    if fiscal_year:
        where_clauses.append("fiscal_year = :fiscal_year")
        params["fiscal_year"] = fiscal_year
    
    if fiscal_quarter:
        where_clauses.append("fiscal_quarter = :fiscal_quarter")
        params["fiscal_quarter"] = fiscal_quarter
    
    where_clause = " AND ".join(where_clauses)
    
    query_sql = f"""
    SELECT * FROM lp_strategy_quarterly_view
    WHERE {where_clause}
    ORDER BY fiscal_year DESC, fiscal_quarter, lp_name
    LIMIT :limit
    """
    
    results = db.execute(text(query_sql), params).fetchall()
    
    return [dict(row._mapping) for row in results]


# =============================================================================
# USAGE DOCUMENTATION
# =============================================================================

"""
USAGE EXAMPLES:

1. Query CalPERS Q3 2025 private equity strategy:

    from app.sources.public_lp_strategies.analytics_view import query_strategy_by_lp_program_quarter
    from app.core.database import get_db
    
    db = next(get_db())
    result = query_strategy_by_lp_program_quarter(
        db, "CalPERS", "private_equity", 2025, "Q3"
    )
    print(result)
    # Output: {
    #     'strategy_id': 1,
    #     'lp_name': 'CalPERS',
    #     'program': 'private_equity',
    #     'fiscal_year': 2025,
    #     'fiscal_quarter': 'Q3',
    #     'target_private_equity_pct': '25.0',
    #     'current_private_equity_pct': '27.5',
    #     'pe_commitment_plan_3y_amount': '5000000000',
    #     'theme_ai': 1,
    #     ...
    # }


2. Query all CalPERS programs for Q3 2025:

    results = query_strategies_by_lp_quarter(db, "CalPERS", 2025, "Q3")
    # Returns list of strategies for total_fund, private_equity, real_estate, etc.


3. Query all LPs' private equity strategies for Q3 2025:

    results = query_strategies_by_program_quarter(db, "private_equity", 2025, "Q3")
    # Returns private equity strategies for CalPERS, CalSTRS, etc.


4. Query all strategies with "AI" theme in 2025:

    results = query_strategies_with_theme(db, "ai", fiscal_year=2025)


5. Direct SQL query:

    from sqlalchemy import text
    
    query = text(\"\"\"
    SELECT lp_name, program, fiscal_quarter,
           target_private_equity_pct, current_private_equity_pct
    FROM lp_strategy_quarterly_view
    WHERE fiscal_year = :year
      AND theme_ai = 1
    ORDER BY lp_name, fiscal_quarter
    \"\"\")
    
    results = db.execute(query, {"year": 2025}).fetchall()
"""


