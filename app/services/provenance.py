"""
Data provenance helpers.

Provides functions to determine whether data behind a scorer factor
is real or synthetic, by joining back to ingestion_jobs.data_origin.
"""

from typing import Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.source_registry import SOURCE_REGISTRY


def get_origin_for_source(source_key: str) -> str:
    """Return 'real' or 'synthetic' from the source registry.

    Falls back to 'unknown' if the source is not registered.
    """
    ctx = SOURCE_REGISTRY.get(source_key)
    if ctx is None:
        return "unknown"
    return ctx.origin


def get_origin_from_jobs(db: Session, source_key: str) -> str:
    """Check ingestion_jobs for the data_origin of the most recent job
    for a given source.

    Returns 'real', 'synthetic', 'mixed', or 'unknown'.
    """
    result = db.execute(
        text(
            "SELECT DISTINCT data_origin FROM ingestion_jobs "
            "WHERE source = :source AND status = 'success' "
            "ORDER BY data_origin"
        ),
        {"source": source_key},
    ).fetchall()

    if not result:
        return "unknown"

    origins = {row[0] for row in result}
    if origins == {"real"}:
        return "real"
    if origins == {"synthetic"}:
        return "synthetic"
    return "mixed"


def build_scorer_provenance(
    factor_origins: Dict[str, str],
) -> Dict:
    """Build a provenance summary from a {factor_name: origin} mapping.

    Args:
        factor_origins: e.g. {"safety_risk": "real", "growth_momentum": "synthetic"}

    Returns:
        Dict with real_factors, synthetic_factors, total_factors,
        real_pct, and per-factor detail.
    """
    total = len(factor_origins)
    real_count = sum(1 for v in factor_origins.values() if v == "real")
    synthetic_count = sum(1 for v in factor_origins.values() if v == "synthetic")

    return {
        "real_factors": real_count,
        "synthetic_factors": synthetic_count,
        "total_factors": total,
        "real_pct": round(real_count / total * 100) if total > 0 else 0,
        "detail": factor_origins,
    }


# ---------------------------------------------------------------------------
# Source-to-origin mapping for scorer factors
# ---------------------------------------------------------------------------

# Maps the source keys used by each scorer factor to determine provenance.
# Scorers call get_origin_for_source() with these keys.
FACTOR_SOURCE_MAP: Dict[str, str] = {
    # Company Diligence factors
    "revenue_concentration": "usaspending",
    "environmental_risk": "epa_echo",
    "safety_risk": "osha",
    "legal_exposure": "courtlistener",
    "innovation_capacity": "uspto",
    "growth_momentum": "job_postings",
    # Exec Signal factors
    "management_buildup": "job_postings",
    "senior_hiring": "job_postings",
    "hiring_velocity": "job_postings",
    # Healthcare Practice factors
    "market_attractiveness": "irs_soi",
    "clinical_credibility": "nppes",
    "competitive_position": "yelp",
    "revenue_potential": "yelp",
    "multi_unit_potential": "yelp",
}


def get_provenance_for_factors(
    factor_names: List[str],
    source_overrides: Optional[Dict[str, str]] = None,
) -> Dict:
    """Convenience: build provenance for a list of factor names.

    Uses FACTOR_SOURCE_MAP to look up origin, with optional overrides.
    """
    overrides = source_overrides or {}
    factor_origins = {}
    for name in factor_names:
        source_key = overrides.get(name, FACTOR_SOURCE_MAP.get(name))
        if source_key:
            factor_origins[name] = get_origin_for_source(source_key)
        else:
            factor_origins[name] = "unknown"
    return build_scorer_provenance(factor_origins)
