"""
Cross-Source Validation Service.

Validates referential integrity and consistency between data sources.
Checks FIPS codes, entity identifiers, geographic coherence, and temporal consistency.
"""

import logging
import time
from datetime import datetime
from typing import Any, Dict, List

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.models import (
    DQCrossSourceValidation,
    DQCrossSourceResult,
    RuleSeverity,
)

logger = logging.getLogger(__name__)

# Match rate threshold for pass/fail
DEFAULT_MATCH_RATE_THRESHOLD = 0.8


# =============================================================================
# Validation type handlers
# =============================================================================

def _table_exists(db: Session, table_name: str) -> bool:
    """Check if a table exists in the public schema."""
    return db.execute(
        text("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_name = :table AND table_schema = 'public'
            )
        """),
        {"table": table_name},
    ).scalar()


def run_fips_validation(db: Session, config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate FIPS code consistency between two sources.

    LEFT JOIN on FIPS codes, count orphans on each side.
    """
    left = config["left"]
    right = config["right"]
    threshold = config.get("threshold", DEFAULT_MATCH_RATE_THRESHOLD)

    left_table = left["table"]
    left_col = left["column"]
    right_table = right["table"]
    right_col = right["column"]

    if not _table_exists(db, left_table) or not _table_exists(db, right_table):
        return {
            "passed": False, "message": f"Table(s) not found: {left_table} or {right_table}",
            "left_count": 0, "right_count": 0, "matched_count": 0,
            "orphan_left": 0, "orphan_right": 0, "match_rate": 0,
        }

    # Count distinct values on each side
    left_count = db.execute(
        text(f'SELECT COUNT(DISTINCT "{left_col}") FROM "{left_table}" WHERE "{left_col}" IS NOT NULL')
    ).scalar() or 0

    right_count = db.execute(
        text(f'SELECT COUNT(DISTINCT "{right_col}") FROM "{right_table}" WHERE "{right_col}" IS NOT NULL')
    ).scalar() or 0

    # Count matches
    matched = db.execute(
        text(f"""
            SELECT COUNT(DISTINCT l."{left_col}")
            FROM "{left_table}" l
            INNER JOIN "{right_table}" r ON l."{left_col}" = r."{right_col}"
            WHERE l."{left_col}" IS NOT NULL
        """)
    ).scalar() or 0

    orphan_left = left_count - matched
    orphan_right = right_count - matched
    total_unique = left_count + right_count - matched
    match_rate = (matched / total_unique) if total_unique > 0 else 1.0

    # Sample orphans from left side
    sample_orphans_rows = db.execute(
        text(f"""
            SELECT DISTINCT l."{left_col}"
            FROM "{left_table}" l
            LEFT JOIN "{right_table}" r ON l."{left_col}" = r."{right_col}"
            WHERE r."{right_col}" IS NULL AND l."{left_col}" IS NOT NULL
            LIMIT 10
        """)
    ).fetchall()
    sample_orphans = [str(r[0]) for r in sample_orphans_rows]

    passed = match_rate >= threshold

    return {
        "passed": passed,
        "left_count": left_count,
        "right_count": right_count,
        "matched_count": matched,
        "orphan_left": orphan_left,
        "orphan_right": orphan_right,
        "match_rate": round(match_rate, 4),
        "sample_orphans": sample_orphans,
        "message": (
            f"FIPS match rate: {match_rate:.1%} "
            f"({matched} matched, {orphan_left} orphan left, {orphan_right} orphan right)"
        ),
    }


def run_identifier_validation(db: Session, config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate identifier coverage between sources (e.g., CIK, ticker).

    Joins via a shared identifier column.
    """
    left = config["left"]
    right = config["right"]
    threshold = config.get("threshold", DEFAULT_MATCH_RATE_THRESHOLD)

    left_table = left["table"]
    left_col = left["column"]
    right_table = right["table"]
    right_col = right["column"]

    if not _table_exists(db, left_table) or not _table_exists(db, right_table):
        return {
            "passed": False, "message": f"Table(s) not found: {left_table} or {right_table}",
            "left_count": 0, "right_count": 0, "matched_count": 0,
            "orphan_left": 0, "orphan_right": 0, "match_rate": 0,
        }

    left_count = db.execute(
        text(f'SELECT COUNT(DISTINCT "{left_col}") FROM "{left_table}" WHERE "{left_col}" IS NOT NULL')
    ).scalar() or 0

    right_count = db.execute(
        text(f'SELECT COUNT(DISTINCT "{right_col}") FROM "{right_table}" WHERE "{right_col}" IS NOT NULL')
    ).scalar() or 0

    matched = db.execute(
        text(f"""
            SELECT COUNT(DISTINCT l."{left_col}")
            FROM "{left_table}" l
            INNER JOIN "{right_table}" r ON l."{left_col}"::text = r."{right_col}"::text
            WHERE l."{left_col}" IS NOT NULL
        """)
    ).scalar() or 0

    orphan_left = left_count - matched
    orphan_right = right_count - matched
    denominator = max(left_count, right_count, 1)
    match_rate = matched / denominator

    sample_orphans_rows = db.execute(
        text(f"""
            SELECT DISTINCT l."{left_col}"::text
            FROM "{left_table}" l
            LEFT JOIN "{right_table}" r ON l."{left_col}"::text = r."{right_col}"::text
            WHERE r."{right_col}" IS NULL AND l."{left_col}" IS NOT NULL
            LIMIT 10
        """)
    ).fetchall()
    sample_orphans = [str(r[0]) for r in sample_orphans_rows]

    passed = match_rate >= threshold

    return {
        "passed": passed,
        "left_count": left_count,
        "right_count": right_count,
        "matched_count": matched,
        "orphan_left": orphan_left,
        "orphan_right": orphan_right,
        "match_rate": round(match_rate, 4),
        "sample_orphans": sample_orphans,
        "message": (
            f"Identifier match rate: {match_rate:.1%} "
            f"({matched} matched of {denominator})"
        ),
    }


def run_geo_coherence_validation(db: Session, config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate geographic coherence between sources.

    Checks that state/FIPS combinations are consistent.
    """
    left = config["left"]
    right = config["right"]
    threshold = config.get("threshold", DEFAULT_MATCH_RATE_THRESHOLD)

    left_table = left["table"]
    left_col = left["column"]
    right_table = right["table"]
    right_col = right["column"]

    if not _table_exists(db, left_table) or not _table_exists(db, right_table):
        return {
            "passed": False, "message": f"Table(s) not found: {left_table} or {right_table}",
            "left_count": 0, "right_count": 0, "matched_count": 0,
            "orphan_left": 0, "orphan_right": 0, "match_rate": 0,
        }

    # Compare state/FIPS values
    left_count = db.execute(
        text(f'SELECT COUNT(DISTINCT "{left_col}") FROM "{left_table}" WHERE "{left_col}" IS NOT NULL')
    ).scalar() or 0

    right_count = db.execute(
        text(f'SELECT COUNT(DISTINCT "{right_col}") FROM "{right_table}" WHERE "{right_col}" IS NOT NULL')
    ).scalar() or 0

    matched = db.execute(
        text(f"""
            SELECT COUNT(DISTINCT l."{left_col}")
            FROM "{left_table}" l
            INNER JOIN "{right_table}" r ON l."{left_col}"::text = r."{right_col}"::text
            WHERE l."{left_col}" IS NOT NULL
        """)
    ).scalar() or 0

    orphan_left = left_count - matched
    orphan_right = right_count - matched
    total_unique = left_count + right_count - matched
    match_rate = (matched / total_unique) if total_unique > 0 else 1.0

    passed = match_rate >= threshold

    return {
        "passed": passed,
        "left_count": left_count,
        "right_count": right_count,
        "matched_count": matched,
        "orphan_left": orphan_left,
        "orphan_right": orphan_right,
        "match_rate": round(match_rate, 4),
        "sample_orphans": [],
        "message": f"Geo coherence match rate: {match_rate:.1%}",
    }


def run_temporal_validation(db: Session, config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate temporal consistency between sources.

    Checks that date ranges or ordering constraints are maintained.
    """
    left = config["left"]
    right = config["right"]

    left_table = left["table"]
    left_col = left["column"]
    right_table = right["table"]
    right_col = right["column"]

    if not _table_exists(db, left_table) or not _table_exists(db, right_table):
        return {
            "passed": False, "message": f"Table(s) not found: {left_table} or {right_table}",
            "left_count": 0, "right_count": 0, "matched_count": 0,
            "orphan_left": 0, "orphan_right": 0, "match_rate": 0,
        }

    # Get date ranges for both
    left_range = db.execute(
        text(f'SELECT MIN("{left_col}"), MAX("{left_col}"), COUNT(*) FROM "{left_table}" WHERE "{left_col}" IS NOT NULL')
    ).fetchone()

    right_range = db.execute(
        text(f'SELECT MIN("{right_col}"), MAX("{right_col}"), COUNT(*) FROM "{right_table}" WHERE "{right_col}" IS NOT NULL')
    ).fetchone()

    left_count = int(left_range[2]) if left_range else 0
    right_count = int(right_range[2]) if right_range else 0

    # Check overlap
    if left_range and right_range and left_range[0] and right_range[0]:
        left_min, left_max = left_range[0], left_range[1]
        right_min, right_max = right_range[0], right_range[1]

        # Calculate overlap ratio
        overlap_start = max(left_min, right_min)
        overlap_end = min(left_max, right_max)

        has_overlap = overlap_start <= overlap_end
        passed = has_overlap

        message = (
            f"Left range: {left_min} to {left_max}, "
            f"Right range: {right_min} to {right_max}, "
            f"Overlap: {'yes' if has_overlap else 'no'}"
        )
    else:
        passed = False
        message = "Could not determine date ranges"

    return {
        "passed": passed,
        "left_count": left_count,
        "right_count": right_count,
        "matched_count": 0,
        "orphan_left": 0,
        "orphan_right": 0,
        "match_rate": 1.0 if passed else 0.0,
        "sample_orphans": [],
        "message": message,
    }


# =============================================================================
# Validation dispatcher
# =============================================================================

VALIDATION_HANDLERS = {
    "fips_consistency": run_fips_validation,
    "identifier_match": run_identifier_validation,
    "geo_coherence": run_geo_coherence_validation,
    "temporal_consistency": run_temporal_validation,
}


def run_validation(
    db: Session, validation: DQCrossSourceValidation
) -> DQCrossSourceResult:
    """Run a single cross-source validation and store the result."""
    start_time = time.time()

    handler = VALIDATION_HANDLERS.get(validation.validation_type)
    if not handler:
        logger.error(f"Unknown validation type: {validation.validation_type}")
        result = DQCrossSourceResult(
            validation_id=validation.id,
            passed=0,
            message=f"Unknown validation type: {validation.validation_type}",
            evaluated_at=datetime.utcnow(),
        )
        db.add(result)
        db.commit()
        return result

    try:
        result_data = handler(db, validation.config)
    except Exception as e:
        logger.error(f"Validation {validation.name} failed: {e}")
        result_data = {
            "passed": False,
            "message": f"Error: {str(e)}",
            "left_count": 0, "right_count": 0, "matched_count": 0,
            "orphan_left": 0, "orphan_right": 0, "match_rate": 0,
        }

    execution_time_ms = int((time.time() - start_time) * 1000)

    result = DQCrossSourceResult(
        validation_id=validation.id,
        passed=1 if result_data.get("passed") else 0,
        left_count=result_data.get("left_count"),
        right_count=result_data.get("right_count"),
        matched_count=result_data.get("matched_count"),
        orphan_left=result_data.get("orphan_left"),
        orphan_right=result_data.get("orphan_right"),
        match_rate=result_data.get("match_rate"),
        sample_orphans=result_data.get("sample_orphans"),
        message=result_data.get("message"),
        execution_time_ms=execution_time_ms,
        evaluated_at=datetime.utcnow(),
    )
    db.add(result)

    # Update validation stats
    validation.times_evaluated = (validation.times_evaluated or 0) + 1
    validation.last_evaluated_at = datetime.utcnow()
    validation.last_pass_rate = result_data.get("match_rate")

    db.commit()

    logger.info(
        f"Validation '{validation.name}': "
        f"{'PASSED' if result.passed else 'FAILED'} "
        f"(match_rate={result.match_rate}, {execution_time_ms}ms)"
    )
    return result


def run_all_validations(db: Session) -> List[DQCrossSourceResult]:
    """Run all enabled cross-source validations."""
    validations = (
        db.query(DQCrossSourceValidation)
        .filter(DQCrossSourceValidation.is_enabled == 1)
        .all()
    )

    results = []
    for v in validations:
        try:
            result = run_validation(db, v)
            results.append(result)
        except Exception as e:
            logger.error(f"Failed to run validation {v.name}: {e}")
            continue

    logger.info(
        f"Completed {len(results)}/{len(validations)} cross-source validations"
    )
    return results


# =============================================================================
# Default validations
# =============================================================================

def create_default_validations(db: Session) -> List[DQCrossSourceValidation]:
    """Seed built-in cross-source validation rules."""
    defaults = [
        {
            "name": "BLS FIPS <-> Census FIPS consistency",
            "description": "Verify BLS employment FIPS codes match Census ACS FIPS codes",
            "validation_type": "fips_consistency",
            "config": {
                "left": {"table": "bls_employment", "column": "geo_fips", "source": "bls"},
                "right": {"table": "census_acs5_data", "column": "geo_id", "source": "census"},
                "threshold": 0.7,
            },
            "severity": RuleSeverity.WARNING,
        },
        {
            "name": "SEC CIK <-> canonical_entities coverage",
            "description": "Verify SEC filing CIK numbers appear in canonical entities",
            "validation_type": "identifier_match",
            "config": {
                "left": {"table": "sec_filings", "column": "cik", "source": "sec"},
                "right": {"table": "canonical_entities", "column": "cik", "source": "canonical"},
                "threshold": 0.5,
            },
            "severity": RuleSeverity.INFO,
        },
        {
            "name": "PE portfolio <-> industrial_companies name match",
            "description": "Verify PE portfolio companies are linked to industrial companies",
            "validation_type": "identifier_match",
            "config": {
                "left": {"table": "pe_portfolio_companies", "column": "company_name", "source": "pe"},
                "right": {"table": "industrial_companies", "column": "name", "source": "industrial"},
                "threshold": 0.3,
            },
            "severity": RuleSeverity.INFO,
        },
        {
            "name": "Site intel state/FIPS coherence",
            "description": "Verify site intelligence location FIPS codes match state codes",
            "validation_type": "geo_coherence",
            "config": {
                "left": {"table": "site_intel_locations", "column": "state_fips", "source": "site_intel"},
                "right": {"table": "census_acs5_data", "column": "geo_id", "source": "census"},
                "threshold": 0.6,
            },
            "severity": RuleSeverity.WARNING,
        },
    ]

    created = []
    for defn in defaults:
        existing = (
            db.query(DQCrossSourceValidation)
            .filter(DQCrossSourceValidation.name == defn["name"])
            .first()
        )
        if existing:
            logger.debug(f"Validation '{defn['name']}' already exists, skipping")
            continue

        validation = DQCrossSourceValidation(
            name=defn["name"],
            description=defn["description"],
            validation_type=defn["validation_type"],
            config=defn["config"],
            severity=defn["severity"],
        )
        db.add(validation)
        created.append(validation)

    if created:
        db.commit()
        logger.info(f"Created {len(created)} default cross-source validations")

    return created


# =============================================================================
# Scheduled runner (for scheduler integration)
# =============================================================================

def scheduled_cross_source_validation():
    """Entry point for scheduled cross-source validation runs."""
    from app.core.database import get_session_factory

    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        results = run_all_validations(db)
        passed = sum(1 for r in results if r.passed)
        logger.info(
            f"Scheduled cross-source validation: {passed}/{len(results)} passed"
        )
    except Exception as e:
        logger.error(f"Scheduled cross-source validation failed: {e}")
    finally:
        db.close()
