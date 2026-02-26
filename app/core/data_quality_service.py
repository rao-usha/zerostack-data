"""
Data quality rules engine service.

Provides functionality for defining, evaluating, and reporting on data quality rules.
Supports various rule types including range checks, null checks, freshness, regex patterns.
"""

import logging
import re
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.core.models import (
    DataQualityRule,
    DataQualityResult,
    DataQualityReport,
    RuleType,
    RuleSeverity,
    IngestionJob,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Rule Evaluation Results
# =============================================================================


@dataclass
class RuleEvaluationResult:
    """Result of evaluating a single rule."""

    rule_id: int
    rule_name: str
    passed: bool
    severity: RuleSeverity
    message: str
    actual_value: Optional[str] = None
    expected_value: Optional[str] = None
    rows_checked: Optional[int] = None
    rows_passed: Optional[int] = None
    rows_failed: Optional[int] = None
    sample_failures: Optional[List[Any]] = None
    execution_time_ms: Optional[int] = None


# =============================================================================
# Rule Evaluators
# =============================================================================


def evaluate_range_rule(
    db: Session, rule: DataQualityRule, table_name: str, column_name: str
) -> RuleEvaluationResult:
    """
    Evaluate a range rule (value must be within min/max).

    Parameters:
        min: Minimum allowed value (optional)
        max: Maximum allowed value (optional)
    """
    start_time = time.time()
    params = rule.parameters or {}
    min_val = params.get("min")
    max_val = params.get("max")

    # Build query to check range violations
    conditions = []
    if min_val is not None:
        conditions.append(f'"{column_name}" < :min_val')
    if max_val is not None:
        conditions.append(f'"{column_name}" > :max_val')

    if not conditions:
        return RuleEvaluationResult(
            rule_id=rule.id,
            rule_name=rule.name,
            passed=True,
            severity=rule.severity,
            message="No range constraints specified",
            execution_time_ms=int((time.time() - start_time) * 1000),
        )

    where_clause = " OR ".join(conditions)
    query = text(f"""
        SELECT COUNT(*) as total,
               SUM(CASE WHEN {where_clause} THEN 1 ELSE 0 END) as violations
        FROM "{table_name}"
        WHERE "{column_name}" IS NOT NULL
    """)

    try:
        bind_params = {}
        if min_val is not None:
            bind_params["min_val"] = min_val
        if max_val is not None:
            bind_params["max_val"] = max_val

        result = db.execute(query, bind_params).fetchone()
        total = result[0] or 0
        violations = result[1] or 0

        passed = violations == 0
        expected = f"[{min_val or '-inf'}, {max_val or '+inf'}]"

        # Get sample failures if any
        sample_failures = None
        if violations > 0:
            sample_query = text(f"""
                SELECT "{column_name}" FROM "{table_name}"
                WHERE {where_clause}
                LIMIT 5
            """)
            samples = db.execute(sample_query, bind_params).fetchall()
            sample_failures = [str(s[0]) for s in samples]

        return RuleEvaluationResult(
            rule_id=rule.id,
            rule_name=rule.name,
            passed=passed,
            severity=rule.severity,
            message=f"Found {violations} values outside range {expected}"
            if not passed
            else f"All {total} values within range",
            actual_value=f"{violations} violations",
            expected_value=expected,
            rows_checked=total,
            rows_passed=total - violations,
            rows_failed=violations,
            sample_failures=sample_failures,
            execution_time_ms=int((time.time() - start_time) * 1000),
        )

    except Exception as e:
        logger.error(f"Error evaluating range rule: {e}")
        return RuleEvaluationResult(
            rule_id=rule.id,
            rule_name=rule.name,
            passed=False,
            severity=rule.severity,
            message=f"Error evaluating rule: {str(e)}",
            execution_time_ms=int((time.time() - start_time) * 1000),
        )


def evaluate_not_null_rule(
    db: Session, rule: DataQualityRule, table_name: str, column_name: str
) -> RuleEvaluationResult:
    """
    Evaluate a not-null rule (value must not be null).
    """
    start_time = time.time()

    query = text(f"""
        SELECT COUNT(*) as total,
               SUM(CASE WHEN "{column_name}" IS NULL THEN 1 ELSE 0 END) as nulls
        FROM "{table_name}"
    """)

    try:
        result = db.execute(query).fetchone()
        total = result[0] or 0
        nulls = result[1] or 0

        passed = nulls == 0

        return RuleEvaluationResult(
            rule_id=rule.id,
            rule_name=rule.name,
            passed=passed,
            severity=rule.severity,
            message=f"Found {nulls} null values"
            if not passed
            else f"No null values in {total} rows",
            actual_value=f"{nulls} nulls",
            expected_value="0 nulls",
            rows_checked=total,
            rows_passed=total - nulls,
            rows_failed=nulls,
            execution_time_ms=int((time.time() - start_time) * 1000),
        )

    except Exception as e:
        logger.error(f"Error evaluating not-null rule: {e}")
        return RuleEvaluationResult(
            rule_id=rule.id,
            rule_name=rule.name,
            passed=False,
            severity=rule.severity,
            message=f"Error evaluating rule: {str(e)}",
            execution_time_ms=int((time.time() - start_time) * 1000),
        )


def evaluate_unique_rule(
    db: Session, rule: DataQualityRule, table_name: str, column_name: str
) -> RuleEvaluationResult:
    """
    Evaluate a uniqueness rule (values must be unique).
    """
    start_time = time.time()

    query = text(f"""
        SELECT COUNT(*) as total,
               COUNT(DISTINCT "{column_name}") as unique_count
        FROM "{table_name}"
        WHERE "{column_name}" IS NOT NULL
    """)

    try:
        result = db.execute(query).fetchone()
        total = result[0] or 0
        unique_count = result[1] or 0
        duplicates = total - unique_count

        passed = duplicates == 0

        # Get sample duplicates
        sample_failures = None
        if duplicates > 0:
            dup_query = text(f"""
                SELECT "{column_name}", COUNT(*) as cnt
                FROM "{table_name}"
                WHERE "{column_name}" IS NOT NULL
                GROUP BY "{column_name}"
                HAVING COUNT(*) > 1
                LIMIT 5
            """)
            samples = db.execute(dup_query).fetchall()
            sample_failures = [f"{s[0]} (x{s[1]})" for s in samples]

        return RuleEvaluationResult(
            rule_id=rule.id,
            rule_name=rule.name,
            passed=passed,
            severity=rule.severity,
            message=f"Found {duplicates} duplicate values"
            if not passed
            else f"All {total} values are unique",
            actual_value=f"{unique_count} unique / {total} total",
            expected_value="All unique",
            rows_checked=total,
            rows_passed=unique_count,
            rows_failed=duplicates,
            sample_failures=sample_failures,
            execution_time_ms=int((time.time() - start_time) * 1000),
        )

    except Exception as e:
        logger.error(f"Error evaluating unique rule: {e}")
        return RuleEvaluationResult(
            rule_id=rule.id,
            rule_name=rule.name,
            passed=False,
            severity=rule.severity,
            message=f"Error evaluating rule: {str(e)}",
            execution_time_ms=int((time.time() - start_time) * 1000),
        )


def evaluate_row_count_rule(
    db: Session, rule: DataQualityRule, table_name: str
) -> RuleEvaluationResult:
    """
    Evaluate a row count rule (min/max row count).

    Parameters:
        min: Minimum required rows (optional)
        max: Maximum allowed rows (optional)
    """
    start_time = time.time()
    params = rule.parameters or {}
    min_rows = params.get("min")
    max_rows = params.get("max")

    query = text(f'SELECT COUNT(*) FROM "{table_name}"')

    try:
        result = db.execute(query).fetchone()
        row_count = result[0] or 0

        passed = True
        messages = []

        if min_rows is not None and row_count < min_rows:
            passed = False
            messages.append(f"row count {row_count} < min {min_rows}")

        if max_rows is not None and row_count > max_rows:
            passed = False
            messages.append(f"row count {row_count} > max {max_rows}")

        expected = f"[{min_rows or 0}, {max_rows or 'unlimited'}]"

        return RuleEvaluationResult(
            rule_id=rule.id,
            rule_name=rule.name,
            passed=passed,
            severity=rule.severity,
            message="; ".join(messages)
            if messages
            else f"Row count {row_count} within expected range",
            actual_value=str(row_count),
            expected_value=expected,
            rows_checked=row_count,
            execution_time_ms=int((time.time() - start_time) * 1000),
        )

    except Exception as e:
        logger.error(f"Error evaluating row count rule: {e}")
        return RuleEvaluationResult(
            rule_id=rule.id,
            rule_name=rule.name,
            passed=False,
            severity=rule.severity,
            message=f"Error evaluating rule: {str(e)}",
            execution_time_ms=int((time.time() - start_time) * 1000),
        )


def evaluate_enum_rule(
    db: Session, rule: DataQualityRule, table_name: str, column_name: str
) -> RuleEvaluationResult:
    """
    Evaluate an enum rule (value must be in allowed list).

    Parameters:
        allowed: List of allowed values
    """
    start_time = time.time()
    params = rule.parameters or {}
    allowed = params.get("allowed", [])

    if not allowed:
        return RuleEvaluationResult(
            rule_id=rule.id,
            rule_name=rule.name,
            passed=False,
            severity=rule.severity,
            message="No allowed values specified in rule parameters",
            execution_time_ms=int((time.time() - start_time) * 1000),
        )

    # Build parameterized query
    placeholders = ", ".join([f":val_{i}" for i in range(len(allowed))])
    query = text(f"""
        SELECT COUNT(*) as total,
               SUM(CASE WHEN "{column_name}" NOT IN ({placeholders}) THEN 1 ELSE 0 END) as violations
        FROM "{table_name}"
        WHERE "{column_name}" IS NOT NULL
    """)

    try:
        bind_params = {f"val_{i}": v for i, v in enumerate(allowed)}
        result = db.execute(query, bind_params).fetchone()
        total = result[0] or 0
        violations = result[1] or 0

        passed = violations == 0

        # Get sample invalid values
        sample_failures = None
        if violations > 0:
            sample_query = text(f"""
                SELECT DISTINCT "{column_name}" FROM "{table_name}"
                WHERE "{column_name}" IS NOT NULL
                  AND "{column_name}" NOT IN ({placeholders})
                LIMIT 5
            """)
            samples = db.execute(sample_query, bind_params).fetchall()
            sample_failures = [str(s[0]) for s in samples]

        return RuleEvaluationResult(
            rule_id=rule.id,
            rule_name=rule.name,
            passed=passed,
            severity=rule.severity,
            message=f"Found {violations} values not in allowed list"
            if not passed
            else f"All {total} values valid",
            actual_value=f"{violations} invalid values",
            expected_value=f"One of: {allowed[:5]}{'...' if len(allowed) > 5 else ''}",
            rows_checked=total,
            rows_passed=total - violations,
            rows_failed=violations,
            sample_failures=sample_failures,
            execution_time_ms=int((time.time() - start_time) * 1000),
        )

    except Exception as e:
        logger.error(f"Error evaluating enum rule: {e}")
        return RuleEvaluationResult(
            rule_id=rule.id,
            rule_name=rule.name,
            passed=False,
            severity=rule.severity,
            message=f"Error evaluating rule: {str(e)}",
            execution_time_ms=int((time.time() - start_time) * 1000),
        )


def evaluate_freshness_rule(
    db: Session, rule: DataQualityRule, table_name: str, column_name: str
) -> RuleEvaluationResult:
    """
    Evaluate a freshness rule (data must be recent).

    Parameters:
        max_age_hours: Maximum age in hours
        max_age_days: Maximum age in days (alternative)
    """
    start_time = time.time()
    params = rule.parameters or {}
    max_age_hours = params.get("max_age_hours")
    max_age_days = params.get("max_age_days")

    if max_age_days:
        max_age_hours = max_age_days * 24

    if not max_age_hours:
        return RuleEvaluationResult(
            rule_id=rule.id,
            rule_name=rule.name,
            passed=False,
            severity=rule.severity,
            message="No max_age_hours or max_age_days specified",
            execution_time_ms=int((time.time() - start_time) * 1000),
        )

    cutoff_time = datetime.utcnow() - timedelta(hours=max_age_hours)

    query = text(f"""
        SELECT MAX("{column_name}") as latest
        FROM "{table_name}"
    """)

    try:
        result = db.execute(query).fetchone()
        latest = result[0]

        if latest is None:
            return RuleEvaluationResult(
                rule_id=rule.id,
                rule_name=rule.name,
                passed=False,
                severity=rule.severity,
                message="No data found in table",
                execution_time_ms=int((time.time() - start_time) * 1000),
            )

        # Handle different date formats
        if isinstance(latest, str):
            try:
                latest = datetime.fromisoformat(latest.replace("Z", "+00:00"))
            except ValueError:
                latest = datetime.strptime(latest[:19], "%Y-%m-%d %H:%M:%S")

        passed = latest >= cutoff_time
        age_hours = (datetime.utcnow() - latest).total_seconds() / 3600

        return RuleEvaluationResult(
            rule_id=rule.id,
            rule_name=rule.name,
            passed=passed,
            severity=rule.severity,
            message=f"Data is {age_hours:.1f} hours old"
            + (" (stale)" if not passed else " (fresh)"),
            actual_value=f"{age_hours:.1f} hours",
            expected_value=f"<= {max_age_hours} hours",
            execution_time_ms=int((time.time() - start_time) * 1000),
        )

    except Exception as e:
        logger.error(f"Error evaluating freshness rule: {e}")
        return RuleEvaluationResult(
            rule_id=rule.id,
            rule_name=rule.name,
            passed=False,
            severity=rule.severity,
            message=f"Error evaluating rule: {str(e)}",
            execution_time_ms=int((time.time() - start_time) * 1000),
        )


def evaluate_regex_rule(
    db: Session, rule: DataQualityRule, table_name: str, column_name: str
) -> RuleEvaluationResult:
    """
    Evaluate a regex rule (value must match pattern).

    Parameters:
        pattern: Regular expression pattern
    """
    start_time = time.time()
    params = rule.parameters or {}
    pattern = params.get("pattern")

    if not pattern:
        return RuleEvaluationResult(
            rule_id=rule.id,
            rule_name=rule.name,
            passed=False,
            severity=rule.severity,
            message="No regex pattern specified",
            execution_time_ms=int((time.time() - start_time) * 1000),
        )

    # Fetch values and check with Python regex (more portable than DB-specific regex)
    query = text(f"""
        SELECT "{column_name}" FROM "{table_name}"
        WHERE "{column_name}" IS NOT NULL
    """)

    try:
        compiled_pattern = re.compile(pattern)
        result = db.execute(query).fetchall()

        total = len(result)
        violations = 0
        sample_failures = []

        for row in result:
            value = str(row[0])
            if not compiled_pattern.match(value):
                violations += 1
                if len(sample_failures) < 5:
                    sample_failures.append(value)

        passed = violations == 0

        return RuleEvaluationResult(
            rule_id=rule.id,
            rule_name=rule.name,
            passed=passed,
            severity=rule.severity,
            message=f"Found {violations} values not matching pattern"
            if not passed
            else f"All {total} values match pattern",
            actual_value=f"{violations} mismatches",
            expected_value=f"Pattern: {pattern}",
            rows_checked=total,
            rows_passed=total - violations,
            rows_failed=violations,
            sample_failures=sample_failures if sample_failures else None,
            execution_time_ms=int((time.time() - start_time) * 1000),
        )

    except re.error as e:
        return RuleEvaluationResult(
            rule_id=rule.id,
            rule_name=rule.name,
            passed=False,
            severity=rule.severity,
            message=f"Invalid regex pattern: {str(e)}",
            execution_time_ms=int((time.time() - start_time) * 1000),
        )
    except Exception as e:
        logger.error(f"Error evaluating regex rule: {e}")
        return RuleEvaluationResult(
            rule_id=rule.id,
            rule_name=rule.name,
            passed=False,
            severity=rule.severity,
            message=f"Error evaluating rule: {str(e)}",
            execution_time_ms=int((time.time() - start_time) * 1000),
        )


# DDL/DML keywords forbidden in custom SQL rules
_FORBIDDEN_SQL_KEYWORDS = re.compile(
    r"\b(CREATE|DROP|ALTER|TRUNCATE|INSERT|UPDATE|DELETE|GRANT|REVOKE|EXEC|EXECUTE)\b",
    re.IGNORECASE,
)


def evaluate_custom_sql_rule(
    db: Session, rule: DataQualityRule, table_name: str
) -> RuleEvaluationResult:
    """
    Evaluate a custom SQL rule.

    The user provides SQL via parameters.condition with a {table} placeholder.
    SQL must return a single integer (violation count); 0 = pass.

    Parameters:
        condition: SQL query with {table} placeholder
        timeout_seconds: Max execution time (default 30, max 60)
    """
    start_time = time.time()
    params = rule.parameters or {}
    condition = params.get("condition", "")
    timeout_seconds = min(params.get("timeout_seconds", 30), 60)

    if not condition:
        return RuleEvaluationResult(
            rule_id=rule.id,
            rule_name=rule.name,
            passed=False,
            severity=rule.severity,
            message="No SQL condition specified in parameters.condition",
            execution_time_ms=int((time.time() - start_time) * 1000),
        )

    # Safety: reject DDL/DML keywords
    if _FORBIDDEN_SQL_KEYWORDS.search(condition):
        return RuleEvaluationResult(
            rule_id=rule.id,
            rule_name=rule.name,
            passed=False,
            severity=rule.severity,
            message="Rejected: SQL contains forbidden DDL/DML keywords",
            execution_time_ms=int((time.time() - start_time) * 1000),
        )

    # Substitute {table} placeholder
    sql = condition.replace("{table}", f'"{table_name}"')

    try:
        # Set statement timeout and read-only transaction
        db.execute(
            text(f"SET LOCAL statement_timeout = '{timeout_seconds * 1000}'")
        )
        result = db.execute(text(sql)).fetchone()

        if result is None or len(result) == 0:
            db.rollback()
            return RuleEvaluationResult(
                rule_id=rule.id,
                rule_name=rule.name,
                passed=False,
                severity=rule.severity,
                message="Custom SQL returned no results (expected single integer)",
                execution_time_ms=int((time.time() - start_time) * 1000),
            )

        violation_count = int(result[0])
        passed = violation_count == 0

        return RuleEvaluationResult(
            rule_id=rule.id,
            rule_name=rule.name,
            passed=passed,
            severity=rule.severity,
            message=f"Custom SQL found {violation_count} violations"
            if not passed
            else "Custom SQL check passed (0 violations)",
            actual_value=str(violation_count),
            expected_value="0",
            rows_failed=violation_count,
            execution_time_ms=int((time.time() - start_time) * 1000),
        )

    except Exception as e:
        db.rollback()
        logger.error(f"Error evaluating custom SQL rule: {e}")
        return RuleEvaluationResult(
            rule_id=rule.id,
            rule_name=rule.name,
            passed=False,
            severity=rule.severity,
            message=f"Error evaluating custom SQL: {str(e)}",
            execution_time_ms=int((time.time() - start_time) * 1000),
        )


def evaluate_comparison_rule(
    db: Session, rule: DataQualityRule, table_name: str, column_name: str
) -> RuleEvaluationResult:
    """
    Evaluate a comparison rule (compare two columns).

    Parameters:
        operator: One of <, >, <=, >=, =, !=
        compare_column: Column to compare against
    """
    start_time = time.time()
    params = rule.parameters or {}
    operator = params.get("operator")
    compare_column = params.get("compare_column")

    valid_operators = {"<", ">", "<=", ">=", "=", "!="}
    if not operator or operator not in valid_operators:
        return RuleEvaluationResult(
            rule_id=rule.id,
            rule_name=rule.name,
            passed=False,
            severity=rule.severity,
            message=f"Invalid or missing operator. Must be one of: {valid_operators}",
            execution_time_ms=int((time.time() - start_time) * 1000),
        )

    if not compare_column:
        return RuleEvaluationResult(
            rule_id=rule.id,
            rule_name=rule.name,
            passed=False,
            severity=rule.severity,
            message="Missing compare_column parameter",
            execution_time_ms=int((time.time() - start_time) * 1000),
        )

    # Count rows where the comparison is NOT true (both columns non-null)
    query = text(f"""
        SELECT COUNT(*) as total,
               SUM(CASE WHEN NOT ("{column_name}" {operator} "{compare_column}") THEN 1 ELSE 0 END) as violations
        FROM "{table_name}"
        WHERE "{column_name}" IS NOT NULL AND "{compare_column}" IS NOT NULL
    """)

    try:
        result = db.execute(query).fetchone()
        total = result[0] or 0
        violations = result[1] or 0

        passed = violations == 0

        # Get sample failures
        sample_failures = None
        if violations > 0:
            sample_query = text(f"""
                SELECT "{column_name}", "{compare_column}"
                FROM "{table_name}"
                WHERE "{column_name}" IS NOT NULL AND "{compare_column}" IS NOT NULL
                  AND NOT ("{column_name}" {operator} "{compare_column}")
                LIMIT 5
            """)
            samples = db.execute(sample_query).fetchall()
            sample_failures = [
                f"{column_name}={s[0]}, {compare_column}={s[1]}" for s in samples
            ]

        return RuleEvaluationResult(
            rule_id=rule.id,
            rule_name=rule.name,
            passed=passed,
            severity=rule.severity,
            message=f"Found {violations} rows where {column_name} {operator} {compare_column} is false"
            if not passed
            else f"All {total} rows satisfy {column_name} {operator} {compare_column}",
            actual_value=f"{violations} violations",
            expected_value=f"{column_name} {operator} {compare_column}",
            rows_checked=total,
            rows_passed=total - violations,
            rows_failed=violations,
            sample_failures=sample_failures,
            execution_time_ms=int((time.time() - start_time) * 1000),
        )

    except Exception as e:
        logger.error(f"Error evaluating comparison rule: {e}")
        return RuleEvaluationResult(
            rule_id=rule.id,
            rule_name=rule.name,
            passed=False,
            severity=rule.severity,
            message=f"Error evaluating rule: {str(e)}",
            execution_time_ms=int((time.time() - start_time) * 1000),
        )


# =============================================================================
# Rule Matching
# =============================================================================


def get_matching_rules(
    db: Session,
    source: str,
    dataset_name: Optional[str] = None,
    enabled_only: bool = True,
) -> List[DataQualityRule]:
    """
    Get rules that apply to a source/dataset.

    Args:
        db: Database session
        source: Data source name
        dataset_name: Optional dataset/table name
        enabled_only: Only return enabled rules

    Returns:
        List of matching rules ordered by priority
    """
    query = db.query(DataQualityRule)

    if enabled_only:
        query = query.filter(DataQualityRule.is_enabled == 1)

    # Match by source (null = all sources)
    query = query.filter(
        (DataQualityRule.source == source) | (DataQualityRule.source.is_(None))
    )

    rules = query.order_by(DataQualityRule.priority).all()

    # Filter by dataset pattern if specified
    if dataset_name:
        matched_rules = []
        for rule in rules:
            if rule.dataset_pattern:
                try:
                    if re.match(rule.dataset_pattern, dataset_name):
                        matched_rules.append(rule)
                except re.error:
                    logger.warning(
                        f"Invalid dataset pattern in rule {rule.id}: {rule.dataset_pattern}"
                    )
            else:
                # No pattern = matches all datasets
                matched_rules.append(rule)
        return matched_rules

    return rules


# =============================================================================
# Rule Evaluation Engine
# =============================================================================


def evaluate_rule(
    db: Session, rule: DataQualityRule, table_name: str
) -> RuleEvaluationResult:
    """
    Evaluate a single rule against a table.

    Args:
        db: Database session
        rule: The rule to evaluate
        table_name: Name of the table to check

    Returns:
        RuleEvaluationResult with evaluation outcome
    """
    column_name = rule.column_name

    # Dispatch to appropriate evaluator
    if rule.rule_type == RuleType.RANGE:
        if not column_name:
            return RuleEvaluationResult(
                rule_id=rule.id,
                rule_name=rule.name,
                passed=False,
                severity=rule.severity,
                message="Range rule requires column_name",
            )
        return evaluate_range_rule(db, rule, table_name, column_name)

    elif rule.rule_type == RuleType.NOT_NULL:
        if not column_name:
            return RuleEvaluationResult(
                rule_id=rule.id,
                rule_name=rule.name,
                passed=False,
                severity=rule.severity,
                message="Not-null rule requires column_name",
            )
        return evaluate_not_null_rule(db, rule, table_name, column_name)

    elif rule.rule_type == RuleType.UNIQUE:
        if not column_name:
            return RuleEvaluationResult(
                rule_id=rule.id,
                rule_name=rule.name,
                passed=False,
                severity=rule.severity,
                message="Unique rule requires column_name",
            )
        return evaluate_unique_rule(db, rule, table_name, column_name)

    elif rule.rule_type == RuleType.ROW_COUNT:
        return evaluate_row_count_rule(db, rule, table_name)

    elif rule.rule_type == RuleType.ENUM:
        if not column_name:
            return RuleEvaluationResult(
                rule_id=rule.id,
                rule_name=rule.name,
                passed=False,
                severity=rule.severity,
                message="Enum rule requires column_name",
            )
        return evaluate_enum_rule(db, rule, table_name, column_name)

    elif rule.rule_type == RuleType.FRESHNESS:
        if not column_name:
            return RuleEvaluationResult(
                rule_id=rule.id,
                rule_name=rule.name,
                passed=False,
                severity=rule.severity,
                message="Freshness rule requires column_name (date/timestamp column)",
            )
        return evaluate_freshness_rule(db, rule, table_name, column_name)

    elif rule.rule_type == RuleType.REGEX:
        if not column_name:
            return RuleEvaluationResult(
                rule_id=rule.id,
                rule_name=rule.name,
                passed=False,
                severity=rule.severity,
                message="Regex rule requires column_name",
            )
        return evaluate_regex_rule(db, rule, table_name, column_name)

    elif rule.rule_type == RuleType.CUSTOM_SQL:
        return evaluate_custom_sql_rule(db, rule, table_name)

    elif rule.rule_type == RuleType.COMPARISON:
        if not column_name:
            return RuleEvaluationResult(
                rule_id=rule.id,
                rule_name=rule.name,
                passed=False,
                severity=rule.severity,
                message="Comparison rule requires column_name",
            )
        return evaluate_comparison_rule(db, rule, table_name, column_name)

    else:
        return RuleEvaluationResult(
            rule_id=rule.id,
            rule_name=rule.name,
            passed=False,
            severity=rule.severity,
            message=f"Unsupported rule type: {rule.rule_type}",
        )


def evaluate_rules_for_job(
    db: Session, job: IngestionJob, table_name: str
) -> DataQualityReport:
    """
    Evaluate all applicable rules for an ingestion job.

    Args:
        db: Database session
        job: The ingestion job
        table_name: Name of the ingested table

    Returns:
        DataQualityReport with aggregated results
    """
    start_time = time.time()

    # Get matching rules
    rules = get_matching_rules(db, job.source, table_name)

    if not rules:
        # No rules to evaluate
        report = DataQualityReport(
            job_id=job.id,
            source=job.source,
            report_type="job",
            total_rules=0,
            rules_passed=0,
            rules_failed=0,
            overall_status="passed",
            completed_at=datetime.utcnow(),
            execution_time_ms=int((time.time() - start_time) * 1000),
        )
        db.add(report)
        db.commit()
        db.refresh(report)
        return report

    # Create report
    report = DataQualityReport(
        job_id=job.id, source=job.source, report_type="job", total_rules=len(rules)
    )
    db.add(report)
    db.commit()
    db.refresh(report)

    # Evaluate each rule
    results = []
    failed_rules = []

    for rule in rules:
        eval_result = evaluate_rule(db, rule, table_name)
        results.append(eval_result)

        # Save result
        db_result = DataQualityResult(
            rule_id=rule.id,
            job_id=job.id,
            source=job.source,
            dataset_name=table_name,
            column_name=rule.column_name,
            passed=1 if eval_result.passed else 0,
            severity=eval_result.severity,
            message=eval_result.message,
            actual_value=eval_result.actual_value,
            expected_value=eval_result.expected_value,
            sample_failures=eval_result.sample_failures,
            rows_checked=eval_result.rows_checked,
            rows_passed=eval_result.rows_passed,
            rows_failed=eval_result.rows_failed,
            execution_time_ms=eval_result.execution_time_ms,
        )
        db.add(db_result)

        # Update rule statistics
        rule.times_evaluated += 1
        if eval_result.passed:
            rule.times_passed += 1
        else:
            rule.times_failed += 1
            failed_rules.append({"id": rule.id, "name": rule.name})
        rule.last_evaluated_at = datetime.utcnow()

    db.commit()

    # Calculate report summary
    passed_count = sum(1 for r in results if r.passed)
    failed_count = len(results) - passed_count
    errors = sum(
        1 for r in results if not r.passed and r.severity == RuleSeverity.ERROR
    )
    warnings = sum(
        1 for r in results if not r.passed and r.severity == RuleSeverity.WARNING
    )
    info = sum(1 for r in results if not r.passed and r.severity == RuleSeverity.INFO)

    # Determine overall status
    if errors > 0:
        overall_status = "failed"
    elif warnings > 0:
        overall_status = "warning"
    else:
        overall_status = "passed"

    # Update report
    report.rules_passed = passed_count
    report.rules_failed = failed_count
    report.errors = errors
    report.warnings = warnings
    report.info = info
    report.overall_status = overall_status
    report.failed_rules = failed_rules if failed_rules else None
    report.completed_at = datetime.utcnow()
    report.execution_time_ms = int((time.time() - start_time) * 1000)

    db.commit()
    db.refresh(report)

    logger.info(
        f"Data quality check for job {job.id}: "
        f"{passed_count}/{len(rules)} rules passed, status={overall_status}"
    )

    return report


# =============================================================================
# Rule Management
# =============================================================================


def create_rule(
    db: Session,
    name: str,
    rule_type: RuleType,
    severity: RuleSeverity = RuleSeverity.ERROR,
    source: Optional[str] = None,
    dataset_pattern: Optional[str] = None,
    column_name: Optional[str] = None,
    parameters: Optional[Dict[str, Any]] = None,
    description: Optional[str] = None,
    priority: int = 5,
) -> DataQualityRule:
    """
    Create a new data quality rule.
    """
    rule = DataQualityRule(
        name=name,
        description=description,
        source=source,
        dataset_pattern=dataset_pattern,
        column_name=column_name,
        rule_type=rule_type,
        severity=severity,
        parameters=parameters or {},
        priority=priority,
    )

    db.add(rule)
    db.commit()
    db.refresh(rule)

    logger.info(
        f"Created data quality rule '{name}' (type={rule_type}, severity={severity})"
    )
    return rule


def get_rule_results(
    db: Session,
    rule_id: Optional[int] = None,
    job_id: Optional[int] = None,
    passed_only: Optional[bool] = None,
    limit: int = 100,
) -> List[DataQualityResult]:
    """
    Get rule evaluation results with optional filters.
    """
    query = db.query(DataQualityResult)

    if rule_id:
        query = query.filter(DataQualityResult.rule_id == rule_id)
    if job_id:
        query = query.filter(DataQualityResult.job_id == job_id)
    if passed_only is not None:
        query = query.filter(DataQualityResult.passed == (1 if passed_only else 0))

    return query.order_by(DataQualityResult.evaluated_at.desc()).limit(limit).all()


def get_report(db: Session, report_id: int) -> Optional[DataQualityReport]:
    """Get a data quality report by ID."""
    return db.query(DataQualityReport).filter(DataQualityReport.id == report_id).first()


def get_job_report(db: Session, job_id: int) -> Optional[DataQualityReport]:
    """Get the data quality report for a job."""
    return (
        db.query(DataQualityReport)
        .filter(DataQualityReport.job_id == job_id)
        .order_by(DataQualityReport.started_at.desc())
        .first()
    )


# =============================================================================
# Evaluate All Rules
# =============================================================================


def evaluate_all_rules(db: Session) -> Dict[str, Any]:
    """
    Evaluate all enabled rules against their target tables.

    For each rule, resolves target tables from DatasetRegistry
    (by rule.source + rule.dataset_pattern regex), evaluates each
    (rule, table) pair, stores results, and creates a DataQualityReport.

    Returns:
        Summary dict with total/passed/failed counts
    """
    from app.core.models import DatasetRegistry

    start_time = time.time()

    # Get all enabled rules
    rules = (
        db.query(DataQualityRule)
        .filter(DataQualityRule.is_enabled == 1)
        .order_by(DataQualityRule.priority)
        .all()
    )

    if not rules:
        return {
            "total_rules": 0,
            "evaluations": 0,
            "passed": 0,
            "failed": 0,
            "errors": 0,
            "execution_time_ms": int((time.time() - start_time) * 1000),
        }

    # Get all registered tables
    all_datasets = db.query(DatasetRegistry).all()
    table_by_source: Dict[str, List[str]] = {}
    for ds in all_datasets:
        table_by_source.setdefault(ds.source, []).append(ds.table_name)

    # All table names for rules without a source filter
    all_table_names = [ds.table_name for ds in all_datasets]

    total_evaluations = 0
    total_passed = 0
    total_failed = 0
    total_errors = 0
    failed_rules_list = []

    for rule in rules:
        # Resolve target tables
        if rule.source:
            candidate_tables = table_by_source.get(rule.source, [])
        else:
            candidate_tables = all_table_names

        # Filter by dataset_pattern regex
        if rule.dataset_pattern:
            try:
                pattern = re.compile(rule.dataset_pattern)
                candidate_tables = [t for t in candidate_tables if pattern.match(t)]
            except re.error:
                logger.warning(
                    f"Invalid dataset pattern in rule {rule.id}: {rule.dataset_pattern}"
                )
                continue

        for table_name in candidate_tables:
            try:
                eval_result = evaluate_rule(db, rule, table_name)
                total_evaluations += 1

                # Store result
                db_result = DataQualityResult(
                    rule_id=rule.id,
                    source=rule.source or "all",
                    dataset_name=table_name,
                    column_name=rule.column_name,
                    passed=1 if eval_result.passed else 0,
                    severity=eval_result.severity,
                    message=eval_result.message,
                    actual_value=eval_result.actual_value,
                    expected_value=eval_result.expected_value,
                    sample_failures=eval_result.sample_failures,
                    rows_checked=eval_result.rows_checked,
                    rows_passed=eval_result.rows_passed,
                    rows_failed=eval_result.rows_failed,
                    execution_time_ms=eval_result.execution_time_ms,
                )
                db.add(db_result)

                # Update rule statistics
                rule.times_evaluated += 1
                if eval_result.passed:
                    rule.times_passed += 1
                    total_passed += 1
                else:
                    rule.times_failed += 1
                    total_failed += 1
                    failed_rules_list.append(
                        {"id": rule.id, "name": rule.name, "table": table_name}
                    )
                rule.last_evaluated_at = datetime.utcnow()

            except Exception as e:
                total_errors += 1
                logger.error(
                    f"Error evaluating rule {rule.id} on {table_name}: {e}"
                )

    # Create report record
    exec_ms = int((time.time() - start_time) * 1000)

    if total_failed > 0:
        overall_status = "failed"
    elif total_errors > 0:
        overall_status = "warning"
    else:
        overall_status = "passed"

    report = DataQualityReport(
        source="all",
        report_type="scheduled",
        total_rules=len(rules),
        rules_passed=total_passed,
        rules_failed=total_failed,
        overall_status=overall_status,
        failed_rules=failed_rules_list[:50] if failed_rules_list else None,
        completed_at=datetime.utcnow(),
        execution_time_ms=exec_ms,
    )
    db.add(report)
    db.commit()
    db.refresh(report)

    logger.info(
        f"Evaluate-all completed: {total_evaluations} evaluations, "
        f"{total_passed} passed, {total_failed} failed, {total_errors} errors "
        f"in {exec_ms}ms"
    )

    return {
        "report_id": report.id,
        "total_rules": len(rules),
        "evaluations": total_evaluations,
        "passed": total_passed,
        "failed": total_failed,
        "errors": total_errors,
        "overall_status": overall_status,
        "execution_time_ms": exec_ms,
    }


def scheduled_rule_evaluation():
    """Entry point for scheduled rule evaluation (called by APScheduler)."""
    from app.core.database import get_session_factory

    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        result = evaluate_all_rules(db)
        logger.info(
            f"Scheduled rule evaluation: {result['evaluations']} evaluations, "
            f"{result['passed']} passed, {result['failed']} failed"
        )
    except Exception as e:
        logger.error(f"Scheduled rule evaluation failed: {e}")
    finally:
        db.close()
