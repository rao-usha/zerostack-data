"""
Auto-generate data quality rules from profiling data.

Analyzes DataProfileColumn stats from the latest DataProfileSnapshot per table
and creates appropriate rules (NOT_NULL, RANGE, ENUM, REGEX, ROW_COUNT, FRESHNESS).
"""

import logging
import re
from typing import Dict, Any, Optional, List
from sqlalchemy.orm import Session
from sqlalchemy import func

from app.core.models import (
    DataQualityRule,
    DataProfileSnapshot,
    DataProfileColumn,
    RuleType,
    RuleSeverity,
)

logger = logging.getLogger(__name__)

# Columns to skip for NOT_NULL / ENUM rules (common auto-generated columns)
_SKIP_COLUMNS = {"id", "created_at", "updated_at", "deleted_at"}

# Column name patterns that suggest FIPS codes
_FIPS_PATTERNS = re.compile(r"(fips|geo_id|geoid|fips_code|state_fips|county_fips)", re.IGNORECASE)

# FIPS regex: 2-digit state, 5-digit county, or 11-digit tract
_FIPS_REGEX = r"^\d{2}$|^\d{5}$|^\d{11}$"

# Temporal column names for freshness rules
_TEMPORAL_COLUMNS = {"created_at", "updated_at", "ingested_at", "last_updated_at"}


def seed_rules_from_profiles(
    db: Session,
    source_filter: Optional[str] = None,
    table_filter: Optional[str] = None,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    Auto-generate data quality rules from profiling data.

    Analyzes the latest DataProfileSnapshot per table and creates rules:
    - NOT_NULL: columns with null_pct == 0.0 on tables with ≥50 rows
    - RANGE: numeric columns with valid mean/stddev (mean ± 4×stddev)
    - ENUM: columns with distinct_count ≤ 20 on tables with ≥50 rows
    - REGEX: columns matching fips/geo_id patterns
    - ROW_COUNT: one per table (min = 50% of current row count)
    - FRESHNESS: temporal columns (created_at/updated_at/ingested_at)

    Args:
        db: Database session
        source_filter: Only process tables from this source
        table_filter: Only process this specific table
        dry_run: If True, preview rules without creating them

    Returns:
        Dict with tables_analyzed, rules_created, rules_skipped_existing, by_type, dry_run
    """
    # Get latest snapshot per table using a subquery
    latest_snap_ids = (
        db.query(func.max(DataProfileSnapshot.id))
        .group_by(DataProfileSnapshot.table_name)
    )

    if source_filter:
        latest_snap_ids = latest_snap_ids.filter(
            DataProfileSnapshot.source == source_filter
        )

    if table_filter:
        latest_snap_ids = latest_snap_ids.filter(
            DataProfileSnapshot.table_name == table_filter
        )

    snapshots = (
        db.query(DataProfileSnapshot)
        .filter(DataProfileSnapshot.id.in_(latest_snap_ids))
        .all()
    )

    if not snapshots:
        return {
            "tables_analyzed": 0,
            "rules_created": 0,
            "rules_skipped_existing": 0,
            "by_type": {},
            "dry_run": dry_run,
        }

    # Get all column profiles for these snapshots
    snap_ids = [s.id for s in snapshots]
    columns = (
        db.query(DataProfileColumn)
        .filter(DataProfileColumn.snapshot_id.in_(snap_ids))
        .all()
    )

    # Group columns by snapshot_id
    cols_by_snap: Dict[int, List[DataProfileColumn]] = {}
    for col in columns:
        cols_by_snap.setdefault(col.snapshot_id, []).append(col)

    # Get existing auto-generated rule names for idempotency
    existing_names = set(
        name
        for (name,) in db.query(DataQualityRule.name)
        .filter(DataQualityRule.name.like("auto_%"))
        .all()
    )

    rules_created = 0
    rules_skipped = 0
    by_type: Dict[str, int] = {}
    proposed_rules: List[Dict[str, Any]] = []

    for snap in snapshots:
        table_name = snap.table_name
        row_count = snap.row_count or 0
        snap_cols = cols_by_snap.get(snap.id, [])

        # --- ROW_COUNT rule (one per table) ---
        if row_count > 0:
            min_rows = max(1, int(row_count * 0.5))
            rule_spec = _make_rule_spec(
                name=f"auto_row_count_{table_name}",
                rule_type=RuleType.ROW_COUNT,
                severity=RuleSeverity.WARNING,
                source=snap.source,
                dataset_pattern=f"^{re.escape(table_name)}$",
                column_name=None,
                parameters={"min": min_rows},
                description=f"[Auto-generated] Table {table_name} should have at least {min_rows} rows (50% of {row_count})",
            )
            proposed_rules.append(rule_spec)

        for col in snap_cols:
            col_name = col.column_name
            col_lower = col_name.lower()
            stats = col.stats or {}

            # Skip common auto-generated columns for some rule types
            skip_col = col_lower in _SKIP_COLUMNS

            # --- NOT_NULL rule ---
            if not skip_col and row_count >= 50 and col.null_pct is not None and col.null_pct == 0.0:
                rule_spec = _make_rule_spec(
                    name=f"auto_not_null_{table_name}_{col_name}",
                    rule_type=RuleType.NOT_NULL,
                    severity=RuleSeverity.WARNING,
                    source=snap.source,
                    dataset_pattern=f"^{re.escape(table_name)}$",
                    column_name=col_name,
                    parameters={},
                    description=f"[Auto-generated] {table_name}.{col_name} has 0% nulls — enforce not-null",
                )
                proposed_rules.append(rule_spec)

            # --- RANGE rule (numeric columns with valid stats) ---
            mean = stats.get("mean")
            stddev = stats.get("stddev")
            if mean is not None and stddev is not None:
                try:
                    mean_f = float(mean)
                    stddev_f = float(stddev)
                    if stddev_f > 0:
                        # Detect skewed distributions: if stddev >> mean, data is
                        # heavily skewed (e.g., populations, dollar amounts). Use
                        # percentile-based bounds when available, otherwise widen
                        # the multiplier to avoid false positives.
                        p25 = stats.get("p25")
                        p75 = stats.get("p75")
                        median = stats.get("median")
                        stat_min = stats.get("min")
                        stat_max = stats.get("max")

                        # Coefficient of variation > 1.5 = highly skewed
                        cv = stddev_f / abs(mean_f) if mean_f != 0 else float("inf")
                        is_skewed = cv > 1.5

                        if is_skewed and p25 is not None and p75 is not None:
                            # Use IQR-based range: wider and handles skew better
                            iqr = float(p75) - float(p25)
                            range_min = round(float(p25) - 6 * iqr, 4)
                            range_max = round(float(p75) + 6 * iqr, 4)
                            method = "IQR×6"
                        elif is_skewed and stat_min is not None and stat_max is not None:
                            # Fallback for skewed: use actual min/max with 50% headroom
                            actual_range = float(stat_max) - float(stat_min)
                            range_min = round(float(stat_min) - 0.5 * actual_range, 4)
                            range_max = round(float(stat_max) + 0.5 * actual_range, 4)
                            method = "min/max×1.5"
                        else:
                            # Normal distribution: mean ± 4σ
                            range_min = round(mean_f - 4 * stddev_f, 4)
                            range_max = round(mean_f + 4 * stddev_f, 4)
                            method = "mean±4σ"

                        rule_spec = _make_rule_spec(
                            name=f"auto_range_{table_name}_{col_name}",
                            rule_type=RuleType.RANGE,
                            severity=RuleSeverity.WARNING,
                            source=snap.source,
                            dataset_pattern=f"^{re.escape(table_name)}$",
                            column_name=col_name,
                            parameters={"min": range_min, "max": range_max},
                            description=(
                                f"[Auto-generated] {table_name}.{col_name} range check "
                                f"({method}, cv={cv:.1f})"
                            ),
                        )
                        proposed_rules.append(rule_spec)
                except (ValueError, TypeError):
                    pass

            # --- ENUM rule (low cardinality columns) ---
            if (
                not skip_col
                and row_count >= 50
                and col.distinct_count is not None
                and col.distinct_count <= 20
                and col.distinct_count > 0
            ):
                # Skip ID-like columns (high cardinality ratio = unique)
                if col.cardinality_ratio is not None and col.cardinality_ratio > 0.5:
                    pass
                else:
                    top_values = stats.get("top_values", [])
                    if top_values:
                        allowed = [
                            tv["value"] if isinstance(tv, dict) else str(tv)
                            for tv in top_values[:20]
                        ]
                        if allowed:
                            rule_spec = _make_rule_spec(
                                name=f"auto_enum_{table_name}_{col_name}",
                                rule_type=RuleType.ENUM,
                                severity=RuleSeverity.INFO,
                                source=snap.source,
                                dataset_pattern=f"^{re.escape(table_name)}$",
                                column_name=col_name,
                                parameters={"allowed": allowed},
                                description=(
                                    f"[Auto-generated] {table_name}.{col_name} has "
                                    f"{col.distinct_count} distinct values — enforce enum"
                                ),
                            )
                            proposed_rules.append(rule_spec)

            # --- REGEX rule (FIPS/geo_id columns) ---
            if _FIPS_PATTERNS.search(col_name):
                rule_spec = _make_rule_spec(
                    name=f"auto_regex_{table_name}_{col_name}",
                    rule_type=RuleType.REGEX,
                    severity=RuleSeverity.WARNING,
                    source=snap.source,
                    dataset_pattern=f"^{re.escape(table_name)}$",
                    column_name=col_name,
                    parameters={"pattern": _FIPS_REGEX},
                    description=(
                        f"[Auto-generated] {table_name}.{col_name} should match "
                        "FIPS code format (2/5/11 digits)"
                    ),
                )
                proposed_rules.append(rule_spec)

            # --- FRESHNESS rule (temporal columns) ---
            if col_lower in _TEMPORAL_COLUMNS:
                rule_spec = _make_rule_spec(
                    name=f"auto_freshness_{table_name}_{col_name}",
                    rule_type=RuleType.FRESHNESS,
                    severity=RuleSeverity.INFO,
                    source=snap.source,
                    dataset_pattern=f"^{re.escape(table_name)}$",
                    column_name=col_name,
                    parameters={"max_age_days": 90},
                    description=(
                        f"[Auto-generated] {table_name}.{col_name} should have data "
                        "within the last 90 days"
                    ),
                )
                proposed_rules.append(rule_spec)

    # Create or count rules
    for spec in proposed_rules:
        rule_type_key = spec["rule_type"].value
        if spec["name"] in existing_names:
            rules_skipped += 1
            continue

        by_type[rule_type_key] = by_type.get(rule_type_key, 0) + 1

        if not dry_run:
            rule = DataQualityRule(
                name=spec["name"],
                description=spec["description"],
                source=spec["source"],
                dataset_pattern=spec["dataset_pattern"],
                column_name=spec["column_name"],
                rule_type=spec["rule_type"],
                severity=spec["severity"],
                parameters={**spec["parameters"], "_auto_generated": True},
                priority=8,
            )
            db.add(rule)
            existing_names.add(spec["name"])

        rules_created += 1

    if not dry_run and rules_created > 0:
        db.commit()

    logger.info(
        f"Rule seeder: {len(snapshots)} tables analyzed, "
        f"{rules_created} rules {'proposed' if dry_run else 'created'}, "
        f"{rules_skipped} skipped (existing)"
    )

    return {
        "tables_analyzed": len(snapshots),
        "rules_created": rules_created,
        "rules_skipped_existing": rules_skipped,
        "by_type": by_type,
        "dry_run": dry_run,
    }


def delete_auto_generated_rules(db: Session) -> Dict[str, Any]:
    """
    Delete all auto-generated rules (names starting with 'auto_').

    Returns:
        Dict with count of deleted rules
    """
    rules = (
        db.query(DataQualityRule)
        .filter(DataQualityRule.name.like("auto_%"))
        .all()
    )

    count = len(rules)
    for rule in rules:
        db.delete(rule)

    if count > 0:
        db.commit()

    logger.info(f"Deleted {count} auto-generated rules")
    return {"deleted": count}


def _make_rule_spec(
    name: str,
    rule_type: RuleType,
    severity: RuleSeverity,
    source: Optional[str],
    dataset_pattern: Optional[str],
    column_name: Optional[str],
    parameters: Dict[str, Any],
    description: str,
) -> Dict[str, Any]:
    """Build a rule specification dict."""
    return {
        "name": name,
        "rule_type": rule_type,
        "severity": severity,
        "source": source,
        "dataset_pattern": dataset_pattern,
        "column_name": column_name,
        "parameters": parameters,
        "description": description,
    }
