"""
Data Profiling Engine.

Auto-profiles database tables after ingestion, computing per-column statistics
and storing snapshots for historical comparison and drift detection.
"""

import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.models import (
    DataProfileSnapshot,
    DataProfileColumn,
    DatasetRegistry,
)

logger = logging.getLogger(__name__)

# Row threshold for sampling
SAMPLE_THRESHOLD = 1_000_000
SAMPLE_PCT = 10  # BERNOULLI percentage for large tables


# =============================================================================
# Column type classification
# =============================================================================

NUMERIC_TYPES = {
    "integer", "bigint", "smallint", "numeric", "decimal", "real",
    "double precision", "float", "int", "int4", "int8", "int2",
    "float4", "float8", "serial", "bigserial",
}

TEMPORAL_TYPES = {
    "date", "timestamp", "timestamp without time zone",
    "timestamp with time zone", "timestamptz", "time",
    "time without time zone", "time with time zone",
}

STRING_TYPES = {
    "text", "varchar", "character varying", "char", "character",
    "name", "citext", "uuid",
}

# Types that don't support equality comparison (skip COUNT(DISTINCT))
NON_COMPARABLE_TYPES = {
    "json", "jsonb", "xml", "bytea", "tsvector", "tsquery",
    "point", "line", "lseg", "box", "path", "polygon", "circle",
}


def _classify_column_type(pg_type: str) -> str:
    """Classify a PostgreSQL column type into numeric/temporal/string/other."""
    pg_type_lower = pg_type.lower().strip()
    # Strip length specifiers like varchar(255)
    base_type = pg_type_lower.split("(")[0].strip()

    if base_type in NON_COMPARABLE_TYPES:
        return "skip"
    if base_type in NUMERIC_TYPES:
        return "numeric"
    if base_type in TEMPORAL_TYPES:
        return "temporal"
    if base_type in STRING_TYPES:
        return "string"
    return "other"


# =============================================================================
# Profiling queries
# =============================================================================

def _get_table_row_count_estimate(db: Session, table_name: str) -> int:
    """Get estimated row count from pg_class (fast, no full scan)."""
    result = db.execute(
        text(
            "SELECT reltuples::bigint FROM pg_class WHERE relname = :table"
        ),
        {"table": table_name},
    ).fetchone()
    return int(result[0]) if result and result[0] > 0 else 0


def _get_schema_info(db: Session, table_name: str) -> List[Dict[str, Any]]:
    """Get column schema information from information_schema."""
    rows = db.execute(
        text("""
            SELECT column_name, data_type, is_nullable,
                   udt_name, character_maximum_length
            FROM information_schema.columns
            WHERE table_name = :table
              AND table_schema = 'public'
            ORDER BY ordinal_position
        """),
        {"table": table_name},
    ).fetchall()

    return [
        {
            "name": r[0],
            "type": r[1],
            "udt_name": r[3],
            "nullable": r[2] == "YES",
            "max_length": r[4],
        }
        for r in rows
    ]


def _build_numeric_stats_sql(col: str, from_clause: str) -> str:
    """Build SQL for numeric column statistics."""
    safe_col = f'"{col}"'
    return f"""
        SELECT
            COUNT(*) AS total,
            COUNT({safe_col}) AS non_null,
            COUNT(*) - COUNT({safe_col}) AS null_count,
            COUNT(DISTINCT {safe_col}) AS distinct_count,
            MIN({safe_col}::numeric) AS min_val,
            MAX({safe_col}::numeric) AS max_val,
            AVG({safe_col}::numeric) AS mean_val,
            STDDEV({safe_col}::numeric) AS stddev_val,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {safe_col}::numeric) AS p25,
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY {safe_col}::numeric) AS median_val,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {safe_col}::numeric) AS p75
        FROM {from_clause}
    """


def _build_string_stats_sql(col: str, from_clause: str) -> str:
    """Build SQL for string column statistics."""
    safe_col = f'"{col}"'
    return f"""
        SELECT
            COUNT(*) AS total,
            COUNT({safe_col}) AS non_null,
            COUNT(*) - COUNT({safe_col}) AS null_count,
            COUNT(DISTINCT {safe_col}) AS distinct_count,
            MIN(LENGTH({safe_col}::text)) AS min_length,
            MAX(LENGTH({safe_col}::text)) AS max_length,
            AVG(LENGTH({safe_col}::text)) AS avg_length
        FROM {from_clause}
    """


def _build_temporal_stats_sql(col: str, from_clause: str) -> str:
    """Build SQL for temporal column statistics."""
    safe_col = f'"{col}"'
    return f"""
        SELECT
            COUNT(*) AS total,
            COUNT({safe_col}) AS non_null,
            COUNT(*) - COUNT({safe_col}) AS null_count,
            COUNT(DISTINCT {safe_col}) AS distinct_count,
            MIN({safe_col}) AS min_date,
            MAX({safe_col}) AS max_date,
            EXTRACT(DAY FROM MAX({safe_col}) - MIN({safe_col})) AS date_range_days
        FROM {from_clause}
    """


def _build_basic_stats_sql(col: str, from_clause: str) -> str:
    """Build SQL for columns with unknown type (basic null/distinct counts)."""
    safe_col = f'"{col}"'
    return f"""
        SELECT
            COUNT(*) AS total,
            COUNT({safe_col}) AS non_null,
            COUNT(*) - COUNT({safe_col}) AS null_count,
            COUNT(DISTINCT {safe_col}) AS distinct_count
        FROM {from_clause}
    """


def _get_top_values(db: Session, table_name: str, col: str, from_clause: str, limit: int = 10) -> List[Dict]:
    """Get top N most frequent values for a column."""
    safe_col = f'"{col}"'
    try:
        rows = db.execute(
            text(f"""
                SELECT {safe_col}::text AS val, COUNT(*) AS cnt
                FROM {from_clause}
                WHERE {safe_col} IS NOT NULL
                GROUP BY {safe_col}
                ORDER BY cnt DESC
                LIMIT :lim
            """),
            {"lim": limit},
        ).fetchall()
        return [{"value": r[0], "count": int(r[1])} for r in rows]
    except Exception:
        return []


# =============================================================================
# Main profiling functions
# =============================================================================

def profile_table(
    db: Session,
    table_name: str,
    job_id: Optional[int] = None,
    source: Optional[str] = None,
    domain: Optional[str] = None,
) -> Optional[DataProfileSnapshot]:
    """
    Profile a single table. Computes per-column statistics and stores a snapshot.

    Uses TABLESAMPLE BERNOULLI(10) for tables > 1M rows.
    Uses pg_try_advisory_lock to prevent concurrent profiling of same table.
    """
    start_time = time.time()

    # Advisory lock to prevent concurrent profiling of same table
    lock_key = hash(table_name) & 0x7FFFFFFF  # positive 32-bit int
    lock_result = db.execute(
        text("SELECT pg_try_advisory_lock(:key)"), {"key": lock_key}
    ).scalar()
    if not lock_result:
        logger.info(f"Profiling already in progress for {table_name}, skipping")
        return None

    try:
        # Check table exists
        table_exists = db.execute(
            text("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_name = :table AND table_schema = 'public'
                )
            """),
            {"table": table_name},
        ).scalar()

        if not table_exists:
            logger.warning(f"Table {table_name} does not exist, skipping profile")
            return None

        # Get schema info
        columns = _get_schema_info(db, table_name)
        if not columns:
            logger.warning(f"No columns found for {table_name}")
            return None

        # Determine if sampling is needed
        estimated_rows = _get_table_row_count_estimate(db, table_name)
        use_sampling = estimated_rows > SAMPLE_THRESHOLD
        from_clause = (
            f'"{table_name}" TABLESAMPLE BERNOULLI({SAMPLE_PCT})'
            if use_sampling
            else f'"{table_name}"'
        )

        # Get actual row count
        row_count_result = db.execute(
            text(f'SELECT COUNT(*) FROM "{table_name}"')
        ).scalar()
        row_count = int(row_count_result) if row_count_result else 0

        # Profile each column
        column_profiles = []
        total_null_count = 0

        for col_info in columns:
            col_name = col_info["name"]
            col_type = _classify_column_type(col_info.get("udt_name", col_info["type"]))

            # Skip non-comparable types (json, jsonb, xml, etc.)
            if col_type == "skip":
                # Only count nulls for these columns
                try:
                    safe_col = f'"{col_name}"'
                    null_result = db.execute(
                        text(f'SELECT COUNT(*) - COUNT({safe_col}) FROM {from_clause}')
                    ).scalar()
                    nc = int(null_result) if null_result else 0
                    total_null_count += nc
                    column_profiles.append({
                        "column_name": col_name,
                        "data_type": col_info["type"],
                        "classified_type": col_type,
                        "null_count": nc,
                        "null_pct": round((nc / row_count * 100) if row_count > 0 else 0, 2),
                        "distinct_count": None,
                        "cardinality_ratio": None,
                        "stats": {},
                    })
                except Exception:
                    db.rollback()
                continue

            try:
                if col_type == "numeric":
                    sql = _build_numeric_stats_sql(col_name, from_clause)
                elif col_type == "string":
                    sql = _build_string_stats_sql(col_name, from_clause)
                elif col_type == "temporal":
                    sql = _build_temporal_stats_sql(col_name, from_clause)
                else:
                    sql = _build_basic_stats_sql(col_name, from_clause)

                result = db.execute(text(sql)).fetchone()
                if not result:
                    continue

                total = int(result[0]) if result[0] else 0
                non_null = int(result[1]) if result[1] else 0
                null_count = int(result[2]) if result[2] else 0
                distinct_count = int(result[3]) if result[3] else 0

                total_null_count += null_count
                null_pct = (null_count / total * 100) if total > 0 else 0
                cardinality = (distinct_count / non_null) if non_null > 0 else 0

                # Build type-specific stats
                stats = {}
                if col_type == "numeric" and len(result) >= 11:
                    stats = {
                        "min": float(result[4]) if result[4] is not None else None,
                        "max": float(result[5]) if result[5] is not None else None,
                        "mean": float(result[6]) if result[6] is not None else None,
                        "stddev": float(result[7]) if result[7] is not None else None,
                        "p25": float(result[8]) if result[8] is not None else None,
                        "median": float(result[9]) if result[9] is not None else None,
                        "p75": float(result[10]) if result[10] is not None else None,
                    }
                elif col_type == "string" and len(result) >= 7:
                    stats = {
                        "min_length": int(result[4]) if result[4] is not None else None,
                        "max_length": int(result[5]) if result[5] is not None else None,
                        "avg_length": float(result[6]) if result[6] is not None else None,
                        "top_values": _get_top_values(db, table_name, col_name, from_clause),
                    }
                elif col_type == "temporal" and len(result) >= 7:
                    stats = {
                        "min_date": str(result[4]) if result[4] is not None else None,
                        "max_date": str(result[5]) if result[5] is not None else None,
                        "date_range_days": float(result[6]) if result[6] is not None else None,
                    }

                column_profiles.append({
                    "column_name": col_name,
                    "data_type": col_info["type"],
                    "classified_type": col_type,
                    "null_count": null_count,
                    "null_pct": round(null_pct, 2),
                    "distinct_count": distinct_count,
                    "cardinality_ratio": round(cardinality, 4),
                    "stats": stats,
                })

            except Exception as e:
                db.rollback()  # Recover from failed SQL transaction
                logger.warning(f"Error profiling column {col_name} in {table_name}: {e}")
                continue

        # Compute overall completeness
        total_cells = row_count * len(columns) if columns else 1
        overall_completeness = ((total_cells - total_null_count) / total_cells * 100) if total_cells > 0 else 0

        execution_time_ms = int((time.time() - start_time) * 1000)

        # Create snapshot
        snapshot = DataProfileSnapshot(
            table_name=table_name,
            source=source,
            domain=domain,
            job_id=job_id,
            row_count=row_count,
            column_count=len(columns),
            total_null_count=total_null_count,
            overall_completeness_pct=round(overall_completeness, 2),
            schema_snapshot=[
                {"name": c["name"], "type": c["type"], "nullable": c["nullable"]}
                for c in columns
            ],
            profiled_at=datetime.utcnow(),
            execution_time_ms=execution_time_ms,
        )
        db.add(snapshot)
        db.flush()  # Get the ID

        # Create column profiles
        for cp in column_profiles:
            col_record = DataProfileColumn(
                snapshot_id=snapshot.id,
                column_name=cp["column_name"],
                data_type=cp["data_type"],
                null_count=cp["null_count"],
                null_pct=cp["null_pct"],
                distinct_count=cp["distinct_count"],
                cardinality_ratio=cp["cardinality_ratio"],
                stats=cp["stats"],
            )
            db.add(col_record)

        db.commit()

        logger.info(
            f"Profiled {table_name}: {row_count} rows, {len(columns)} columns, "
            f"{overall_completeness:.1f}% complete ({execution_time_ms}ms)"
        )
        return snapshot

    except Exception as e:
        db.rollback()
        logger.error(f"Error profiling table {table_name}: {e}")
        raise
    finally:
        # Release advisory lock
        db.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": lock_key})


def profile_all_tables(db: Session) -> List[DataProfileSnapshot]:
    """Profile all tables in the dataset registry."""
    registries = db.query(DatasetRegistry).all()
    snapshots = []

    for registry in registries:
        try:
            snapshot = profile_table(
                db,
                registry.table_name,
                source=registry.source,
            )
            if snapshot:
                snapshots.append(snapshot)
        except Exception as e:
            logger.error(f"Failed to profile {registry.table_name}: {e}")
            continue

    logger.info(f"Profiled {len(snapshots)}/{len(registries)} tables")
    return snapshots


def get_latest_profile(db: Session, table_name: str) -> Optional[DataProfileSnapshot]:
    """Get the most recent profile snapshot for a table."""
    return (
        db.query(DataProfileSnapshot)
        .filter(DataProfileSnapshot.table_name == table_name)
        .order_by(DataProfileSnapshot.profiled_at.desc())
        .first()
    )


def get_profile_history(
    db: Session, table_name: str, limit: int = 30
) -> List[DataProfileSnapshot]:
    """Get profile history for a table (most recent first)."""
    return (
        db.query(DataProfileSnapshot)
        .filter(DataProfileSnapshot.table_name == table_name)
        .order_by(DataProfileSnapshot.profiled_at.desc())
        .limit(limit)
        .all()
    )


def get_column_stats(db: Session, snapshot_id: int) -> List[DataProfileColumn]:
    """Get column-level stats for a specific snapshot."""
    return (
        db.query(DataProfileColumn)
        .filter(DataProfileColumn.snapshot_id == snapshot_id)
        .order_by(DataProfileColumn.null_pct.desc())
        .all()
    )
