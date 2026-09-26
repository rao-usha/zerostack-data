"""
Data Profiling Engine.

Auto-profiles database tables after ingestion, computing per-column statistics
and storing snapshots for historical comparison and drift detection.

SPEC_144: the tables come from the dataset catalog (``app.catalog.quality.
dq_targets``), not only ``DatasetRegistry.ingested()``; schema-qualified tables
(``core.entity``) are profiled; every statement runs under a timeout, large
tables are sampled and use the planner estimate instead of ``count(*)``; the
advisory lock is keyed by ``hashtext`` on its own connection, so the API and
the worker processes agree on it.
"""

import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.export_policy import is_exportable
from app.core.models import (
    DataProfileSnapshot,
    DataProfileColumn,
)
from app.core.safe_sql import qi

logger = logging.getLogger(__name__)

# Row threshold for sampling
SAMPLE_THRESHOLD = 1_000_000
SAMPLE_PCT = 10  # BERNOULLI percentage for large tables
# SPEC_144 guards for the catalog's largest tables (SEC bulk, 13F holdings)
HUGE_THRESHOLD = 20_000_000       # above this, SYSTEM (block) sampling
HUGE_SAMPLE_PCT = 1
EXACT_COUNT_MAX_ROWS = 5_000_000  # above this, row_count is the planner estimate
PROFILE_STATEMENT_TIMEOUT_MS = 60_000
# advisory lock namespace (first int of the two-int form; dataset runs use 124)
PROFILE_LOCK_NAMESPACE = 144
# scheduled profiling bounds (profile_stale_catalog_tables)
SCHEDULED_MAX_TABLES = 40
SCHEDULED_DEADLINE_S = 1800
UNCHANGED_RECHECK_FACTOR = 4      # re-profile an unchanged table after 4x its cadence


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

def _split(table_name: str):
    """'core.entity' -> ('core', 'entity'); 'pe_firms' -> ('public', 'pe_firms')."""
    if "." in table_name:
        schema, name = table_name.split(".", 1)
        return schema, name
    return "public", table_name


def _fq(table_name: str) -> str:
    schema, name = _split(table_name)
    return f"{qi(schema)}.{qi(name)}"


def _engine(db: Session):
    bind = db.get_bind()
    return getattr(bind, "engine", bind)


def _is_pg(db: Session) -> bool:
    return getattr(getattr(_engine(db), "dialect", None), "name", "") == "postgresql"


def _guarded(db: Session, sql: str, params: Optional[Dict[str, Any]] = None):
    """Execute under SET LOCAL statement_timeout, re-applied per statement
    (a rollback after a failed column query ends the transaction it was set in)."""
    if _is_pg(db):
        db.execute(text(f"SET LOCAL statement_timeout = {int(PROFILE_STATEMENT_TIMEOUT_MS)}"))
    return db.execute(text(sql), params or {})


def _get_table_row_count_estimate(db: Session, table_name: str) -> int:
    """Get estimated row count from pg_class (fast, no full scan)."""
    schema, name = _split(table_name)
    result = db.execute(
        text("SELECT reltuples::bigint FROM pg_class WHERE oid = to_regclass(:fq)"),
        {"fq": f'"{schema}"."{name}"'},
    ).fetchone()
    return int(result[0]) if result and result[0] is not None and result[0] > 0 else 0


def _get_schema_info(db: Session, table_name: str) -> List[Dict[str, Any]]:
    """Get column schema information from information_schema."""
    schema, name = _split(table_name)
    rows = db.execute(
        text("""
            SELECT column_name, data_type, is_nullable,
                   udt_name, character_maximum_length
            FROM information_schema.columns
            WHERE table_name = :table
              AND table_schema = :schema
            ORDER BY ordinal_position
        """),
        {"table": name, "schema": schema},
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
        rows = _guarded(
            db,
            f"""
                SELECT {safe_col}::text AS val, COUNT(*) AS cnt
                FROM {from_clause}
                WHERE {safe_col} IS NOT NULL
                GROUP BY {safe_col}
                ORDER BY cnt DESC
                LIMIT :lim
            """,
            {"lim": limit},
        ).fetchall()
        return [{"value": r[0], "count": int(r[1])} for r in rows]
    except Exception:
        db.rollback()
        return []


def is_profilable(db: Session, table_name: str) -> bool:
    """SPEC_127: profiles store top_values of string columns, so they are a
    table reader like /export. Apply the same policy: auth, key, lead and
    credential-bearing tables are never profiled or served."""
    columns = [c["name"] for c in _get_schema_info(db, table_name)]
    return is_exportable(table_name, columns)


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

    Uses TABLESAMPLE BERNOULLI(10) for tables > 1M rows, SYSTEM(1) above 20M,
    and the planner estimate instead of count(*) above 5M. Every statement runs
    under ``PROFILE_STATEMENT_TIMEOUT_MS``. ``table_name`` may be
    schema-qualified (``core.entity``).

    Concurrent profiling of the same table (API and worker processes) is
    prevented by ``pg_try_advisory_lock(144, hashtext(table))`` held on a
    dedicated connection: the key is computed by the server, so every process
    agrees on it, and the unlock runs on the connection that holds the lock.
    """
    start_time = time.time()

    lock_conn = _acquire_profile_lock(db, table_name)
    if lock_conn is False:
        logger.info(f"Profiling already in progress for {table_name}, skipping")
        return None

    try:
        schema, name = _split(table_name)
        # Check table exists
        table_exists = db.execute(
            text("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_name = :table AND table_schema = :schema
                )
            """),
            {"table": name, "schema": schema},
        ).scalar()

        if not table_exists:
            logger.warning(f"Table {table_name} does not exist, skipping profile")
            return None

        # Get schema info
        columns = _get_schema_info(db, table_name)
        if not columns:
            logger.warning(f"No columns found for {table_name}")
            return None

        if not is_exportable(table_name, [c["name"] for c in columns]):
            logger.warning(f"Table {table_name} is not profilable (SPEC_127 policy), skipping")
            return None

        # Determine if sampling is needed
        estimated_rows = _get_table_row_count_estimate(db, table_name)
        from_clause = _from_clause(table_name, estimated_rows)

        # Row count: exact up to EXACT_COUNT_MAX_ROWS, the planner estimate above
        row_count = _row_count(db, table_name, estimated_rows)

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
                    seen, null_result = _guarded(
                        db, f'SELECT COUNT(*), COUNT(*) - COUNT({safe_col}) FROM {from_clause}'
                    ).fetchone()
                    nc = int(null_result) if null_result else 0
                    total_null_count += nc
                    column_profiles.append({
                        "column_name": col_name,
                        "data_type": col_info["type"],
                        "classified_type": col_type,
                        "null_count": nc,
                        # over the rows read (the sample, when sampling)
                        "null_pct": round((nc / seen * 100) if seen else 0, 2),
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

                result = _guarded(db, sql).fetchone()
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

        # Overall completeness: the mean of the column completeness (the same as
        # non-null cells / cells without sampling; still right when sampled,
        # where the null counts are over the sample, not over row_count)
        if column_profiles and row_count > 0:
            overall_completeness = 100 - sum(cp["null_pct"] for cp in column_profiles) / len(column_profiles)
            total_null_count = int(sum(cp["null_pct"] / 100 * row_count for cp in column_profiles))
        else:
            overall_completeness = 0
        total_null_count = min(total_null_count, INT4_MAX)
        row_count = min(row_count, INT4_MAX)

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
        _release_profile_lock(lock_conn, table_name)


# =============================================================================
# Guards (SPEC_144)
# =============================================================================

INT4_MAX = 2_147_483_647  # row_count / total_null_count are INTEGER columns


def profile_lock_sql() -> str:
    """The lock statement: the key is computed by the server (hashtext), so
    every process -- api, worker -- derives the same one."""
    return "SELECT pg_try_advisory_lock(:ns, hashtext(:t))"


def _acquire_profile_lock(db: Session, table_name: str):
    """A dedicated autocommit connection holding the lock; None when locking
    does not apply (not PostgreSQL); False when another process holds it."""
    if not _is_pg(db):
        return None
    conn = _engine(db).connect().execution_options(isolation_level="AUTOCOMMIT")
    try:
        got = conn.execute(text(profile_lock_sql()),
                           {"ns": PROFILE_LOCK_NAMESPACE, "t": table_name}).scalar()
    except Exception:
        conn.close()
        raise
    if not got:
        conn.close()
        return False
    return conn


def _release_profile_lock(conn, table_name: str) -> None:
    if not conn:
        return
    try:
        conn.execute(text("SELECT pg_advisory_unlock(:ns, hashtext(:t))"),
                     {"ns": PROFILE_LOCK_NAMESPACE, "t": table_name})
    except Exception as e:
        logger.warning(f"Advisory unlock for {table_name} failed: {type(e).__name__}")
    finally:
        conn.close()  # closing the session also releases a session-level lock


def _from_clause(table_name: str, estimated_rows: int) -> str:
    fq = _fq(table_name)
    if estimated_rows > HUGE_THRESHOLD:
        return f"{fq} TABLESAMPLE SYSTEM({HUGE_SAMPLE_PCT})"
    if estimated_rows > SAMPLE_THRESHOLD:
        return f"{fq} TABLESAMPLE BERNOULLI({SAMPLE_PCT})"
    return fq


def _row_count(db: Session, table_name: str, estimated_rows: int) -> int:
    if estimated_rows > EXACT_COUNT_MAX_ROWS:
        return estimated_rows
    try:
        return int(_guarded(db, f"SELECT COUNT(*) FROM {_fq(table_name)}").scalar() or 0)
    except Exception as e:
        db.rollback()
        logger.warning(f"Exact count of {table_name} failed ({type(e).__name__}); using the estimate")
        return estimated_rows


# =============================================================================
# Catalog-driven runs (SPEC_144)
# =============================================================================


def profile_all_tables(db: Session) -> List[DataProfileSnapshot]:
    """Profile every catalog table that exists, plus ingested registry tables
    the catalog does not cover (``app.catalog.quality.dq_targets``)."""
    from app.catalog.quality import dq_targets

    targets = dq_targets(db)
    snapshots = []

    for target in targets:
        try:
            snapshot = profile_table(
                db,
                target["table"],
                source=target["source"],
                domain=target.get("domain"),
            )
            if snapshot:
                snapshots.append(snapshot)
        except Exception as e:
            logger.error(f"Failed to profile {target['table']}: {e}")
            continue

    logger.info(f"Profiled {len(snapshots)}/{len(targets)} tables")
    return snapshots


def _profile_due(latest: Optional[DataProfileSnapshot], estimate: Optional[int],
                 cadence_h: float, now: datetime) -> Optional[str]:
    """Why a table should be profiled now, or None to skip it."""
    if latest is None:
        return "never profiled"
    age_h = (now - latest.profiled_at).total_seconds() / 3600
    if age_h < cadence_h:
        return None
    if estimate is not None and estimate == latest.row_count and \
            age_h < cadence_h * UNCHANGED_RECHECK_FACTOR:
        return None  # row estimate unchanged since the last profile
    return f"profile {age_h:.0f}h old (cadence {cadence_h:.0f}h)"


def profile_stale_catalog_tables(
    db: Session,
    now: Optional[datetime] = None,
    max_tables: int = SCHEDULED_MAX_TABLES,
    deadline_s: float = SCHEDULED_DEADLINE_S,
) -> Dict[str, Any]:
    """Profile catalog tables whose latest profile is older than the dataset's
    cadence, oldest first; bounded by ``max_tables`` and ``deadline_s``."""
    from app.catalog.quality import cadence_hours, dq_targets
    from app.catalog.registry import get_spec

    now = now or datetime.utcnow()
    started = time.monotonic()
    due = []
    for target in dq_targets(db, include_registry=False):
        spec = get_spec(target["dataset_key"]) if target["dataset_key"] else None
        latest = get_latest_profile(db, target["table"])
        why = _profile_due(latest, target.get("rows_estimate"),
                           cadence_hours(spec.cadence if spec else None), now)
        if why:
            due.append((latest.profiled_at if latest else datetime.min, target, why))
    due.sort(key=lambda d: d[0])
    summary: Dict[str, Any] = {"due": len(due), "profiled": [], "skipped": [], "errors": 0}
    for _, target, why in due:
        if len(summary["profiled"]) >= max_tables or time.monotonic() - started > deadline_s:
            summary["skipped"].append(target["table"])
            continue
        try:
            if profile_table(db, target["table"], source=target["source"]):
                summary["profiled"].append(target["table"])
        except Exception as e:
            summary["errors"] += 1
            logger.warning(f"Scheduled profile of {target['table']} failed ({why}): {e}")
            db.rollback()
    logger.info(f"Scheduled catalog profiling: {len(summary['profiled'])} profiled, "
                f"{len(summary['skipped'])} left for the next run, {summary['errors']} errors")
    return summary


def scheduled_catalog_profiling():
    """Entry point for the APScheduler job (SPEC_144)."""
    from app.core.database import get_session_factory

    db = get_session_factory()()
    try:
        profile_stale_catalog_tables(db)
    except Exception as e:
        logger.error(f"Scheduled catalog profiling failed: {e}")
    finally:
        db.close()


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
