"""
Economic Data Quality & Governance API endpoints.

Surfaces DQ health, freshness compliance, series gaps, and revision tracking
for FRED, BLS, BEA, and Census data.

Router prefix: /econ-dq
Tag: Economic Data Quality
"""
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.core.database import get_db

router = APIRouter(prefix="/econ-dq", tags=["Economic Data Quality"])


# ---------------------------------------------------------------------------
# Internal helper
# ---------------------------------------------------------------------------

def _safe_query(db, sql: str, params: dict = None):
    """Execute SQL and return rows, or [] on any exception."""
    if params is None:
        params = {}
    try:
        result = db.execute(text(sql), params)
        return result.fetchall()
    except Exception:
        db.rollback()
        return []


# ---------------------------------------------------------------------------
# Known economic tables
# ---------------------------------------------------------------------------

ECON_TABLES = [
    "fred_interest_rates",
    "fred_economic_indicators",
    "fred_housing_market",
    "fred_consumer_sentiment",
    "bls_jolts",
    "bls_ces_employment",
    "bls_cps_labor_force",
    "bls_cpi",
    "bls_laus_state",
    "bea_regional",
    "bea_nipa",
]


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/health")
def get_econ_dq_health(db: Session = Depends(get_db)):
    """DQ scores for all economic data tables."""
    results = []
    for table in ECON_TABLES:
        exists_rows = _safe_query(
            db,
            "SELECT 1 FROM information_schema.tables WHERE table_name = :t",
            {"t": table},
        )
        if not exists_rows:
            results.append({"table": table, "status": "not_ingested", "score": None})
            continue

        count_rows = _safe_query(db, f"SELECT COUNT(*) FROM {table}", {})
        row_count = count_rows[0][0] if count_rows else 0

        # Pull latest DQ score from data_quality_reports if available
        score_rows = _safe_query(db, """
            SELECT quality_score FROM data_quality_reports
            WHERE source_key LIKE :pattern
            ORDER BY created_at DESC LIMIT 1
        """, {"pattern": f"%{table}%"})
        score = float(score_rows[0][0]) if score_rows and score_rows[0][0] is not None else None

        results.append({
            "table": table,
            "status": "ok" if row_count > 0 else "empty",
            "row_count": row_count,
            "quality_score": score,
        })
    return {"tables": results, "total": len(results)}


@router.get("/freshness")
def get_econ_freshness(db: Session = Depends(get_db)):
    """Per-table freshness status vs. release calendar SLA."""
    from app.services.econ_release_calendar import get_release_config
    from datetime import datetime, date

    results = []
    for table in ECON_TABLES:
        rc = get_release_config(table)
        latest_date = None

        date_rows = _safe_query(db, f"SELECT MAX(date) FROM {table}", {})
        if date_rows and date_rows[0][0] is not None:
            latest_date = date_rows[0][0]
            if hasattr(latest_date, "date"):
                latest_date = latest_date.date()
        else:
            year_rows = _safe_query(db, f"SELECT MAX(year) FROM {table}", {})
            if year_rows and year_rows[0][0] is not None:
                latest_date = date(year_rows[0][0], 12, 31)

        if latest_date is not None:
            age_days = (datetime.now().date() - latest_date).days
            is_stale = age_days > rc.expected_lag_days * rc.sla_multiplier
        else:
            age_days = None
            is_stale = None

        results.append({
            "table": table,
            "latest_date": str(latest_date) if latest_date else None,
            "age_days": age_days,
            "expected_lag_days": rc.expected_lag_days,
            "frequency": rc.frequency,
            "is_stale": is_stale,
            "sla_description": rc.description,
        })
    return {"sources": results}


@router.get("/revisions")
def get_econ_revisions(limit: int = 50, db: Session = Depends(get_db)):
    """Recent historical data revisions detected across economic sources."""
    rows = _safe_query(db, """
        SELECT source, table_name, series_id, geo_fips, period,
               old_value, new_value, revision_pct, detected_at
        FROM econ_data_revisions
        ORDER BY detected_at DESC
        LIMIT :limit
    """, {"limit": limit})

    if not rows:
        return {"status": "ok", "count": 0, "revisions": []}

    return {
        "status": "ok",
        "count": len(rows),
        "revisions": [dict(r._mapping) for r in rows],
    }


@router.get("/coherence")
def get_cross_source_coherence(db: Session = Depends(get_db)):
    """Cross-source coherence: FRED UNRATE vs BLS LNS14000000 for overlapping periods."""
    rows = _safe_query(db, """
        SELECT f.date, f.value as fred_unrate, b.value as bls_unrate,
               ABS(f.value - b.value) as divergence
        FROM fred_economic_indicators f
        JOIN bls_cps_labor_force b
          ON b.year = EXTRACT(YEAR FROM f.date)::INTEGER
         AND b.period = 'M' || LPAD(EXTRACT(MONTH FROM f.date)::TEXT, 2, '0')
         AND b.series_id = 'LNS14000000'
        WHERE f.series_id = 'UNRATE'
          AND ABS(f.value - b.value) > 0.1
        ORDER BY f.date DESC
        LIMIT 10
    """, {})

    divergences = []
    if rows:
        divergences = [dict(r._mapping) for r in rows]

    return {
        "status": "ok",
        "series_compared": "FRED UNRATE vs BLS LNS14000000",
        "threshold_pp": 0.1,
        "divergences": divergences,
        "coherent": len(divergences) == 0,
    }


@router.post("/run/{table_name}")
def run_econ_dq(table_name: str, db: Session = Depends(get_db)):
    """Trigger DQ check for a specific economic table by name."""
    from app.services.econ_dq_service import EconDQService

    # Only allow known table names to prevent SQL injection
    if table_name not in ECON_TABLES:
        return {
            "error": f"Unknown table '{table_name}'. "
                     f"Allowed: {', '.join(ECON_TABLES)}"
        }

    service = EconDQService(db=db, table_name=table_name)
    report = service.run_checks()
    return {
        "table": table_name,
        "score": report.quality_score,
        "completeness": report.completeness,
        "freshness": report.freshness,
        "validity": report.validity,
        "consistency": report.consistency,
        "issues": [
            {
                "check": i.check,
                "dimension": i.dimension,
                "severity": i.severity,
                "message": i.message,
                "count": i.count,
            }
            for i in report.issues
        ],
    }


@router.get("/sla")
def get_sla_compliance(db: Session = Depends(get_db)):
    """SLA compliance summary — which economic tables are within their release window."""
    from app.services.econ_release_calendar import get_release_config
    from datetime import datetime, date

    compliant = []
    stale = []
    unknown = []

    for table in ECON_TABLES:
        rc = get_release_config(table)
        latest_date = None

        date_rows = _safe_query(db, f"SELECT MAX(date) FROM {table}", {})
        if date_rows and date_rows[0][0] is not None:
            latest_date = date_rows[0][0]
            if hasattr(latest_date, "date"):
                latest_date = latest_date.date()
        else:
            year_rows = _safe_query(db, f"SELECT MAX(year) FROM {table}", {})
            if year_rows and year_rows[0][0] is not None:
                latest_date = date(year_rows[0][0], 12, 31)

        if latest_date is None:
            unknown.append({"table": table, "reason": "no_data"})
            continue

        age_days = (datetime.now().date() - latest_date).days
        is_stale = age_days > rc.expected_lag_days * rc.sla_multiplier

        entry = {
            "table": table,
            "age_days": age_days,
            "expected_lag_days": rc.expected_lag_days,
            "frequency": rc.frequency,
        }
        (stale if is_stale else compliant).append(entry)

    return {
        "compliant_count": len(compliant),
        "stale_count": len(stale),
        "unknown_count": len(unknown),
        "compliant": compliant,
        "stale": stale,
        "unknown": unknown,
    }
