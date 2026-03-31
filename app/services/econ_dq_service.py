"""
Economic Data Quality Service.

Domain-specific DQ provider for economic time series data (FRED, BLS, BEA, Census).
Extends BaseQualityProvider with checks tuned for time series failure modes:
  - Release lag violations (stale data vs. known publish schedule)
  - Series gaps (missing months in a continuous series)
  - Geographic coverage (BEA regional / Census ACS state tables)
  - Statistical outliers (4-sigma detection over rolling 5-year window)
  - Hard-bound violations (unemployment can't be negative, etc.)
  - Cross-source coherence (FRED UNRATE vs BLS LNS14000000)

Freshness is weighted at 40% (higher than the default 25%) because economic
signals are time-sensitive — a stale FRED rate series is worse than a stale
people record.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import List, Optional

from app.core.dq_base import (
    BaseQualityProvider,
    QualityIssue,
    QualityReport,
    _penalty,
    compute_quality_score,
)
from app.services.econ_release_calendar import get_release_config

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Module-level helper — wraps every DB call so a bad table never crashes the hook
# ---------------------------------------------------------------------------

def _safe_query(db, sql: str, params: dict):
    """Execute a SQL query and return rows, or [] on any exception."""
    try:
        from sqlalchemy import text
        result = db.execute(text(sql), params)
        return result.fetchall()
    except Exception:
        db.rollback()
        return []


# ---------------------------------------------------------------------------
# Hard bounds by series ID
# ---------------------------------------------------------------------------

SERIES_BOUNDS = {
    "UNRATE":      (0, 100),    # unemployment %
    "LNS14000000": (0, 100),
    "CPIAUCSL":    (1, 1000),   # CPI index level (can't be zero or negative)
    "DFF":         (0, 30),     # Fed Funds Rate %
    "DGS10":       (0, 30),     # 10-year Treasury yield %
    "GDPC1":       (0, None),   # Real GDP must be positive
}


# ---------------------------------------------------------------------------
# Main service
# ---------------------------------------------------------------------------

class EconDQService(BaseQualityProvider):
    """Data quality provider for economic time series data (FRED, BLS, BEA, Census)."""

    dataset = "econ"

    ECON_SCORING_WEIGHTS = {
        "completeness": 0.25,
        "freshness": 0.40,   # higher than default — economic signals are time-sensitive
        "validity": 0.25,
        "consistency": 0.10,
    }

    def __init__(self, db, table_name: str, series_type: str = "auto"):
        self.db = db
        self.table_name = table_name
        self.series_type = series_type
        self.release_config = get_release_config(table_name)

    # ------------------------------------------------------------------
    # BaseQualityProvider abstract methods
    # (EconDQService is table-scoped, not entity-scoped like PEDQService,
    #  so run() and run_all() delegate to the table-level run_checks())
    # ------------------------------------------------------------------

    def run(self, entity_id: int, db) -> QualityReport:
        """Run all checks. entity_id is unused; table_name drives the check."""
        return self.run_checks()

    def run_all(self, db, limit=None) -> list:
        """Run checks for this single table (no multi-entity iteration needed)."""
        return [self.run_checks()]

    def get_scoring_weights(self):
        return self.ECON_SCORING_WEIGHTS

    def run_checks(self) -> QualityReport:
        """Run all 6 checks and return a QualityReport."""
        issues: List[QualityIssue] = []
        issues.extend(self._check_series_stale())
        issues.extend(self._check_series_gap())
        issues.extend(self._check_geographic_coverage())
        issues.extend(self._check_outlier_values())
        issues.extend(self._check_invalid_ranges())
        issues.extend(self._check_cross_source_coherence())
        return self._build_report(issues)

    # ------------------------------------------------------------------
    # Check 1: Staleness vs. release calendar
    # ------------------------------------------------------------------

    def _check_series_stale(self) -> List[QualityIssue]:
        """Check if latest data is older than expected release lag."""
        from datetime import date as date_type
        issues = []

        rc = self.release_config
        expected_stale_days = rc.expected_lag_days * rc.sla_multiplier

        latest_date = None

        if self._has_date_column():
            rows = _safe_query(
                self.db,
                f"SELECT MAX(date) as latest FROM {self.table_name}",
                {},
            )
            if rows and rows[0][0] is not None:
                latest_date = rows[0][0]
        elif self._has_year_period_columns():
            rows = _safe_query(
                self.db,
                f"SELECT MAX(year) as latest_year, MAX(period) as latest_period "
                f"FROM {self.table_name}",
                {},
            )
            if rows and rows[0][0] is not None:
                latest_year = rows[0][0]
                latest_period = rows[0][1] or "M12"
                try:
                    month = int(latest_period.replace("M", "")) if latest_period.startswith("M") else 12
                    latest_date = date_type(latest_year, month, 1)
                except (ValueError, TypeError):
                    latest_date = date_type(latest_year, 12, 1)

        if latest_date is None:
            return issues

        # Handle both date and datetime objects
        if hasattr(latest_date, "date"):
            latest_date = latest_date.date()

        age_days = (datetime.now().date() - latest_date).days

        if age_days > expected_stale_days * 2:
            issues.append(QualityIssue(
                check="series_stale",
                dimension="freshness",
                severity="ERROR",
                message=(
                    f"{self.table_name} last updated {age_days}d ago — "
                    f"expected ≤{rc.expected_lag_days}d ({rc.description})"
                ),
            ))
        elif age_days > expected_stale_days:
            issues.append(QualityIssue(
                check="series_stale",
                dimension="freshness",
                severity="WARNING",
                message=(
                    f"{self.table_name} may be stale: {age_days}d old "
                    f"(expected ≤{rc.expected_lag_days}d)"
                ),
            ))
        return issues

    # ------------------------------------------------------------------
    # Check 2: Series gap detection
    # ------------------------------------------------------------------

    def _check_series_gap(self) -> List[QualityIssue]:
        """Check for month-over-month gaps in continuous series."""
        issues = []

        if self._has_date_column():
            rows = _safe_query(self.db, f"""
                SELECT series_id, gap_days FROM (
                    SELECT series_id,
                           date - LAG(date) OVER (PARTITION BY series_id ORDER BY date) AS gap_days
                    FROM {self.table_name}
                ) sub
                WHERE gap_days > 45
                LIMIT 20
            """, {})

            if rows:
                max_gap = max(r[1] for r in rows if r[1] is not None)
                affected_series = list({r[0] for r in rows})[:5]
                severity = "ERROR" if max_gap > 92 else "WARNING"
                issues.append(QualityIssue(
                    check="series_gap",
                    dimension="completeness",
                    severity=severity,
                    message=(
                        f"{self.table_name}: {len(rows)} gap(s) detected "
                        f"(max {max_gap}d) in series: {', '.join(str(s) for s in affected_series)}"
                    ),
                    count=len(rows),
                ))

        elif self._has_year_period_columns():
            # For BLS tables: check sequential periods within each series
            rows = _safe_query(self.db, f"""
                SELECT series_id, COUNT(DISTINCT period) as period_count,
                       MAX(year) - MIN(year) as year_span
                FROM {self.table_name}
                WHERE period LIKE 'M%%'
                GROUP BY series_id
                HAVING (MAX(year) - MIN(year) + 1) * 12 > COUNT(DISTINCT period) + 2
                LIMIT 10
            """, {})

            if rows:
                issues.append(QualityIssue(
                    check="series_gap",
                    dimension="completeness",
                    severity="WARNING",
                    message=(
                        f"{self.table_name}: {len(rows)} series have missing "
                        f"monthly periods in the expected range"
                    ),
                    count=len(rows),
                ))

        return issues

    # ------------------------------------------------------------------
    # Check 3: Geographic coverage
    # ------------------------------------------------------------------

    def _check_geographic_coverage(self) -> List[QualityIssue]:
        """Check state/geo coverage for regional tables."""
        issues = []

        if not self._has_geo_fips_column():
            return issues

        # Detect the time column name
        time_col = None
        for candidate in ("year", "time_period", "date"):
            rows = _safe_query(self.db,
                "SELECT 1 FROM information_schema.columns "
                "WHERE table_name = :t AND column_name = :c",
                {"t": self.table_name, "c": candidate},
            )
            if rows:
                time_col = candidate
                break

        if not time_col:
            return issues

        rows = _safe_query(self.db, f"""
            SELECT {time_col}, COUNT(DISTINCT geo_fips) as state_count
            FROM {self.table_name}
            GROUP BY {time_col}
            ORDER BY {time_col} DESC
            LIMIT 5
        """, {})

        if not rows:
            return issues

        latest_count = rows[0][1] if rows[0][1] else 0
        latest_period = rows[0][0]

        if latest_count < 45:
            issues.append(QualityIssue(
                check="geographic_coverage",
                dimension="completeness",
                severity="ERROR",
                message=(
                    f"{self.table_name}: only {latest_count} distinct geo_fips "
                    f"for {latest_period} — expected ≥50 states+DC"
                ),
                count=latest_count,
            ))
        elif latest_count < 51:
            issues.append(QualityIssue(
                check="geographic_coverage",
                dimension="completeness",
                severity="WARNING",
                message=(
                    f"{self.table_name}: {latest_count}/51 states+DC present "
                    f"for {latest_period}"
                ),
                count=latest_count,
            ))

        return issues

    # ------------------------------------------------------------------
    # Check 4: Statistical outlier detection (4-sigma)
    # ------------------------------------------------------------------

    def _check_outlier_values(self) -> List[QualityIssue]:
        """Flag series where max deviates >4 standard deviations from mean."""
        issues = []

        value_col = self._get_value_column()
        if not value_col:
            return issues

        date_filter = ""
        if self._has_date_column():
            date_filter = "WHERE date >= NOW() - INTERVAL '5 years'"

        rows = _safe_query(self.db, f"""
            SELECT series_id,
                   AVG({value_col}) as mean_val,
                   STDDEV({value_col}) as std_val,
                   MAX({value_col}) as max_val,
                   MIN({value_col}) as min_val
            FROM {self.table_name}
            {date_filter}
            GROUP BY series_id
            HAVING STDDEV({value_col}) > 0
        """, {})

        outlier_series = []
        for row in rows:
            series_id, mean_val, std_val, max_val, min_val = row
            if std_val and std_val > 0:
                max_zscore = (max_val - mean_val) / std_val if mean_val is not None else 0
                min_zscore = (mean_val - min_val) / std_val if mean_val is not None else 0
                if max(abs(max_zscore), abs(min_zscore)) > 4:
                    outlier_series.append(str(series_id))

        if outlier_series:
            issues.append(QualityIssue(
                check="outlier_value",
                dimension="validity",
                severity="WARNING",
                message=(
                    f"{self.table_name}: {len(outlier_series)} series with "
                    f"potential outliers (>4σ): {', '.join(outlier_series[:5])}"
                ),
                count=len(outlier_series),
            ))

        return issues

    # ------------------------------------------------------------------
    # Check 5: Hard-bound violations
    # ------------------------------------------------------------------

    def _check_invalid_ranges(self) -> List[QualityIssue]:
        """Check hardcoded bounds for known series."""
        issues = []

        value_col = self._get_value_column()
        if not value_col:
            return issues

        violations = []
        for series_id, (low, high) in SERIES_BOUNDS.items():
            conditions = []
            if low is not None:
                conditions.append(f"{value_col} < {low}")
            if high is not None:
                conditions.append(f"{value_col} > {high}")
            if not conditions:
                continue

            where_clause = " OR ".join(conditions)
            rows = _safe_query(self.db, f"""
                SELECT series_id, {value_col}
                FROM {self.table_name}
                WHERE series_id = :sid AND ({where_clause})
                LIMIT 3
            """, {"sid": series_id})

            if rows:
                for r in rows:
                    violations.append(f"{r[0]}={r[1]}")

        if violations:
            issues.append(QualityIssue(
                check="invalid_range",
                dimension="validity",
                severity="ERROR",
                message=(
                    f"{self.table_name}: {len(violations)} out-of-bounds value(s): "
                    f"{', '.join(violations[:5])}"
                ),
                count=len(violations),
            ))

        return issues

    # ------------------------------------------------------------------
    # Check 6: Cross-source coherence (FRED UNRATE vs BLS LNS14000000)
    # ------------------------------------------------------------------

    def _check_cross_source_coherence(self) -> List[QualityIssue]:
        """Compare FRED UNRATE vs BLS LNS14000000 for overlapping periods."""
        issues = []

        # Only run from a FRED or BLS table context
        if not (
            self.table_name.startswith("fred_")
            or self.table_name.startswith("bls_")
        ):
            return issues

        # Check both source tables exist
        fred_rows = _safe_query(self.db,
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_name = 'fred_economic_indicators'",
            {})
        bls_rows = _safe_query(self.db,
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_name = 'bls_cps_labor_force'",
            {})

        if not fred_rows or not bls_rows:
            return issues

        rows = _safe_query(self.db, """
            SELECT f.date, f.value as fred_val, b.value as bls_val,
                   ABS(f.value - b.value) as divergence
            FROM fred_economic_indicators f
            JOIN bls_cps_labor_force b
              ON b.year = EXTRACT(YEAR FROM f.date)::INTEGER
             AND b.period = 'M' || LPAD(EXTRACT(MONTH FROM f.date)::TEXT, 2, '0')
             AND b.series_id = 'LNS14000000'
            WHERE f.series_id = 'UNRATE'
              AND ABS(f.value - b.value) > 0.2
            ORDER BY f.date DESC
            LIMIT 5
        """, {})

        if rows:
            issues.append(QualityIssue(
                check="cross_source_coherence",
                dimension="consistency",
                severity="WARNING",
                message=(
                    f"FRED UNRATE and BLS LNS14000000 diverge by >0.2pp "
                    f"in {len(rows)} period(s)"
                ),
                count=len(rows),
            ))

        return issues

    # ------------------------------------------------------------------
    # Helper: schema introspection
    # ------------------------------------------------------------------

    def _has_date_column(self) -> bool:
        rows = _safe_query(self.db,
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_name = :t AND column_name = 'date'",
            {"t": self.table_name})
        return len(rows) > 0

    def _has_year_period_columns(self) -> bool:
        rows = _safe_query(self.db,
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_name = :t AND column_name = 'year'",
            {"t": self.table_name})
        return len(rows) > 0

    def _has_geo_fips_column(self) -> bool:
        rows = _safe_query(self.db,
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_name = :t AND column_name = 'geo_fips'",
            {"t": self.table_name})
        return len(rows) > 0

    def _get_value_column(self) -> Optional[str]:
        """Return the name of the value column, or None if not found."""
        for candidate in ("value", "data_value"):
            rows = _safe_query(self.db,
                "SELECT 1 FROM information_schema.columns "
                "WHERE table_name = :t AND column_name = :c",
                {"t": self.table_name, "c": candidate})
            if rows:
                return candidate
        return None

    # ------------------------------------------------------------------
    # Report builder
    # ------------------------------------------------------------------

    def _build_report(self, issues: List[QualityIssue]) -> QualityReport:
        w = self.ECON_SCORING_WEIGHTS
        completeness = _penalty(issues, "completeness")
        freshness    = _penalty(issues, "freshness")
        validity     = _penalty(issues, "validity")
        consistency  = _penalty(issues, "consistency")

        # Weighted scoring override (freshness 40%, not the default 25%)
        raw_score = (
            w["completeness"] * completeness
            + w["freshness"]   * freshness
            + w["validity"]    * validity
            + w["consistency"] * consistency
        )
        score = max(0, min(100, round(raw_score)))

        return QualityReport(
            entity_id=0,
            entity_name=self.table_name,
            dataset="econ",
            quality_score=score,
            completeness=completeness,
            freshness=freshness,
            validity=validity,
            consistency=consistency,
            issues=issues,
        )
