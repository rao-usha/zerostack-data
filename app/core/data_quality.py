"""
Data quality validation module.

Provides post-ingestion validation to ensure data quality standards are met.
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.core.models import IngestionJob, JobStatus

logger = logging.getLogger(__name__)


class DataQualityResult:
    """Result of a data quality validation."""

    def __init__(
        self,
        check_name: str,
        passed: bool,
        message: str,
        severity: str = "warning",
        details: Optional[Dict[str, Any]] = None,
    ):
        self.check_name = check_name
        self.passed = passed
        self.message = message
        self.severity = severity  # "info", "warning", "error"
        self.details = details or {}
        self.timestamp = datetime.utcnow()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "check_name": self.check_name,
            "passed": self.passed,
            "message": self.message,
            "severity": self.severity,
            "details": self.details,
            "timestamp": self.timestamp.isoformat(),
        }


class DataQualityValidator:
    """
    Validates data quality after ingestion.

    Performs checks like:
    - Row count validation
    - Null value detection in required columns
    - Duplicate detection
    - Range validation for numeric fields
    - Freshness validation
    """

    def __init__(self, db: Session):
        self.db = db
        self.results: List[DataQualityResult] = []

    def validate_job(
        self,
        job_id: int,
        table_name: str,
        expected_min_rows: int = 1,
        required_columns: Optional[List[str]] = None,
        unique_columns: Optional[List[str]] = None,
        numeric_ranges: Optional[Dict[str, tuple]] = None,
    ) -> Dict[str, Any]:
        """
        Run all validation checks for a completed ingestion job.

        Args:
            job_id: The ingestion job ID
            table_name: The table that was populated
            expected_min_rows: Minimum expected row count
            required_columns: Columns that should not have nulls
            unique_columns: Columns that should be unique (for duplicate check)
            numeric_ranges: Dict of column_name -> (min, max) tuples

        Returns:
            Dictionary with validation results
        """
        self.results = []

        # Get job
        job = self.db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if not job:
            return {
                "job_id": job_id,
                "status": "error",
                "message": "Job not found",
                "checks": [],
            }

        # Run checks
        self._check_row_count(table_name, expected_min_rows, job.rows_inserted)

        if required_columns:
            self._check_null_values(table_name, required_columns)

        if unique_columns:
            self._check_duplicates(table_name, unique_columns)

        if numeric_ranges:
            self._check_numeric_ranges(table_name, numeric_ranges)

        # Determine overall status
        failed_errors = [
            r for r in self.results if not r.passed and r.severity == "error"
        ]
        failed_warnings = [
            r for r in self.results if not r.passed and r.severity == "warning"
        ]

        if failed_errors:
            overall_status = "failed"
        elif failed_warnings:
            overall_status = "warning"
        else:
            overall_status = "passed"

        return {
            "job_id": job_id,
            "table_name": table_name,
            "status": overall_status,
            "total_checks": len(self.results),
            "passed_checks": len([r for r in self.results if r.passed]),
            "failed_checks": len([r for r in self.results if not r.passed]),
            "checks": [r.to_dict() for r in self.results],
        }

    def _check_row_count(
        self, table_name: str, expected_min: int, actual_rows: Optional[int]
    ) -> None:
        """Check if row count meets minimum expectation."""
        if actual_rows is None:
            result = self.db.execute(
                text(f"SELECT COUNT(*) FROM {table_name}")
            ).scalar()
            actual_rows = result or 0

        passed = actual_rows >= expected_min

        self.results.append(
            DataQualityResult(
                check_name="row_count",
                passed=passed,
                message=f"Row count: {actual_rows} (expected >= {expected_min})",
                severity="error" if not passed else "info",
                details={"actual_rows": actual_rows, "expected_min_rows": expected_min},
            )
        )

    def _check_null_values(self, table_name: str, required_columns: List[str]) -> None:
        """Check for null values in required columns."""
        for column in required_columns:
            try:
                result = self.db.execute(
                    text(f"SELECT COUNT(*) FROM {table_name} WHERE {column} IS NULL")
                ).scalar()
                null_count = result or 0

                passed = null_count == 0

                self.results.append(
                    DataQualityResult(
                        check_name=f"null_check_{column}",
                        passed=passed,
                        message=f"Column '{column}': {null_count} null values",
                        severity="warning" if not passed else "info",
                        details={"column": column, "null_count": null_count},
                    )
                )
            except Exception as e:
                self.results.append(
                    DataQualityResult(
                        check_name=f"null_check_{column}",
                        passed=False,
                        message=f"Error checking column '{column}': {str(e)}",
                        severity="error",
                        details={"column": column, "error": str(e)},
                    )
                )

    def _check_duplicates(self, table_name: str, unique_columns: List[str]) -> None:
        """Check for duplicate records based on unique columns."""
        columns_str = ", ".join(unique_columns)

        try:
            query = f"""
                SELECT COUNT(*) as dup_count FROM (
                    SELECT {columns_str}, COUNT(*) as cnt
                    FROM {table_name}
                    GROUP BY {columns_str}
                    HAVING COUNT(*) > 1
                ) dups
            """
            result = self.db.execute(text(query)).scalar()
            dup_groups = result or 0

            passed = dup_groups == 0

            self.results.append(
                DataQualityResult(
                    check_name="duplicate_check",
                    passed=passed,
                    message=f"Duplicate groups on ({columns_str}): {dup_groups}",
                    severity="warning" if not passed else "info",
                    details={
                        "unique_columns": unique_columns,
                        "duplicate_groups": dup_groups,
                    },
                )
            )
        except Exception as e:
            self.results.append(
                DataQualityResult(
                    check_name="duplicate_check",
                    passed=False,
                    message=f"Error checking duplicates: {str(e)}",
                    severity="error",
                    details={"unique_columns": unique_columns, "error": str(e)},
                )
            )

    def _check_numeric_ranges(
        self, table_name: str, numeric_ranges: Dict[str, tuple]
    ) -> None:
        """Check if numeric values fall within expected ranges."""
        for column, (min_val, max_val) in numeric_ranges.items():
            try:
                query = f"""
                    SELECT
                        COUNT(*) as total,
                        SUM(CASE WHEN {column} < {min_val} OR {column} > {max_val} THEN 1 ELSE 0 END) as out_of_range,
                        MIN({column}) as min_val,
                        MAX({column}) as max_val
                    FROM {table_name}
                    WHERE {column} IS NOT NULL
                """
                result = self.db.execute(text(query)).fetchone()

                total = result[0] or 0
                out_of_range = result[1] or 0
                actual_min = result[2]
                actual_max = result[3]

                passed = out_of_range == 0

                self.results.append(
                    DataQualityResult(
                        check_name=f"range_check_{column}",
                        passed=passed,
                        message=f"Column '{column}': {out_of_range}/{total} values out of range [{min_val}, {max_val}]",
                        severity="warning" if not passed else "info",
                        details={
                            "column": column,
                            "expected_range": [min_val, max_val],
                            "actual_range": [actual_min, actual_max],
                            "out_of_range_count": out_of_range,
                            "total_count": total,
                        },
                    )
                )
            except Exception as e:
                self.results.append(
                    DataQualityResult(
                        check_name=f"range_check_{column}",
                        passed=False,
                        message=f"Error checking range for '{column}': {str(e)}",
                        severity="error",
                        details={"column": column, "error": str(e)},
                    )
                )


def validate_ingestion_job(
    db: Session,
    job_id: int,
    table_name: str,
    validation_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Convenience function to validate an ingestion job.

    Args:
        db: Database session
        job_id: Ingestion job ID
        table_name: Table that was populated
        validation_config: Optional configuration with:
            - expected_min_rows: int
            - required_columns: List[str]
            - unique_columns: List[str]
            - numeric_ranges: Dict[str, tuple]

    Returns:
        Validation results
    """
    config = validation_config or {}
    validator = DataQualityValidator(db)

    return validator.validate_job(
        job_id=job_id,
        table_name=table_name,
        expected_min_rows=config.get("expected_min_rows", 1),
        required_columns=config.get("required_columns"),
        unique_columns=config.get("unique_columns"),
        numeric_ranges=config.get("numeric_ranges"),
    )


# Default validation configs by source
DEFAULT_VALIDATION_CONFIGS = {
    "fdic": {
        "institutions": {
            "expected_min_rows": 1,
            "required_columns": ["cert", "name", "stalp"],
            "unique_columns": ["cert"],
        },
        "financials": {
            "expected_min_rows": 1,
            "required_columns": ["cert", "repdte"],
            "unique_columns": ["cert", "repdte"],
        },
        "failed_banks": {
            "expected_min_rows": 1,
            "required_columns": ["cert", "faildate"],
            "unique_columns": ["cert"],
        },
    },
    "census": {
        "expected_min_rows": 1,
        "required_columns": ["state"],
    },
    "fred": {
        "expected_min_rows": 1,
        "required_columns": ["series_id", "date", "value"],
    },
    "bls": {
        "expected_min_rows": 1,
        "required_columns": ["series_id", "year", "period"],
    },
}


def get_default_validation_config(
    source: str, dataset: Optional[str] = None
) -> Dict[str, Any]:
    """Get default validation config for a source."""
    config = DEFAULT_VALIDATION_CONFIGS.get(source, {})

    if isinstance(config, dict) and dataset and dataset in config:
        return config[dataset]

    return config if isinstance(config, dict) else {}
