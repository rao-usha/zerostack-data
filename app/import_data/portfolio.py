"""
Bulk Portfolio Import Engine.

Handles CSV/Excel file parsing, validation, and import
of portfolio data into the database.
"""

import csv
import io
import json
import logging
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
from difflib import SequenceMatcher

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


# Required and optional columns
REQUIRED_COLUMNS = {"company_name", "investor_name", "investor_type"}
OPTIONAL_COLUMNS = {
    "company_website",
    "company_industry",
    "company_stage",
    "company_location",
    "investment_date",
    "investment_amount",
    "shares_held",
    "market_value",
    "ownership_percentage",
    "investment_type",
}
VALID_INVESTOR_TYPES = {"lp", "family_office"}


class ValidationError:
    """Represents a validation error for a row."""

    def __init__(self, row: int, column: str, error: str):
        self.row = row
        self.column = column
        self.error = error

    def to_dict(self) -> Dict:
        return {"row": self.row, "column": self.column, "error": self.error}


class ValidationWarning:
    """Represents a validation warning for a row."""

    def __init__(self, row: int, message: str):
        self.row = row
        self.message = message

    def to_dict(self) -> Dict:
        return {"row": self.row, "message": self.message}


class PortfolioImporter:
    """
    Bulk portfolio import engine.

    Handles file parsing, validation, and database import.
    """

    def __init__(self, db: Session):
        self.db = db
        self._investors_cache: Dict[str, Tuple[int, str]] = {}
        self._companies_cache: Dict[str, str] = {}

    def _ensure_table(self) -> None:
        """Ensure import tracking table exists."""
        create_table = text("""
            CREATE TABLE IF NOT EXISTS portfolio_imports (
                id SERIAL PRIMARY KEY,
                filename VARCHAR(255) NOT NULL,
                file_size INTEGER,
                row_count INTEGER,
                status VARCHAR(20) DEFAULT 'pending',
                valid_rows INTEGER DEFAULT 0,
                invalid_rows INTEGER DEFAULT 0,
                validation_errors JSONB,
                imported_count INTEGER DEFAULT 0,
                skipped_count INTEGER DEFAULT 0,
                error_count INTEGER DEFAULT 0,
                rollback_data JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                started_at TIMESTAMP,
                completed_at TIMESTAMP
            )
        """)
        try:
            self.db.execute(create_table)
            self.db.commit()
        except Exception as e:
            logger.warning(f"Table creation warning: {e}")
            self.db.rollback()

    def create_import(self, filename: str, file_size: int, row_count: int) -> int:
        """Create a new import job record."""
        self._ensure_table()

        query = text("""
            INSERT INTO portfolio_imports (filename, file_size, row_count, status)
            VALUES (:filename, :file_size, :row_count, 'pending')
            RETURNING id
        """)
        result = self.db.execute(
            query,
            {
                "filename": filename,
                "file_size": file_size,
                "row_count": row_count,
            },
        )
        self.db.commit()
        row = result.fetchone()
        return row[0] if row else 0

    def update_import(self, import_id: int, **kwargs) -> None:
        """Update import job fields."""
        # Build dynamic update query
        set_clauses = []
        params = {"import_id": import_id}

        for key, value in kwargs.items():
            if key in (
                "status",
                "valid_rows",
                "invalid_rows",
                "validation_errors",
                "imported_count",
                "skipped_count",
                "error_count",
                "rollback_data",
                "started_at",
                "completed_at",
            ):
                set_clauses.append(f"{key} = :{key}")
                # Handle JSONB fields
                if key in ("validation_errors", "rollback_data") and isinstance(
                    value, (dict, list)
                ):
                    params[key] = json.dumps(value)
                else:
                    params[key] = value

        if not set_clauses:
            return

        query = text(f"""
            UPDATE portfolio_imports
            SET {', '.join(set_clauses)}
            WHERE id = :import_id
        """)
        self.db.execute(query, params)
        self.db.commit()

    def get_import(self, import_id: int) -> Optional[Dict]:
        """Get import job by ID."""
        self._ensure_table()

        query = text("""
            SELECT * FROM portfolio_imports WHERE id = :import_id
        """)
        result = self.db.execute(query, {"import_id": import_id})
        row = result.mappings().fetchone()

        if not row:
            return None

        return dict(row)

    def list_imports(self, limit: int = 50, offset: int = 0) -> List[Dict]:
        """List import jobs."""
        self._ensure_table()

        query = text("""
            SELECT id, filename, file_size, row_count, status,
                   valid_rows, invalid_rows, imported_count,
                   created_at, completed_at
            FROM portfolio_imports
            ORDER BY created_at DESC
            LIMIT :limit OFFSET :offset
        """)
        result = self.db.execute(query, {"limit": limit, "offset": offset})
        return [dict(row) for row in result.mappings()]

    def parse_csv(self, content: bytes) -> Tuple[List[Dict], List[str]]:
        """
        Parse CSV content into rows.

        Returns:
            Tuple of (rows, column_names)
        """
        try:
            # Try to decode as UTF-8, fallback to latin-1
            try:
                text_content = content.decode("utf-8")
            except UnicodeDecodeError:
                text_content = content.decode("latin-1")

            reader = csv.DictReader(io.StringIO(text_content))
            columns = reader.fieldnames or []
            rows = list(reader)

            # Normalize column names (lowercase, strip whitespace)
            normalized_rows = []
            for row in rows:
                normalized = {}
                for key, value in row.items():
                    if key:
                        normalized_key = key.lower().strip().replace(" ", "_")
                        normalized[normalized_key] = value.strip() if value else ""
                normalized_rows.append(normalized)

            return normalized_rows, [
                c.lower().strip().replace(" ", "_") for c in columns if c
            ]

        except Exception as e:
            logger.error(f"CSV parsing error: {e}")
            raise ValueError(f"Failed to parse CSV: {e}")

    def parse_excel(self, content: bytes) -> Tuple[List[Dict], List[str]]:
        """
        Parse Excel content into rows.

        Returns:
            Tuple of (rows, column_names)
        """
        try:
            import openpyxl
            from io import BytesIO

            wb = openpyxl.load_workbook(BytesIO(content), read_only=True)
            ws = wb.active

            rows_iter = ws.iter_rows(values_only=True)
            header = next(rows_iter, None)

            if not header:
                raise ValueError("Excel file has no header row")

            # Normalize column names
            columns = [
                str(c).lower().strip().replace(" ", "_") if c else f"col_{i}"
                for i, c in enumerate(header)
            ]

            rows = []
            for row_values in rows_iter:
                if any(v is not None for v in row_values):
                    row = {}
                    for i, value in enumerate(row_values):
                        if i < len(columns):
                            row[columns[i]] = (
                                str(value).strip() if value is not None else ""
                            )
                    rows.append(row)

            return rows, columns

        except ImportError:
            raise ValueError("Excel support requires openpyxl: pip install openpyxl")
        except Exception as e:
            logger.error(f"Excel parsing error: {e}")
            raise ValueError(f"Failed to parse Excel: {e}")

    def validate_columns(self, columns: List[str]) -> List[str]:
        """
        Validate that required columns are present.

        Returns list of missing required columns.
        """
        columns_set = set(columns)
        missing = REQUIRED_COLUMNS - columns_set
        return list(missing)

    def validate_row(
        self, row: Dict, row_num: int
    ) -> Tuple[List[ValidationError], List[ValidationWarning]]:
        """
        Validate a single row.

        Returns:
            Tuple of (errors, warnings)
        """
        errors = []
        warnings = []

        # Check required fields
        for col in REQUIRED_COLUMNS:
            if not row.get(col):
                errors.append(ValidationError(row_num, col, "Required field is empty"))

        # Validate investor_type
        investor_type = row.get("investor_type", "").lower()
        if investor_type and investor_type not in VALID_INVESTOR_TYPES:
            errors.append(
                ValidationError(
                    row_num,
                    "investor_type",
                    f"Invalid value '{investor_type}', must be 'lp' or 'family_office'",
                )
            )

        # Validate investment_date if present
        if row.get("investment_date"):
            try:
                self._parse_date(row["investment_date"])
            except ValueError:
                errors.append(
                    ValidationError(
                        row_num,
                        "investment_date",
                        f"Invalid date format '{row['investment_date']}'",
                    )
                )

        # Check if investor exists (warning if not)
        if row.get("investor_name") and not errors:
            investor_match = self._match_investor(row["investor_name"], investor_type)
            if not investor_match:
                warnings.append(
                    ValidationWarning(
                        row_num,
                        f"Investor '{row['investor_name']}' not found, will create new record",
                    )
                )

        return errors, warnings

    def _parse_date(self, date_str: str) -> datetime:
        """Parse date string in various formats."""
        formats = [
            "%Y-%m-%d",
            "%m/%d/%Y",
            "%d/%m/%Y",
            "%Y/%m/%d",
            "%m-%d-%Y",
            "%d-%m-%Y",
        ]
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        raise ValueError(f"Cannot parse date: {date_str}")

    def _similarity(self, a: str, b: str) -> float:
        """Calculate string similarity ratio."""
        return SequenceMatcher(None, a.lower(), b.lower()).ratio()

    def _load_investors_cache(self) -> None:
        """Load existing investors into cache for matching."""
        if self._investors_cache:
            return

        # Load LPs
        lp_query = text("SELECT id, name FROM lp_fund")
        result = self.db.execute(lp_query)
        for row in result.mappings():
            key = row["name"].lower()
            self._investors_cache[key] = (row["id"], "lp")

        # Load Family Offices
        fo_query = text("SELECT id, name FROM family_offices")
        result = self.db.execute(fo_query)
        for row in result.mappings():
            key = row["name"].lower()
            self._investors_cache[key] = (row["id"], "family_office")

    def _match_investor(
        self, name: str, investor_type: str
    ) -> Optional[Tuple[int, str]]:
        """
        Match investor name to existing record using fuzzy matching.

        Returns:
            Tuple of (investor_id, investor_type) or None
        """
        self._load_investors_cache()

        name_lower = name.lower()

        # Exact match first
        if name_lower in self._investors_cache:
            cached = self._investors_cache[name_lower]
            if not investor_type or cached[1] == investor_type:
                return cached

        # Fuzzy match
        best_match = None
        best_score = 0.85  # Minimum threshold

        for cached_name, (inv_id, inv_type) in self._investors_cache.items():
            if investor_type and inv_type != investor_type:
                continue
            score = self._similarity(name_lower, cached_name)
            if score > best_score:
                best_score = score
                best_match = (inv_id, inv_type)

        return best_match

    def _load_companies_cache(self) -> None:
        """Load existing companies into cache for matching."""
        if self._companies_cache:
            return

        query = text("SELECT DISTINCT company_name FROM portfolio_companies")
        result = self.db.execute(query)
        for row in result.mappings():
            self._companies_cache[row["company_name"].lower()] = row["company_name"]

    def _match_company(self, name: str) -> Optional[str]:
        """
        Match company name to existing record.

        Returns:
            Matched company name or None
        """
        self._load_companies_cache()

        name_lower = name.lower()

        # Exact match
        if name_lower in self._companies_cache:
            return self._companies_cache[name_lower]

        # Fuzzy match
        for cached_name, original in self._companies_cache.items():
            if self._similarity(name_lower, cached_name) > 0.9:
                return original

        return None

    def validate_file(self, import_id: int, rows: List[Dict]) -> Dict:
        """
        Validate all rows in the file.

        Returns validation summary.
        """
        self.update_import(import_id, status="validating")

        all_errors = []
        all_warnings = []
        valid_count = 0
        invalid_count = 0

        for i, row in enumerate(rows, start=2):  # Start at 2 (row 1 is header)
            errors, warnings = self.validate_row(row, i)
            all_errors.extend([e.to_dict() for e in errors])
            all_warnings.extend([w.to_dict() for w in warnings])

            if errors:
                invalid_count += 1
            else:
                valid_count += 1

        self.update_import(
            import_id,
            status="previewing",
            valid_rows=valid_count,
            invalid_rows=invalid_count,
            validation_errors={"errors": all_errors, "warnings": all_warnings},
        )

        return {
            "total_rows": len(rows),
            "valid_rows": valid_count,
            "invalid_rows": invalid_count,
            "errors": all_errors[:50],  # Limit to first 50 errors
            "warnings": all_warnings[:50],
        }

    def get_preview(self, rows: List[Dict], limit: int = 10) -> List[Dict]:
        """Get preview of rows with validation status."""
        preview = []
        for i, row in enumerate(rows[:limit], start=2):
            errors, warnings = self.validate_row(row, i)

            status = "valid"
            warning_msg = None
            if errors:
                status = "invalid"
            elif warnings:
                status = "warning"
                warning_msg = warnings[0].message

            preview.append(
                {
                    "row_num": i,
                    "company_name": row.get("company_name", ""),
                    "investor_name": row.get("investor_name", ""),
                    "investor_type": row.get("investor_type", ""),
                    "status": status,
                    "warning": warning_msg,
                }
            )

        return preview

    def import_rows(self, import_id: int, rows: List[Dict]) -> Dict:
        """
        Execute the import, inserting validated rows.

        Returns import results.
        """
        self.update_import(import_id, status="importing", started_at=datetime.utcnow())

        imported_ids = []
        imported_count = 0
        skipped_count = 0
        error_count = 0

        for i, row in enumerate(rows, start=2):
            errors, _ = self.validate_row(row, i)
            if errors:
                skipped_count += 1
                continue

            try:
                record_id = self._insert_portfolio_record(row)
                if record_id:
                    imported_ids.append(record_id)
                    imported_count += 1
                else:
                    skipped_count += 1
            except Exception as e:
                logger.error(f"Error importing row {i}: {e}")
                error_count += 1

        self.update_import(
            import_id,
            status="completed",
            imported_count=imported_count,
            skipped_count=skipped_count,
            error_count=error_count,
            rollback_data={"imported_ids": imported_ids},
            completed_at=datetime.utcnow(),
        )

        return {
            "imported": imported_count,
            "skipped": skipped_count,
            "errors": error_count,
        }

    def _insert_portfolio_record(self, row: Dict) -> Optional[int]:
        """Insert a single portfolio record."""
        investor_type = row.get("investor_type", "").lower()
        investor_name = row.get("investor_name", "")

        # Try to match existing investor
        investor_match = self._match_investor(investor_name, investor_type)

        if investor_match:
            investor_id, matched_type = investor_match
        else:
            # Create new investor record
            investor_id = self._create_investor(investor_name, investor_type)
            matched_type = investor_type

        if not investor_id:
            return None

        # Parse investment date
        investment_date = None
        if row.get("investment_date"):
            try:
                investment_date = self._parse_date(row["investment_date"])
            except ValueError:
                pass

        # Insert portfolio company record
        query = text("""
            INSERT INTO portfolio_companies (
                investor_id, investor_type, company_name, company_website,
                company_industry, company_stage, company_location,
                investment_type, investment_date, investment_amount_usd,
                shares_held, market_value_usd, ownership_percentage,
                current_holding, source_type, confidence_level, collected_date, created_at
            ) VALUES (
                :investor_id, :investor_type, :company_name, :website,
                :industry, :stage, :location,
                :investment_type, :investment_date, :amount,
                :shares, :market_value, :ownership,
                1, 'bulk_import', 'high', NOW(), NOW()
            )
            RETURNING id
        """)

        result = self.db.execute(
            query,
            {
                "investor_id": investor_id,
                "investor_type": matched_type,
                "company_name": row.get("company_name", ""),
                "website": row.get("company_website"),
                "industry": row.get("company_industry"),
                "stage": row.get("company_stage"),
                "location": row.get("company_location"),
                "investment_type": row.get("investment_type"),
                "investment_date": investment_date,
                "amount": row.get("investment_amount"),
                "shares": row.get("shares_held"),
                "market_value": row.get("market_value"),
                "ownership": row.get("ownership_percentage"),
            },
        )
        self.db.commit()

        record = result.fetchone()
        return record[0] if record else None

    def _create_investor(self, name: str, investor_type: str) -> Optional[int]:
        """Create a new investor record."""
        if investor_type == "lp":
            query = text("""
                INSERT INTO lp_fund (name, lp_type, created_at)
                VALUES (:name, 'Other', NOW())
                RETURNING id
            """)
        elif investor_type == "family_office":
            query = text("""
                INSERT INTO family_offices (name, type, created_at)
                VALUES (:name, 'Single Family Office', NOW())
                RETURNING id
            """)
        else:
            return None

        try:
            result = self.db.execute(query, {"name": name})
            self.db.commit()
            row = result.fetchone()

            # Add to cache
            if row:
                self._investors_cache[name.lower()] = (row[0], investor_type)

            return row[0] if row else None
        except Exception as e:
            logger.error(f"Error creating investor: {e}")
            self.db.rollback()
            return None

    def rollback_import(self, import_id: int) -> bool:
        """
        Rollback a completed import by deleting created records.

        Returns True if successful.
        """
        import_job = self.get_import(import_id)
        if not import_job:
            return False

        if import_job["status"] != "completed":
            return False

        rollback_data = import_job.get("rollback_data")
        if not rollback_data:
            return False

        # Parse rollback data if it's a string
        if isinstance(rollback_data, str):
            rollback_data = json.loads(rollback_data)

        imported_ids = rollback_data.get("imported_ids", [])
        if not imported_ids:
            return False

        try:
            # Delete imported portfolio records
            delete_query = text("""
                DELETE FROM portfolio_companies
                WHERE id = ANY(:ids)
            """)
            self.db.execute(delete_query, {"ids": imported_ids})
            self.db.commit()

            self.update_import(import_id, status="rolled_back")
            return True

        except Exception as e:
            logger.error(f"Rollback error: {e}")
            self.db.rollback()
            return False
