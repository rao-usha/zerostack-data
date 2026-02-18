"""
Glassdoor CSV Importer.

Reads Glassdoor CSV exports and bulk-inserts into existing tables.
"""

import csv
import io
import logging
from typing import Dict, Any, List, Optional

from sqlalchemy.orm import Session
from sqlalchemy import text

from app.core.batch_operations import batch_insert

logger = logging.getLogger(__name__)

# Map CSV column names to DB column names for each table
COMPANY_COLUMN_MAP = {
    "company_name": "company_name",
    "name": "company_name",
    "glassdoor_id": "glassdoor_id",
    "logo_url": "logo_url",
    "website": "website",
    "headquarters": "headquarters",
    "industry": "industry",
    "company_size": "company_size",
    "size": "company_size",
    "founded_year": "founded_year",
    "founded": "founded_year",
    "overall_rating": "overall_rating",
    "rating": "overall_rating",
    "work_life_balance": "work_life_balance",
    "compensation_benefits": "compensation_benefits",
    "comp_benefits": "compensation_benefits",
    "career_opportunities": "career_opportunities",
    "culture_values": "culture_values",
    "senior_management": "senior_management",
    "ceo_approval": "ceo_approval",
    "recommend_to_friend": "recommend_to_friend",
    "business_outlook": "business_outlook",
    "ceo_name": "ceo_name",
    "review_count": "review_count",
    "salary_count": "salary_count",
    "interview_count": "interview_count",
}

SALARY_COLUMN_MAP = {
    "company_name": "company_name",
    "job_title": "job_title",
    "title": "job_title",
    "location": "location",
    "base_salary_min": "base_salary_min",
    "base_salary_median": "base_salary_median",
    "base_salary_max": "base_salary_max",
    "total_comp_min": "total_comp_min",
    "total_comp_median": "total_comp_median",
    "total_comp_max": "total_comp_max",
    "sample_size": "sample_size",
    "experience_level": "experience_level",
}


class GlassdoorCSVImporter:
    """Import Glassdoor data from CSV files into existing tables."""

    def __init__(self, db: Session):
        self.db = db

    def import_companies_csv(self, csv_content: str) -> Dict[str, Any]:
        """
        Import company data from CSV content.

        Args:
            csv_content: CSV string with company data

        Returns:
            Dict with rows_imported and errors
        """
        reader = csv.DictReader(io.StringIO(csv_content))
        rows = []
        errors = []

        for i, raw_row in enumerate(reader):
            try:
                row = self._map_columns(raw_row, COMPANY_COLUMN_MAP)
                if not row.get("company_name"):
                    errors.append(f"Row {i+1}: missing company_name")
                    continue
                # Coerce numeric fields
                for field in (
                    "founded_year",
                    "review_count",
                    "salary_count",
                    "interview_count",
                ):
                    if row.get(field):
                        row[field] = int(float(row[field]))
                for field in (
                    "overall_rating",
                    "work_life_balance",
                    "compensation_benefits",
                    "career_opportunities",
                    "culture_values",
                    "senior_management",
                    "ceo_approval",
                    "recommend_to_friend",
                    "business_outlook",
                ):
                    if row.get(field):
                        row[field] = float(row[field])
                row["data_source"] = "csv_import"
                rows.append(row)
            except Exception as e:
                errors.append(f"Row {i+1}: {e}")

        if not rows:
            return {"rows_imported": 0, "errors": errors}

        columns = list(rows[0].keys())
        result = batch_insert(
            db=self.db,
            table_name="glassdoor_companies",
            rows=rows,
            columns=columns,
            conflict_columns=["company_name"],
            update_columns=[c for c in columns if c != "company_name"],
        )

        return {
            "rows_imported": result.rows_inserted + result.rows_updated,
            "rows_inserted": result.rows_inserted,
            "rows_updated": result.rows_updated,
            "errors": errors,
        }

    def import_salaries_csv(self, csv_content: str) -> Dict[str, Any]:
        """
        Import salary data from CSV content.

        Expects a company_name column to link salaries to companies.

        Args:
            csv_content: CSV string with salary data

        Returns:
            Dict with rows_imported and errors
        """
        reader = csv.DictReader(io.StringIO(csv_content))
        rows_by_company: Dict[str, List[Dict]] = {}
        errors = []

        for i, raw_row in enumerate(reader):
            try:
                row = self._map_columns(raw_row, SALARY_COLUMN_MAP)
                company = row.pop("company_name", None)
                if not company:
                    errors.append(f"Row {i+1}: missing company_name")
                    continue
                if not row.get("job_title"):
                    errors.append(f"Row {i+1}: missing job_title")
                    continue
                # Coerce numeric fields
                for field in (
                    "base_salary_min",
                    "base_salary_median",
                    "base_salary_max",
                    "total_comp_min",
                    "total_comp_median",
                    "total_comp_max",
                    "sample_size",
                ):
                    if row.get(field):
                        row[field] = int(float(row[field]))
                rows_by_company.setdefault(company, []).append(row)
            except Exception as e:
                errors.append(f"Row {i+1}: {e}")

        total_imported = 0
        for company, salaries in rows_by_company.items():
            # Look up company ID
            result = self.db.execute(
                text(
                    "SELECT id FROM glassdoor_companies WHERE LOWER(company_name) = LOWER(:name)"
                ),
                {"name": company},
            )
            company_row = result.fetchone()
            if not company_row:
                errors.append(f"Company '{company}' not found in glassdoor_companies")
                continue

            company_id = company_row[0]
            for salary in salaries:
                salary["company_id"] = company_id

            columns = list(salaries[0].keys())
            insert_result = batch_insert(
                db=self.db,
                table_name="glassdoor_salaries",
                rows=salaries,
                columns=columns,
            )
            total_imported += insert_result.rows_inserted

        return {
            "rows_imported": total_imported,
            "companies_processed": len(rows_by_company),
            "errors": errors,
        }

    def _map_columns(
        self, raw_row: Dict[str, str], column_map: Dict[str, str]
    ) -> Dict[str, Any]:
        """Map CSV columns to DB columns using the mapping dict."""
        mapped = {}
        for csv_col, value in raw_row.items():
            csv_col_lower = csv_col.strip().lower().replace(" ", "_")
            db_col = column_map.get(csv_col_lower)
            if db_col and value and value.strip():
                mapped[db_col] = value.strip()
        return mapped
