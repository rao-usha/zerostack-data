"""
Tests for SPEC 026 — PE Portfolio Import Service.
CSV/Excel import with 4 templates, validation, preview, execute, rollback.
"""
import csv
import io
import pytest
from unittest.mock import MagicMock, patch, PropertyMock
from decimal import Decimal
from datetime import date

from app.core.pe_import import (
    PEPortfolioImporter,
    ImportRecord,
    TEMPLATES,
    COLUMN_ALIASES,
    TEMPLATE_SIGNATURES,
    _import_store,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_csv(headers: list, rows: list) -> bytes:
    """Build CSV bytes from headers + row lists."""
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(headers)
    for row in rows:
        writer.writerow(row)
    return buf.getvalue().encode("utf-8")


def _mock_db():
    """Create a mock DB session with common patterns."""
    db = MagicMock()
    # Default: no existing company found
    db.execute.return_value.scalar_one_or_none.return_value = None
    return db


# ---------------------------------------------------------------------------
# T1: Validate portfolio companies
# ---------------------------------------------------------------------------

class TestValidatePortfolioCompanies:
    """T1: Valid CSV passes, missing columns caught."""

    def test_valid_csv_passes(self):
        """Valid portfolio companies CSV has no errors."""
        csv_bytes = _make_csv(
            ["company_name", "industry", "revenue", "hq_city", "hq_state"],
            [
                ["Acme Corp", "Manufacturing", "50000000", "Chicago", "IL"],
                ["Beta Inc", "Technology", "30000000", "Austin", "TX"],
            ],
        )
        db = _mock_db()
        importer = PEPortfolioImporter(db)
        record = importer.upload(csv_bytes, "portfolio.csv", "portfolio_companies")

        assert record.rows_parsed == 2
        assert len(record.errors) == 0
        assert record.template_type == "portfolio_companies"

    def test_missing_required_column(self):
        """Missing company_name column produces validation error."""
        csv_bytes = _make_csv(
            ["industry", "revenue"],
            [["Manufacturing", "50000000"]],
        )
        db = _mock_db()
        importer = PEPortfolioImporter(db)
        record = importer.upload(csv_bytes, "portfolio.csv", "portfolio_companies")

        assert len(record.errors) > 0
        assert "company_name" in record.errors[0]["message"]

    def test_empty_required_value(self):
        """Row with empty company_name produces per-row error."""
        csv_bytes = _make_csv(
            ["company_name", "industry"],
            [
                ["Acme Corp", "Manufacturing"],
                ["", "Technology"],  # empty required value
            ],
        )
        db = _mock_db()
        importer = PEPortfolioImporter(db)
        record = importer.upload(csv_bytes, "portfolio.csv", "portfolio_companies")

        assert any("Missing required value" in e["message"] for e in record.errors)

    def test_empty_file_error(self):
        """Empty CSV returns error."""
        csv_bytes = b""
        db = _mock_db()
        importer = PEPortfolioImporter(db)
        record = importer.upload(csv_bytes, "empty.csv", "portfolio_companies")

        assert len(record.errors) > 0
        assert "No data" in record.errors[0]["message"]


# ---------------------------------------------------------------------------
# T2: Validate financial history
# ---------------------------------------------------------------------------

class TestValidateFinancialHistory:
    """T2: Financial CSV validated correctly."""

    def test_valid_financial_csv(self):
        """Valid financial CSV passes validation."""
        csv_bytes = _make_csv(
            ["company_name", "fiscal_year", "revenue", "ebitda"],
            [
                ["Acme Corp", "2023", "50000000", "10000000"],
                ["Acme Corp", "2024", "55000000", "12000000"],
            ],
        )
        db = _mock_db()
        importer = PEPortfolioImporter(db)
        record = importer.upload(csv_bytes, "financials.csv", "financial_history")

        assert record.rows_parsed == 2
        assert len(record.errors) == 0

    def test_invalid_fiscal_year(self):
        """Non-numeric fiscal_year produces error."""
        csv_bytes = _make_csv(
            ["company_name", "fiscal_year", "revenue"],
            [["Acme Corp", "twenty-twenty", "50000000"]],
        )
        db = _mock_db()
        importer = PEPortfolioImporter(db)
        record = importer.upload(csv_bytes, "financials.csv", "financial_history")

        assert any("fiscal_year" in e["message"] for e in record.errors)

    def test_non_numeric_revenue_warning(self):
        """Non-numeric revenue produces warning, not error."""
        csv_bytes = _make_csv(
            ["company_name", "fiscal_year", "revenue"],
            [["Acme Corp", "2023", "fifty million"]],
        )
        db = _mock_db()
        importer = PEPortfolioImporter(db)
        record = importer.upload(csv_bytes, "financials.csv", "financial_history")

        assert any("Non-numeric" in w["message"] for w in record.warnings)


# ---------------------------------------------------------------------------
# T3: Preview returns structure
# ---------------------------------------------------------------------------

class TestPreviewReturnsStructure:
    """T3: Preview has expected keys."""

    def test_preview_keys(self):
        """Preview returns all expected keys."""
        csv_bytes = _make_csv(
            ["company_name", "industry", "hq_city"],
            [
                ["Acme Corp", "Manufacturing", "Chicago"],
                ["Beta Inc", "Technology", "Austin"],
            ],
        )
        db = _mock_db()
        importer = PEPortfolioImporter(db)
        record = importer.upload(csv_bytes, "portfolio.csv", "portfolio_companies")
        preview = importer.preview(record.import_id)

        assert preview is not None
        expected_keys = {
            "import_id", "template_type", "status", "rows_parsed",
            "errors", "warnings", "sample_rows", "column_mappings",
            "error_count", "warning_count",
        }
        assert expected_keys.issubset(set(preview.keys()))
        assert preview["status"] == "previewed"
        assert len(preview["sample_rows"]) == 2

    def test_preview_nonexistent_import(self):
        """Preview returns None for unknown import_id."""
        db = _mock_db()
        importer = PEPortfolioImporter(db)
        assert importer.preview("nonexistent") is None

    def test_preview_sample_rows_capped(self):
        """Preview returns at most 5 sample rows."""
        rows = [[f"Company {i}", "Tech"] for i in range(10)]
        csv_bytes = _make_csv(["company_name", "industry"], rows)
        db = _mock_db()
        importer = PEPortfolioImporter(db)
        record = importer.upload(csv_bytes, "portfolio.csv", "portfolio_companies")
        preview = importer.preview(record.import_id)

        assert len(preview["sample_rows"]) == 5


# ---------------------------------------------------------------------------
# T4: Execute creates records
# ---------------------------------------------------------------------------

class TestExecuteCreatesRecords:
    """T4: Records inserted into correct tables."""

    def test_execute_portfolio_companies(self):
        """Execute creates PEPortfolioCompany records."""
        csv_bytes = _make_csv(
            ["company_name", "industry", "revenue", "hq_city", "hq_state"],
            [
                ["Acme Corp", "Manufacturing", "50000000", "Chicago", "IL"],
                ["Beta Inc", "Technology", "30000000", "Austin", "TX"],
            ],
        )
        db = _mock_db()
        # Mock flush to set id on added objects
        call_count = [0]

        def mock_add(obj):
            call_count[0] += 1
            obj.id = call_count[0]

        db.add.side_effect = mock_add

        importer = PEPortfolioImporter(db)
        record = importer.upload(csv_bytes, "portfolio.csv", "portfolio_companies")
        result = importer.execute(record.import_id)

        assert result is not None
        assert result["status"] == "imported"
        assert result["rows_imported"] == 2
        assert "pe_portfolio_companies" in result["created_ids"]
        assert result["created_ids"]["pe_portfolio_companies"] == 2
        assert db.commit.called

    def test_execute_with_errors_rejected(self):
        """Execute refuses to run when validation errors exist."""
        csv_bytes = _make_csv(
            ["industry"],  # missing company_name
            [["Manufacturing"]],
        )
        db = _mock_db()
        importer = PEPortfolioImporter(db)
        record = importer.upload(csv_bytes, "portfolio.csv", "portfolio_companies")
        result = importer.execute(record.import_id)

        assert "error" in result
        assert "validation errors" in result["error"]

    def test_execute_financial_history(self):
        """Execute creates PECompanyFinancials records."""
        csv_bytes = _make_csv(
            ["company_name", "fiscal_year", "revenue", "ebitda"],
            [["Acme Corp", "2023", "50000000", "10000000"]],
        )
        db = _mock_db()
        # _find_company needs to return a company with id
        mock_company = MagicMock()
        mock_company.id = 99
        db.execute.return_value.scalar_one_or_none.return_value = mock_company

        call_count = [0]

        def mock_add(obj):
            call_count[0] += 1
            obj.id = call_count[0]

        db.add.side_effect = mock_add

        importer = PEPortfolioImporter(db)
        record = importer.upload(csv_bytes, "financials.csv", "financial_history")
        result = importer.execute(record.import_id)

        assert result["status"] == "imported"
        assert result["rows_imported"] == 1
        assert "pe_company_financials" in result["created_ids"]

    def test_execute_nonexistent_import(self):
        """Execute returns None for unknown import_id."""
        db = _mock_db()
        importer = PEPortfolioImporter(db)
        assert importer.execute("nonexistent") is None


# ---------------------------------------------------------------------------
# T5: Rollback deletes records
# ---------------------------------------------------------------------------

class TestRollbackDeletesRecords:
    """T5: Rollback removes imported records."""

    def test_rollback_successful(self):
        """Rollback deletes created records and changes status."""
        csv_bytes = _make_csv(
            ["company_name", "industry"],
            [["Acme Corp", "Manufacturing"]],
        )
        db = _mock_db()
        call_count = [0]

        def mock_add(obj):
            call_count[0] += 1
            obj.id = call_count[0]

        db.add.side_effect = mock_add

        importer = PEPortfolioImporter(db)
        record = importer.upload(csv_bytes, "portfolio.csv", "portfolio_companies")
        importer.execute(record.import_id)
        result = importer.rollback(record.import_id)

        assert result["status"] == "rolled_back"
        assert "pe_portfolio_companies" in result["deleted"]
        assert db.execute.called

    def test_rollback_non_imported_fails(self):
        """Cannot rollback an import that hasn't been executed."""
        csv_bytes = _make_csv(
            ["company_name", "industry"],
            [["Acme Corp", "Manufacturing"]],
        )
        db = _mock_db()
        importer = PEPortfolioImporter(db)
        record = importer.upload(csv_bytes, "portfolio.csv", "portfolio_companies")
        result = importer.rollback(record.import_id)

        assert "error" in result
        assert "Cannot rollback" in result["error"]

    def test_rollback_nonexistent_import(self):
        """Rollback returns None for unknown import_id."""
        db = _mock_db()
        importer = PEPortfolioImporter(db)
        assert importer.rollback("nonexistent") is None


# ---------------------------------------------------------------------------
# T6: Auto-detect template
# ---------------------------------------------------------------------------

class TestAutoDetectTemplate:
    """T6: Template inferred from column headers."""

    def test_detect_portfolio_companies(self):
        """Columns with entry_multiple/hq_city detected as portfolio_companies."""
        csv_bytes = _make_csv(
            ["company_name", "entry_multiple", "hq_city", "hq_state", "industry"],
            [["Acme Corp", "8.5", "Chicago", "IL", "Manufacturing"]],
        )
        db = _mock_db()
        importer = PEPortfolioImporter(db)
        record = importer.upload(csv_bytes, "portfolio.csv")  # no template specified

        assert record.template_type == "portfolio_companies"

    def test_detect_financial_history(self):
        """Columns with fiscal_year/ebitda detected as financial_history."""
        csv_bytes = _make_csv(
            ["company_name", "fiscal_year", "ebitda", "revenue", "gross_margin_pct"],
            [["Acme Corp", "2023", "10000000", "50000000", "45.2"]],
        )
        db = _mock_db()
        importer = PEPortfolioImporter(db)
        record = importer.upload(csv_bytes, "financials.csv")

        assert record.template_type == "financial_history"

    def test_detect_deal_history(self):
        """Columns with deal_name/deal_type detected as deal_history."""
        csv_bytes = _make_csv(
            ["company_name", "deal_name", "deal_type", "enterprise_value", "ev_ebitda_multiple"],
            [["Acme Corp", "Acme LBO", "LBO", "100000000", "8.5"]],
        )
        db = _mock_db()
        importer = PEPortfolioImporter(db)
        record = importer.upload(csv_bytes, "deals.csv")

        assert record.template_type == "deal_history"

    def test_detect_leadership(self):
        """Columns with person_name/title/is_board_member detected as leadership."""
        csv_bytes = _make_csv(
            ["company_name", "person_name", "title", "is_board_member", "linkedin_url"],
            [["Acme Corp", "John Smith", "CEO", "yes", ""]],
        )
        db = _mock_db()
        importer = PEPortfolioImporter(db)
        record = importer.upload(csv_bytes, "leaders.csv")

        assert record.template_type == "leadership"

    def test_detect_unknown_fails(self):
        """Unrecognizable columns produce error."""
        csv_bytes = _make_csv(
            ["foo", "bar", "baz"],
            [["1", "2", "3"]],
        )
        db = _mock_db()
        importer = PEPortfolioImporter(db)
        record = importer.upload(csv_bytes, "mystery.csv")

        assert len(record.errors) > 0
        assert "auto-detect" in record.errors[0]["message"].lower()


# ---------------------------------------------------------------------------
# T7: Column mapping suggestions
# ---------------------------------------------------------------------------

class TestColumnMappingSuggestions:
    """T7: Close column names get suggested mappings."""

    def test_alias_mapping(self):
        """Known aliases are mapped automatically (sector → industry)."""
        csv_bytes = _make_csv(
            ["company_name", "sector", "city", "state"],
            [["Acme Corp", "Manufacturing", "Chicago", "IL"]],
        )
        db = _mock_db()
        importer = PEPortfolioImporter(db)
        record = importer.upload(csv_bytes, "portfolio.csv", "portfolio_companies")

        # After normalization, "sector" should have been mapped to "industry"
        assert record.raw_data[0].get("industry") == "Manufacturing"

    def test_fuzzy_suggestions_in_preview(self):
        """Close-but-not-exact column names get suggested mappings in preview."""
        csv_bytes = _make_csv(
            ["company_name", "headquarter_city", "revenues"],
            [["Acme Corp", "Chicago", "50000000"]],
        )
        db = _mock_db()
        importer = PEPortfolioImporter(db)
        record = importer.upload(csv_bytes, "portfolio.csv", "portfolio_companies")
        preview = importer.preview(record.import_id)

        # "headquarter_city" and "revenues" should get suggestions
        mappings = preview.get("column_mappings", {})
        # At least one mapping should be suggested
        assert len(mappings) >= 1

    def test_get_templates_lists_all(self):
        """get_templates() returns all 4 template definitions."""
        db = _mock_db()
        importer = PEPortfolioImporter(db)
        templates = importer.get_templates()

        assert len(templates) == 4
        names = {t["name"] for t in templates}
        assert names == {"portfolio_companies", "financial_history", "deal_history", "leadership"}

        for t in templates:
            assert "required_columns" in t
            assert "optional_columns" in t
            assert "all_columns" in t

    def test_list_imports(self):
        """list_imports returns tracked imports."""
        db = _mock_db()
        importer = PEPortfolioImporter(db)

        # Clear import store for isolation
        _import_store.clear()

        csv_bytes = _make_csv(
            ["company_name", "industry"],
            [["Acme Corp", "Manufacturing"]],
        )
        importer.upload(csv_bytes, "portfolio.csv", "portfolio_companies")
        imports = importer.list_imports()

        assert len(imports) >= 1
        assert imports[0]["template_type"] == "portfolio_companies"


# ---------------------------------------------------------------------------
# Helper function tests
# ---------------------------------------------------------------------------

class TestHelpers:
    """Tests for type conversion helpers."""

    def test_to_decimal(self):
        assert PEPortfolioImporter._to_decimal("1,000,000") == Decimal("1000000")
        assert PEPortfolioImporter._to_decimal("$50.5") == Decimal("50.5")
        assert PEPortfolioImporter._to_decimal("45.2%") == Decimal("45.2")
        assert PEPortfolioImporter._to_decimal("") is None
        assert PEPortfolioImporter._to_decimal("not a number") is None

    def test_to_int(self):
        assert PEPortfolioImporter._to_int("1,000") == 1000
        assert PEPortfolioImporter._to_int("500") == 500
        assert PEPortfolioImporter._to_int("") is None
        assert PEPortfolioImporter._to_int("abc") is None

    def test_to_date(self):
        assert PEPortfolioImporter._to_date("2023-06-15") == date(2023, 6, 15)
        assert PEPortfolioImporter._to_date("06/15/2023") == date(2023, 6, 15)
        assert PEPortfolioImporter._to_date("") is None
        assert PEPortfolioImporter._to_date("not a date") is None

    def test_is_numeric(self):
        assert PEPortfolioImporter._is_numeric("123.45") is True
        assert PEPortfolioImporter._is_numeric("$1,000") is True
        assert PEPortfolioImporter._is_numeric("abc") is False
