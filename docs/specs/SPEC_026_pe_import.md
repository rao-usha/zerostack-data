# SPEC 026 — PE Portfolio Import Service

**Status:** Draft
**Task type:** service
**Date:** 2026-03-13
**Test file:** tests/test_spec_026_pe_import.py

## Goal

Build a PE-specific data import pipeline supporting 4 CSV/Excel templates (portfolio companies, financials, deals, leadership). Enables PE firms to onboard their own data with validation, preview, and rollback.

## Acceptance Criteria

- [ ] PEPortfolioImporter class with validate(), preview(), execute(), rollback()
- [ ] 4 templates: portfolio_companies, financial_history, deal_history, leadership
- [ ] CSV and Excel parsing (csv.DictReader + openpyxl)
- [ ] Column validation: required columns check, type coercion, error reporting
- [ ] Fuzzy match existing companies by name (avoid duplicates)
- [ ] Preview returns row count, errors, warnings, sample rows
- [ ] Execute returns created record counts per table
- [ ] Rollback deletes records created by a specific import
- [ ] Auto-create PEFirm if firm_name provided but not found
- [ ] Auto-detect template type from column headers

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_validate_portfolio_companies | Valid CSV passes, missing columns caught |
| T2 | test_validate_financial_history | Financial CSV validated correctly |
| T3 | test_preview_returns_structure | Preview has expected keys |
| T4 | test_execute_creates_records | Records inserted into correct tables |
| T5 | test_rollback_deletes_records | Rollback removes imported records |
| T6 | test_auto_detect_template | Template inferred from column headers |
| T7 | test_column_mapping_suggestions | Close column names get suggested mappings |

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| app/core/pe_import.py | Create | Import service |
| tests/test_spec_026_pe_import.py | Create | Tests |

## Feedback History

_No corrections yet._
