# SPEC 016 — PE Deal Memo Report

**Status:** Draft
**Task type:** report
**Date:** 2026-03-13
**Test file:** tests/test_spec_016_pe_deal_memo.py

## Goal

Build an IC-ready deal memo report template that generates multi-section investment committee documents. Gathers company profile, financial benchmarks, exit readiness, comparable transactions, buyer analysis, competitive landscape, leadership team.

## Acceptance Criteria

- [ ] Template class with `gather_data()`, `render_html()`, `render_excel()` methods
- [ ] Registered in builder.py as `pe_deal_memo`
- [ ] Accepts `company_id` parameter (required)
- [ ] Sections: Executive Summary, Company Overview, Financial Analysis, Market Position, Management Assessment, Valuation & Comparables, Exit Readiness, Recommended Next Steps
- [ ] Financial benchmark radar chart (Chart.js)
- [ ] Comparable transactions table
- [ ] Uses design system (dark/light mode, KPI strip)
- [ ] Excel export with multi-sheet workbook

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_gather_data_returns_expected_keys | gather_data returns all required sections |
| T2 | test_render_html_contains_sections | HTML output has all 8 IC memo sections |
| T3 | test_render_excel_has_sheets | Excel has required sheet names |
| T4 | test_template_registered | Template name and methods exist |
| T5 | test_gather_data_missing_company | Handles non-existent company_id |

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| app/reports/templates/pe_deal_memo.py | Create | Template class |
| app/reports/builder.py | Modify | Register template |
| tests/test_spec_016_pe_deal_memo.py | Create | Tests |
