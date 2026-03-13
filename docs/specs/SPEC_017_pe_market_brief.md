# SPEC 017 — PE Market Brief Report

**Status:** Draft
**Task type:** report
**Date:** 2026-03-13
**Test file:** tests/test_spec_017_pe_market_brief.py

## Goal

Build a market intelligence brief report template that generates a 2-3 page overview of PE deal activity, sector momentum, fragmentation opportunities, and rollup targets.

## Acceptance Criteria

- [ ] Template class with `gather_data()`, `render_html()`, `render_excel()` methods
- [ ] Registered in builder.py as `pe_market_brief`
- [ ] Accepts optional `industry` parameter (defaults to all sectors)
- [ ] Sections: Sector Overview, Deal Activity Trends, Fragmentation Opportunities, Top Rollup Targets, Market Timing Assessment
- [ ] Uses design system, Chart.js charts
- [ ] Excel export

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_gather_data_returns_expected_keys | gather_data returns all required sections |
| T2 | test_render_html_contains_sections | HTML output has all sections |
| T3 | test_render_excel_has_sheets | Excel has required sheets |
| T4 | test_template_registered | Template name and methods exist |
| T5 | test_gather_data_no_industry | Works without industry filter |

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| app/reports/templates/pe_market_brief.py | Create | Template class |
| app/reports/builder.py | Modify | Register template |
| tests/test_spec_017_pe_market_brief.py | Create | Tests |
