# SPEC 015 — PE Fund Tearsheet Report

**Status:** Draft
**Task type:** report
**Date:** 2026-03-13
**Test file:** tests/test_spec_015_pe_fund_tearsheet.py

## Goal

Build a professional fund tearsheet report template that generates IC-ready single-page documents. Gathers fund metadata, performance metrics (IRR/TVPI/DPI/MOIC), portfolio company table, sector allocation, cash flow summary, J-curve chart, and top/bottom performers from PE data models.

## Acceptance Criteria

- [ ] Template class with `gather_data()`, `render_html()`, `render_excel()` methods
- [ ] Registered in builder.py as `pe_fund_tearsheet`
- [ ] `gather_data()` queries PEFund, PEFundPerformance, PEFundInvestment, PEPortfolioCompany, PECompanyFinancials, PECashFlow
- [ ] HTML output uses design system (dark/light mode, KPI strip, Chart.js charts)
- [ ] KPI strip: Fund Size, Net IRR, TVPI, DPI, MOIC
- [ ] J-curve line chart from cash flow data
- [ ] Bar chart for portfolio company valuations
- [ ] Sector allocation doughnut chart
- [ ] Portfolio company table with investment details
- [ ] Top/bottom performers section
- [ ] Cash flow summary table
- [ ] Excel output: Summary, Portfolio, Cash Flows, Performance sheets
- [ ] Accepts `fund_id` parameter (required)

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_gather_data_returns_expected_keys | gather_data returns all required sections |
| T2 | test_render_html_contains_sections | HTML output has KPI strip, charts, tables |
| T3 | test_render_excel_has_sheets | Excel has 4 required sheet names |
| T4 | test_template_registered | Template is in ReportBuilder.templates |
| T5 | test_gather_data_missing_fund | Handles non-existent fund_id gracefully |

## Rubric Checklist

- [ ] Self-contained HTML file (no external CDN dependencies except Chart.js)
- [ ] Uses shared design system (dark/light mode toggle, flat design)
- [ ] Chart.js for all data visualizations
- [ ] Responsive layout (works on desktop and tablet)
- [ ] Data injection points clearly marked with template variables
- [ ] Print-friendly styles included
- [ ] Executive summary section at top
- [ ] Source attribution and data freshness timestamps
- [ ] Professional color palette (no garish colors)

## Design Notes

- Template accepts `{"fund_id": int}` in params
- Queries chain: PEFund → PEFundPerformance (latest), PEFundInvestment → PEPortfolioCompany → PECompanyFinancials
- J-curve: PECashFlow records ordered by date, cumulative sum
- MOIC = total value / invested capital (from fund performance or computed)
- Top/bottom performers ranked by exit_multiple or unrealized gain

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| app/reports/templates/pe_fund_tearsheet.py | Create | Template class |
| app/reports/builder.py | Modify | Register template |
| tests/test_spec_015_pe_fund_tearsheet.py | Create | Tests |

## Feedback History

_No corrections yet._
