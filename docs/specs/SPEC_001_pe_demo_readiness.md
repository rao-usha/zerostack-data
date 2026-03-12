# SPEC 001 — PE Demo Readiness

**Status:** Draft
**Task type:** service
**Date:** 2026-03-12
**Test file:** tests/test_spec_001_pe_demo_readiness.py

## Goal

Build three backend services needed for a compelling PE demo: (1) a demo data seeder that populates realistic PE firms, funds, portfolio companies, and financials, (2) a financial benchmarking engine that compares portfolio companies against peers, and (3) an exit readiness scorer that produces a composite 0-100 grade. These make the PE screens useful out-of-the-box.

## Acceptance Criteria

- [ ] Demo seeder creates 3 PE firms, 6 funds, 24+ portfolio companies with financials, 20+ people, and 5+ deals
- [ ] Seeder is idempotent (running twice doesn't duplicate data)
- [ ] `GET /api/v1/pe/benchmarks/{company_id}` returns percentile ranks for revenue growth, EBITDA margin, revenue/employee, debt/EBITDA
- [ ] `GET /api/v1/pe/benchmarks/portfolio/{firm_id}` returns heatmap data for all portfolio companies
- [ ] `GET /api/v1/pe/exit-readiness/{company_id}` returns composite score (0-100) with 6 sub-scores and letter grade
- [ ] Each sub-score includes a letter grade + explanation
- [ ] Exit readiness includes recommended actions to improve score
- [ ] All endpoints return meaningful data against seeded demo data
- [ ] `POST /api/v1/pe/seed-demo` triggers the seeder
- [ ] All DB operations use parameterized queries
- [ ] 15+ unit tests passing

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_seeder_creates_firms | Seeder creates 3 firms with correct attributes |
| T2 | test_seeder_creates_funds_per_firm | Each firm gets 2 funds with vintage years and performance |
| T3 | test_seeder_creates_portfolio_companies | 8+ companies per firm with financials |
| T4 | test_seeder_idempotent | Running seeder twice yields same record count |
| T5 | test_benchmark_single_company | Returns percentile ranks for known company |
| T6 | test_benchmark_missing_company | Returns 404 for nonexistent company_id |
| T7 | test_benchmark_no_financials | Handles company with no financial records gracefully |
| T8 | test_benchmark_portfolio_heatmap | Returns all companies for a firm with metric scores |
| T9 | test_benchmark_portfolio_empty_firm | Returns empty list for firm with no portfolio |
| T10 | test_exit_score_full_data | Returns composite score with all 6 sub-scores |
| T11 | test_exit_score_missing_company | Returns 404 for nonexistent company_id |
| T12 | test_exit_score_partial_data | Handles missing financials/people gracefully with reduced confidence |
| T13 | test_exit_score_grades | Score 80+ = A, 65-79 = B, 50-64 = C, 35-49 = D, <35 = F |
| T14 | test_exit_score_recommendations | Returns at least 1 recommendation per sub-score below B |
| T15 | test_benchmark_percentile_math | Verify percentile calculation against known distribution |

## Rubric Checklist

- [ ] Clear single responsibility (one domain concern per service)
- [ ] Async methods where I/O is involved
- [ ] Uses dependency injection for DB sessions and external clients
- [ ] All DB operations use parameterized queries
- [ ] Uses `null_preserving_upsert()` for enrichment workflows
- [ ] Error handling follows the error hierarchy (`RetryableError`, `FatalError`, etc.)
- [ ] Logging with structured context (source, operation, record counts)
- [ ] Has corresponding test file with mocked dependencies
- [ ] Tests cover happy path, error cases, and boundary conditions

## Design Notes

### Demo Seeder (`app/sources/pe/demo_seeder.py`)
- Uses existing PE models: `PEFirm`, `PEFund`, `PEFundPerformance`, `PEPortfolioCompany`, `PECompanyFinancials`, `PEFundInvestment`, `PEPerson`, `PEFirmPeople`, `PEDeal`
- Upserts by name to ensure idempotency
- 3 firms: growth equity, buyout, and sector-focused
- Financial data spans 2021-2025 with realistic trends

### Benchmarking Engine (`app/core/pe_benchmarking.py`)
- Queries `PECompanyFinancials` for the target + all companies in same industry/sub_industry
- Computes percentile rank using `scipy`-style formula or simple rank/count
- Metrics: `revenue_growth_pct`, `ebitda_margin_pct`, `revenue_usd / employee_count`, `debt_to_ebitda`
- Portfolio heatmap: one row per company, columns = metrics, cells = percentile rank (0-100)

### Exit Readiness Scorer (`app/core/pe_exit_scoring.py`)
- 6 weighted sub-scores:
  - Financial Health (30%): revenue growth, EBITDA margin, FCF, debt level
  - Market Position (20%): industry, competitor count, relative size
  - Management Quality (15%): leadership count, CEO tenure, key person risk
  - Data Room Readiness (15%): coverage of financials, valuations, leadership, news
  - Market Timing (10%): sector deal volume trend, entry multiple vs current
  - Regulatory Risk (10%): EPA/OSHA findings if available, default to neutral

### API Router (`app/api/v1/pe_benchmarks.py`)
- `POST /pe/seed-demo` — trigger seeder
- `GET /pe/benchmarks/{company_id}` — single company benchmarks
- `GET /pe/benchmarks/portfolio/{firm_id}` — portfolio heatmap
- `GET /pe/exit-readiness/{company_id}` — exit readiness score

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| `app/sources/pe/demo_seeder.py` | Create | Demo data seeder with 3 firms + portfolio |
| `app/core/pe_benchmarking.py` | Create | Financial benchmarking engine |
| `app/core/pe_exit_scoring.py` | Create | Exit readiness scoring engine |
| `app/api/v1/pe_benchmarks.py` | Create | API router for benchmarks + exit score + seeder |
| `app/main.py` | Modify | Register pe_benchmarks router + OpenAPI tag |
| `tests/test_spec_001_pe_demo_readiness.py` | Create | 15+ unit tests |

## Feedback History

_No corrections yet._
