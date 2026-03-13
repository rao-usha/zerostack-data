# SPEC 014 — PE Fund Performance Engine

**Status:** Draft
**Task type:** service
**Date:** 2026-03-13
**Test file:** tests/test_spec_014_pe_fund_performance.py

## Goal

Build a real IRR/MOIC/TVPI/DPI calculation engine from cash flow data so fund performance numbers are defensible, not hardcoded. This is the single biggest credibility gap for PE demos. Implements Newton-Raphson IRR without numpy dependency.

## Acceptance Criteria

- [ ] PECashFlow model added to pe_models.py (fund_id, date, amount, cash_flow_type, description)
- [ ] IRR calculation via Newton-Raphson with edge case handling (single flow, all negative, convergence failure)
- [ ] MOIC = total_distributions / total_invested
- [ ] TVPI = (distributions + NAV) / called_capital
- [ ] DPI = distributions / called_capital
- [ ] RVPI = NAV / called_capital
- [ ] `calculate_fund_returns(fund_id, as_of_date)` computes all metrics from cash flows
- [ ] `calculate_fund_timeseries(fund_id)` computes quarterly snapshots
- [ ] All static computation methods testable without DB
- [ ] All tests pass

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_irr_simple_cashflows | IRR correct for known cash flow series |
| T2 | test_irr_single_cashflow | Edge case: single cash flow returns None |
| T3 | test_irr_no_convergence | Handles convergence failure gracefully |
| T4 | test_moic_calculation | MOIC = distributions / invested |
| T5 | test_tvpi_calculation | TVPI = (dist + NAV) / called |
| T6 | test_dpi_rvpi_calculation | DPI and RVPI computed correctly |
| T7 | test_zero_called_capital | Division by zero returns None |
| T8 | test_quarterly_timeseries | Timeseries produces quarterly snapshots |
| T9 | test_all_negative_flows | All outflows returns negative/None metrics |

## Rubric Checklist

- [ ] Clear single responsibility (one domain concern per service)
- [ ] Uses dependency injection for DB sessions
- [ ] All DB operations use parameterized queries
- [ ] Logging with structured context
- [ ] Has corresponding test file with mocked dependencies
- [ ] Tests cover happy path, error cases, and boundary conditions

## Design Notes

- `FundPerformanceService` class with DB session injection
- Static methods for pure computation (IRR, MOIC, TVPI, DPI, RVPI) — testable without DB
- Newton-Raphson IRR: NPV(r) = sum(CF_i / (1+r)^t_i) = 0, iterate until |NPV| < epsilon
- Cash flow convention: negative = outflow (capital call), positive = inflow (distribution)
- NAV parameter for unrealized value in TVPI/RVPI calculations

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| app/core/pe_fund_performance.py | Create | Service with IRR calculator and fund metrics |
| app/core/pe_models.py | Modify | Add PECashFlow model |
| tests/test_spec_014_pe_fund_performance.py | Create | Tests for fund performance calculations |

## Feedback History

_No corrections yet._
