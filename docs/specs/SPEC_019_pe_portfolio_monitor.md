# SPEC 019 — PE Portfolio Monitor

**Status:** Draft
**Task type:** service
**Date:** 2026-03-13
**Test file:** tests/test_spec_019_pe_portfolio_monitor.py

## Goal

Build a portfolio monitoring service that detects changes in exit readiness scores, financials, and leadership across a PE firm's portfolio companies. Compares current state against stored snapshots and fires webhook alerts on significant changes.

## Acceptance Criteria

- [ ] 6 new PE webhook event types added to WebhookEventType enum
- [ ] pe_portfolio_snapshots table stores company state over time
- [ ] monitor_exit_readiness detects grade boundary crossings
- [ ] monitor_financials flags >10% revenue swings and margin compression
- [ ] monitor_leadership detects C-suite additions/departures
- [ ] run_full_portfolio_check orchestrates all monitors and returns health report
- [ ] Pure comparison functions are testable without DB

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_detect_exit_grade_change | B→A or B→C detected |
| T2 | test_no_change_no_alert | Same grade → no alert |
| T3 | test_detect_revenue_decline | >10% decline flagged |
| T4 | test_detect_margin_compression | EBITDA margin drop flagged |
| T5 | test_detect_leadership_departure | CEO/CFO departure detected |
| T6 | test_detect_leadership_addition | New C-suite detected |
| T7 | test_health_report_structure | Report has all required fields |
| T8 | test_no_snapshot_baseline | First run creates snapshot, no alerts |

## Rubric Checklist

- [ ] Clear single responsibility
- [ ] Uses dependency injection for DB sessions
- [ ] All DB operations use parameterized queries
- [ ] Logging with structured context
- [ ] Tests cover happy path, error cases, boundary conditions

## Design Notes

**Snapshot comparison pattern:**
1. Fetch current state (exit score, financials, leadership)
2. Fetch most recent snapshot for each company
3. Compare → generate alerts for significant changes
4. Store new snapshot
5. Fire webhooks for any alerts

**Key interfaces:**
```python
def monitor_exit_readiness(db, firm_id) -> List[Alert]
def monitor_financials(db, firm_id) -> List[Alert]
def monitor_leadership(db, firm_id) -> List[Alert]
def run_full_portfolio_check(db, firm_id) -> PortfolioHealthReport
```

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| app/core/models.py | Modify | Add 6 PE event types to WebhookEventType |
| app/core/pe_models.py | Modify | Add pe_portfolio_snapshots table |
| app/core/pe_portfolio_monitor.py | Create | Monitor service |
| tests/test_spec_019_pe_portfolio_monitor.py | Create | Unit tests |
