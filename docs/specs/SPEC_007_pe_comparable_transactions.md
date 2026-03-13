# SPEC 007 — PE Comparable Transaction Database

**Status:** Draft
**Task type:** service
**Date:** 2026-03-12
**Test file:** tests/test_spec_007_pe_comparable_transactions.py

## Goal

Build a comparable transaction service that queries completed exits in the same industry as a target company. Returns deal details with multiples, market stats (median multiple, trend, volume), and supports exit valuation decisions.

## Acceptance Criteria

- [ ] Service queries pe_deals for completed exits matching industry
- [ ] Returns deal details: name, buyer, seller, value, EV/EBITDA, date
- [ ] Calculates market stats: median deal multiple, volume, trend
- [ ] `GET /pe/comparable-transactions/{company_id}` returns comp transactions
- [ ] 15-20 historical exit transactions seeded in demo_seeder.py
- [ ] Tests cover happy path, no transactions, market stats

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| app/core/pe_comparable_transactions.py | Create | Service |
| app/api/v1/pe_benchmarks.py | Modify | Add endpoint |
| app/sources/pe/demo_seeder.py | Modify | Add historical exits |
| tests/test_spec_007_pe_comparable_transactions.py | Create | Tests |
