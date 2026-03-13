# SPEC 011 — PE Firms List Endpoint + ID Drift Fix

**Status:** Draft
**Task type:** api_endpoint
**Date:** 2026-03-13
**Test file:** tests/test_spec_011_pe_firms_endpoint.py

## Goal

Add a GET /pe/firms endpoint that returns all demo PE firms with their current IDs, fund counts, and company counts. This allows the frontend to discover firm IDs dynamically instead of hardcoding them.

## Acceptance Criteria

- [ ] `GET /pe/firms` returns all PE firms with id, name, fund_count, company_count
- [ ] Seed endpoint returns firm IDs in response
- [ ] POST /pe/seed-demo → GET /pe/firms → GET /pe/benchmarks/portfolio/{id} works e2e
- [ ] Tests cover firms list with data and empty state

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| app/api/v1/pe_benchmarks.py | Modify | Add GET /pe/firms endpoint |
| tests/test_spec_011_pe_firms_endpoint.py | Create | Tests |
