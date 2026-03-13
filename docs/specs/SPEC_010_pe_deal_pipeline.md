# SPEC 010 — PE Deal Pipeline Enhancements

**Status:** Draft
**Task type:** service
**Date:** 2026-03-12
**Test file:** tests/test_spec_010_pe_deal_pipeline.py

## Goal

Build deal pipeline management endpoints for PE deal teams — CRUD for pipeline deals, pipeline summary/insights, and stage management. Enables demo story walkthrough of tracking deals from sourcing through close.

## Acceptance Criteria

- [ ] `GET /pe/pipeline` returns all pipeline deals with filters
- [ ] `POST /pe/pipeline` creates a new pipeline deal
- [ ] `PATCH /pe/pipeline/{deal_id}` updates deal status/stage
- [ ] `GET /pe/pipeline/insights` returns pipeline health summary
- [ ] Seed 8-10 additional pipeline deals at various stages
- [ ] Tests cover CRUD + insights

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| app/core/pe_deal_pipeline.py | Create | Pipeline service |
| app/api/v1/pe_benchmarks.py | Modify | Add 4 endpoints |
| app/sources/pe/demo_seeder.py | Modify | Seed pipeline deals |
| tests/test_spec_010_pe_deal_pipeline.py | Create | Tests |
