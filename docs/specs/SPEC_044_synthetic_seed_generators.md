# SPEC 044 — Synthetic Seed Generators (Phase A)

**Status:** Draft
**Task type:** service
**Date:** 2026-04-02
**Test file:** tests/test_spec_044_synthetic_seed_generators.py

## Goal

Build two synthetic data generators (job postings + LP-GP universe) and wire macro scenarios into the portfolio stress scorer. These unblock 3 dead scorers (exec_signal at 5%, gp_pipeline at 0%, lp_gp_graph at 0%) and make portfolio_stress scenario-capable. All synthetic generators must create ingestion jobs with `data_origin='synthetic'`.

## Acceptance Criteria

- [ ] Synthetic job postings generator produces ~10K postings across 120+ companies
- [ ] Postings have correct schema: company_id, title, seniority_level, status, department, location, posted_date
- [ ] Seniority distribution: ~5% c_suite/vp, ~15% director, ~30% manager, ~50% IC
- [ ] All synthetic jobs created with `data_origin='synthetic'` on ingestion job
- [ ] Synthetic LP-GP generator produces ~500 LPs + ~1K relationships
- [ ] LP types distributed: pension 40%, endowment 20%, insurance 15%, sovereign_wealth 10%, family_office 10%, fund_of_funds 5%
- [ ] LP-GP relationships use real PE firm names from seeded `pe_firm` table
- [ ] All synthetic LP-GP data created with `data_origin='synthetic'`
- [ ] Portfolio stress scorer accepts optional macro scenario input
- [ ] API endpoints registered and return valid responses
- [ ] exec_signal_scorer returns results after job postings seeded
- [ ] gp_pipeline_scorer returns results after LP-GP seeded

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_job_postings_output_shape | Generator returns list of dicts with required keys |
| T2 | test_job_postings_seniority_distribution | Seniority mix matches spec (~5% c_suite+vp) |
| T3 | test_job_postings_sector_awareness | Tech companies get engineering-heavy roles |
| T4 | test_job_postings_data_origin | Ingestion job has data_origin='synthetic' |
| T5 | test_lp_generation_type_distribution | LP types match spec proportions (±5%) |
| T6 | test_lp_gp_relationship_power_law | Mega-GPs get more LPs than mid-market |
| T7 | test_lp_commitment_sizing | Commitments are 1-5% of LP AUM |
| T8 | test_lp_gp_data_origin | Ingestion job has data_origin='synthetic' |
| T9 | test_stress_scorer_with_scenario | Stress scorer accepts scenario dict input |
| T10 | test_empty_company_list | Handles zero companies gracefully |

## Rubric Checklist

- [ ] Clear single responsibility (one domain concern per service)
- [ ] Async methods where I/O is involved
- [ ] Uses dependency injection for DB sessions and external clients
- [ ] All DB operations use parameterized queries
- [ ] Uses `null_preserving_upsert()` for enrichment workflows
- [ ] Error handling follows the error hierarchy
- [ ] Logging with structured context (source, operation, record counts)
- [ ] Has corresponding test file with mocked dependencies
- [ ] Tests cover happy path, error cases, and boundary conditions

## Design Notes

### Job Postings Generator
```python
# app/services/synthetic/job_postings.py
class SyntheticJobPostingsGenerator:
    def __init__(self, db: Session): ...
    def generate(self, n_per_company: int = 80) -> dict:
        # 1. Query industrial_companies + pe_portfolio_companies for seeded companies
        # 2. Per company: generate n_per_company postings
        # 3. Sector → role distribution (SECTOR_ROLE_MIX)
        # 4. Seniority → title templates (SENIORITY_TITLES)
        # 5. Bulk insert into job_postings
        # 6. Create ingestion job with data_origin='synthetic'
```

Target columns (matching existing schema): company_id, title, seniority_level, status='open', department, location, posted_date, created_at

### LP-GP Generator
```python
# app/services/synthetic/lp_gp_universe.py
class SyntheticLpGpGenerator:
    def __init__(self, db: Session): ...
    def generate(self, n_lps: int = 500) -> dict:
        # 1. Generate LP funds with type distribution
        # 2. Query pe_firm for real GP names
        # 3. Power-law LP→GP assignment
        # 4. Commitment sizing from LP AUM
        # 5. Bulk insert lp_fund + lp_gp_relationships
        # 6. Create ingestion job with data_origin='synthetic'
```

### Macro → Stress Wiring
Add optional `macro_overrides: dict` param to `PortfolioStressScorer.score_portfolio()` that replaces live FRED queries with provided scenario values.

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| `app/services/synthetic/job_postings.py` | Create | Job postings generator |
| `app/services/synthetic/lp_gp_universe.py` | Create | LP-GP universe generator |
| `app/services/portfolio_stress_scorer.py` | Modify | Accept macro scenario overrides |
| `app/api/v1/synthetic.py` | Modify | Add endpoints for job postings + LP-GP |
| `tests/test_spec_044_synthetic_seed_generators.py` | Create | All test cases |

## Feedback History

_No corrections yet._
