# SPEC 027 — Real Data Pipeline for Executive Intelligence Visualizations

**Status:** Draft
**Task type:** service
**Date:** 2026-03-25
**Test file:** tests/test_spec_027_real_data_pipeline.py

## Goal

Populate empty tables (`board_seats`, `board_interlocks`, `people_experience`, `people_education`, `person_pedigree_scores`) using existing DB data and the already-implemented `LLMExtractor.parse_bio()` method, then wire `board-interlocks.html` and `pedigree.html` to live API endpoints so both visualizations show real data.

## Acceptance Criteria

- [ ] `board_seats` is seeded from `company_people.is_board_member=true` via POST endpoint
- [ ] `board_interlocks` is populated by running `BoardInterlockService` on all companies with board seats
- [ ] `board-interlocks.html` fetches from `/api/v1/board-interlocks/company/{id}/network` with `?company_id=` URL param support
- [ ] `BioParserService.parse_all()` processes people with `bio` text → `people_experience` + `people_education` rows
- [ ] POST `/people-analytics/parse-bios` endpoint triggers batch bio parsing as a background job
- [ ] POST `/people-analytics/score-all-pedigrees` runs `PedigreeScorer.score_company()` for all companies with experience data
- [ ] `pedigree.html` fetches from `/api/v1/people-analytics/companies/{id}/pedigree-report` with URL param support
- [ ] Both D3 visualizations fall back to static demo data if API returns empty results

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_seed_board_seats_from_company_people | Seeds board_seats from company_people correctly |
| T2 | test_seed_board_seats_empty | Returns 0 seats when no board members exist |
| T3 | test_compute_all_interlocks | Calls BoardInterlockService for each company in board_seats |
| T4 | test_bio_parser_parse_person | BioParserService.parse_person() stores experience + education rows |
| T5 | test_bio_parser_skip_existing | Skips person already in people_experience when overwrite=False |
| T6 | test_bio_parser_parse_all_limit | Respects limit param, processes correct set of people |
| T7 | test_parse_bios_endpoint | POST /parse-bios returns job_id, runs in background |
| T8 | test_score_all_pedigrees_endpoint | POST /score-all-pedigrees scores all companies with experience |

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

### BioParserService
```python
class BioParserService:
    async def parse_person(self, person_id: int, person_name: str, company_name: str, bio: str, db: Session) -> dict
    async def parse_all(self, db: Session, limit: int = None, overwrite: bool = False) -> dict
```
- Reuses `LLMExtractor().parse_bio(bio_text, person_name, company_name)` — DO NOT re-implement
- Maps `ParsedBio.experience` → `PersonExperience` rows (person_id, company_name, title, start_year, end_year, is_current, source="bio_parse")
- Maps `ParsedBio.education` → `PersonEducation` rows (person_id, institution, degree, degree_type, field_of_study)
- Skips people already in `people_experience` unless overwrite=True
- Batches in groups of 20, uses asyncio.Semaphore(4) for concurrency

### New endpoints in `board_interlocks.py`
- `POST /board-interlocks/seed-from-company-people` — seeds board_seats from company_people
- `POST /board-interlocks/compute-all` — loops all companies in board_seats, calls compute_interlocks_for_company

### New endpoints in `people_analytics.py`
- `POST /people-analytics/parse-bios` — body: `{limit: int, overwrite: bool}`, background job
- `POST /people-analytics/score-all-pedigrees` — scores all companies that have experience data

### Frontend wiring pattern
```js
// URL param: ?company_id=123
const companyId = new URLSearchParams(window.location.search).get('company_id');
const data = await fetch(`/api/v1/board-interlocks/company/${companyId}/network`).then(r => r.json());
// Fall back to STATIC_DATA if data.nodes.length === 0
```

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| `app/services/bio_parser_service.py` | Create | BioParserService: batch bio → experience/education |
| `app/api/v1/board_interlocks.py` | Modify | Add seed-from-company-people + compute-all endpoints |
| `app/api/v1/people_analytics.py` | Modify | Add parse-bios + score-all-pedigrees endpoints |
| `frontend/d3/board-interlocks.html` | Modify | Wire to live API, company selector, URL params |
| `frontend/d3/pedigree.html` | Modify | Wire to live API, company selector, URL params |

## Feedback History

_No corrections yet._
