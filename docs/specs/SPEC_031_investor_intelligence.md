# SPEC 031 — Investor Intelligence API: Generalizable Report Data Layer

**Status:** Draft
**Task type:** api_endpoint
**Date:** 2026-03-27
**Test file:** tests/test_spec_031_investor_intelligence.py

## Goal

Build a generalizable Investor Intelligence API (PLAN_039) that makes any investor report data-driven instead of hardcoded. The system adds 3 data gap fills (FRED auto sector series, DOE AFDC EV registrations, BLS auto service industry series) and a new `/investor/` API that maps sectors to their relevant data sources and returns structured payloads that any IC report can consume.

## Acceptance Criteria

- [ ] `app/sources/fred/metadata.py` has new `auto_sector` category with TOTALSA, GASREGCOVW series
- [ ] `app/sources/bls/metadata.py` has new `auto_sector` dataset with NAICS 441/4413 CES series
- [ ] `app/sources/afdc/` exists as a full source plugin (client, ingest, metadata, router) following BaseAPIClient/BaseSourceIngestor patterns
- [ ] `POST /afdc/ingest/ev_registrations` triggers background ingestion job, returns job_id
- [ ] `POST /afdc/ingest/ev_stations` triggers background ingestion job, returns job_id
- [ ] `app/sources/investor_intel/sector_registry.py` defines all 9 sectors with their data source mappings
- [ ] `GET /investor/sectors` returns all 9 sectors with label, data sources, and coverage status
- [ ] `GET /investor/sector/{sector_slug}` returns live macro/sector data (FRED values, BLS trends, EV regs if applicable)
- [ ] `GET /investor/company/{ticker}/context` returns job trends + EDGAR financials for a given ticker
- [ ] `GET /investor/report-context/{sector}/{question_type}` returns structured payload (kpi_cards, chart_data, narrative_context) for disruption_analysis, market_sizing, operations_benchmarking, exit_readiness
- [ ] `POST /investor/run-comps/{ticker}` returns peer financial benchmarks using SIC/NAICS peer lookup
- [ ] Both new routers registered in `app/main.py` with OpenAPI tags
- [ ] All SQL queries use parameterized style, no string concatenation

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_sectors_list_returns_all_nine | GET /investor/sectors returns 9 sectors with correct keys |
| T2 | test_sector_detail_auto_service | GET /investor/sector/auto_service returns macro_indicators list |
| T3 | test_sector_detail_unknown_slug | GET /investor/sector/nonexistent returns 404 |
| T4 | test_company_context_with_ticker | GET /investor/company/MNRO/context returns expected structure |
| T5 | test_company_context_unknown_ticker | GET /investor/company/ZZZZ/context returns 404 or empty gracefully |
| T6 | test_report_context_disruption | GET /investor/report-context/auto_service/disruption_analysis returns kpi_cards, chart_data |
| T7 | test_report_context_invalid_question_type | Returns 400 with valid question types listed |
| T8 | test_afdc_ingest_returns_job_id | POST /afdc/ingest/ev_registrations returns job_id immediately |
| T9 | test_fred_auto_sector_category_exists | COMMON_SERIES dict has auto_sector key with TOTALSA |
| T10 | test_bls_auto_sector_dataset_exists | BLS metadata has auto_sector dataset with CES series |

## Rubric Checklist

- [ ] Router created in `app/api/v1/investor_intelligence.py` and `app/api/v1/afdc.py`
- [ ] Both routers registered in `app/main.py` with prefix and OpenAPI tag
- [ ] Uses `BackgroundTasks` for AFDC ingestion (returns job_id immediately)
- [ ] Request/response models defined with Pydantic (typed, validated)
- [ ] Error responses use the error hierarchy from `app/core/api_errors.py`
- [ ] Database session obtained via `get_db()` dependency
- [ ] SQL queries use parameterized style (`:param`), never string concatenation
- [ ] Endpoint docstrings describe purpose and parameters
- [ ] Has corresponding test file `tests/test_spec_031_investor_intelligence.py`
- [ ] Tests mock database and external services
- [ ] Tests cover happy path, validation errors, and edge cases
- [ ] No PII exposure beyond what sources explicitly provide

## Design Notes

**Sector registry** is a pure Python dict in `app/sources/investor_intel/sector_registry.py` — no DB, no model. Keys are slug strings; values map to FRED categories, BLS datasets, EIA datasets, AFDC datasets, and SIC/NAICS codes.

**`/investor/sector/{slug}`** does N live DB reads: one per relevant FRED category (latest value per series), one BLS employment query, one AFDC query if sector has EV data. All reads use parameterized SQL via `db.execute(text(...), {...})`.

**`/investor/report-context/{sector}/{question_type}`** has a hardcoded schema per question type that specifies which data fields map to which kpi_card slots and which chart_data arrays. The payload is JSON — not rendered HTML — so any report template can consume it.

**AFDC client** uses `DATA_GOV_API` env var (same key used by other data.gov sources). Base URL: `https://developer.nrel.gov/api/afdc/`. Tables: `afdc_ev_registrations`, `afdc_ev_stations`.

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| `app/sources/fred/metadata.py` | Modify | Add `auto_sector` to COMMON_SERIES |
| `app/sources/bls/metadata.py` | Modify | Add `auto_sector` dataset |
| `app/sources/afdc/__init__.py` | Create | Empty package init |
| `app/sources/afdc/client.py` | Create | AFDCClient(BaseAPIClient) |
| `app/sources/afdc/ingest.py` | Create | AFDCIngestor(BaseSourceIngestor) |
| `app/sources/afdc/metadata.py` | Create | Table schemas, series defs |
| `app/sources/investor_intel/__init__.py` | Create | Empty package init |
| `app/sources/investor_intel/sector_registry.py` | Create | 9-sector registry dict |
| `app/api/v1/afdc.py` | Create | EV data ingestion endpoints |
| `app/api/v1/investor_intelligence.py` | Create | Investor Intelligence API (5 endpoints) |
| `app/main.py` | Modify | Register both new routers + OpenAPI tags |

## Feedback History

_No corrections yet._
