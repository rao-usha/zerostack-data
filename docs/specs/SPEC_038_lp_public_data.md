# SPEC_038 — LP Public Data: Seed + HTML Portal Scraper

**Type:** service
**Status:** active
**Plan:** PLAN_040

## Problem
`lp_gp_commitments` table is empty. CAFR PDFs all 404. Form 990s only have aggregate data.

## Solution
- **Path B:** Hardcode known public fund commitment data (~250 records) from published annual reports
- **Path C:** Scrape pension HTML investment portals for dynamic data
- **Path D:** Accept result and move on

## Acceptance Criteria

1. `get_seed_records()` returns >= 200 records covering >= 5 LPs and >= 20 unique GPs
2. Every seed record has: `lp_name`, `gp_name`, `fund_vintage`, `data_source == "public_seed"`
3. `PensionHtmlScraper.collect_all()` returns [] gracefully on HTTP errors (no exceptions)
4. `PensionHtmlScraper` correctly parses a PE table from fixture HTML
5. `POST /pe/conviction/seed-public` returns `{"status": "started"}` immediately
6. After seeding: `GET /pe/conviction/coverage` shows `total_lp_commitments >= 200`
7. `fund_lp_tracker_agent.run(sources=["public_seed"])` persists records to DB

## Test Cases (tests/test_spec_038_lp_public_data.py)

- `test_seed_records_schema` — all records have required keys
- `test_seed_records_count` — >= 200 records
- `test_seed_records_lp_coverage` — >= 5 unique lp_names
- `test_seed_records_gp_coverage` — >= 20 unique gp_names
- `test_html_scraper_graceful_404` — returns [] on error
- `test_html_scraper_table_parse` — parses fixture HTML table

## Rubric

- [ ] No exceptions raised by scraper on network errors
- [ ] Seed data sourced from real published annual reports (not fabricated)
- [ ] Data source tagged `public_seed` (not `cafr` or `pension_ir`)
- [ ] API endpoint is non-blocking (BackgroundTasks)
- [ ] Agent wires sources without breaking existing CAFR/990/FormD flow
