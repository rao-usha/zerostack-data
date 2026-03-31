---
name: SPEC_037_lp_conviction_v2
description: LP Conviction 2.0 — fix broken collectors, add CAFR/HTML-aware parsers, tier classifier, wire conviction scoring
type: service
---

# Spec 037 — LP Conviction 2.0

## Plan Reference
See `docs/plans/PLAN_037_lp_conviction_v2.md` for full root cause analysis and implementation details.

## Acceptance Criteria

1. `pension_cafr_collector.py` — `PensionCafrCollector.collect_all()` tries PDF URLs for 10 pensions (year 2024→2023→2022 fallback); returns list of dicts with `{lp_name, gp_name, fund_name, vintage_year, commitment_amount_usd, data_source='cafr'}`
2. `form_990_html_parser.py` — `parse_form_990_schedule_d(html_text, lp_name)` uses BeautifulSoup to find Schedule D HTML tables, returns PE/VC/Partnership investment rows
3. `lp_tier_classifier.py` — `classify_lp_tier(lp)` → int 1-5; `classify_all_lps(db)` → int (rows updated)
4. `LpFund.lp_tier` column added to `app/core/models.py`
5. `fund_lp_tracker_agent.py` uses PensionCafrCollector as primary source; calls `_score_affected_funds()` at end of run
6. `form_990_pe_extractor.py` uses HTML-aware parser; ENDOWMENT_TARGETS expanded to 12
7. `pe_conviction.py` has 3 new endpoints: POST /classify-lps, GET /lp-commitments, GET /coverage
8. `database.py` runs ALTER TABLE for lp_tier at startup

## Test Cases (skeleton)
- classify_lp_tier: sovereign/endowment → 1; public_pension + AUM>50B → 2; family_office → 5
- parse_form_990_schedule_d: valid HTML table with PE rows → returns records; no schedule D → returns []
- PensionCafrCollector: mock HTTP HEAD success → tries extract; all 404 → returns []
