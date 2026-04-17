# SPEC 029 — Deal Radar: NL Query Bar + AI Deal Memo Generator

**Status:** Draft
**Task type:** service
**Date:** 2026-03-30
**Test file:** tests/test_spec_029_deal_radar_nlq_memo.py

## Goal

Add two AI-powered features to Deal Radar: (1) a natural language query bar that lets users filter the convergence map by typing plain English ("show me regions with high EPA violations"), powered by Claude parsing intent into structured filters; (2) a deal memo generator that produces a full investment memo for any convergence cluster, pulling real signal data and synthesizing via Claude.

## Acceptance Criteria

- [ ] NLQ: `POST /deal-radar/query` accepts `{"query": "..."}` and returns filtered regions + explanation
- [ ] NLQ: Claude parses intent into safe filter objects — never raw SQL
- [ ] NLQ: Filters validated against whitelist of fields and operators before execution
- [ ] NLQ: Frontend query bar overlays the map, dims non-matching regions
- [ ] NLQ: `/` keyboard shortcut focuses the search bar
- [ ] Memo: `POST /deal-radar/memo/{region_id}` returns full HTML memo
- [ ] Memo: Gathers real data from all 5 source tables for the region
- [ ] Memo: Claude generates 6 sections (exec summary, market opp, signal analysis, target profile, risks, action)
- [ ] Memo: HTML rendered with design system components (kpi_card, data_table, etc.)
- [ ] Memo: Frontend modal displays the memo with close and print/download buttons
- [ ] Both endpoints handle missing data / LLM failures gracefully

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_nlq_parse_epa_filter | "high EPA violations" → epa_score >= 60 filter |
| T2 | test_nlq_parse_multiple_filters | "EPA and migration" → two filters |
| T3 | test_nlq_parse_cluster_status | "hot regions" → cluster_status = HOT |
| T4 | test_nlq_invalid_field_rejected | Filters with unknown fields are dropped |
| T5 | test_nlq_empty_query | Empty string returns all regions |
| T6 | test_nlq_valid_operators | Only >=, <=, =, >, < allowed |
| T7 | test_memo_data_gathering | Gathers scores + source stats for region |
| T8 | test_memo_html_has_sections | Output HTML contains all 6 section headings |
| T9 | test_memo_unknown_region | Returns 404 for invalid region_id |
| T10 | test_memo_fallback_on_llm_failure | Returns data-only memo if Claude is unavailable |

## Rubric Checklist

- [ ] Services follow existing patterns (LLMClient, ConvergenceEngine)
- [ ] All DB queries use parameterized SQLAlchemy text() — no string concatenation
- [ ] Filter fields validated against whitelist before query execution
- [ ] API endpoints return consistent JSON shapes
- [ ] Graceful degradation when LLM unavailable
- [ ] No hardcoded API keys in source code
- [ ] Design system components reused for memo HTML

## Design Notes

- **NLQ flow**: user query → Claude parses to `{filters, sort_by, explanation}` → backend validates filters against ALLOWED_FIELDS whitelist → builds safe WHERE clause → returns matching regions
- **Memo flow**: region_id → gather signal data from 5 source tables → build context dict → Claude generates 6 sections → render with design_system → return HTML
- Reuse: `LLMClient` from `app/agentic/llm_client.py`, design system from `app/reports/design_system.py`

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| app/services/deal_radar_nlq.py | Create | NL query parsing + execution |
| app/services/deal_radar_memo.py | Create | Memo data gathering + AI generation + HTML rendering |
| app/api/v1/deal_radar.py | Modify | Add POST /query and POST /memo/{region_id} |
| frontend/deal-radar.html | Modify | Add query bar + memo modal |
| tests/test_spec_029_deal_radar_nlq_memo.py | Create | Unit tests |

## Feedback History

_No corrections yet._
