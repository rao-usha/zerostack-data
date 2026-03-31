# SPEC 029 — Org Chart Dedup, Board Tier, Network Limiting, Map Context

**Status:** Draft
**Task type:** bug_fix
**Date:** 2026-03-27
**Test file:** tests/test_spec_029_orgchart-dedup-network-limit-map-context.py

## Goal

Fix three D3 visualization issues: (1) org chart shows duplicate people (e.g., "Andrew Sullivan" and "Andrew F. Sullivan") and buries board members in the exec tree — dedup at API level and restructure to show Board of Directors above CEO; (2) network graph loads 1000+ nodes in production (unbounded portfolio companies), making the browser unresponsive — add API cap and frontend limit control; (3) map shows scores with no explanation — add methodology panel and score tier labels.

## Acceptance Criteria

- [ ] `GET /people-analytics/companies/{id}/org-chart` deduplicates people by person_id (no duplicate entries)
- [ ] Name-based dedup strips middle initials so "Andrew Sullivan" and "Andrew F. Sullivan" are treated as one person
- [ ] Org chart frontend shows "Board of Directors" as visual root with board members, CEO below
- [ ] Root node no longer labeled "Organization" — replaced by proper board/CEO hierarchy
- [ ] `GET /pe/network-graph?limit=10&max_per_firm=5` returns at most 10 PE firms × 5 portfolio companies = 55 nodes max
- [ ] `max_per_firm` defaults to 5, is capped at 20
- [ ] Network frontend has a "Top N firms" dropdown (10/20/30), sends limit to API, shows node count
- [ ] Map shows a "How to Read This" methodology panel explaining what score factors mean
- [ ] Map legend has "Low suitability" / "High suitability" end labels
- [ ] Map tooltip shows score tier label (Top tier / Strong / Emerging / Developing)

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_sanitize_deduplicates_by_person_id | Same person_id in two nodes → only one survives |
| T2 | test_sanitize_keeps_richer_node | Node with children is kept over leaf duplicate |
| T3 | test_sanitize_name_normalization | "Andrew F. Sullivan" and "Andrew Sullivan" deduplicated |
| T4 | test_sanitize_empty_chart | Empty/null chart_data returns safely |
| T5 | test_network_max_per_firm_param | max_per_firm=2 limits portfolio companies per firm |
| T6 | test_network_default_limit | Default call returns ≤20 firms, ≤5 companies each |
| T7 | test_normalize_name_strips_middle_initial | _normalize_name helper strips "F." from "Andrew F. Sullivan" |

## Rubric Checklist

- [ ] Root cause identified and documented (not just symptoms)
- [ ] Fix addresses root cause (not a workaround unless justified)
- [ ] Regression test added that fails before fix, passes after
- [ ] Existing tests still pass after fix
- [ ] No new security vulnerabilities introduced (OWASP top 10)
- [ ] Fix is minimal — no unrelated changes bundled in
- [ ] Edge cases considered (null, empty, malformed inputs)
- [ ] If DB-related: uses parameterized queries, no raw SQL concat
- [ ] If API-related: error responses follow error hierarchy
- [ ] Commit message references the bug/issue being fixed

## Design Notes

**Root cause — org chart duplicates:**
- `OrgChartBuilder._build_chart_json()` builds the tree from `company_people` rows. Multiple collection runs (SEC + Website) can create two `company_people` rows for the same person with different `company_person_id`. The `PersonNameMatcher` doesn't auto-merge "Andrew Sullivan" vs "Andrew F. Sullivan" (similarity ~0.85, below 0.95 auto-merge threshold) — these land in the review queue but never get manually merged. Both records appear as separate root nodes in the stored `chart_data` JSON, triggering the virtual "Organization" wrapper.
- Fix: `_sanitize_org_chart()` in `get_company_org_chart()` — dedup by person_id, then by normalized name (strip middle initial + suffix).

**Root cause — network graph overload:**
- API returns all portfolio companies for each PE firm via `PEFundInvestment` join. No per-firm cap. With 20 large PE firms, each holding 50–200 companies, the response can be 1000+ nodes. Frontend never sends `limit`, always gets API default (20 firms, uncapped companies).
- Fix: Add `max_per_firm` query param to API, apply per-firm limit in query. Frontend sends both `limit` and `max_per_firm=5`.

**Board tier restructure:**
- `restructureForDisplay(root)` in `orgchart.html` — called after API response, before `transformOrgNode()`. Separates board members from the root children, wraps them in a synthetic "Board of Directors" node, places CEO as last child (appearing at bottom of board tier in tree layout).

**_normalize_name algorithm:**
```python
name = name.lower().strip()
name = re.sub(r'\s[a-z]\.\s', ' ', name)   # "andrew f. sullivan" → "andrew sullivan"
name = re.sub(r'\b(jr|sr|ii|iii|iv|phd|md|esq)\.?\b', '', name)
name = re.sub(r'\s+', ' ', name).strip()
```

**Network max_per_firm implementation:**
- In `get_pe_network_graph()`, after getting firms, iterate per firm and limit portfolio company query with `.limit(max_per_firm)`.

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| `app/api/v1/people_analytics.py` | Modify | Add `_normalize_name`, `_count_desc`, `_sanitize_org_chart`; call sanitize in `get_company_org_chart` |
| `app/api/v1/pe_benchmarks.py` | Modify | Add `max_per_firm` Query param; limit portfolio companies per firm |
| `frontend/d3/orgchart.html` | Modify | Add `restructureForDisplay()`; Board dept color; call restructure in `loadCompany()` |
| `frontend/d3/network.html` | Modify | Firm limit dropdown; pass `limit`+`max_per_firm` to API; node count display |
| `frontend/d3/map.html` | Modify | Methodology panel; legend end labels; score tier in tooltip |

## Feedback History

_No corrections yet._
