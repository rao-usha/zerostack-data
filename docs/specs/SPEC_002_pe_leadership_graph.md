# SPEC_002: PE Leadership Graph

## Summary
Seed realistic leadership data for all 24 portfolio companies and build an interactive D3 force-directed network visualization showing the leadership graph across a firm's portfolio.

## Problem
The PE demo seeder seeds 232 rows but leaves `pe_company_leadership` empty, causing Management Quality to score F in exit readiness. This is a critical gap for demo credibility.

## Acceptance Criteria
1. **Seed Data**: ~54 executives across 24 companies (2-3 C-suite per company: CEO, CFO, COO/CTO)
2. **Board Links**: PE firm partners sitting on portfolio company boards (cross-links)
3. **API Endpoint**: `GET /pe/leadership-graph/{firm_id}` returns graph JSON with nodes and links
4. **D3 Visualization**: Interactive force-directed graph in frontend PE Intelligence tab
5. **Exit Readiness**: Management Quality improves from F to B+ or better for seeded companies
6. **Idempotent**: Re-running seed produces same results (no duplicates)

## API Contract

### GET /pe/leadership-graph/{firm_id}
Response:
```json
{
  "firm_id": 166,
  "firm_name": "Summit Ridge Partners",
  "nodes": [
    {"id": "firm_166", "name": "Summit Ridge Partners", "type": "firm"},
    {"id": "person_1", "name": "James Harrington", "type": "pe_person", "title": "Managing Partner"},
    {"id": "company_1", "name": "MedVantage Health Systems", "type": "company", "industry": "Healthcare"},
    {"id": "exec_50", "name": "Dr. Robert Chen", "type": "executive", "title": "CEO"}
  ],
  "links": [
    {"source": "firm_166", "target": "person_1", "type": "employment"},
    {"source": "person_1", "target": "company_1", "type": "board_seat"},
    {"source": "company_1", "target": "exec_50", "type": "management"}
  ]
}
```

## Test Cases
1. Seed inserts ~54 executive PEPerson records and ~54 PECompanyLeadership records
2. Board links connect PE partners to portfolio companies
3. API returns valid graph JSON with all 4 node types
4. Each active portfolio company has at least 2 leadership records
5. Exit readiness Management Quality scores >= 70 for seeded companies
6. Re-seeding is idempotent (no duplicate records)

## Files
- `app/sources/pe/demo_seeder.py` — Add executive data + leadership records
- `app/api/v1/pe_benchmarks.py` — Add leadership-graph endpoint
- `frontend/index.html` — Add D3 force-directed visualization
- `tests/test_spec_002_pe_leadership_graph.py` — Tests
