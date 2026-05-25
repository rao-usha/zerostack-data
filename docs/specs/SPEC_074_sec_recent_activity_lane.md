# SPEC 074 — SEC Recent Activity lane + state-grain filers layer

**Status:** Draft
**Task type:** report (wire-up of already-ingested SEC data — no new ingest needed)
**Date:** 2026-05-25
**Plan:** PLAN_067 §3 SPEC_074 (was "SEC filings with dates" — pre-flight confirmed dates already exist)
**Test file:** `tests/test_spec_074_sec_recent.py`
**Builds on:** SPEC_067 RECENT_SOURCES whitelist; existing `app/sources/sec/` ingestor + populated tables on cloud.

## Goal

PLAN_067 §3 originally feared `sec_company_metadata` had no
`filing_date`. Pre-flight surfaced the truth: the SEC ingestor has been
running and `sec_10k` / `sec_10q` / `sec_balance_sheet` /
`sec_income_statement` / `sec_cash_flow_statement` all have populated
`filing_date` columns. The most recent filing date on cloud is
**2026-05-11** — fresher than FEMA (2026-02-20).

Two thin additive surfaces:

1. **Wire the SEC lane into the Recent Activity feed** (SPEC_067).
   `RECENT_SOURCES.add('sec')`; SEC items render alongside FEMA items
   with their own type-chip color. Item-click opens the SEC EDGAR
   filing URL in a new tab (no need for a county map jump — SEC data
   is state-grain at best).
2. **Bonus state-grain layer** `econ_sec_active_filers`: count of
   distinct CIKs per `business_state`, surfaced as a regular Atlas
   layer. Cheap — single SQL query, no new ingest.

## Pre-flight findings (logged 2026-05-25)

| Table | Rows | filing_date | report_date | Latest |
|---|---|---|---|---|
| `sec_10k`           | 789   | ✓ 789      | ✓ 789      | 2025-11-25 |
| `sec_10q`           | 2,475 | ✓ 2,475    | ✓ 2,475    | 2025-11-26 |
| `sec_balance_sheet` | 97k   | ✓ 97k      | (period)   | 2026-05-11 |
| `sec_income_statement` | 97k | ✓ 97k     | (period)   | 2026-05-11 |
| `sec_cash_flow_statement` | 97k | ✓ 97k  | (period)   | 2026-05-11 |
| `sec_company_metadata` | 2,306 | (no dates) | — | — |
| `sec_8k` | 0 | empty — ingestor exists but hasn't run | | |

`sec_company_metadata` carries `business_state` (e.g. `'IL'`) but no
city or county — so SEC items are **state-grain only**. The Recent
Activity item gets `place_id` = 2-digit state FIPS, `place_name` =
company name + state code.

## Acceptance Criteria

### Server
- [ ] `RECENT_SOURCES` adds `'sec'`.
- [ ] `_fetch_recent_sec(db, limit)` returns items merging `sec_10k`
      + `sec_10q` by `filing_date desc`. Each item has:
      `{source: 'sec', event_id: accession_number, date, type, title,
       place_id: state_fips, place_name, url: filing_url}`.
- [ ] `fetch_recent_events(db, ['sec'], 50)` and
      `fetch_recent_events(db, ['fema', 'sec'], 50)` both work; mixed
      results sorted by date desc.
- [ ] State→FIPS map (50 states + DC + 6 territories) lives in
      `app/services/atlas/state_fips.py` (reusable).
- [ ] `GET /atlas/recent?sources=sec&limit=10` returns ≥10 items.

### Bonus layer
- [ ] New layer `econ_sec_active_filers` registered in `layers.py` —
      state grain, vintage "latest", reads
      `SELECT business_state, COUNT(DISTINCT cik) FROM sec_company_metadata GROUP BY business_state`.
- [ ] `GET /atlas/layer/econ_sec_active_filers` returns ≥40 state values.

### Frontend
- [ ] Recent Activity item with `url` field → click opens URL in
      new tab + records telemetry.
- [ ] Recent Activity item without `url` (FEMA) → keeps existing
      behavior (map fit + place panel).
- [ ] SEC source items render with a source-prefix chip ("SEC") so
      users can tell sources apart.
- [ ] Type chip colors for "10-K" / "10-Q" / "8-K" added.
- [ ] Atlas tour mentions the new SEC lane + layer.

### Quality
- [ ] Smoke harness adds a scenario for the merged-sources panel
- [ ] No regression — existing 36/36 smoke scenarios still pass

## Test Cases

| ID | What | Where |
|---|---|---|
| T1 | `_fetch_recent_sec` returns ≥10 items with required fields | pytest |
| T2 | Items have place_id = 2-digit state FIPS (or null if state missing) | pytest |
| T3 | Item urls are well-formed SEC EDGAR URLs | pytest |
| T4 | Merged `['fema', 'sec']` request returns items sorted by date desc | pytest |
| T5 | `/atlas/layer/econ_sec_active_filers` returns ≥40 state values | pytest |
| T6 | New layer in registry (`/atlas/layers`) under economy domain | pytest |
| T7 | Smoke harness `recent-merged-fema-sec` scenario | smoke_atlas.py |

## Design Notes

### State→FIPS lookup
A small static dict in a new file `app/services/atlas/state_fips.py`.
Maps USPS abbreviations to 2-digit FIPS. Lives at the service layer
so the SEC builder and any future state-grain source can reuse it.

### SEC item URL — what `filing_url` looks like
`sec_10k.filing_url` already populated like
`https://www.sec.gov/Archives/edgar/data/{cik}/{accession}/index.html`.
Use it directly — no construction needed.

### Merged sort
The existing `fetch_recent_events` already does `items.sort(key=date desc)`.
Just add the SEC source; the merge sort handles ordering naturally.

### Optional `sec_8k` later
The `sec_8k` table is empty; the ingestor exists but hasn't run.
Triggering the ingest is out of scope for SPEC_074. When it lands,
`_fetch_recent_sec` extends trivially to UNION ALL `sec_8k`.

## Files to Create / Modify

| File | Action | Description |
|---|---|---|
| `docs/specs/SPEC_074_sec_recent_activity_lane.md` | Create | This file |
| `docs/specs/.active_spec` | Modify | → `SPEC_074_sec_recent_activity_lane` |
| `tests/test_spec_074_sec_recent.py` | Create | T1-T6 |
| `app/services/atlas/state_fips.py` | Create | Static USPS→FIPS map |
| `app/services/atlas/series.py` | Modify | Add `_fetch_recent_sec` + extend `RECENT_SOURCES` |
| `app/services/atlas/layers.py` | Modify | Add `_build_sec_active_filers` + layer entry |
| `frontend/atlas.html` | Modify | Item-click `url` branch + source chip + 10-K/10-Q type colors |
| `scripts/smoke_atlas.py` | Modify | Add `recent-merged-fema-sec` scenario |
| `docs/ATLAS_TOUR.md` | Modify | Document SEC lane + new layer |

## Verification (manual)

1. `curl 'http://localhost:8001/api/v1/atlas/recent?sources=sec&limit=5'`
   → ~5 recent 10-K/10-Q items with company names + filing dates.
2. `curl 'http://localhost:8001/api/v1/atlas/recent?sources=fema,sec&limit=20'`
   → mixed feed, sorted by date desc.
3. `curl 'http://localhost:8001/api/v1/atlas/layer/econ_sec_active_filers' | jq '.values | length'`
   → returns ≥40 state values.
4. Open `/atlas.html?recent=1` then change source via UI (or hit
   `/recent?sources=sec`) — SEC items render with source chip; click
   one → opens filing URL.
5. Smoke harness `python scripts/smoke_atlas.py` → all scenarios pass.

## Feedback History

_No corrections yet._
