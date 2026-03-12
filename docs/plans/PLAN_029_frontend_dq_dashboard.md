# PLAN 029: Frontend DQ Dashboard Tab

## Goal
Add a "Data Quality" tab to the frontend that surfaces the 744+ DQ recommendations with filtering, actions, and summary visualizations. Only touches `frontend/index.html`.

## Context
The backend DQ review API is live:
- `GET /api/v1/dq-review/recommendations` — list with filters (category, priority, status, source, table_name, limit, offset)
- `GET /api/v1/dq-review/summary` — counts by category, priority, status
- `POST /api/v1/dq-review/apply/{id}` — apply a recommendation
- `POST /api/v1/dq-review/dismiss/{id}` — dismiss a recommendation
- `POST /api/v1/dq-review/run` — trigger fresh analysis

## Steps

### 1. Add tab button + content container
- [ ] Add "Data Quality" tab button in the nav (between existing tabs)
- [ ] Add `<div id="tab-dq" class="tab-content hidden">` container
- [ ] Register in `showTab()` routing + URL path `/dq`

### 2. Summary header cards
- [ ] Row of 4 stat cards: Total Open, Critical, High, Auto-fixable
- [ ] Fetch from `/dq-review/summary` on tab load
- [ ] Color-code: critical=red, high=orange, medium=yellow, low=blue

### 3. Summary donut chart
- [ ] Chart.js donut showing recommendations by category
- [ ] Categories: missing_data, stale_data, low_coverage, schema_drift, cross_source_mismatch, enrichment_opportunity, anomaly, orchestration
- [ ] Click segment to filter the table below

### 4. Filter toolbar
- [ ] Priority dropdown (all, critical, high, medium, low)
- [ ] Category dropdown (all + 8 categories)
- [ ] Status dropdown (open, applied, dismissed)
- [ ] Source text input
- [ ] Table name text input
- [ ] "Run Analysis" button → `POST /dq-review/run`

### 5. Recommendations table
- [ ] Columns: Priority (colored badge), Category, Source, Table, Title, Description, Auto-fix?, Actions
- [ ] Auto-fix column: checkmark icon if `auto_fixable`
- [ ] Actions: "Apply" button (green, calls POST apply), "Dismiss" button (gray, calls POST dismiss)
- [ ] Pagination: load 50 at a time, "Load more" button
- [ ] Expandable rows: click to show `evidence` JSON and `fix_action`/`fix_params`

### 6. Styling
- [ ] Consistent with existing dark theme
- [ ] Priority badges: critical=red, high=orange, medium=amber, low=blue
- [ ] Category pills with distinct colors
- [ ] Smooth transitions on filter changes

### 7. Testing
- [ ] Verify tab loads and fetches summary
- [ ] Verify filters work (priority, category, status)
- [ ] Verify apply/dismiss update the card and remove from list
- [ ] Verify "Run Analysis" triggers and shows progress
- [ ] Verify donut chart renders and click-to-filter works

## Files touched
- `frontend/index.html` — ONLY this file (CSS + HTML + JS)

## Parallel work note
This tab only touches `frontend/index.html`. No backend changes. Tab 1 (commits) should commit the current frontend state BEFORE this tab starts editing, or this tab should wait for Tab 1 to commit first.
