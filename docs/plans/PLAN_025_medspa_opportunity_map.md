# PLAN 025 — Medspa Market Opportunity Map

**Status:** Draft
**Date:** 2026-03-11

## Goal

Build an interactive Leaflet.js map visualization showing 4,489 medspa prospects with state choropleth, clustered markers, filters, and stats — as a self-contained HTML report.

## Approach

Extend `design_system.py` with Leaflet helpers, create a new report template, register it in builder.

## Checklist
- [ ] Add Leaflet.js helpers to `design_system.py`
- [ ] Create `medspa_opportunity_map.py` template with `gather_data()` + `render_html()`
- [ ] Register template in `builder.py`
- [ ] Restart API and test end-to-end
- [ ] Log session checkpoint

## Files to Change
| File | Action | Description |
|------|--------|-------------|
| `app/reports/design_system.py` | Modify | Add `leaflet_head()`, `map_container()` |
| `app/reports/templates/medspa_opportunity_map.py` | Create | Full map template |
| `app/reports/builder.py` | Modify | Register template |

## Verification
1. Restart API, generate report via POST endpoint
2. Open HTML in browser — verify map, markers, popups, filters, choropleth
