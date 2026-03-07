# PLAN 021 — Datacenter Report Visual & Content Fixes

**Status: COMPLETE**

## Problem Statement

The datacenter site selection report had multiple visual and content issues:
1. Geographic Heat Map bars were cramped/stuck together
2. Executive Summary was washed out (low contrast, near-invisible on light bg)
3. Inconsistent color usage throughout — too many accent colors
4. CEO Overview "BUY/WATCH/PASS" verdict was inappropriate — should be analytical narrative
5. Hardcoded inline colors broke dark mode

## Checklist

### 1. Geographic Heat Map — Fix Bar Chart Spacing
- [x] **1a.** Removed fixed `barThickness: 28`, replaced with `barPercentage: 0.7` + `categoryPercentage: 0.8`
- [x] **1b.** Added `barPercentage` and `categoryPercentage` to Chart.js dataset config
- [x] **1c.** Dynamic chart height: `height=f"{max(300, len(labels) * 36)}px"` (540px for 15 labels)
- [x] **1d.** Added `layout: { padding: { top: 8, bottom: 8 } }` to Chart.js options
- [x] **1e.** Added `padding: 4px 0` to `.fb-row` fallback CSS bars

### 2. Executive Summary — Fix Contrast & Readability
- [x] **2c.** Replaced inline-styled gradient div with `callout("info")` from the design system
- [x] Proper dark mode support via design system callout CSS

### 3. Color Consistency — Tighten the Palette
- [x] **3a.** Replaced hardcoded hex colors in metric-item, deal-card, muted text with CSS variables
- [x] **3b.** Removed purple/orange/teal chart colors; connectivity chart uses BLUE_LIGHT, power uses GREEN
- [x] **3c.** Removed `.risk-card` CSS entirely (replaced with callout pattern)
- [x] **3d.** `.metric-item .value` → `var(--primary-light)`, `.label` → `var(--gray-500)`
- [x] **3e.** Added `.muted-text { color: var(--gray-500); font-size: 13px; }`, replaced all inline color styles
- [x] Cleaned up unused imports (ORANGE, RED, GRAY, PURPLE, TEAL, CHART_COLORS, build_doughnut_config, build_chart_legend)

### 4. CEO Overview — Replace Verdict with Data Narrative
- [x] **4a.** Removed BUY/WATCH/PASS verdict banner and all verdict CSS classes
- [x] **4b.** Removed verdict-banner/buy/watch/pass CSS from DATACENTER_EXTRA_CSS
- [x] **4c.** Rewrote `_compute_ceo_overview()` as narrative builder with 3 paragraphs:
  - Market Scope: county count, state count, avg/max scores, A-grade percentage with interpretation
  - Top Opportunities: top 3 counties with strongest/weakest domain analysis
  - Data Coverage: sources populated, gaps listed, recommended next steps
- [x] **4d.** Removed metric-grid from CEO overview (coverage info is in narrative now)
- [x] **4e.** Top recommendations rendered as proper `data_table()` with County, State, Score, Grade
- [x] **4f.** Risks replaced with `callout("warn")` and next steps with `callout("info")`

### 5. Dark Mode — Fix Hardcoded Colors
- [x] **5a.** `.ceo-overview` uses `var(--white)` with dark mode box-shadow override
- [x] **5b.** `.deal-card` dark mode override for border and recommended variant
- [x] **5c.** `.metric-item .value` uses `var(--primary-light)` (maps to #90cdf4 in dark)
- [x] **5d.** All inline styles replaced with CSS classes using variables

## Files Changed

- `app/reports/templates/datacenter_site.py` — DATACENTER_EXTRA_CSS, _compute_ceo_overview, render_html
- `app/reports/design_system.py` — build_horizontal_bar_config (bar sizing), .fb-row (padding)
- `tests/test_datacenter_site_report.py` — Updated 2 tests for new narrative structure

## Testing

- [x] `pytest tests/test_datacenter_site_report.py -v` — 9/9 passed
- [x] TX report regenerated and visually verified
- [x] National report regenerated and visually verified
