# SPEC 073 — Atlas headless-Chrome smoke harness

**Status:** Draft
**Task type:** report (frontend / e2e verification)
**Date:** 2026-05-24
**Plan:** PLAN_072 Wave 1
**Test file:** `tests/test_spec_073_atlas_smoke.py`
**Builds on:** `docs/ATLAS_TOUR.md`, SPEC_065/066/066b layer + endpoint surface

## Goal

A headless-Chrome harness that runs `docs/ATLAS_TOUR.md` end-to-end
and reports pass/fail per scenario. Catches the regressions that
otherwise show up only after Phase-2 measurement starts (and bias it).

Every future spec ships against this baseline.

## Acceptance Criteria

- [ ] New script `scripts/smoke_atlas.py` that runs:
      - **L** = one scenario per registered Atlas layer (currently 20)
      - **D** = one scenario per dynamic feature (5: bivariate, scatter,
        brushed histogram, migration arcs, sparklines)
      - **P** = one scenario per featured place deep-dive (8 from
        ATLAS_TOUR.md §Compelling place deep-dives)
- [ ] Per scenario the harness:
      - GETs the right `/atlas.html?…` URL via headless Chrome
      - Captures the JS console (errors fail the scenario)
      - DOM-dumps and asserts the scenario-specific selector is present
        + populated
      - Records elapsed ms
- [ ] Outputs:
      - `docs/atlas_smoke_report.csv` — one row per scenario:
        `category, scenario, url, status, selector_asserted, ms, error`
      - Console summary: total / pass / fail / pass-rate%
- [ ] Exit code: 0 on all-pass, 1 on any fail (CI-friendly)
- [ ] Total run ≤ 5 minutes on the dev machine
- [ ] `tests/test_spec_073_atlas_smoke.py` runs the harness as a
      single test (skipped if `ATLAS_SMOKE=1` env var not set — heavy)

## Test Cases (the harness IS the test surface)

| Cat | Scenario | Selector asserted |
|---|---|---|
| L | each `/layers` registry id, county | `.leaflet-overlay-pane svg path[fill]` non-empty |
| L | each registry id, point | `.leaflet-overlay-pane circle` count ≥ 50 |
| L | each registry id, state | `.leaflet-overlay-pane svg path[fill]` non-empty + ≥40 features |
| D | bivariate `?layer=A&compare=B` | `#bivariate-legend .cell` count == 9 |
| D | scatter (URL trigger → JS opens overlay) | (skip in harness — requires button click) |
| D | brushed histogram | `#brush-host svg .brush` present |
| D | migration arcs `?layer=demo_irs_migration_net_agi` | `svg.atlas-arc-overlay path.arc` count ≥ 50 |
| D | sparklines (place panel) | `.lspark svg` count ≥ 1 |
| P | each place `?place={fips}` | `#place-content` visible + `.place-layer` count ≥ 4 |

(Scatter requires a real button click; deferred to Wave 2 of SPEC_073
or to manual verification.)

## Design Notes

### Stack
- Python `subprocess` driving the local Chrome `--headless`
- `--dump-dom` + `--enable-logging=stderr --log-level=0` for assertions
- No Selenium / Playwright dependency — keeps the spec tiny and the
  CI footprint trivial (Chrome is already installed)
- Selector assertions via regex on the dumped DOM string (not a real
  DOM parser — simpler, robust enough for "present and non-empty")

### URL construction
Reads the live `/atlas/layers` registry at start to enumerate
layer IDs. The 8 deep-dive places + 5 dynamic-feature deep-links
come from a constants block at the top of the script (kept in sync
with `ATLAS_TOUR.md`).

### Console-error policy
A scenario fails if the captured stderr contains a JS error pattern
that's not on a small allowlist (USB warnings, GPU init noise,
font fallback). The allowlist is regex-based and lives in the script.

### Output schema
```csv
category,scenario,url,status,selector_asserted,ms,error
L,disaster_nri (county),http://localhost:3001/atlas.html?layer=disaster_nri,pass,.leaflet-overlay-pane svg path[fill] >0,3127,
D,bivariate,http://localhost:3001/atlas.html?layer=demo_irs_county_agi_per_return&compare=disaster_nri,pass,#bivariate-legend .cell ==9,3902,
P,Harris Co TX (48201),http://localhost:3001/atlas.html?place=48201,pass,.place-layer >=4,2944,
```

### Performance budget
- ~3-5 seconds per scenario (page load + virtual-time-budget)
- ~33 scenarios → ~3 minutes wall clock
- Acceptable for pre-deploy gate or on-demand verification, not for
  every-commit CI (yet)

## Files to Create / Modify

| File | Action | Description |
|---|---|---|
| `docs/specs/SPEC_073_atlas_smoke_harness.md` | Create | This file |
| `docs/specs/.active_spec` | Modify | → `SPEC_073_atlas_smoke_harness` |
| `tests/test_spec_073_atlas_smoke.py` | Create | Runs harness when `ATLAS_SMOKE=1` |
| `scripts/smoke_atlas.py` | Create | The harness |
| `docs/atlas_smoke_report.csv` | Create (output) | Generated on each run; committed snapshot |

## Verification (manual)

```bash
ATLAS_SMOKE=1 pytest tests/test_spec_073_atlas_smoke.py -v
# OR
python scripts/smoke_atlas.py
# → prints summary, writes docs/atlas_smoke_report.csv
# → exit 0 if all pass
```

## Feedback History

_No corrections yet._
