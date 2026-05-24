# SPEC 072 — County broadband (ACS B28002 subscription rate)

**Status:** Draft
**Task type:** collector (extends SPEC_070 county-ACS path) + atlas layer wire-up
**Date:** 2026-05-24
**Plan:** PLAN_067 §3 (Atlas Data Coverage Expansion — Phase 3a)
**Test file:** `tests/test_spec_072_county_broadband.py`
**Builds on:** SPEC_070 county-ACS ingest path; SPEC_065 layer registry.

## Goal

Add a real **county-grain broadband layer** to the Atlas. PLAN_066 ships
the FCC layer as state-only; PLAN_067 §3 SPEC_072's original ambition
was "re-ingest FCC broadband at county grain — providers, technologies,
max speeds per county."

## Pre-flight findings (logged 2026-05-24)

- `fcc_broadband_coverage`: 10,145 rows, **all `geography_type='state'`**.
- `broadband_availability`: 51 rows (one per state + DC) — derived
  state-level summary (fiber/cable/DSL flags, max speeds, broadband_pct,
  gigabit_pct, provider_competition). State-only.
- `fcc_broadband_summary`: similar shape; state-only.
- No FCC ingestor under `app/sources/`.
- **FCC National Broadband Map public county endpoint** returns
  `HTTP 405 Method Not Available` (`https://broadbandmap.fcc.gov/api/public/map/county/{fips}`),
  so the cheap-API route to BDC county data is gone.
- The BDC alternative is per-state CSV bulk downloads — millions of
  block-level rows, plus a CSV-parse-and-aggregate pipeline. Significant
  scope for a single spec; appropriate as its own multi-day SPEC.
- **Census ACS B28002 (Internet Subscription) at county grain works in
  one Census API call**: 3,222 county rows, returning total households +
  households with a broadband subscription. Live-tested against
  `for=county:*` filter. Same pattern as SPEC_070's B19013.

## Scope decision — ship ACS broadband subscription now; defer FCC BDC

PLAN_067's intent for SPEC_072 was "a county-grain connectivity layer."
There are two honest readings of "connectivity":

| Path | Data source | Meaning | Effort |
|---|---|---|---|
| **A (this spec)** | ACS B28002 county | demand: households *subscribed* to broadband | reuses SPEC_070 path · ships today |
| **B (deferred)** | FCC BDC bulk CSVs | supply: providers/technologies/max speeds *available* | multi-day · own SPEC |

Path A ships immediately, gives the user real per-county broadband
signal, and is more directly useful for many questions ("where is
broadband actually adopted, not just on paper"). Path B is the
deeper-data dataset and is reasonable as **SPEC_072b** when the FCC BDC
ingest is the focused work.

**This spec ships A.** The layer is named honestly:
`infra_broadband_subscription` — not `infra_broadband_coverage`.

## Acceptance Criteria

- [ ] New table `acs5_county_2023_b28002` exists with schema
      `(geo_id text PRIMARY KEY, state_fips text, geo_name text,
       b28002_001e integer, b28002_004e integer, ingested_at timestamp)`.
- [ ] ≥3,000 rows landed.
- [ ] `geo_id` is the 5-digit county FIPS for every row; no nulls.
- [ ] `app/sources/census/county_acs.py` extended with a multi-variable
      ingester `ingest_county_acs_multi(db, year, table_id, variables)`.
      The original `ingest_county_acs_variable` continues to work
      (shimmed → multi).
- [ ] One-shot run script
      `scripts/ingest_acs_county_b28002.py`.
- [ ] New Atlas layer `infra_broadband_subscription` registered in
      `app/services/atlas/layers.py` — county grain, vintage
      "2023 ACS 5-year", reading the B28002 county table, builder
      computes `pct = with_broadband / total * 100`.
- [ ] `GET /api/v1/atlas/layer/infra_broadband_subscription` returns
      ≥3,000 county values with legend min ~50%, max ~100%.
- [ ] `GET /api/v1/atlas/place/48201` (Harris Co TX) now includes a
      broadband-subscription percentage.
- [ ] SPEC_070 (`demo_acs_median_income`) still passes — the refactor
      to multi-variable must be backward compatible.

## Test Cases

| ID | Test | What it verifies |
|---|---|---|
| T1 | `test_ingest_produces_county_rows` | ≥3,000 county rows for B28002 |
| T2 | `test_pct_values_are_realistic` | All pct ∈ [40, 100] — no impossibilities |
| T3 | `test_geo_ids_are_5_digit_county_fips` | every geo_id matches `^\d{5}$` |
| T4 | `test_layer_appears_in_registry` | `/atlas/layers` includes `infra_broadband_subscription` |
| T5 | `test_layer_returns_county_values` | `/atlas/layer/...` returns ≥3,000 |
| T6 | `test_spec_070_backward_compat` | `demo_acs_median_income` layer still works after refactor |

## Design Notes

### Generalize county_acs.py — multi-variable
```python
def ingest_county_acs_multi(db, year, table_id, variables: List[str]):
    """Fetch N ACS scalars at county grain in one API call.
    Schema is generic: one column per variable, named lowercase of
    variable code (e.g. B28002_001E → b28002_001e)."""
    ...
def ingest_county_acs_variable(db, year=2023, table_id='B19013',
                                variable='B19013_001E'):
    return ingest_county_acs_multi(db, year, table_id, [variable])
```

### Table schema (generic — one column per ACS variable)
```sql
CREATE TABLE IF NOT EXISTS acs5_county_2023_b28002 (
    geo_id      TEXT PRIMARY KEY,
    state_fips  TEXT NOT NULL,
    geo_name    TEXT,
    b28002_001e INTEGER,   -- total households
    b28002_004e INTEGER,   -- households with broadband subscription
    ingested_at TIMESTAMP DEFAULT NOW()
);
```

### Layer builder
```python
def _build_broadband_subscription(db) -> LayerResult:
    rows = _safe_query(db, """
        SELECT geo_id,
               (b28002_004e::numeric / NULLIF(b28002_001e,0) * 100) AS value
        FROM acs5_county_2023_b28002
        WHERE b28002_001e IS NOT NULL AND b28002_001e > 0
          AND b28002_004e IS NOT NULL
    """)
    values = {r["geo_id"]: float(r["value"]) for r in rows if r["geo_id"] and r["value"] is not None}
    return LayerResult(
        layer_id="infra_broadband_subscription", grain="county", values=values,
        legend=_legend(list(values.values()), "% households with broadband subscription"),
        provenance=[{"table": "acs5_county_2023_b28002", "rows": len(values)}],
    )
```

### What we keep / change
- Keep `infra_fcc_providers_state` as-is (state-grain layer remains useful
  as a coarser fallback; doesn't lie).
- Add `infra_broadband_subscription` (county). Two layers in the same
  domain at different grains — both honest about their grain.

## Files to Create / Modify

| File | Action | Description |
|---|---|---|
| `docs/specs/SPEC_072_county_broadband.md` | Create | This file |
| `docs/specs/.active_spec` | Modify | → `SPEC_072_county_broadband` |
| `tests/test_spec_072_county_broadband.py` | Create | T1-T6 |
| `app/sources/census/county_acs.py` | Modify | Add multi-variable ingester; shim single-variable for SPEC_070 backward compat |
| `scripts/ingest_acs_county_b28002.py` | Create | One-shot runner |
| `app/services/atlas/layers.py` | Modify | Add `infra_broadband_subscription` builder + layer entry |

## Verification (manual)

1. `python scripts/ingest_acs_county_b28002.py` → ≥3,000 rows.
2. `docker-compose restart api`.
3. `curl /atlas/layers | jq '.layers_by_domain.infrastructure'` shows
   the new layer.
4. `curl /atlas/layer/infra_broadband_subscription | jq '.values | length'`
   returns ≥3,000.
5. `curl /atlas/place/48201` shows Harris County's broadband pct
   (~93-95% — Houston metro is high).

## Feedback History

_No corrections yet._
