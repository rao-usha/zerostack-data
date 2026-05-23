# SPEC 065 — Atlas Layer-Data + Boundaries API

**Status:** Draft
**Task type:** service
**Date:** 2026-05-23
**Plan:** PLAN_066 v3 (Atlas: The General Public-Data Explorer)
**Test file:** tests/test_spec_065_atlas_layer_api.py
**Builds on:** SPEC_064 (Atlas engine — resolver/cards/graph/telemetry, shipped `be1bb97`); SPEC_060 (NAICS/MSA taxonomies)

## Goal

Build the data foundation of the general Atlas explorer: serve every Nexdata
public-data table that can be a map layer, with **honest grain and vintage**,
plus the boundary geometry and a single-place aggregation endpoint that
powers click-a-place drill-down.

The work has **two phases**:

1. **Catalog audit (pre-flight)** — classify every cloud table by domain,
   geo grain, vintage, and coverage; produce the layer registry that
   PLAN_066 v3 §4 sketched. This is the data-honest foundation; without it
   the registry is guesswork.
2. **Implementation** — `layers.py`, `boundaries.py`, four endpoints. Bake
   in the verified correctness from the data review (FDIC join, IRS
   `county_code`, FCC state-only, USAspending excluded, ACS deferred).

## Acceptance Criteria

- [ ] Catalog audit committed at
      `data/reference/atlas_layer_catalog_<date>.csv` (and JSON) — one row
      per candidate cloud table with: `table`, `row_count`, `domain`,
      `geo_grain` (`county`|`state`|`zcta`|`point`|`national`), `geo_key`,
      `vintage`, `coverage_pct` (against US baseline where applicable),
      `verdict` (✅/🟡/🔴/⏸), `notes`.
- [ ] Layer registry in `app/services/atlas/layers.py` carries ≥12 layers
      across ≥6 domains; each layer has `id`, `label`, `domain`, `grain`,
      `default_on`, `vintage`, `coverage_note`, plus a query builder.
- [ ] **Honest correctness baked in (the data-review findings):**
  - FDIC layer joins `cert`→`fdic_institutions.stcnty` for county values.
  - IRS county layer keys on `county_code` directly (it is the 5-digit FIPS).
  - FCC broadband layer ships at **state** grain (no county data in the table).
  - Census CBP industry layer ships at **state** grain (county on drill-down).
  - **USAspending NOT registered as a layer** — re-rated when PLAN_067 SPEC_071 lands.
  - **ACS NOT registered as a county layer** — ZIP-keyed; re-rated when PLAN_067 SPEC_070 lands.
- [ ] `app/services/atlas/boundaries.py` serves `geojson_boundaries` with
      server-side simplified geometry; payload measured + tuned in pre-flight.
- [ ] Four endpoints on `app/api/v1/atlas.py` (extend, don't replace):
  - `GET /api/v1/atlas/layers` — registry, grouped by domain
  - `GET /api/v1/atlas/layer/{id}` — choropleth `{geo_id: value}` + legend,
    or point GeoJSON `FeatureCollection`
  - `GET /api/v1/atlas/boundaries?geo_level=county|state` — boundary GeoJSON
  - `GET /api/v1/atlas/place/{geo_id}` — multi-layer aggregate values for one
    place (the click-a-place data backbone)
- [ ] All SQL parameterized; weak/failing queries return `[]` and rollback
      (the SPEC_061 lesson — `_safe_query` must rollback to avoid poisoning
      the session).
- [ ] No regression: full SPEC_052..064 + config suite stays green.

## Test Cases

| ID | Test | Verifies |
|---|---|---|
| T1 | test_registry_has_layers_across_domains | ≥12 layers across ≥6 domains; required fields present |
| T2 | test_registry_excludes_usaspending_and_acs_county | Honest cuts from PLAN_066 §4 enforced in code |
| T3 | test_registry_groups_by_domain | Endpoint groups layers under `domain` keys, deterministic order |
| T4 | test_choropleth_layer_returns_fips_value_map | A registered choropleth layer returns `{fips: number}` shape |
| T5 | test_point_layer_returns_geojson_featurecollection | A registered point layer returns valid GeoJSON FeatureCollection |
| T6 | test_unknown_layer_id_returns_404 | `GET /layer/does-not-exist` → 404 |
| T7 | test_fdic_layer_county_join | FDIC layer values keyed by 5-digit `stcnty` (the join works) |
| T8 | test_irs_county_uses_county_code_directly | IRS layer keys on `county_code` (5-digit FIPS), not concat |
| T9 | test_boundaries_endpoint_returns_county_geojson | `?geo_level=county` returns FeatureCollection with ≥3,000 features |
| T10 | test_place_endpoint_returns_multilayer_aggregate | `GET /place/{county_fips}` returns ≥4 layer values for a real county |
| T11 | test_safe_query_rolls_back_on_failure | A deliberately bad query inside a layer builder doesn't poison the session |
| T12 | test_layer_declares_honest_grain | Each layer's declared `grain` matches what its data actually has (FCC=state, CBP=state, etc.) |

## Rubric Checklist

_(No `service.md` rubric in memory — generic service checklist.)_

- [ ] Pure-Python module; no DB at import time. DB calls only inside query
      builders.
- [ ] All SQL parameterized via `text(...)` + `:param` bindings.
- [ ] All weak queries wrapped to rollback on failure (carry the SPEC_061
      `_safe_query` pattern explicitly).
- [ ] Layer query builders cached where deterministic (an `@lru_cache` on a
      small `(layer_id, geo_filter)` key for short TTL is acceptable; bias
      simplicity over premature caching).
- [ ] No PII / no scraping / public-data only.
- [ ] Endpoints public (no auth) — matches the rest of the Atlas router.
- [ ] No external dependencies beyond what's already in the project.
- [ ] Logger used; no `print()`. Defensive: any layer failure logs WARN +
      degrades to "no data" for that layer, never breaks the registry response.

## Design Notes

### Phase 1: Catalog audit (pre-flight)

**Script:** `scripts/audit_atlas_layer_catalog.py` (one-off, idempotent).
Runs against cloud. For every public-data table in cloud's `public` schema
(filter out infra tables — `*_jobs`, `*_log`, `dataset_registry`, etc.),
inspects:
- row count (via `pg_class.reltuples` for speed)
- geo columns present (`county_fips`, `state_fips`, `state_code`,
  `state` (alpha), `geo_id`, `latitude`/`longitude`, etc.)
- a date column (`year` / `date` / similar) for vintage
- a small `count(distinct geo_key)` probe to measure coverage
Classifies each table into:
- **domain** — manually mapped from the table name + ingest module
  (disaster, environment, energy, transport, demographics, economy, finance,
  health, real_estate, trade, federal, agriculture, markets, infra)
- **geo_grain** — `county` / `state` / `zcta` / `point` / `national` /
  `n_a`
- **vintage** — min/max year or date
- **verdict** — ✅ (full, current, mappable), 🟡 (partial / coarse / stale),
  🔴 (thin / wrong grain / unusable), ⏸ (needs PLAN_067 backfill)

Output: `data/reference/atlas_layer_catalog_2026-05-23.csv` + a JSON
companion at `data/reference/atlas_layer_catalog_2026-05-23.json` consumed
by `layers.py` at import time. The CSV is the human-readable artifact; the
JSON is the machine-readable source of truth for the registry.

### Phase 2: Implementation

#### `app/services/atlas/layers.py`

```python
@dataclass
class LayerSpec:
    id: str
    label: str
    domain: str           # 'disaster' | 'environment' | ...
    grain: str            # 'county' | 'state' | 'point'
    default_on: bool
    vintage: str          # human-readable
    coverage_note: str    # e.g. "full county, 1999-2026"
    builder: Callable[[Session, dict], LayerResult]

@dataclass
class LayerResult:
    layer_id: str
    grain: str
    values: dict | None         # for choropleth: {geo_id: number}
    features: list | None       # for point: GeoJSON features
    legend: dict                # {min, max, breaks, unit}
    provenance: list            # tables touched + row counts

LAYERS: dict[str, LayerSpec] = {...}   # populated from the JSON catalog

def get_layer(layer_id: str) -> LayerSpec: ...   # raises KeyError → 404 at API
def list_layers() -> dict[str, list[LayerSpec]]:   # grouped by domain
```

Each builder is a small function. The notable ones encoding the
data-review findings:

```python
def _build_disaster_exposure(db, params): ...           # NRI + FEMA → county
def _build_local_wealth(db, params): ...                # IRS SOI county (county_code IS FIPS)
def _build_banking(db, params):                         # FDIC via cert→stcnty
    db.execute(text("""
        SELECT i.stcnty AS geo_id,
               AVG(f.asset)::numeric AS value
        FROM fdic_institutions i
        JOIN fdic_bank_financials f USING (cert)
        WHERE i.stcnty IS NOT NULL
        GROUP BY i.stcnty
    """))
def _build_industry_density_state(db, params): ...      # CBP at state — HONEST grain
def _build_connectivity_state(db, params): ...          # FCC at state — HONEST grain
def _build_infrastructure_points(db, params): ...       # EPA ECHO + power/etc
```

USAspending and ACS-county are explicitly absent from `LAYERS` until
PLAN_067 backfills them. T2 enforces this in code.

#### `app/services/atlas/boundaries.py`

```python
def serve_boundaries(db, geo_level: str = 'county', simplify: float = 0.005) -> dict:
    """Return a GeoJSON FeatureCollection with simplified geometry."""
```

Server-side simplification with Douglas-Peucker (Postgres has
`ST_Simplify` — but we may not have PostGIS on cloud; pre-flight checks. If
PostGIS absent, simplification in Python via `shapely` or a hand-rolled
simplifier. Measure raw vs simplified payload; tune tolerance to keep
county boundaries under ~3MB total). Aggressive HTTP cache headers — the
boundary geometry doesn't change.

#### Endpoints (extend `app/api/v1/atlas.py`)

```python
@router.get("/layers")
def list_layers_endpoint(): ...     # grouped registry

@router.get("/layer/{layer_id}")
def get_layer_endpoint(layer_id: str, db: Session = Depends(get_db)): ...

@router.get("/boundaries")
def get_boundaries(geo_level: str = "county", db: Session = Depends(get_db)): ...

@router.get("/place/{geo_id}")
def get_place(geo_id: str, db: Session = Depends(get_db)):
    """For every registered choropleth layer, return its value for this
    place. The data backbone for click-a-place drill-down."""
```

`/place/{geo_id}` iterates the registered choropleth layers and returns a
dict `{layer_id: {value, legend_position, ...}}`. Layers that don't apply
to the place (different grain, no data) skip silently.

### What's deliberately NOT in this spec
- No map rendering — that's SPEC_066 (frontend).
- No drill-down cards — already shipped in SPEC_064; `/place/{geo_id}` is
  the *layer-aggregate* sibling of `/explore`, not a replacement.
- No layer Recent Activity / Follow / Stories — those are SPEC_067-069.
- No layer caching infra beyond `lru_cache` — premature for v1.

## Files to Create/Modify

| File | Action | Description |
|---|---|---|
| `docs/specs/SPEC_065_atlas_layer_api.md` | Create | This file |
| `tests/test_spec_065_atlas_layer_api.py` | Create | T1–T12 skeleton |
| `docs/specs/.active_spec` | Modify | → `SPEC_065_atlas_layer_api` |
| `scripts/audit_atlas_layer_catalog.py` | Create | Pre-flight audit; outputs to `data/reference/` |
| `data/reference/atlas_layer_catalog_2026-05-23.csv` | Create (via script) | Human-readable catalog |
| `data/reference/atlas_layer_catalog_2026-05-23.json` | Create (via script) | Source of truth for the registry |
| `app/services/atlas/layers.py` | Create | Layer registry + builders |
| `app/services/atlas/boundaries.py` | Create | Boundary serving with simplification |
| `app/api/v1/atlas.py` | Modify | Add the 4 endpoints |

## Feedback History

_No corrections yet._
