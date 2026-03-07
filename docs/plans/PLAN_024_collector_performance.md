# PLAN 024 — Collector Performance Optimization

**Status:** Draft
**Created:** 2026-03-06
**Priority:** High — directly affects data freshness SLAs and operational cost

---

## Problem Statement

All 10+ site intelligence collectors process states/entities **sequentially**, even though:
- The APIs support concurrent requests (bounded by rate limits, not by design)
- The orchestrator already parallelizes across domains and sources
- `asyncio` is available throughout the stack but unused inside collectors

The worst offender is USGS Elevation: **16,000 sequential point queries at 0.2s each = ~53 minutes** for what should take ~3 minutes with bounded concurrency.

### Current Architecture

```
Runner (full_sync)
  |-- Domain 1 (asyncio.gather)        <-- parallel
  |     |-- Source A.collect()          <-- parallel with Source B
  |     |     |-- for state in states:  <-- SEQUENTIAL (the bottleneck)
  |     |     |     await rate_limit()
  |     |     |     await fetch()
  |     |     |     await upsert()
  |     |-- Source B.collect()
  |-- Domain 2 (asyncio.gather)
```

The inner `for state in states` loop is always sequential. This plan adds a **concurrent state collection** primitive to `BaseCollector` and applies it across all collectors that process multiple states or entities.

---

## Audit Summary (10 Collectors)

| # | Collector | File | Calls | Rate Limit | Est. Time | Speedup Potential |
|---|-----------|------|-------|------------|-----------|-------------------|
| 1 | USGS Elevation | `risk/usgs_elevation_collector.py` | 16,000 (5/county) | 0.2s | **53 min** | **20x** |
| 2 | NREL Resource | `power/nrel_resource_collector.py` | 44 (1/centroid) | 2.5s | 110s | 4x |
| 3 | BLS QCEW | `labor/bls_qcew_collector.py` | 50 (1/state) | 1.0s | 50s | 4x |
| 4 | EPA ACRES | `risk/epa_acres_collector.py` | 100-500 (paginated) | 1.0s | 2-8 min | 4x |
| 5 | FEMA NFHL | `risk/fema_nfhl_collector.py` | 51 (1/state) | 1.0s | 51s | 4x |
| 6 | EIA Power | `power/eia_collector.py` | 2-5 (paginated) | 0.5s | 5s | Minimal |
| 7 | HIFLD | `power/hifld_collector.py` | 2-6 (paginated) | 0.2s | 3s | Minimal |
| 8 | Census BPS | `labor/census_bps_collector.py` | 3 (bulk files) | 1.0s | 3s | None |
| 9 | Census Gov | `labor/census_gov_collector.py` | 1 (ZIP) | 0.5s | 2s | None |
| 10 | Epoch DC | `telecom/epoch_dc_collector.py` | 1 (CSV) | 0.5s | 1s | None |

**Bottom line:** Collectors 1-5 have real optimization potential. Collectors 6-10 are already fast or use bulk downloads.

---

## Phase 1 — BaseCollector Concurrent Primitives (P0)

**Goal:** Add reusable concurrency helpers to `BaseCollector` so any collector can opt in.

### 1A. Add `gather_with_limit()` to BaseCollector

```python
# app/sources/site_intel/base_collector.py

async def gather_with_limit(
    self,
    coros: list,
    max_concurrent: int = 4,
    return_exceptions: bool = True,
) -> list:
    """
    Run coroutines with bounded concurrency.

    Respects self.rate_limit_delay between task starts.
    Each coroutine sees the rate limit via the semaphore,
    so the effective throughput is max_concurrent / rate_limit_delay.
    """
    sem = asyncio.Semaphore(max_concurrent)

    async def _bounded(coro):
        async with sem:
            await self.apply_rate_limit()
            return await coro

    return await asyncio.gather(
        *[_bounded(c) for c in coros],
        return_exceptions=return_exceptions,
    )
```

This is the core primitive. It wraps any list of coroutines with:
- A semaphore to bound concurrency
- Rate limiting between task starts
- Exception isolation (one failure doesn't kill the batch)

### 1B. Add `collect_states_concurrent()` convenience method

```python
async def collect_states_concurrent(
    self,
    states: list[str],
    collect_fn,  # async def collect_fn(state: str) -> list[dict]
    max_concurrent: int = 4,
) -> list[dict]:
    """
    Collect data for multiple states with bounded concurrency.

    Args:
        states: List of state codes
        collect_fn: Async function that takes a state and returns records
        max_concurrent: Max parallel state collections

    Returns:
        Flat list of all records across all states
    """
    coros = [collect_fn(state) for state in states]
    results = await self.gather_with_limit(coros, max_concurrent)

    all_records = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            logger.error(f"State {states[i]} failed: {result}")
            continue
        if result:
            all_records.extend(result)
    return all_records
```

### Files changed
- `app/sources/site_intel/base_collector.py` — add 2 methods (~40 lines)

### Tests
- `tests/test_base_collector_concurrency.py` — verify semaphore bounds, rate limit spacing, exception isolation

---

## Phase 2 — USGS Elevation Collector (P0, biggest win)

**Current:** 16,000 sequential queries, ~53 min
**Target:** ~3 min (20x speedup)

Three changes:

### 2A. Parallel point queries within each county

Currently queries 5 elevation points per county sequentially. These are independent:

```python
# Before (sequential):
for lat, lon in sample_points:
    await self.apply_rate_limit()
    elev = await self._query_elevation(lat, lon)

# After (concurrent):
coros = [self._query_elevation(lat, lon) for lat, lon in sample_points]
elevations = await self.gather_with_limit(coros, max_concurrent=5)
```

**Impact:** 5x speedup per county

### 2B. Parallel county processing across states

Currently processes counties one at a time. Use `gather_with_limit` for concurrent counties:

```python
# Before:
for county in counties:
    record = await self._collect_county(county)
    records.append(record)

# After:
coros = [self._collect_county(c) for c in counties]
records = await self.gather_with_limit(coros, max_concurrent=8)
```

**Impact:** 8x throughput (8 counties in flight simultaneously)

### 2C. Reduce rate limit delay

USGS EPQS has no documented rate limit. Current 0.2s delay is overly conservative. With concurrency, the effective per-request delay is `delay / max_concurrent`. Reduce to 0.05s:

```python
rate_limit_delay = 0.05  # 0.2 -> 0.05 (with 8 concurrent = 0.4s effective)
```

**Impact:** 4x per-request speedup

### 2D. Batch DB commits

Currently commits after ALL counties. Change to commit every 50 counties (already partially implemented in the running script, formalize in the collector):

```python
if len(pending_records) >= 50:
    self.bulk_upsert(CountyElevation, pending_records, ...)
    pending_records.clear()
```

### Combined impact
- Sequential: 16,000 queries x 0.2s = 3,200s (53 min)
- Optimized: 16,000 queries / 8 concurrent x 0.05s = 100s (~1.7 min)
- With overhead (DB writes, retries): **~3 min realistic**

### Files changed
- `app/sources/site_intel/risk/usgs_elevation_collector.py` — refactor collect loop
- `tests/test_usgs_elevation_collector.py` — update mocks for concurrent execution

---

## Phase 3 — State-Parallel Collectors (P1)

Apply `collect_states_concurrent()` to the 4 other state-looping collectors. Each is a small refactor.

### 3A. BLS QCEW (`labor/bls_qcew_collector.py`)

```python
# Before:
for state in states:
    result = await self._collect_state(state, ...)

# After:
records = await self.collect_states_concurrent(
    states,
    lambda s: self._collect_state(s, area_fips, year, quarter),
    max_concurrent=4,
)
```

**Impact:** 50s -> ~13s (4x)

### 3B. EPA ACRES (`risk/epa_acres_collector.py`)

```python
# Before:
for state in states:
    records = await self._collect_state(state)
    self.bulk_upsert(...)

# After:
all_records = await self.collect_states_concurrent(
    states, self._collect_state, max_concurrent=4,
)
self.bulk_upsert(BrownfieldSite, all_records, ...)
```

**Impact:** 2-8 min -> ~30s-2 min (4x)

### 3C. FEMA NFHL (`risk/fema_nfhl_collector.py`)

Already well-optimized (outStatistics = 1 query/state). Still benefits from concurrent state queries:

```python
all_records = await self.collect_states_concurrent(
    states, self._collect_state_stats, max_concurrent=4,
)
```

**Impact:** 51s -> ~13s (4x). Minor since already fast.

### 3D. NREL Resource (`power/nrel_resource_collector.py`)

Two fixes:
1. Concurrent centroid queries (same as state parallelism)
2. **Batch DB writes** — currently 44 individual `bulk_upsert` calls, change to 1:

```python
# Before:
for centroid in centroids:
    record = await self._collect_centroid(centroid)
    self.bulk_upsert(SolarResource, [record], ...)  # 44 DB round-trips

# After:
coros = [self._collect_centroid(c) for c in centroids]
records = await self.gather_with_limit(coros, max_concurrent=4)
self.bulk_upsert(SolarResource, records, ...)  # 1 DB round-trip
```

**Impact:** 110s -> ~30s (4x) + 44 DB calls -> 1

### Files changed
- `app/sources/site_intel/labor/bls_qcew_collector.py`
- `app/sources/site_intel/risk/epa_acres_collector.py`
- `app/sources/site_intel/risk/fema_nfhl_collector.py`
- `app/sources/site_intel/power/nrel_resource_collector.py`

### Tests
- Update existing test files to verify concurrent execution paths
- Add integration test that verifies semaphore bounds are respected

---

## Phase 4 — Rate Limit Tuning (P1)

Reduce over-conservative delays where APIs can handle more throughput.

| Collector | Current Delay | API Actual Limit | New Delay | Rationale |
|-----------|--------------|-----------------|-----------|-----------|
| USGS EPQS | 0.2s | No documented limit | 0.05s | Public, no auth, no 429s observed |
| EIA | 0.5s | 5,000/hr (~1.4/s) | 0.2s | Well under limit even at 5/s |
| BLS QCEW | 1.0s | ~2/s (observed) | 0.5s | Conservative gov API |
| EPA ACRES | 1.0s | Unknown | 0.5s | Gov API, moderate |
| NREL | 2.5s | 1,000/hr (real key) | 1.0s | Still well under 1000/hr |
| FEMA NFHL | 1.0s | Flaky (503s common) | 1.0s | **Keep as-is** — FEMA is unreliable |
| HIFLD | 0.2s | Unlimited (ArcGIS) | 0.1s | Mirror server, low traffic |

### Files changed
- Each collector file: update `rate_limit_delay` constant

### Risk mitigation
- Add adaptive rate limiting: if a 429 is received, double the delay for the remainder of the run
- Already partially implemented in `fetch_json()` retry loop — extend to persist across retries

---

## Phase 5 — Pipeline Parallelism (P2, optional)

Overlap HTTP fetching with DB writes. Currently: fetch -> upsert -> fetch -> upsert. Could be: fetch -> (upsert + fetch) -> (upsert + fetch) -> upsert.

### 5A. Add `pipeline_collect()` to BaseCollector

```python
async def pipeline_collect(
    self,
    states: list[str],
    collect_fn,
    upsert_fn,
):
    """Overlap fetch and DB write for sequential state collection."""
    pending_upsert = None
    for state in states:
        data = await collect_fn(state)
        if pending_upsert:
            await pending_upsert
        pending_upsert = asyncio.create_task(upsert_fn(data))
    if pending_upsert:
        await pending_upsert
```

**Impact:** ~10-20% speedup for collectors with large per-state result sets (EPA, FEMA).

**Risk:** SQLAlchemy sessions are not thread-safe. The upsert task must use the same event loop and session carefully. May need a dedicated session per task, or just use `run_in_executor` for the synchronous DB call.

### Recommendation
Defer this phase. Phases 1-4 provide 4-20x speedup. Pipeline parallelism adds complexity for marginal gain. Revisit only if DB write latency becomes the bottleneck after Phase 1-4.

---

## Phase 6 — State Fan-Out in Batch System (P2, optional)

Instead of one job per source (50 states sequential), create N jobs per source with state subsets:

```
Tier 2:
  EIA (AL-FL) → Worker 1
  EIA (GA-ME) → Worker 2
  EIA (MI-NY) → Worker 1
  EIA (NJ-TX) → Worker 2
  EIA (UT-WY) → Worker 1
```

### Implementation
- `nightly_batch_service.py`: When launching a source, check if `collector.supports_state_sharding`
- If yes, split states into N chunks (N = number of workers) and create N jobs
- Each job gets a `states` filter in its config
- Workers pick up jobs independently — natural load balancing

### Files changed
- `app/core/nightly_batch_service.py` — state sharding logic in `launch_batch_collection()`
- `app/sources/site_intel/types.py` — add `supports_state_sharding` flag to CollectionConfig

### Recommendation
Defer until we have 3+ workers. With 2 workers, Phase 1-4 concurrency within a single collector is sufficient. State fan-out becomes valuable at 4+ workers where intra-collector concurrency hits diminishing returns.

---

## Phase 7 — Config Plumbing: UI + Batch + Incremental (P0)

**Problem:** Concurrency settings are currently hardcoded as class attributes on each collector. They are NOT configurable from:
- The UI/API (`CollectionRequest` has no concurrency fields)
- Batch runs (`SourceDef.default_config` doesn't pass concurrency)
- The `source_configs` DB table (has `max_concurrent` and `rate_limit_rps` columns but collectors ignore them)

This phase wires concurrency settings through all three collection paths so the UI can control them.

### Current Config Flow (broken for concurrency)

```
UI/API → CollectionRequest(domains, sources, states)        ← no concurrency fields
       → CollectionConfig(domain, source, states, options)  ← options dict exists but unused
       → collector.collect(config)
       → collector.rate_limit_delay = 0.5  (HARDCODED)      ← ignores config
```

### Target Config Flow

```
UI/API → CollectionRequest(domains, sources, states, max_concurrent=8, rate_limit_delay=0.1)
       → CollectionConfig(domain, source, ..., max_concurrent=8, rate_limit_delay=0.1)
       → collector.collect(config)
       → collector reads config.max_concurrent (or falls back to class default)

Batch  → SourceDef("usgs_3dep", {"max_concurrent": 8, "rate_limit_delay": 0.05})
       → IngestionJob.config = {"max_concurrent": 8, ...}
       → collector reads from job config

DB     → source_configs table: source="usgs_3dep", max_concurrent=8, rate_limit_rps=20
       → collector reads at init if no override in CollectionConfig
```

### 7A. Add concurrency fields to `CollectionConfig`

```python
# app/sources/site_intel/types.py — CollectionConfig

class CollectionConfig(BaseModel):
    # ... existing fields ...

    # Performance tuning (optional — falls back to collector defaults, then source_configs DB)
    max_concurrent: Optional[int] = None       # max parallel requests within this collector
    rate_limit_delay: Optional[float] = None   # seconds between requests (overrides class default)
    batch_commit_size: Optional[int] = None    # records per DB commit (default 1000)
```

### 7B. Add concurrency fields to `CollectionRequest` (API layer)

```python
# app/api/v1/site_intel_sites.py — CollectionRequest

class CollectionRequest(BaseModel):
    domains: Optional[List[str]] = None
    sources: Optional[List[str]] = None
    states: Optional[List[str]] = None

    # Performance tuning
    max_concurrent: Optional[int] = Field(
        default=None, ge=1, le=20,
        description="Max parallel requests per collector (1-20). Default varies by source.",
    )
    rate_limit_delay: Optional[float] = Field(
        default=None, ge=0.0, le=10.0,
        description="Seconds between requests. Lower = faster but more aggressive.",
    )
```

### 7C. BaseCollector reads config at collect() time

```python
# app/sources/site_intel/base_collector.py — in collect() or a new _apply_config() method

def _apply_performance_config(self, config: CollectionConfig):
    """Apply runtime performance overrides from config, then DB, then class defaults."""
    # Priority: config param > source_configs DB > class default
    if config.max_concurrent is not None:
        self._max_concurrent = config.max_concurrent
    else:
        db_config = self._get_source_config()
        self._max_concurrent = (
            db_config.max_concurrent if db_config else 4
        )

    if config.rate_limit_delay is not None:
        self.rate_limit_delay = config.rate_limit_delay
    # else: keep class default

def _get_source_config(self):
    """Read source_configs row for this source (cached)."""
    from app.core.models import SourceConfig
    return self.db.query(SourceConfig).filter(
        SourceConfig.source == self.source.value
    ).first()
```

### 7D. Wire through batch system

The `SourceDef.default_config` dict already flows into `IngestionJob.config`, and collectors receive it. Just need to ensure collectors extract concurrency fields:

```python
# nightly_batch_service.py — tier definitions can include concurrency overrides:
SourceDef("usgs_3dep", {"max_concurrent": 8, "rate_limit_delay": 0.05})

# This already flows through:
# SourceDef.default_config → IngestionJob.config → worker payload → collector config
```

The site_intel worker executor needs to extract these from the payload and pass to `CollectionConfig`:

```python
# app/worker/executors/site_intel.py (or wherever site_intel jobs are dispatched)
config = CollectionConfig(
    domain=..., source=...,
    max_concurrent=payload.get("max_concurrent"),
    rate_limit_delay=payload.get("rate_limit_delay"),
)
```

### 7E. Wire through incremental/scheduler path

The scheduler calls `launch_batch_collection()` which uses the same `SourceDef` configs. No additional changes needed — if 7D works, incremental works too.

### 7F. Source Configs UI/API endpoints

The `source_configs` table already exists with `max_concurrent` and `rate_limit_rps`. Add API endpoints so the frontend can manage these:

```
GET    /api/v1/sources/configs                — list all source configs
GET    /api/v1/sources/configs/{source}       — get one
PUT    /api/v1/sources/configs/{source}       — update (max_concurrent, rate_limit_rps, etc.)
POST   /api/v1/sources/configs/{source}/reset — reset to defaults
```

### Files changed
- `app/sources/site_intel/types.py` — add fields to CollectionConfig
- `app/api/v1/site_intel_sites.py` — add fields to CollectionRequest, pass through
- `app/sources/site_intel/base_collector.py` — `_apply_performance_config()`, `_get_source_config()`
- `app/core/nightly_batch_service.py` — add concurrency defaults to SourceDefs
- `app/api/v1/sources.py` (new or extend existing) — CRUD for source_configs
- Worker executor for site_intel — extract concurrency from payload

### Priority cascade (how a collector resolves its concurrency)

```
1. CollectionConfig.max_concurrent   (per-request override from API/UI)
   ↓ if None
2. source_configs.max_concurrent     (per-source DB setting, editable from UI)
   ↓ if no row
3. Collector class default           (e.g., USGSElevationCollector._max_concurrent = 8)
   ↓ if not set
4. BaseCollector default             (4)
```

This means:
- **UI users** can tune concurrency per collection run
- **Admins** can set defaults per source in the DB that persist
- **Batch runs** can include concurrency overrides in tier config
- **Developers** set sensible class-level defaults

---

## Implementation Order

```
Phase 7 (Config plumbing)             ~2 hours   — MUST come first so all phases use it
  |
  v
Phase 1 (BaseCollector primitives)     ~1 hour    — foundation for everything else
  |
  v
Phase 2 (USGS Elevation)              ~1 hour    — biggest single win (53 min -> 3 min)
  |
  v
Phase 3 (4 state-parallel collectors)  ~2 hours   — 4x speedup on BLS/EPA/FEMA/NREL
  |
  v
Phase 4 (Rate limit tuning)           ~30 min    — 2x compound speedup
  |
  v
Phase 5 (Pipeline parallelism)        DEFERRED   — marginal gain, high complexity
  |
  v
Phase 6 (State fan-out in batch)      DEFERRED   — needs 4+ workers to justify
```

---

## Expected Results

### Before Optimization (full national collection)

| Collector | Time |
|-----------|------|
| USGS Elevation | 53 min |
| NREL Resource | 110s |
| BLS QCEW | 50s |
| EPA ACRES | 2-8 min |
| FEMA NFHL | 51s |
| **Total (sequential bottleneck)** | **~60 min** |

### After Optimization (Phases 1-4)

| Collector | Time | Speedup |
|-----------|------|---------|
| USGS Elevation | **3 min** | 20x |
| NREL Resource | **30s** | 4x |
| BLS QCEW | **13s** | 4x |
| EPA ACRES | **30s-2 min** | 4x |
| FEMA NFHL | **13s** | 4x |
| **Total** | **~5 min** | **~12x** |

### Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| API rate limiting (429s) | Medium | Temporary slowdown | Adaptive backoff already in `fetch_json()` |
| API server overload | Low | 503 errors | `return_exceptions=True` + per-state retry |
| DB connection contention | Low | Deadlocks | Single session, sequential upserts after gather |
| Memory spike (large gathers) | Low | OOM | Chunk states into batches of 10-20 |

---

## Checklist

- [ ] Phase 7A: Add `max_concurrent`, `rate_limit_delay`, `batch_commit_size` to CollectionConfig
- [ ] Phase 7B: Add concurrency fields to CollectionRequest (API/UI)
- [ ] Phase 7C: BaseCollector `_apply_performance_config()` with priority cascade
- [ ] Phase 7D: Wire concurrency through batch SourceDef → IngestionJob → collector
- [ ] Phase 7E: Verify incremental/scheduler path inherits batch config
- [ ] Phase 7F: Source configs CRUD API endpoints
- [ ] Phase 7 tests
- [ ] Phase 1A: `gather_with_limit()` in BaseCollector
- [ ] Phase 1B: `collect_states_concurrent()` in BaseCollector
- [ ] Phase 1 tests
- [ ] Phase 2A: USGS parallel point queries
- [ ] Phase 2B: USGS parallel county processing
- [ ] Phase 2C: USGS rate limit 0.2 -> 0.05
- [ ] Phase 2D: USGS batch DB commits
- [ ] Phase 2 tests
- [ ] Phase 3A: BLS QCEW concurrent states
- [ ] Phase 3B: EPA ACRES concurrent states
- [ ] Phase 3C: FEMA NFHL concurrent states
- [ ] Phase 3D: NREL batch DB writes + concurrent centroids
- [ ] Phase 3 tests
- [ ] Phase 4: Rate limit tuning (all collectors)
- [ ] Smoke test: full national collection, verify data integrity
- [ ] Commit
