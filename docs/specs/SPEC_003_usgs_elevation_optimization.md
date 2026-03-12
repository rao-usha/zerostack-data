# SPEC 003: USGS Elevation Collector Optimization

## Goal
Refactor the USGS 3DEP Elevation Collector to use `collect_states_concurrent` from BaseCollector, achieving ~20x speedup (53 min -> ~3 min for national collection).

## Current State
- Collector has its own Semaphore(8) for county concurrency + Semaphore(5) for points
- `_query_elevation` calls `self.apply_rate_limit()` per point (double-limiting)
- ~3,200 counties x 5 points = 16,000 API calls
- EPQS has no documented rate limit
- 53 min runtime primarily due to EPQS response latency (~2s/req) + low concurrency

## Changes
1. Refactor `collect()` to use `collect_states_concurrent(states, collect_fn, max_concurrent=8)` for state-level parallelism
2. Each state function processes its counties sequentially with point-level concurrency (5 concurrent per county)
3. Remove duplicate rate limiting — let `gather_with_limit` handle it
4. Increase effective concurrency: 8 states * 5 points = 40 in-flight requests
5. Keep batch commit pattern (50 counties at a time)

## Acceptance Criteria
- [ ] Uses `collect_states_concurrent` from BaseCollector
- [ ] Handles `config.states` filter
- [ ] Batch commits preserved (50 counties)
- [ ] Progress reporting preserved
- [ ] Error handling preserved (partial success)
- [ ] All existing tests pass
- [ ] New tests for state-concurrent logic

## Files
- `app/sources/site_intel/risk/usgs_elevation_collector.py` — refactor
- `tests/test_usgs_elevation_optimization.py` — new tests
