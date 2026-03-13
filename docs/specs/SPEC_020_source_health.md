# SPEC 020 — Source Health Scoring Service

**Status:** Draft
**Task type:** service
**Date:** 2026-03-13
**Test file:** tests/test_spec_020_source_health.py

## Goal

Build a source-level health scoring engine that computes a 0-100 composite score for each data source based on freshness, reliability, coverage, and consistency. Enables operational dashboards and collection prioritization.

## Acceptance Criteria

- [ ] `calculate_source_health(db, source_key)` returns 0-100 score with tier label
- [ ] Score components: Freshness (40%), Reliability (30%), Coverage (20%), Consistency (10%)
- [ ] Health tiers: Healthy (80-100), Warning (60-79), Degraded (40-59), Critical (0-39)
- [ ] `get_source_health_detail(db, source_key)` returns full breakdown with component scores, last 10 jobs, recommendations
- [ ] `get_all_source_health(db)` returns all sources sorted worst-first
- [ ] `get_health_summary(db)` returns aggregate platform health (counts per tier, overall score)
- [ ] All DB queries use parameterized SQL
- [ ] Graceful handling of sources with no jobs or no config
- [ ] Pure sync functions using SQLAlchemy Session (matching watermark_service pattern)

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_calculate_health_healthy_source | Source with recent success, high reliability → Healthy tier |
| T2 | test_calculate_health_degraded_source | Source with old data, low success rate → Degraded/Critical |
| T3 | test_calculate_health_no_jobs | Source with zero ingestion history → Critical (0) |
| T4 | test_calculate_health_no_config | Source without SourceConfig row → uses sensible defaults |
| T5 | test_freshness_scoring | Freshness score decays correctly based on hours since last success |
| T6 | test_reliability_scoring | Reliability score = success_count / total_count over 7 days |
| T7 | test_consistency_scoring | Low variance in row counts → high consistency score |
| T8 | test_get_health_detail_structure | Detail response has all expected keys |
| T9 | test_get_all_source_health_sorted | Results sorted worst-first (lowest score first) |
| T10 | test_get_health_summary_counts | Summary has correct tier counts and overall score |

## Rubric Checklist

- [ ] Clear single responsibility (one domain concern per service)
- [ ] Sync methods (no async needed — read-only DB queries)
- [ ] Uses dependency injection for DB sessions
- [ ] All DB operations use parameterized queries
- [ ] Error handling with structured logging
- [ ] Logging with structured context (source, operation, scores)
- [ ] Has corresponding test file with mocked dependencies
- [ ] Tests cover happy path, error cases, and boundary conditions

## Design Notes

### Scoring Formula

```
freshness_score = max(0, 100 - (hours_since_last_success / expected_hours) * 100)
reliability_score = (success_count / total_count) * 100  # last 7 days
coverage_score = min(100, (avg_rows_inserted / baseline_rows) * 100)
consistency_score = max(0, 100 - (cv * 100))  # cv = stddev / mean of rows per run

composite = freshness * 0.4 + reliability * 0.3 + coverage * 0.2 + consistency * 0.1
```

### Expected Frequency Defaults

If no `schedule_frequency` in SourceConfig, default to 24 hours. Map: hourly→1, daily→24, weekly→168, monthly→720.

### Data Sources

- `IngestionJob` — job history (status, created_at, completed_at, rows_inserted)
- `SourceWatermark` — last_success_at per source
- `SourceConfig` — schedule_frequency, enabled
- `DatasetRegistry` — source existence

### Function Signatures

```python
def calculate_source_health(db: Session, source_key: str) -> dict
# Returns: {"source": str, "score": int, "tier": str, "components": {...}}

def get_source_health_detail(db: Session, source_key: str) -> dict
# Returns: {score, tier, components, recent_jobs, recommendations, ...}

def get_all_source_health(db: Session) -> list[dict]
# Returns: sorted list of health dicts, worst-first

def get_health_summary(db: Session) -> dict
# Returns: {"overall_score": int, "total_sources": int, "by_tier": {...}, "critical_sources": [...]}
```

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| app/core/source_health.py | Create | Source health scoring service |
| tests/test_spec_020_source_health.py | Create | Tests |

## Feedback History

_No corrections yet._
