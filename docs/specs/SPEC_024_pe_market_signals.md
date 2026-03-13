# SPEC 024 — PE Market Signals Service

**Status:** Draft
**Task type:** service
**Date:** 2026-03-13
**Test file:** tests/test_spec_024_pe_market_signals.py

## Goal

Create a market signal persistence and retrieval service that stores results from MarketScannerService.get_market_signals() into a new PEMarketSignal table. Enables scheduled scanning and historical trend tracking of sector momentum.

## Acceptance Criteria

- [ ] PEMarketSignal model in pe_models.py with: sector, momentum_score, deal_count, avg_multiple, signal_type, top_companies (JSON), scanned_at
- [ ] `store_signals(db, signals)` — persists scanner output to pe_market_signals table
- [ ] `get_latest_signals(db)` — returns most recent scan results per sector
- [ ] `get_high_momentum_sectors(db, threshold)` — sectors with momentum > threshold
- [ ] `run_market_scan(db)` — orchestrates scan + store + returns results
- [ ] Scheduled market scan registered in main.py at 7 AM UTC daily
- [ ] Fires PE_NEW_MARKET_OPPORTUNITY webhook when momentum_score > 75

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_store_signals | Signals are persisted to DB correctly |
| T2 | test_get_latest_signals | Returns most recent signals per sector |
| T3 | test_get_high_momentum_sectors | Filters by momentum threshold |
| T4 | test_run_market_scan_orchestration | Full scan-store-return flow |
| T5 | test_model_exists | PEMarketSignal model has expected columns |

## Rubric Checklist

- [ ] Clear single responsibility (market signal storage/retrieval)
- [ ] Uses dependency injection for DB sessions
- [ ] All DB operations use parameterized queries
- [ ] Error handling with structured logging
- [ ] Has corresponding test file with mocked dependencies
- [ ] Tests cover happy path, error cases, and boundary conditions

## Design Notes

### PEMarketSignal Model
```python
class PEMarketSignal(Base):
    __tablename__ = "pe_market_signals"
    id, sector, momentum_score, deal_count, avg_multiple,
    signal_type, top_companies (JSON), scanned_at, batch_id
```

### Function Signatures
```python
def store_signals(db, signals: list[dict]) -> int  # returns count stored
def get_latest_signals(db) -> list[dict]
def get_high_momentum_sectors(db, threshold=60) -> list[dict]
def run_market_scan(db) -> dict  # orchestrates full scan
```

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| app/core/pe_models.py | Modify | Add PEMarketSignal model |
| app/core/pe_market_signals.py | Create | Signal service |
| app/main.py | Modify | Register scheduled scan |
| tests/test_spec_024_pe_market_signals.py | Create | Tests |

## Feedback History

_No corrections yet._
