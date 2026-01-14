# Parallel Work Tracking

Last updated: 2026-01-14

This file tracks multiple parallel implementation efforts being worked on simultaneously.

---

## Active Plans

| Plan | Description | Status | Assigned |
|------|-------------|--------|----------|
| [PLAN_001](PLAN_001_export_integration.md) | Export & Integration | COMPLETED | Tab 1 |
| [PLAN_002](PLAN_002_uspto_patents.md) | USPTO Patent Data Source | COMPLETED | Tab 2 |

---

## Status Legend

- `NOT_STARTED` - Plan created but not yet researched
- `RESEARCHING` - Gathering API documentation and requirements
- `PENDING_APPROVAL` - Plan complete, awaiting user approval
- `APPROVED` - User approved, ready to implement
- `IN_PROGRESS` - Implementation underway
- `COMPLETED` - Implementation finished
- `BLOCKED` - Waiting on external dependency

---

## Plan 001: Export & Integration

**Status:** COMPLETED
**Assigned:** Tab 1

Summary: Enable users to export ingested data from PostgreSQL tables to CSV, JSON, or Parquet formats with async job processing.

**Last Update:** Implementation completed (2026-01-14). Commit: 790ca0e

---

## Plan 002: USPTO Patent Data Source

**Status:** COMPLETED
**Assigned:** Tab 2

Summary: Integrate USPTO PatentsView API to ingest US patent data including patents, inventors, assignees, and classifications.

**Last Update:** Implementation completed (2026-01-14).

**Files Created:**
- `app/sources/uspto/__init__.py` - Module init
- `app/sources/uspto/client.py` - PatentsView API client
- `app/sources/uspto/metadata.py` - Field definitions and CPC mappings
- `app/sources/uspto/ingest.py` - Ingestion logic
- `app/api/v1/uspto.py` - REST API endpoints

**API Endpoints:**
- `GET /api/v1/uspto/patents` - Search patents
- `GET /api/v1/uspto/patents/{id}` - Get patent
- `GET /api/v1/uspto/assignees` - Search assignees
- `GET /api/v1/uspto/inventors` - Search inventors
- `GET /api/v1/uspto/cpc-codes` - Get CPC codes
- `POST /api/v1/uspto/ingest/assignee` - Ingest by assignee
- `POST /api/v1/uspto/ingest/cpc` - Ingest by CPC code
- `POST /api/v1/uspto/ingest/search` - Ingest by search query

---

## Notes

- Both plans are independent and can be implemented in parallel
- Neither requires the other as a dependency
- Coordinate on shared infrastructure changes (e.g., new migrations)
