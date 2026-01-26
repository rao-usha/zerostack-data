# Plan 001: Export & Integration

**Date:** 2026-01-14
**Author:** Tab 1 (Claude)
**Status:** PENDING_APPROVAL
**Assigned To:** Tab 1

---

## Goal

Enable users to export ingested data from PostgreSQL tables to downloadable files in multiple formats.

---

## Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/v1/export/jobs` | Create export job for a table |
| GET | `/api/v1/export/jobs` | List all export jobs |
| GET | `/api/v1/export/jobs/{id}` | Get export job status |
| GET | `/api/v1/export/jobs/{id}/download` | Download exported file |
| DELETE | `/api/v1/export/jobs/{id}` | Delete export job and file |
| GET | `/api/v1/export/formats` | List supported formats |
| GET | `/api/v1/export/tables` | List exportable tables |

---

## Features

1. **Export Formats**
   - CSV (with headers)
   - JSON (array of objects)
   - Parquet (columnar, efficient for large data)

2. **Export Options**
   - Column selection (export specific columns)
   - Row limit (max rows to export)
   - Date range filter (for time-series tables)
   - Compression (gzip optional)

3. **Async Processing**
   - Large exports run in background
   - Job status: pending → running → completed/failed
   - Progress tracking (rows exported)

4. **File Management**
   - Files stored in `/tmp/exports/` or configurable path
   - Auto-cleanup after expiration (configurable, default 24h)
   - Unique file names with job ID

---

## Models

```python
class ExportFormat(str, enum.Enum):
    CSV = "csv"
    JSON = "json"
    PARQUET = "parquet"

class ExportStatus(str, enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    EXPIRED = "expired"

class ExportJob(Base):
    __tablename__ = "export_jobs"

    id = Column(Integer, primary_key=True)
    table_name = Column(String(255), nullable=False)
    format = Column(Enum(ExportFormat), nullable=False)
    status = Column(Enum(ExportStatus), default=ExportStatus.PENDING)

    # Options
    columns = Column(JSON, nullable=True)  # List of columns, null = all
    row_limit = Column(Integer, nullable=True)
    filters = Column(JSON, nullable=True)  # {"date_from": "...", "date_to": "..."}
    compress = Column(Boolean, default=False)

    # Results
    file_path = Column(String(500), nullable=True)
    file_size_bytes = Column(Integer, nullable=True)
    row_count = Column(Integer, nullable=True)
    error_message = Column(Text, nullable=True)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    expires_at = Column(DateTime, nullable=True)
```

---

## Files to Create

| File | Purpose |
|------|---------|
| `app/core/export_service.py` | Export logic, file generation, cleanup |
| `app/api/v1/export.py` | REST API endpoints |

---

## Dependencies

- `pandas` - DataFrame operations, CSV/JSON export
- `pyarrow` - Parquet export (already in requirements for some sources)

---

## Example Usage

```bash
# Create export job
curl -X POST http://localhost:8001/api/v1/export/jobs \
  -H "Content-Type: application/json" \
  -d '{"table_name": "fred_series", "format": "csv", "row_limit": 10000}'

# Response
{
  "id": 1,
  "status": "pending",
  "table_name": "fred_series",
  "format": "csv"
}

# Check status
curl http://localhost:8001/api/v1/export/jobs/1

# Download when complete
curl -O http://localhost:8001/api/v1/export/jobs/1/download
```

---

## Approval

- [x] User approved (2026-01-14)
- [x] Ready to implement

**User feedback:**
Approved as-is. Local file download for now, S3 can be added later.
