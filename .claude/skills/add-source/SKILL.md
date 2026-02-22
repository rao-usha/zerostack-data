---
name: add-source
description: Scaffold a new data source following Nexdata project patterns. Creates client, ingestor, metadata, API router, and test files. Use when adding a new external data source.
allowed-tools:
  - Bash
  - Read
  - Write
  - Edit
  - Glob
  - Grep
---

Scaffold all files needed for a new Nexdata data source.

## Behavior

1. **Gather requirements from `$ARGUMENTS`:**
   - Source name (e.g., `alpha_vantage`, `polygon`, `newsapi`)
   - Brief description of what data it provides
   - Whether it needs an API key
   - Base URL of the external API

   If not enough info is provided, ask the user.

2. **Create the source directory structure:**

   ```
   app/sources/<source_name>/
   ├── __init__.py
   ├── client.py      # HTTP client (inherits BaseAPIClient)
   ├── ingest.py      # Ingestor (inherits BaseSourceIngestor)
   └── metadata.py    # Dataset schemas, field definitions
   app/api/v1/<source_name>.py  # API router
   tests/test_<source_name>.py  # Unit tests
   ```

3. **Follow existing patterns exactly.** Read these reference files before generating code:

   **Client pattern** — read `app/core/http_client.py` for `BaseAPIClient`:
   ```python
   from app.core.http_client import BaseAPIClient

   class <SourceName>Client(BaseAPIClient):
       BASE_URL = "https://api.example.com"

       def __init__(self):
           super().__init__(
               base_url=self.BASE_URL,
               max_concurrency=4,
               max_requests_per_second=2.0,
           )
           # API key from settings if needed
           self.api_key = settings.<source>_api_key

       async def get_<dataset>(self, **params) -> dict:
           """Fetch <dataset> from the API."""
           return await self._get("/endpoint", params=params)
   ```

   **Ingestor pattern** — read `app/core/ingest_base.py` for `BaseSourceIngestor`:
   ```python
   from app.core.ingest_base import BaseSourceIngestor

   class <SourceName>Ingestor(BaseSourceIngestor):
       SOURCE_NAME = "<source_name>"

       def __init__(self, db: Session):
           super().__init__(db)
           self.client = <SourceName>Client()

       async def ingest_<dataset>(self, job_id: int, **params) -> int:
           """Ingest <dataset>. Returns row count."""
           self.update_job_status(job_id, "running")
           try:
               data = await self.client.get_<dataset>(**params)
               rows = self._transform_and_load(data, "<table_name>")
               self.update_job_status(job_id, "success", rows_collected=rows)
               return rows
           except Exception as e:
               self.update_job_status(job_id, "failed", error=str(e))
               raise
   ```

   **Router pattern** — read any existing router like `app/api/v1/fred.py`:
   ```python
   from fastapi import APIRouter, BackgroundTasks, Depends
   from sqlalchemy.orm import Session
   from app.core.database import get_db

   router = APIRouter(prefix="/<source-name>", tags=["<Source Name>"])

   @router.post("/ingest", status_code=201)
   async def ingest_data(
       background_tasks: BackgroundTasks,
       param: str = Query(...),
       db: Session = Depends(get_db),
   ):
       """Ingest <source> data."""
       ingestor = <SourceName>Ingestor(db)
       job = ingestor.create_job(config={"param": param})
       background_tasks.add_task(ingestor.ingest_<dataset>, job.id, param=param)
       return {"job_id": job.id, "status": "pending"}

   @router.get("/datasets")
   def list_datasets():
       """List available datasets."""
       return [...]
   ```

4. **Register the router** in `app/main.py`:
   - Add import
   - Add `app.include_router(router, prefix="/api/v1")`
   - Add OpenAPI tag

5. **Add config** for the API key (if needed) in `app/core/config.py`:
   ```python
   <source>_api_key: Optional[str] = Field(default=None, env="<SOURCE>_API_KEY")
   ```

6. **Create basic unit test:**
   ```python
   import pytest
   from app.sources.<source_name>.client import <SourceName>Client

   @pytest.mark.unit
   class Test<SourceName>Client:
       def test_base_url(self):
           client = <SourceName>Client()
           assert client.BASE_URL == "https://..."

       def test_requires_api_key(self):
           # Test that missing key raises appropriate error
           ...
   ```

7. **Update the frontend SOURCE_REGISTRY** in `frontend/index.html` to include the new source.

8. **Show the user what was created** and next steps:
   ```
   Created files:
     app/sources/<source_name>/__init__.py
     app/sources/<source_name>/client.py
     app/sources/<source_name>/ingest.py
     app/sources/<source_name>/metadata.py
     app/api/v1/<source_name>.py
     tests/test_<source_name>.py

   Modified files:
     app/main.py (router registration)
     app/core/config.py (API key config)

   Next steps:
     1. Set API key: PUT /settings/api-keys {"source": "<source>", "key": "..."}
     2. Restart: /restart
     3. Test: /test-endpoint POST /<source>/ingest
     4. Verify: /explore-source <source>
   ```

## Important
- Always read BaseAPIClient and BaseSourceIngestor before generating code — patterns may have evolved
- Use `asyncio.Semaphore` for bounded concurrency (inherited from BaseAPIClient)
- All table names should be snake_case, prefixed with the source name
- Use parameterized SQL (`:param` style) — never string-concatenate user input
- Rate limiting should respect the source API's limits (check their docs)
- Add the source to the `SOURCE_REGISTRY` in the frontend for the Data Sources dashboard
