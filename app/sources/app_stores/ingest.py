"""
App Store Ingestor.

Iterates tracked companies' apps, fetches data from iTunes API,
and stores results in the database.
"""

import logging
from typing import Dict, Any, Optional
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.core.models import IngestionJob, JobStatus
from app.sources.app_stores.client import AppStoreClient

logger = logging.getLogger(__name__)


class AppStoreIngestor:
    """
    Ingestor that discovers and updates iOS app data for tracked companies.

    Workflow:
    1. Query company_app_portfolios for tracked apps
    2. For each app, call iTunes lookup API
    3. Update app_store_apps table with latest data
    4. Record rating snapshots
    """

    SOURCE_NAME = "app_stores"

    def __init__(self, db: Session):
        self.db = db
        self.client = AppStoreClient(db)

    async def run(
        self,
        job_id: int,
        company_name: Optional[str] = None,
        search_query: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Run app store ingestion.

        Args:
            job_id: Ingestion job ID
            company_name: Optional company to scope the ingestion to
            search_query: Optional search term to find new apps
            limit: Max apps to process

        Returns:
            Dict with ingestion results
        """
        start_time = datetime.utcnow()

        # Update job
        job = self.db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.RUNNING
            job.started_at = start_time
            self.db.commit()

        try:
            apps_updated = 0
            apps_discovered = 0
            errors = []

            # Phase 1: Update existing tracked apps
            tracked = self._get_tracked_apps(company_name)
            logger.info(f"Found {len(tracked)} tracked apps to update")

            for app_info in tracked[:limit] if limit else tracked:
                try:
                    app_data = await self.client.lookup_ios_app(
                        app_info["app_id"], country="us"
                    )
                    if app_data:
                        apps_updated += 1
                except Exception as e:
                    errors.append(f"App {app_info['app_id']}: {e}")

            # Phase 2: Search for new apps if query provided
            if search_query:
                try:
                    search_results = await self.client.search_ios_apps(
                        search_query, limit=min(limit or 25, 25)
                    )
                    apps_discovered = len(search_results)
                    logger.info(f"Discovered {apps_discovered} apps for query '{search_query}'")
                except Exception as e:
                    errors.append(f"Search '{search_query}': {e}")

            total = apps_updated + apps_discovered
            duration = (datetime.utcnow() - start_time).total_seconds()

            if job:
                job.status = JobStatus.SUCCESS if total > 0 else JobStatus.FAILED
                if total == 0:
                    job.error_message = "No apps processed"
                job.completed_at = datetime.utcnow()
                job.rows_inserted = total
                self.db.commit()

            logger.info(
                f"App store ingestion complete: {apps_updated} updated, "
                f"{apps_discovered} discovered in {duration:.1f}s"
            )

            return {
                "apps_updated": apps_updated,
                "apps_discovered": apps_discovered,
                "total_processed": total,
                "errors": errors,
                "duration_seconds": duration,
            }

        except Exception as e:
            logger.error(f"App store ingestion failed: {e}", exc_info=True)
            if job:
                job.status = JobStatus.FAILED
                job.error_message = str(e)
                job.completed_at = datetime.utcnow()
                self.db.commit()
            raise

    def _get_tracked_apps(self, company_name: Optional[str] = None):
        """Get list of tracked app IDs from company_app_portfolios."""
        query = "SELECT app_id, store, company_name FROM company_app_portfolios"
        params = {}
        if company_name:
            query += " WHERE LOWER(company_name) = LOWER(:company)"
            params["company"] = company_name

        result = self.db.execute(text(query), params)
        return [dict(row._mapping) for row in result.fetchall()]
