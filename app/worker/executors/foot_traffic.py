"""Foot traffic executor for the worker queue."""

import logging

from sqlalchemy.orm import Session

from app.core.models_queue import JobQueue
from app.core.pg_notify import send_job_event

logger = logging.getLogger(__name__)


async def execute(job: JobQueue, db: Session):
    """Execute a foot traffic discovery/collection job."""
    from app.core.database import get_session_factory
    from app.sources.foot_traffic.ingest import (
        discover_brand_locations,
        collect_traffic_for_location,
    )

    payload = job.payload or {}
    action = payload.get("action", "discover")

    SessionLocal = get_session_factory()
    work_db = SessionLocal()

    try:
        if action == "discover":
            brand_name = payload.get("brand_name", "")
            city = payload.get("city")
            state = payload.get("state")
            limit = payload.get("limit", 50)

            send_job_event(
                db,
                "job_progress",
                {
                    "job_id": job.id,
                    "job_type": "foot_traffic",
                    "progress_pct": 10.0,
                    "progress_message": f"Discovering locations for {brand_name}",
                },
            )
            db.commit()

            result = await discover_brand_locations(
                work_db, brand_name, city=city, state=state, limit=limit
            )

            job.progress_pct = 100.0
            job.progress_message = f"Discovered {len(result)} locations"
            db.commit()

        elif action == "collect":
            location_id = payload.get("location_id")

            send_job_event(
                db,
                "job_progress",
                {
                    "job_id": job.id,
                    "job_type": "foot_traffic",
                    "progress_pct": 10.0,
                    "progress_message": f"Collecting traffic for location {location_id}",
                },
            )
            db.commit()

            await collect_traffic_for_location(work_db, location_id)

            job.progress_pct = 100.0
            job.progress_message = "Traffic collection completed"
            db.commit()

        else:
            raise ValueError(f"Unknown foot_traffic action: {action}")

    finally:
        work_db.close()
