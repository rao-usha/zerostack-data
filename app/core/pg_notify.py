"""
Helper for sending PG NOTIFY events from workers.

Workers call send_job_event() after updating job_queue rows.
The API process receives these via pg_listener.py and republishes
them into the in-memory EventBus for SSE streaming.
"""

import json
import logging

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# PG NOTIFY channel name
CHANNEL = "job_events"

# PG NOTIFY payload limit is 8000 bytes
_MAX_PAYLOAD = 7900


def send_job_event(
    db: Session,
    event_type: str,
    data: dict,
) -> None:
    """
    Send a job event via PG NOTIFY.

    Args:
        db: SQLAlchemy session (must be inside a transaction)
        event_type: Event name (e.g. "job_started", "job_progress", "job_completed", "job_failed")
        data: Event payload (will be JSON-serialized)
    """
    payload = json.dumps({"event": event_type, "data": data}, default=str)

    # Truncate if too large — keep the event type and essential fields
    if len(payload.encode("utf-8")) > _MAX_PAYLOAD:
        # Strip large fields and add truncation notice
        trimmed = {
            "event": event_type,
            "data": {
                k: v
                for k, v in data.items()
                if k
                in (
                    "job_id",
                    "job_type",
                    "worker_id",
                    "progress_pct",
                    "progress_message",
                    "status",
                    "error_message",
                )
            },
            "_truncated": True,
        }
        payload = json.dumps(trimmed, default=str)
        if len(payload.encode("utf-8")) > _MAX_PAYLOAD:
            # Last resort: just the basics
            payload = json.dumps(
                {
                    "event": event_type,
                    "data": {"job_id": data.get("job_id")},
                    "_truncated": True,
                }
            )

    db.execute(
        text("SELECT pg_notify(:channel, :payload)"),
        {
            "channel": CHANNEL,
            "payload": payload,
        },
    )
    # Flush so the NOTIFY goes out even if caller hasn't committed yet
    db.flush()
