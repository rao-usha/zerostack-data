"""
PG LISTEN → EventBus bridge.

Runs as an asyncio task inside the API process. Listens on the
'job_events' PostgreSQL channel and republishes received events
into the in-memory EventBus so SSE clients get live updates.

Uses psycopg2 raw connection (not SQLAlchemy) because LISTEN
requires a persistent, non-pooled connection.
"""
import asyncio
import json
import logging
import select
from typing import Optional

import psycopg2

from app.core.config import get_settings
from app.core.event_bus import EventBus
from app.core.pg_notify import CHANNEL

logger = logging.getLogger(__name__)

# How often to poll for notifications (seconds)
_POLL_INTERVAL = 0.5

_listener_task: Optional[asyncio.Task] = None


def _get_raw_dsn() -> str:
    """Convert SQLAlchemy DATABASE_URL to psycopg2 DSN."""
    url = get_settings().database_url
    # SQLAlchemy uses postgresql:// but psycopg2 also accepts it
    if url.startswith("postgresql+psycopg2://"):
        url = url.replace("postgresql+psycopg2://", "postgresql://", 1)
    return url


def _listen_loop(dsn: str, stop_event: asyncio.Event, loop: asyncio.AbstractEventLoop):
    """
    Blocking listener thread.

    Connects to PG, issues LISTEN, and polls for notifications.
    When a notification arrives, schedules EventBus.publish() on the main loop.
    """
    conn = None
    try:
        conn = psycopg2.connect(dsn)
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

        cur = conn.cursor()
        cur.execute(f"LISTEN {CHANNEL};")
        logger.info(f"PG listener connected, listening on channel '{CHANNEL}'")

        while not stop_event.is_set():
            # Use select to wait for data with a timeout
            if select.select([conn], [], [], _POLL_INTERVAL) == ([], [], []):
                # Timeout — no data, loop back to check stop_event
                continue

            conn.poll()
            while conn.notifies:
                notify = conn.notifies.pop(0)
                try:
                    msg = json.loads(notify.payload)
                    event_type = msg.get("event", "unknown")
                    data = msg.get("data", {})
                    job_id = data.get("job_id")

                    # Publish to "jobs_all" channel (all subscribers)
                    loop.call_soon_threadsafe(
                        EventBus.publish, "jobs_all", event_type, data
                    )

                    # Publish to job-specific channel
                    if job_id:
                        loop.call_soon_threadsafe(
                            EventBus.publish, f"job_{job_id}", event_type, data
                        )

                except (json.JSONDecodeError, KeyError) as e:
                    logger.warning(f"Bad PG notification payload: {e}")
    except Exception as e:
        logger.error(f"PG listener error: {e}")
    finally:
        if conn and not conn.closed:
            conn.close()
        logger.info("PG listener disconnected")


async def start_pg_listener() -> asyncio.Task:
    """
    Start the PG LISTEN background task.

    Returns the asyncio.Task so the caller can cancel it on shutdown.
    """
    global _listener_task

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    dsn = _get_raw_dsn()

    async def _run():
        try:
            await loop.run_in_executor(None, _listen_loop, dsn, stop_event, loop)
        except asyncio.CancelledError:
            stop_event.set()

    _listener_task = asyncio.create_task(_run())

    # Store stop_event on the task so we can signal it on shutdown
    _listener_task._stop_event = stop_event  # type: ignore[attr-defined]

    return _listener_task


async def stop_pg_listener():
    """Stop the PG LISTEN background task gracefully."""
    global _listener_task
    if _listener_task is not None:
        # Signal the blocking thread to exit
        if hasattr(_listener_task, "_stop_event"):
            _listener_task._stop_event.set()
        _listener_task.cancel()
        try:
            await _listener_task
        except asyncio.CancelledError:
            pass
        _listener_task = None
        logger.info("PG listener stopped")
