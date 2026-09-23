"""
Data watchdog endpoints (SPEC_128).

The watchdog runs every 15 minutes on its own; these endpoints show what it
currently considers broken and let an operator trigger a run.
"""

import os

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.services import data_watchdog

router = APIRouter(prefix="/watchdog", tags=["watchdog"])


@router.get("/status")
def watchdog_status(db: Session = Depends(get_db)):
    """Open alerts, the last run's summary, and whether delivery is configured."""
    alerts = data_watchdog.open_alerts(db)
    return {
        "open_alerts": alerts,
        "open_count": len(alerts),
        "critical_count": sum(1 for a in alerts if a["severity"] == "critical"),
        "last_run": data_watchdog.last_run() or None,
        "config": {
            "webhook_configured": bool(os.getenv("ALERT_WEBHOOK_URL", "").strip()),
            "heartbeat_configured": bool(os.getenv("HEARTBEAT_PING_URL", "").strip()),
            "interval_minutes": data_watchdog._env_int(
                "WATCHDOG_INTERVAL_MINUTES", data_watchdog.DEFAULT_INTERVAL_MINUTES),
        },
    }


@router.post("/run")
async def watchdog_run(db: Session = Depends(get_db)):
    """Run the watchdog now (same dedupe as the timer: nothing is re-sent early)."""
    return await data_watchdog.run_watchdog(db)
