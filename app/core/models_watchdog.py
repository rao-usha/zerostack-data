"""
State for the data watchdog (SPEC_128).

One row per alert key. The watchdog runs every 15 minutes; this table is what
lets it say something once, remind at most daily, and say when it is over,
instead of paging every 15 minutes or never.
"""

from datetime import datetime

from sqlalchemy import JSON, Column, DateTime, Integer, String, Text

from app.core.models import Base


class WatchdogAlert(Base):
    """An alert the watchdog has raised, open or resolved."""

    __tablename__ = "watchdog_alerts"

    key = Column(String(200), primary_key=True)  # e.g. "schedule:stalled:12"
    rule = Column(String(50), nullable=False, index=True)
    severity = Column(String(16), nullable=False)  # warning | critical
    status = Column(String(16), nullable=False, default="open", index=True)  # open | resolved
    message = Column(Text, nullable=False)
    details = Column(JSON, nullable=True)

    first_seen_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    last_seen_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    # Last time an open/reminder/escalation notice was delivered (any episode);
    # it rate-limits warnings that flap.
    last_notified_at = Column(DateTime, nullable=True)
    resolved_at = Column(DateTime, nullable=True)
    notify_count = Column(Integer, nullable=False, default=0)
    # "webhook" or "log": an alert only logged is re-sent once a webhook exists.
    notified_via = Column(String(16), nullable=True)
    # 1 once this episode's open notice was delivered. 0 = not said yet (retried
    # next run), or said nothing on purpose (quiet re-open of a flapping warning).
    announced = Column(Integer, nullable=False, default=0)
    quiet = Column(Integer, nullable=False, default=0)
    # Registered ALERT_DATA_STALENESS subscribers told about this episode.
    fanned_out_at = Column(DateTime, nullable=True)

    def __repr__(self) -> str:
        return f"<WatchdogAlert key={self.key} status={self.status} severity={self.severity}>"
