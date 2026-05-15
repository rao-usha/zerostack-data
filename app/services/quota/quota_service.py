"""
Free-tier quota / metering — PLAN_063 / SPEC_055.

A per-day counter keyed by (subject_type, subject_key, day). The playground
router (SPEC_056) enforces it via a FastAPI dependency: anonymous visitors are
metered by IP, authenticated users by user id + tier limit.

Nexdata's existing rate limiting is per-data-source and per-API-key — neither
fits per-user/per-IP playground quotas, hence this purpose-built counter.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


@dataclass
class QuotaResult:
    """Outcome of a quota check."""

    allowed: bool
    remaining: int
    reset_ts: str
    limit: int
    used: int


class QuotaService:
    """Per-day run quota for the Synthetic Data Playground."""

    def __init__(self, db: Session):
        self.db = db
        self._ensure_tables()

    def _ensure_tables(self) -> None:
        self.db.execute(
            text("""
            CREATE TABLE IF NOT EXISTS playground_quota_buckets (
                subject_type VARCHAR(10) NOT NULL,
                subject_key VARCHAR(64) NOT NULL,
                day VARCHAR(10) NOT NULL,
                run_count INTEGER DEFAULT 0,
                PRIMARY KEY (subject_type, subject_key, day)
            )
        """)
        )
        self.db.commit()

    # ------------------------------------------------------------------
    # Pure date helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _day_key(now: Optional[datetime] = None) -> str:
        """Current UTC date as YYYY-MM-DD."""
        now = now or datetime.now(timezone.utc)
        return now.astimezone(timezone.utc).strftime("%Y-%m-%d")

    @staticmethod
    def _reset_ts(now: Optional[datetime] = None) -> str:
        """ISO timestamp of the next UTC midnight (when the bucket resets)."""
        now = now or datetime.now(timezone.utc)
        now = now.astimezone(timezone.utc)
        tomorrow = (now + timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        return tomorrow.isoformat()

    # ------------------------------------------------------------------
    # Quota operations
    # ------------------------------------------------------------------

    def _current_count(self, subject_type: str, subject_key: str, day: str) -> int:
        row = self.db.execute(
            text("""
            SELECT run_count FROM playground_quota_buckets
            WHERE subject_type = :st AND subject_key = :sk AND day = :day
        """),
            {"st": subject_type, "sk": subject_key, "day": day},
        ).fetchone()
        return int(row[0]) if row else 0

    def check_and_consume(
        self, subject_type: str, subject_key: str, limit: int
    ) -> QuotaResult:
        """Consume one run from the subject's daily bucket if under `limit`.

        Returns allowed=True (and increments) when there is room, else
        allowed=False without incrementing.
        """
        limit = max(0, int(limit))
        day = self._day_key()
        reset_ts = self._reset_ts()
        used = self._current_count(subject_type, subject_key, day)

        if used >= limit:
            return QuotaResult(
                allowed=False,
                remaining=max(0, limit - used),
                reset_ts=reset_ts,
                limit=limit,
                used=used,
            )

        # Atomic increment — concurrent calls can't double-spend a slot.
        self.db.execute(
            text("""
            INSERT INTO playground_quota_buckets (subject_type, subject_key, day, run_count)
            VALUES (:st, :sk, :day, 1)
            ON CONFLICT (subject_type, subject_key, day)
            DO UPDATE SET run_count = playground_quota_buckets.run_count + 1
        """),
            {"st": subject_type, "sk": subject_key, "day": day},
        )
        self.db.commit()

        used_after = used + 1
        return QuotaResult(
            allowed=True,
            remaining=max(0, limit - used_after),
            reset_ts=reset_ts,
            limit=limit,
            used=used_after,
        )

    def peek(self, subject_type: str, subject_key: str, limit: int) -> QuotaResult:
        """Read-only quota check — never mutates the bucket."""
        limit = max(0, int(limit))
        day = self._day_key()
        used = self._current_count(subject_type, subject_key, day)
        return QuotaResult(
            allowed=used < limit,
            remaining=max(0, limit - used),
            reset_ts=self._reset_ts(),
            limit=limit,
            used=used,
        )
