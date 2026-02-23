"""
Job Posting Change Detector — compares snapshots to generate hiring alerts.

Runs after each snapshot creation. Compares today's snapshot to 7 days ago
and creates alerts when hiring patterns shift significantly.
"""

import json
import logging
from datetime import date, timedelta
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


class JobPostingChangeDetector:
    """Detects significant changes in job posting patterns."""

    # Company-level thresholds
    SURGE_PCT = 50        # +50% WoW = hiring surge
    FREEZE_PCT = -30      # -30% WoW = hiring freeze
    ABS_SURGE = 20        # +20 absolute when base < 40
    MIN_BASE = 10         # ignore companies with fewer than this

    # Department-level thresholds
    DEPT_SURGE_PCT = 100  # +100% WoW for a department
    DEPT_DECLINE_PCT = -50  # -50% WoW for a department
    DEPT_MIN_CHANGE = 5   # minimum absolute change for dept alerts

    LOOKBACK_DAYS = 7

    def detect(self, db: Session, company_id: int, snapshot_date: date) -> list[dict]:
        """
        Compare today's snapshot to 7 days ago and generate alerts.

        Returns list of alert dicts that were inserted.
        """
        alerts = []

        # Fetch current and previous snapshots
        current = self._get_snapshot(db, company_id, snapshot_date)
        if not current:
            return alerts

        previous_date = snapshot_date - timedelta(days=self.LOOKBACK_DAYS)
        previous = self._get_snapshot(db, company_id, previous_date)
        if not previous:
            return alerts

        # Company-level alerts
        alerts.extend(
            self._check_company_level(company_id, snapshot_date, current, previous)
        )

        # Department-level alerts
        alerts.extend(
            self._check_department_level(company_id, snapshot_date, current, previous)
        )

        # Insert alerts
        if alerts:
            self._insert_alerts(db, alerts)
            logger.info(
                f"Company {company_id}: {len(alerts)} alert(s) generated "
                f"on {snapshot_date}"
            )

        return alerts

    def _get_snapshot(
        self, db: Session, company_id: int, snapshot_date: date
    ) -> Optional[dict]:
        """Fetch a single snapshot row."""
        row = db.execute(
            text("""
                SELECT total_open, new_postings, closed_postings,
                       by_department, by_seniority
                FROM job_posting_snapshots
                WHERE company_id = :cid AND snapshot_date = :sd
            """),
            {"cid": company_id, "sd": snapshot_date},
        ).fetchone()
        if not row:
            return None
        return {
            "total_open": row[0] or 0,
            "new_postings": row[1] or 0,
            "closed_postings": row[2] or 0,
            "by_department": row[3] if isinstance(row[3], dict) else {},
            "by_seniority": row[4] if isinstance(row[4], dict) else {},
        }

    def _check_company_level(
        self,
        company_id: int,
        snapshot_date: date,
        current: dict,
        previous: dict,
    ) -> list[dict]:
        """Check for company-level hiring surges or freezes."""
        alerts = []
        cur_total = current["total_open"]
        prev_total = previous["total_open"]

        if prev_total < self.MIN_BASE and cur_total < self.MIN_BASE:
            return alerts

        change_abs = cur_total - prev_total
        change_pct = (
            round(100.0 * change_abs / prev_total, 2) if prev_total > 0 else None
        )

        # Hiring surge
        is_surge = False
        if change_pct is not None and change_pct >= self.SURGE_PCT:
            is_surge = True
        elif prev_total < 40 and change_abs >= self.ABS_SURGE:
            is_surge = True

        if is_surge:
            severity = self._classify_severity(change_pct, cur_total, prev_total, "hiring_surge")
            alerts.append({
                "company_id": company_id,
                "alert_type": "hiring_surge",
                "severity": severity,
                "snapshot_date": snapshot_date,
                "current_total": cur_total,
                "previous_total": prev_total,
                "change_pct": change_pct,
                "change_abs": change_abs,
                "department": None,
                "details": {
                    "new_postings": current["new_postings"],
                    "lookback_days": self.LOOKBACK_DAYS,
                },
            })

        # Hiring freeze
        is_freeze = False
        if change_pct is not None and change_pct <= self.FREEZE_PCT:
            is_freeze = True
        elif prev_total >= 20 and cur_total <= 5:
            is_freeze = True

        if is_freeze:
            severity = self._classify_severity(change_pct, cur_total, prev_total, "hiring_freeze")
            alerts.append({
                "company_id": company_id,
                "alert_type": "hiring_freeze",
                "severity": severity,
                "snapshot_date": snapshot_date,
                "current_total": cur_total,
                "previous_total": prev_total,
                "change_pct": change_pct,
                "change_abs": change_abs,
                "department": None,
                "details": {
                    "closed_postings": current["closed_postings"],
                    "lookback_days": self.LOOKBACK_DAYS,
                },
            })

        return alerts

    def _check_department_level(
        self,
        company_id: int,
        snapshot_date: date,
        current: dict,
        previous: dict,
    ) -> list[dict]:
        """Check for department-level surges or declines."""
        alerts = []
        cur_depts = current["by_department"]
        prev_depts = previous["by_department"]

        all_depts = set(cur_depts.keys()) | set(prev_depts.keys())

        for dept in all_depts:
            cur_count = cur_depts.get(dept, 0)
            prev_count = prev_depts.get(dept, 0)
            change_abs = cur_count - prev_count

            if abs(change_abs) < self.DEPT_MIN_CHANGE:
                continue

            change_pct = (
                round(100.0 * change_abs / prev_count, 2) if prev_count > 0 else None
            )

            # Department surge
            if change_pct is not None and change_pct >= self.DEPT_SURGE_PCT:
                alerts.append({
                    "company_id": company_id,
                    "alert_type": "department_surge",
                    "severity": "low",
                    "snapshot_date": snapshot_date,
                    "current_total": cur_count,
                    "previous_total": prev_count,
                    "change_pct": change_pct,
                    "change_abs": change_abs,
                    "department": dept,
                    "details": {"lookback_days": self.LOOKBACK_DAYS},
                })

            # Department decline
            if change_pct is not None and change_pct <= self.DEPT_DECLINE_PCT:
                alerts.append({
                    "company_id": company_id,
                    "alert_type": "department_decline",
                    "severity": "low",
                    "snapshot_date": snapshot_date,
                    "current_total": cur_count,
                    "previous_total": prev_count,
                    "change_pct": change_pct,
                    "change_abs": change_abs,
                    "department": dept,
                    "details": {"lookback_days": self.LOOKBACK_DAYS},
                })

        return alerts

    def _classify_severity(
        self,
        change_pct: Optional[float],
        current: int,
        previous: int,
        alert_type: str,
    ) -> str:
        """Determine alert severity based on magnitude."""
        if alert_type == "hiring_surge":
            if change_pct is not None and change_pct >= 100:
                return "high"
            return "medium"

        if alert_type == "hiring_freeze":
            if current <= 5 and previous >= 20:
                return "high"
            if change_pct is not None and change_pct <= -60:
                return "high"
            return "medium"

        return "low"

    def _insert_alerts(self, db: Session, alerts: list[dict]):
        """Bulk insert alerts into job_posting_alerts."""
        for alert in alerts:
            db.execute(
                text("""
                    INSERT INTO job_posting_alerts (
                        company_id, alert_type, severity, snapshot_date,
                        current_total, previous_total, change_pct, change_abs,
                        department, details
                    ) VALUES (
                        :company_id, :alert_type, :severity, :snapshot_date,
                        :current_total, :previous_total, :change_pct, :change_abs,
                        :department, :details
                    )
                """),
                {
                    **alert,
                    "details": json.dumps(alert.get("details")) if alert.get("details") else None,
                },
            )
