"""
PE Alert Subscription Service.

Manages alert subscriptions for PE firms and provides alert history.
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import select, and_, update
from sqlalchemy.orm import Session

from app.core.pe_models import PEAlert, PEAlertSubscription, PEFirm

logger = logging.getLogger(__name__)


PE_ALERT_TYPES = [
    "PE_EXIT_READINESS_CHANGE",
    "PE_DEAL_STAGE_CHANGE",
    "PE_FINANCIAL_ALERT",
    "PE_LEADERSHIP_CHANGE",
    "PE_NEW_MARKET_OPPORTUNITY",
    "PE_PORTFOLIO_HEALTH_SUMMARY",
]


class AlertSubscriptionService:
    """Manage PE alert subscriptions and history."""

    def __init__(self, db: Session):
        self.db = db

    def subscribe(
        self,
        firm_id: int,
        alert_types: List[str],
        webhook_id: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Subscribe a firm to one or more alert types.

        Returns list of subscription dicts created/updated.
        """
        results = []
        for alert_type in alert_types:
            if alert_type not in PE_ALERT_TYPES:
                logger.warning("Unknown alert type: %s", alert_type)
                continue

            existing = self.db.execute(
                select(PEAlertSubscription).where(
                    PEAlertSubscription.firm_id == firm_id,
                    PEAlertSubscription.alert_type == alert_type,
                )
            ).scalar_one_or_none()

            if existing:
                existing.enabled = True
                existing.webhook_id = webhook_id
                sub = existing
            else:
                sub = PEAlertSubscription(
                    firm_id=firm_id,
                    alert_type=alert_type,
                    webhook_id=webhook_id,
                    enabled=True,
                )
                self.db.add(sub)

            self.db.flush()
            results.append({
                "id": sub.id,
                "firm_id": firm_id,
                "alert_type": alert_type,
                "webhook_id": webhook_id,
                "enabled": True,
            })

        self.db.commit()
        logger.info("Firm %d subscribed to %d alert types", firm_id, len(results))
        return results

    def unsubscribe(
        self,
        firm_id: int,
        alert_types: List[str],
    ) -> int:
        """Unsubscribe a firm from alert types. Returns count disabled."""
        count = 0
        for alert_type in alert_types:
            existing = self.db.execute(
                select(PEAlertSubscription).where(
                    PEAlertSubscription.firm_id == firm_id,
                    PEAlertSubscription.alert_type == alert_type,
                )
            ).scalar_one_or_none()

            if existing and existing.enabled:
                existing.enabled = False
                count += 1

        self.db.commit()
        logger.info("Firm %d unsubscribed from %d alert types", firm_id, count)
        return count

    def list_subscriptions(self, firm_id: int) -> List[Dict[str, Any]]:
        """List active alert subscriptions for a firm."""
        subs = self.db.execute(
            select(PEAlertSubscription).where(
                PEAlertSubscription.firm_id == firm_id,
                PEAlertSubscription.enabled == True,
            )
        ).scalars().all()

        return [
            {
                "id": s.id,
                "firm_id": s.firm_id,
                "alert_type": s.alert_type,
                "webhook_id": s.webhook_id,
                "enabled": s.enabled,
                "created_at": s.created_at.isoformat() if s.created_at else None,
            }
            for s in subs
        ]

    def get_alert_history(
        self,
        firm_id: int,
        limit: int = 50,
        alert_type: Optional[str] = None,
        severity: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Get recent alerts for a firm, optionally filtered."""
        stmt = (
            select(PEAlert)
            .where(PEAlert.firm_id == firm_id)
            .order_by(PEAlert.created_at.desc())
            .limit(limit)
        )

        if alert_type:
            stmt = stmt.where(PEAlert.alert_type == alert_type)
        if severity:
            stmt = stmt.where(PEAlert.severity == severity)

        alerts = self.db.execute(stmt).scalars().all()

        return [
            {
                "id": a.id,
                "firm_id": a.firm_id,
                "company_id": a.company_id,
                "alert_type": a.alert_type,
                "severity": a.severity,
                "title": a.title,
                "detail": a.detail,
                "created_at": a.created_at.isoformat() if a.created_at else None,
                "acknowledged_at": a.acknowledged_at.isoformat() if a.acknowledged_at else None,
            }
            for a in alerts
        ]

    def acknowledge_alert(self, alert_id: int) -> bool:
        """Mark an alert as acknowledged."""
        alert = self.db.execute(
            select(PEAlert).where(PEAlert.id == alert_id)
        ).scalar_one_or_none()
        if not alert:
            return False

        alert.acknowledged_at = datetime.utcnow()
        self.db.commit()
        return True
