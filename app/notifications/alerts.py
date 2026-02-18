"""
Portfolio Change Alert Engine (T11).

Detects changes in portfolio data and creates alerts for subscribed users.

Features:
- Change detection: new holdings, removed holdings, value changes
- Subscription management: users subscribe to investor alerts
- Alert lifecycle: pending -> delivered -> acknowledged/expired
- Snapshot management: stores portfolio state for comparison
"""

import json
import logging
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


class ChangeType(str, Enum):
    """Types of portfolio changes that can trigger alerts."""

    NEW_HOLDING = "new_holding"
    REMOVED_HOLDING = "removed_holding"
    VALUE_CHANGE = "value_change"
    SHARES_CHANGE = "shares_change"


class AlertStatus(str, Enum):
    """Alert lifecycle status."""

    PENDING = "pending"
    DELIVERED = "delivered"
    ACKNOWLEDGED = "acknowledged"
    EXPIRED = "expired"


@dataclass
class PortfolioChange:
    """Represents a detected change in portfolio."""

    change_type: ChangeType
    company_name: str
    details: Dict[str, Any]

    @property
    def summary(self) -> str:
        """Generate human-readable summary of the change."""
        if self.change_type == ChangeType.NEW_HOLDING:
            return f"New holding: {self.company_name}"
        elif self.change_type == ChangeType.REMOVED_HOLDING:
            return f"Removed holding: {self.company_name}"
        elif self.change_type == ChangeType.VALUE_CHANGE:
            pct = self.details.get("change_pct", 0)
            direction = "increased" if pct > 0 else "decreased"
            return f"{self.company_name} value {direction} by {abs(pct):.1f}%"
        elif self.change_type == ChangeType.SHARES_CHANGE:
            pct = self.details.get("change_pct", 0)
            direction = "increased" if pct > 0 else "decreased"
            return f"{self.company_name} shares {direction} by {abs(pct):.1f}%"
        return f"Change in {self.company_name}"


# SQL for table creation (run on first use)
CREATE_TABLES_SQL = """
-- Alert subscriptions
CREATE TABLE IF NOT EXISTS alert_subscriptions (
    id SERIAL PRIMARY KEY,
    investor_id INTEGER NOT NULL,
    investor_type VARCHAR(20) NOT NULL,
    user_id VARCHAR(255) NOT NULL,
    change_types JSONB DEFAULT '["new_holding", "removed_holding"]',
    value_threshold_pct FLOAT DEFAULT 10.0,
    delivery_channels JSONB DEFAULT '["in_app"]',
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(investor_id, investor_type, user_id)
);

-- Portfolio alerts
CREATE TABLE IF NOT EXISTS portfolio_alerts (
    id SERIAL PRIMARY KEY,
    subscription_id INTEGER REFERENCES alert_subscriptions(id) ON DELETE CASCADE,
    investor_id INTEGER NOT NULL,
    investor_type VARCHAR(20) NOT NULL,
    investor_name VARCHAR(255),
    change_type VARCHAR(50) NOT NULL,
    company_name VARCHAR(255) NOT NULL,
    summary TEXT,
    details JSONB,
    status VARCHAR(20) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT NOW(),
    delivered_at TIMESTAMP,
    acknowledged_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_alerts_subscription_status
    ON portfolio_alerts(subscription_id, status);
CREATE INDEX IF NOT EXISTS idx_alerts_investor
    ON portfolio_alerts(investor_id, investor_type);
CREATE INDEX IF NOT EXISTS idx_alerts_created
    ON portfolio_alerts(created_at DESC);

-- Portfolio snapshots for change detection
CREATE TABLE IF NOT EXISTS portfolio_snapshots (
    id SERIAL PRIMARY KEY,
    investor_id INTEGER NOT NULL,
    investor_type VARCHAR(20) NOT NULL,
    snapshot_date TIMESTAMP DEFAULT NOW(),
    snapshot_data JSONB NOT NULL,
    company_count INTEGER,
    total_value_usd NUMERIC,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_snapshots_investor
    ON portfolio_snapshots(investor_id, investor_type);
CREATE INDEX IF NOT EXISTS idx_snapshots_date
    ON portfolio_snapshots(snapshot_date DESC);
"""


class AlertEngine:
    """
    Core alert engine for detecting portfolio changes and managing alerts.

    Usage:
        engine = AlertEngine(db)
        await engine.initialize()  # Creates tables if needed

        # Detect changes after collection
        changes = engine.detect_changes(investor_id, investor_type, old_data, new_data)
        alerts = await engine.create_alerts_for_changes(investor_id, investor_type, investor_name, changes)
    """

    # Alert expiration period
    ALERT_EXPIRY_DAYS = 30

    def __init__(self, db: Session):
        self.db = db
        self._initialized = False

    async def initialize(self):
        """Initialize database tables if they don't exist."""
        if self._initialized:
            return

        try:
            # Split and execute each statement
            for statement in CREATE_TABLES_SQL.split(";"):
                statement = statement.strip()
                if statement and not statement.startswith("--"):
                    self.db.execute(text(statement))
            self.db.commit()
            self._initialized = True
            logger.info("Alert tables initialized")
        except Exception as e:
            logger.warning(f"Table initialization (may already exist): {e}")
            self.db.rollback()
            self._initialized = True

    def detect_changes(
        self,
        investor_id: int,
        investor_type: str,
        old_snapshot: List[Dict],
        new_snapshot: List[Dict],
        value_threshold_pct: float = 10.0,
    ) -> List[PortfolioChange]:
        """
        Detect changes between two portfolio snapshots.

        Args:
            investor_id: Investor ID
            investor_type: 'lp' or 'family_office'
            old_snapshot: Previous portfolio state (list of holdings)
            new_snapshot: Current portfolio state
            value_threshold_pct: Minimum % change to report value changes

        Returns:
            List of detected changes
        """
        changes = []

        # Index holdings by company name for comparison
        old_by_name = {h.get("company_name", "").lower(): h for h in old_snapshot}
        new_by_name = {h.get("company_name", "").lower(): h for h in new_snapshot}

        old_names = set(old_by_name.keys())
        new_names = set(new_by_name.keys())

        # Detect new holdings
        for name in new_names - old_names:
            if not name:
                continue
            holding = new_by_name[name]
            changes.append(
                PortfolioChange(
                    change_type=ChangeType.NEW_HOLDING,
                    company_name=holding.get("company_name", name),
                    details={
                        "market_value_usd": holding.get("market_value_usd"),
                        "shares_held": holding.get("shares_held"),
                        "source_type": holding.get("source_type"),
                    },
                )
            )

        # Detect removed holdings
        for name in old_names - new_names:
            if not name:
                continue
            holding = old_by_name[name]
            changes.append(
                PortfolioChange(
                    change_type=ChangeType.REMOVED_HOLDING,
                    company_name=holding.get("company_name", name),
                    details={
                        "last_market_value_usd": holding.get("market_value_usd"),
                        "last_shares_held": holding.get("shares_held"),
                    },
                )
            )

        # Detect value/share changes for existing holdings
        for name in old_names & new_names:
            if not name:
                continue
            old_h = old_by_name[name]
            new_h = new_by_name[name]

            # Value change
            old_value = old_h.get("market_value_usd") or 0
            new_value = new_h.get("market_value_usd") or 0

            if old_value > 0 and new_value > 0:
                value_change_pct = ((new_value - old_value) / old_value) * 100
                if abs(value_change_pct) >= value_threshold_pct:
                    changes.append(
                        PortfolioChange(
                            change_type=ChangeType.VALUE_CHANGE,
                            company_name=new_h.get("company_name", name),
                            details={
                                "old_value": old_value,
                                "new_value": new_value,
                                "change_pct": round(value_change_pct, 2),
                            },
                        )
                    )

            # Shares change
            old_shares = old_h.get("shares_held") or 0
            new_shares = new_h.get("shares_held") or 0

            if old_shares > 0 and new_shares > 0:
                shares_change_pct = ((new_shares - old_shares) / old_shares) * 100
                if abs(shares_change_pct) >= value_threshold_pct:
                    changes.append(
                        PortfolioChange(
                            change_type=ChangeType.SHARES_CHANGE,
                            company_name=new_h.get("company_name", name),
                            details={
                                "old_shares": old_shares,
                                "new_shares": new_shares,
                                "change_pct": round(shares_change_pct, 2),
                            },
                        )
                    )

        logger.info(
            f"Detected {len(changes)} changes for {investor_type} {investor_id}"
        )
        return changes

    async def get_subscriptions_for_investor(
        self, investor_id: int, investor_type: str
    ) -> List[Dict]:
        """Get all active subscriptions for an investor."""
        await self.initialize()

        result = self.db.execute(
            text("""
                SELECT id, user_id, change_types, value_threshold_pct, delivery_channels
                FROM alert_subscriptions
                WHERE investor_id = :investor_id
                    AND investor_type = :investor_type
                    AND is_active = TRUE
            """),
            {"investor_id": investor_id, "investor_type": investor_type},
        ).fetchall()

        return [
            {
                "id": row[0],
                "user_id": row[1],
                "change_types": row[2]
                if isinstance(row[2], list)
                else json.loads(row[2] or "[]"),
                "value_threshold_pct": row[3],
                "delivery_channels": row[4]
                if isinstance(row[4], list)
                else json.loads(row[4] or "[]"),
            }
            for row in result
        ]

    async def create_alerts_for_changes(
        self,
        investor_id: int,
        investor_type: str,
        investor_name: str,
        changes: List[PortfolioChange],
    ) -> List[int]:
        """
        Create alerts for detected changes, matched to subscriptions.

        Returns list of created alert IDs.
        """
        if not changes:
            return []

        await self.initialize()

        # Get subscriptions for this investor
        subscriptions = await self.get_subscriptions_for_investor(
            investor_id, investor_type
        )

        if not subscriptions:
            logger.debug(f"No subscriptions for {investor_type} {investor_id}")
            return []

        alert_ids = []

        for sub in subscriptions:
            sub_change_types = set(sub["change_types"])

            for change in changes:
                # Check if this subscription wants this change type
                if change.change_type.value not in sub_change_types:
                    continue

                # For value/share changes, check threshold
                if change.change_type in (
                    ChangeType.VALUE_CHANGE,
                    ChangeType.SHARES_CHANGE,
                ):
                    change_pct = abs(change.details.get("change_pct", 0))
                    if change_pct < sub["value_threshold_pct"]:
                        continue

                # Create alert
                result = self.db.execute(
                    text("""
                        INSERT INTO portfolio_alerts (
                            subscription_id, investor_id, investor_type, investor_name,
                            change_type, company_name, summary, details, status
                        ) VALUES (
                            :subscription_id, :investor_id, :investor_type, :investor_name,
                            :change_type, :company_name, :summary, :details, 'pending'
                        )
                        RETURNING id
                    """),
                    {
                        "subscription_id": sub["id"],
                        "investor_id": investor_id,
                        "investor_type": investor_type,
                        "investor_name": investor_name,
                        "change_type": change.change_type.value,
                        "company_name": change.company_name,
                        "summary": change.summary,
                        "details": json.dumps(change.details),
                    },
                )
                alert_id = result.fetchone()[0]
                alert_ids.append(alert_id)

        self.db.commit()
        logger.info(
            f"Created {len(alert_ids)} alerts for {investor_type} {investor_id}"
        )
        return alert_ids

    async def create_subscription(
        self,
        investor_id: int,
        investor_type: str,
        user_id: str,
        change_types: Optional[List[str]] = None,
        value_threshold_pct: float = 10.0,
        delivery_channels: Optional[List[str]] = None,
    ) -> Dict:
        """Create or update an alert subscription."""
        await self.initialize()

        if change_types is None:
            change_types = ["new_holding", "removed_holding"]
        if delivery_channels is None:
            delivery_channels = ["in_app"]

        # Upsert subscription
        result = self.db.execute(
            text("""
                INSERT INTO alert_subscriptions (
                    investor_id, investor_type, user_id,
                    change_types, value_threshold_pct, delivery_channels
                ) VALUES (
                    :investor_id, :investor_type, :user_id,
                    :change_types, :value_threshold_pct, :delivery_channels
                )
                ON CONFLICT (investor_id, investor_type, user_id)
                DO UPDATE SET
                    change_types = EXCLUDED.change_types,
                    value_threshold_pct = EXCLUDED.value_threshold_pct,
                    delivery_channels = EXCLUDED.delivery_channels,
                    is_active = TRUE,
                    updated_at = NOW()
                RETURNING id, created_at, updated_at
            """),
            {
                "investor_id": investor_id,
                "investor_type": investor_type,
                "user_id": user_id,
                "change_types": json.dumps(change_types),
                "value_threshold_pct": value_threshold_pct,
                "delivery_channels": json.dumps(delivery_channels),
            },
        )
        row = result.fetchone()
        self.db.commit()

        return {
            "id": row[0],
            "investor_id": investor_id,
            "investor_type": investor_type,
            "user_id": user_id,
            "change_types": change_types,
            "value_threshold_pct": value_threshold_pct,
            "delivery_channels": delivery_channels,
            "is_active": True,
            "created_at": row[1].isoformat() if row[1] else None,
            "updated_at": row[2].isoformat() if row[2] else None,
        }

    async def get_user_subscriptions(
        self, user_id: str, include_inactive: bool = False
    ) -> List[Dict]:
        """Get all subscriptions for a user."""
        await self.initialize()

        query = """
            SELECT
                s.id, s.investor_id, s.investor_type, s.user_id,
                s.change_types, s.value_threshold_pct, s.delivery_channels,
                s.is_active, s.created_at, s.updated_at,
                CASE
                    WHEN s.investor_type = 'lp' THEN lp.name
                    ELSE fo.name
                END as investor_name
            FROM alert_subscriptions s
            LEFT JOIN lp_fund lp ON s.investor_type = 'lp' AND s.investor_id = lp.id
            LEFT JOIN family_offices fo ON s.investor_type = 'family_office' AND s.investor_id = fo.id
            WHERE s.user_id = :user_id
        """

        if not include_inactive:
            query += " AND s.is_active = TRUE"

        query += " ORDER BY s.created_at DESC"

        result = self.db.execute(text(query), {"user_id": user_id}).fetchall()

        return [
            {
                "id": row[0],
                "investor_id": row[1],
                "investor_type": row[2],
                "user_id": row[3],
                "change_types": row[4]
                if isinstance(row[4], list)
                else json.loads(row[4] or "[]"),
                "value_threshold_pct": row[5],
                "delivery_channels": row[6]
                if isinstance(row[6], list)
                else json.loads(row[6] or "[]"),
                "is_active": row[7],
                "created_at": row[8].isoformat() if row[8] else None,
                "updated_at": row[9].isoformat() if row[9] else None,
                "investor_name": row[10],
            }
            for row in result
        ]

    async def delete_subscription(self, subscription_id: int, user_id: str) -> bool:
        """Deactivate a subscription (soft delete)."""
        await self.initialize()

        result = self.db.execute(
            text("""
                UPDATE alert_subscriptions
                SET is_active = FALSE, updated_at = NOW()
                WHERE id = :id AND user_id = :user_id
            """),
            {"id": subscription_id, "user_id": user_id},
        )
        self.db.commit()
        return result.rowcount > 0

    async def update_subscription(
        self,
        subscription_id: int,
        user_id: str,
        change_types: Optional[List[str]] = None,
        value_threshold_pct: Optional[float] = None,
        is_active: Optional[bool] = None,
    ) -> Optional[Dict]:
        """Update subscription settings."""
        await self.initialize()

        updates = []
        params = {"id": subscription_id, "user_id": user_id}

        if change_types is not None:
            updates.append("change_types = :change_types")
            params["change_types"] = json.dumps(change_types)

        if value_threshold_pct is not None:
            updates.append("value_threshold_pct = :value_threshold_pct")
            params["value_threshold_pct"] = value_threshold_pct

        if is_active is not None:
            updates.append("is_active = :is_active")
            params["is_active"] = is_active

        if not updates:
            return None

        updates.append("updated_at = NOW()")

        result = self.db.execute(
            text(f"""
                UPDATE alert_subscriptions
                SET {', '.join(updates)}
                WHERE id = :id AND user_id = :user_id
                RETURNING id, investor_id, investor_type, change_types,
                          value_threshold_pct, is_active, updated_at
            """),
            params,
        )
        row = result.fetchone()
        self.db.commit()

        if not row:
            return None

        return {
            "id": row[0],
            "investor_id": row[1],
            "investor_type": row[2],
            "change_types": row[3]
            if isinstance(row[3], list)
            else json.loads(row[3] or "[]"),
            "value_threshold_pct": row[4],
            "is_active": row[5],
            "updated_at": row[6].isoformat() if row[6] else None,
        }

    async def get_pending_alerts(
        self, user_id: str, limit: int = 50, offset: int = 0
    ) -> Tuple[List[Dict], int]:
        """Get pending alerts for a user."""
        await self.initialize()

        # Get alerts
        result = self.db.execute(
            text("""
                SELECT
                    a.id, a.investor_id, a.investor_type, a.investor_name,
                    a.change_type, a.company_name, a.summary, a.details,
                    a.status, a.created_at
                FROM portfolio_alerts a
                JOIN alert_subscriptions s ON a.subscription_id = s.id
                WHERE s.user_id = :user_id AND a.status = 'pending'
                ORDER BY a.created_at DESC
                LIMIT :limit OFFSET :offset
            """),
            {"user_id": user_id, "limit": limit, "offset": offset},
        ).fetchall()

        alerts = [
            {
                "id": row[0],
                "investor_id": row[1],
                "investor_type": row[2],
                "investor_name": row[3],
                "change_type": row[4],
                "company_name": row[5],
                "summary": row[6],
                "details": row[7]
                if isinstance(row[7], dict)
                else json.loads(row[7] or "{}"),
                "status": row[8],
                "created_at": row[9].isoformat() if row[9] else None,
            }
            for row in result
        ]

        # Get total count
        count_result = self.db.execute(
            text("""
                SELECT COUNT(*)
                FROM portfolio_alerts a
                JOIN alert_subscriptions s ON a.subscription_id = s.id
                WHERE s.user_id = :user_id AND a.status = 'pending'
            """),
            {"user_id": user_id},
        ).fetchone()

        return alerts, count_result[0]

    async def get_alert_history(
        self,
        user_id: str,
        limit: int = 100,
        offset: int = 0,
        investor_id: Optional[int] = None,
        investor_type: Optional[str] = None,
    ) -> Tuple[List[Dict], int]:
        """Get alert history for a user."""
        await self.initialize()

        query = """
            SELECT
                a.id, a.investor_id, a.investor_type, a.investor_name,
                a.change_type, a.company_name, a.summary, a.details,
                a.status, a.created_at, a.acknowledged_at
            FROM portfolio_alerts a
            JOIN alert_subscriptions s ON a.subscription_id = s.id
            WHERE s.user_id = :user_id
        """
        params = {"user_id": user_id, "limit": limit, "offset": offset}

        if investor_id is not None:
            query += " AND a.investor_id = :investor_id"
            params["investor_id"] = investor_id

        if investor_type is not None:
            query += " AND a.investor_type = :investor_type"
            params["investor_type"] = investor_type

        query += " ORDER BY a.created_at DESC LIMIT :limit OFFSET :offset"

        result = self.db.execute(text(query), params).fetchall()

        alerts = [
            {
                "id": row[0],
                "investor_id": row[1],
                "investor_type": row[2],
                "investor_name": row[3],
                "change_type": row[4],
                "company_name": row[5],
                "summary": row[6],
                "details": row[7]
                if isinstance(row[7], dict)
                else json.loads(row[7] or "{}"),
                "status": row[8],
                "created_at": row[9].isoformat() if row[9] else None,
                "acknowledged_at": row[10].isoformat() if row[10] else None,
            }
            for row in result
        ]

        # Count query
        count_query = """
            SELECT COUNT(*)
            FROM portfolio_alerts a
            JOIN alert_subscriptions s ON a.subscription_id = s.id
            WHERE s.user_id = :user_id
        """
        count_params = {"user_id": user_id}

        if investor_id is not None:
            count_query += " AND a.investor_id = :investor_id"
            count_params["investor_id"] = investor_id

        if investor_type is not None:
            count_query += " AND a.investor_type = :investor_type"
            count_params["investor_type"] = investor_type

        count_result = self.db.execute(text(count_query), count_params).fetchone()

        return alerts, count_result[0]

    async def acknowledge_alert(self, alert_id: int, user_id: str) -> bool:
        """Mark an alert as acknowledged."""
        await self.initialize()

        result = self.db.execute(
            text("""
                UPDATE portfolio_alerts a
                SET status = 'acknowledged', acknowledged_at = NOW()
                FROM alert_subscriptions s
                WHERE a.id = :alert_id
                    AND a.subscription_id = s.id
                    AND s.user_id = :user_id
                    AND a.status = 'pending'
            """),
            {"alert_id": alert_id, "user_id": user_id},
        )
        self.db.commit()
        return result.rowcount > 0

    async def acknowledge_all_alerts(self, user_id: str) -> int:
        """Acknowledge all pending alerts for a user."""
        await self.initialize()

        result = self.db.execute(
            text("""
                UPDATE portfolio_alerts a
                SET status = 'acknowledged', acknowledged_at = NOW()
                FROM alert_subscriptions s
                WHERE a.subscription_id = s.id
                    AND s.user_id = :user_id
                    AND a.status = 'pending'
            """),
            {"user_id": user_id},
        )
        self.db.commit()
        return result.rowcount

    async def save_snapshot(
        self, investor_id: int, investor_type: str, portfolio_data: List[Dict]
    ) -> int:
        """Save a portfolio snapshot for change detection."""
        await self.initialize()

        total_value = sum(h.get("market_value_usd", 0) or 0 for h in portfolio_data)

        result = self.db.execute(
            text("""
                INSERT INTO portfolio_snapshots (
                    investor_id, investor_type, snapshot_data,
                    company_count, total_value_usd
                ) VALUES (
                    :investor_id, :investor_type, :snapshot_data,
                    :company_count, :total_value_usd
                )
                RETURNING id
            """),
            {
                "investor_id": investor_id,
                "investor_type": investor_type,
                "snapshot_data": json.dumps(portfolio_data),
                "company_count": len(portfolio_data),
                "total_value_usd": total_value,
            },
        )
        snapshot_id = result.fetchone()[0]
        self.db.commit()
        return snapshot_id

    async def get_latest_snapshot(
        self, investor_id: int, investor_type: str
    ) -> Optional[List[Dict]]:
        """Get the most recent snapshot for an investor."""
        await self.initialize()

        result = self.db.execute(
            text("""
                SELECT snapshot_data
                FROM portfolio_snapshots
                WHERE investor_id = :investor_id AND investor_type = :investor_type
                ORDER BY snapshot_date DESC
                LIMIT 1
            """),
            {"investor_id": investor_id, "investor_type": investor_type},
        ).fetchone()

        if not result:
            return None

        data = result[0]
        if isinstance(data, str):
            return json.loads(data)
        return data

    async def cleanup_expired_alerts(self) -> int:
        """Mark old pending alerts as expired."""
        await self.initialize()

        cutoff = datetime.utcnow() - timedelta(days=self.ALERT_EXPIRY_DAYS)

        result = self.db.execute(
            text("""
                UPDATE portfolio_alerts
                SET status = 'expired'
                WHERE status = 'pending' AND created_at < :cutoff
            """),
            {"cutoff": cutoff},
        )
        self.db.commit()
        return result.rowcount


# Singleton instance
_alert_engine: Optional[AlertEngine] = None


def get_alert_engine(db: Session) -> AlertEngine:
    """Get or create the alert engine instance."""
    global _alert_engine
    if _alert_engine is None or _alert_engine.db != db:
        _alert_engine = AlertEngine(db)
    return _alert_engine
