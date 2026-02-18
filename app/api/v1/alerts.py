"""
Portfolio Change Alerts API (T11).

Endpoints for subscribing to investor portfolio alerts and managing notifications.
"""

import logging
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field, EmailStr
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.notifications.alerts import get_alert_engine, ChangeType

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/alerts", tags=["Portfolio Alerts"])


# =============================================================================
# Request/Response Models
# =============================================================================


class AlertSubscriptionRequest(BaseModel):
    """Request to subscribe to portfolio alerts."""

    investor_id: int = Field(..., description="Investor ID to watch")
    investor_type: str = Field(..., description="'lp' or 'family_office'")
    user_id: str = Field(..., description="Email or user identifier for notifications")
    change_types: List[str] = Field(
        default=["new_holding", "removed_holding"],
        description="Types of changes to alert on: new_holding, removed_holding, value_change, shares_change",
    )
    value_threshold_pct: float = Field(
        default=10.0,
        ge=0.1,
        le=100.0,
        description="Minimum % change to trigger value/shares alerts",
    )


class AlertSubscriptionUpdate(BaseModel):
    """Request to update subscription settings."""

    change_types: Optional[List[str]] = Field(
        None, description="Types of changes to alert on"
    )
    value_threshold_pct: Optional[float] = Field(
        None, ge=0.1, le=100.0, description="Minimum % change threshold"
    )
    is_active: Optional[bool] = Field(None, description="Enable/disable subscription")


class AlertSubscriptionResponse(BaseModel):
    """Response for subscription operations."""

    id: int
    investor_id: int
    investor_type: str
    investor_name: Optional[str]
    user_id: str
    change_types: List[str]
    value_threshold_pct: float
    is_active: bool
    created_at: Optional[str]
    updated_at: Optional[str]


class AlertResponse(BaseModel):
    """Response for a single alert."""

    id: int
    investor_id: int
    investor_type: str
    investor_name: Optional[str]
    change_type: str
    company_name: str
    summary: Optional[str]
    details: dict
    status: str
    created_at: Optional[str]
    acknowledged_at: Optional[str] = None


class AlertListResponse(BaseModel):
    """Response for alert list."""

    total: int
    limit: int
    offset: int
    alerts: List[AlertResponse]


class SubscriptionListResponse(BaseModel):
    """Response for subscription list."""

    total: int
    subscriptions: List[AlertSubscriptionResponse]


# =============================================================================
# Endpoints
# =============================================================================


@router.post("/subscribe", response_model=AlertSubscriptionResponse)
async def subscribe_to_alerts(
    request: AlertSubscriptionRequest, db: Session = Depends(get_db)
):
    """
    🔔 Subscribe to portfolio change alerts for an investor.

    Creates a subscription to receive alerts when the investor's portfolio changes.
    If a subscription already exists for this investor/user combo, it will be updated.

    **Change Types:**
    - `new_holding`: Alert when investor adds a new company
    - `removed_holding`: Alert when investor exits a position
    - `value_change`: Alert when position value changes by threshold %
    - `shares_change`: Alert when share count changes by threshold %

    **Example Request:**
    ```json
    {
        "investor_id": 123,
        "investor_type": "lp",
        "user_id": "user@example.com",
        "change_types": ["new_holding", "removed_holding"],
        "value_threshold_pct": 10.0
    }
    ```
    """
    try:
        # Validate investor_type
        if request.investor_type not in ("lp", "family_office"):
            raise HTTPException(
                status_code=400, detail="investor_type must be 'lp' or 'family_office'"
            )

        # Validate change_types
        valid_types = {ct.value for ct in ChangeType}
        for ct in request.change_types:
            if ct not in valid_types:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid change_type '{ct}'. Valid types: {valid_types}",
                )

        engine = get_alert_engine(db)

        result = await engine.create_subscription(
            investor_id=request.investor_id,
            investor_type=request.investor_type,
            user_id=request.user_id,
            change_types=request.change_types,
            value_threshold_pct=request.value_threshold_pct,
        )

        # Get investor name
        investor_name = await _get_investor_name(
            db, request.investor_id, request.investor_type
        )

        return AlertSubscriptionResponse(
            id=result["id"],
            investor_id=result["investor_id"],
            investor_type=result["investor_type"],
            investor_name=investor_name,
            user_id=result["user_id"],
            change_types=result["change_types"],
            value_threshold_pct=result["value_threshold_pct"],
            is_active=result["is_active"],
            created_at=result["created_at"],
            updated_at=result["updated_at"],
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating subscription: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/subscriptions", response_model=SubscriptionListResponse)
async def list_subscriptions(
    user_id: str = Query(..., description="User ID to get subscriptions for"),
    include_inactive: bool = Query(
        False, description="Include deactivated subscriptions"
    ),
    db: Session = Depends(get_db),
):
    """
    📋 List all alert subscriptions for a user.

    Returns all investors the user is subscribed to receive alerts for.
    """
    try:
        engine = get_alert_engine(db)
        subscriptions = await engine.get_user_subscriptions(
            user_id=user_id, include_inactive=include_inactive
        )

        return SubscriptionListResponse(
            total=len(subscriptions),
            subscriptions=[
                AlertSubscriptionResponse(
                    id=sub["id"],
                    investor_id=sub["investor_id"],
                    investor_type=sub["investor_type"],
                    investor_name=sub.get("investor_name"),
                    user_id=sub["user_id"],
                    change_types=sub["change_types"],
                    value_threshold_pct=sub["value_threshold_pct"],
                    is_active=sub["is_active"],
                    created_at=sub["created_at"],
                    updated_at=sub["updated_at"],
                )
                for sub in subscriptions
            ],
        )

    except Exception as e:
        logger.error(f"Error listing subscriptions: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/subscriptions/{subscription_id}")
async def unsubscribe(
    subscription_id: int,
    user_id: str = Query(..., description="User ID for verification"),
    db: Session = Depends(get_db),
):
    """
    🔕 Unsubscribe from alerts for an investor.

    Deactivates the subscription (soft delete). Alerts already created are preserved.
    """
    try:
        engine = get_alert_engine(db)
        success = await engine.delete_subscription(subscription_id, user_id)

        if not success:
            raise HTTPException(
                status_code=404,
                detail=f"Subscription {subscription_id} not found or not owned by user",
            )

        return {
            "message": "Subscription deactivated",
            "subscription_id": subscription_id,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting subscription: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/subscriptions/{subscription_id}", response_model=dict)
async def update_subscription(
    subscription_id: int,
    request: AlertSubscriptionUpdate,
    user_id: str = Query(..., description="User ID for verification"),
    db: Session = Depends(get_db),
):
    """
    ✏️ Update subscription settings.

    Modify alert preferences like change types or value threshold.
    """
    try:
        # Validate change_types if provided
        if request.change_types:
            valid_types = {ct.value for ct in ChangeType}
            for ct in request.change_types:
                if ct not in valid_types:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Invalid change_type '{ct}'. Valid types: {valid_types}",
                    )

        engine = get_alert_engine(db)
        result = await engine.update_subscription(
            subscription_id=subscription_id,
            user_id=user_id,
            change_types=request.change_types,
            value_threshold_pct=request.value_threshold_pct,
            is_active=request.is_active,
        )

        if not result:
            raise HTTPException(
                status_code=404,
                detail=f"Subscription {subscription_id} not found or not owned by user",
            )

        return {"message": "Subscription updated", **result}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating subscription: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("", response_model=AlertListResponse)
async def get_pending_alerts(
    user_id: str = Query(..., description="User ID to get alerts for"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """
    🔔 Get pending alerts for a user.

    Returns alerts that have not been acknowledged yet.
    """
    try:
        engine = get_alert_engine(db)
        alerts, total = await engine.get_pending_alerts(
            user_id=user_id, limit=limit, offset=offset
        )

        return AlertListResponse(
            total=total,
            limit=limit,
            offset=offset,
            alerts=[AlertResponse(**alert) for alert in alerts],
        )

    except Exception as e:
        logger.error(f"Error getting alerts: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{alert_id}/acknowledge")
async def acknowledge_alert(
    alert_id: int,
    user_id: str = Query(..., description="User ID for verification"),
    db: Session = Depends(get_db),
):
    """
    ✅ Acknowledge (dismiss) an alert.

    Marks the alert as acknowledged so it no longer appears in pending alerts.
    """
    try:
        engine = get_alert_engine(db)
        success = await engine.acknowledge_alert(alert_id, user_id)

        if not success:
            raise HTTPException(
                status_code=404,
                detail=f"Alert {alert_id} not found, already acknowledged, or not owned by user",
            )

        return {"message": "Alert acknowledged", "alert_id": alert_id}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error acknowledging alert: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/acknowledge-all")
async def acknowledge_all_alerts(
    user_id: str = Query(..., description="User ID to acknowledge alerts for"),
    db: Session = Depends(get_db),
):
    """
    ✅ Acknowledge all pending alerts for a user.

    Bulk dismiss operation for clearing the alert queue.
    """
    try:
        engine = get_alert_engine(db)
        count = await engine.acknowledge_all_alerts(user_id)

        return {"message": f"Acknowledged {count} alerts", "acknowledged_count": count}

    except Exception as e:
        logger.error(f"Error acknowledging all alerts: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/history", response_model=AlertListResponse)
async def get_alert_history(
    user_id: str = Query(..., description="User ID to get history for"),
    investor_id: Optional[int] = Query(None, description="Filter by investor ID"),
    investor_type: Optional[str] = Query(None, description="Filter by investor type"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """
    📜 Get alert history for a user.

    Returns all alerts (pending, acknowledged, expired) with optional filtering.
    """
    try:
        engine = get_alert_engine(db)
        alerts, total = await engine.get_alert_history(
            user_id=user_id,
            limit=limit,
            offset=offset,
            investor_id=investor_id,
            investor_type=investor_type,
        )

        return AlertListResponse(
            total=total,
            limit=limit,
            offset=offset,
            alerts=[AlertResponse(**alert) for alert in alerts],
        )

    except Exception as e:
        logger.error(f"Error getting alert history: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# Helper Functions
# =============================================================================


async def _get_investor_name(
    db: Session, investor_id: int, investor_type: str
) -> Optional[str]:
    """Get investor name from database."""
    from sqlalchemy import text

    if investor_type == "lp":
        query = "SELECT name FROM lp_fund WHERE id = :id"
    else:
        query = "SELECT name FROM family_offices WHERE id = :id"

    result = db.execute(text(query), {"id": investor_id}).fetchone()
    return result[0] if result else None
