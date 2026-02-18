"""
Webhook management endpoints.

Provides API for creating, updating, and managing webhook notifications.
"""

import logging
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field, HttpUrl
from sqlalchemy.orm import Session
from datetime import datetime

from app.core.database import get_db
from app.core.models import Webhook, WebhookDelivery, WebhookEventType
from app.core import webhook_service
from app.core import monitoring

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/webhooks", tags=["webhooks"])


# =============================================================================
# Pydantic Schemas
# =============================================================================


class WebhookCreate(BaseModel):
    """Request schema for creating a webhook."""

    name: str = Field(..., min_length=1, max_length=255)
    url: str = Field(..., min_length=1, max_length=2048)
    event_types: List[str] = Field(..., min_length=1)
    source_filter: Optional[str] = Field(None, max_length=50)
    secret: Optional[str] = Field(None, max_length=255)
    headers: Optional[dict] = None
    is_active: bool = True


class WebhookUpdate(BaseModel):
    """Request schema for updating a webhook."""

    name: Optional[str] = Field(None, min_length=1, max_length=255)
    url: Optional[str] = Field(None, min_length=1, max_length=2048)
    event_types: Optional[List[str]] = None
    source_filter: Optional[str] = Field(None, max_length=50)
    secret: Optional[str] = Field(None, max_length=255)
    headers: Optional[dict] = None
    is_active: Optional[bool] = None


class WebhookResponse(BaseModel):
    """Response schema for webhook information."""

    id: int
    name: str
    url: str
    event_types: List[str]
    source_filter: Optional[str]
    has_secret: bool
    headers: Optional[dict]
    is_active: bool
    total_sent: int
    total_failed: int
    last_sent_at: Optional[datetime]
    last_error: Optional[str]
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}

    @classmethod
    def from_orm_with_masked_secret(cls, obj: Webhook) -> "WebhookResponse":
        """Convert ORM object, masking the secret."""
        return cls(
            id=obj.id,
            name=obj.name,
            url=obj.url,
            event_types=obj.event_types or [],
            source_filter=obj.source_filter,
            has_secret=bool(obj.secret),
            headers=obj.headers,
            is_active=bool(obj.is_active),
            total_sent=obj.total_sent,
            total_failed=obj.total_failed,
            last_sent_at=obj.last_sent_at,
            last_error=obj.last_error,
            created_at=obj.created_at,
            updated_at=obj.updated_at,
        )


class WebhookDeliveryResponse(BaseModel):
    """Response schema for webhook delivery information."""

    id: int
    webhook_id: int
    event_type: str
    status: str
    response_code: Optional[int]
    error_message: Optional[str]
    created_at: datetime
    delivered_at: Optional[datetime]
    attempt_number: int

    model_config = {"from_attributes": True}


class WebhookTestResult(BaseModel):
    """Response schema for webhook test result."""

    webhook_id: int
    webhook_name: str
    test_successful: bool
    status_code: Optional[int]
    error: Optional[str]


# =============================================================================
# Endpoints
# =============================================================================


@router.get("/event-types")
def list_event_types():
    """
    List all available webhook event types.

    Returns the event types that can be subscribed to for webhook notifications.
    """
    return {
        "event_types": [
            {
                "value": event_type.value,
                "name": event_type.name,
                "description": _get_event_description(event_type),
            }
            for event_type in WebhookEventType
        ]
    }


def _get_event_description(event_type: WebhookEventType) -> str:
    """Get human-readable description for event type."""
    descriptions = {
        WebhookEventType.JOB_FAILED: "Triggered when an ingestion job fails",
        WebhookEventType.JOB_SUCCESS: "Triggered when an ingestion job completes successfully",
        WebhookEventType.ALERT_HIGH_FAILURE_RATE: "Triggered when failure rate exceeds threshold",
        WebhookEventType.ALERT_STUCK_JOB: "Triggered when a job is stuck in running state",
        WebhookEventType.ALERT_DATA_STALENESS: "Triggered when data becomes stale",
        WebhookEventType.SCHEDULE_TRIGGERED: "Triggered when a scheduled job starts",
        WebhookEventType.CLEANUP_COMPLETED: "Triggered when stuck job cleanup completes",
    }
    return descriptions.get(event_type, "No description available")


@router.get("", response_model=List[WebhookResponse])
def list_webhooks(
    active_only: bool = False, db: Session = Depends(get_db)
) -> List[WebhookResponse]:
    """
    List all webhooks with optional filtering.
    """
    query = db.query(Webhook)

    if active_only:
        query = query.filter(Webhook.is_active == 1)

    query = query.order_by(Webhook.name)
    webhooks = query.all()

    return [WebhookResponse.from_orm_with_masked_secret(w) for w in webhooks]


@router.post("", response_model=WebhookResponse, status_code=201)
def create_webhook(
    webhook_request: WebhookCreate, db: Session = Depends(get_db)
) -> WebhookResponse:
    """
    Create a new webhook configuration.

    Supported webhook URLs:
    - Slack: https://hooks.slack.com/services/...
    - Discord: https://discord.com/api/webhooks/...
    - Generic: Any HTTPS endpoint accepting POST with JSON body
    """
    # Validate event types
    valid_event_types = [e.value for e in WebhookEventType]
    for event_type in webhook_request.event_types:
        if event_type not in valid_event_types:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid event type: {event_type}. Valid types: {valid_event_types}",
            )

    # Check for duplicate name
    existing = db.query(Webhook).filter(Webhook.name == webhook_request.name).first()
    if existing:
        raise HTTPException(status_code=400, detail="Webhook name already exists")

    webhook = webhook_service.create_webhook(
        db=db,
        name=webhook_request.name,
        url=webhook_request.url,
        event_types=webhook_request.event_types,
        source_filter=webhook_request.source_filter,
        secret=webhook_request.secret,
        headers=webhook_request.headers,
        is_active=webhook_request.is_active,
    )

    return WebhookResponse.from_orm_with_masked_secret(webhook)


@router.get("/{webhook_id}", response_model=WebhookResponse)
def get_webhook(webhook_id: int, db: Session = Depends(get_db)) -> WebhookResponse:
    """
    Get a specific webhook by ID.
    """
    webhook = db.query(Webhook).filter(Webhook.id == webhook_id).first()

    if not webhook:
        raise HTTPException(status_code=404, detail="Webhook not found")

    return WebhookResponse.from_orm_with_masked_secret(webhook)


@router.put("/{webhook_id}", response_model=WebhookResponse)
def update_webhook(
    webhook_id: int, webhook_request: WebhookUpdate, db: Session = Depends(get_db)
) -> WebhookResponse:
    """
    Update an existing webhook.
    """
    # Get update data, excluding None values
    update_data = webhook_request.model_dump(exclude_none=True)

    if not update_data:
        raise HTTPException(status_code=400, detail="No fields to update")

    # Validate event types if provided
    if "event_types" in update_data:
        valid_event_types = [e.value for e in WebhookEventType]
        for event_type in update_data["event_types"]:
            if event_type not in valid_event_types:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid event type: {event_type}. Valid types: {valid_event_types}",
                )

    webhook = webhook_service.update_webhook(db, webhook_id, **update_data)

    if not webhook:
        raise HTTPException(status_code=404, detail="Webhook not found")

    return WebhookResponse.from_orm_with_masked_secret(webhook)


@router.delete("/{webhook_id}")
def delete_webhook(webhook_id: int, db: Session = Depends(get_db)):
    """
    Delete a webhook configuration.
    """
    success = webhook_service.delete_webhook(db, webhook_id)

    if not success:
        raise HTTPException(status_code=404, detail="Webhook not found")

    return {"message": f"Webhook {webhook_id} deleted"}


@router.post("/{webhook_id}/activate", response_model=WebhookResponse)
def activate_webhook(webhook_id: int, db: Session = Depends(get_db)) -> WebhookResponse:
    """
    Activate a paused webhook.
    """
    webhook = webhook_service.update_webhook(db, webhook_id, is_active=True)

    if not webhook:
        raise HTTPException(status_code=404, detail="Webhook not found")

    return WebhookResponse.from_orm_with_masked_secret(webhook)


@router.post("/{webhook_id}/pause", response_model=WebhookResponse)
def pause_webhook(webhook_id: int, db: Session = Depends(get_db)) -> WebhookResponse:
    """
    Pause an active webhook.
    """
    webhook = webhook_service.update_webhook(db, webhook_id, is_active=False)

    if not webhook:
        raise HTTPException(status_code=404, detail="Webhook not found")

    return WebhookResponse.from_orm_with_masked_secret(webhook)


@router.post("/{webhook_id}/test", response_model=WebhookTestResult)
async def test_webhook(
    webhook_id: int, db: Session = Depends(get_db)
) -> WebhookTestResult:
    """
    Send a test notification to a webhook.

    Sends a test payload to verify the webhook is configured correctly.
    """
    webhook = db.query(Webhook).filter(Webhook.id == webhook_id).first()

    if not webhook:
        raise HTTPException(status_code=404, detail="Webhook not found")

    result = await webhook_service.test_webhook(webhook)

    return WebhookTestResult(**result)


@router.get("/{webhook_id}/deliveries", response_model=List[WebhookDeliveryResponse])
def get_webhook_deliveries(
    webhook_id: int,
    limit: int = Query(default=50, ge=1, le=200),
    db: Session = Depends(get_db),
) -> List[WebhookDeliveryResponse]:
    """
    Get recent delivery history for a webhook.
    """
    webhook = db.query(Webhook).filter(Webhook.id == webhook_id).first()

    if not webhook:
        raise HTTPException(status_code=404, detail="Webhook not found")

    deliveries = webhook_service.get_webhook_deliveries(db, webhook_id, limit)

    return [WebhookDeliveryResponse.model_validate(d) for d in deliveries]


@router.get("/{webhook_id}/stats")
def get_webhook_stats(webhook_id: int, db: Session = Depends(get_db)):
    """
    Get delivery statistics for a webhook.
    """
    webhook = db.query(Webhook).filter(Webhook.id == webhook_id).first()

    if not webhook:
        raise HTTPException(status_code=404, detail="Webhook not found")

    # Calculate success rate
    total = webhook.total_sent + webhook.total_failed
    success_rate = (webhook.total_sent / total * 100) if total > 0 else 0

    return {
        "webhook_id": webhook.id,
        "webhook_name": webhook.name,
        "total_sent": webhook.total_sent,
        "total_failed": webhook.total_failed,
        "total_attempts": total,
        "success_rate": round(success_rate, 2),
        "last_sent_at": webhook.last_sent_at.isoformat()
        if webhook.last_sent_at
        else None,
        "last_error": webhook.last_error,
        "is_active": bool(webhook.is_active),
    }


# =============================================================================
# Alert Notification Endpoints
# =============================================================================


@router.post("/alerts/check")
async def check_and_notify_alerts(
    failure_threshold: int = Query(
        default=3, ge=1, description="Failures to trigger alert"
    ),
    time_window_hours: int = Query(
        default=1, ge=1, description="Time window for failure count"
    ),
    db: Session = Depends(get_db),
):
    """
    Check for monitoring alerts and send webhook notifications.

    Scans for alert conditions (high failure rate, stuck jobs, data staleness)
    and sends notifications to all configured webhooks subscribed to those events.

    Args:
        failure_threshold: Number of failures required to trigger high_failure_rate alert
        time_window_hours: Time window in hours for counting failures

    Returns:
        Summary of alerts found and notifications sent
    """
    result = await monitoring.check_and_notify_alerts(
        db=db, failure_threshold=failure_threshold, time_window_hours=time_window_hours
    )
    return result
