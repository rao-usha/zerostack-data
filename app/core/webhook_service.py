"""
Webhook notification service.

Sends HTTP POST notifications to configured webhook endpoints when events occur.
Supports Slack, Discord, and generic webhook formats.
"""

import logging
import hashlib
import hmac
import json
from typing import Dict, Any, List, Optional
from datetime import datetime
import httpx
from sqlalchemy.orm import Session

from app.core.models import Webhook, WebhookDelivery, WebhookEventType
from app.core.database import get_session_factory

logger = logging.getLogger(__name__)

# Timeout for webhook requests
WEBHOOK_TIMEOUT = 10.0  # seconds


def get_active_webhooks(
    db: Session, event_type: WebhookEventType, source: Optional[str] = None
) -> List[Webhook]:
    """
    Get all active webhooks that should receive a given event type.

    Args:
        db: Database session
        event_type: The type of event
        source: Optional source filter

    Returns:
        List of matching webhooks
    """
    query = db.query(Webhook).filter(Webhook.is_active == 1)

    webhooks = query.all()

    # Filter by event type (stored as JSON array)
    matching = []
    for webhook in webhooks:
        event_types = webhook.event_types or []
        if event_type.value in event_types:
            # Check source filter if specified
            if webhook.source_filter:
                if source and webhook.source_filter == source:
                    matching.append(webhook)
            else:
                matching.append(webhook)

    return matching


def format_payload(
    event_type: WebhookEventType, event_data: Dict[str, Any], webhook: Webhook
) -> Dict[str, Any]:
    """
    Format the webhook payload.

    Detects Slack/Discord URLs and formats accordingly.

    Args:
        event_type: The event type
        event_data: The event data
        webhook: The webhook configuration

    Returns:
        Formatted payload for the webhook
    """
    # Detect Slack webhook
    if "hooks.slack.com" in webhook.url:
        return format_slack_payload(event_type, event_data)

    # Detect Discord webhook
    if "discord.com/api/webhooks" in webhook.url:
        return format_discord_payload(event_type, event_data)

    # Generic payload
    return {
        "event_type": event_type.value,
        "timestamp": datetime.utcnow().isoformat(),
        "data": event_data,
    }


def format_slack_payload(
    event_type: WebhookEventType, event_data: Dict[str, Any]
) -> Dict[str, Any]:
    """Format payload for Slack webhook."""
    # Choose emoji and color based on event type
    emoji_map = {
        WebhookEventType.JOB_FAILED: ":x:",
        WebhookEventType.JOB_SUCCESS: ":white_check_mark:",
        WebhookEventType.ALERT_HIGH_FAILURE_RATE: ":warning:",
        WebhookEventType.ALERT_STUCK_JOB: ":hourglass:",
        WebhookEventType.ALERT_DATA_STALENESS: ":calendar:",
        WebhookEventType.SCHEDULE_TRIGGERED: ":clock3:",
        WebhookEventType.CLEANUP_COMPLETED: ":broom:",
        WebhookEventType.SITE_INTEL_FAILED: ":rotating_light:",
        WebhookEventType.SITE_INTEL_SUCCESS: ":satellite:",
        WebhookEventType.ALERT_CONSECUTIVE_FAILURES: ":fire:",
    }

    color_map = {
        WebhookEventType.JOB_FAILED: "danger",
        WebhookEventType.JOB_SUCCESS: "good",
        WebhookEventType.ALERT_HIGH_FAILURE_RATE: "danger",
        WebhookEventType.ALERT_STUCK_JOB: "warning",
        WebhookEventType.ALERT_DATA_STALENESS: "warning",
        WebhookEventType.SCHEDULE_TRIGGERED: "#439FE0",
        WebhookEventType.CLEANUP_COMPLETED: "good",
        WebhookEventType.SITE_INTEL_FAILED: "danger",
        WebhookEventType.SITE_INTEL_SUCCESS: "good",
        WebhookEventType.ALERT_CONSECUTIVE_FAILURES: "danger",
    }

    emoji = emoji_map.get(event_type, ":bell:")
    color = color_map.get(event_type, "#808080")

    # Build message text
    title = f"{emoji} Nexdata: {event_type.value.replace('_', ' ').title()}"

    # Build fields from event data
    fields = []
    for key, value in event_data.items():
        if key not in ["timestamp", "created_at"]:
            fields.append(
                {
                    "title": key.replace("_", " ").title(),
                    "value": str(value)[:200],
                    "short": len(str(value)) < 40,
                }
            )

    return {
        "attachments": [
            {
                "fallback": title,
                "color": color,
                "title": title,
                "fields": fields[:10],  # Slack limits fields
                "footer": "Nexdata Monitoring",
                "ts": int(datetime.utcnow().timestamp()),
            }
        ]
    }


def format_discord_payload(
    event_type: WebhookEventType, event_data: Dict[str, Any]
) -> Dict[str, Any]:
    """Format payload for Discord webhook."""
    color_map = {
        WebhookEventType.JOB_FAILED: 0xFF0000,  # Red
        WebhookEventType.JOB_SUCCESS: 0x00FF00,  # Green
        WebhookEventType.ALERT_HIGH_FAILURE_RATE: 0xFF0000,
        WebhookEventType.ALERT_STUCK_JOB: 0xFFA500,  # Orange
        WebhookEventType.ALERT_DATA_STALENESS: 0xFFA500,
        WebhookEventType.SCHEDULE_TRIGGERED: 0x0099FF,  # Blue
        WebhookEventType.CLEANUP_COMPLETED: 0x00FF00,
        WebhookEventType.SITE_INTEL_FAILED: 0xFF0000,
        WebhookEventType.SITE_INTEL_SUCCESS: 0x00FF00,
        WebhookEventType.ALERT_CONSECUTIVE_FAILURES: 0xFF4500,  # OrangeRed
    }

    color = color_map.get(event_type, 0x808080)
    title = f"Nexdata: {event_type.value.replace('_', ' ').title()}"

    # Build fields
    fields = []
    for key, value in event_data.items():
        if key not in ["timestamp", "created_at"]:
            fields.append(
                {
                    "name": key.replace("_", " ").title(),
                    "value": str(value)[:1024],
                    "inline": len(str(value)) < 40,
                }
            )

    return {
        "embeds": [
            {
                "title": title,
                "color": color,
                "fields": fields[:25],  # Discord limits to 25 fields
                "footer": {"text": "Nexdata Monitoring"},
                "timestamp": datetime.utcnow().isoformat(),
            }
        ]
    }


def compute_signature(payload: str, secret: str) -> str:
    """Compute HMAC-SHA256 signature for payload."""
    return hmac.new(
        secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256
    ).hexdigest()


async def send_webhook(
    webhook: Webhook, event_type: WebhookEventType, event_data: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Send a webhook notification.

    Args:
        webhook: The webhook configuration
        event_type: The event type
        event_data: The event data

    Returns:
        Dictionary with delivery result
    """
    # Format payload
    payload = format_payload(event_type, event_data, webhook)
    payload_str = json.dumps(payload)

    # Build headers
    headers = {"Content-Type": "application/json", "User-Agent": "Nexdata-Webhook/1.0"}

    # Add custom headers
    if webhook.headers:
        headers.update(webhook.headers)

    # Add signature if secret is configured
    if webhook.secret:
        signature = compute_signature(payload_str, webhook.secret)
        headers["X-Webhook-Signature"] = f"sha256={signature}"

    # Send request
    try:
        async with httpx.AsyncClient(timeout=WEBHOOK_TIMEOUT) as client:
            response = await client.post(
                webhook.url, content=payload_str, headers=headers
            )

            return {
                "success": response.status_code < 400,
                "status_code": response.status_code,
                "response_body": response.text[:500] if response.text else None,
                "error": None,
            }

    except httpx.TimeoutException:
        return {
            "success": False,
            "status_code": None,
            "response_body": None,
            "error": "Request timed out",
        }
    except Exception as e:
        return {
            "success": False,
            "status_code": None,
            "response_body": None,
            "error": str(e),
        }


async def trigger_webhooks(
    event_type: WebhookEventType,
    event_data: Dict[str, Any],
    source: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Trigger all webhooks for an event.

    Args:
        event_type: The event type
        event_data: The event data
        source: Optional source filter

    Returns:
        Summary of webhook deliveries
    """
    SessionLocal = get_session_factory()
    db = SessionLocal()

    results = {
        "event_type": event_type.value,
        "webhooks_triggered": 0,
        "successful": 0,
        "failed": 0,
        "deliveries": [],
    }

    try:
        # Get matching webhooks
        webhooks = get_active_webhooks(db, event_type, source)

        if not webhooks:
            logger.debug(f"No webhooks configured for event {event_type.value}")
            return results

        results["webhooks_triggered"] = len(webhooks)

        # Send to each webhook
        for webhook in webhooks:
            # Create delivery record
            delivery = WebhookDelivery(
                webhook_id=webhook.id,
                event_type=event_type.value,
                event_data=event_data,
                status="pending",
            )
            db.add(delivery)
            db.commit()
            db.refresh(delivery)

            # Send webhook
            result = await send_webhook(webhook, event_type, event_data)

            # Update delivery record
            delivery.status = "success" if result["success"] else "failed"
            delivery.response_code = result["status_code"]
            delivery.response_body = result["response_body"]
            delivery.error_message = result["error"]
            delivery.delivered_at = datetime.utcnow()

            # Update webhook statistics
            if result["success"]:
                webhook.total_sent += 1
                webhook.last_sent_at = datetime.utcnow()
                webhook.last_error = None
                results["successful"] += 1
            else:
                webhook.total_failed += 1
                webhook.last_error = result["error"]
                results["failed"] += 1

            db.commit()

            results["deliveries"].append(
                {
                    "webhook_id": webhook.id,
                    "webhook_name": webhook.name,
                    "success": result["success"],
                    "status_code": result["status_code"],
                    "error": result["error"],
                }
            )

            logger.info(
                f"Webhook {webhook.name}: {event_type.value} - "
                f"{'success' if result['success'] else 'failed'}"
            )

    except Exception as e:
        logger.error(f"Error triggering webhooks: {e}", exc_info=True)
        results["error"] = str(e)

    finally:
        db.close()

    return results


# =============================================================================
# Convenience Functions for Common Events
# =============================================================================


async def notify_job_failed(
    job_id: int,
    source: str,
    error_message: str,
    config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Send notification for a failed job."""
    return await trigger_webhooks(
        event_type=WebhookEventType.JOB_FAILED,
        event_data={
            "job_id": job_id,
            "source": source,
            "error_message": error_message[:500],
            "config": config,
        },
        source=source,
    )


async def notify_job_success(
    job_id: int,
    source: str,
    rows_inserted: int,
    config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Send notification for a successful job."""
    return await trigger_webhooks(
        event_type=WebhookEventType.JOB_SUCCESS,
        event_data={
            "job_id": job_id,
            "source": source,
            "rows_inserted": rows_inserted,
            "config": config,
        },
        source=source,
    )


async def notify_alert(
    alert_type: str, source: str, message: str, details: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Send notification for a monitoring alert."""
    # Map alert type to event type
    event_type_map = {
        "high_failure_rate": WebhookEventType.ALERT_HIGH_FAILURE_RATE,
        "stuck_job": WebhookEventType.ALERT_STUCK_JOB,
        "data_staleness": WebhookEventType.ALERT_DATA_STALENESS,
    }

    event_type = event_type_map.get(
        alert_type, WebhookEventType.ALERT_HIGH_FAILURE_RATE
    )

    event_data = {
        "alert_type": alert_type,
        "source": source,
        "message": message,
        **(details or {}),
    }

    return await trigger_webhooks(
        event_type=event_type, event_data=event_data, source=source
    )


async def notify_cleanup_completed(
    cleaned_up: int, jobs: List[Dict[str, Any]], timeout_hours: int
) -> Dict[str, Any]:
    """Send notification when stuck job cleanup completes."""
    if cleaned_up == 0:
        return {"webhooks_triggered": 0}

    return await trigger_webhooks(
        event_type=WebhookEventType.CLEANUP_COMPLETED,
        event_data={
            "cleaned_up": cleaned_up,
            "timeout_hours": timeout_hours,
            "sources_affected": list(set(j["source"] for j in jobs)),
            "job_ids": [j["job_id"] for j in jobs],
        },
    )


# =============================================================================
# Webhook Management Functions
# =============================================================================


def create_webhook(
    db: Session,
    name: str,
    url: str,
    event_types: List[str],
    source_filter: Optional[str] = None,
    secret: Optional[str] = None,
    headers: Optional[Dict[str, str]] = None,
    is_active: bool = True,
) -> Webhook:
    """Create a new webhook configuration."""
    webhook = Webhook(
        name=name,
        url=url,
        event_types=event_types,
        source_filter=source_filter,
        secret=secret,
        headers=headers,
        is_active=1 if is_active else 0,
    )

    db.add(webhook)
    db.commit()
    db.refresh(webhook)

    logger.info(f"Created webhook: {name}")
    return webhook


def update_webhook(db: Session, webhook_id: int, **kwargs) -> Optional[Webhook]:
    """Update an existing webhook."""
    webhook = db.query(Webhook).filter(Webhook.id == webhook_id).first()

    if not webhook:
        return None

    for key, value in kwargs.items():
        if hasattr(webhook, key):
            if key == "is_active":
                value = 1 if value else 0
            setattr(webhook, key, value)

    db.commit()
    db.refresh(webhook)

    logger.info(f"Updated webhook: {webhook.name}")
    return webhook


def delete_webhook(db: Session, webhook_id: int) -> bool:
    """Delete a webhook configuration."""
    webhook = db.query(Webhook).filter(Webhook.id == webhook_id).first()

    if not webhook:
        return False

    db.delete(webhook)
    db.commit()

    logger.info(f"Deleted webhook: {webhook.name}")
    return True


def get_webhook_deliveries(
    db: Session, webhook_id: int, limit: int = 50
) -> List[WebhookDelivery]:
    """Get recent deliveries for a webhook."""
    return (
        db.query(WebhookDelivery)
        .filter(WebhookDelivery.webhook_id == webhook_id)
        .order_by(WebhookDelivery.created_at.desc())
        .limit(limit)
        .all()
    )


async def notify_site_intel_result(
    domain: str,
    source: str,
    job_id: int,
    status: str,
    inserted: int = 0,
    error_msg: Optional[str] = None,
) -> Dict[str, Any]:
    """Send notification for a site intel collection result."""
    event_type = (
        WebhookEventType.SITE_INTEL_SUCCESS
        if status == "success"
        else WebhookEventType.SITE_INTEL_FAILED
    )
    return await trigger_webhooks(
        event_type=event_type,
        event_data={
            "domain": domain,
            "source": source,
            "job_id": job_id,
            "status": status,
            "inserted_items": inserted,
            "error_message": error_msg[:500] if error_msg else None,
        },
        source=source,
    )


async def notify_consecutive_failures(
    source: str,
    count: int,
    domain: Optional[str] = None,
) -> Dict[str, Any]:
    """Send alert for consecutive collection failures."""
    return await trigger_webhooks(
        event_type=WebhookEventType.ALERT_CONSECUTIVE_FAILURES,
        event_data={
            "source": source,
            "domain": domain,
            "consecutive_failures": count,
            "message": f"Source '{source}' has {count} consecutive failures",
        },
        source=source,
    )


async def test_webhook(webhook: Webhook) -> Dict[str, Any]:
    """Send a test notification to a webhook."""
    test_data = {
        "test": True,
        "message": "This is a test notification from Nexdata",
        "timestamp": datetime.utcnow().isoformat(),
    }

    result = await send_webhook(webhook, WebhookEventType.JOB_SUCCESS, test_data)

    return {
        "webhook_id": webhook.id,
        "webhook_name": webhook.name,
        "test_successful": result["success"],
        "status_code": result["status_code"],
        "error": result["error"],
    }
