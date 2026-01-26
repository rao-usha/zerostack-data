# PLAN T11: Portfolio Change Alerts

## Overview
**Task:** T11
**Tab:** 1
**Feature:** Portfolio Change Alerts - Notify users when portfolio data changes
**Status:** PENDING_APPROVAL

---

## Goal
Build an alert system that detects and notifies users when portfolio data changes for watched investors. Changes include:
- New companies added to portfolio
- Companies removed from portfolio
- Significant value changes (configurable threshold)
- Source data updates

---

## Scope

### 1. Alert Engine (`app/notifications/alerts.py`)

Core change detection and alert management system.

**Classes:**
```python
class ChangeType(Enum):
    NEW_HOLDING = "new_holding"
    REMOVED_HOLDING = "removed_holding"
    VALUE_CHANGE = "value_change"
    SHARES_CHANGE = "shares_change"

class AlertStatus(Enum):
    PENDING = "pending"      # Not yet delivered
    DELIVERED = "delivered"  # Sent to user
    ACKNOWLEDGED = "acknowledged"  # User dismissed
    EXPIRED = "expired"      # Too old, auto-dismissed

class AlertSubscription:
    investor_id: int
    investor_type: str  # 'lp' or 'family_office'
    user_id: str        # Who subscribed (email or user ID)
    change_types: List[ChangeType]  # Which changes to alert on
    value_threshold_pct: float  # Min % change for value alerts (default 10%)
    delivery_channels: List[str]  # ['in_app', 'email', 'webhook']
    is_active: bool
    created_at: datetime

class PortfolioAlert:
    id: int
    subscription_id: int
    investor_id: int
    investor_type: str
    change_type: ChangeType
    company_name: str
    details: dict  # {old_value, new_value, change_pct, etc.}
    status: AlertStatus
    created_at: datetime
    delivered_at: datetime
    acknowledged_at: datetime
```

**Functions:**
```python
class AlertEngine:
    async def detect_changes(
        self,
        investor_id: int,
        investor_type: str,
        old_snapshot: List[dict],
        new_snapshot: List[dict]
    ) -> List[PortfolioChange]

    async def create_alerts_for_changes(
        self,
        investor_id: int,
        investor_type: str,
        changes: List[PortfolioChange]
    ) -> List[PortfolioAlert]

    async def get_pending_alerts(
        self,
        user_id: str,
        limit: int = 50
    ) -> List[PortfolioAlert]

    async def acknowledge_alert(
        self,
        alert_id: int,
        user_id: str
    ) -> bool
```

### 2. Database Tables

**alert_subscriptions:**
```sql
CREATE TABLE alert_subscriptions (
    id SERIAL PRIMARY KEY,
    investor_id INTEGER NOT NULL,
    investor_type VARCHAR(20) NOT NULL,
    user_id VARCHAR(255) NOT NULL,  -- email or user identifier
    change_types JSONB DEFAULT '["new_holding", "removed_holding"]',
    value_threshold_pct FLOAT DEFAULT 10.0,
    delivery_channels JSONB DEFAULT '["in_app"]',
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(investor_id, investor_type, user_id)
);
```

**portfolio_alerts:**
```sql
CREATE TABLE portfolio_alerts (
    id SERIAL PRIMARY KEY,
    subscription_id INTEGER REFERENCES alert_subscriptions(id),
    investor_id INTEGER NOT NULL,
    investor_type VARCHAR(20) NOT NULL,
    investor_name VARCHAR(255),
    change_type VARCHAR(50) NOT NULL,
    company_name VARCHAR(255) NOT NULL,
    details JSONB,
    status VARCHAR(20) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT NOW(),
    delivered_at TIMESTAMP,
    acknowledged_at TIMESTAMP
);
CREATE INDEX idx_alerts_user_status ON portfolio_alerts(subscription_id, status);
CREATE INDEX idx_alerts_investor ON portfolio_alerts(investor_id, investor_type);
```

**portfolio_snapshots:** (for change detection)
```sql
CREATE TABLE portfolio_snapshots (
    id SERIAL PRIMARY KEY,
    investor_id INTEGER NOT NULL,
    investor_type VARCHAR(20) NOT NULL,
    snapshot_date TIMESTAMP DEFAULT NOW(),
    snapshot_data JSONB NOT NULL,  -- Serialized portfolio state
    company_count INTEGER,
    total_value_usd NUMERIC,
    UNIQUE(investor_id, investor_type, snapshot_date)
);
```

### 3. API Endpoints (`app/api/v1/alerts.py`)

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/v1/alerts/subscribe` | Subscribe to investor alerts |
| GET | `/api/v1/alerts/subscriptions` | List user's subscriptions |
| DELETE | `/api/v1/alerts/subscriptions/{id}` | Unsubscribe |
| PATCH | `/api/v1/alerts/subscriptions/{id}` | Update subscription settings |
| GET | `/api/v1/alerts` | List pending alerts for user |
| POST | `/api/v1/alerts/{id}/acknowledge` | Mark alert as acknowledged |
| POST | `/api/v1/alerts/acknowledge-all` | Acknowledge all alerts |
| GET | `/api/v1/alerts/history` | Get alert history |

**Request/Response Models:**
```python
class AlertSubscriptionRequest(BaseModel):
    investor_id: int
    investor_type: str  # 'lp' or 'family_office'
    user_id: str  # Email address
    change_types: List[str] = ["new_holding", "removed_holding"]
    value_threshold_pct: float = 10.0

class AlertSubscriptionResponse(BaseModel):
    id: int
    investor_id: int
    investor_type: str
    investor_name: str
    user_id: str
    change_types: List[str]
    value_threshold_pct: float
    is_active: bool
    created_at: datetime

class AlertResponse(BaseModel):
    id: int
    investor_name: str
    change_type: str
    company_name: str
    summary: str  # Human-readable description
    details: dict
    status: str
    created_at: datetime
```

### 4. Integration Points

**After portfolio collection completes:**
```python
# In run_portfolio_collection() background task:
# 1. Take snapshot before collection
# 2. Run collection
# 3. Take snapshot after collection
# 4. Detect changes and create alerts
```

---

## Files to Create

| File | Description |
|------|-------------|
| `app/notifications/__init__.py` | Package init |
| `app/notifications/alerts.py` | Alert engine and change detection |
| `app/api/v1/alerts.py` | API endpoints |

## Files to Modify

| File | Change |
|------|--------|
| `app/main.py` | Register alerts router |
| `app/api/v1/agentic_research.py` | Add snapshot capture and alert creation after collection |

---

## Implementation Steps

1. Create `app/notifications/` directory
2. Implement `alerts.py` with AlertEngine class
3. Implement `app/api/v1/alerts.py` with endpoints
4. Add database migrations for new tables (via SQL in alerts.py init)
5. Register router in main.py
6. Integrate with portfolio collection to trigger change detection
7. Test endpoints

---

## Testing Plan

1. **Unit tests:**
   - Change detection logic (new/removed/value changes)
   - Threshold filtering

2. **Integration tests:**
   - Subscribe to investor
   - Trigger collection
   - Verify alerts created
   - Acknowledge alerts
   - Check history

3. **Manual testing:**
   ```bash
   # Subscribe to alerts
   curl -X POST http://localhost:8001/api/v1/alerts/subscribe \
     -H "Content-Type: application/json" \
     -d '{"investor_id": 1, "investor_type": "lp", "user_id": "test@example.com"}'

   # List subscriptions
   curl http://localhost:8001/api/v1/alerts/subscriptions?user_id=test@example.com

   # Get pending alerts
   curl http://localhost:8001/api/v1/alerts?user_id=test@example.com
   ```

---

## Dependencies

- No new packages required
- Uses existing SQLAlchemy and FastAPI patterns

---

## Notes

- Phase 1: In-app alerts only (stored in DB, retrieved via API)
- Webhook/email delivery channels stubbed but not implemented (T14, T15)
- Snapshots enable historical comparison and audit trail
- Alert expiration after 30 days (auto-cleanup)

---

## Approval

- [ ] Approved by user
