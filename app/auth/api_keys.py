"""
API Key Service and Rate Limiter for public API access.

T19: Public API with Auth & Rate Limits
"""

import hashlib
import secrets
from datetime import datetime, timedelta
from typing import Optional, Tuple, Dict, Any, List

from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.database import get_db


# ============================================================================
# Pydantic Models
# ============================================================================


class APIKeyCreate(BaseModel):
    """Request model for creating an API key."""

    name: str = Field(..., min_length=1, max_length=255, description="Key name")
    owner_email: str = Field(..., description="Owner's email address")
    scope: str = Field(
        "read", pattern="^(read|write|admin)$", description="Access scope"
    )
    rate_limit_per_minute: int = Field(
        60, ge=1, le=1000, description="Requests per minute"
    )
    rate_limit_per_day: int = Field(
        10000, ge=1, le=1000000, description="Requests per day"
    )
    expires_in_days: Optional[int] = Field(
        None, ge=1, le=365, description="Expiration in days"
    )


class APIKeyUpdate(BaseModel):
    """Request model for updating an API key."""

    name: Optional[str] = Field(None, min_length=1, max_length=255)
    rate_limit_per_minute: Optional[int] = Field(None, ge=1, le=1000)
    rate_limit_per_day: Optional[int] = Field(None, ge=1, le=1000000)
    is_active: Optional[bool] = None


class APIKeyResponse(BaseModel):
    """Response model for API key (without secret)."""

    id: int
    key_prefix: str
    name: str
    owner_email: str
    scope: str
    rate_limit_per_minute: int
    rate_limit_per_day: int
    is_active: bool
    expires_at: Optional[str]
    created_at: str
    last_used_at: Optional[str]


class APIKeyCreatedResponse(APIKeyResponse):
    """Response model for newly created API key (includes secret)."""

    key: str  # Full key, shown only once


class UsageStatsResponse(BaseModel):
    """Response model for usage statistics."""

    api_key_id: int
    total_requests: int
    requests_today: int
    requests_this_month: int
    daily_breakdown: List[Dict[str, Any]]
    by_endpoint: Dict[str, int]


class RateLimitInfo(BaseModel):
    """Rate limit information for headers."""

    limit: int
    remaining: int
    reset: int  # Unix timestamp
    limit_type: str  # 'minute' or 'day'


# ============================================================================
# API Key Service
# ============================================================================


class APIKeyService:
    """Service for API key management."""

    KEY_PREFIX = "nxd_"
    KEY_LENGTH = 32

    def __init__(self, db: Session):
        self.db = db
        self._ensure_tables()

    def _ensure_tables(self):
        """Create tables if they don't exist."""
        self.db.execute(
            text("""
            CREATE TABLE IF NOT EXISTS api_keys (
                id SERIAL PRIMARY KEY,
                key_hash VARCHAR(64) NOT NULL UNIQUE,
                key_prefix VARCHAR(12) NOT NULL,
                name VARCHAR(255) NOT NULL,
                owner_email VARCHAR(255) NOT NULL,
                scope VARCHAR(50) DEFAULT 'read',
                rate_limit_per_minute INTEGER DEFAULT 60,
                rate_limit_per_day INTEGER DEFAULT 10000,
                is_active BOOLEAN DEFAULT TRUE,
                expires_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT NOW(),
                last_used_at TIMESTAMP
            )
        """)
        )

        self.db.execute(
            text("""
            CREATE TABLE IF NOT EXISTS api_usage (
                id SERIAL PRIMARY KEY,
                api_key_id INTEGER NOT NULL REFERENCES api_keys(id),
                endpoint VARCHAR(255) NOT NULL,
                method VARCHAR(10) NOT NULL,
                status_code INTEGER,
                response_time_ms INTEGER,
                requested_at TIMESTAMP DEFAULT NOW()
            )
        """)
        )

        self.db.execute(
            text("""
            CREATE TABLE IF NOT EXISTS rate_limit_buckets (
                api_key_id INTEGER NOT NULL,
                bucket_type VARCHAR(20) NOT NULL,
                bucket_key VARCHAR(50) NOT NULL,
                request_count INTEGER DEFAULT 0,
                PRIMARY KEY (api_key_id, bucket_type, bucket_key)
            )
        """)
        )

        # Create indexes if they don't exist
        self.db.execute(
            text("""
            CREATE INDEX IF NOT EXISTS idx_api_keys_hash ON api_keys(key_hash)
        """)
        )
        self.db.execute(
            text("""
            CREATE INDEX IF NOT EXISTS idx_api_keys_owner ON api_keys(owner_email)
        """)
        )
        self.db.execute(
            text("""
            CREATE INDEX IF NOT EXISTS idx_api_usage_key ON api_usage(api_key_id)
        """)
        )
        self.db.execute(
            text("""
            CREATE INDEX IF NOT EXISTS idx_api_usage_date ON api_usage(requested_at)
        """)
        )

        self.db.commit()

    def _generate_key(self) -> str:
        """Generate a secure random API key."""
        random_part = secrets.token_urlsafe(self.KEY_LENGTH)
        return f"{self.KEY_PREFIX}{random_part}"

    def _hash_key(self, key: str) -> str:
        """Hash an API key using SHA-256."""
        return hashlib.sha256(key.encode()).hexdigest()

    def _get_key_prefix(self, key: str) -> str:
        """Get the prefix portion of a key for identification."""
        return key[:12] if len(key) >= 12 else key

    def create_key(self, data: APIKeyCreate) -> APIKeyCreatedResponse:
        """Create a new API key."""
        # Generate the key
        raw_key = self._generate_key()
        key_hash = self._hash_key(raw_key)
        key_prefix = self._get_key_prefix(raw_key)

        # Calculate expiration
        expires_at = None
        if data.expires_in_days:
            expires_at = datetime.utcnow() + timedelta(days=data.expires_in_days)

        # Insert into database
        result = self.db.execute(
            text("""
            INSERT INTO api_keys (
                key_hash, key_prefix, name, owner_email, scope,
                rate_limit_per_minute, rate_limit_per_day, expires_at
            ) VALUES (
                :key_hash, :key_prefix, :name, :owner_email, :scope,
                :rate_limit_per_minute, :rate_limit_per_day, :expires_at
            )
            RETURNING id, created_at
        """),
            {
                "key_hash": key_hash,
                "key_prefix": key_prefix,
                "name": data.name,
                "owner_email": data.owner_email,
                "scope": data.scope,
                "rate_limit_per_minute": data.rate_limit_per_minute,
                "rate_limit_per_day": data.rate_limit_per_day,
                "expires_at": expires_at,
            },
        )

        row = result.fetchone()
        self.db.commit()

        return APIKeyCreatedResponse(
            id=row[0],
            key=raw_key,  # Only time the full key is returned
            key_prefix=key_prefix,
            name=data.name,
            owner_email=data.owner_email,
            scope=data.scope,
            rate_limit_per_minute=data.rate_limit_per_minute,
            rate_limit_per_day=data.rate_limit_per_day,
            is_active=True,
            expires_at=expires_at.isoformat() if expires_at else None,
            created_at=row[1].isoformat(),
            last_used_at=None,
        )

    def list_keys(self, owner_email: Optional[str] = None) -> List[APIKeyResponse]:
        """List API keys, optionally filtered by owner."""
        if owner_email:
            result = self.db.execute(
                text("""
                SELECT id, key_prefix, name, owner_email, scope,
                       rate_limit_per_minute, rate_limit_per_day, is_active,
                       expires_at, created_at, last_used_at
                FROM api_keys
                WHERE owner_email = :owner_email
                ORDER BY created_at DESC
            """),
                {"owner_email": owner_email},
            )
        else:
            result = self.db.execute(
                text("""
                SELECT id, key_prefix, name, owner_email, scope,
                       rate_limit_per_minute, rate_limit_per_day, is_active,
                       expires_at, created_at, last_used_at
                FROM api_keys
                ORDER BY created_at DESC
            """)
            )

        keys = []
        for row in result:
            keys.append(
                APIKeyResponse(
                    id=row[0],
                    key_prefix=row[1],
                    name=row[2],
                    owner_email=row[3],
                    scope=row[4],
                    rate_limit_per_minute=row[5],
                    rate_limit_per_day=row[6],
                    is_active=row[7],
                    expires_at=row[8].isoformat() if row[8] else None,
                    created_at=row[9].isoformat(),
                    last_used_at=row[10].isoformat() if row[10] else None,
                )
            )

        return keys

    def get_key(self, key_id: int) -> Optional[APIKeyResponse]:
        """Get a single API key by ID."""
        result = self.db.execute(
            text("""
            SELECT id, key_prefix, name, owner_email, scope,
                   rate_limit_per_minute, rate_limit_per_day, is_active,
                   expires_at, created_at, last_used_at
            FROM api_keys
            WHERE id = :key_id
        """),
            {"key_id": key_id},
        )

        row = result.fetchone()
        if not row:
            return None

        return APIKeyResponse(
            id=row[0],
            key_prefix=row[1],
            name=row[2],
            owner_email=row[3],
            scope=row[4],
            rate_limit_per_minute=row[5],
            rate_limit_per_day=row[6],
            is_active=row[7],
            expires_at=row[8].isoformat() if row[8] else None,
            created_at=row[9].isoformat(),
            last_used_at=row[10].isoformat() if row[10] else None,
        )

    def update_key(self, key_id: int, data: APIKeyUpdate) -> Optional[APIKeyResponse]:
        """Update an API key."""
        # Build update query dynamically
        updates = []
        params = {"key_id": key_id}

        if data.name is not None:
            updates.append("name = :name")
            params["name"] = data.name
        if data.rate_limit_per_minute is not None:
            updates.append("rate_limit_per_minute = :rate_limit_per_minute")
            params["rate_limit_per_minute"] = data.rate_limit_per_minute
        if data.rate_limit_per_day is not None:
            updates.append("rate_limit_per_day = :rate_limit_per_day")
            params["rate_limit_per_day"] = data.rate_limit_per_day
        if data.is_active is not None:
            updates.append("is_active = :is_active")
            params["is_active"] = data.is_active

        if not updates:
            return self.get_key(key_id)

        query = f"UPDATE api_keys SET {', '.join(updates)} WHERE id = :key_id"
        self.db.execute(text(query), params)
        self.db.commit()

        return self.get_key(key_id)

    def revoke_key(self, key_id: int) -> bool:
        """Revoke an API key."""
        result = self.db.execute(
            text("""
            UPDATE api_keys SET is_active = FALSE WHERE id = :key_id
            RETURNING id
        """),
            {"key_id": key_id},
        )

        row = result.fetchone()
        self.db.commit()
        return row is not None

    def validate_key(self, raw_key: str) -> Optional[Dict[str, Any]]:
        """
        Validate an API key and return key info if valid.
        Returns None if invalid, revoked, or expired.
        """
        key_hash = self._hash_key(raw_key)

        result = self.db.execute(
            text("""
            SELECT id, name, owner_email, scope,
                   rate_limit_per_minute, rate_limit_per_day,
                   is_active, expires_at
            FROM api_keys
            WHERE key_hash = :key_hash
        """),
            {"key_hash": key_hash},
        )

        row = result.fetchone()
        if not row:
            return None

        key_info = {
            "id": row[0],
            "name": row[1],
            "owner_email": row[2],
            "scope": row[3],
            "rate_limit_per_minute": row[4],
            "rate_limit_per_day": row[5],
            "is_active": row[6],
            "expires_at": row[7],
        }

        # Check if active
        if not key_info["is_active"]:
            return None

        # Check expiration
        if key_info["expires_at"] and key_info["expires_at"] < datetime.utcnow():
            return None

        # Update last used timestamp
        self.db.execute(
            text("""
            UPDATE api_keys SET last_used_at = NOW() WHERE id = :key_id
        """),
            {"key_id": key_info["id"]},
        )
        self.db.commit()

        return key_info

    def get_usage(self, key_id: int) -> Optional[UsageStatsResponse]:
        """Get usage statistics for an API key."""
        # Verify key exists
        if not self.get_key(key_id):
            return None

        # Get total requests
        total_result = self.db.execute(
            text("""
            SELECT COUNT(*) FROM api_usage WHERE api_key_id = :key_id
        """),
            {"key_id": key_id},
        )
        total_requests = total_result.fetchone()[0]

        # Get today's requests
        today_result = self.db.execute(
            text("""
            SELECT COUNT(*) FROM api_usage
            WHERE api_key_id = :key_id
            AND requested_at >= CURRENT_DATE
        """),
            {"key_id": key_id},
        )
        requests_today = today_result.fetchone()[0]

        # Get this month's requests
        month_result = self.db.execute(
            text("""
            SELECT COUNT(*) FROM api_usage
            WHERE api_key_id = :key_id
            AND requested_at >= DATE_TRUNC('month', CURRENT_DATE)
        """),
            {"key_id": key_id},
        )
        requests_this_month = month_result.fetchone()[0]

        # Get daily breakdown (last 30 days)
        daily_result = self.db.execute(
            text("""
            SELECT DATE(requested_at) as date, COUNT(*) as count
            FROM api_usage
            WHERE api_key_id = :key_id
            AND requested_at >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY DATE(requested_at)
            ORDER BY date DESC
        """),
            {"key_id": key_id},
        )
        daily_breakdown = [
            {"date": str(row[0]), "count": row[1]} for row in daily_result
        ]

        # Get by endpoint
        endpoint_result = self.db.execute(
            text("""
            SELECT endpoint, COUNT(*) as count
            FROM api_usage
            WHERE api_key_id = :key_id
            GROUP BY endpoint
            ORDER BY count DESC
        """),
            {"key_id": key_id},
        )
        by_endpoint = {row[0]: row[1] for row in endpoint_result}

        return UsageStatsResponse(
            api_key_id=key_id,
            total_requests=total_requests,
            requests_today=requests_today,
            requests_this_month=requests_this_month,
            daily_breakdown=daily_breakdown,
            by_endpoint=by_endpoint,
        )

    def record_usage(
        self,
        key_id: int,
        endpoint: str,
        method: str,
        status_code: int,
        response_time_ms: int,
    ):
        """Record API usage for tracking."""
        self.db.execute(
            text("""
            INSERT INTO api_usage (api_key_id, endpoint, method, status_code, response_time_ms)
            VALUES (:key_id, :endpoint, :method, :status_code, :response_time_ms)
        """),
            {
                "key_id": key_id,
                "endpoint": endpoint,
                "method": method,
                "status_code": status_code,
                "response_time_ms": response_time_ms,
            },
        )
        self.db.commit()


# ============================================================================
# Rate Limiter
# ============================================================================


class RateLimiter:
    """Token bucket rate limiter with per-minute and per-day limits."""

    def __init__(self, db: Session):
        self.db = db

    def check_rate_limit(
        self, key_id: int, limits: Dict[str, int]
    ) -> Tuple[bool, RateLimitInfo]:
        """
        Check if a request is within rate limits.

        Args:
            key_id: API key ID
            limits: Dict with 'per_minute' and 'per_day' limits

        Returns:
            Tuple of (allowed: bool, rate_info: RateLimitInfo)
        """
        now = datetime.utcnow()
        minute_key = now.strftime("%Y-%m-%d-%H:%M")
        day_key = now.strftime("%Y-%m-%d")

        # Check minute limit
        minute_count = self._get_bucket_count(key_id, "minute", minute_key)
        if minute_count >= limits["per_minute"]:
            # Calculate reset time (next minute)
            reset_time = now.replace(second=0, microsecond=0) + timedelta(minutes=1)
            return False, RateLimitInfo(
                limit=limits["per_minute"],
                remaining=0,
                reset=int(reset_time.timestamp()),
                limit_type="minute",
            )

        # Check daily limit
        day_count = self._get_bucket_count(key_id, "day", day_key)
        if day_count >= limits["per_day"]:
            # Calculate reset time (next day)
            reset_time = (now + timedelta(days=1)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            return False, RateLimitInfo(
                limit=limits["per_day"],
                remaining=0,
                reset=int(reset_time.timestamp()),
                limit_type="day",
            )

        # Increment counters
        self._increment_bucket(key_id, "minute", minute_key)
        self._increment_bucket(key_id, "day", day_key)

        # Return info based on which limit is closer to being hit
        minute_remaining = limits["per_minute"] - minute_count - 1
        day_remaining = limits["per_day"] - day_count - 1

        if minute_remaining <= day_remaining:
            reset_time = now.replace(second=0, microsecond=0) + timedelta(minutes=1)
            return True, RateLimitInfo(
                limit=limits["per_minute"],
                remaining=minute_remaining,
                reset=int(reset_time.timestamp()),
                limit_type="minute",
            )
        else:
            reset_time = (now + timedelta(days=1)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            return True, RateLimitInfo(
                limit=limits["per_day"],
                remaining=day_remaining,
                reset=int(reset_time.timestamp()),
                limit_type="day",
            )

    def _get_bucket_count(self, key_id: int, bucket_type: str, bucket_key: str) -> int:
        """Get current count for a rate limit bucket."""
        result = self.db.execute(
            text("""
            SELECT request_count FROM rate_limit_buckets
            WHERE api_key_id = :key_id AND bucket_type = :bucket_type AND bucket_key = :bucket_key
        """),
            {"key_id": key_id, "bucket_type": bucket_type, "bucket_key": bucket_key},
        )

        row = result.fetchone()
        return row[0] if row else 0

    def _increment_bucket(self, key_id: int, bucket_type: str, bucket_key: str):
        """Increment a rate limit bucket counter."""
        self.db.execute(
            text("""
            INSERT INTO rate_limit_buckets (api_key_id, bucket_type, bucket_key, request_count)
            VALUES (:key_id, :bucket_type, :bucket_key, 1)
            ON CONFLICT (api_key_id, bucket_type, bucket_key)
            DO UPDATE SET request_count = rate_limit_buckets.request_count + 1
        """),
            {"key_id": key_id, "bucket_type": bucket_type, "bucket_key": bucket_key},
        )
        self.db.commit()


# ============================================================================
# Dependency Injection
# ============================================================================


def get_api_key_service(db: Session = next(get_db())) -> APIKeyService:
    """Dependency for getting API key service."""
    return APIKeyService(db)
