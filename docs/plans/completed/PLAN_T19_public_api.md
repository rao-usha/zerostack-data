# PLAN T19: Public API with Auth & Rate Limits

## Overview
**Task:** T19
**Tab:** 2
**Feature:** Public API with API key authentication and rate limiting for external developers
**Status:** COMPLETE
**Dependency:** None

---

## Business Context

### The Problem

The Nexdata platform has powerful data APIs, but they're not secure for external use:

1. **No Authentication**: Anyone can call the API endpoints without credentials, making it impossible to track usage or restrict access.

2. **No Rate Limiting**: A single user (or bot) can overwhelm the system with requests, degrading performance for everyone.

3. **No Usage Tracking**: We can't see who's using the API, how much, or bill for usage.

4. **No Access Control**: All users have the same access - we can't offer tiered plans or restrict certain endpoints.

### Real-World User Scenarios

#### Scenario 1: Data Partner Integration
**DataCorp** wants to integrate Nexdata's investor data into their CRM. They need:
- A dedicated API key for their integration
- Rate limits that match their subscription tier
- Usage tracking for billing purposes

**Pain Point**: No way to give them secure, tracked access.

**With Public API**: They get an API key with 10,000 requests/day, usage is tracked, and we can bill them monthly.

#### Scenario 2: Developer Building an App
**Alex** is building a portfolio analytics app. He needs:
- A free tier API key for development
- Clear documentation on authentication
- Reasonable rate limits for testing

**Pain Point**: Can't safely expose the API for third-party development.

**With Public API**: Alex registers, gets a free-tier key (1,000 requests/day), and builds his app.

#### Scenario 3: Enterprise Customer
**MegaBank** wants bulk data access for their research team. They need:
- High rate limits (100,000 requests/day)
- Multiple API keys for different teams
- Usage analytics and audit logs

**Pain Point**: No enterprise-grade access controls.

**With Public API**: MegaBank gets multiple keys, each with customizable limits and full usage tracking.

### Business Value

| Value | Description | Metric |
|-------|-------------|--------|
| **Revenue** | Enable paid API access | New revenue stream |
| **Security** | Prevent unauthorized access | 100% authenticated traffic |
| **Scalability** | Protect system from abuse | Rate limit enforcement |
| **Analytics** | Track API usage patterns | Per-key usage metrics |
| **Partnerships** | Enable data partnerships | Partner integrations |

### Integration with Existing Features

| Feature | Integration |
|---------|-------------|
| **All v1 endpoints** | Protected by API key auth |
| **T12: Search** | Available via public API |
| **T18: Recommendations** | Premium endpoint option |
| **T13: Analytics** | Usage analytics integration |

---

## Success Criteria

### Must Have (Launch Blockers)

| ID | Criteria | Metric | Target | Verification Method |
|----|----------|--------|--------|---------------------|
| M1 | **Generate API key** | Key created | Returns secure key | `POST /api-keys` returns key |
| M2 | **Authenticate requests** | Key validated | Valid keys accepted | Request with `X-API-Key` header works |
| M3 | **Reject invalid keys** | Auth error | 401 Unauthorized | Invalid key returns 401 |
| M4 | **Rate limit enforcement** | Requests limited | Limits enforced | 429 after exceeding limit |
| M5 | **Track usage** | Requests counted | Per-key counts | `GET /api-keys/{id}/usage` shows counts |
| M6 | **List user's keys** | Keys returned | All keys listed | `GET /api-keys` returns array |
| M7 | **Revoke key** | Key disabled | Key stops working | `DELETE /api-keys/{id}` disables key |
| M8 | **Rate limit headers** | Headers present | X-RateLimit-* headers | Response includes limit headers |

### Should Have (Quality Bar)

| ID | Criteria | Metric | Target | Verification Method |
|----|----------|--------|--------|---------------------|
| S1 | **Key scopes** | Permissions | Read-only vs full access | Scope enforced |
| S2 | **Key naming** | Descriptive names | User-defined names | Name stored and returned |
| S3 | **Usage by endpoint** | Granular tracking | Per-endpoint counts | Breakdown in usage stats |
| S4 | **Key expiration** | Optional expiry | Configurable | Expired keys rejected |
| S5 | **Daily/monthly limits** | Quota periods | Configurable | Period-based enforcement |
| S6 | **Remaining quota** | Real-time info | In response headers | X-RateLimit-Remaining header |

### Nice to Have (Future)

| ID | Criteria | Description |
|----|----------|-------------|
| N1 | **Webhook on quota** | Alert when approaching limit |
| N2 | **IP allowlist** | Restrict key to specific IPs |
| N3 | **Key rotation** | Generate new key, grace period |
| N4 | **Usage alerts** | Email when quota low |
| N5 | **Billing integration** | Usage-based billing |

---

## User Stories & Acceptance Criteria

### Story 1: Generate and Manage API Keys

**As a** developer, **I want to** create API keys **so that** I can authenticate my API requests.

**Acceptance Criteria:**

| AC# | Scenario | Given | When | Then |
|-----|----------|-------|------|------|
| 1.1 | Create key | User authenticated | POST `/api-keys` with `{name, owner_email}` | Returns new key (shown once) |
| 1.2 | Key format | Key generated | Response received | Key is 32+ char alphanumeric |
| 1.3 | Key stored hashed | Key created | Check database | Only hash stored, not plaintext |
| 1.4 | List keys | User has 3 keys | GET `/api-keys?owner=email` | Returns 3 keys (no secrets) |
| 1.5 | Get key details | Key exists | GET `/api-keys/{id}` | Returns metadata (not secret) |
| 1.6 | Update key name | Key exists | PATCH `/api-keys/{id}` | Name updated |
| 1.7 | Revoke key | Key exists | DELETE `/api-keys/{id}` | Key marked revoked, stops working |
| 1.8 | Revoked key rejected | Key revoked | Request with revoked key | Returns 401 |

**Test Commands:**
```bash
# AC 1.1: Create API key
curl -X POST "http://localhost:8001/api/v1/api-keys" \
  -H "Content-Type: application/json" \
  -d '{"name": "My Test Key", "owner_email": "dev@example.com"}'
# Expected: 201, {"id": 1, "key": "nxd_...", "name": "My Test Key", ...}

# AC 1.4: List keys
curl "http://localhost:8001/api/v1/api-keys?owner_email=dev@example.com"
# Expected: 200, array of keys (without secrets)

# AC 1.7: Revoke key
curl -X DELETE "http://localhost:8001/api/v1/api-keys/1"
# Expected: 204
```

---

### Story 2: Authenticate API Requests

**As a** developer, **I want to** authenticate requests with my API key **so that** I can access protected endpoints.

**Acceptance Criteria:**

| AC# | Scenario | Given | When | Then |
|-----|----------|-------|------|------|
| 2.1 | Valid key accepted | Valid API key | Request with `X-API-Key: nxd_...` header | Request succeeds |
| 2.2 | Missing key rejected | No key provided | Request without header | 401 "API key required" |
| 2.3 | Invalid key rejected | Bad key | Request with invalid key | 401 "Invalid API key" |
| 2.4 | Revoked key rejected | Revoked key | Request with revoked key | 401 "API key revoked" |
| 2.5 | Expired key rejected | Expired key | Request with expired key | 401 "API key expired" |
| 2.6 | Key in query param | Key in URL | `?api_key=nxd_...` | Request succeeds (fallback) |

**Test Commands:**
```bash
# AC 2.1: Valid key
curl -H "X-API-Key: nxd_abc123..." "http://localhost:8001/api/v1/public/investors"
# Expected: 200, data returned

# AC 2.2: Missing key
curl "http://localhost:8001/api/v1/public/investors"
# Expected: 401, {"detail": "API key required"}

# AC 2.3: Invalid key
curl -H "X-API-Key: invalid" "http://localhost:8001/api/v1/public/investors"
# Expected: 401, {"detail": "Invalid API key"}
```

---

### Story 3: Rate Limiting

**As an** API provider, **I want to** limit request rates **so that** the system stays stable and fair.

**Acceptance Criteria:**

| AC# | Scenario | Given | When | Then |
|-----|----------|-------|------|------|
| 3.1 | Under limit allowed | Key with 100/min limit | 50 requests in 1 min | All succeed |
| 3.2 | Over limit blocked | Key with 100/min limit | 101st request | 429 "Rate limit exceeded" |
| 3.3 | Limit headers present | Any request | Check response headers | X-RateLimit-Limit, X-RateLimit-Remaining, X-RateLimit-Reset |
| 3.4 | Limit resets | After limit period | Wait for reset | Requests allowed again |
| 3.5 | Different limits per key | Keys with different tiers | Exceed lower tier | Higher tier still works |
| 3.6 | Retry-After header | Rate limited | 429 response | Includes Retry-After header |

**Test Commands:**
```bash
# AC 3.1 & 3.2: Rate limiting
for i in {1..110}; do
  curl -s -o /dev/null -w "%{http_code}\n" \
    -H "X-API-Key: nxd_..." "http://localhost:8001/api/v1/public/investors"
done | sort | uniq -c
# Expected: 100x "200", 10x "429"

# AC 3.3: Check headers
curl -I -H "X-API-Key: nxd_..." "http://localhost:8001/api/v1/public/investors"
# Expected: X-RateLimit-Limit, X-RateLimit-Remaining, X-RateLimit-Reset headers
```

---

### Story 4: Usage Tracking

**As an** API provider, **I want to** track usage per key **so that** I can monitor and bill customers.

**Acceptance Criteria:**

| AC# | Scenario | Given | When | Then |
|-----|----------|-------|------|------|
| 4.1 | Request counted | Valid request | Check usage | Count incremented |
| 4.2 | Usage endpoint | Key has usage | GET `/api-keys/{id}/usage` | Returns usage stats |
| 4.3 | Daily breakdown | Key used over days | Get usage | Shows per-day counts |
| 4.4 | Monthly totals | Key used over month | Get usage | Shows monthly total |
| 4.5 | Endpoint breakdown | Requests to different endpoints | Get usage | Shows per-endpoint counts |

**Test Commands:**
```bash
# AC 4.2: Get usage
curl "http://localhost:8001/api/v1/api-keys/1/usage"
# Expected: {"total_requests": 150, "daily": [...], "by_endpoint": {...}}
```

---

## Technical Scope

### Database Schema

```sql
-- API keys table
CREATE TABLE api_keys (
    id SERIAL PRIMARY KEY,
    key_hash VARCHAR(64) NOT NULL UNIQUE,  -- SHA-256 hash of key
    key_prefix VARCHAR(8) NOT NULL,  -- First 8 chars for identification (nxd_xxxx)
    name VARCHAR(255) NOT NULL,
    owner_email VARCHAR(255) NOT NULL,
    scope VARCHAR(50) DEFAULT 'read',  -- 'read', 'write', 'admin'
    rate_limit_per_minute INTEGER DEFAULT 60,
    rate_limit_per_day INTEGER DEFAULT 10000,
    is_active BOOLEAN DEFAULT TRUE,
    expires_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW(),
    last_used_at TIMESTAMP
);

CREATE INDEX idx_api_keys_hash ON api_keys(key_hash);
CREATE INDEX idx_api_keys_owner ON api_keys(owner_email);
CREATE INDEX idx_api_keys_prefix ON api_keys(key_prefix);

-- API usage tracking table
CREATE TABLE api_usage (
    id SERIAL PRIMARY KEY,
    api_key_id INTEGER NOT NULL REFERENCES api_keys(id),
    endpoint VARCHAR(255) NOT NULL,
    method VARCHAR(10) NOT NULL,
    status_code INTEGER,
    response_time_ms INTEGER,
    requested_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_api_usage_key ON api_usage(api_key_id);
CREATE INDEX idx_api_usage_date ON api_usage(requested_at);
CREATE INDEX idx_api_usage_key_date ON api_usage(api_key_id, requested_at);

-- Rate limiting cache (in-memory, but can be backed by table)
CREATE TABLE rate_limit_buckets (
    api_key_id INTEGER NOT NULL,
    bucket_type VARCHAR(20) NOT NULL,  -- 'minute', 'day'
    bucket_key VARCHAR(50) NOT NULL,  -- e.g., '2026-01-16-14:30' or '2026-01-16'
    request_count INTEGER DEFAULT 0,
    PRIMARY KEY (api_key_id, bucket_type, bucket_key)
);
```

### API Endpoints

| Method | Endpoint | Description | Auth Required |
|--------|----------|-------------|---------------|
| POST | `/api/v1/api-keys` | Create new API key | No (self-service) |
| GET | `/api/v1/api-keys` | List keys for owner | No (filter by email) |
| GET | `/api/v1/api-keys/{id}` | Get key details | No |
| PATCH | `/api/v1/api-keys/{id}` | Update key (name, limits) | No |
| DELETE | `/api/v1/api-keys/{id}` | Revoke key | No |
| GET | `/api/v1/api-keys/{id}/usage` | Get usage stats | No |
| GET | `/api/v1/public/investors` | List investors (protected) | API Key |
| GET | `/api/v1/public/investors/{id}` | Get investor details | API Key |
| GET | `/api/v1/public/search` | Search (protected) | API Key |

### Data Models (Pydantic)

```python
# Request Models
class APIKeyCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    owner_email: str = Field(..., description="Owner's email")
    scope: str = Field("read", pattern="^(read|write|admin)$")
    rate_limit_per_minute: int = Field(60, ge=1, le=1000)
    rate_limit_per_day: int = Field(10000, ge=1, le=1000000)
    expires_in_days: Optional[int] = Field(None, ge=1, le=365)

class APIKeyUpdate(BaseModel):
    name: Optional[str] = None
    rate_limit_per_minute: Optional[int] = None
    rate_limit_per_day: Optional[int] = None
    is_active: Optional[bool] = None

# Response Models
class APIKeyResponse(BaseModel):
    id: int
    key_prefix: str  # nxd_xxxx (for identification)
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
    key: str  # Full key, shown only once at creation

class UsageStatsResponse(BaseModel):
    api_key_id: int
    total_requests: int
    requests_today: int
    requests_this_month: int
    daily_breakdown: List[dict]  # [{date, count}, ...]
    by_endpoint: dict  # {endpoint: count, ...}
```

### Rate Limiting Implementation

```python
# Token bucket algorithm (simplified)
class RateLimiter:
    def __init__(self, db: Session):
        self.db = db
        self._cache = {}  # In-memory cache for performance

    def check_rate_limit(self, api_key_id: int, limits: dict) -> tuple[bool, dict]:
        """
        Check if request is within rate limits.
        Returns (allowed, headers_dict)
        """
        now = datetime.utcnow()
        minute_key = now.strftime("%Y-%m-%d-%H:%M")
        day_key = now.strftime("%Y-%m-%d")

        # Check minute limit
        minute_count = self._get_bucket_count(api_key_id, "minute", minute_key)
        if minute_count >= limits["per_minute"]:
            return False, self._build_headers(limits, minute_count, "minute")

        # Check daily limit
        day_count = self._get_bucket_count(api_key_id, "day", day_key)
        if day_count >= limits["per_day"]:
            return False, self._build_headers(limits, day_count, "day")

        # Increment counters
        self._increment_bucket(api_key_id, "minute", minute_key)
        self._increment_bucket(api_key_id, "day", day_key)

        return True, self._build_headers(limits, minute_count + 1, "minute")
```

---

## Files to Create

| File | Description |
|------|-------------|
| `app/auth/__init__.py` | Package init |
| `app/auth/api_keys.py` | API key service and rate limiter |
| `app/auth/middleware.py` | Authentication middleware |
| `app/api/v1/api_keys.py` | API key management endpoints |
| `app/api/v1/public.py` | Protected public API endpoints |

## Files to Modify

| File | Change |
|------|--------|
| `app/main.py` | Register api_keys and public routers |

---

## Implementation Steps

1. Create `app/auth/` directory with `__init__.py`
2. Implement `api_keys.py`:
   - Database schema setup
   - APIKeyService class (CRUD operations)
   - Key generation (secure random)
   - Key hashing (SHA-256)
   - RateLimiter class
3. Implement `middleware.py`:
   - API key authentication middleware
   - Rate limit checking
   - Usage tracking
4. Implement `app/api/v1/api_keys.py`:
   - Key management endpoints
   - Usage stats endpoint
5. Implement `app/api/v1/public.py`:
   - Protected endpoints with auth dependency
   - Standard response envelope
6. Register routers in main.py
7. Test all endpoints

---

## Testing Plan

### Test Suite

#### Suite 1: API Key Management

| Test ID | Test Name | Steps | Expected Result | Pass/Fail |
|---------|-----------|-------|-----------------|-----------|
| AK-001 | Create key | POST with valid data | 201, returns key | |
| AK-002 | Create without name | POST without name | 422 validation error | |
| AK-003 | List keys | GET with owner_email | 200, array of keys | |
| AK-004 | Get key | GET by ID | 200, key details (no secret) | |
| AK-005 | Update key | PATCH with new name | 200, name updated | |
| AK-006 | Revoke key | DELETE by ID | 204, key revoked | |
| AK-007 | Get revoked key | GET revoked key | 200, is_active=false | |
| AK-008 | Key not found | GET invalid ID | 404 | |

#### Suite 2: Authentication

| Test ID | Test Name | Steps | Expected Result | Pass/Fail |
|---------|-----------|-------|-----------------|-----------|
| AU-001 | Valid key accepted | Request with valid key | 200, data returned | |
| AU-002 | Missing key rejected | Request without key | 401 | |
| AU-003 | Invalid key rejected | Request with bad key | 401 | |
| AU-004 | Revoked key rejected | Request with revoked key | 401 | |
| AU-005 | Key in query param | `?api_key=...` | 200 (fallback works) | |

#### Suite 3: Rate Limiting

| Test ID | Test Name | Steps | Expected Result | Pass/Fail |
|---------|-----------|-------|-----------------|-----------|
| RL-001 | Under limit | Requests under limit | All 200 | |
| RL-002 | Over limit | Exceed rate limit | 429 after limit | |
| RL-003 | Headers present | Check response | X-RateLimit-* headers | |
| RL-004 | Retry-After | 429 response | Retry-After header | |

#### Suite 4: Usage Tracking

| Test ID | Test Name | Steps | Expected Result | Pass/Fail |
|---------|-----------|-------|-----------------|-----------|
| US-001 | Usage tracked | Make requests, get usage | Count matches | |
| US-002 | Daily breakdown | Get usage stats | Daily counts shown | |

---

## Integration Test Script

```bash
#!/bin/bash
# T19 Integration Test Suite

BASE_URL="http://localhost:8001/api/v1"
PASS=0
FAIL=0

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

log_pass() { echo -e "${GREEN}PASS${NC}: $1"; ((PASS++)); }
log_fail() { echo -e "${RED}FAIL${NC}: $1 - $2"; ((FAIL++)); }

echo "=========================================="
echo "T19: Public API with Auth & Rate Limits"
echo "=========================================="

# Test 1: Create API key
echo -e "\n--- Test 1: Create API Key ---"
RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "$BASE_URL/api-keys" \
  -H "Content-Type: application/json" \
  -d '{"name": "Test Key", "owner_email": "test@example.com", "rate_limit_per_minute": 10}')
HTTP_CODE=$(echo "$RESPONSE" | tail -1)
BODY=$(echo "$RESPONSE" | head -n -1)
API_KEY=$(echo "$BODY" | python -c "import sys,json; print(json.load(sys.stdin).get('key',''))" 2>/dev/null)
KEY_ID=$(echo "$BODY" | python -c "import sys,json; print(json.load(sys.stdin).get('id',''))" 2>/dev/null)

if [ "$HTTP_CODE" == "201" ] && [ -n "$API_KEY" ]; then
  log_pass "Create API key (ID: $KEY_ID)"
  echo "  Key: ${API_KEY:0:20}..."
else
  log_fail "Create API key" "HTTP $HTTP_CODE"
fi

# Test 2: Valid key accepted
echo -e "\n--- Test 2: Valid Key Authentication ---"
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
  -H "X-API-Key: $API_KEY" "$BASE_URL/public/investors")
if [ "$HTTP_CODE" == "200" ]; then
  log_pass "Valid key accepted"
else
  log_fail "Valid key authentication" "HTTP $HTTP_CODE"
fi

# Test 3: Missing key rejected
echo -e "\n--- Test 3: Missing Key Rejected ---"
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$BASE_URL/public/investors")
if [ "$HTTP_CODE" == "401" ]; then
  log_pass "Missing key rejected"
else
  log_fail "Missing key check" "Expected 401, got $HTTP_CODE"
fi

# Test 4: Invalid key rejected
echo -e "\n--- Test 4: Invalid Key Rejected ---"
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
  -H "X-API-Key: invalid_key" "$BASE_URL/public/investors")
if [ "$HTTP_CODE" == "401" ]; then
  log_pass "Invalid key rejected"
else
  log_fail "Invalid key check" "Expected 401, got $HTTP_CODE"
fi

# Test 5: Rate limit headers
echo -e "\n--- Test 5: Rate Limit Headers ---"
HEADERS=$(curl -s -I -H "X-API-Key: $API_KEY" "$BASE_URL/public/investors")
if echo "$HEADERS" | grep -q "X-RateLimit-Limit"; then
  log_pass "Rate limit headers present"
else
  log_fail "Rate limit headers" "X-RateLimit-Limit not found"
fi

# Test 6: Rate limiting (exceed limit)
echo -e "\n--- Test 6: Rate Limiting ---"
SUCCESS=0
LIMITED=0
for i in {1..15}; do
  CODE=$(curl -s -o /dev/null -w "%{http_code}" \
    -H "X-API-Key: $API_KEY" "$BASE_URL/public/investors")
  if [ "$CODE" == "200" ]; then ((SUCCESS++)); fi
  if [ "$CODE" == "429" ]; then ((LIMITED++)); fi
done
if [ "$LIMITED" -gt "0" ]; then
  log_pass "Rate limiting works ($SUCCESS ok, $LIMITED limited)"
else
  log_fail "Rate limiting" "No 429 responses"
fi

# Test 7: Get usage stats
echo -e "\n--- Test 7: Usage Stats ---"
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$BASE_URL/api-keys/$KEY_ID/usage")
if [ "$HTTP_CODE" == "200" ]; then
  log_pass "Usage stats endpoint"
else
  log_fail "Usage stats" "HTTP $HTTP_CODE"
fi

# Test 8: Revoke key
echo -e "\n--- Test 8: Revoke Key ---"
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X DELETE "$BASE_URL/api-keys/$KEY_ID")
if [ "$HTTP_CODE" == "204" ]; then
  log_pass "Key revoked"
else
  log_fail "Revoke key" "HTTP $HTTP_CODE"
fi

# Test 9: Revoked key rejected
echo -e "\n--- Test 9: Revoked Key Rejected ---"
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
  -H "X-API-Key: $API_KEY" "$BASE_URL/public/investors")
if [ "$HTTP_CODE" == "401" ]; then
  log_pass "Revoked key rejected"
else
  log_fail "Revoked key check" "Expected 401, got $HTTP_CODE"
fi

# Summary
echo -e "\n=========================================="
echo "Test Summary: $PASS passed, $FAIL failed"
echo "=========================================="

if [ $FAIL -eq 0 ]; then
  echo -e "${GREEN}All tests passed!${NC}"
  exit 0
else
  echo -e "${RED}Some tests failed!${NC}"
  exit 1
fi
```

---

## Risks & Mitigations

| Risk | Impact | Likelihood | Mitigation |
|------|--------|------------|------------|
| Key leakage | Security breach | Medium | Hash keys, show once, encourage rotation |
| Rate limit bypass | System overload | Low | Server-side enforcement, no client trust |
| DB bottleneck on usage | Performance | Medium | Batch inserts, async logging |
| Clock skew | Incorrect limits | Low | Use server time only |

---

## Out of Scope (Future Work)

- OAuth2/JWT authentication
- IP-based allowlisting
- Webhook notifications for quota
- Automatic key rotation
- Usage-based billing integration
- Admin dashboard for key management

---

## Approval

- [x] Approved by user
