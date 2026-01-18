# Plan T30: User Auth & Workspaces

## Overview
**Task:** T30
**Tab:** 2
**Feature:** Add user authentication and team workspaces for collaboration
**Status:** COMPLETE
**Dependency:** T19 (API keys) - COMPLETE

---

## Business Context

### The Problem

Nexdata needs user management for:

1. **Authentication**: Secure login/registration for platform access
2. **Team Collaboration**: Shared workspaces for investment teams
3. **Data Isolation**: Workspace-scoped data visibility
4. **Role-Based Access**: Admin, member, viewer permissions
5. **Integration**: Link users to existing features (watchlists, deals, etc.)

### User Scenarios

#### Scenario 1: User Registration
**New User** joins the platform.
- Action: Register with email and password
- Result: Account created, JWT token returned for access

#### Scenario 2: Team Setup
**Team Lead** creates a workspace for their team.
- Action: Create workspace, invite team members
- Result: Members receive invitations, join with assigned roles

#### Scenario 3: Collaborative Work
**Team Members** work together on deals.
- Query: "Show me all deals in our workspace"
- Result: Workspace-scoped data visible to all members

---

## Success Criteria

### Must Have

| ID | Criteria | Verification |
|----|----------|--------------|
| M1 | User registration | Create account with email/password |
| M2 | User login | JWT token returned on valid credentials |
| M3 | Password security | Bcrypt hashing with salt |
| M4 | Workspace CRUD | Create, read, update workspaces |
| M5 | Member management | Invite, list, remove members |
| M6 | Role enforcement | Admin, member, viewer roles |

### Should Have

| ID | Criteria | Verification |
|----|----------|--------------|
| S1 | Token refresh | Refresh expired tokens |
| S2 | Password reset | Reset flow with token |
| S3 | Profile management | Update user details |

---

## Technical Design

### Database Schema

```sql
-- Users table
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    name VARCHAR(255),
    is_active BOOLEAN DEFAULT TRUE,
    is_verified BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_login_at TIMESTAMP
);

-- Workspaces table
CREATE TABLE IF NOT EXISTS workspaces (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    owner_id INTEGER REFERENCES users(id),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Workspace members table
CREATE TABLE IF NOT EXISTS workspace_members (
    id SERIAL PRIMARY KEY,
    workspace_id INTEGER REFERENCES workspaces(id) ON DELETE CASCADE,
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    role VARCHAR(20) DEFAULT 'member',  -- admin, member, viewer
    invited_by INTEGER REFERENCES users(id),
    joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(workspace_id, user_id)
);

-- Workspace invitations table
CREATE TABLE IF NOT EXISTS workspace_invitations (
    id SERIAL PRIMARY KEY,
    workspace_id INTEGER REFERENCES workspaces(id) ON DELETE CASCADE,
    email VARCHAR(255) NOT NULL,
    role VARCHAR(20) DEFAULT 'member',
    token VARCHAR(64) NOT NULL UNIQUE,
    invited_by INTEGER REFERENCES users(id),
    expires_at TIMESTAMP NOT NULL,
    accepted_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Password reset tokens
CREATE TABLE IF NOT EXISTS password_reset_tokens (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    token VARCHAR(64) NOT NULL UNIQUE,
    expires_at TIMESTAMP NOT NULL,
    used_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_workspace_members_user ON workspace_members(user_id);
CREATE INDEX idx_workspace_members_workspace ON workspace_members(workspace_id);
CREATE INDEX idx_invitations_token ON workspace_invitations(token);
CREATE INDEX idx_invitations_email ON workspace_invitations(email);
```

### User Roles

| Role | Permissions |
|------|-------------|
| `admin` | Full workspace access, manage members, delete workspace |
| `member` | Create/edit content, view all workspace data |
| `viewer` | Read-only access to workspace data |

### API Endpoints

#### Authentication (8 endpoints)

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/v1/auth/register` | Register new user |
| POST | `/api/v1/auth/login` | Login, get JWT |
| POST | `/api/v1/auth/logout` | Invalidate token |
| POST | `/api/v1/auth/refresh` | Refresh JWT token |
| GET | `/api/v1/auth/me` | Get current user |
| PATCH | `/api/v1/auth/me` | Update profile |
| POST | `/api/v1/auth/password/reset-request` | Request reset |
| POST | `/api/v1/auth/password/reset` | Reset with token |

#### Workspaces (9 endpoints)

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/v1/workspaces` | Create workspace |
| GET | `/api/v1/workspaces` | List user's workspaces |
| GET | `/api/v1/workspaces/{id}` | Get workspace |
| PATCH | `/api/v1/workspaces/{id}` | Update workspace |
| DELETE | `/api/v1/workspaces/{id}` | Delete workspace |
| GET | `/api/v1/workspaces/{id}/members` | List members |
| POST | `/api/v1/workspaces/{id}/invite` | Invite member |
| DELETE | `/api/v1/workspaces/{id}/members/{user_id}` | Remove member |
| POST | `/api/v1/workspaces/invitations/{token}/accept` | Accept invite |

### Request/Response Models

**Register Request:**
```json
{
  "email": "user@example.com",
  "password": "securepassword123",
  "name": "John Doe"
}
```

**Login Response:**
```json
{
  "access_token": "eyJhbGc...",
  "token_type": "bearer",
  "expires_in": 3600,
  "user": {
    "id": 1,
    "email": "user@example.com",
    "name": "John Doe"
  }
}
```

**Workspace Response:**
```json
{
  "id": 1,
  "name": "Investment Team",
  "description": "Our main workspace",
  "owner_id": 1,
  "member_count": 5,
  "your_role": "admin",
  "created_at": "2026-01-16T12:00:00Z"
}
```

### Auth Service

```python
class AuthService:
    """User authentication service."""

    def __init__(self, db: Session):
        self.db = db

    def register(self, email: str, password: str, name: str) -> dict:
        """Register a new user."""

    def login(self, email: str, password: str) -> dict:
        """Authenticate and return JWT token."""

    def verify_token(self, token: str) -> dict:
        """Verify JWT and return user info."""

    def refresh_token(self, token: str) -> dict:
        """Refresh an expired token."""

    def get_user(self, user_id: int) -> dict:
        """Get user by ID."""

    def update_user(self, user_id: int, updates: dict) -> dict:
        """Update user profile."""

    def request_password_reset(self, email: str) -> bool:
        """Create password reset token."""

    def reset_password(self, token: str, new_password: str) -> bool:
        """Reset password using token."""
```

### Workspace Service

```python
class WorkspaceService:
    """Workspace management service."""

    def __init__(self, db: Session):
        self.db = db

    def create_workspace(self, owner_id: int, name: str, description: str) -> dict:
        """Create a new workspace."""

    def get_workspace(self, workspace_id: int, user_id: int) -> dict:
        """Get workspace if user has access."""

    def list_workspaces(self, user_id: int) -> list:
        """List workspaces user belongs to."""

    def update_workspace(self, workspace_id: int, user_id: int, updates: dict) -> dict:
        """Update workspace (admin only)."""

    def delete_workspace(self, workspace_id: int, user_id: int) -> bool:
        """Delete workspace (owner only)."""

    def invite_member(self, workspace_id: int, inviter_id: int, email: str, role: str) -> dict:
        """Invite user to workspace."""

    def accept_invitation(self, token: str, user_id: int) -> dict:
        """Accept workspace invitation."""

    def remove_member(self, workspace_id: int, admin_id: int, user_id: int) -> bool:
        """Remove member from workspace."""

    def get_members(self, workspace_id: int, user_id: int) -> list:
        """List workspace members."""

    def check_permission(self, workspace_id: int, user_id: int, required_role: str) -> bool:
        """Check if user has required role."""
```

### JWT Implementation

Using PyJWT for token generation:

```python
import jwt
from datetime import datetime, timedelta

SECRET_KEY = "your-secret-key"  # From env
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60

def create_access_token(user_id: int, email: str) -> str:
    """Create JWT access token."""
    expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    payload = {
        "sub": str(user_id),
        "email": email,
        "exp": expire,
        "iat": datetime.utcnow()
    }
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)

def verify_token(token: str) -> dict:
    """Verify and decode JWT token."""
    payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    return payload
```

### Password Hashing

Using bcrypt for secure password storage:

```python
import bcrypt

def hash_password(password: str) -> str:
    """Hash password with bcrypt."""
    salt = bcrypt.gensalt()
    return bcrypt.hashpw(password.encode(), salt).decode()

def verify_password(password: str, hashed: str) -> bool:
    """Verify password against hash."""
    return bcrypt.checkpw(password.encode(), hashed.encode())
```

---

## Files to Create

| File | Description |
|------|-------------|
| `app/users/auth.py` | AuthService with JWT, registration, login |
| `app/users/workspaces.py` | WorkspaceService (modify existing) |
| `app/api/v1/auth.py` | 8 auth endpoints |
| `app/api/v1/workspaces.py` | 9 workspace endpoints |

## Files to Modify

| File | Change |
|------|--------|
| `app/main.py` | Register auth and workspaces routers |
| `requirements.txt` | Add PyJWT, bcrypt |

---

## Implementation Steps

1. Add dependencies (PyJWT, bcrypt) to requirements.txt
2. Create `app/users/auth.py` with AuthService
3. Implement user registration and password hashing
4. Implement JWT token generation and validation
5. Update `app/users/workspaces.py` with WorkspaceService
6. Create `app/api/v1/auth.py` with auth endpoints
7. Create `app/api/v1/workspaces.py` with workspace endpoints
8. Register routers in main.py
9. Test all endpoints

---

## Test Plan

| Test ID | Test | Expected |
|---------|------|----------|
| AUTH-001 | Register user | User created, token returned |
| AUTH-002 | Login valid | JWT token returned |
| AUTH-003 | Login invalid | 401 Unauthorized |
| AUTH-004 | Get current user | User info returned |
| AUTH-005 | Refresh token | New token returned |
| WS-001 | Create workspace | Workspace created, owner assigned |
| WS-002 | List workspaces | Only user's workspaces |
| WS-003 | Invite member | Invitation created |
| WS-004 | Accept invite | User added to workspace |
| WS-005 | Remove member | Member removed (admin only) |

### Test Commands

```bash
# Register user
curl -s -X POST "http://localhost:8001/api/v1/auth/register" \
  -H "Content-Type: application/json" \
  -d '{"email": "test@example.com", "password": "password123", "name": "Test User"}' \
  | python -m json.tool

# Login
curl -s -X POST "http://localhost:8001/api/v1/auth/login" \
  -H "Content-Type: application/json" \
  -d '{"email": "test@example.com", "password": "password123"}' \
  | python -m json.tool

# Get current user (with token)
curl -s "http://localhost:8001/api/v1/auth/me" \
  -H "Authorization: Bearer <token>" \
  | python -m json.tool

# Create workspace
curl -s -X POST "http://localhost:8001/api/v1/workspaces" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <token>" \
  -d '{"name": "Investment Team", "description": "Our main workspace"}' \
  | python -m json.tool

# List workspaces
curl -s "http://localhost:8001/api/v1/workspaces" \
  -H "Authorization: Bearer <token>" \
  | python -m json.tool

# Invite member
curl -s -X POST "http://localhost:8001/api/v1/workspaces/1/invite" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <token>" \
  -d '{"email": "colleague@example.com", "role": "member"}' \
  | python -m json.tool
```

---

## Approval

- [x] **Approved by user** (2026-01-18)

## Implementation Notes

- Created `app/users/auth.py` with AuthService (JWT, bcrypt password hashing)
- Created `app/users/workspaces.py` with WorkspaceService (replaced old watchlist content)
- Tables auto-created: users, workspaces, workspace_members, workspace_invitations, password_reset_tokens, refresh_tokens
- 9 auth endpoints: register, login, logout, refresh, me, update profile, change password, reset request, reset
- 10 workspace endpoints: CRUD, members, invite, accept, update role, pending invitations
- JWT tokens with 1-hour expiry, refresh tokens with 7-day expiry
- Role-based access: admin, member, viewer

---

*Plan created: 2026-01-18*
*Completed: 2026-01-18*

