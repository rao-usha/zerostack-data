"""
Workspace Service for T30: User Auth & Workspaces.

Provides workspace management, member invitations, and role-based access.

Note: This file replaces the previous watchlists.py content with
workspace functionality. Watchlist features are preserved in a separate module.
"""

import secrets
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# Role hierarchy
ROLES = ["viewer", "member", "admin"]
ROLE_HIERARCHY = {role: i for i, role in enumerate(ROLES)}

INVITATION_EXPIRE_DAYS = 7


class WorkspaceService:
    """Workspace management service."""

    def __init__(self, db: Session):
        self.db = db
        self._ensure_tables()

    def _ensure_tables(self):
        """Create tables if they don't exist."""
        self.db.execute(
            text("""
            CREATE TABLE IF NOT EXISTS workspaces (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                description TEXT,
                owner_id INTEGER NOT NULL,
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        )

        self.db.execute(
            text("""
            CREATE TABLE IF NOT EXISTS workspace_members (
                id SERIAL PRIMARY KEY,
                workspace_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                role VARCHAR(20) DEFAULT 'member',
                invited_by INTEGER,
                joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(workspace_id, user_id)
            )
        """)
        )

        self.db.execute(
            text("""
            CREATE TABLE IF NOT EXISTS workspace_invitations (
                id SERIAL PRIMARY KEY,
                workspace_id INTEGER NOT NULL,
                email VARCHAR(255) NOT NULL,
                role VARCHAR(20) DEFAULT 'member',
                token VARCHAR(64) NOT NULL UNIQUE,
                invited_by INTEGER,
                expires_at TIMESTAMP NOT NULL,
                accepted_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        )

        # Create indexes
        self.db.execute(
            text("""
            CREATE INDEX IF NOT EXISTS idx_workspace_members_user ON workspace_members(user_id)
        """)
        )
        self.db.execute(
            text("""
            CREATE INDEX IF NOT EXISTS idx_workspace_members_workspace ON workspace_members(workspace_id)
        """)
        )
        self.db.execute(
            text("""
            CREATE INDEX IF NOT EXISTS idx_invitations_token ON workspace_invitations(token)
        """)
        )
        self.db.execute(
            text("""
            CREATE INDEX IF NOT EXISTS idx_invitations_email ON workspace_invitations(email)
        """)
        )

        self.db.commit()

    def _check_permission(
        self, workspace_id: int, user_id: int, required_role: str = "viewer"
    ) -> bool:
        """Check if user has required role in workspace."""
        result = self.db.execute(
            text("""
            SELECT role FROM workspace_members
            WHERE workspace_id = :workspace_id AND user_id = :user_id
        """),
            {"workspace_id": workspace_id, "user_id": user_id},
        )

        row = result.fetchone()
        if not row:
            # Check if user is owner
            owner_result = self.db.execute(
                text("""
                SELECT owner_id FROM workspaces WHERE id = :workspace_id
            """),
                {"workspace_id": workspace_id},
            )
            owner_row = owner_result.fetchone()
            if owner_row and owner_row[0] == user_id:
                return True  # Owner has all permissions
            return False

        user_role = row[0]
        return ROLE_HIERARCHY.get(user_role, -1) >= ROLE_HIERARCHY.get(required_role, 0)

    def create_workspace(
        self, owner_id: int, name: str, description: Optional[str] = None
    ) -> Dict[str, Any]:
        """Create a new workspace."""
        result = self.db.execute(
            text("""
            INSERT INTO workspaces (name, description, owner_id)
            VALUES (:name, :description, :owner_id)
            RETURNING id, created_at
        """),
            {"name": name, "description": description, "owner_id": owner_id},
        )

        row = result.fetchone()
        workspace_id = row[0]

        # Add owner as admin member
        self.db.execute(
            text("""
            INSERT INTO workspace_members (workspace_id, user_id, role, invited_by)
            VALUES (:workspace_id, :user_id, 'admin', :user_id)
        """),
            {"workspace_id": workspace_id, "user_id": owner_id},
        )

        self.db.commit()

        return {
            "id": workspace_id,
            "name": name,
            "description": description,
            "owner_id": owner_id,
            "member_count": 1,
            "your_role": "admin",
            "created_at": row[1].isoformat() if row[1] else None,
        }

    def get_workspace(
        self, workspace_id: int, user_id: int
    ) -> Optional[Dict[str, Any]]:
        """Get workspace if user has access."""
        if not self._check_permission(workspace_id, user_id, "viewer"):
            return None

        result = self.db.execute(
            text("""
            SELECT w.id, w.name, w.description, w.owner_id, w.is_active, w.created_at,
                   (SELECT COUNT(*) FROM workspace_members WHERE workspace_id = w.id) as member_count,
                   (SELECT role FROM workspace_members WHERE workspace_id = w.id AND user_id = :user_id) as your_role
            FROM workspaces w
            WHERE w.id = :workspace_id AND w.is_active = TRUE
        """),
            {"workspace_id": workspace_id, "user_id": user_id},
        )

        row = result.fetchone()
        if not row:
            return None

        return {
            "id": row[0],
            "name": row[1],
            "description": row[2],
            "owner_id": row[3],
            "is_active": row[4],
            "created_at": row[5].isoformat() if row[5] else None,
            "member_count": row[6],
            "your_role": row[7] or ("admin" if row[3] == user_id else None),
        }

    def list_workspaces(self, user_id: int) -> List[Dict[str, Any]]:
        """List workspaces user belongs to."""
        result = self.db.execute(
            text("""
            SELECT w.id, w.name, w.description, w.owner_id, w.created_at,
                   wm.role,
                   (SELECT COUNT(*) FROM workspace_members WHERE workspace_id = w.id) as member_count
            FROM workspaces w
            JOIN workspace_members wm ON w.id = wm.workspace_id
            WHERE wm.user_id = :user_id AND w.is_active = TRUE
            ORDER BY w.created_at DESC
        """),
            {"user_id": user_id},
        )

        workspaces = []
        for row in result.fetchall():
            workspaces.append(
                {
                    "id": row[0],
                    "name": row[1],
                    "description": row[2],
                    "owner_id": row[3],
                    "created_at": row[4].isoformat() if row[4] else None,
                    "your_role": row[5],
                    "member_count": row[6],
                }
            )

        return workspaces

    def update_workspace(
        self, workspace_id: int, user_id: int, updates: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Update workspace (admin only)."""
        if not self._check_permission(workspace_id, user_id, "admin"):
            raise PermissionError("Admin access required")

        allowed_fields = ["name", "description"]
        set_clauses = []
        params = {"workspace_id": workspace_id}

        for field in allowed_fields:
            if field in updates:
                set_clauses.append(f"{field} = :{field}")
                params[field] = updates[field]

        if not set_clauses:
            return self.get_workspace(workspace_id, user_id)

        set_clauses.append("updated_at = CURRENT_TIMESTAMP")

        query = (
            f"UPDATE workspaces SET {', '.join(set_clauses)} WHERE id = :workspace_id"
        )
        self.db.execute(text(query), params)
        self.db.commit()

        return self.get_workspace(workspace_id, user_id)

    def delete_workspace(self, workspace_id: int, user_id: int) -> bool:
        """Delete workspace (owner only)."""
        # Check if user is owner
        result = self.db.execute(
            text("""
            SELECT owner_id FROM workspaces WHERE id = :workspace_id
        """),
            {"workspace_id": workspace_id},
        )

        row = result.fetchone()
        if not row or row[0] != user_id:
            raise PermissionError("Only workspace owner can delete")

        # Soft delete
        self.db.execute(
            text("""
            UPDATE workspaces SET is_active = FALSE, updated_at = CURRENT_TIMESTAMP
            WHERE id = :workspace_id
        """),
            {"workspace_id": workspace_id},
        )
        self.db.commit()

        return True

    def invite_member(
        self, workspace_id: int, inviter_id: int, email: str, role: str = "member"
    ) -> Dict[str, Any]:
        """Invite user to workspace."""
        if not self._check_permission(workspace_id, inviter_id, "admin"):
            raise PermissionError("Admin access required to invite members")

        if role not in ROLES:
            raise ValueError(f"Invalid role. Must be one of: {ROLES}")

        # Check if already invited
        result = self.db.execute(
            text("""
            SELECT id FROM workspace_invitations
            WHERE workspace_id = :workspace_id AND email = :email AND accepted_at IS NULL
        """),
            {"workspace_id": workspace_id, "email": email.lower()},
        )

        if result.fetchone():
            raise ValueError("User already has pending invitation")

        # Check if already a member
        member_check = self.db.execute(
            text("""
            SELECT wm.id FROM workspace_members wm
            JOIN users u ON wm.user_id = u.id
            WHERE wm.workspace_id = :workspace_id AND u.email = :email
        """),
            {"workspace_id": workspace_id, "email": email.lower()},
        )

        if member_check.fetchone():
            raise ValueError("User is already a member")

        # Create invitation
        token = secrets.token_urlsafe(32)
        expires_at = datetime.utcnow() + timedelta(days=INVITATION_EXPIRE_DAYS)

        result = self.db.execute(
            text("""
            INSERT INTO workspace_invitations (workspace_id, email, role, token, invited_by, expires_at)
            VALUES (:workspace_id, :email, :role, :token, :invited_by, :expires_at)
            RETURNING id, created_at
        """),
            {
                "workspace_id": workspace_id,
                "email": email.lower(),
                "role": role,
                "token": token,
                "invited_by": inviter_id,
                "expires_at": expires_at,
            },
        )

        row = result.fetchone()
        self.db.commit()

        return {
            "id": row[0],
            "workspace_id": workspace_id,
            "email": email.lower(),
            "role": role,
            "token": token,
            "expires_at": expires_at.isoformat(),
            "created_at": row[1].isoformat() if row[1] else None,
        }

    def accept_invitation(self, token: str, user_id: int) -> Dict[str, Any]:
        """Accept workspace invitation."""
        # Get invitation
        result = self.db.execute(
            text("""
            SELECT wi.id, wi.workspace_id, wi.email, wi.role, wi.expires_at, wi.accepted_at,
                   w.name as workspace_name
            FROM workspace_invitations wi
            JOIN workspaces w ON wi.workspace_id = w.id
            WHERE wi.token = :token
        """),
            {"token": token},
        )

        row = result.fetchone()
        if not row:
            raise ValueError("Invalid invitation token")

        inv_id, workspace_id, email, role, expires_at, accepted_at, workspace_name = row

        if accepted_at:
            raise ValueError("Invitation already accepted")

        if expires_at < datetime.utcnow():
            raise ValueError("Invitation expired")

        # Verify user email matches invitation
        user_result = self.db.execute(
            text("""
            SELECT email FROM users WHERE id = :user_id
        """),
            {"user_id": user_id},
        )

        user_row = user_result.fetchone()
        if not user_row or user_row[0].lower() != email.lower():
            raise ValueError("Invitation email does not match your account")

        # Add member
        self.db.execute(
            text("""
            INSERT INTO workspace_members (workspace_id, user_id, role, invited_by)
            SELECT :workspace_id, :user_id, :role, wi.invited_by
            FROM workspace_invitations wi WHERE wi.id = :inv_id
            ON CONFLICT (workspace_id, user_id) DO NOTHING
        """),
            {
                "workspace_id": workspace_id,
                "user_id": user_id,
                "role": role,
                "inv_id": inv_id,
            },
        )

        # Mark invitation as accepted
        self.db.execute(
            text("""
            UPDATE workspace_invitations SET accepted_at = CURRENT_TIMESTAMP
            WHERE id = :inv_id
        """),
            {"inv_id": inv_id},
        )

        self.db.commit()

        return {
            "workspace_id": workspace_id,
            "workspace_name": workspace_name,
            "role": role,
            "message": "Successfully joined workspace",
        }

    def remove_member(self, workspace_id: int, admin_id: int, user_id: int) -> bool:
        """Remove member from workspace."""
        if not self._check_permission(workspace_id, admin_id, "admin"):
            raise PermissionError("Admin access required")

        # Check if trying to remove owner
        owner_result = self.db.execute(
            text("""
            SELECT owner_id FROM workspaces WHERE id = :workspace_id
        """),
            {"workspace_id": workspace_id},
        )

        owner_row = owner_result.fetchone()
        if owner_row and owner_row[0] == user_id:
            raise ValueError("Cannot remove workspace owner")

        result = self.db.execute(
            text("""
            DELETE FROM workspace_members
            WHERE workspace_id = :workspace_id AND user_id = :user_id
            RETURNING id
        """),
            {"workspace_id": workspace_id, "user_id": user_id},
        )

        row = result.fetchone()
        self.db.commit()

        return row is not None

    def get_members(self, workspace_id: int, user_id: int) -> List[Dict[str, Any]]:
        """List workspace members."""
        if not self._check_permission(workspace_id, user_id, "viewer"):
            raise PermissionError("Access denied")

        result = self.db.execute(
            text("""
            SELECT wm.user_id, u.email, u.name, wm.role, wm.joined_at,
                   (w.owner_id = wm.user_id) as is_owner
            FROM workspace_members wm
            JOIN users u ON wm.user_id = u.id
            JOIN workspaces w ON wm.workspace_id = w.id
            WHERE wm.workspace_id = :workspace_id
            ORDER BY wm.joined_at
        """),
            {"workspace_id": workspace_id},
        )

        members = []
        for row in result.fetchall():
            members.append(
                {
                    "user_id": row[0],
                    "email": row[1],
                    "name": row[2],
                    "role": row[3],
                    "joined_at": row[4].isoformat() if row[4] else None,
                    "is_owner": row[5],
                }
            )

        return members

    def get_pending_invitations(
        self, workspace_id: int, user_id: int
    ) -> List[Dict[str, Any]]:
        """Get pending invitations for workspace."""
        if not self._check_permission(workspace_id, user_id, "admin"):
            raise PermissionError("Admin access required")

        result = self.db.execute(
            text("""
            SELECT wi.id, wi.email, wi.role, wi.expires_at, wi.created_at,
                   u.name as invited_by_name
            FROM workspace_invitations wi
            LEFT JOIN users u ON wi.invited_by = u.id
            WHERE wi.workspace_id = :workspace_id AND wi.accepted_at IS NULL
            ORDER BY wi.created_at DESC
        """),
            {"workspace_id": workspace_id},
        )

        invitations = []
        for row in result.fetchall():
            invitations.append(
                {
                    "id": row[0],
                    "email": row[1],
                    "role": row[2],
                    "expires_at": row[3].isoformat() if row[3] else None,
                    "created_at": row[4].isoformat() if row[4] else None,
                    "invited_by": row[5],
                }
            )

        return invitations

    def update_member_role(
        self, workspace_id: int, admin_id: int, user_id: int, new_role: str
    ) -> bool:
        """Update member role (admin only)."""
        if not self._check_permission(workspace_id, admin_id, "admin"):
            raise PermissionError("Admin access required")

        if new_role not in ROLES:
            raise ValueError(f"Invalid role. Must be one of: {ROLES}")

        # Check if trying to change owner's role
        owner_result = self.db.execute(
            text("""
            SELECT owner_id FROM workspaces WHERE id = :workspace_id
        """),
            {"workspace_id": workspace_id},
        )

        owner_row = owner_result.fetchone()
        if owner_row and owner_row[0] == user_id:
            raise ValueError("Cannot change owner's role")

        result = self.db.execute(
            text("""
            UPDATE workspace_members SET role = :role
            WHERE workspace_id = :workspace_id AND user_id = :user_id
            RETURNING id
        """),
            {"workspace_id": workspace_id, "user_id": user_id, "role": new_role},
        )

        row = result.fetchone()
        self.db.commit()

        return row is not None
