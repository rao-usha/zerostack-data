"""
Workspace API endpoints.

Provides workspace management, member invitations, and role-based access.
"""

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field, EmailStr
from typing import Optional

from app.core.database import get_db
from app.users.workspaces import WorkspaceService
from app.api.v1.auth import get_current_user

router = APIRouter(prefix="/workspaces", tags=["workspaces"])


# Request/Response Models


class CreateWorkspaceRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=255, description="Workspace name")
    description: Optional[str] = Field(None, description="Workspace description")


class UpdateWorkspaceRequest(BaseModel):
    name: Optional[str] = Field(
        None, min_length=1, max_length=255, description="Workspace name"
    )
    description: Optional[str] = Field(None, description="Workspace description")


class InviteMemberRequest(BaseModel):
    email: EmailStr = Field(..., description="Email of user to invite")
    role: str = Field(
        "member", pattern="^(viewer|member|admin)$", description="Role to assign"
    )


class UpdateMemberRoleRequest(BaseModel):
    role: str = Field(..., pattern="^(viewer|member|admin)$", description="New role")


# Endpoints


@router.post("")
def create_workspace(
    request: CreateWorkspaceRequest, current_user: dict = Depends(get_current_user)
):
    """Create a new workspace."""
    db = next(get_db())
    try:
        service = WorkspaceService(db)
        result = service.create_workspace(
            owner_id=current_user["user_id"],
            name=request.name,
            description=request.description,
        )
        return result
    finally:
        db.close()


@router.get("")
def list_workspaces(current_user: dict = Depends(get_current_user)):
    """List workspaces user belongs to."""
    db = next(get_db())
    try:
        service = WorkspaceService(db)
        workspaces = service.list_workspaces(current_user["user_id"])
        return {"workspaces": workspaces, "count": len(workspaces)}
    finally:
        db.close()


@router.get("/{workspace_id}")
def get_workspace(workspace_id: int, current_user: dict = Depends(get_current_user)):
    """Get workspace details."""
    db = next(get_db())
    try:
        service = WorkspaceService(db)
        workspace = service.get_workspace(workspace_id, current_user["user_id"])
        if not workspace:
            raise HTTPException(
                status_code=404, detail="Workspace not found or access denied"
            )
        return workspace
    finally:
        db.close()


@router.patch("/{workspace_id}")
def update_workspace(
    workspace_id: int,
    request: UpdateWorkspaceRequest,
    current_user: dict = Depends(get_current_user),
):
    """Update workspace (admin only)."""
    db = next(get_db())
    try:
        service = WorkspaceService(db)

        updates = {}
        if request.name is not None:
            updates["name"] = request.name
        if request.description is not None:
            updates["description"] = request.description

        workspace = service.update_workspace(
            workspace_id, current_user["user_id"], updates
        )
        return workspace
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    finally:
        db.close()


@router.delete("/{workspace_id}")
def delete_workspace(workspace_id: int, current_user: dict = Depends(get_current_user)):
    """Delete workspace (owner only)."""
    db = next(get_db())
    try:
        service = WorkspaceService(db)
        service.delete_workspace(workspace_id, current_user["user_id"])
        return {"message": "Workspace deleted successfully", "id": workspace_id}
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    finally:
        db.close()


@router.get("/{workspace_id}/members")
def list_members(workspace_id: int, current_user: dict = Depends(get_current_user)):
    """List workspace members."""
    db = next(get_db())
    try:
        service = WorkspaceService(db)
        members = service.get_members(workspace_id, current_user["user_id"])
        return {"workspace_id": workspace_id, "members": members, "count": len(members)}
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    finally:
        db.close()


@router.post("/{workspace_id}/invite")
def invite_member(
    workspace_id: int,
    request: InviteMemberRequest,
    current_user: dict = Depends(get_current_user),
):
    """Invite a user to the workspace (admin only)."""
    db = next(get_db())
    try:
        service = WorkspaceService(db)
        invitation = service.invite_member(
            workspace_id=workspace_id,
            inviter_id=current_user["user_id"],
            email=request.email,
            role=request.role,
        )
        return invitation
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        db.close()


@router.get("/{workspace_id}/invitations")
def list_invitations(workspace_id: int, current_user: dict = Depends(get_current_user)):
    """List pending invitations (admin only)."""
    db = next(get_db())
    try:
        service = WorkspaceService(db)
        invitations = service.get_pending_invitations(
            workspace_id, current_user["user_id"]
        )
        return {
            "workspace_id": workspace_id,
            "invitations": invitations,
            "count": len(invitations),
        }
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    finally:
        db.close()


@router.delete("/{workspace_id}/members/{user_id}")
def remove_member(
    workspace_id: int, user_id: int, current_user: dict = Depends(get_current_user)
):
    """Remove a member from workspace (admin only)."""
    db = next(get_db())
    try:
        service = WorkspaceService(db)
        removed = service.remove_member(workspace_id, current_user["user_id"], user_id)
        if not removed:
            raise HTTPException(status_code=404, detail="Member not found")
        return {"message": "Member removed successfully", "user_id": user_id}
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        db.close()


@router.patch("/{workspace_id}/members/{user_id}")
def update_member_role(
    workspace_id: int,
    user_id: int,
    request: UpdateMemberRoleRequest,
    current_user: dict = Depends(get_current_user),
):
    """Update member role (admin only)."""
    db = next(get_db())
    try:
        service = WorkspaceService(db)
        updated = service.update_member_role(
            workspace_id=workspace_id,
            admin_id=current_user["user_id"],
            user_id=user_id,
            new_role=request.role,
        )
        if not updated:
            raise HTTPException(status_code=404, detail="Member not found")
        return {
            "message": "Role updated successfully",
            "user_id": user_id,
            "role": request.role,
        }
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        db.close()


@router.post("/invitations/{token}/accept")
def accept_invitation(token: str, current_user: dict = Depends(get_current_user)):
    """Accept a workspace invitation."""
    db = next(get_db())
    try:
        service = WorkspaceService(db)
        result = service.accept_invitation(token, current_user["user_id"])
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        db.close()
