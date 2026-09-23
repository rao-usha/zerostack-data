"""
Access policy for the API (SPEC_127).

Every router in ``app/main.py`` is mounted with exactly one of these
dependencies (public routers excepted — see the spec's router table):

- ``require_user``                 any signed-in platform user, all methods
- ``require_admin``                role ``admin``, all methods
- ``require_admin_for_writes``     GET/HEAD/OPTIONS for users; writes need admin
- ``require_admin_or_stream_token``  admin via bearer, or ``?stream_token=``
                                   for EventSource (which cannot send headers)

``REQUIRE_AUTH`` is read per request, not at import, so the dependencies are
always attached and a route-table test can verify the policy. With
``REQUIRE_AUTH=false`` (local dev only) every dependency returns a synthetic
local-dev admin.
"""

import logging
from typing import Any, Callable, Dict, Optional

from fastapi import Depends, Header, HTTPException, Query, Request, status

from app.core.config import get_settings
from app.users.auth import AUD_APP, ROLE_ADMIN

logger = logging.getLogger(__name__)

SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})

# POST endpoints on read/write routers that are pure computations (no
# persistence, no LLM call at the router layer), so a user-level account may
# call them. Keyed "<module>.<function>" — see endpoint_key(). Before adding
# one, check the service does not write (lineage.compute_impact_analysis was
# removed: it caches its result into impact_analysis rows).
USER_WRITE_ENDPOINTS = frozenset(
    {
        "app.api.v1.compare.compare_portfolios",
        "app.api.v1.app_rankings.compare_apps",
        "app.api.v1.site_intel_sites.search_sites",
        "app.api.v1.site_intel_sites.compare_sites",
        "app.api.v1.site_intel_sites.score_site",
        "app.api.v1.site_intel_sites.unified_score",
        "app.api.v1.site_intel_sites.unified_compare",
        "app.api.v1.deal_models.run_sensitivity",
        "app.api.v1.pe_macro.score_lbo_entry",
    }
)

LOCAL_DEV_PRINCIPAL: Dict[str, Any] = {
    "user_id": None,
    "email": "local-dev@localhost",
    "name": "Local dev (REQUIRE_AUTH=false)",
    "role": ROLE_ADMIN,
    "aud": AUD_APP,
    "local_dev": True,
}


def auth_required() -> bool:
    return bool(get_settings().require_auth)


def endpoint_key(endpoint: Optional[Callable]) -> str:
    if endpoint is None:
        return ""
    return f"{getattr(endpoint, '__module__', '')}.{getattr(endpoint, '__name__', '')}"


def _unauthorized(detail: str) -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail=detail,
        headers={"WWW-Authenticate": "Bearer"},
    )


def _forbidden() -> HTTPException:
    return HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin role required")


def _with_auth_service(fn):
    from app.core.database import get_db
    from app.users.auth import AuthService

    db = next(get_db())
    try:
        return fn(AuthService(db))
    finally:
        db.close()


def authenticate_bearer(authorization: Optional[str]) -> Dict[str, Any]:
    """Validate a platform (``aud=nexdata-app``) bearer token or raise 401."""
    if not authorization:
        raise _unauthorized("Authorization header required")
    if not authorization.startswith("Bearer "):
        raise _unauthorized("Invalid authorization format")
    token = authorization[7:]
    try:
        return _with_auth_service(lambda svc: svc.verify_token(token))
    except ValueError as e:
        raise _unauthorized(str(e))


def authenticate_api_key(raw_key: str) -> Dict[str, Any]:
    """An ``admin``-scope API key (``X-API-Key``) as an admin principal.

    This is the non-interactive path for operator tooling — skills, curl,
    ``scripts/nexdata_client.py``, eval runs — which cannot hold a 60-minute
    browser JWT. ``read``/``write`` keys are for the ``/public`` API only and
    are refused here, so handing a customer a read key never opens the
    internal routers.
    """
    from app.auth.api_keys import APIKeyService, scope_allows

    from app.core.database import get_db

    db = next(get_db())
    try:
        info = APIKeyService(db).validate_key(raw_key)
    finally:
        db.close()
    if not info:
        raise _unauthorized("Invalid or expired API key")
    if not scope_allows(info.get("scope"), "admin"):
        raise _unauthorized(
            "Only admin-scope API keys are accepted outside /api/v1/public; "
            "use a bearer token"
        )
    return {
        "user_id": None,
        "email": info.get("owner_email"),
        "name": f"api-key:{info.get('name')}",
        "role": ROLE_ADMIN,
        "aud": AUD_APP,
        "api_key_id": info.get("id"),
    }


def current_principal(
    authorization: Optional[str] = Header(None),
    x_api_key: Optional[str] = Header(None, alias="X-API-Key"),
) -> Dict[str, Any]:
    if not auth_required():
        return dict(LOCAL_DEV_PRINCIPAL)
    if not authorization and x_api_key:
        return authenticate_api_key(x_api_key)
    return authenticate_bearer(authorization)


def require_user(principal: Dict[str, Any] = Depends(current_principal)) -> Dict[str, Any]:
    return principal


def require_admin(principal: Dict[str, Any] = Depends(current_principal)) -> Dict[str, Any]:
    if principal.get("role") != ROLE_ADMIN:
        raise _forbidden()
    return principal


def require_admin_for_writes(
    request: Request, principal: Dict[str, Any] = Depends(current_principal)
) -> Dict[str, Any]:
    if request.method in SAFE_METHODS:
        return principal
    if endpoint_key(request.scope.get("endpoint")) in USER_WRITE_ENDPOINTS:
        return principal
    if principal.get("role") != ROLE_ADMIN:
        raise _forbidden()
    return principal


def require_admin_or_stream_token(
    authorization: Optional[str] = Header(None),
    x_api_key: Optional[str] = Header(None, alias="X-API-Key"),
    stream_token: Optional[str] = Query(
        None, description="Short-lived token from POST /auth/stream-token (EventSource)"
    ),
) -> Dict[str, Any]:
    if not auth_required():
        return dict(LOCAL_DEV_PRINCIPAL)
    if authorization:
        principal = authenticate_bearer(authorization)
    elif x_api_key:
        principal = authenticate_api_key(x_api_key)
    elif stream_token:
        try:
            principal = _with_auth_service(lambda svc: svc.verify_stream_token(stream_token))
        except ValueError as e:
            raise _unauthorized(str(e))
    else:
        raise _unauthorized("Authorization header or stream_token required")
    if principal.get("role") != ROLE_ADMIN:
        raise _forbidden()
    return principal


def log_auth_mode() -> None:
    """Loud startup notice when auth is disabled."""
    try:
        required = auth_required()
    except Exception:  # noqa: BLE001 — settings incomplete at import (e.g. tests)
        return
    if not required:
        logger.warning(
            "=" * 72
            + "\nREQUIRE_AUTH=false — EVERY API ROUTE IS OPEN AND ACTS AS ADMIN. "
            "Local development only; never expose this process.\n"
            + "=" * 72
        )
