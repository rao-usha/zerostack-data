"""
Catalog rights review endpoints (SPEC_142).

    GET  /api/v1/catalog/rights/review        the review queue (admin)
    POST /api/v1/catalog/rights/{key}/review  record a sign-off decision (admin)
    GET  /api/v1/catalog/rights/report       rights report, json or markdown
    GET  /api/v1/catalog/rights/{key}         one dataset's rights block, hash, proposal, history

A recorded decision is an audit row (``catalog_rights_review``); it never
makes a dataset reviewed. ``reviewed`` comes only from the committed
``app/catalog/rights_reviewed.py`` hash (see ``app.catalog.rights_review``).

Mounted with ``require_admin_for_writes`` and included **before** the
``catalog_schema`` / ``catalog`` routers in ``app/main.py``; the review routes
also require the admin role for GET.
"""

import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.catalog import get_spec
from app.core.authz import require_admin
from app.core.database import get_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/catalog/rights", tags=["catalog"])


class ReviewRequest(BaseModel):
    decision: str = Field(..., description="confirm_current | accept_proposal | reject")
    rights_hash: str = Field(..., description="rights_hash of the block you reviewed (from the queue)")
    note: str = Field(..., description="what was checked (terms page, date, caveats); >= 10 characters")


def _spec_or_404(key: str):
    spec = get_spec(key)
    if spec is None:
        raise HTTPException(status_code=404, detail=f"unknown dataset {key!r}")
    return spec


def _engine(db: Session):
    try:
        return db.get_bind()
    except Exception:  # no database configured: static answers only
        return None


@router.get("/review")
def rights_review_queue(
    live: bool = Query(True, description="count rows of storage-limited holdings (cached 5 min)"),
    db: Session = Depends(get_db),
    _admin: Dict[str, Any] = Depends(require_admin),
):
    """Proposals, candidates for reviewed=True (PLAN_088 §1.7, PE/entity pack first),
    storage-forbidden / time-limited / agreement-required datasets with row counts,
    PII-blocked datasets, and decisions awaiting commit or gone stale (admin)."""
    from app.catalog.rights_review import build_queue

    return build_queue(_engine(db), live=live)


@router.post("/{key}/review", status_code=201)
def record_rights_review(
    key: str,
    body: ReviewRequest,
    db: Session = Depends(get_db),
    principal: Dict[str, Any] = Depends(require_admin),
):
    """Record a decision on the dataset's *current* rights block (admin). 409 when
    ``rights_hash`` is not the current block's. Does not change ``reviewed``."""
    from app.catalog.rights_review import ReviewError, record_review

    spec = _spec_or_404(key)
    try:
        return record_review(db.get_bind(), spec, body.decision, body.rights_hash, body.note, principal)
    except ReviewError as e:
        raise HTTPException(status_code=e.status, detail=e.detail)


@router.get("/report")
def rights_report(
    format: str = Query("json", pattern="^(json|md)$"),
    live: bool = Query(True, description="live row counts of storage-limited holdings"),
    db: Session = Depends(get_db),
):
    """Per source: current vs proposed rights with citations; storage-forbidden holdings with
    live row counts; the gating effect on samples and export."""
    from app.catalog.rights_review import build_report, render_markdown

    report = build_report(_engine(db), live=live)
    if format == "md":
        return PlainTextResponse(render_markdown(report), media_type="text/markdown; charset=utf-8")
    return report


@router.get("/{key}")
def dataset_rights(
    key: str,
    history: int = Query(20, ge=0, le=200, description="review rows to include, newest first"),
    db: Session = Depends(get_db),
):
    """The dataset's rights block with its hash, proposal, gate and review history."""
    from app.catalog.rights_review import (latest_reviews, proposal_hash, review_history, review_state,
                                           rights_diff)

    spec = _spec_or_404(key)
    engine: Optional[Any] = _engine(db)
    rows = []
    latest = None
    if engine is not None and history:
        try:
            rows = review_history(engine, key, history)
            latest = latest_reviews(engine).get(key)
        except Exception as e:
            logger.info(f"[rights] history of {key} unavailable: {type(e).__name__}")
    return {
        "dataset": key,
        "rights": spec.rights_dict(),
        "pii_class": spec.pii_class,
        "origin": spec.origin,
        "rights_hash": spec.rights_hash,
        "proposed_block": spec.proposed_block(),
        "proposal_hash": proposal_hash(spec),
        "diff": rights_diff(spec.rights_block(), spec.proposed_block()),
        "review_state": review_state(spec, latest),
        "reviews": rows,
    }
