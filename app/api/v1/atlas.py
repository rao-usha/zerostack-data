"""
Nexdata Atlas API — SPEC_064 / PLAN_065 rev_01.

Public router (no auth) — the primary product surface. Query a market /
sector / geography, get back an exploration: resolved entities, insight
cards, cross-dataset connections, provenance, related queries, share URL.

Endpoints:
  POST /api/v1/atlas/explore             — run an exploration
  GET  /api/v1/atlas/explorations/{ref}  — load a saved/shareable exploration
  POST /api/v1/atlas/events              — record interaction telemetry
  POST /api/v1/atlas/feedback            — record card thumbs up/down
  GET  /api/v1/atlas/taxonomies          — NAICS sectors + MSA list for pickers
"""

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.services.atlas import AtlasService
from app.services.atlas.telemetry import AtlasTelemetry
from app.services.diligence.taxonomies import load_msa, load_naics

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/atlas", tags=["Nexdata Atlas (public-data explorer)"])


# ─────────────────────────────────────────────────────────────────────────────
# Schemas
# ─────────────────────────────────────────────────────────────────────────────

class ExploreBody(BaseModel):
    query: str = Field(..., min_length=1, max_length=400)
    msa: Optional[str] = None
    naics: Optional[str] = None
    session_id: Optional[str] = None


class EventBody(BaseModel):
    event_type: str = Field(..., min_length=1, max_length=64)
    exploration_id: Optional[int] = None
    session_id: Optional[str] = None
    card_id: Optional[str] = None
    payload: Optional[Dict[str, Any]] = None


class FeedbackBody(BaseModel):
    card_id: str = Field(..., min_length=1, max_length=64)
    feedback: str = Field(..., pattern="^(thumbs_up|thumbs_down)$")
    exploration_id: Optional[int] = None
    session_id: Optional[str] = None
    comment: Optional[str] = Field(None, max_length=1000)


# ─────────────────────────────────────────────────────────────────────────────
# Endpoints
# ─────────────────────────────────────────────────────────────────────────────

@router.post("/explore")
def explore(body: ExploreBody, request: Request, db: Session = Depends(get_db)):
    """Run an exploration. Public, anonymous-friendly. Returns the full
    exploration object (resolved entities, cards, connections, related)."""
    anon_ip = request.client.host if request.client else None
    try:
        exploration = AtlasService(db).explore(
            query=body.query,
            msa=body.msa,
            naics=body.naics,
            session_id=body.session_id,
            anon_ip=anon_ip,
        )
    except ValueError as exc:
        # Resolution failure — the query named neither a market nor a sector.
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        )
    except Exception as exc:  # noqa: BLE001
        logger.error("Atlas explore failed for %r: %s", body.query, exc)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Exploration failed to assemble. The data backend may be unreachable.",
        )
    return exploration.to_dict()


@router.get("/explorations/{ref}")
def get_exploration(ref: str, db: Session = Depends(get_db)):
    """Load a saved exploration by slug or numeric id — powers shareable URLs."""
    found = AtlasService(db).get_exploration(ref)
    if not found:
        raise HTTPException(status_code=404, detail="Exploration not found")
    return found


@router.post("/events")
def record_event(body: EventBody, db: Session = Depends(get_db)):
    """Record an interaction telemetry event. No auth. Best-effort — a
    telemetry failure returns 200 with recorded=false rather than erroring,
    so the client UI never breaks on a telemetry hiccup."""
    try:
        event_id = AtlasTelemetry(db).record_event(
            event_type=body.event_type,
            exploration_id=body.exploration_id,
            session_id=body.session_id,
            card_id=body.card_id,
            payload=body.payload,
        )
        return {"recorded": True, "event_id": event_id}
    except Exception as exc:  # noqa: BLE001
        logger.warning("Atlas event record failed: %s", exc)
        return {"recorded": False}


@router.post("/feedback")
def record_feedback(body: FeedbackBody, db: Session = Depends(get_db)):
    """Record a card thumbs up/down. No auth. Best-effort."""
    try:
        feedback_id = AtlasTelemetry(db).record_feedback(
            card_id=body.card_id,
            feedback=body.feedback,
            exploration_id=body.exploration_id,
            session_id=body.session_id,
            comment=body.comment,
        )
        return {"recorded": True, "feedback_id": feedback_id}
    except Exception as exc:  # noqa: BLE001
        logger.warning("Atlas feedback record failed: %s", exc)
        return {"recorded": False}


@router.get("/taxonomies")
def get_taxonomies():
    """NAICS sector→industry tree + MSA flat list, for the explorer pickers."""
    naics = load_naics()
    msa = load_msa()

    sectors: Dict[str, str] = {}
    industries: Dict[str, List[Dict[str, str]]] = {}
    for code, node in naics.items():
        if node.digits == 2:
            sectors[code] = node.label
            industries.setdefault(code, [])
    for code, node in naics.items():
        if node.digits == 4:
            industries.setdefault(code[:2], []).append(
                {"code": code, "label": node.label})
    for sec in industries:
        industries[sec].sort(key=lambda x: x["code"])

    naics_tree = [
        {"sector_code": s, "sector_label": sectors[s],
         "industries": industries.get(s, [])}
        for s in sorted(sectors)
    ]
    msa_list = sorted(
        ({"cbsa_code": r.cbsa_code, "title": r.title,
          "state_abbrs": list(r.state_abbrs)} for r in msa.values()),
        key=lambda x: x["title"],
    )
    return {"naics": naics_tree, "msa": msa_list}
