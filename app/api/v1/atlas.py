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
from app.services.atlas import boundaries as boundaries_mod
from app.services.atlas import layers as layers_mod
from app.services.atlas import series as series_mod
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


# ─────────────────────────────────────────────────────────────────────────────
# Layer API — SPEC_065 (the map's data backbone)
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/layers")
def list_layers_endpoint():
    """Return the layer registry grouped by domain. Drives the map's layer
    panel. Honest grain + vintage per layer; layers cut by the data-review
    (usaspending thin/dateless; ACS county-wealth deferred) are simply absent."""
    return {
        "layers_by_domain": layers_mod.list_layers_by_domain(),
        "excluded_by_design": layers_mod.EXCLUDED_BY_DESIGN,
    }


@router.get("/layer/{layer_id}")
def get_layer_endpoint(layer_id: str, db: Session = Depends(get_db)):
    """Return one layer's data — choropleth `{geo_id: value}` + legend OR
    point GeoJSON FeatureCollection, depending on the layer's declared grain."""
    try:
        layers_mod.get_layer(layer_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Unknown layer: {layer_id!r}")
    result = layers_mod.build_layer(db, layer_id)
    return result.to_dict()


@router.get("/boundaries")
def get_boundaries_endpoint(
    geo_level: str = "county",
    tolerance: float = 0.005,
    bbox: Optional[str] = None,
    db: Session = Depends(get_db),
):
    """Return a GeoJSON FeatureCollection of boundaries at the requested
    geo_level. `bbox` is "minx,miny,maxx,maxy" — restricts to polygons
    intersecting the bbox. Critical for tract grain (~74k features
    nationally would otherwise time out)."""
    try:
        return boundaries_mod.fetch_boundaries(
            db, geo_level=geo_level, tolerance=tolerance, bbox=bbox,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.get("/place/{geo_id}")
def get_place_endpoint(geo_id: str, db: Session = Depends(get_db)):
    """For every registered choropleth layer applicable at this place's
    grain (county or state), return its value at `geo_id`. The data
    backbone for click-a-place drill-down — the layer-aggregate sibling of
    `/explore`'s cross-dataset cards."""
    if not (geo_id.isdigit() and len(geo_id) in (2, 5)):
        raise HTTPException(
            status_code=400,
            detail=f"geo_id must be 2-digit state FIPS or 5-digit county FIPS; got {geo_id!r}",
        )
    return {
        "geo_id": geo_id,
        "grain": "state" if len(geo_id) == 2 else "county",
        "layers": layers_mod.place_aggregate(db, geo_id),
    }


# ─────────────────────────────────────────────────────────────────────────────
# SPEC_066b additions — place-level time series (sparklines) + migration arcs
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/place/{geo_id}/series")
def get_place_series_endpoint(
    geo_id: str,
    layer: str,
    db: Session = Depends(get_db),
):
    """Time series at a place for one series-capable layer (FEMA per year,
    FDIC quarterly deposits, IRS migration net AGI per year). Drives the
    place panel sparklines in SPEC_066b. Unsupported layers return an
    empty `points: []` rather than 404 — UI gracefully hides the spark."""
    if not (geo_id.isdigit() and len(geo_id) in (2, 5)):
        raise HTTPException(
            status_code=400,
            detail=f"geo_id must be 2- or 5-digit FIPS; got {geo_id!r}",
        )
    return series_mod.fetch_place_series(db, geo_id=geo_id, layer_id=layer)


# ─────────────────────────────────────────────────────────────────────────────
# SPEC_066c additions — calendar events + FEMA time-cascade
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/place/{geo_id}/events")
def get_place_events_endpoint(
    geo_id: str,
    source: str = "fema",
    limit: int = 200,
    db: Session = Depends(get_db),
):
    """Raw dated events for a place — the calendar-heatmap data layer."""
    try:
        return series_mod.fetch_place_events(
            db, geo_id=geo_id, source=source, limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.get("/layer/disaster_fema_declarations/cascade")
def get_fema_cascade_endpoint(db: Session = Depends(get_db)):
    """Pre-fetched per-(county, year) FEMA counts. Drives the
    time-cascade scrubber — frontend fetches once, slides client-side."""
    return series_mod.fetch_fema_cascade(db)


@router.get("/layer/econ_cbp_establishments_county/cascade")
def get_cbp_cascade_endpoint(db: Session = Depends(get_db)):
    """Pre-fetched per-(county, year) CBP establishment counts. Same
    shape as the FEMA cascade — drives the same scrubber UI pattern
    over industry density. SPEC_075."""
    return series_mod.fetch_cbp_cascade(db)


# ─────────────────────────────────────────────────────────────────────────────
# SPEC_067 — Recent Activity feed
# ─────────────────────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────────────────────
# SPEC_078 Phase B — Atlas Pilot (LLM-driven agent)
# ─────────────────────────────────────────────────────────────────────────────

class FitScoreBody(BaseModel):
    # SPEC_087 — Decision Map fit-score request body. thesis is the same
    # shape the Pilot accepts via thesis_context; top_n is capped server-
    # side in compute_fit_score (1..50).
    # SPEC_088 — constraints is a list of hard filters applied AFTER the
    # weighted score: [{dimension, value, label?}, ...]. See
    # _CONSTRAINT_DEFS in fit_score.py for recognized dimensions.
    thesis: Optional[Dict[str, Any]] = None
    top_n: Optional[int] = 10
    constraints: Optional[List[Dict[str, Any]]] = None
    # SPEC_097 — Pilot can set weights via set_fit_weights tool.
    weights_override: Optional[Dict[str, Any]] = None


@router.post("/fit-score")
def atlas_fit_score(body: FitScoreBody, db: Session = Depends(get_db)):
    """SPEC_087/088 — per-geo thesis fit scores 0-100 + weights + top-N,
    optionally filtered by a constraint list (HHI ≥ X, exclude high-NRI…).
    Land-page replacement for the old default-layer choropleth."""
    from app.services.atlas.fit_score import compute_fit_score
    return compute_fit_score(db, thesis=body.thesis,
                              top_n=body.top_n or 10,
                              constraints=body.constraints,
                              weights_override=body.weights_override)


class PlanBody(BaseModel):
    # SPEC_094 — Planner request. Either thesis_context or prompt (or
    # both) provides the LLM with enough context to author a plan; if
    # neither is set, the planner returns a generic site-selection plan.
    thesis_context: Optional[Dict[str, Any]] = None
    prompt: Optional[str] = Field(None, max_length=2000)


@router.post("/plan")
def atlas_plan(body: PlanBody, db: Session = Depends(get_db)):
    """SPEC_094 — return a Plan (4-8 beat walkthrough) the frontend
    executor can run for any thesis. Always 200; on any failure path
    the response includes a frozen fallback plan + an error string."""
    from app.services.atlas.planner import generate_plan
    plan, err, cache_hit = generate_plan(db, body.thesis_context, body.prompt)
    return {"plan": plan, "error": err, "cache_hit": cache_hit}


class ExplainBody(BaseModel):
    # SPEC_093 — chain-of-thought streaming for the storytelling demo.
    # Lightweight LLM call: 2-3 sentence rationale for a single beat.
    prompt: str = Field(..., min_length=1, max_length=2000)
    thesis_context: Optional[Dict[str, Any]] = None
    max_tokens: Optional[int] = 200


@router.post("/explain")
def atlas_explain(body: ExplainBody, db: Session = Depends(get_db)):
    """SPEC_093 — stream a 2-3 sentence rationale for a demo beat.
    No tools, no UI actions — narration delta events only."""
    from fastapi.responses import StreamingResponse
    from app.services.atlas.pilot import run_explain_streaming
    return StreamingResponse(
        run_explain_streaming(
            body.prompt, body.thesis_context,
            body.max_tokens or 200,
        ),
        media_type="application/x-ndjson",
        headers={"X-Accel-Buffering": "no", "Cache-Control": "no-cache"},
    )


class CompetitionBody(BaseModel):
    # SPEC_091 — Yelp-backed in-radius competition lookup. Used by the
    # Trade Area panel and the Pilot's find_competition tool. radius_mi
    # is capped server-side (Yelp's hard 40 km / ~25 mi limit).
    lat: float
    lon: float
    radius_mi: Optional[float] = 5.0
    term: Optional[str] = None
    categories: Optional[str] = None
    limit: Optional[int] = 20


@router.post("/competition")
def atlas_competition(body: CompetitionBody, db: Session = Depends(get_db)):
    """SPEC_091 — return competing businesses within radius (Yelp Fusion).
    Soft-fails (200 with error string + empty list) when Yelp is
    unconfigured or unavailable, so the trade-area UI keeps working."""
    from app.services.atlas.competition import find_competition
    return find_competition(
        lat=body.lat, lon=body.lon,
        radius_mi=body.radius_mi,
        term=body.term, categories=body.categories,
        limit=body.limit,
    )


class TradeAreaBody(BaseModel):
    # SPEC_089 — trade-area request: focal county geo_id + radius in miles
    # (clamped 1..250 server-side).
    geo_id: str = Field(..., min_length=2, max_length=12)
    radius_mi: Optional[float] = 50.0


@router.post("/trade-area")
def atlas_trade_area(body: TradeAreaBody, db: Session = Depends(get_db)):
    """SPEC_089 — return focal county stats + neighbour counties within
    `radius_mi` miles (haversine on county centroids) + an aggregate
    summary. Used by the Decision Map trade-area mode."""
    from app.services.atlas.trade_area import compute_trade_area
    return compute_trade_area(db, body.geo_id, body.radius_mi)


class PilotBody(BaseModel):
    question: str = Field(..., min_length=1, max_length=2000)
    session_id: Optional[str] = None
    history: Optional[List[Dict[str, Any]]] = None   # SPEC_081 — prior turns
    thesis_context: Optional[Dict[str, Any]] = None  # SPEC_082 — user thesis
    # SPEC_095 — Decision Map snapshot (chips, top pins, trade area, map
    # view). Injected at the very front of the system prompt so the agent
    # sees what the user is looking at before it speaks.
    session_state: Optional[Dict[str, Any]] = None


@router.post("/pilot")
def atlas_pilot_endpoint(body: PilotBody, db: Session = Depends(get_db)):
    """Atlas Pilot v0 — LLM agent that answers questions by calling
    constrained tools over the governed Atlas data corpus. Non-streaming;
    client waits for full transcript. SPEC_078."""
    from app.services.atlas.pilot import run_pilot
    result = run_pilot(db, question=body.question, session_id=body.session_id,
                        history=body.history,
                        thesis_context=body.thesis_context,
                        session_state=body.session_state)
    return result


@router.post("/pilot/stream")
def atlas_pilot_stream_endpoint(body: PilotBody, db: Session = Depends(get_db)):
    """Atlas Pilot v1 — streaming variant. Yields one NDJSON event per
    state transition (plan_started, thinking, tool_call_started,
    tool_call_completed, ui_action_queued, narration, done, error).
    Frontend reads with fetch streaming and renders progressively.
    SPEC_079."""
    from fastapi.responses import StreamingResponse
    from app.services.atlas.pilot import run_pilot_streaming
    return StreamingResponse(
        run_pilot_streaming(db, question=body.question,
                             session_id=body.session_id,
                             history=body.history,
                             thesis_context=body.thesis_context,
                             session_state=body.session_state),
        media_type="application/x-ndjson",
        headers={"X-Accel-Buffering": "no", "Cache-Control": "no-cache"},
    )


@router.get("/recent")
def get_recent_events_endpoint(
    sources: str = "fema",
    limit: int = 50,
    db: Session = Depends(get_db),
):
    """Merged most-recent event stream for the Atlas Recent Activity
    feed. `sources` is comma-separated; v1 supports only `fema` (SEC
    + USAspending lanes wait for PLAN_067 SPEC_074 / SPEC_071-stretch)."""
    src_list = [s.strip() for s in sources.split(",") if s.strip()]
    try:
        items = series_mod.fetch_recent_events(db, src_list, limit=limit)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return {"items": items, "sources": src_list, "limit": limit, "count": len(items)}


@router.get("/migration")
def get_migration_flows_endpoint(
    top_n: int = 100,
    flow_type: str = "inflow",
    db: Session = Depends(get_db),
):
    """Top-N county-to-county IRS migration flows for the latest tax year.
    Drives the animated arc layer in SPEC_066b. Frontend joins origin/dest
    FIPS to its already-loaded boundary centroids — no centroids in the
    payload."""
    try:
        return series_mod.fetch_top_migration_flows(db, top_n=top_n, flow_type=flow_type)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
