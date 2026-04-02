"""
GP Pipeline API — Chain 3 of PLAN_052.

Endpoints for GP Pipeline Scores and LP→GP relationship graph analytics.
"""
from fastapi import APIRouter, Depends, Query
from typing import Optional
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.services.gp_pipeline_scorer import GPPipelineScorer
from app.services.lp_gp_graph import LPGPGraphBuilder

router = APIRouter(prefix="/pe/gp-pipeline", tags=["GP Pipeline (Chain 3)"])


@router.get("/scores")
def get_all_gp_scores(
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    """All GPs ranked by pipeline score. Shows LP base breadth, tier-1 concentration, re-up rates."""
    scorer = GPPipelineScorer(db)
    results = scorer.score_all_gps()
    return {
        "status": "ok",
        "total": len(results),
        "scores": [
            {
                "firm_id": r.firm_id,
                "firm_name": r.firm_name,
                "score": r.score,
                "grade": r.grade,
                "signal": r.signal,
                "lp_count": r.lp_count,
                "tier1_count": r.tier1_count,
                "total_committed_usd": r.total_committed_usd,
                "recommendation": r.recommendation,
            }
            for r in results[:limit]
        ],
    }


@router.get("/scores/{firm_id}")
def get_gp_score_detail(firm_id: int, db: Session = Depends(get_db)):
    """Detailed GP pipeline score with full signal breakdown and LP base listing."""
    scorer = GPPipelineScorer(db)
    r = scorer.score_gp(firm_id=firm_id)
    return {
        "status": "ok",
        "firm_id": r.firm_id,
        "firm_name": r.firm_name,
        "score": r.score,
        "grade": r.grade,
        "signal": r.signal,
        "recommendation": r.recommendation,
        "lp_count": r.lp_count,
        "tier1_count": r.tier1_count,
        "total_committed_usd": r.total_committed_usd,
        "signals": [
            {
                "signal": s.signal,
                "score": s.score,
                "weight": s.weight,
                "reading": s.reading,
                "details": s.details,
            }
            for s in r.signals
        ],
        "lp_base": [
            {
                "lp_id": lp.lp_id,
                "lp_name": lp.lp_name,
                "lp_type": lp.lp_type,
                "is_tier1": lp.is_tier1,
                "vintages_committed": lp.vintages_committed,
                "total_committed_usd": lp.total_committed_usd,
                "commitment_trend": lp.commitment_trend,
            }
            for lp in r.lp_base
        ],
    }


@router.get("/graph")
def get_full_graph(
    min_strength: int = Query(0, ge=0, le=100),
    db: Session = Depends(get_db),
):
    """Full LP→GP bipartite edge list with relationship strength scores."""
    builder = LPGPGraphBuilder(db)
    edges = builder.build_graph()
    filtered = [e for e in edges if e.relationship_strength >= min_strength]
    return {
        "status": "ok",
        "total_edges": len(filtered),
        "edges": [
            {
                "lp_id": e.lp_id,
                "lp_name": e.lp_name,
                "lp_type": e.lp_type,
                "gp_firm_id": e.gp_firm_id,
                "gp_name": e.gp_name,
                "vintages_together": e.vintages_together,
                "total_commitment_usd": e.total_commitment_usd,
                "commitment_trend": e.commitment_trend,
                "relationship_strength": e.relationship_strength,
            }
            for e in filtered
        ],
    }


@router.get("/graph/gp/{firm_id}")
def get_gp_network(firm_id: int, db: Session = Depends(get_db)):
    """LP network for a specific GP — all LPs committed to this GP."""
    builder = LPGPGraphBuilder(db)
    edges = builder.get_gp_edges(firm_id)
    centrality = builder.gp_centrality_ranking()
    rank = next((c for c in centrality if c.firm_id == firm_id), None)
    return {
        "status": "ok",
        "firm_id": firm_id,
        "lp_count": len(edges),
        "centrality_rank": rank.centrality_rank if rank else None,
        "avg_strength": rank.avg_strength if rank else None,
        "edges": [
            {
                "lp_id": e.lp_id,
                "lp_name": e.lp_name,
                "lp_type": e.lp_type,
                "vintages_together": e.vintages_together,
                "total_commitment_usd": e.total_commitment_usd,
                "commitment_trend": e.commitment_trend,
                "relationship_strength": e.relationship_strength,
            }
            for e in sorted(edges, key=lambda x: x.relationship_strength, reverse=True)
        ],
    }


@router.get("/graph/lp/{lp_id}")
def get_lp_network(lp_id: int, db: Session = Depends(get_db)):
    """GP network for a specific LP — all GPs this LP has committed to."""
    builder = LPGPGraphBuilder(db)
    edges = builder.get_lp_edges(lp_id)
    lp_name = edges[0].lp_name if edges else None
    return {
        "status": "ok",
        "lp_id": lp_id,
        "lp_name": lp_name,
        "gp_count": len(edges),
        "edges": [
            {
                "gp_firm_id": e.gp_firm_id,
                "gp_name": e.gp_name,
                "vintages_together": e.vintages_together,
                "total_commitment_usd": e.total_commitment_usd,
                "commitment_trend": e.commitment_trend,
                "relationship_strength": e.relationship_strength,
            }
            for e in sorted(edges, key=lambda x: x.relationship_strength, reverse=True)
        ],
    }


@router.get("/overlap/{firm_id}")
def get_lp_overlap(firm_id: int, db: Session = Depends(get_db)):
    """Which GPs share LPs with this GP? Shows co-investor LP overlap."""
    builder = LPGPGraphBuilder(db)
    overlaps = builder.lp_overlap(firm_id)

    # Get the firm name
    gp_edges = builder.get_gp_edges(firm_id)
    firm_name = gp_edges[0].gp_name if gp_edges else f"Firm #{firm_id}"
    lp_count = len(set(e.lp_id for e in gp_edges))

    return {
        "status": "ok",
        "firm_id": firm_id,
        "firm_name": firm_name,
        "lp_count": lp_count,
        "overlapping_gps": len(overlaps),
        "overlaps": [
            {
                "firm_id": o.other_firm_id,
                "firm_name": o.other_firm_name,
                "shared_lp_count": o.shared_lp_count,
                "overlap_pct": o.overlap_pct,
                "shared_lps": o.shared_lps,
            }
            for o in overlaps
        ],
    }
