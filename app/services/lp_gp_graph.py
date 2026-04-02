"""
LP→GP Relationship Graph Builder — Chain 3 of PLAN_052.

Constructs a bipartite network from lp_gp_relationships with edge weights
representing relationship strength. Provides graph analytics: centrality,
LP overlap between GPs, and cluster detection.
"""
from __future__ import annotations
import logging
import math
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set
from sqlalchemy.orm import Session
from sqlalchemy import text

logger = logging.getLogger(__name__)


@dataclass
class GraphEdge:
    lp_id: int
    lp_name: str
    lp_type: str
    gp_firm_id: int
    gp_name: str
    vintages_together: int
    total_commitment_usd: float
    commitment_trend: str
    relationship_strength: int  # 0-100


@dataclass
class GPCentrality:
    firm_id: int
    firm_name: str
    lp_count: int
    total_commitment_usd: float
    avg_strength: float
    centrality_rank: int


@dataclass
class LPOverlap:
    other_firm_id: int
    other_firm_name: str
    shared_lp_count: int
    shared_lps: List[str]
    overlap_pct: float  # % of target GP's LPs that also commit to the other GP


def _safe_query(db: Session, sql: str, params: dict):
    try:
        result = db.execute(text(sql), params)
        return result.fetchall()
    except Exception as exc:
        try:
            db.rollback()
        except Exception:
            pass
        logger.debug("Graph query failed: %s", exc)
        return []


def _compute_strength(vintages: int, commitment_usd: float, trend: str) -> int:
    """Compute relationship strength 0-100."""
    # Vintage component: each re-up adds 30 points (capped at 60)
    vintage_score = min(vintages * 30, 60)

    # Capital component: log10 of commitment USD * 10 (capped at 30)
    if commitment_usd and commitment_usd > 0:
        capital_score = min(math.log10(commitment_usd) * 4, 30)
    else:
        capital_score = 0

    # Trend bonus
    trend_bonus = {"growing": 15, "stable": 5, "declining": -10, "new": 0}.get(trend or "new", 0)

    return max(0, min(100, int(vintage_score + capital_score + trend_bonus)))


class LPGPGraphBuilder:

    def __init__(self, db: Session):
        self.db = db
        self._edges: Optional[List[GraphEdge]] = None

    def build_graph(self) -> List[GraphEdge]:
        """Build full bipartite graph from lp_gp_relationships."""
        if self._edges is not None:
            return self._edges

        rows = _safe_query(self.db, """
            SELECT r.lp_id, l.name as lp_name, l.lp_type,
                   r.gp_firm_id, r.gp_name,
                   r.total_vintages_committed, r.total_committed_usd,
                   r.commitment_trend
            FROM lp_gp_relationships r
            JOIN lp_fund l ON l.id = r.lp_id
            WHERE r.gp_firm_id IS NOT NULL
            ORDER BY r.total_committed_usd DESC NULLS LAST
        """, {})

        self._edges = []
        for r in rows:
            vintages = int(r[5] or 1)
            commitment = float(r[6] or 0)
            trend = r[7] or "new"
            strength = _compute_strength(vintages, commitment, trend)

            self._edges.append(GraphEdge(
                lp_id=r[0], lp_name=r[1], lp_type=r[2] or "other",
                gp_firm_id=r[3], gp_name=r[4],
                vintages_together=vintages,
                total_commitment_usd=commitment,
                commitment_trend=trend,
                relationship_strength=strength,
            ))

        return self._edges

    def get_gp_edges(self, firm_id: int) -> List[GraphEdge]:
        """Get all LP connections for a specific GP."""
        edges = self.build_graph()
        return [e for e in edges if e.gp_firm_id == firm_id]

    def get_lp_edges(self, lp_id: int) -> List[GraphEdge]:
        """Get all GP connections for a specific LP."""
        edges = self.build_graph()
        return [e for e in edges if e.lp_id == lp_id]

    def gp_centrality_ranking(self) -> List[GPCentrality]:
        """Rank GPs by their LP network centrality (connection count + strength)."""
        edges = self.build_graph()

        gp_stats: Dict[int, dict] = {}
        for e in edges:
            fid = e.gp_firm_id
            if fid not in gp_stats:
                gp_stats[fid] = {
                    "firm_name": e.gp_name,
                    "lp_ids": set(),
                    "total_usd": 0.0,
                    "strengths": [],
                }
            gp_stats[fid]["lp_ids"].add(e.lp_id)
            gp_stats[fid]["total_usd"] += e.total_commitment_usd
            gp_stats[fid]["strengths"].append(e.relationship_strength)

        rankings = []
        for fid, stats in gp_stats.items():
            avg_str = sum(stats["strengths"]) / len(stats["strengths"]) if stats["strengths"] else 0
            rankings.append(GPCentrality(
                firm_id=fid, firm_name=stats["firm_name"],
                lp_count=len(stats["lp_ids"]),
                total_commitment_usd=stats["total_usd"],
                avg_strength=round(avg_str, 1),
                centrality_rank=0,
            ))

        # Rank by composite: lp_count * avg_strength
        rankings.sort(key=lambda r: r.lp_count * r.avg_strength, reverse=True)
        for i, r in enumerate(rankings):
            r.centrality_rank = i + 1

        return rankings

    def lp_overlap(self, firm_id: int) -> List[LPOverlap]:
        """Find GPs that share LPs with the target GP. Returns sorted by overlap count."""
        edges = self.build_graph()

        # LP set for the target GP
        target_lps: Set[int] = set()
        target_lp_names: Dict[int, str] = {}
        for e in edges:
            if e.gp_firm_id == firm_id:
                target_lps.add(e.lp_id)
                target_lp_names[e.lp_id] = e.lp_name

        if not target_lps:
            return []

        # LP sets for all other GPs
        other_gps: Dict[int, dict] = {}
        for e in edges:
            if e.gp_firm_id == firm_id:
                continue
            fid = e.gp_firm_id
            if fid not in other_gps:
                other_gps[fid] = {"firm_name": e.gp_name, "lp_ids": set()}
            other_gps[fid]["lp_ids"].add(e.lp_id)

        overlaps = []
        for fid, info in other_gps.items():
            shared = target_lps & info["lp_ids"]
            if shared:
                overlaps.append(LPOverlap(
                    other_firm_id=fid,
                    other_firm_name=info["firm_name"],
                    shared_lp_count=len(shared),
                    shared_lps=[target_lp_names.get(lid, f"LP#{lid}") for lid in shared],
                    overlap_pct=round(len(shared) / len(target_lps) * 100, 1),
                ))

        overlaps.sort(key=lambda o: o.shared_lp_count, reverse=True)
        return overlaps
