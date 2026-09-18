"""
CIK <-> CRD bridge (SPEC_116), ported from wildcard-workbench.

Three tiers, strict precedence. A CIK is accepted only when the winning tier
names exactly one CRD and no other tier disagrees; anything else is refused
and recorded with its candidates, never guessed.

  1 cover_page_crd    — the filer's own 13F cover page (self-reported)
  2 other_manager_crd — a CRD listed by another manager on the same filing
  3 name_state        — normalized name + state, and ONLY when the name is
                        unique on both sides (5,096 normalized names in
                        sec_filers are shared by several CIKs)

`crd_cik_count` records how many CIKs claim the same CRD: the relation really
is many-to-many (92 CRDs map to more than one CIK), so this is a warning
column, never a foreign key. The resolver consumes tiers 1-2 only.
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from sqlalchemy import text

from app.entities import norm
from app.entities.resolve_core import _cik, _crd

logger = logging.getLogger(__name__)

BUILDER_VERSION = "bridge_v1"
TIER_RANK = {"cover_page_crd": 1, "other_manager_crd": 2, "name_state": 3}
EVIDENCE_LIST_CAP = 25


@dataclass
class _Cand:
    cik: str
    crd: str
    tier: str
    matched_on: str
    evidence: dict = field(default_factory=dict)

    @property
    def rank(self) -> int:
        return TIER_RANK[self.tier]


def _tier_cover_page(conn) -> List[_Cand]:
    rows = conn.execute(
        text(
            """
            SELECT cik, crd_number, MAX(filing_date) AS last_filed, COUNT(*) AS filings
            FROM sec_13f_filings
            WHERE cik IS NOT NULL AND crd_number IS NOT NULL
            GROUP BY cik, crd_number
            """
        )
    ).mappings()
    out = []
    for r in rows:
        cik, crd = _cik(r["cik"]), _crd(r["crd_number"])
        if cik and crd:
            out.append(
                _Cand(cik, crd, "cover_page_crd", "13f_coverpage",
                      {"filings": r["filings"], "last_filed": str(r["last_filed"])})
            )
    return out


def _tier_other_manager(conn) -> List[_Cand]:
    rows = conn.execute(
        text(
            """
            SELECT f.cik, m.crd_number, COUNT(*) AS mentions
            FROM sec_13f_other_managers m
            JOIN sec_13f_filings f ON f.accession_number = m.accession_number
            WHERE m.cik IS NOT NULL AND m.crd_number IS NOT NULL
            GROUP BY f.cik, m.crd_number
            """
        )
    ).mappings()
    out = []
    for r in rows:
        cik, crd = _cik(r["cik"]), _crd(r["crd_number"])
        if cik and crd:
            out.append(
                _Cand(cik, crd, "other_manager_crd", "13f_othermanager",
                      {"mentions": r["mentions"]})
            )
    return out


def _tier_name_state(conn, already: set) -> List[_Cand]:
    """Name+state, unique on both sides, for CIKs no identifier tier reached."""
    adv_rows = conn.execute(
        text(
            """
            SELECT DISTINCT ON (crd_number) crd_number, legal_name, business_name, main_office_state
            FROM sec_adv_roster_snapshots
            WHERE crd_number IS NOT NULL
            ORDER BY crd_number, roster_date DESC
            """
        )
    ).mappings()
    adv_index: Dict[Tuple[str, str], List[str]] = defaultdict(list)
    for r in adv_rows:
        crd = _crd(r["crd_number"])
        state = norm.state2(r["main_office_state"])
        if not crd or not state:
            continue
        for raw_name in (r["legal_name"], r["business_name"]):
            key_name = norm.norm(raw_name)
            if key_name:
                adv_index[(key_name, state)].append(crd)

    filer_rows = conn.execute(
        text(
            """
            SELECT f.cik, f.name, COALESCE(f.state_of_incorporation, f.biz_state2) AS state
            FROM sec_filers f
            WHERE f.name IS NOT NULL
              AND f.cik IN (SELECT cik FROM sec_13f_filings)
            """
        )
    ).mappings()
    by_name: Dict[Tuple[str, str], List[str]] = defaultdict(list)
    for r in filer_rows:
        cik = _cik(r["cik"])
        state = norm.state2(r["state"])
        key_name = norm.norm(r["name"])
        if cik and state and key_name and cik not in already:
            by_name[(key_name, state)].append(cik)

    out = []
    for key, ciks in by_name.items():
        crds = {c for c in adv_index.get(key, [])}
        # unique on BOTH sides or nothing: one name shared by two CIKs is not
        # evidence, it is a collision
        if len(ciks) == 1 and len(crds) == 1:
            out.append(
                _Cand(ciks[0], next(iter(crds)), "name_state", "name_state",
                      {"name_norm": key[0], "state": key[1],
                       "name_norm_version": norm.NAME_NORM_VERSION})
            )
    return out


def _resolve(cands: List[_Cand]) -> Tuple[Optional[_Cand], Optional[str]]:
    """Winning candidate for one CIK, or (None, refusal reason)."""
    best_rank = min(c.rank for c in cands)
    winners = [c for c in cands if c.rank == best_rank]
    if len({c.crd for c in winners}) > 1:
        return None, "ambiguous_within_tier"
    winner = sorted(winners, key=lambda c: (c.crd, c.matched_on))[0]
    if any(c.crd != winner.crd for c in cands):
        return None, "ambiguous_cross_tier"
    return winner, None


def build(conn, include_name_tier: bool = True) -> Dict[str, int]:
    """Rebuild core.cik_crd_bridge. Full replace inside the caller's transaction."""
    by_cik: Dict[str, List[_Cand]] = defaultdict(list)
    for cand in _tier_cover_page(conn) + _tier_other_manager(conn):
        by_cik[cand.cik].append(cand)
    identifier_ciks = set(by_cik)

    if include_name_tier:
        for cand in _tier_name_state(conn, identifier_ciks):
            by_cik[cand.cik].append(cand)

    accepted: List[_Cand] = []
    refused: List[Tuple[str, str, List[_Cand]]] = []
    for cik, cands in by_cik.items():
        winner, reason = _resolve(cands)
        if winner is None:
            refused.append((cik, reason, cands))
        else:
            accepted.append(winner)

    crd_counts: Dict[str, int] = defaultdict(int)
    for cand in accepted:
        crd_counts[cand.crd] += 1

    conn.execute(text("DELETE FROM core.cik_crd_bridge"))
    conn.execute(text("DELETE FROM core.cik_crd_bridge_refused"))
    for cand in accepted:
        conn.execute(
            text(
                """
                INSERT INTO core.cik_crd_bridge
                    (cik, crd, tier, tier_rank, matched_on, evidence, crd_cik_count, builder_version)
                VALUES (:cik, :crd, :tier, :rank, :matched_on, CAST(:evidence AS JSONB),
                        :crd_cik_count, :version)
                """
            ),
            {
                "cik": cand.cik, "crd": cand.crd, "tier": cand.tier, "rank": cand.rank,
                "matched_on": cand.matched_on, "evidence": json.dumps(cand.evidence),
                "crd_cik_count": crd_counts[cand.crd], "version": BUILDER_VERSION,
            },
        )
    for cik, reason, cands in refused:
        conn.execute(
            text(
                """
                INSERT INTO core.cik_crd_bridge_refused (cik, reason, tier, candidates)
                VALUES (:cik, :reason, :tier, CAST(:candidates AS JSONB))
                """
            ),
            {
                "cik": cik, "reason": reason, "tier": sorted({c.tier for c in cands})[0],
                "candidates": json.dumps(
                    [{"crd": c.crd, "tier": c.tier, "matched_on": c.matched_on}
                     for c in cands[:EVIDENCE_LIST_CAP]]
                ),
            },
        )

    stats = {
        "accepted": len(accepted),
        "refused": len(refused),
        "cover_page_crd": sum(1 for c in accepted if c.tier == "cover_page_crd"),
        "other_manager_crd": sum(1 for c in accepted if c.tier == "other_manager_crd"),
        "name_state": sum(1 for c in accepted if c.tier == "name_state"),
        "crd_multi_cik": sum(1 for c in accepted if crd_counts[c.crd] > 1),
    }
    logger.info(f"[entities:bridge] {stats}")
    return stats
