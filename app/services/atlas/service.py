"""
Atlas orchestrator — SPEC_064.

`AtlasService.explore(...)` is the single public entry point: it ties
resolver → cards → graph → telemetry into one exploration, persists it, and
returns the `Exploration` object.

The report-first product (SPEC_061-063) is intentionally NOT referenced as a
CTA here — `market_intelligence_pack` is reachable only as a downstream
"export this exploration" action, surfaced by the API/frontend, not by this
service.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from app.services.atlas import cards as cards_mod
from app.services.atlas import graph as graph_mod
from app.services.atlas import resolver as resolver_mod
from app.services.atlas.telemetry import AtlasTelemetry
from app.services.atlas.types import Exploration
from app.services.diligence.taxonomies import load_msa, load_naics, naics_parents

logger = logging.getLogger(__name__)


class AtlasService:
    """Public-data exploration orchestrator."""

    def __init__(self, db: Session):
        self.db = db
        self.telemetry = AtlasTelemetry(db)

    # ── explore ──────────────────────────────────────────────────────────────

    def explore(
        self,
        query: str,
        msa: Optional[str] = None,
        naics: Optional[str] = None,
        session_id: Optional[str] = None,
        anon_ip: Optional[str] = None,
        persist: bool = True,
    ) -> Exploration:
        """Run a full exploration: resolve → cards → connections → persist.

        Raises ValueError if the query resolves to neither a market nor a
        sector (the resolver enforces this)."""
        # 1. Resolve entities + slug
        resolved, slug = resolver_mod.resolve(query, msa=msa, naics=naics)

        # 2. Build cards (skip-on-empty) + coverage notes
        card_list, coverage_notes, datasets = cards_mod.build_cards(self.db, resolved)
        resolved.datasets = datasets

        # 3. Connections between the cards that rendered
        connections = graph_mod.build_connections(card_list)

        # 4. Summary + related queries
        summary = self._build_summary(query, resolved, card_list, coverage_notes)
        related = self._related_queries(resolved)

        exploration = Exploration(
            slug=slug,
            query=query,
            resolved_entities=resolved,
            summary=summary,
            cards=card_list,
            connections=connections,
            related_queries=related,
            share_url=f"/atlas/{slug}",
        )

        # 5. Persist + telemetry (best-effort — never fail the exploration)
        if persist:
            try:
                exploration_id = self.telemetry.save_exploration(exploration.to_dict())
                exploration.id = exploration_id
                self.telemetry.record_query(query, exploration_id, session_id, anon_ip)
                self.telemetry.record_event(
                    "query_submitted", exploration_id, session_id,
                    payload={"query": query},
                )
                if resolved.msa or resolved.naics:
                    self.telemetry.record_event(
                        "entity_resolved", exploration_id, session_id,
                        payload={"msa": resolved.msa, "naics": resolved.naics},
                    )
            except Exception as exc:  # noqa: BLE001
                logger.warning("Atlas: persistence/telemetry failed: %s", exc)

        return exploration

    def get_exploration(self, slug_or_id: str) -> Optional[Dict[str, Any]]:
        """Load a stored exploration (for shareable URLs)."""
        return self.telemetry.get_exploration(slug_or_id)

    # ── summary ──────────────────────────────────────────────────────────────

    def _build_summary(self, query, resolved, card_list, coverage_notes) -> Dict[str, Any]:
        msa_title = (resolved.msa or {}).get("title", "")
        naics_label = (resolved.naics or {}).get("label", "")
        n = len(card_list)

        if resolved.msa and resolved.naics:
            headline = f"{naics_label} in {msa_title.split(',')[0]}"
        elif resolved.naics:
            headline = f"{naics_label} — national view"
        elif resolved.msa:
            headline = f"Market overview — {msa_title.split(',')[0]}"
        else:
            headline = query

        if n == 0:
            why = ("No insight cards had usable public data for this "
                   "sector × geography. Try a broader industry or a larger metro.")
        else:
            why = (f"{n} cross-dataset insight card{'s' if n != 1 else ''} assembled "
                   f"from governed public data — each traceable to source.")
        return {
            "headline": headline,
            "why_interesting": why,
            "card_count": n,
            "coverage_notes": coverage_notes,
        }

    # ── related queries ──────────────────────────────────────────────────────

    def _related_queries(self, resolved) -> List[Dict[str, str]]:
        """Suggest forked explorations — same sector elsewhere, sibling sectors."""
        related: List[Dict[str, str]] = []
        msa = resolved.msa or {}
        naics = resolved.naics or {}

        if naics and msa:
            # Same sector, a few comparison metros.
            msa_dict = load_msa()
            comp_metros = ["19100", "31080", "16980", "35620"]  # Dallas, LA, Chicago, NYC
            for code in comp_metros:
                rec = msa_dict.get(code)
                if rec and rec.cbsa_code != msa.get("code"):
                    city = rec.title.split(",")[0].split("-")[0]
                    related.append({
                        "query": f"{city} {naics['label']}",
                        "msa": rec.cbsa_code,
                        "naics": naics["code"],
                        "kind": "compare_metro",
                    })
                if len(related) >= 3:
                    break

        if naics and msa:
            # Parent sector, same metro — zoom out one NAICS level.
            naics_dict = load_naics()
            parents = naics_parents(naics["code"])
            # The 3-digit parent's first 4-digit sibling industry.
            if len(parents) >= 2:
                three_digit = parents[-1]
                siblings = sorted(
                    c for c, node in naics_dict.items()
                    if node.digits == 4 and c.startswith(three_digit)
                    and c != naics["code"]
                )
                if siblings:
                    sib = siblings[0]
                    city = msa.get("title", "").split(",")[0].split("-")[0]
                    related.append({
                        "query": f"{city} {naics_dict[sib].label}",
                        "msa": msa["code"],
                        "naics": sib,
                        "kind": "sibling_sector",
                    })
        return related
