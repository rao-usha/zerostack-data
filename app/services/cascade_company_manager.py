"""
Cascade Company Manager — PLAN_058 Phase 1.

Dynamically adds/removes companies from the macro causal graph.
When a company is added:
  1. Creates a MacroNode (node_type=company)
  2. Auto-detects macro linkages via sector/industry mapping
  3. Creates CausalEdges from relevant macro nodes to the company
  4. Optionally runs MacroSensitivityAgent for 10-K–based linkages (async, slower)

Companies can be found from:
  - pe_portfolio_companies (1,082 with tickers)
  - industrial_companies (61 with tickers)
  - Manual entry (any ticker or name)
"""
from __future__ import annotations
import logging
from typing import Dict, List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import text

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Sector → macro node mappings (which FRED series affect which sectors)
# Used for instant linkage when adding a company (no EDGAR call needed)
# ---------------------------------------------------------------------------

SECTOR_MACRO_LINKS = {
    "housing": [
        {"series_id": "HOUST", "elasticity": 0.80, "direction": "positive", "lag": 6, "mechanism": "Housing activity directly drives company revenue"},
        {"series_id": "MORTGAGE30US", "elasticity": -0.50, "direction": "negative", "lag": 4, "mechanism": "Higher mortgage rates reduce housing demand"},
        {"series_id": "HSN1F", "elasticity": 0.65, "direction": "positive", "lag": 3, "mechanism": "New home sales drive building materials/services demand"},
    ],
    "real_estate": [
        {"series_id": "MORTGAGE30US", "elasticity": -0.60, "direction": "negative", "lag": 3, "mechanism": "Mortgage rates directly affect real estate activity"},
        {"series_id": "HOUST", "elasticity": 0.70, "direction": "positive", "lag": 4, "mechanism": "Housing starts drive construction-adjacent demand"},
        {"series_id": "DFF", "elasticity": -0.40, "direction": "negative", "lag": 2, "mechanism": "Rate environment affects property valuations"},
    ],
    "consumer": [
        {"series_id": "UMCSENT", "elasticity": 0.55, "direction": "positive", "lag": 2, "mechanism": "Consumer sentiment drives discretionary spending"},
        {"series_id": "UNRATE", "elasticity": -0.40, "direction": "negative", "lag": 3, "mechanism": "Unemployment reduces consumer purchasing power"},
    ],
    "technology": [
        {"series_id": "DFF", "elasticity": -0.50, "direction": "negative", "lag": 3, "mechanism": "Higher rates increase discount rates on future cash flows, compressing tech valuations"},
        {"series_id": "DGS10", "elasticity": -0.35, "direction": "negative", "lag": 2, "mechanism": "Long-term rates affect growth stock valuations"},
    ],
    "energy": [
        {"series_id": "DCOILWTICO", "elasticity": 0.80, "direction": "positive", "lag": 1, "mechanism": "Oil prices directly drive energy company revenue"},
    ],
    "industrials": [
        {"series_id": "HOUST", "elasticity": 0.50, "direction": "positive", "lag": 4, "mechanism": "Construction activity drives industrial materials demand"},
        {"series_id": "DCOILWTICO", "elasticity": -0.30, "direction": "negative", "lag": 2, "mechanism": "Energy costs compress industrial margins"},
    ],
    "financial": [
        {"series_id": "DFF", "elasticity": 0.60, "direction": "positive", "lag": 1, "mechanism": "Higher rates improve net interest margins for banks"},
        {"series_id": "DGS10", "elasticity": 0.40, "direction": "positive", "lag": 1, "mechanism": "Long-term rates drive lending profitability"},
    ],
    "healthcare": [
        {"series_id": "UNRATE", "elasticity": -0.20, "direction": "negative", "lag": 6, "mechanism": "Unemployment reduces employer-sponsored insurance coverage"},
    ],
    "logistics": [
        {"series_id": "DCOILWTICO", "elasticity": -0.50, "direction": "negative", "lag": 1, "mechanism": "Fuel costs directly hit transportation margins"},
    ],
}

# Industry keywords → sector (reused from portfolio_stress_scorer pattern)
INDUSTRY_TO_SECTOR = {
    "home": "housing", "build": "housing", "lumber": "housing", "paint": "housing",
    "construction": "housing", "roofing": "housing", "flooring": "housing",
    "real estate": "real_estate", "reit": "real_estate", "property": "real_estate",
    "mortgage": "real_estate",
    "retail": "consumer", "restaurant": "consumer", "food": "consumer",
    "consumer": "consumer", "apparel": "consumer", "leisure": "consumer",
    "software": "technology", "tech": "technology", "saas": "technology",
    "cloud": "technology", "data": "technology", "cyber": "technology",
    "oil": "energy", "gas": "energy", "energy": "energy", "petroleum": "energy",
    "solar": "energy", "wind": "energy", "utility": "energy",
    "industrial": "industrials", "manufacturing": "industrials", "chemical": "industrials",
    "aerospace": "industrials", "defense": "industrials", "equipment": "industrials",
    "bank": "financial", "insurance": "financial", "credit": "financial",
    "financial": "financial", "fintech": "financial",
    "biotech": "healthcare", "healthcare": "healthcare", "pharma": "healthcare",
    "medical": "healthcare", "hospital": "healthcare",
    "logistics": "logistics", "freight": "logistics", "transport": "logistics",
    "shipping": "logistics", "trucking": "logistics",
}


def _detect_sector(industry: str, name: str) -> str:
    """Detect sector from industry text or company name."""
    combined = f"{industry or ''} {name or ''}".lower()
    for keyword, sector in INDUSTRY_TO_SECTOR.items():
        if keyword in combined:
            return sector
    return "industrials"  # default


def _safe_query(db: Session, sql: str, params: dict):
    try:
        return db.execute(text(sql), params).fetchall()
    except Exception as exc:
        try:
            db.rollback()
        except Exception:
            pass
        logger.debug("Cascade company query failed: %s", exc)
        return []


class CascadeCompanyManager:

    def __init__(self, db: Session):
        self.db = db

    def add_company(
        self,
        name: str,
        ticker: Optional[str] = None,
        industry: Optional[str] = None,
        cik: Optional[int] = None,
    ) -> Dict:
        """
        Add a company to the macro causal graph.

        1. Creates MacroNode (or returns existing)
        2. Detects sector from industry/name
        3. Creates CausalEdges from relevant macro nodes
        4. Returns node + edges created
        """
        from app.core.macro_models import MacroNode, CausalEdge

        # Check if already exists
        if ticker:
            existing = _safe_query(self.db, """
                SELECT id, name, ticker, sector_tag FROM macro_nodes
                WHERE ticker = :ticker AND node_type = 'company'
            """, {"ticker": ticker.upper()})
            if existing:
                edges = _safe_query(self.db, """
                    SELECT ce.id, mn.name as source_name, ce.elasticity,
                           ce.relationship_direction, ce.typical_lag_months
                    FROM causal_edges ce
                    JOIN macro_nodes mn ON mn.id = ce.source_node_id
                    WHERE ce.target_node_id = :nid AND ce.is_active = true
                """, {"nid": existing[0][0]})
                return {
                    "status": "already_exists",
                    "node_id": existing[0][0],
                    "name": existing[0][1],
                    "ticker": existing[0][2],
                    "sector": existing[0][3],
                    "edges": len(edges),
                }

        # Detect sector
        sector = _detect_sector(industry or "", name)

        # Look up company info from our tables if not provided
        if not industry and ticker:
            co_row = _safe_query(self.db, """
                SELECT industry FROM pe_portfolio_companies WHERE ticker = :t LIMIT 1
            """, {"t": ticker.upper()})
            if co_row and co_row[0][0]:
                industry = co_row[0][0]
                sector = _detect_sector(industry, name)

            # Also try industrial_companies
            if not industry:
                ic_row = _safe_query(self.db, """
                    SELECT industry_segment FROM industrial_companies WHERE ticker = :t LIMIT 1
                """, {"t": ticker.upper()})
                if ic_row and ic_row[0][0]:
                    industry = ic_row[0][0]
                    sector = _detect_sector(industry, name)

        # Create MacroNode
        node = MacroNode(
            name=f"{name} Revenue",
            node_type="company",
            description=f"{name} ({ticker or 'private'}) — {industry or sector} sector",
            ticker=ticker.upper() if ticker else None,
            sector_tag=sector,
            frequency="quarterly",
            is_lagging=True,
        )
        self.db.add(node)
        self.db.flush()  # get ID
        node_id = node.id

        # Create CausalEdges based on sector
        sector_links = SECTOR_MACRO_LINKS.get(sector, SECTOR_MACRO_LINKS.get("industrials", []))
        edges_created = []

        for link in sector_links:
            # Find the source macro node by series_id
            source_rows = _safe_query(self.db, """
                SELECT id, name FROM macro_nodes
                WHERE series_id = :sid LIMIT 1
            """, {"sid": link["series_id"]})

            if not source_rows:
                continue

            source_id = source_rows[0][0]
            source_name = source_rows[0][1]

            # Check edge doesn't already exist
            existing_edge = _safe_query(self.db, """
                SELECT id FROM causal_edges
                WHERE source_node_id = :src AND target_node_id = :tgt
            """, {"src": source_id, "tgt": node_id})

            if existing_edge:
                continue

            edge = CausalEdge(
                source_node_id=source_id,
                target_node_id=node_id,
                relationship_direction=link["direction"],
                elasticity=link["elasticity"] if link["direction"] == "positive" else -abs(link["elasticity"]),
                typical_lag_months=link["lag"],
                lag_min_months=max(1, link["lag"] - 1),
                lag_max_months=link["lag"] + 2,
                confidence=0.70,
                mechanism_description=link["mechanism"],
                is_active=True,
            )
            self.db.add(edge)
            edges_created.append({
                "source": source_name,
                "source_id": source_id,
                "elasticity": edge.elasticity,
                "direction": link["direction"],
                "lag_months": link["lag"],
                "mechanism": link["mechanism"],
            })

        self.db.commit()

        return {
            "status": "created",
            "node_id": node_id,
            "name": node.name,
            "ticker": node.ticker,
            "sector": sector,
            "industry": industry,
            "edges_created": len(edges_created),
            "edges": edges_created,
        }

    def remove_company(self, node_id: int) -> Dict:
        """Remove a company node and its edges from the graph."""
        # Get node info first
        node_rows = _safe_query(self.db, """
            SELECT name, ticker FROM macro_nodes WHERE id = :nid AND node_type = 'company'
        """, {"nid": node_id})

        if not node_rows:
            return {"status": "not_found", "node_id": node_id}

        name, ticker = node_rows[0]

        # Deactivate edges (causal_edges has is_active)
        self.db.execute(text("""
            UPDATE causal_edges SET is_active = false
            WHERE target_node_id = :nid OR source_node_id = :nid
        """), {"nid": node_id})

        # Delete the node (macro_nodes has no is_active — hard delete)
        self.db.execute(text("""
            DELETE FROM macro_nodes WHERE id = :nid AND node_type = 'company'
        """), {"nid": node_id})
        self.db.commit()

        return {"status": "removed", "node_id": node_id, "name": name, "ticker": ticker}

    def list_addable_companies(self, search: Optional[str] = None, limit: int = 20) -> List[Dict]:
        """List companies from our data store that can be added to the graph."""
        params: dict = {"limit": limit}
        where = ""
        if search:
            where = "AND (name ILIKE :q OR ticker ILIKE :q)"
            params["q"] = f"%{search}%"

        # PE portfolio companies with tickers
        pe_rows = _safe_query(self.db, f"""
            SELECT ticker, name, industry, 'pe_portfolio' as source
            FROM pe_portfolio_companies
            WHERE ticker IS NOT NULL AND ticker != '' {where}
            ORDER BY name
            LIMIT :limit
        """, params)

        # Industrial companies with tickers
        ic_rows = _safe_query(self.db, f"""
            SELECT ticker, name, industry_segment as industry, 'industrial' as source
            FROM industrial_companies
            WHERE ticker IS NOT NULL AND ticker != '' {where}
            ORDER BY name
            LIMIT :limit
        """, params)

        # Already in graph
        in_graph = set()
        graph_rows = _safe_query(self.db, """
            SELECT ticker FROM macro_nodes WHERE node_type = 'company' AND is_active = true
        """, {})
        for r in graph_rows:
            if r[0]:
                in_graph.add(r[0].upper())

        results = []
        seen = set()
        for r in pe_rows + ic_rows:
            ticker = r[0].upper() if r[0] else None
            if not ticker or ticker in seen:
                continue
            seen.add(ticker)
            results.append({
                "ticker": ticker,
                "name": r[1],
                "industry": r[2],
                "source": r[3],
                "in_graph": ticker in in_graph,
            })

        results.sort(key=lambda x: x["name"])
        return results[:limit]
