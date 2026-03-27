"""
Macro Causal Cascade API — PLAN_035

Endpoints:
  GET   /macro/graph                      Full causal graph (nodes + edges)
  GET   /macro/nodes                      List all macro nodes
  POST  /macro/nodes                      Create a custom node
  GET   /macro/nodes/{id}/upstream        Nodes that causally affect this node
  GET   /macro/nodes/{id}/downstream      Nodes this node affects
  POST  /macro/simulate                   Run cascade simulation
  GET   /macro/scenarios                  List saved scenarios
  GET   /macro/scenarios/{id}/results     Cascade results for a scenario
  GET   /macro/company-impact/{ticker}    Macro drivers for a company
  GET   /macro/portfolio-impact           Macro exposure for all PE portfolio companies
  GET   /macro/current-environment        Current FRED values for key nodes
  POST  /macro/collect/edgar-facts        Trigger SEC EDGAR company facts ingestion
  POST  /macro/collect/seed-relationships Seed/re-seed known causal relationships
"""
import logging
from datetime import date
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select, desc
from sqlalchemy.orm import Session

from app.core.database import get_db

router = APIRouter(prefix="/macro", tags=["Macro Cascade"])
logger = logging.getLogger(__name__)


# =============================================================================
# REQUEST SCHEMAS
# =============================================================================


class SimulateRequest(BaseModel):
    node_id: int
    change_pct: float                     # +1.0 = +1%, -20.0 = -20%
    horizon_months: int = 24
    name: Optional[str] = None
    description: Optional[str] = None
    as_of_date: Optional[date] = None


class CreateNodeRequest(BaseModel):
    name: str
    node_type: str = "custom"             # 'fred_series','bls_series','sector','company','custom'
    description: Optional[str] = None
    unit: Optional[str] = None
    series_id: Optional[str] = None
    ticker: Optional[str] = None
    frequency: Optional[str] = None
    sector_tag: Optional[str] = None
    is_leading_indicator: bool = False
    is_coincident: bool = False
    is_lagging: bool = False


# =============================================================================
# GRAPH ENDPOINTS
# =============================================================================


@router.get("/graph")
async def get_causal_graph(db: Session = Depends(get_db)):
    """Return the full causal graph (all nodes + all active edges) for D3 visualization."""
    from app.core.macro_models import CausalEdge, MacroNode

    nodes = db.execute(select(MacroNode)).scalars().all()
    edges = db.execute(
        select(CausalEdge).where(CausalEdge.is_active == True)  # noqa: E712
    ).scalars().all()

    return {
        "nodes": [
            {
                "id": n.id,
                "name": n.name,
                "node_type": n.node_type,
                "series_id": n.series_id,
                "ticker": n.ticker,
                "sector_tag": n.sector_tag,
                "unit": n.unit,
                "frequency": n.frequency,
                "current_value": n.current_value,
                "current_value_date": n.current_value_date.isoformat() if n.current_value_date else None,
                "is_leading_indicator": n.is_leading_indicator,
                "is_coincident": n.is_coincident,
                "is_lagging": n.is_lagging,
                "description": n.description,
            }
            for n in nodes
        ],
        "edges": [
            {
                "id": e.id,
                "source": e.source_node_id,
                "target": e.target_node_id,
                "elasticity": e.elasticity,
                "relationship_direction": e.relationship_direction,
                "typical_lag_months": e.typical_lag_months,
                "lag_min_months": e.lag_min_months,
                "lag_max_months": e.lag_max_months,
                "confidence": e.confidence,
                "mechanism_description": e.mechanism_description,
            }
            for e in edges
        ],
        "node_count": len(nodes),
        "edge_count": len(edges),
    }


@router.get("/nodes")
async def list_nodes(
    sector_tag: Optional[str] = None,
    node_type: Optional[str] = None,
    db: Session = Depends(get_db),
):
    """List all macro nodes, optionally filtered by sector or type."""
    from app.core.macro_models import MacroNode

    stmt = select(MacroNode)
    if sector_tag:
        stmt = stmt.where(MacroNode.sector_tag == sector_tag)
    if node_type:
        stmt = stmt.where(MacroNode.node_type == node_type)
    stmt = stmt.order_by(MacroNode.sector_tag, MacroNode.display_order, MacroNode.name)

    nodes = db.execute(stmt).scalars().all()
    return {
        "nodes": [
            {
                "id": n.id,
                "name": n.name,
                "node_type": n.node_type,
                "series_id": n.series_id,
                "ticker": n.ticker,
                "sector_tag": n.sector_tag,
                "current_value": n.current_value,
                "current_value_date": n.current_value_date.isoformat() if n.current_value_date else None,
                "description": n.description,
            }
            for n in nodes
        ],
        "count": len(nodes),
    }


@router.post("/nodes", status_code=201)
async def create_node(req: CreateNodeRequest, db: Session = Depends(get_db)):
    """Create a custom macro node."""
    from app.core.macro_models import MacroNode

    node = MacroNode(
        name=req.name,
        node_type=req.node_type,
        description=req.description,
        unit=req.unit,
        series_id=req.series_id,
        ticker=req.ticker,
        frequency=req.frequency,
        sector_tag=req.sector_tag,
        is_leading_indicator=req.is_leading_indicator,
        is_coincident=req.is_coincident,
        is_lagging=req.is_lagging,
    )
    db.add(node)
    db.commit()
    db.refresh(node)
    return {"id": node.id, "name": node.name, "node_type": node.node_type}


@router.get("/nodes/{node_id}/upstream")
async def get_upstream_nodes(node_id: int, db: Session = Depends(get_db)):
    """Return all nodes that causally affect this node (direct + transitive)."""
    from app.core.macro_models import CausalEdge, MacroNode

    node = db.get(MacroNode, node_id)
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")

    # Load all active edges, build reverse adjacency, BFS upstream
    edges = db.execute(
        select(CausalEdge).where(CausalEdge.is_active == True)  # noqa: E712
    ).scalars().all()

    # incoming: target_id → list of source edges
    incoming: dict[int, list] = {}
    for e in edges:
        incoming.setdefault(e.target_node_id, []).append(e)

    all_nodes = {n.id: n for n in db.execute(select(MacroNode)).scalars().all()}

    visited = set()
    queue = [node_id]
    upstream = []

    while queue:
        current = queue.pop(0)
        for edge in incoming.get(current, []):
            src = edge.source_node_id
            if src not in visited:
                visited.add(src)
                src_node = all_nodes.get(src)
                if src_node:
                    upstream.append({
                        "id": src_node.id,
                        "name": src_node.name,
                        "sector_tag": src_node.sector_tag,
                        "elasticity": edge.elasticity,
                        "typical_lag_months": edge.typical_lag_months,
                        "confidence": edge.confidence,
                        "mechanism": edge.mechanism_description,
                    })
                queue.append(src)

    return {"node_id": node_id, "node_name": node.name, "upstream_count": len(upstream), "upstream": upstream}


@router.get("/nodes/{node_id}/downstream")
async def get_downstream_nodes(node_id: int, db: Session = Depends(get_db)):
    """Return all nodes causally affected by this node (direct + transitive)."""
    from app.core.macro_models import CausalEdge, MacroNode

    node = db.get(MacroNode, node_id)
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")

    edges = db.execute(
        select(CausalEdge).where(CausalEdge.is_active == True)  # noqa: E712
    ).scalars().all()

    outgoing: dict[int, list] = {}
    for e in edges:
        outgoing.setdefault(e.source_node_id, []).append(e)

    all_nodes = {n.id: n for n in db.execute(select(MacroNode)).scalars().all()}

    visited = set()
    queue = [node_id]
    downstream = []

    while queue:
        current = queue.pop(0)
        for edge in outgoing.get(current, []):
            tgt = edge.target_node_id
            if tgt not in visited:
                visited.add(tgt)
                tgt_node = all_nodes.get(tgt)
                if tgt_node:
                    downstream.append({
                        "id": tgt_node.id,
                        "name": tgt_node.name,
                        "sector_tag": tgt_node.sector_tag,
                        "elasticity": edge.elasticity,
                        "typical_lag_months": edge.typical_lag_months,
                        "confidence": edge.confidence,
                        "mechanism": edge.mechanism_description,
                    })
                queue.append(tgt)

    return {"node_id": node_id, "node_name": node.name, "downstream_count": len(downstream), "downstream": downstream}


# =============================================================================
# SIMULATION ENDPOINTS
# =============================================================================


@router.post("/simulate")
async def run_simulation(req: SimulateRequest, db: Session = Depends(get_db)):
    """
    Run a cascade simulation for a given macro shock.

    Example: {node_id: <DFF id>, change_pct: 1.0} simulates a +100bps Fed Funds Rate hike
    and returns estimated downstream impacts on all affected nodes.
    """
    from app.core.macro_models import CascadeScenario, MacroNode
    from app.services.macro_cascade_engine import MacroCascadeEngine

    node = db.get(MacroNode, req.node_id)
    if not node:
        raise HTTPException(status_code=404, detail="Macro node not found")

    # Build scenario name if not provided
    direction = "+" if req.change_pct >= 0 else ""
    scenario_name = req.name or f"{node.name} {direction}{req.change_pct:g}%"

    scenario = CascadeScenario(
        name=scenario_name,
        description=req.description,
        input_node_id=req.node_id,
        input_change_pct=req.change_pct,
        horizon_months=req.horizon_months,
        as_of_date=req.as_of_date,
    )
    db.add(scenario)
    db.commit()
    db.refresh(scenario)

    engine = MacroCascadeEngine(db)
    results, persisted_count = engine.run_scenario(scenario)

    return {
        "scenario_id": scenario.id,
        "scenario_name": scenario.name,
        "input_node": node.name,
        "input_change_pct": req.change_pct,
        "horizon_months": req.horizon_months,
        "nodes_impacted": len(results),
        "persisted_results": persisted_count,
        "results": [
            {
                "node_id": r.node_id,
                "node_name": r.node_name,
                "estimated_impact_pct": r.estimated_impact_pct,
                "peak_impact_month": r.peak_impact_month,
                "confidence": r.confidence,
                "distance_from_input": r.distance_from_input,
                "impact_path": r.impact_path,
            }
            for r in results
        ],
    }


@router.get("/scenarios")
async def list_scenarios(limit: int = 50, db: Session = Depends(get_db)):
    """List all saved cascade scenarios."""
    from app.core.macro_models import CascadeScenario, MacroNode

    stmt = (
        select(CascadeScenario, MacroNode)
        .join(MacroNode, CascadeScenario.input_node_id == MacroNode.id)
        .order_by(desc(CascadeScenario.created_at))
        .limit(limit)
    )
    rows = db.execute(stmt).all()

    return {
        "scenarios": [
            {
                "id": s.id,
                "name": s.name,
                "description": s.description,
                "input_node_id": s.input_node_id,
                "input_node_name": n.name,
                "input_change_pct": s.input_change_pct,
                "horizon_months": s.horizon_months,
                "as_of_date": s.as_of_date.isoformat() if s.as_of_date else None,
                "created_at": s.created_at.isoformat(),
            }
            for s, n in rows
        ],
        "count": len(rows),
    }


@router.get("/scenarios/{scenario_id}/results")
async def get_scenario_results(scenario_id: int, db: Session = Depends(get_db)):
    """Get all cascade results for a scenario, sorted by impact magnitude."""
    from app.core.macro_models import CascadeResult, CascadeScenario, MacroNode

    scenario = db.get(CascadeScenario, scenario_id)
    if not scenario:
        raise HTTPException(status_code=404, detail="Scenario not found")

    stmt = (
        select(CascadeResult, MacroNode)
        .join(MacroNode, CascadeResult.node_id == MacroNode.id)
        .where(CascadeResult.scenario_id == scenario_id)
        .order_by(CascadeResult.estimated_impact_pct)  # most negative first
    )
    rows = db.execute(stmt).all()

    input_node = db.get(MacroNode, scenario.input_node_id)

    return {
        "scenario_id": scenario_id,
        "scenario_name": scenario.name,
        "input_node": input_node.name if input_node else None,
        "input_change_pct": scenario.input_change_pct,
        "horizon_months": scenario.horizon_months,
        "result_count": len(rows),
        "results": [
            {
                "node_id": r.node_id,
                "node_name": n.name,
                "sector_tag": n.sector_tag,
                "node_type": n.node_type,
                "ticker": n.ticker,
                "estimated_impact_pct": r.estimated_impact_pct,
                "peak_impact_month": r.peak_impact_month,
                "confidence": r.confidence,
                "distance_from_input": r.distance_from_input,
                "impact_path": r.impact_path,
                "computed_at": r.computed_at.isoformat(),
            }
            for r, n in rows
        ],
    }


# =============================================================================
# COMPANY IMPACT ENDPOINTS
# =============================================================================


@router.get("/company-impact/{ticker}")
async def get_company_macro_impact(ticker: str, db: Session = Depends(get_db)):
    """Return macro node linkages for a company — what macro factors drive this company."""
    from app.core.macro_models import CompanyMacroLinkage, MacroNode

    stmt = (
        select(CompanyMacroLinkage, MacroNode)
        .join(MacroNode, CompanyMacroLinkage.node_id == MacroNode.id)
        .where(CompanyMacroLinkage.ticker == ticker.upper())
        .order_by(CompanyMacroLinkage.linkage_strength.desc())
    )
    rows = db.execute(stmt).all()

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No macro linkages found for ticker {ticker.upper()}. "
                   "Run POST /macro/collect/seed-relationships to seed linkages.",
        )

    return {
        "ticker": ticker.upper(),
        "linkage_count": len(rows),
        "macro_drivers": [
            {
                "node_id": n.id,
                "node_name": n.name,
                "sector_tag": n.sector_tag,
                "series_id": n.series_id,
                "linkage_type": lnk.linkage_type,
                "linkage_strength": lnk.linkage_strength,
                "direction": lnk.direction,
                "evidence_source": lnk.evidence_source,
                "evidence_text": lnk.evidence_text,
                "current_value": n.current_value,
                "current_value_date": n.current_value_date.isoformat() if n.current_value_date else None,
            }
            for lnk, n in rows
        ],
    }


@router.get("/portfolio-impact")
async def get_portfolio_macro_impact(db: Session = Depends(get_db)):
    """
    For all PE portfolio companies, show their macro exposure.

    Returns companies sorted by number of macro linkages (most exposed first).
    """
    from sqlalchemy import func as sqlfunc
    from app.core.macro_models import CompanyMacroLinkage, MacroNode

    # Aggregate by company
    stmt = (
        select(
            CompanyMacroLinkage.ticker,
            CompanyMacroLinkage.company_name,
            sqlfunc.count(CompanyMacroLinkage.id).label("linkage_count"),
            sqlfunc.avg(CompanyMacroLinkage.linkage_strength).label("avg_strength"),
        )
        .group_by(CompanyMacroLinkage.ticker, CompanyMacroLinkage.company_name)
        .order_by(sqlfunc.count(CompanyMacroLinkage.id).desc())
    )
    rows = db.execute(stmt).all()

    return {
        "portfolio_count": len(rows),
        "companies": [
            {
                "ticker": r.ticker,
                "company_name": r.company_name,
                "macro_linkage_count": r.linkage_count,
                "avg_linkage_strength": round(float(r.avg_strength), 3) if r.avg_strength else None,
            }
            for r in rows
        ],
    }


@router.get("/current-environment")
async def get_current_macro_environment(db: Session = Depends(get_db)):
    """
    Return current FRED values for key macro nodes — snapshot of the macro environment.

    Useful for showing context alongside cascade simulations.
    """
    from app.core.macro_models import MacroNode

    # Key series we always want to surface
    key_series = [
        "DFF", "DGS10", "MORTGAGE30US", "HOUST", "HSN1F",
        "UNRATE", "CPIAUCSL", "DCOILWTICO", "GDPC1", "UMCSENT",
    ]

    stmt = select(MacroNode).where(MacroNode.series_id.in_(key_series))
    nodes = db.execute(stmt).scalars().all()

    # Sort by sector, then series_id
    nodes_sorted = sorted(nodes, key=lambda n: (n.sector_tag or "z", n.series_id or ""))

    return {
        "as_of": None,  # populated when nodes have current_value_date
        "indicators": [
            {
                "series_id": n.series_id,
                "name": n.name,
                "sector_tag": n.sector_tag,
                "current_value": n.current_value,
                "unit": n.unit,
                "current_value_date": n.current_value_date.isoformat() if n.current_value_date else None,
                "is_leading_indicator": n.is_leading_indicator,
            }
            for n in nodes_sorted
        ],
        "node_count": len(nodes_sorted),
    }


# =============================================================================
# COLLECTION ENDPOINTS
# =============================================================================


@router.post("/collect/edgar-facts")
async def collect_edgar_facts(
    background_tasks: BackgroundTasks,
    tickers: Optional[list[str]] = None,
):
    """Trigger SEC EDGAR XBRL company facts ingestion for anchor companies."""
    import asyncio

    target_tickers = tickers or ["SHW", "DHI", "LEN", "HD", "LOW", "XOM"]

    def run_ingest():
        from app.sources.edgar_company_facts.client import EDGARCompanyFactsClient, TARGET_COMPANIES
        from app.sources.edgar_company_facts.ingest import EDGARCompanyFactsIngestor
        from app.core.database import get_engine
        import psycopg2

        # Filter companies to requested tickers
        companies = [c for c in TARGET_COMPANIES if c["ticker"] in target_tickers]

        # Fetch from EDGAR
        client = EDGARCompanyFactsClient()
        records = asyncio.run(client.fetch_all())

        if tickers:
            records = [r for r in records if r["ticker"] in target_tickers]

        # Get raw psycopg2 connection via SQLAlchemy engine
        engine = get_engine()
        with engine.connect() as sa_conn:
            raw_conn = sa_conn.connection.dbapi_connection
            ingestor = EDGARCompanyFactsIngestor(raw_conn)
            result = ingestor.upsert_records(records)
            logger.info(f"EDGAR ingest complete: {result}")

    background_tasks.add_task(run_ingest)

    return {
        "status": "started",
        "message": "SEC EDGAR company facts ingestion started in background",
        "tickers": target_tickers,
    }


@router.post("/collect/sensitivity")
async def collect_sensitivity(
    background_tasks: BackgroundTasks,
    tickers: Optional[list[str]] = None,
    db: Session = Depends(get_db),
):
    """
    Trigger 10-K macro sensitivity extraction for portfolio companies.

    Uses MacroSensitivityAgent to parse Risk Factor sections of SEC 10-K
    filings and create CompanyMacroLinkage records for each company.
    """
    import asyncio

    def run_sensitivity():
        try:
            from app.agents.macro_sensitivity_agent import MacroSensitivityAgent

            agent = MacroSensitivityAgent(db)
            asyncio.run(agent.run(tickers=tickers))
            logger.info("Macro sensitivity extraction complete")
        except Exception as exc:
            logger.error(f"Sensitivity extraction error: {exc}", exc_info=True)

    background_tasks.add_task(run_sensitivity)

    return {
        "status": "started",
        "message": "10-K macro sensitivity extraction started in background",
        "tickers": tickers or "all PE portfolio companies",
    }


@router.post("/collect/sync-node-values")
async def sync_node_values(db: Session = Depends(get_db)):
    """
    Sync current_value on all macro nodes from the underlying FRED/BLS data tables.

    Reads the most recent observation for each series_id from:
    - fred_economic_indicators (for fred_series nodes)
    - bls_ppi (for bls_series nodes with series_id starting with WPU/PCU)

    Updates macro_nodes.current_value + current_value_date in place.
    Returns count of nodes updated.
    """
    from sqlalchemy import text
    from app.core.macro_models import MacroNode

    updated = 0
    skipped = 0

    # Load all nodes that have a series_id
    nodes = db.execute(
        select(MacroNode).where(MacroNode.series_id.isnot(None))
    ).scalars().all()

    for node in nodes:
        sid = node.series_id
        latest_value = None
        latest_date = None

        # FRED series — search across all fred_* category tables
        if node.node_type in ("fred_series", "custom") or (
            sid and not sid.startswith(("WPU", "PCU", "CEU", "LNS", "CES"))
        ):
            fred_tables = [
                "fred_economic_indicators",
                "fred_housing_market",
                "fred_consumer_sentiment",
                "fred_commodities",
                "fred_interest_rates",
                "fred_monetary_aggregates",
                "fred_industrial_production",
            ]
            for tbl in fred_tables:
                try:
                    row = db.execute(
                        text(
                            f"SELECT value, date FROM {tbl} "
                            "WHERE series_id = :sid ORDER BY date DESC LIMIT 1"
                        ),
                        {"sid": sid},
                    ).fetchone()
                    if row and row[0] is not None:
                        latest_value = float(row[0])
                        latest_date = row[1]
                        break
                except Exception:
                    continue

        # BLS PPI series (WPU prefix)
        if latest_value is None and sid and sid.startswith(("WPU", "PCU")):
            row = db.execute(
                text(
                    "SELECT value, year, period_name FROM bls_ppi "
                    "WHERE series_id = :sid ORDER BY year DESC, period_name DESC LIMIT 1"
                ),
                {"sid": sid},
            ).fetchone()
            if row:
                latest_value = float(row[0]) if row[0] is not None else None
                # Construct approximate date from year + period
                try:
                    import datetime
                    year = int(row[1])
                    period = str(row[2])  # e.g. "December", "September"
                    month_map = {
                        "January": 1, "February": 2, "March": 3, "April": 4,
                        "May": 5, "June": 6, "July": 7, "August": 8,
                        "September": 9, "October": 10, "November": 11, "December": 12,
                    }
                    month = month_map.get(period, 12)
                    latest_date = datetime.date(year, month, 1)
                except Exception:
                    latest_date = None

        if latest_value is not None:
            node.current_value = latest_value
            node.current_value_date = latest_date
            updated += 1
        else:
            skipped += 1

    db.commit()

    return {
        "status": "complete",
        "nodes_updated": updated,
        "nodes_skipped_no_data": skipped,
        "message": f"Synced current_value for {updated} macro nodes ({skipped} had no data yet).",
    }


@router.post("/collect/seed-relationships")
async def seed_causal_relationships(db: Session = Depends(get_db)):
    """
    Seed (or re-seed) all known causal relationships into the graph.

    Idempotent — existing nodes/edges are updated, new ones are inserted.
    Returns counts of nodes and edges created/updated.
    """
    from app.services.macro_node_seeder import seed_causal_graph

    result = await seed_causal_graph(db)

    nodes_total = result["nodes_created"] + result["nodes_existing"]
    edges_total = result["edges_created"] + result["edges_existing"]

    return {
        "status": "complete",
        "nodes_upserted": nodes_total,
        "nodes_created": result["nodes_created"],
        "nodes_existing": result["nodes_existing"],
        "edges_upserted": edges_total,
        "edges_created": result["edges_created"],
        "edges_existing": result["edges_existing"],
        "edges_skipped": result["edges_skipped"],
        "message": (
            f"Seeded {nodes_total} macro nodes and {edges_total} causal edges "
            f"({result['nodes_created']} new, {result['edges_created']} new edges)."
        ),
    }
