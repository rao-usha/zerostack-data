"""
Macro Node Seeder — seeds the causal graph with known economic relationships.

Pre-seeds 4 complete cascades:
1. Housing (Fed Funds Rate → Sherwin-Williams)
2. Credit/PE (rates → deal activity)
3. Energy/Industrial (oil → industrial margins)
4. Consumer/Labor (employment → spending)

All elasticities and lags are based on established economic research.
Run via POST /macro/collect/seed-relationships
"""

import logging

logger = logging.getLogger(__name__)


# =============================================================================
# NODE DEFINITIONS — use series_id matching FRED/BLS
# =============================================================================

NODES = [
    # --- Monetary Policy ---
    {
        "series_id": "DFF",
        "name": "Federal Funds Rate",
        "node_type": "fred_series",
        "unit": "percent",
        "frequency": "daily",
        "is_leading_indicator": True,
        "sector_tag": "credit",
        "description": "The interest rate at which depository institutions lend reserve balances to each other overnight",
    },
    {
        "series_id": "DGS10",
        "name": "10-Year Treasury Yield",
        "node_type": "fred_series",
        "unit": "percent",
        "frequency": "daily",
        "is_coincident": True,
        "sector_tag": "credit",
        "description": "Market yield on US Treasury securities at 10-year constant maturity",
    },

    # --- Housing Market ---
    {
        "series_id": "MORTGAGE30US",
        "name": "30-Year Fixed Mortgage Rate",
        "node_type": "fred_series",
        "unit": "percent",
        "frequency": "weekly",
        "is_leading_indicator": True,
        "sector_tag": "housing",
        "description": "Average commitment rate charged on 30-year fixed-rate mortgages",
    },
    {
        "series_id": "HOUST",
        "name": "Housing Starts",
        "node_type": "fred_series",
        "unit": "thousands of units",
        "frequency": "monthly",
        "is_coincident": True,
        "sector_tag": "housing",
        "description": "New privately-owned housing units started",
    },
    {
        "series_id": "HSN1F",
        "name": "New Home Sales",
        "node_type": "fred_series",
        "unit": "thousands of units",
        "frequency": "monthly",
        "is_coincident": True,
        "sector_tag": "housing",
        "description": "New one-family houses sold",
    },
    {
        "series_id": "EXHOSLUSM495S",
        "name": "Existing Home Sales",
        "node_type": "fred_series",
        "unit": "millions of units",
        "frequency": "monthly",
        "is_lagging": True,
        "sector_tag": "housing",
        "description": "Existing homes sold",
    },
    {
        "series_id": "PERMIT",
        "name": "Building Permits",
        "node_type": "fred_series",
        "unit": "thousands of units",
        "frequency": "monthly",
        "is_leading_indicator": True,
        "sector_tag": "housing",
        "description": "New private housing units authorized by building permits",
    },
    {
        "series_id": "CSUSHPINSA",
        "name": "Case-Shiller Home Price Index",
        "node_type": "fred_series",
        "unit": "index",
        "frequency": "monthly",
        "is_lagging": True,
        "sector_tag": "housing",
        "description": "S&P/Case-Shiller U.S. National Home Price Index",
    },
    {
        "series_id": "BSXRNSA",
        "name": "NAHB Housing Market Index",
        "node_type": "fred_series",
        "unit": "index",
        "frequency": "monthly",
        "is_leading_indicator": True,
        "sector_tag": "housing",
        "description": "Builder confidence in single-family housing market (>50 = positive)",
    },

    # --- Industry PPI ---
    {
        "series_id": "WPU0613",
        "name": "Paint & Coatings PPI",
        "node_type": "bls_series",
        "unit": "index",
        "frequency": "monthly",
        "is_coincident": True,
        "sector_tag": "industrial",
        "description": "Producer Price Index for Paint, Varnish, Lacquers, Coatings",
    },
    {
        "series_id": "WPU132",
        "name": "Construction Materials PPI",
        "node_type": "bls_series",
        "unit": "index",
        "frequency": "monthly",
        "is_coincident": True,
        "sector_tag": "industrial",
        "description": "PPI for construction materials",
    },

    # --- Energy ---
    {
        "series_id": "DCOILWTICO",
        "name": "WTI Crude Oil Price",
        "node_type": "fred_series",
        "unit": "USD per barrel",
        "frequency": "daily",
        "is_leading_indicator": True,
        "sector_tag": "energy",
        "description": "West Texas Intermediate crude oil spot price",
    },

    # --- Consumer ---
    {
        "series_id": "UMCSENT",
        "name": "Consumer Sentiment",
        "node_type": "fred_series",
        "unit": "index",
        "frequency": "monthly",
        "is_leading_indicator": True,
        "sector_tag": "consumer",
        "description": "University of Michigan Consumer Sentiment Index",
    },
    {
        "series_id": "UNRATE",
        "name": "Unemployment Rate",
        "node_type": "fred_series",
        "unit": "percent",
        "frequency": "monthly",
        "is_lagging": True,
        "sector_tag": "labor",
        "description": "Civilian unemployment rate",
    },
    {
        "series_id": "RSXFS",
        "name": "Retail Sales",
        "node_type": "fred_series",
        "unit": "millions of USD",
        "frequency": "monthly",
        "is_coincident": True,
        "sector_tag": "consumer",
        "description": "Advance retail sales excluding food service",
    },

    # --- Company Nodes ---
    {
        "ticker": "SHW",
        "series_id": None,
        "name": "Sherwin-Williams Revenue",
        "node_type": "company",
        "unit": "USD millions",
        "frequency": "quarterly",
        "is_lagging": True,
        "sector_tag": "housing",
        "description": "Sherwin-Williams quarterly revenue — ~45% from architectural coatings tied to new construction",
    },
    {
        "ticker": "DHI",
        "series_id": None,
        "name": "D.R. Horton Revenue",
        "node_type": "company",
        "unit": "USD millions",
        "frequency": "quarterly",
        "is_lagging": True,
        "sector_tag": "housing",
        "description": "D.R. Horton quarterly revenue — largest US homebuilder by volume",
    },

    # --- Custom Sector Nodes ---
    {
        "series_id": None,
        "name": "PE LBO Financing Cost",
        "node_type": "custom",
        "unit": "percent spread",
        "frequency": "monthly",
        "is_coincident": True,
        "sector_tag": "credit",
        "description": "Average cost of LBO debt (leveraged loan spread over SOFR)",
    },
    {
        "series_id": None,
        "name": "PE Deal Activity",
        "node_type": "custom",
        "unit": "deal count",
        "frequency": "quarterly",
        "is_lagging": True,
        "sector_tag": "credit",
        "description": "Count of PE buyout transactions per quarter",
    },
    {
        "series_id": None,
        "name": "Industrial Margins",
        "node_type": "custom",
        "unit": "percent",
        "frequency": "quarterly",
        "is_lagging": True,
        "sector_tag": "industrial",
        "description": "Average EBITDA margin for industrial/manufacturing sector",
    },
]


# =============================================================================
# EDGE DEFINITIONS
# Keys in "source" / "target" match either series_id or node name.
# =============================================================================

EDGES = [
    # === HOUSING CASCADE: Fed Rates → SHW ===
    {
        "source": "DFF",
        "target": "MORTGAGE30US",
        "direction": "positive",
        "elasticity": 0.85,
        "lag_min": 1,
        "lag_typical": 1,
        "lag_max": 2,
        "confidence": 0.90,
        "mechanism": (
            "Fed Funds Rate directly drives short-term borrowing costs; "
            "mortgage lenders price 30-year rates based on 10yr Treasury which tracks Fed policy"
        ),
    },
    {
        "source": "MORTGAGE30US",
        "target": "HOUST",
        "direction": "negative",
        "elasticity": -0.60,
        "lag_min": 3,
        "lag_typical": 4,
        "lag_max": 6,
        "confidence": 0.85,
        "mechanism": (
            "Higher mortgage rates reduce housing affordability, causing builders to pull back "
            "new starts; permit-to-start lag is ~3 months"
        ),
    },
    {
        "source": "MORTGAGE30US",
        "target": "HSN1F",
        "direction": "negative",
        "elasticity": -0.50,
        "lag_min": 2,
        "lag_typical": 3,
        "lag_max": 4,
        "confidence": 0.85,
        "mechanism": (
            "Higher rates price out buyers and reduce purchase applications, "
            "directly cutting new home sales"
        ),
    },
    {
        "source": "MORTGAGE30US",
        "target": "BSXRNSA",
        "direction": "negative",
        "elasticity": -0.70,
        "lag_min": 0,
        "lag_typical": 1,
        "lag_max": 2,
        "confidence": 0.88,
        "mechanism": (
            "Builder confidence (NAHB HMI) responds quickly to rate changes "
            "as affordability shifts buyer demand"
        ),
    },
    {
        "source": "HOUST",
        "target": "WPU0613",
        "direction": "positive",
        "elasticity": 0.70,
        "lag_min": 1,
        "lag_typical": 2,
        "lag_max": 3,
        "confidence": 0.75,
        "mechanism": (
            "New construction drives demand for architectural coatings; "
            "housing starts lead coatings volume by ~2 months (construction completion lag)"
        ),
    },
    {
        "source": "WPU0613",
        "target": "Sherwin-Williams Revenue",
        "direction": "positive",
        "elasticity": 0.80,
        "lag_min": 1,
        "lag_typical": 2,
        "lag_max": 3,
        "confidence": 0.70,
        "mechanism": (
            "Paint & coatings PPI tracks architectural segment demand; "
            "SHW Americas Group (~60% of revenue) is highly correlated with new construction volume"
        ),
    },
    {
        "source": "HSN1F",
        "target": "Sherwin-Williams Revenue",
        "direction": "positive",
        "elasticity": 0.65,
        "lag_min": 1,
        "lag_typical": 2,
        "lag_max": 4,
        "confidence": 0.72,
        "mechanism": (
            "Direct link: new home sales drive move-in painting demand "
            "(buyer-applied paint) and builder-applied paint orders"
        ),
    },
    {
        "source": "HOUST",
        "target": "D.R. Horton Revenue",
        "direction": "positive",
        "elasticity": 0.90,
        "lag_min": 3,
        "lag_typical": 6,
        "lag_max": 9,
        "confidence": 0.82,
        "mechanism": (
            "Housing starts and closings are directly correlated for homebuilders; "
            "DHI revenue lags starts by 6+ months (construction completion)"
        ),
    },

    # === CREDIT/PE CASCADE: Rates → Deal Activity ===
    {
        "source": "DFF",
        "target": "DGS10",
        "direction": "positive",
        "elasticity": 0.70,
        "lag_min": 0,
        "lag_typical": 1,
        "lag_max": 2,
        "confidence": 0.92,
        "mechanism": (
            "Fed policy expectations are primary driver of 10-year Treasury yield; "
            "short-end moves rapidly, long-end with slight lag"
        ),
    },
    {
        "source": "DGS10",
        "target": "PE LBO Financing Cost",
        "direction": "positive",
        "elasticity": 1.20,
        "lag_min": 1,
        "lag_typical": 2,
        "lag_max": 4,
        "confidence": 0.80,
        "mechanism": (
            "LBO debt is priced off SOFR/Treasuries plus credit spread; rising rates raise "
            "all-in cost of leveraged finance, often more than 1:1 due to credit spread widening"
        ),
    },
    {
        "source": "PE LBO Financing Cost",
        "target": "PE Deal Activity",
        "direction": "negative",
        "elasticity": -0.50,
        "lag_min": 3,
        "lag_typical": 4,
        "lag_max": 6,
        "confidence": 0.70,
        "mechanism": (
            "Higher financing costs compress LBO returns (IRR), reducing the number of deals "
            "that clear return hurdles; GPs become more selective"
        ),
    },

    # === ENERGY/INDUSTRIAL CASCADE: Oil → Margins ===
    {
        "source": "DCOILWTICO",
        "target": "Industrial Margins",
        "direction": "negative",
        "elasticity": -0.35,
        "lag_min": 1,
        "lag_typical": 2,
        "lag_max": 3,
        "confidence": 0.75,
        "mechanism": (
            "Energy costs are a major input cost for industrial/manufacturing companies; "
            "higher oil prices compress margins unless companies can pass through price increases"
        ),
    },
    {
        "source": "DCOILWTICO",
        "target": "WPU0613",
        "direction": "positive",
        "elasticity": 0.30,
        "lag_min": 1,
        "lag_typical": 2,
        "lag_max": 3,
        "confidence": 0.70,
        "mechanism": (
            "Petrochemical feedstocks (resins, solvents) used in paint/coatings are oil-derived; "
            "higher crude prices flow through to coatings input costs"
        ),
    },

    # === CONSUMER/LABOR CASCADE: Employment → Spending ===
    {
        "source": "UNRATE",
        "target": "UMCSENT",
        "direction": "negative",
        "elasticity": -0.80,
        "lag_min": 1,
        "lag_typical": 2,
        "lag_max": 3,
        "confidence": 0.85,
        "mechanism": (
            "Rising unemployment strongly reduces consumer confidence; "
            "household balance sheet uncertainty directly suppresses sentiment index"
        ),
    },
    {
        "source": "UMCSENT",
        "target": "RSXFS",
        "direction": "positive",
        "elasticity": 0.55,
        "lag_min": 1,
        "lag_typical": 2,
        "lag_max": 3,
        "confidence": 0.75,
        "mechanism": (
            "Consumer sentiment leads spending decisions by 1-2 months; "
            "confident consumers spend more on discretionary categories"
        ),
    },
    {
        "source": "UMCSENT",
        "target": "HSN1F",
        "direction": "positive",
        "elasticity": 0.45,
        "lag_min": 1,
        "lag_typical": 2,
        "lag_max": 4,
        "confidence": 0.72,
        "mechanism": (
            "Consumer confidence affects big-ticket purchase decisions like home buying; "
            "sentiment is a leading indicator for home sales"
        ),
    },
]


# =============================================================================
# SEEDER FUNCTION
# =============================================================================


async def seed_causal_graph(db_session) -> dict:
    """
    Seed all macro nodes and causal edges. Idempotent — safe to run multiple times.

    Lookup priority for node identity:
      1. series_id  (for FRED/BLS nodes)
      2. ticker     (for company nodes)
      3. name       (for custom nodes with no series_id or ticker)

    Returns a dict with counts of nodes/edges created vs already existing.
    """
    from sqlalchemy import select

    from app.core.macro_models import CausalEdge, MacroNode

    created_nodes = 0
    existing_nodes = 0
    # Map of lookup key → DB id — populated as nodes are upserted.
    # Each node gets TWO keys: its primary identifier (series_id/ticker) AND its name.
    node_id_map: dict[str, int] = {}

    # ------------------------------------------------------------------
    # 1. Upsert nodes
    # ------------------------------------------------------------------
    for node_def in NODES:
        series_id = node_def.get("series_id")
        ticker = node_def.get("ticker")
        name = node_def["name"]

        # Determine primary lookup key and query
        if series_id:
            identifier = series_id
            stmt = select(MacroNode).where(MacroNode.series_id == series_id)
        elif ticker:
            identifier = ticker
            stmt = select(MacroNode).where(
                MacroNode.ticker == ticker,
                MacroNode.node_type == "company",
            )
        else:
            identifier = name
            stmt = select(MacroNode).where(MacroNode.name == name)

        existing = db_session.execute(stmt).scalar_one_or_none()

        if existing:
            node_id_map[identifier] = existing.id
            node_id_map[name] = existing.id
            existing_nodes += 1
        else:
            node = MacroNode(
                node_type=node_def["node_type"],
                name=name,
                description=node_def.get("description"),
                unit=node_def.get("unit"),
                series_id=series_id,
                ticker=ticker,
                frequency=node_def.get("frequency"),
                is_leading_indicator=node_def.get("is_leading_indicator", False),
                is_coincident=node_def.get("is_coincident", False),
                is_lagging=node_def.get("is_lagging", False),
                sector_tag=node_def.get("sector_tag"),
            )
            db_session.add(node)
            db_session.flush()  # obtain the PK before next iteration
            node_id_map[identifier] = node.id
            node_id_map[name] = node.id
            created_nodes += 1
            logger.debug(f"Created MacroNode: {name!r} (id={node.id})")

    db_session.commit()
    logger.info(
        f"Nodes: {created_nodes} created, {existing_nodes} already existed"
    )

    # ------------------------------------------------------------------
    # 2. Upsert edges
    # ------------------------------------------------------------------
    created_edges = 0
    existing_edges = 0
    skipped_edges = 0

    for edge_def in EDGES:
        src_key = edge_def["source"]
        tgt_key = edge_def["target"]

        src_id = node_id_map.get(src_key)
        tgt_id = node_id_map.get(tgt_key)

        if not src_id or not tgt_id:
            logger.warning(
                f"Cannot create edge {src_key!r} → {tgt_key!r}: "
                f"source found={bool(src_id)}, target found={bool(tgt_id)}"
            )
            skipped_edges += 1
            continue

        stmt = select(CausalEdge).where(
            CausalEdge.source_node_id == src_id,
            CausalEdge.target_node_id == tgt_id,
        )
        existing_edge = db_session.execute(stmt).scalar_one_or_none()

        if existing_edge:
            existing_edges += 1
        else:
            edge = CausalEdge(
                source_node_id=src_id,
                target_node_id=tgt_id,
                relationship_direction=edge_def["direction"],
                elasticity=edge_def["elasticity"],
                typical_lag_months=edge_def["lag_typical"],
                lag_min_months=edge_def["lag_min"],
                lag_max_months=edge_def["lag_max"],
                confidence=edge_def["confidence"],
                mechanism_description=edge_def.get("mechanism"),
                is_active=True,
            )
            db_session.add(edge)
            created_edges += 1
            logger.debug(f"Created CausalEdge: {src_key!r} → {tgt_key!r}")

    db_session.commit()
    logger.info(
        f"Edges: {created_edges} created, {existing_edges} already existed, "
        f"{skipped_edges} skipped (node not found)"
    )

    return {
        "nodes_created": created_nodes,
        "nodes_existing": existing_nodes,
        "edges_created": created_edges,
        "edges_existing": existing_edges,
        "edges_skipped": skipped_edges,
    }
