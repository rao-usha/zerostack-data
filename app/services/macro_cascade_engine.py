"""
Macro Cascade Engine — BFS simulation of causal chain effects.

Given a scenario (input node + % change), traverses the causal graph
and computes estimated impact on all downstream nodes.

Algorithm:
- BFS from input node outward
- At each hop: impact = parent_impact × elasticity × damping_factor
- Cumulative lag = sum of edge lags along path
- Damping factor: 0.7 per hop (distant effects attenuate)
- Max depth: 6 hops (0.7^6 ≈ 12% of original signal — noise threshold)
- Confidence: product of edge confidences along best path
"""

import logging
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

DAMPING_FACTOR = 0.70     # signal attenuation per hop
MAX_DEPTH = 6             # maximum BFS depth (0.7^6 ≈ 0.12 of original)
MIN_IMPACT_THRESHOLD = 0.01  # ignore impacts below 1% (noise floor)


# =============================================================================
# DATA CLASSES
# =============================================================================


@dataclass
class NodeImpact:
    """Computed impact on a single downstream node for a given scenario."""

    node_id: int
    node_name: str
    estimated_impact_pct: float
    peak_impact_month: int
    confidence: float
    distance_from_input: int
    impact_path: list = field(default_factory=list)  # node names in causal chain


# =============================================================================
# ENGINE
# =============================================================================


class MacroCascadeEngine:
    """
    BFS cascade simulator for the macro causal graph.

    Usage:
        engine = MacroCascadeEngine(db_session)
        results, count = engine.run_scenario(scenario)
    """

    def __init__(self, db_session):
        self.db = db_session

    # ------------------------------------------------------------------
    # Graph loading
    # ------------------------------------------------------------------

    def _load_graph(self) -> tuple[dict, dict]:
        """
        Load all active nodes and edges into memory for BFS traversal.

        Returns:
            nodes: dict of node_id → MacroNode
            edges_by_source: dict of source_node_id → list[CausalEdge]
        """
        from sqlalchemy import select

        from app.core.macro_models import CausalEdge, MacroNode

        nodes = {
            n.id: n
            for n in self.db.execute(select(MacroNode)).scalars().all()
        }

        edges_by_source: dict[int, list] = {}
        active_edges = self.db.execute(
            select(CausalEdge).where(CausalEdge.is_active == True)  # noqa: E712
        ).scalars().all()

        for edge in active_edges:
            edges_by_source.setdefault(edge.source_node_id, []).append(edge)

        logger.debug(
            f"Graph loaded: {len(nodes)} nodes, "
            f"{sum(len(v) for v in edges_by_source.values())} active edges"
        )
        return nodes, edges_by_source

    # ------------------------------------------------------------------
    # BFS simulation
    # ------------------------------------------------------------------

    def simulate(self, scenario) -> list[NodeImpact]:
        """
        Run BFS cascade simulation for a CascadeScenario.

        The BFS state tuple is:
            (node_id, impact_pct, cumulative_lag, confidence, depth, path)

        At each step the impact is multiplied by:
            edge.elasticity × DAMPING_FACTOR

        We keep the *strongest* (highest absolute impact) path to each node.
        Nodes already visited with equal or stronger impact are skipped.

        Returns:
            list[NodeImpact] sorted by abs(estimated_impact_pct) descending.
        """
        nodes, edges_by_source = self._load_graph()

        if scenario.input_node_id not in nodes:
            raise ValueError(
                f"Input node {scenario.input_node_id} not found in macro_nodes"
            )

        input_change = scenario.input_change_pct
        horizon = scenario.horizon_months or 24
        input_node_id = scenario.input_node_id
        input_node_name = nodes[input_node_id].name

        # BFS queue items: (node_id, impact, lag, confidence, depth, path)
        queue: deque = deque()
        # visited: node_id → best NodeImpact recorded so far
        visited: dict[int, NodeImpact] = {}

        # Seed the queue with direct neighbours of the input node
        for edge in edges_by_source.get(input_node_id, []):
            target_id = edge.target_node_id
            if target_id not in nodes:
                continue

            edge_impact = input_change * edge.elasticity * DAMPING_FACTOR
            edge_lag = edge.typical_lag_months
            edge_conf = edge.confidence
            path = [input_node_name, nodes[target_id].name]

            queue.append((target_id, edge_impact, edge_lag, edge_conf, 1, path))

        # BFS traversal
        while queue:
            node_id, impact, lag, confidence, depth, path = queue.popleft()

            # Hard limits
            if depth > MAX_DEPTH:
                continue
            if lag > horizon:
                continue
            if abs(impact) < MIN_IMPACT_THRESHOLD:
                continue

            node = nodes.get(node_id)
            if not node:
                continue

            # Keep only the strongest path to each node
            if node_id in visited:
                if abs(impact) <= abs(visited[node_id].estimated_impact_pct):
                    continue  # weaker path — skip

            visited[node_id] = NodeImpact(
                node_id=node_id,
                node_name=node.name,
                estimated_impact_pct=round(impact, 4),
                peak_impact_month=min(lag, horizon),
                confidence=round(confidence, 3),
                distance_from_input=depth,
                impact_path=list(path),
            )

            # Expand neighbours
            for edge in edges_by_source.get(node_id, []):
                next_id = edge.target_node_id

                # Never loop back to the input node
                if next_id == input_node_id:
                    continue
                if depth + 1 > MAX_DEPTH:
                    continue

                next_impact = impact * edge.elasticity * DAMPING_FACTOR
                next_lag = lag + edge.typical_lag_months
                next_conf = confidence * edge.confidence

                # Skip if we already have a better path and this one is weaker
                if next_id in visited and abs(next_impact) <= abs(visited[next_id].estimated_impact_pct):
                    continue

                next_path = path + [nodes[next_id].name] if next_id in nodes else list(path)
                queue.append((next_id, next_impact, next_lag, next_conf, depth + 1, next_path))

        # Sort by absolute impact magnitude descending
        results = sorted(
            visited.values(),
            key=lambda x: abs(x.estimated_impact_pct),
            reverse=True,
        )

        logger.info(
            f"Scenario {scenario.id!r}: {len(results)} nodes impacted "
            f"from input change of {input_change:+.1f}% on '{input_node_name}'"
        )
        return results

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def persist_results(self, scenario_id: int, results: list[NodeImpact]) -> int:
        """
        Persist CascadeResult records for a scenario.

        Clears any previous results for the scenario before inserting new ones,
        so this is safe to call multiple times (re-run scenario).

        Returns:
            Number of CascadeResult records persisted.
        """
        from sqlalchemy import delete

        from app.core.macro_models import CascadeResult

        # Clear previous results for this scenario
        self.db.execute(
            delete(CascadeResult).where(CascadeResult.scenario_id == scenario_id)
        )

        persisted = 0
        for impact in results:
            result = CascadeResult(
                scenario_id=scenario_id,
                node_id=impact.node_id,
                estimated_impact_pct=impact.estimated_impact_pct,
                peak_impact_month=impact.peak_impact_month,
                confidence=impact.confidence,
                impact_path=impact.impact_path,
                distance_from_input=impact.distance_from_input,
                computed_at=datetime.utcnow(),
            )
            self.db.add(result)
            persisted += 1

        self.db.commit()
        logger.info(f"Persisted {persisted} CascadeResult records for scenario {scenario_id}")
        return persisted

    # ------------------------------------------------------------------
    # Convenience
    # ------------------------------------------------------------------

    def run_scenario(self, scenario) -> tuple[list[NodeImpact], int]:
        """
        Simulate and persist in a single call.

        Returns:
            (results, persisted_count)
        """
        results = self.simulate(scenario)
        count = self.persist_results(scenario.id, results)
        return results, count
