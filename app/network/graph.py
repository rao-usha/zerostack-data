"""
Co-investor Network Analysis Engine.

Builds and analyzes network graphs showing investor relationships
based on shared portfolio investments and co-investment records.
"""

import logging
from typing import Dict, List, Optional, Tuple, Set
from collections import defaultdict
from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


class NetworkEngine:
    """
    Co-investor network analysis engine.

    Builds network graphs from:
    1. co_investments table (direct co-investor records)
    2. portfolio_companies table (investors sharing same companies)
    """

    def __init__(self, db: Session):
        self.db = db
        self._nodes: Dict[str, Dict] = {}
        self._edges: Dict[Tuple[str, str], Dict] = {}
        self._built = False

    def _node_id(self, investor_id: int, investor_type: str) -> str:
        """Generate unique node ID."""
        return f"{investor_type}_{investor_id}"

    def _parse_node_id(self, node_id: str) -> Tuple[str, int]:
        """Parse node ID back to type and ID."""
        parts = node_id.split("_", 1)
        return parts[0], int(parts[1])

    def build_network(self, force_rebuild: bool = False) -> None:
        """
        Build the full co-investor network from database.

        Combines data from co_investments table and shared portfolio companies.
        """
        if self._built and not force_rebuild:
            return

        self._nodes = {}
        self._edges = {}

        # Load all investors
        self._load_investors()

        # Build edges from co_investments table
        self._build_edges_from_coinvestments()

        # Build edges from shared portfolio companies
        self._build_edges_from_shared_portfolios()

        # Calculate node metrics
        self._calculate_node_metrics()

        self._built = True
        logger.info(
            f"Network built: {len(self._nodes)} nodes, {len(self._edges)} edges"
        )

    def _load_investors(self) -> None:
        """Load all investors as nodes."""
        # Load LPs
        lp_query = text("""
            SELECT id, name, lp_type as investor_subtype, jurisdiction as location
            FROM lp_fund
        """)
        result = self.db.execute(lp_query)
        for row in result.mappings():
            node_id = self._node_id(row["id"], "lp")
            self._nodes[node_id] = {
                "id": node_id,
                "investor_id": row["id"],
                "type": "lp",
                "name": row["name"],
                "subtype": row.get("investor_subtype"),
                "location": row.get("location"),
                "degree": 0,
                "weighted_degree": 0,
                "centrality": 0.0,
                "cluster_id": None,
            }

        # Load Family Offices
        fo_query = text("""
            SELECT id, name, type as investor_subtype, region as location
            FROM family_offices
        """)
        result = self.db.execute(fo_query)
        for row in result.mappings():
            node_id = self._node_id(row["id"], "family_office")
            self._nodes[node_id] = {
                "id": node_id,
                "investor_id": row["id"],
                "type": "family_office",
                "name": row["name"],
                "subtype": row.get("investor_subtype"),
                "location": row.get("location"),
                "degree": 0,
                "weighted_degree": 0,
                "centrality": 0.0,
                "cluster_id": None,
            }

    def _build_edges_from_coinvestments(self) -> None:
        """Build edges from co_investments table."""
        query = text("""
            SELECT primary_investor_id, primary_investor_type,
                   co_investor_name, co_investor_type,
                   deal_name, co_investment_count,
                   MIN(deal_date) as first_date,
                   MAX(deal_date) as last_date
            FROM co_investments
            GROUP BY primary_investor_id, primary_investor_type,
                     co_investor_name, co_investor_type, deal_name, co_investment_count
        """)
        result = self.db.execute(query)

        for row in result.mappings():
            source_id = self._node_id(
                row["primary_investor_id"], row["primary_investor_type"]
            )

            # Co-investor might not be in our database - create external node
            co_name = row["co_investor_name"]
            target_id = f"external_{hash(co_name) % 100000}"

            if target_id not in self._nodes:
                self._nodes[target_id] = {
                    "id": target_id,
                    "investor_id": None,
                    "type": "external",
                    "name": co_name,
                    "subtype": row.get("co_investor_type"),
                    "location": None,
                    "degree": 0,
                    "weighted_degree": 0,
                    "centrality": 0.0,
                    "cluster_id": None,
                }

            edge_key = tuple(sorted([source_id, target_id]))
            if edge_key not in self._edges:
                self._edges[edge_key] = {
                    "source": edge_key[0],
                    "target": edge_key[1],
                    "weight": 0,
                    "shared_companies": [],
                    "first_date": None,
                    "last_date": None,
                }

            edge = self._edges[edge_key]
            edge["weight"] += row.get("co_investment_count", 1)
            if row.get("deal_name"):
                if row["deal_name"] not in edge["shared_companies"]:
                    edge["shared_companies"].append(row["deal_name"])
            if row.get("first_date"):
                if edge["first_date"] is None or row["first_date"] < edge["first_date"]:
                    edge["first_date"] = row["first_date"]
            if row.get("last_date"):
                if edge["last_date"] is None or row["last_date"] > edge["last_date"]:
                    edge["last_date"] = row["last_date"]

    def _build_edges_from_shared_portfolios(self) -> None:
        """Build edges from investors sharing portfolio companies."""
        query = text("""
            SELECT
                a.investor_id as investor_a_id,
                a.investor_type as investor_a_type,
                b.investor_id as investor_b_id,
                b.investor_type as investor_b_type,
                ARRAY_AGG(DISTINCT a.company_name) as shared_companies,
                COUNT(DISTINCT a.company_name) as shared_count
            FROM portfolio_companies a
            JOIN portfolio_companies b
                ON LOWER(TRIM(a.company_name)) = LOWER(TRIM(b.company_name))
                AND (a.investor_id != b.investor_id OR a.investor_type != b.investor_type)
            WHERE a.current_holding = 1 AND b.current_holding = 1
                AND a.investor_id < b.investor_id OR
                    (a.investor_id = b.investor_id AND a.investor_type < b.investor_type)
            GROUP BY a.investor_id, a.investor_type, b.investor_id, b.investor_type
            HAVING COUNT(DISTINCT a.company_name) >= 1
        """)

        try:
            result = self.db.execute(query)
            for row in result.mappings():
                source_id = self._node_id(row["investor_a_id"], row["investor_a_type"])
                target_id = self._node_id(row["investor_b_id"], row["investor_b_type"])

                # Skip if either node doesn't exist
                if source_id not in self._nodes or target_id not in self._nodes:
                    continue

                edge_key = tuple(sorted([source_id, target_id]))
                if edge_key not in self._edges:
                    self._edges[edge_key] = {
                        "source": edge_key[0],
                        "target": edge_key[1],
                        "weight": 0,
                        "shared_companies": [],
                        "first_date": None,
                        "last_date": None,
                    }

                edge = self._edges[edge_key]
                edge["weight"] += row["shared_count"]
                shared = row.get("shared_companies") or []
                for company in shared:
                    if company and company not in edge["shared_companies"]:
                        edge["shared_companies"].append(company)
        except Exception as e:
            logger.warning(f"Error building edges from shared portfolios: {e}")

    def _calculate_node_metrics(self) -> None:
        """Calculate degree and centrality metrics for all nodes."""
        # Calculate degree
        for edge in self._edges.values():
            source = edge["source"]
            target = edge["target"]
            weight = edge["weight"]

            if source in self._nodes:
                self._nodes[source]["degree"] += 1
                self._nodes[source]["weighted_degree"] += weight
            if target in self._nodes:
                self._nodes[target]["degree"] += 1
                self._nodes[target]["weighted_degree"] += weight

        # Calculate simple centrality (normalized degree)
        max_degree = max((n["degree"] for n in self._nodes.values()), default=1)
        for node in self._nodes.values():
            node["centrality"] = (
                round(node["degree"] / max_degree, 3) if max_degree > 0 else 0
            )

    def get_network_graph(
        self,
        limit: Optional[int] = None,
        min_weight: int = 1,
        include_external: bool = False,
    ) -> Dict:
        """
        Get full network graph for visualization.

        Args:
            limit: Max number of edges to return
            min_weight: Minimum edge weight to include
            include_external: Include external (non-database) investors
        """
        self.build_network()

        # Filter edges
        filtered_edges = [e for e in self._edges.values() if e["weight"] >= min_weight]

        # Sort by weight descending
        filtered_edges.sort(key=lambda x: x["weight"], reverse=True)

        if limit:
            filtered_edges = filtered_edges[:limit]

        # Get nodes that appear in filtered edges
        active_node_ids = set()
        for edge in filtered_edges:
            active_node_ids.add(edge["source"])
            active_node_ids.add(edge["target"])

        # Filter nodes
        filtered_nodes = []
        for node_id in active_node_ids:
            if node_id in self._nodes:
                node = self._nodes[node_id]
                if not include_external and node["type"] == "external":
                    continue
                filtered_nodes.append(node)

        # Calculate stats
        total_weight = sum(e["weight"] for e in filtered_edges)
        avg_degree = (
            sum(n["degree"] for n in filtered_nodes) / len(filtered_nodes)
            if filtered_nodes
            else 0
        )
        max_possible_edges = len(filtered_nodes) * (len(filtered_nodes) - 1) / 2
        density = (
            len(filtered_edges) / max_possible_edges if max_possible_edges > 0 else 0
        )

        return {
            "nodes": filtered_nodes,
            "edges": filtered_edges,
            "stats": {
                "total_nodes": len(filtered_nodes),
                "total_edges": len(filtered_edges),
                "total_weight": total_weight,
                "avg_degree": round(avg_degree, 2),
                "density": round(density, 3),
            },
        }

    def get_investor_network(
        self,
        investor_id: int,
        investor_type: str,
        depth: int = 1,
        min_weight: int = 1,
    ) -> Dict:
        """
        Get ego network for a specific investor.

        Args:
            investor_id: Investor ID
            investor_type: 'lp' or 'family_office'
            depth: How many hops from the investor (1 = direct connections)
            min_weight: Minimum edge weight to include
        """
        self.build_network()

        center_id = self._node_id(investor_id, investor_type)
        if center_id not in self._nodes:
            return {
                "nodes": [],
                "edges": [],
                "stats": {"total_nodes": 0, "total_edges": 0},
            }

        # BFS to find connected nodes up to depth
        visited: Set[str] = {center_id}
        current_level: Set[str] = {center_id}

        for _ in range(depth):
            next_level: Set[str] = set()
            for node_id in current_level:
                for edge_key, edge in self._edges.items():
                    if edge["weight"] < min_weight:
                        continue
                    if edge["source"] == node_id and edge["target"] not in visited:
                        next_level.add(edge["target"])
                    elif edge["target"] == node_id and edge["source"] not in visited:
                        next_level.add(edge["source"])
            visited.update(next_level)
            current_level = next_level

        # Collect nodes and edges
        nodes = [self._nodes[nid] for nid in visited if nid in self._nodes]
        edges = [
            e
            for e in self._edges.values()
            if e["source"] in visited
            and e["target"] in visited
            and e["weight"] >= min_weight
        ]

        # Mark the center node
        for node in nodes:
            node["is_center"] = node["id"] == center_id

        return {
            "center": self._nodes.get(center_id),
            "nodes": nodes,
            "edges": edges,
            "stats": {
                "total_nodes": len(nodes),
                "total_edges": len(edges),
                "direct_connections": sum(
                    1 for n in nodes if n.get("is_center") is False
                ),
            },
        }

    def get_central_investors(self, limit: int = 20) -> List[Dict]:
        """
        Get most central/connected investors.

        Returns investors ranked by weighted degree (connection strength).
        """
        self.build_network()

        # Filter out external nodes and sort by weighted degree
        internal_nodes = [
            n
            for n in self._nodes.values()
            if n["type"] != "external" and n["degree"] > 0
        ]
        internal_nodes.sort(
            key=lambda x: (x["weighted_degree"], x["degree"]), reverse=True
        )

        return internal_nodes[:limit]

    def detect_clusters(self, min_cluster_size: int = 2) -> List[Dict]:
        """
        Detect investor clusters using connected components.

        Simple clustering based on graph connectivity.
        """
        self.build_network()

        # Build adjacency list
        adj: Dict[str, Set[str]] = defaultdict(set)
        for edge in self._edges.values():
            adj[edge["source"]].add(edge["target"])
            adj[edge["target"]].add(edge["source"])

        # Find connected components (simple clustering)
        visited: Set[str] = set()
        clusters: List[List[str]] = []

        for node_id in self._nodes:
            if node_id in visited:
                continue
            if self._nodes[node_id]["type"] == "external":
                continue

            # BFS to find component
            component: List[str] = []
            queue = [node_id]
            while queue:
                current = queue.pop(0)
                if current in visited:
                    continue
                visited.add(current)
                if self._nodes.get(current, {}).get("type") != "external":
                    component.append(current)
                for neighbor in adj[current]:
                    if neighbor not in visited:
                        queue.append(neighbor)

            if len(component) >= min_cluster_size:
                clusters.append(component)

        # Assign cluster IDs and build response
        result = []
        for cluster_id, members in enumerate(clusters, 1):
            # Update node cluster IDs
            for node_id in members:
                if node_id in self._nodes:
                    self._nodes[node_id]["cluster_id"] = cluster_id

            # Find common sectors/industries
            member_nodes = [self._nodes[m] for m in members if m in self._nodes]

            result.append(
                {
                    "id": cluster_id,
                    "size": len(members),
                    "members": [
                        {"id": n["id"], "name": n["name"], "type": n["type"]}
                        for n in member_nodes
                    ],
                    "avg_degree": round(
                        sum(n["degree"] for n in member_nodes) / len(member_nodes), 2
                    )
                    if member_nodes
                    else 0,
                }
            )

        # Sort by size descending
        result.sort(key=lambda x: x["size"], reverse=True)
        return result

    def find_path(
        self,
        source_id: int,
        source_type: str,
        target_id: int,
        target_type: str,
    ) -> Optional[Dict]:
        """
        Find shortest co-investment path between two investors.

        Returns the path with all intermediate investors and connections.
        """
        self.build_network()

        start = self._node_id(source_id, source_type)
        end = self._node_id(target_id, target_type)

        if start not in self._nodes or end not in self._nodes:
            return None

        if start == end:
            return {
                "found": True,
                "path_length": 0,
                "path": [self._nodes[start]],
                "edges": [],
            }

        # Build adjacency list
        adj: Dict[str, List[Tuple[str, Dict]]] = defaultdict(list)
        for edge in self._edges.values():
            adj[edge["source"]].append((edge["target"], edge))
            adj[edge["target"]].append((edge["source"], edge))

        # BFS to find shortest path
        visited = {start: None}
        edge_used = {start: None}
        queue = [start]

        while queue:
            current = queue.pop(0)
            if current == end:
                break

            for neighbor, edge in adj[current]:
                if neighbor not in visited:
                    visited[neighbor] = current
                    edge_used[neighbor] = edge
                    queue.append(neighbor)

        if end not in visited:
            return {"found": False, "path_length": -1, "path": [], "edges": []}

        # Reconstruct path
        path = []
        edges = []
        current = end
        while current is not None:
            path.append(self._nodes[current])
            if edge_used[current] is not None:
                edges.append(edge_used[current])
            current = visited[current]

        path.reverse()
        edges.reverse()

        return {
            "found": True,
            "path_length": len(path) - 1,
            "path": path,
            "edges": edges,
        }
