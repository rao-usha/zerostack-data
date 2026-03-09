"""
Add-On Target Finder — find bolt-on acquisition markets near a portfolio company.

Given a portfolio company's location (state + optional county) and industry,
finds nearby high-scoring markets from rollup_market_scores and enriches with
known prospect and EPA compliance data.
"""

import logging
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# State adjacency (FIPS code → list of adjacent state FIPS codes)
# Covers the contiguous 48 + DC. Alaska/Hawaii have no adjacents.
STATE_ADJACENCY: Dict[str, List[str]] = {
    "01": ["12", "13", "28", "47"],  # AL
    "04": ["06", "32", "35", "49", "08"],  # AZ
    "05": ["28", "29", "40", "47", "48", "22"],  # AR
    "06": ["04", "32", "41"],  # CA
    "08": ["04", "20", "31", "35", "40", "49", "56"],  # CO
    "09": ["25", "36", "44"],  # CT
    "10": ["24", "34", "42"],  # DE
    "11": ["24", "51"],  # DC
    "12": ["01", "13"],  # FL
    "13": ["01", "12", "37", "45", "47"],  # GA
    "16": ["30", "32", "41", "53", "56", "49"],  # ID
    "17": ["18", "19", "21", "29", "55"],  # IL
    "18": ["17", "21", "26", "39"],  # IN
    "19": ["17", "27", "29", "31", "46", "55"],  # IA
    "20": ["08", "29", "31", "40"],  # KS
    "21": ["17", "18", "29", "39", "47", "51", "54"],  # KY
    "22": ["05", "28", "48"],  # LA
    "23": ["33"],  # ME
    "24": ["10", "11", "42", "51", "54"],  # MD
    "25": ["09", "33", "36", "44", "50"],  # MA
    "26": ["18", "39", "55"],  # MI
    "27": ["19", "38", "46", "55"],  # MN
    "28": ["01", "05", "22", "47"],  # MS
    "29": ["05", "17", "19", "20", "21", "31", "40", "47"],  # MO
    "30": ["16", "38", "46", "56"],  # MT
    "31": ["08", "19", "20", "29", "46", "56"],  # NE
    "32": ["04", "06", "16", "41", "49"],  # NV
    "33": ["23", "25", "50"],  # NH
    "34": ["10", "36", "42"],  # NJ
    "35": ["04", "08", "40", "48", "49"],  # NM
    "36": ["09", "25", "34", "42", "50"],  # NY
    "37": ["13", "45", "47", "51"],  # NC
    "38": ["27", "30", "46"],  # ND
    "39": ["18", "21", "26", "42", "54"],  # OH
    "40": ["05", "08", "20", "29", "35", "48"],  # OK
    "41": ["06", "16", "32", "53"],  # OR
    "42": ["10", "24", "34", "36", "39", "54"],  # PA
    "44": ["09", "25"],  # RI
    "45": ["13", "37"],  # SC
    "46": ["19", "27", "30", "31", "38", "56"],  # SD
    "47": ["01", "05", "13", "21", "28", "29", "37", "51"],  # TN
    "48": ["05", "22", "35", "40"],  # TX
    "49": ["04", "08", "16", "32", "35", "56"],  # UT
    "50": ["25", "33", "36"],  # VT
    "51": ["11", "21", "24", "37", "47", "54"],  # VA
    "53": ["16", "41"],  # WA
    "54": ["21", "24", "39", "42", "51"],  # WV
    "55": ["17", "19", "26", "27"],  # WI
    "56": ["08", "16", "30", "31", "46", "49"],  # WY
}


class AddonTargetFinder:
    """Find bolt-on acquisition markets near a portfolio company."""

    def __init__(self, db: Session):
        self.db = db

    def find_targets(
        self,
        naics_code: str,
        state_fips: str,
        county_fips: Optional[str] = None,
        radius_states: int = 1,
        limit: int = 25,
    ) -> Dict[str, Any]:
        """Find add-on target markets.

        Args:
            naics_code: Industry NAICS code
            state_fips: 2-digit state FIPS of portfolio company
            county_fips: Optional 5-digit county FIPS for proximity scoring
            radius_states: How many adjacency hops (1 = same + adjacent states)
            limit: Max results
        """
        # Build set of target states
        target_states = self._get_target_states(state_fips, radius_states)

        # Get scored counties in target states
        state_list = list(target_states)
        placeholders = ", ".join(f":s{i}" for i in range(len(state_list)))
        params: Dict[str, Any] = {
            f"s{i}": s for i, s in enumerate(state_list)
        }
        params["naics"] = naics_code
        params["lim"] = limit

        query = text(f"""
            SELECT r.*
            FROM rollup_market_scores r
            WHERE r.naics_code = :naics
              AND r.state_fips IN ({placeholders})
            ORDER BY r.overall_score DESC
            LIMIT :lim
        """)

        try:
            rows = self.db.execute(query, params).mappings().fetchall()
        except Exception as e:
            logger.error(f"Error finding add-on targets: {e}")
            self.db.rollback()
            return {"error": str(e)}

        markets = [dict(r) for r in rows]

        # Enrich with proximity scoring
        for m in markets:
            m_state = m.get("state_fips", "")
            if m_state == state_fips:
                m["proximity"] = "same_state"
                m["proximity_score"] = 100
            elif m_state in STATE_ADJACENCY.get(state_fips, []):
                m["proximity"] = "adjacent_state"
                m["proximity_score"] = 70
            else:
                m["proximity"] = "extended"
                m["proximity_score"] = 40

            # Composite add-on score: market_quality(40%) + proximity(30%) +
            # known_targets(20% placeholder) + compliance(10% placeholder)
            market_q = float(m.get("overall_score", 0))
            prox = m["proximity_score"]
            m["addon_score"] = round(
                market_q * 0.40 + prox * 0.30 + 50 * 0.20 + 50 * 0.10, 2
            )

        # Sort by addon_score
        markets.sort(key=lambda m: m["addon_score"], reverse=True)

        # Enrich with known prospect counts (if medspa/vertical tables exist)
        self._enrich_prospect_counts(markets, naics_code)

        return {
            "naics_code": naics_code,
            "home_state": state_fips,
            "home_county": county_fips,
            "target_states": sorted(target_states),
            "radius_states": radius_states,
            "total_targets": len(markets),
            "targets": markets,
        }

    def _get_target_states(self, state_fips: str, radius: int) -> set:
        """Get set of state FIPS codes within N adjacency hops."""
        states = {state_fips}
        frontier = {state_fips}
        for _ in range(radius):
            next_frontier = set()
            for s in frontier:
                for adj in STATE_ADJACENCY.get(s, []):
                    if adj not in states:
                        next_frontier.add(adj)
                        states.add(adj)
            frontier = next_frontier
        return states

    def _enrich_prospect_counts(
        self, markets: List[Dict], naics_code: str
    ) -> None:
        """Add known prospect counts from discovery tables if available."""
        # Try medspa_prospects for healthcare NAICS
        if naics_code.startswith("6211"):
            try:
                for m in markets:
                    fips = m.get("county_fips", "")
                    # County FIPS → ZIP matching is approximate
                    state = m.get("state_fips", "")
                    q = text("""
                        SELECT COUNT(*) FROM medspa_prospects
                        WHERE state = (
                            SELECT state_abbr FROM irs_soi_county_income
                            WHERE county_fips = :fips LIMIT 1
                        )
                    """)
                    count = self.db.execute(q, {"fips": fips}).scalar() or 0
                    m["known_prospects"] = count
            except Exception:
                self.db.rollback()
                for m in markets:
                    m["known_prospects"] = None
        else:
            for m in markets:
                m["known_prospects"] = None
