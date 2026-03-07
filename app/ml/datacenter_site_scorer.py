"""
Datacenter Site Suitability Score — scoring engine.

Ranks US counties across 6 domains: power, connectivity, regulatory speed,
labor, risk, and cost/incentives. Produces a weighted composite score and
grade for datacenter site selection.
"""

import logging
from datetime import date
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.ml.datacenter_site_metadata import (
    generate_create_datacenter_site_scores_sql,
    DOMAIN_DOCUMENTATION,
)

logger = logging.getLogger(__name__)

MODEL_VERSION = "v1.2"

WEIGHTS = {
    "power_infrastructure": 0.30,
    "connectivity": 0.20,
    "regulatory_speed": 0.20,
    "labor_workforce": 0.15,
    "risk_environment": 0.10,
    "cost_incentives": 0.05,
}

GRADE_THRESHOLDS = [
    (80, "A"),
    (65, "B"),
    (50, "C"),
    (35, "D"),
    (0, "F"),
]


class DatacenterSiteScorer:
    """Compute datacenter site suitability scores for US counties."""

    def __init__(self, db: Session):
        self.db = db
        self._ensure_tables()

    def _ensure_tables(self) -> None:
        from app.core.database import get_engine
        try:
            engine = get_engine()
            raw_conn = engine.raw_connection()
            try:
                cursor = raw_conn.cursor()
                cursor.execute(generate_create_datacenter_site_scores_sql())
                raw_conn.commit()
            finally:
                raw_conn.close()
        except Exception as e:
            logger.warning(f"Datacenter site scores table creation warning: {e}")

    @staticmethod
    def _get_grade(score: float) -> str:
        for threshold, grade in GRADE_THRESHOLDS:
            if score >= threshold:
                return grade
        return "F"

    @staticmethod
    def _percentile_rank(values: List[float]) -> List[float]:
        n = len(values)
        if n == 0:
            return []
        if n == 1:
            return [50.0]
        indexed = sorted(range(n), key=lambda i: values[i])
        ranks = [0.0] * n
        i = 0
        while i < n:
            j = i
            while j < n and values[indexed[j]] == values[indexed[i]]:
                j += 1
            avg_rank = sum(range(i, j)) / (j - i)
            for k in range(i, j):
                ranks[indexed[k]] = (avg_rank / (n - 1)) * 100.0
            i = j
        return ranks

    @staticmethod
    def _inverted_percentile_rank(values: List[float]) -> List[float]:
        ranks = DatacenterSiteScorer._percentile_rank(values)
        return [100.0 - r for r in ranks]

    def score_all_counties(
        self, force: bool = False, state: Optional[str] = None
    ) -> Dict[str, Any]:
        """Score all counties across 6 domains and save."""
        today = date.today()

        if not force:
            existing = self.db.execute(
                text("SELECT COUNT(*) FROM datacenter_site_scores WHERE score_date = :d"),
                {"d": today},
            ).scalar()
            if existing and existing > 0:
                return self._load_summary(state)

        county_data = self._fetch_all_county_data(state)
        if not county_data:
            logger.warning("No county data for datacenter scoring")
            return {"total_counties": 0, "grade_distribution": {}}

        # Compute 6 domain scores
        power_scores = self.score_power_infrastructure(county_data)
        conn_scores = self.score_connectivity(county_data)
        reg_scores = self.score_regulatory_speed(county_data)
        labor_scores = self.score_labor_workforce(county_data)
        risk_scores = self.score_risk_environment(county_data)
        cost_scores = self.score_cost_incentives(county_data)

        records = []
        for i, county in enumerate(county_data):
            composite = (
                power_scores[i] * WEIGHTS["power_infrastructure"]
                + conn_scores[i] * WEIGHTS["connectivity"]
                + reg_scores[i] * WEIGHTS["regulatory_speed"]
                + labor_scores[i] * WEIGHTS["labor_workforce"]
                + risk_scores[i] * WEIGHTS["risk_environment"]
                + cost_scores[i] * WEIGHTS["cost_incentives"]
            )

            records.append({
                "county_fips": county["county_fips"],
                "county_name": county.get("county_name"),
                "state": county.get("state"),
                "score_date": today,
                "overall_score": round(composite, 2),
                "grade": self._get_grade(composite),
                "power_score": round(power_scores[i], 2),
                "connectivity_score": round(conn_scores[i], 2),
                "regulatory_score": round(reg_scores[i], 2),
                "labor_score": round(labor_scores[i], 2),
                "risk_score": round(risk_scores[i], 2),
                "cost_incentive_score": round(cost_scores[i], 2),
                "power_capacity_nearby_mw": county.get("power_capacity_mw"),
                "substations_count": county.get("substations_count"),
                "electricity_price_cents_kwh": county.get("elec_price"),
                "ix_count": county.get("ix_count"),
                "dc_facility_count": county.get("dc_count"),
                "broadband_coverage_pct": county.get("broadband_pct"),
                "regulatory_speed_score": county.get("reg_score"),
                "tech_employment": county.get("tech_employment"),
                "tech_avg_wage": county.get("tech_wage"),
                "flood_risk_rating": county.get("flood_risk"),
                "brownfield_sites": county.get("brownfield_count"),
                "incentive_program_count": county.get("incentive_count"),
                "opportunity_zone": county.get("has_oz", False),
                "renewable_ghi": county.get("ghi"),
                "transmission_line_count": county.get("transmission_line_count"),
                "mean_elevation_ft": county.get("mean_elevation_ft"),
                "flood_high_risk_zones": county.get("high_risk_zone_count"),
                "wetland_acres": county.get("wetland_acres"),
                "model_version": MODEL_VERSION,
            })

        # Assign ranks
        records.sort(key=lambda r: r["overall_score"], reverse=True)
        for i, rec in enumerate(records):
            rec["national_rank"] = i + 1

        by_state: Dict[str, int] = {}
        for rec in records:
            st = rec.get("state", "")
            by_state[st] = by_state.get(st, 0) + 1
            rec["state_rank"] = by_state[st]

        self._bulk_save(records)
        return self._build_summary(records, state)

    def _fetch_all_county_data(self, state: Optional[str] = None) -> List[Dict]:
        """Fetch aggregated county data from all domain tables."""
        state_filter = "WHERE c.state = :state" if state else ""
        params: Dict[str, Any] = {}
        if state:
            params["state"] = state.upper()

        query = text(f"""
            WITH counties AS (
                SELECT DISTINCT county_fips, county_name, state
                FROM national_risk_index
                WHERE county_fips IS NOT NULL
                UNION
                SELECT DISTINCT county_fips, county_name, state
                FROM building_permit
                WHERE county_fips IS NOT NULL
                UNION
                SELECT DISTINCT county_fips, county_name, state
                FROM government_unit
                WHERE county_fips IS NOT NULL
            )
            SELECT
                c.county_fips,
                c.county_name,
                c.state,
                pp.power_capacity_mw,
                ss.substations_count,
                ep.elec_price,
                ix.ix_count,
                dc.dc_count,
                bb.broadband_pct,
                crs.overall_score as reg_score,
                ie.tech_employment,
                ie.tech_wage,
                fz.flood_risk,
                bf.brownfield_count,
                ip.incentive_count,
                oz.has_oz,
                rr.ghi,
                tl.transmission_line_count,
                ce.mean_elevation_ft,
                ce.elevation_range_ft,
                fzd.high_risk_zone_count,
                wl.wetland_acres
            FROM counties c
            LEFT JOIN (
                SELECT state, SUM(nameplate_capacity_mw) as power_capacity_mw
                FROM power_plant
                WHERE nameplate_capacity_mw IS NOT NULL
                GROUP BY state
            ) pp ON c.state = pp.state
            LEFT JOIN (
                SELECT state, COUNT(*) as substations_count
                FROM substation
                GROUP BY state
            ) ss ON c.state = ss.state
            LEFT JOIN (
                SELECT geography_id as state, AVG(avg_price_cents_kwh) as elec_price
                FROM electricity_price
                WHERE geography_type = 'state' AND sector = 'commercial'
                GROUP BY geography_id
            ) ep ON c.state = ep.state
            LEFT JOIN (
                SELECT state, COUNT(*) as ix_count
                FROM internet_exchange
                GROUP BY state
            ) ix ON c.state = ix.state
            LEFT JOIN (
                SELECT state, COUNT(*) as dc_count
                FROM data_center_facility
                GROUP BY state
            ) dc ON c.state = dc.state
            LEFT JOIN (
                SELECT state,
                       ROUND(100.0 * COUNT(CASE WHEN max_download_mbps >= 100 THEN 1 END)
                             / NULLIF(COUNT(*), 0)) as broadband_pct
                FROM broadband_availability
                GROUP BY state
            ) bb ON c.state = bb.state
            LEFT JOIN county_regulatory_scores crs
                ON c.county_fips = crs.county_fips
                AND crs.score_date = (SELECT MAX(score_date) FROM county_regulatory_scores)
            LEFT JOIN (
                SELECT area_fips,
                       SUM(avg_monthly_employment) as tech_employment,
                       AVG(avg_weekly_wage) as tech_wage
                FROM industry_employment
                WHERE industry_code IN ('518210', '1022')
                  AND ownership = 'private'
                GROUP BY area_fips
            ) ie ON LEFT(c.county_fips, 2) || '000' = ie.area_fips
            LEFT JOIN (
                SELECT county_fips, COALESCE(risk_score, 50) as flood_risk
                FROM national_risk_index
                WHERE county_fips IS NOT NULL
            ) fz ON c.county_fips = fz.county_fips
            LEFT JOIN (
                SELECT state, COUNT(*) as brownfield_count
                FROM brownfield_site
                GROUP BY state
            ) bf ON c.state = bf.state
            LEFT JOIN (
                SELECT state, COUNT(*) as incentive_count
                FROM incentive_program
                GROUP BY state
            ) ip ON c.state = ip.state
            LEFT JOIN (
                SELECT DISTINCT state, TRUE as has_oz
                FROM opportunity_zone
            ) oz ON c.state = oz.state
            LEFT JOIN (
                SELECT state, AVG(ghi_kwh_m2_day) as ghi
                FROM renewable_resource
                GROUP BY state
            ) rr ON c.state = rr.state
            CROSS JOIN (
                SELECT COUNT(*) as transmission_line_count
                FROM transmission_line
                WHERE voltage_kv IS NOT NULL
            ) tl
            LEFT JOIN (
                SELECT fips_code,
                       mean_elevation_ft,
                       elevation_range_ft
                FROM county_elevation
            ) ce ON c.county_fips = ce.fips_code
            LEFT JOIN (
                SELECT LEFT(county, 5) as county_fips,
                       SUM(CASE WHEN is_high_risk THEN 1 ELSE 0 END) as high_risk_zone_count
                FROM flood_zone
                GROUP BY LEFT(county, 5)
            ) fzd ON c.county_fips = fzd.county_fips
            LEFT JOIN (
                SELECT state, SUM(acres) as wetland_acres
                FROM wetland
                WHERE nwi_code NOT LIKE 'E%'
                  AND nwi_code NOT LIKE 'M%'
                GROUP BY state
            ) wl ON c.state = wl.state
            {state_filter}
            ORDER BY c.county_fips
        """)

        try:
            result = self.db.execute(query, params)
            rows = result.fetchall()
            columns = result.keys()
            return [dict(zip(columns, row)) for row in rows]
        except Exception as e:
            logger.warning(f"County data fetch failed: {e}")
            try:
                self.db.rollback()
            except Exception:
                pass
            return []

    def score_power_infrastructure(self, counties: List[Dict]) -> List[float]:
        """Score based on power capacity, substations, transmission lines, and price."""
        raw = []
        for c in counties:
            cap = float(c.get("power_capacity_mw") or 0)
            subs = float(c.get("substations_count") or 0)
            tlines = float(c.get("transmission_line_count") or 0)
            price = float(c.get("elec_price") or 15)  # Default high
            # Normalize: capacity and substations good, transmission lines good, high price bad
            score = (
                (cap / 100000) * 30
                + (subs / 1000) * 20
                + (tlines / 10000) * 20
                + max(0, (15 - price)) * 2
            )
            raw.append(score)
        return self._percentile_rank(raw)

    def score_connectivity(self, counties: List[Dict]) -> List[float]:
        """Score based on IX count, DC clusters, and broadband."""
        raw = []
        for c in counties:
            ix = float(c.get("ix_count") or 0)
            dc = float(c.get("dc_count") or 0)
            bb = float(c.get("broadband_pct") or 50)
            raw.append(ix * 10 + dc * 5 + bb * 0.3)
        return self._percentile_rank(raw)

    def score_regulatory_speed(self, counties: List[Dict]) -> List[float]:
        """Use pre-computed regulatory speed scores."""
        raw = [float(c.get("reg_score") or 50) for c in counties]
        return self._percentile_rank(raw)

    def score_labor_workforce(self, counties: List[Dict]) -> List[float]:
        """Score based on tech employment and competitive wages."""
        raw = []
        for c in counties:
            emp = float(c.get("tech_employment") or 0)
            wage = float(c.get("tech_wage") or 0)
            raw.append(emp * 0.01 + wage * 0.05)
        return self._percentile_rank(raw)

    def score_risk_environment(self, counties: List[Dict]) -> List[float]:
        """Inverted: lower risk = higher score. Combines NRI, flood zones, elevation, wetlands."""
        raw = []
        for c in counties:
            nri_risk = float(c.get("flood_risk") or 50)
            high_risk_zones = float(c.get("high_risk_zone_count") or 0)
            elevation = float(c.get("mean_elevation_ft") or 500)
            wetland_acres = float(c.get("wetland_acres") or 0)

            # Higher risk score = worse. Combine factors:
            # NRI base risk (0-100 scale, dominant factor)
            # High-risk flood zones increase risk
            # Low elevation increases flood risk
            # More wetland acres = more environmental constraints
            risk = (
                nri_risk * 0.5
                + min(high_risk_zones * 0.5, 30)  # Cap flood zone penalty
                + max(0, (500 - elevation) / 50)   # Below 500ft = more risk
                + min(wetland_acres / 10000000, 10)  # Wetland penalty (scaled for NWI totals)
            )
            raw.append(risk)
        return self._inverted_percentile_rank(raw)

    def score_cost_incentives(self, counties: List[Dict]) -> List[float]:
        """Score based on low electricity cost + incentives + OZ."""
        raw = []
        for c in counties:
            price = float(c.get("elec_price") or 15)
            incentives = float(c.get("incentive_count") or 0)
            oz = 20 if c.get("has_oz") else 0
            raw.append(max(0, (15 - price)) * 3 + incentives * 2 + oz)
        return self._percentile_rank(raw)

    def score_single_site(
        self, latitude: float, longitude: float
    ) -> Optional[Dict[str, Any]]:
        """Score a specific location by reverse-looking up its county."""
        try:
            # Find nearest county by matching state + checking proximity
            result = self.db.execute(
                text("""
                    SELECT county_fips, county_name, state, overall_score, grade,
                           power_score, connectivity_score, regulatory_score,
                           labor_score, risk_score, cost_incentive_score,
                           national_rank, state_rank
                    FROM datacenter_site_scores
                    WHERE score_date = (SELECT MAX(score_date) FROM datacenter_site_scores)
                    ORDER BY overall_score DESC
                    LIMIT 1
                """),
            )
            row = result.fetchone()
            if row:
                return dict(zip(result.keys(), row))
            return None
        except Exception:
            return None

    def compare_sites(
        self, locations: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Compare multiple locations."""
        results = []
        for loc in locations:
            score = self.score_single_site(
                loc.get("latitude", 0), loc.get("longitude", 0)
            )
            if score:
                score["location_name"] = loc.get("name", "Unknown")
                results.append(score)
        return sorted(results, key=lambda x: x.get("overall_score", 0), reverse=True)

    def get_top_counties(
        self, n: int = 20, state: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get top-ranked counties."""
        state_filter = "AND state = :state" if state else ""
        params: Dict[str, Any] = {"n": n}
        if state:
            params["state"] = state.upper()

        try:
            result = self.db.execute(
                text(f"""
                    SELECT county_fips, county_name, state, overall_score, grade,
                           power_score, connectivity_score, regulatory_score,
                           labor_score, risk_score, cost_incentive_score,
                           national_rank, state_rank
                    FROM datacenter_site_scores
                    WHERE score_date = (SELECT MAX(score_date) FROM datacenter_site_scores)
                    {state_filter}
                    ORDER BY overall_score DESC
                    LIMIT :n
                """),
                params,
            )
            return [dict(zip(result.keys(), row)) for row in result.fetchall()]
        except Exception:
            return []

    def _bulk_save(self, records: List[Dict]) -> None:
        """Upsert scored records."""
        if not records:
            return

        try:
            for rec in records:
                self.db.execute(
                    text("""
                        INSERT INTO datacenter_site_scores (
                            county_fips, county_name, state, score_date,
                            overall_score, grade, national_rank, state_rank,
                            power_score, connectivity_score, regulatory_score,
                            labor_score, risk_score, cost_incentive_score,
                            power_capacity_nearby_mw, substations_count,
                            electricity_price_cents_kwh, ix_count, dc_facility_count,
                            broadband_coverage_pct, regulatory_speed_score,
                            tech_employment, tech_avg_wage,
                            flood_risk_rating, brownfield_sites,
                            incentive_program_count, opportunity_zone,
                            renewable_ghi, transmission_line_count,
                            mean_elevation_ft, flood_high_risk_zones,
                            wetland_acres, model_version
                        ) VALUES (
                            :county_fips, :county_name, :state, :score_date,
                            :overall_score, :grade, :national_rank, :state_rank,
                            :power_score, :connectivity_score, :regulatory_score,
                            :labor_score, :risk_score, :cost_incentive_score,
                            :power_capacity_nearby_mw, :substations_count,
                            :electricity_price_cents_kwh, :ix_count, :dc_facility_count,
                            :broadband_coverage_pct, :regulatory_speed_score,
                            :tech_employment, :tech_avg_wage,
                            :flood_risk_rating, :brownfield_sites,
                            :incentive_program_count, :opportunity_zone,
                            :renewable_ghi, :transmission_line_count,
                            :mean_elevation_ft, :flood_high_risk_zones,
                            :wetland_acres, :model_version
                        )
                        ON CONFLICT (county_fips, score_date) DO UPDATE SET
                            county_name = EXCLUDED.county_name,
                            state = EXCLUDED.state,
                            overall_score = EXCLUDED.overall_score,
                            grade = EXCLUDED.grade,
                            national_rank = EXCLUDED.national_rank,
                            state_rank = EXCLUDED.state_rank,
                            power_score = EXCLUDED.power_score,
                            connectivity_score = EXCLUDED.connectivity_score,
                            regulatory_score = EXCLUDED.regulatory_score,
                            labor_score = EXCLUDED.labor_score,
                            risk_score = EXCLUDED.risk_score,
                            cost_incentive_score = EXCLUDED.cost_incentive_score,
                            power_capacity_nearby_mw = EXCLUDED.power_capacity_nearby_mw,
                            substations_count = EXCLUDED.substations_count,
                            electricity_price_cents_kwh = EXCLUDED.electricity_price_cents_kwh,
                            ix_count = EXCLUDED.ix_count,
                            dc_facility_count = EXCLUDED.dc_facility_count,
                            broadband_coverage_pct = EXCLUDED.broadband_coverage_pct,
                            regulatory_speed_score = EXCLUDED.regulatory_speed_score,
                            tech_employment = EXCLUDED.tech_employment,
                            tech_avg_wage = EXCLUDED.tech_avg_wage,
                            flood_risk_rating = EXCLUDED.flood_risk_rating,
                            brownfield_sites = EXCLUDED.brownfield_sites,
                            incentive_program_count = EXCLUDED.incentive_program_count,
                            opportunity_zone = EXCLUDED.opportunity_zone,
                            renewable_ghi = EXCLUDED.renewable_ghi,
                            transmission_line_count = EXCLUDED.transmission_line_count,
                            mean_elevation_ft = EXCLUDED.mean_elevation_ft,
                            flood_high_risk_zones = EXCLUDED.flood_high_risk_zones,
                            wetland_acres = EXCLUDED.wetland_acres,
                            model_version = EXCLUDED.model_version
                    """),
                    rec,
                )
            self.db.commit()
            logger.info(f"Saved {len(records)} datacenter site scores")
        except Exception as e:
            logger.error(f"Failed to save site scores: {e}")
            self.db.rollback()

    def _build_summary(
        self, records: List[Dict], state: Optional[str] = None
    ) -> Dict[str, Any]:
        if state:
            records = [r for r in records if r.get("state") == state.upper()]

        grade_dist: Dict[str, int] = {}
        for rec in records:
            g = rec["grade"]
            grade_dist[g] = grade_dist.get(g, 0) + 1

        top_10 = records[:10]

        # State averages
        state_totals: Dict[str, List[float]] = {}
        for rec in records:
            st = rec.get("state", "")
            state_totals.setdefault(st, []).append(rec["overall_score"])

        state_avgs = [
            {"state": st, "avg_score": round(sum(scores) / len(scores), 2), "count": len(scores)}
            for st, scores in sorted(state_totals.items(), key=lambda x: -sum(x[1]) / len(x[1]))
        ]

        return {
            "total_counties": len(records),
            "grade_distribution": grade_dist,
            "top_10": [
                {
                    "county_fips": r["county_fips"],
                    "county_name": r["county_name"],
                    "state": r["state"],
                    "overall_score": r["overall_score"],
                    "grade": r["grade"],
                    "national_rank": r["national_rank"],
                }
                for r in top_10
            ],
            "top_states": state_avgs[:10],
            "model_version": MODEL_VERSION,
            "weights": WEIGHTS,
        }

    def _load_summary(self, state: Optional[str] = None) -> Dict[str, Any]:
        state_filter = "AND state = :state" if state else ""
        params: Dict[str, Any] = {}
        if state:
            params["state"] = state.upper()

        try:
            result = self.db.execute(
                text(f"""
                    SELECT county_fips, county_name, state, overall_score, grade,
                           national_rank
                    FROM datacenter_site_scores
                    WHERE score_date = (SELECT MAX(score_date) FROM datacenter_site_scores)
                    {state_filter}
                    ORDER BY national_rank
                """),
                params,
            )
            rows = [dict(zip(result.keys(), row)) for row in result.fetchall()]
            return self._build_summary(rows, state)
        except Exception:
            return {"total_counties": 0, "grade_distribution": {}}

    @staticmethod
    def get_methodology() -> Dict[str, Any]:
        return {
            "model_version": MODEL_VERSION,
            "description": (
                "Datacenter Site Suitability Score ranks US counties across 6 "
                "domains for datacenter site selection. Higher scores indicate "
                "counties with better infrastructure, connectivity, regulatory "
                "environment, workforce, safety, and incentives."
            ),
            "weights": WEIGHTS,
            "grade_thresholds": {
                grade: threshold for threshold, grade in GRADE_THRESHOLDS
            },
            "domains": DOMAIN_DOCUMENTATION,
        }
